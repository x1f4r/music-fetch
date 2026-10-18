import type { MediaSample } from '@/src/audio/media-clock';
import type { AgentApi, AgentHello, AgentTopics, PlayerInfo, PrepareOptions } from '@/src/capture/agent-protocol';
import { AGENT_METHODS } from '@/src/capture/agent-protocol';
import type { CaptureEvent } from '@/src/capture/types';
import { renderSong, SYNTHETIC_SONGS, type SyntheticSong } from '@/src/engine/host-mock-audio';
import { serveRpc, type PortLike, type ServedPort } from '@/src/messaging/bus';
import type { PageFacts } from '@/src/page/facts';

/** A mix of synthetic songs: [song, length in ms] in order. */
export type Mix = [SyntheticSong, number][];

export const THREE_SONG_MIX: Mix = [
  [SYNTHETIC_SONGS[0]!, 20_000],
  [SYNTHETIC_SONGS[1]!, 20_000],
  [SYNTHETIC_SONGS[2]!, 20_000],
];

/** Render `[fromMs, toMs)` of the mix at `sampleRate`. */
function renderMix(mix: Mix, fromMs: number, toMs: number, sampleRate: number): Float32Array {
  const out = new Float32Array(Math.round(((toMs - fromMs) / 1000) * sampleRate));
  let start = 0;
  for (const [song, length] of mix) {
    const a = Math.max(fromMs, start);
    const b = Math.min(toMs, start + length);
    if (b > a) out.set(renderSong(song, sampleRate, b - a, a - start), Math.round(((a - fromMs) / 1000) * sampleRate));
    start += length;
  }
  return out;
}

/**
 * The capture agent of a fake media page, served over a memory port: a player that plays
 * a synthetic mix in fast simulated time and streams PCM stamped with media time, as the
 * real agent's element capture does.
 */
export class FakeAgent implements AgentApi {
  served: ServedPort<AgentTopics> | null = null;
  positionMs = 0;
  rate = 1;
  muted = false;
  paused = true;
  restored = 0;
  prepared: PrepareOptions | null = null;
  capturing = false;
  /** Media ms ranges that play as an ad (no content audio, the clock stops). */
  ads: [number, number][] = [];
  private timer: ReturnType<typeof setTimeout> | null = null;
  private seq = 0;
  readonly durationMs: number;

  constructor(
    private readonly mix: Mix = THREE_SONG_MIX,
    /** Simulated media ms per step and the step's real delay. */
    private readonly stepMs = 200,
  ) {
    this.durationMs = mix.reduce((sum, [, l]) => sum + l, 0);
  }

  serve(port: PortLike) {
    this.served = serveRpc<AgentApi, AgentTopics>(port, this, AGENT_METHODS);
  }

  private emit<K extends keyof AgentTopics & string>(topic: K, payload: AgentTopics[K]) {
    this.served?.emit(topic, payload);
  }

  async hello(): Promise<AgentHello> {
    return { url: 'http://127.0.0.1/mix', title: 'Fake mix', adapter: 'youtube', page: true };
  }
  async facts(): Promise<PageFacts> {
    return { adapter: 'youtube', url: 'http://127.0.0.1/mix', title: 'Fake mix', ogTitle: null, ogType: null, nowPlaying: null, media: [], site: null };
  }
  async attach(): Promise<PlayerInfo> {
    return (await this.state())!;
  }
  async state(): Promise<PlayerInfo | null> {
    return {
      adapter: 'youtube',
      durationMs: this.durationMs,
      currentMs: this.positionMs,
      paused: this.paused,
      rate: this.rate,
      muted: this.muted,
      live: false,
      canSeek: true,
      title: 'Fake mix',
      mediaUrl: null,
      ad: false,
      protected: false,
      webAudioOnly: false,
    };
  }
  async prepare(options: PrepareOptions) {
    this.prepared = options;
    this.rate = options.rate;
    this.muted = options.mute;
  }
  async seek(ms: number) {
    this.positionMs = ms;
  }
  async play() {
    this.paused = false;
    this.tick();
  }
  async pause() {
    this.paused = true;
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
  }
  async startCapture() {
    this.capturing = true;
    return { sampleRate: 48_000, label: 'fake' };
  }
  async stopCapture() {
    this.capturing = false;
  }
  async watch() {}
  async restore() {
    this.restored++;
    await this.pause();
    this.rate = 1;
    this.muted = false;
    this.prepared = null;
  }

  private inAd(ms: number) {
    return this.ads.some(([a, b]) => ms >= a && ms < b);
  }

  private tick() {
    if (this.paused) return;
    this.timer = setTimeout(() => {
      if (this.paused) return;
      const from = this.positionMs;
      const to = Math.min(this.durationMs, from + this.stepMs * this.rate);
      const ad = this.inAd(from);
      if (ad) {
        const end = this.ads.find(([a, b]) => from >= a && from < b)![1];
        this.emit('event', { type: 'ad', showing: true } satisfies CaptureEvent);
        this.ads = this.ads.filter(([a]) => a !== end - (end - from) && !(from >= a && from < end));
        this.emit('event', { type: 'ad', showing: false } satisfies CaptureEvent);
      } else {
        if (this.capturing) {
          // The capture runs at 48 kHz; at `rate` the media's audio arrives sped up.
          const samples = renderMix(this.mix, from, to, 48_000 / this.rate);
          this.emit('pcm', { samples, sampleRate: 48_000, speed: this.rate, mediaMs: from, wallMs: Date.now(), continuous: this.seq > 0, seq: this.seq++ });
        }
        this.positionMs = to;
      }
      const sample: MediaSample = { wallMs: performance.timeOrigin + performance.now(), mediaMs: this.positionMs, rate: this.rate, advancing: this.positionMs < this.durationMs };
      this.emit('clock', sample);
      if (this.positionMs >= this.durationMs) {
        this.paused = true;
        return;
      }
      this.tick();
    }, 1);
  }
}
