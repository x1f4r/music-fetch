import { PcmFramer } from './framer';
import type { MediaClock } from './media-clock';
import { SilenceWatch } from './silence';
import { Tap, type TapOptions } from './tap';
import { Emitter, type AudioEndReason, type AudioSource, type AudioSourceEvent, type AudioSourceKind, type PcmChunk } from './types';

export interface SourceStats {
  kind: AudioSourceKind;
  sampleRate: number;
  frames: number;
  /** Blocks the media clock dropped (paused, stalled, ad, waiting for a sample). */
  dropped: number;
  tapMode: string | null;
  silent: boolean;
}

/**
 * The part every source shares: a MediaStream through the tap, framing, silence watch
 * and the end of the source (the track ended, `stop()`, an error).
 */
export abstract class StreamSource implements AudioSource {
  abstract readonly kind: AudioSourceKind;
  protected readonly pcm = new Emitter<PcmChunk>();
  protected readonly events = new Emitter<AudioSourceEvent>();
  protected tap: Tap | null = null;
  protected stream: MediaStream | null = null;
  protected ended = false;
  /** Stop the stream's tracks at the end (getUserMedia streams; not a page's element stream we don't own). */
  private ownsTracks = true;
  private readonly framer: PcmFramer;
  private readonly silence = new SilenceWatch();

  constructor(
    protected readonly tapOptions: TapOptions,
    clock: MediaClock | null = null,
    speed?: () => number,
  ) {
    this.framer = new PcmFramer((chunk) => this.pcm.emit(chunk), { clock, speed });
  }

  abstract start(): Promise<void>;

  onPcm(callback: (chunk: PcmChunk) => void): () => void {
    return this.pcm.on(callback);
  }

  onEvent(callback: (event: AudioSourceEvent) => void): () => void {
    return this.events.on(callback);
  }

  stats(): SourceStats {
    return {
      kind: this.kind,
      sampleRate: this.tap?.sampleRate ?? 0,
      frames: this.framer.frames,
      dropped: this.framer.dropped,
      tapMode: this.tap?.mode ?? null,
      silent: this.silence.isSilent,
    };
  }

  stop(): void {
    this.end('stopped');
  }

  /** Run `stream` through the tap; the source is live once this resolves. */
  protected async attach(stream: MediaStream, label: string, stopTracksOnEnd = true): Promise<void> {
    this.stream = stream;
    for (const track of stream.getAudioTracks()) {
      track.addEventListener(
        'ended',
        () => {
          if (this.stream === stream) this.trackEnded();
        },
        { once: true },
      );
    }
    try {
      this.tap = await Tap.open(stream, this.tapOptions, (samples, sampleRate, wall) => this.block(samples, sampleRate, wall));
    } catch (error) {
      if (stopTracksOnEnd) stream.getTracks().forEach((t) => t.stop());
      this.stream = null;
      throw error;
    }
    this.ownsTracks = stopTracksOnEnd;
    if (this.ended) {
      // stop() raced the start
      void this.tap.close();
      return;
    }
    this.events.emit({ type: 'started', sampleRate: this.tap.sampleRate, label });
  }

  /** The captured track ended: the source ends (element capture may pick up the next track). */
  protected trackEnded(): void {
    this.end('ended', 'The audio track ended.');
  }

  /** Swap in another stream (the microphone changed) without ending the source. */
  protected async replace(stream: MediaStream, label: string, ownsTracks = true): Promise<void> {
    const old = { tap: this.tap, stream: this.stream };
    this.tap = null;
    this.stream = null;
    await old.tap?.close();
    if (this.ownsTracks) old.stream?.getTracks().forEach((t) => t.stop());
    await this.attach(stream, label, ownsTracks);
  }

  private block(samples: Float32Array, sampleRate: number, wall: number) {
    if (this.ended) return;
    const change = this.silence.push(samples, sampleRate);
    if (change === 'silent') this.events.emit({ type: 'silent', forMs: this.silence.silentForMs });
    else if (change === 'sound') this.events.emit({ type: 'sound' });
    this.framer.push(samples, sampleRate, wall);
  }

  protected end(reason: AudioEndReason, message?: string): void {
    if (this.ended) return;
    this.ended = true;
    void this.tap?.close();
    if (this.ownsTracks) this.stream?.getTracks().forEach((t) => t.stop());
    this.events.emit({ type: 'ended', reason, ...(message ? { message } : {}) });
    this.pcm.clear();
  }
}
