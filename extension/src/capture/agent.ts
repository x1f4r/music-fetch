import { elementClock, ElementCaptureError, ElementCaptureSource } from '../audio/element-capture';
import type { MediaClock, MediaSample } from '../audio/media-clock';
import { wallNow } from '../audio/types';
import { serveRpc, type PortLike, type ServedPort } from '../messaging/bus';
import { CAPTURE_PORT } from '../messaging/protocol';
import { siteAdapter } from '../page/adapters';
import { PageBridge } from '../page/bridge';
import { adapterFor, readPageFacts, type PageFacts } from '../page/facts';
import { PLAYER_ATTR, type MarkedPlayer } from '../page/main-world';
import { finite, pickElement } from '../page/site';
import { AGENT_METHODS, type AgentApi, type AgentHello, type AgentTopics, type PlayerInfo, type PrepareOptions } from './agent-protocol';
import type { CaptureEvent } from './types';

interface Saved {
  timeMs: number;
  rate: number;
  preservesPitch: boolean;
  volume: number;
  muted: boolean;
  paused: boolean;
  quality: string | null;
  qualityChanged: boolean;
}

export interface AgentEnv {
  win: Window;
  doc: Document;
  connect(name: string): PortLike;
}

/** How long our own player commands may take to show up as events. */
const OWN_ACTION_MS = 1500;

const GLOBAL = '__mfCaptureAgent';

/**
 * The capture agent (isolated world; see `agent-protocol.ts`). One per tab: injecting it
 * again reconnects the same agent to the (new) host.
 */
export function startAgent(env: AgentEnv): { stop(): void } {
  const holder = env.win as unknown as Record<string, { reconnect(): void; stop(): void } | undefined>;
  const existing = holder[GLOBAL];
  if (existing) {
    existing.reconnect();
    return existing;
  }
  const agent = new CaptureAgent(env);
  holder[GLOBAL] = agent;
  return agent;
}

class CaptureAgent implements AgentApi {
  private readonly nonce = crypto.randomUUID();
  private readonly bridge: PageBridge;
  private served: ServedPort<AgentTopics> | null = null;
  private element: HTMLMediaElement | null = null;
  private saved: Saved | null = null;
  private prepared: PrepareOptions | null = null;
  private capture: ElementCaptureSource | null = null;
  private clock: MediaClock | null = null;
  private watchTimer: ReturnType<typeof setInterval> | null = null;
  private watching = false;
  private stalled = false;
  private ad = false;
  private ownUntil: Record<'play' | 'pause' | 'seek' | 'rate', number> = { play: 0, pause: 0, seek: 0, rate: 0 };
  private href: string;
  private pageScript = false;
  private readonly detach: (() => void)[] = [];

  constructor(private readonly env: AgentEnv) {
    // The page script (injected right after) reads the nonce and removes it.
    env.doc.documentElement.dataset.mfNonce = this.nonce;
    this.bridge = new PageBridge(env.win, this.nonce);
    this.href = env.win.location.href;
    this.bridge.onEvent((event) => {
      if (event.type === 'ad') {
        this.ad = event.showing;
        this.event({ type: 'ad', showing: event.showing });
        if (!event.showing) this.reapply();
      } else if (event.type === 'navigated') this.navigated(event.url);
    });
    const onHide = () => this.event({ type: 'closed' });
    env.win.addEventListener('pagehide', onHide);
    this.detach.push(() => env.win.removeEventListener('pagehide', onHide));
    this.reconnect();
  }

  reconnect(): void {
    this.served?.close();
    const port = this.env.connect(CAPTURE_PORT);
    this.served = serveRpc<AgentApi, AgentTopics>(port, this, AGENT_METHODS, () => {
      // The host went away: put the player back as the user had it.
      void this.restore();
    });
  }

  stop(): void {
    void this.restore();
    this.served?.close();
    this.bridge.close();
    for (const d of this.detach) d();
  }

  private emit<K extends keyof AgentTopics & string>(topic: K, payload: AgentTopics[K]) {
    this.served?.emit(topic, payload);
  }

  private event(event: CaptureEvent) {
    this.emit('event', event);
  }

  private own(action: keyof CaptureAgent['ownUntil']) {
    this.ownUntil[action] = performance.now() + OWN_ACTION_MS;
  }

  private isOwn(action: keyof CaptureAgent['ownUntil']): boolean {
    return performance.now() < this.ownUntil[action];
  }

  // MARK: AgentApi

  async hello(): Promise<AgentHello> {
    this.pageScript = await this.bridge
      .call('hello', undefined, 800)
      .then(() => true)
      .catch(() => false);
    return { url: this.env.win.location.href, title: this.env.doc.title, adapter: adapterFor(this.env.win.location.hostname), page: this.pageScript };
  }

  async facts(): Promise<PageFacts> {
    try {
      return await this.bridge.call<PageFacts>('facts');
    } catch {
      // No page script: what the DOM says, plus the adapter's DOM-only facts.
      const facts = readPageFacts(this.env.doc, undefined);
      return { ...facts, site: null };
    }
  }

  async attach(): Promise<PlayerInfo | null> {
    let marked: MarkedPlayer | null = null;
    try {
      marked = await this.bridge.call<MarkedPlayer>('mark', undefined, 2500);
    } catch {
      marked = null;
    }
    const element =
      (marked?.found ? this.env.doc.querySelector<HTMLMediaElement>(`[${PLAYER_ATTR}="${this.nonce}"]`) : null) ??
      siteAdapter(this.env.win.location.hostname).element(this.env.win, this.env.doc, []) ??
      pickElement(this.env.doc.querySelectorAll('audio, video'));
    if (!element) return null;
    if (element !== this.element) this.bind(element);
    return this.info(marked);
  }

  async state(): Promise<PlayerInfo | null> {
    return this.element ? this.info(null) : null;
  }

  private info(marked: MarkedPlayer | null): PlayerInfo {
    const el = this.element!;
    const src = el.currentSrc || el.src || '';
    const durationMs = marked?.durationMs ?? (finite(el.duration) ?? 0) * 1000;
    return {
      adapter: adapterFor(this.env.win.location.hostname),
      durationMs: durationMs || null,
      currentMs: el.currentTime * 1000,
      paused: el.paused,
      rate: el.playbackRate,
      muted: el.muted,
      live: (marked?.live ?? false) || el.duration === Infinity,
      canSeek: el.seekable.length > 0 || Boolean(marked?.controls),
      title: marked?.title ?? null,
      mediaUrl: /^https?:/.test(src) ? src : null,
      ad: this.ad,
      protected: Boolean(el.mediaKeys) || Boolean(marked?.protected),
      webAudioOnly: false,
    };
  }

  async prepare(options: PrepareOptions): Promise<void> {
    const el = this.need();
    if (!this.saved) {
      this.saved = {
        timeMs: el.currentTime * 1000,
        rate: el.playbackRate,
        preservesPitch: el.preservesPitch,
        volume: el.volume,
        muted: el.muted,
        paused: el.paused,
        quality: null,
        qualityChanged: false,
      };
    }
    this.prepared = options;
    this.reapply();
    if (options.lowQuality && !this.saved.qualityChanged) {
      this.saved.qualityChanged = true;
      this.saved.quality = await this.bridge.call<string | null>('quality', { on: true }).catch(() => null);
    }
  }

  /** Apply the scan's rate and mute again (sites reset them, e.g. after an ad). */
  private reapply() {
    const el = this.element;
    const p = this.prepared;
    if (!el || !p) return;
    this.own('rate');
    el.preservesPitch = false;
    if (Math.abs(el.playbackRate - p.rate) > 0.001) el.playbackRate = p.rate;
    // Element capture keeps audio when muted (S2); never set the volume to 0 (S1).
    if (p.mute && !el.muted) el.muted = true;
  }

  async seek(ms: number): Promise<void> {
    const el = this.need();
    this.own('seek');
    const viaSite = this.pageScript && (await this.bridge.call<boolean>('seek', ms).catch(() => false));
    if (!viaSite) el.currentTime = ms / 1000;
    await waitFor(el, 'seeked', 5000, () => !el.seeking && Math.abs(el.currentTime * 1000 - ms) < 1500);
    this.clock?.restart(wallNow());
  }

  async play(): Promise<void> {
    const el = this.need();
    this.own('play');
    const viaSite = this.pageScript && (await this.bridge.call<boolean>('play').catch(() => false));
    if (!viaSite) await el.play();
    this.reapply();
  }

  async pause(): Promise<void> {
    const el = this.need();
    if (el.paused) return;
    this.own('pause');
    const viaSite = this.pageScript && (await this.bridge.call<boolean>('pause').catch(() => false));
    if (!viaSite) el.pause();
  }

  async startCapture(options: { clocked: boolean }): Promise<{ sampleRate: number; label: string }> {
    const el = this.need();
    await this.stopCapture();
    this.clock = options.clocked ? elementClock() : null;
    const source = new ElementCaptureSource(el, this.clock);
    source.onPcm((chunk) => this.emit('pcm', chunk));
    source.onEvent((event) => this.emit('audio', event));
    try {
      await source.start();
    } catch (error) {
      const reason = error instanceof ElementCaptureError ? error.reason : 'CaptureBlocked';
      throw Object.assign(new Error(error instanceof Error ? error.message : String(error)), { code: 'Platform', reason });
    }
    this.capture = source;
    if (options.clocked) this.sample();
    const stats = source.stats();
    return { sampleRate: stats.sampleRate, label: el.currentSrc || el.tagName.toLowerCase() };
  }

  async stopCapture(): Promise<void> {
    this.capture?.stop();
    this.capture = null;
    this.clock = null;
  }

  async watch(on: boolean): Promise<void> {
    this.watching = on;
    if (this.watchTimer) clearInterval(this.watchTimer);
    this.watchTimer = on ? setInterval(() => this.sample(), 50) : null;
    if (on) this.sample();
  }

  async restore(): Promise<void> {
    await this.stopCapture();
    await this.watch(false);
    const el = this.element;
    const saved = this.saved;
    this.saved = null;
    this.prepared = null;
    if (!el || !saved) return;
    this.own('pause');
    this.own('seek');
    this.own('rate');
    this.own('play');
    if (!el.paused) el.pause();
    el.playbackRate = saved.rate;
    el.preservesPitch = saved.preservesPitch;
    el.muted = saved.muted;
    el.volume = saved.volume;
    const viaSite = this.pageScript && (await this.bridge.call<boolean>('seek', saved.timeMs).catch(() => false));
    if (!viaSite) el.currentTime = saved.timeMs / 1000;
    if (saved.qualityChanged) await this.bridge.call('quality', { on: false, restore: saved.quality }).catch(() => undefined);
    if (!saved.paused) await el.play().catch(() => undefined);
  }

  // MARK: the element

  private need(): HTMLMediaElement {
    if (!this.element) throw Object.assign(new Error('Nothing is playing in this tab.'), { code: 'Platform', reason: 'NothingPlaying' });
    return this.element;
  }

  private bind(el: HTMLMediaElement) {
    this.element = el;
    const on = (type: string, handler: () => void) => {
      el.addEventListener(type, handler);
      this.detach.push(() => el.removeEventListener(type, handler));
    };
    on('waiting', () => {
      if (this.stalled) return;
      this.stalled = true;
      this.event({ type: 'stall', stalled: true });
    });
    on('playing', () => {
      if (!this.stalled) return;
      this.stalled = false;
      this.clock?.restart(wallNow());
      this.event({ type: 'stall', stalled: false });
    });
    on('pause', () => {
      if (!this.isOwn('pause') && this.prepared) this.event({ type: 'user', action: 'pause' });
    });
    on('play', () => {
      if (!this.isOwn('play') && this.prepared) this.event({ type: 'user', action: 'play' });
    });
    on('seeking', () => {
      if (this.isOwn('seek')) return;
      this.clock?.restart(wallNow());
      if (this.prepared) this.event({ type: 'user', action: 'seek' });
    });
    on('ratechange', () => {
      if (!this.isOwn('rate') && this.prepared) this.reapply();
      this.clock?.restart(wallNow());
    });
    on('timeupdate', () => {
      if (this.watching) this.sample();
    });
    on('ended', () => this.sample());
  }

  private sample() {
    const el = this.element;
    if (!el) return;
    if (this.env.win.location.href !== this.href) this.navigated(this.env.win.location.href);
    const sample: MediaSample = {
      wallMs: wallNow(),
      mediaMs: el.currentTime * 1000,
      rate: el.playbackRate,
      advancing: !el.paused && !el.seeking && !el.ended && el.readyState >= 3 && !this.stalled && !this.ad,
    };
    this.clock?.observe(sample);
    if (this.watching) this.emit('clock', sample);
  }

  private navigated(url: string) {
    if (url === this.href) return;
    this.href = url;
    this.event({ type: 'navigated', url });
  }
}

function waitFor(el: HTMLMediaElement, type: string, timeoutMs: number, done: () => boolean): Promise<void> {
  if (done()) return Promise.resolve();
  return new Promise((resolve) => {
    const finish = () => {
      clearTimeout(timer);
      el.removeEventListener(type, check);
      resolve();
    };
    const check = () => {
      if (done()) finish();
    };
    const timer = setTimeout(finish, timeoutMs);
    el.addEventListener(type, check);
  });
}
