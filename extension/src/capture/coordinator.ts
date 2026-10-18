import { MediaClock, type MediaSample } from '../audio/media-clock';
import { RelaySource } from '../audio/relay-source';
import { TAB_CAPTURE_LAG_MS } from '../audio/tab-capture';
import type { AudioSource, AudioSourceEvent, PcmChunk } from '../audio/types';
import { RpcClient, type PortLike } from '../messaging/bus';
import type { TabInfo } from '../messaging/protocol';
import type { CaptureStatus } from '../engine/client';
import type { PageFacts } from '../page/facts';
import type { AgentApi, AgentTopics, PlayerInfo } from './agent-protocol';
import { addCoverage, type CaptureEvent, type CaptureHost, type CaptureTarget, type Coverage } from './types';

/** How the host reaches tabs: directly (Firefox event page) or through the service worker (Chromium). */
export interface TabsBridge {
  /** A tab, or the active tab of the last focused window. */
  resolve(tabId?: number): Promise<TabInfo>;
  /** Inject the capture agent and the page script. */
  inject(tabId: number): Promise<void>;
  /** Chromium: a `tabCapture` stream id (needs an invocation on that tab). */
  streamId?(tabId: number): Promise<string>;
}

export interface CoordinatorOptions {
  target: 'chromium' | 'firefox';
  android: boolean;
  tabs: TabsBridge;
  /** Audio for a session (to the engine). */
  pushPcm(sessionId: string, chunk: PcmChunk): void;
  /** Something happened to a session's capture (to the engine). */
  captureEvent(sessionId: string, event: CaptureEvent): void;
  /** How the capture is going, for the pages (the `capture` topic: rate, ad, paused, tab closed). */
  onStatus?(sessionId: string, status: CaptureStatus): void;
  /** Chromium: open a tab capture of `streamId` in this context (the offscreen document). */
  openTabCapture?(streamId: string, clock: MediaClock | null, listenAlong: boolean): Promise<AudioSource>;
  /** How long to wait for an injected agent. Default 10 s. */
  agentTimeoutMs?: number;
}

/** Thrown when capture can't start; `reason` is a `CaptureReason`. */
export class CoordinatorError extends Error {
  constructor(
    readonly reason: 'CantCapture' | 'TabBusy' | 'NeedsInvocation' | 'CaptureBlocked' | 'NothingPlaying' | 'TabGone',
    message: string,
  ) {
    super(message);
    this.name = 'CoordinatorError';
  }
}

interface Consumer {
  pcm?(chunk: PcmChunk): void;
  audio?(event: AudioSourceEvent): void;
  clock?(sample: MediaSample): void;
  event?(event: CaptureEvent): void;
}

/** One injected agent, by tab. */
class AgentLink {
  readonly rpc: RpcClient<AgentApi, AgentTopics>;
  consumer: Consumer | null = null;
  readonly gone: Promise<void>;

  constructor(
    readonly tabId: number,
    port: PortLike,
  ) {
    this.rpc = new RpcClient<AgentApi, AgentTopics>(port, { timeoutMs: 20_000 });
    this.rpc.on('pcm', (chunk) => this.consumer?.pcm?.(chunk));
    this.rpc.on('audio', (event) => this.consumer?.audio?.(event));
    this.rpc.on('clock', (sample) => this.consumer?.clock?.(sample));
    this.rpc.on('event', (event) => this.consumer?.event?.(event));
    this.gone = new Promise((resolve) => this.rpc.onDisconnect(resolve));
  }

  get alive(): boolean {
    return !this.rpc.closed;
  }

  call<K extends keyof AgentApi & string>(method: K, ...args: Parameters<AgentApi[K]>): Promise<Awaited<ReturnType<AgentApi[K]>>> {
    return (this.rpc.call as (m: string, ...a: unknown[]) => Promise<never>)(method, ...args);
  }
}

interface Attached {
  target: CaptureTarget;
  rate: number;
  paused: boolean;
  ad: boolean;
  agent: AgentLink;
  via: 'tab' | 'element';
  clock: MediaClock;
  coverage: Coverage;
  source: AudioSource | null;
  /** Resolves the running listenThrough/windows step. */
  step: { toMs: number; done: () => void } | null;
  released: boolean;
  lastProgress: number;
}

const START_SLACK_MS = 250;

/**
 * The capture coordinator (extension plan §3.2, the host side of `CaptureHost`): picks
 * the tab, injects the agent, takes control of the player, captures (Chromium:
 * `tabCapture`, silent; Firefox and the fallback: the muted element), stamps media time,
 * keeps coverage, follows ads, stalls, user actions and navigation, and restores the
 * user's player state at the end. It also opens live "This tab" capture through the
 * element where `tabCapture` doesn't exist (Firefox).
 */
export class CaptureCoordinator implements CaptureHost {
  private readonly agents = new Map<number, AgentLink>();
  private readonly waiting = new Map<number, ((link: AgentLink) => void)[]>();
  private readonly sessions = new Map<string, Attached>();

  constructor(private readonly options: CoordinatorOptions) {}

  /** A `CAPTURE_PORT` connection from an injected agent (the host routes them here). */
  acceptAgent(port: PortLike & { sender?: { tab?: { id?: number } } }): void {
    const tabId = port.sender?.tab?.id;
    if (tabId === undefined) {
      port.disconnect();
      return;
    }
    const link = new AgentLink(tabId, port);
    this.agents.get(tabId)?.rpc.close();
    this.agents.set(tabId, link);
    for (const resolve of this.waiting.get(tabId) ?? []) resolve(link);
    this.waiting.delete(tabId);
    void link.gone.then(() => {
      if (this.agents.get(tabId) === link) this.agents.delete(tabId);
      for (const [session, attached] of this.sessions) if (attached.agent === link) void this.agentGone(session, attached);
    });
  }

  /** The agent of `tabId`, injecting it when it isn't there yet. */
  async agent(tabId: number): Promise<AgentLink> {
    const existing = this.agents.get(tabId);
    if (existing?.alive) return existing;
    const arrived = new Promise<AgentLink>((resolve, reject) => {
      const list = this.waiting.get(tabId) ?? [];
      list.push(resolve);
      this.waiting.set(tabId, list);
      setTimeout(() => reject(new CoordinatorError('CantCapture', 'The page did not answer.')), this.options.agentTimeoutMs ?? 10_000);
    });
    try {
      await this.options.tabs.inject(tabId);
    } catch (error) {
      this.waiting.delete(tabId);
      throw new CoordinatorError('CantCapture', error instanceof Error ? error.message : String(error));
    }
    return arrived;
  }

  /** Adapter facts of the page in `tabId` (for an analysis's hints). */
  async pageFacts(tabId: number): Promise<PageFacts | null> {
    const agent = await this.agent(tabId);
    return agent.call('facts');
  }

  private async usableTab(tabId?: number): Promise<TabInfo> {
    const tab = await this.options.tabs.resolve(tabId).catch((error: unknown) => {
      throw new CoordinatorError('TabGone', error instanceof Error ? error.message : String(error));
    });
    if (tab.capture !== 'ok') throw new CoordinatorError(tab.capture, tab.capture === 'TabBusy' ? 'Another capture is using this tab.' : "Can't listen to this page.");
    return tab;
  }

  /**
   * Live "This tab" through the element (Firefox; Chromium uses `tabCapture`). The user
   * keeps hearing the page; nothing about the player changes.
   */
  async openLiveElement(tabId?: number): Promise<{ source: AudioSource; tab: TabInfo }> {
    const tab = await this.usableTab(tabId);
    const agent = await this.agent(tab.id);
    const info = await agent.call('attach');
    if (!info || info.webAudioOnly) throw new CoordinatorError('NothingPlaying', 'Nothing is playing in this tab.');
    if (info.protected) throw new CoordinatorError('CaptureBlocked', 'This tab blocks audio capture.');
    const source = new RelaySource(
      'element',
      async () => {
        try {
          return await agent.call('startCapture', { clocked: false });
        } catch (error) {
          const reason = (error as { reason?: string }).reason;
          throw new CoordinatorError(reason === 'NothingPlaying' ? 'NothingPlaying' : 'CaptureBlocked', error instanceof Error ? error.message : String(error));
        }
      },
      () => void agent.call('stopCapture').catch(() => undefined),
    );
    agent.consumer = { pcm: (chunk) => source.feed(chunk), audio: (event) => source.signal(event) };
    void agent.gone.then(() => source.signal({ type: 'ended', reason: 'ended', message: 'The page went away.' }));
    await source.start();
    return { source, tab };
  }

  // MARK: CaptureHost

  async attach(session: string, url: string, tabId: number | null): Promise<CaptureTarget> {
    const tab = await this.usableTab(tabId ?? undefined);
    const agent = await this.agent(tab.id);
    const info: PlayerInfo | null = await agent.call('attach');
    if (!info) throw new CoordinatorError('NothingPlaying', 'There is no audio or video on this page.');
    const facts = await agent.call('facts').catch(() => null);
    // Chromium captures the tab when it may; the element otherwise (and always on Firefox).
    const tabCapture = this.options.target === 'chromium' && Boolean(this.options.openTabCapture && this.options.tabs.streamId);
    if (info.protected && !tabCapture) throw new CoordinatorError('CaptureBlocked', 'This media is protected and cannot be captured here.');
    const maxRate = info.webAudioOnly ? 1 : this.options.android ? 2 : 4;
    const target: CaptureTarget = {
      session,
      tabId: tab.id,
      url: tab.url ?? url,
      title: info.title ?? tab.title,
      durationMs: info.durationMs,
      maxRate,
      canSeek: info.canSeek,
      live: info.live,
      adapter: info.adapter,
      via: tabCapture ? 'tab' : 'element',
      facts,
    };
    const clock =
      target.via === 'tab' ? new MediaClock({ lagMs: TAB_CAPTURE_LAG_MS }) : new MediaClock({ lagMs: 0, anchorOnly: true });
    const attached: Attached = { target, rate: 1, paused: false, ad: false, agent, via: target.via, clock, coverage: [], source: null, step: null, released: false, lastProgress: 0 };
    this.sessions.set(session, attached);
    agent.consumer = {
      pcm: (chunk) => this.pcm(session, attached, chunk),
      audio: (event) => this.audio(session, attached, event),
      clock: (sample) => this.clock(session, attached, sample),
      event: (event) => this.event(session, attached, event),
    };
    return target;
  }

  async listenThrough(session: string, fromMs: number, toMs: number, rate: number): Promise<Coverage> {
    const a = this.attached(session);
    await this.capture(a, fromMs, toMs, rate);
    return a.coverage;
  }

  async windows(session: string, windows: [number, number][], rate: number): Promise<Coverage> {
    const a = this.attached(session);
    for (const [from, to] of windows) {
      if (a.released) break;
      await this.capture(a, from, to, rate);
    }
    return a.coverage;
  }

  async release(session: string): Promise<void> {
    const a = this.sessions.get(session);
    if (!a) return;
    this.sessions.delete(session);
    a.released = true;
    a.step?.done();
    a.source?.stop();
    a.source = null;
    if (a.agent.consumer && a.agent.alive) a.agent.consumer = null;
    if (a.agent.alive) {
      await a.agent.call('watch', false).catch(() => undefined);
      await a.agent.call('restore').catch(() => undefined);
    }
  }

  /** Sessions attached right now (diagnostics). */
  attachedSessions(): { session: string; tabId: number; via: string; coverage: Coverage }[] {
    return [...this.sessions.entries()].map(([session, a]) => ({ session, tabId: a.target.tabId, via: a.via, coverage: a.coverage }));
  }

  // MARK: capture steps

  private attached(session: string): Attached {
    const a = this.sessions.get(session);
    if (!a || a.released) throw new CoordinatorError('TabGone', 'The capture was released.');
    return a;
  }

  /** Play `[from, to)` at `rate` and capture it. */
  private async capture(a: Attached, fromMs: number, toMs: number, rate: number): Promise<void> {
    const r = Math.max(0.25, Math.min(rate, a.target.maxRate));
    a.rate = r;
    await a.agent.call('prepare', { rate: r, mute: a.via === 'element', lowQuality: true });
    await a.agent.call('watch', true);
    await a.agent.call('pause');
    if (a.target.canSeek) {
      a.clock.restart(performance.timeOrigin + performance.now());
      await a.agent.call('seek', fromMs);
    }
    await this.ensureSource(a, r);
    if (a.released) return;
    const reached = new Promise<void>((resolve) => {
      a.step = { toMs, done: resolve };
    });
    await a.agent.call('play');
    this.status(a);
    await Promise.race([reached, a.agent.gone]);
    a.step = null;
    if (a.agent.alive && !a.released) await a.agent.call('pause').catch(() => undefined);
  }

  private async ensureSource(a: Attached, rate: number): Promise<void> {
    if (a.source) return;
    if (a.via === 'tab') {
      try {
        const streamId = await this.options.tabs.streamId!(a.target.tabId);
        a.source = await this.options.openTabCapture!(streamId, a.clock, false);
      } catch (error) {
        // Not invoked on this tab (a link the user pasted) or busy: capture the element instead.
        const reason = (error as { reason?: string }).reason;
        if (reason !== 'NeedsInvocation') throw error;
        a.via = 'element';
        a.clock = new MediaClock({ lagMs: 0, anchorOnly: true });
        await a.agent.call('prepare', { rate, mute: true, lowQuality: true });
        return this.ensureSource(a, rate);
      }
      a.source.onPcm((chunk) => this.pcm(a.target.session, a, chunk));
      a.source.onEvent((event) => this.audio(a.target.session, a, event));
      return;
    }
    const relay = new RelaySource(
      'element',
      () => a.agent.call('startCapture', { clocked: true }),
      () => void a.agent.call('stopCapture').catch(() => undefined),
    );
    a.source = relay;
    // Element PCM arrives already stamped by the agent's own clock.
    await relay.start();
  }

  private pcm(session: string, a: Attached, chunk: PcmChunk) {
    if (a.released || chunk.mediaMs === null) return;
    const end = chunk.mediaMs + (chunk.samples.length / chunk.sampleRate) * 1000 * chunk.speed;
    a.coverage = addCoverage(a.coverage, [chunk.mediaMs, end]);
    this.options.pushPcm(session, chunk);
  }

  private audio(session: string, a: Attached, event: AudioSourceEvent) {
    if (event.type === 'silent' && a.via === 'element') this.options.captureEvent(session, { type: 'silent', mediaUrl: null });
    if (event.type === 'ended' && event.reason !== 'stopped') {
      a.source = null;
      a.step?.done();
    }
  }

  private clock(session: string, a: Attached, sample: MediaSample) {
    a.clock.observe(sample);
    const now = sample.wallMs;
    if (now - a.lastProgress > 1000) {
      a.lastProgress = now;
      this.options.captureEvent(session, { type: 'progress', mediaMs: sample.mediaMs, rate: sample.rate });
    }
    const step = a.step;
    // Wait for the audio that trails the clock before ending the step.
    if (step && sample.advancing && sample.mediaMs >= step.toMs + START_SLACK_MS * sample.rate) step.done();
    if (step && a.target.durationMs && sample.mediaMs >= a.target.durationMs - 30 && !sample.advancing) step.done();
  }

  private event(session: string, a: Attached, event: CaptureEvent) {
    // Ads, stalls, user seeks: blocks wait for a fresh clock sample after the change.
    if (event.type === 'ad' || event.type === 'stall' || event.type === 'user') a.clock.restart(performance.timeOrigin + performance.now());
    if (event.type === 'navigated' || event.type === 'closed') a.step?.done();
    if (event.type === 'ad') a.ad = event.showing;
    if (event.type === 'user' && event.action !== 'seek') a.paused = event.action === 'pause';
    if (event.type === 'closed' || event.type === 'navigated') this.options.onStatus?.(session, { state: 'tabClosed', rate: null, muted: false });
    else if (event.type === 'ad' || event.type === 'user') this.status(a);
    this.options.captureEvent(session, event);
  }

  /** The page's view of a listen-through: silent for the user (tab not routed back, or the element muted). */
  private status(a: Attached) {
    if (a.released) return;
    const state: CaptureStatus['state'] = a.ad ? 'ad' : a.paused ? 'paused' : 'capturing';
    this.options.onStatus?.(a.target.session, { state, rate: a.rate, muted: true });
  }

  private async agentGone(session: string, a: Attached) {
    if (a.released) return;
    const tab = await this.options.tabs.resolve(a.target.tabId).catch(() => null);
    this.options.captureEvent(session, tab ? { type: 'navigated', url: tab.url } : { type: 'closed' });
    this.options.onStatus?.(session, { state: 'tabClosed', rate: null, muted: false });
    a.step?.done();
    a.source?.stop();
    a.source = null;
  }
}
