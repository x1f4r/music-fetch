import type { AudioSource, PcmChunk } from '../audio/types';
import type { CaptureHost } from '../capture/types';
import { RpcError, serveRpc, type EventLike, type PortLike, type ServedPort } from '../messaging/bus';
import {
  ENGINE_PORT,
  HOST_PORT,
  type CaptureReason,
  type EngineTopics,
  type HostActivity,
  type HostSettings,
  type TabInfo,
} from '../messaging/protocol';
import { ENGINE_METHODS, EngineError, type ActiveSession, type EngineApi, type EngineInfo } from './client';
import { isTerminal } from './events';
import type { BackendConfig, EngineBackend } from './host-backend';
import type { SessionRegistry, RegistryEntry } from './host-registry';
import { WorkerEngine, type WorkerLike, type WorkerState } from './host-worker-client';
import type { AnalyzeMode, CaptureSource, ListenMode, ListenOptions, PageContext, SessionEvent, SessionKind } from './types';

/** A port as the host receives it; `sender` is set by the browser. */
export interface IncomingPort extends PortLike {
  sender?: { id?: string; url?: string; tab?: { id?: number } };
}

export interface HostRuntime {
  readonly id: string;
  getURL(path: string): string;
  onConnect: EventLike<(port: IncomingPort) => void>;
}

/** A UI page connected to the host. */
export interface UiConnection {
  readonly id: number;
  readonly url: string;
  emit<K extends keyof EngineTopics & string>(topic: K, payload: EngineTopics[K]): void;
}

/** A source the platform opened and started. */
export interface OpenedSource {
  source: AudioSource;
  via: 'tab' | 'element' | 'mic';
  tab: TabInfo | null;
}

/**
 * What the host needs from the context it runs in: the offscreen document (Chromium,
 * `host-chromium.ts`) or the event page (Firefox, `host-firefox.ts`).
 */
export interface HostPlatform {
  readonly target: 'chromium' | 'firefox';
  readonly android: boolean;
  /** "This tab" for live listening; the user keeps hearing it. */
  openTab(caller: UiConnection | null): Promise<OpenedSource>;
  /** The microphone: here (Chromium) or in the calling page (Firefox). */
  openMic(caller: UiConnection | null, priority: string[]): Promise<OpenedSource>;
  settings(): Promise<HostSettings>;
  /** The host's sessions changed (badge, notification). */
  activity(activity: HostActivity): void;
  /** Nothing is left to do: no sessions, no pages, no Worker. */
  idle(): void;
  /** The mock directive set in an extension page (`localStorage['mf.mock.next']`), consumed. */
  takeDirective(): string | null;
  spawnWorker(): WorkerLike;
  /** Keep the host context alive (Firefox suspends an idle event page after 30 s). */
  keepalive?(needed: boolean): void;
  /** Facts the page adds to an analysis (site adapters), for `analyze` with a tab. */
  pageFacts?(tabId: number): Promise<Record<string, unknown> | null>;
}

/** Thrown by platforms when capture can't start; reaches the UI as `EngineError(Platform, …, reason)`. */
export class CaptureError extends Error {
  constructor(
    readonly reason: CaptureReason,
    message: string,
  ) {
    super(message);
    this.name = 'CaptureError';
  }
}

function toEngineError(error: unknown): EngineError {
  if (error instanceof EngineError) return error;
  if (error instanceof RpcError) return new EngineError(error.code as EngineError['code'], error.message, error.reason as CaptureReason | undefined);
  const reason = (error as { reason?: unknown } | null)?.reason;
  if (typeof reason === 'string') return new EngineError('Platform', error instanceof Error ? error.message : String(error), reason as CaptureReason);
  if (error instanceof DOMException && error.name === 'NotAllowedError') return new EngineError('Platform', error.message, 'MicNotGranted');
  return new EngineError('Internal', error instanceof Error ? error.message : String(error));
}

const MINUTE = 60_000;

/** The host's own controls and state, for extension pages (`HOST_PORT`): diagnostics and tests. */
export interface HostApi {
  state(): Promise<HostState>;
  /** Recycle the engine Worker now (like the idle timeout would). */
  recycle(): Promise<HostState>;
  /** Frames and sample rate of the live sources. */
  sources(): Promise<{ sessionId: string; kind: string; via: string; frames: number }[]>;
}

export interface HostState {
  worker: WorkerState;
  workerStarts: number;
  sessions: string[];
  live: string[];
  surfaces: number;
  interrupted: RegistryEntry[];
  lowPower: boolean;
}

const HOST_METHODS = ['state', 'recycle', 'sources'] as const satisfies readonly (keyof HostApi)[];

interface LiveCapture {
  source: AudioSource;
  via: OpenedSource['via'];
  frames: number;
  stop: () => void;
}

export interface EngineHostOptions {
  platform: HostPlatform;
  runtime: HostRuntime;
  /** Served to the engine for analyses of media pages. */
  capture: CaptureHost;
  registry: SessionRegistry;
  /** Override the idle recycle (tests). */
  idleMs?: number;
  /** `navigator.hardwareConcurrency`. */
  cpus?: number;
  /** `navigator.deviceMemory` in GiB (Chromium only). */
  memoryGiB?: number | null;
}

/**
 * The engine host (extension plan §3.1): serves the engine to every UI page, owns live
 * capture, keeps sessions running when pages close, relays events to all pages and to
 * the background, and recycles the engine Worker when idle. One per extension: in the
 * offscreen document on Chromium, in the event page on Firefox.
 */
export class EngineHost {
  readonly worker: WorkerEngine;
  private readonly ui = new Map<number, { conn: UiConnection; served: ServedPort<EngineTopics> }>();
  private readonly live = new Map<string, LiveCapture>();
  private readonly kinds = new Map<string, ActiveSession>();
  private nextConnection = 1;
  private lowPower = false;
  private interrupted: RegistryEntry[] = [];
  private closed = false;
  private readonly ready: Promise<void>;

  constructor(private readonly options: EngineHostOptions) {
    const { platform } = options;
    this.worker = new WorkerEngine({
      spawn: () => platform.spawnWorker(),
      config: () => this.config(),
      capture: options.capture,
      idleMs: () => options.idleMs ?? (this.lowPower ? 2 * MINUTE : 10 * MINUTE),
      onSessionEvent: (id, event) => this.sessionEvent(id, event),
      onLibraryChanged: () => this.broadcast('library:changed', null),
      onState: (state) => {
        if (state === 'down') this.maybeIdle();
      },
    });
    options.runtime.onConnect.addListener(this.onConnect);
    this.ready = (async () => {
      this.interrupted = await options.registry.takeInterrupted().catch(() => []);
      const settings = await platform.settings().catch(() => null);
      this.lowPower = resolveLowPower(settings?.lowPower ?? 'auto', platform.android, options.cpus ?? 8, options.memoryGiB ?? null);
    })();
  }

  private config(): BackendConfig {
    return { target: this.options.platform.target, android: this.options.platform.android, lowPower: this.lowPower, cpus: this.options.cpus ?? 8 };
  }

  get surfaces(): number {
    return this.ui.size;
  }

  close(): void {
    this.closed = true;
    this.options.runtime.onConnect.removeListener(this.onConnect);
    for (const { served } of this.ui.values()) served.close();
    for (const id of [...this.live.keys()]) this.stopCapture(id);
  }

  // MARK: ports

  private readonly onConnect = (port: IncomingPort) => {
    if (port.name !== ENGINE_PORT && port.name !== HOST_PORT) return;
    const sender = port.sender;
    const origin = this.options.runtime.getURL('/');
    // Only extension pages: content scripts talk to the capture coordinator instead.
    if (sender && (sender.id !== this.options.runtime.id || !sender.url?.startsWith(origin))) {
      port.disconnect();
      return;
    }
    if (port.name === HOST_PORT) {
      serveRpc<HostApi>(port, this.hostApi(), HOST_METHODS);
      return;
    }
    const id = this.nextConnection++;
    let served: ServedPort<EngineTopics> | null = null;
    const conn: UiConnection = {
      id,
      url: sender?.url ?? '',
      emit: (topic, payload) => served?.emit(topic, payload),
    };
    served = serveRpc<EngineApi, EngineTopics>(port, this.engineApi(conn), ENGINE_METHODS, () => {
      this.ui.delete(id);
      this.reportActivity(null);
      this.maybeIdle();
    });
    this.ui.set(id, { conn, served });
    this.reportActivity(null);
  };

  private broadcast<K extends keyof EngineTopics & string>(topic: K, payload: EngineTopics[K]) {
    for (const { served } of this.ui.values()) served.emit(topic, payload);
  }

  private hostApi(): HostApi {
    return {
      state: () => this.state(),
      recycle: async () => {
        await this.worker.recycle('request');
        return this.state();
      },
      sources: async () => [...this.live.entries()].map(([sessionId, l]) => ({ sessionId, kind: l.source.kind, via: l.via, frames: l.frames })),
    };
  }

  async state(): Promise<HostState> {
    await this.ready;
    return {
      worker: this.worker.state,
      workerStarts: this.worker.starts,
      sessions: this.worker.sessions,
      live: [...this.live.keys()],
      surfaces: this.ui.size,
      interrupted: this.interrupted,
      lowPower: this.lowPower,
    };
  }

  // MARK: the engine API, per page

  private engineApi(conn: UiConnection): EngineApi {
    const forward = Object.fromEntries(
      ENGINE_METHODS.map((method) => [method, (...args: unknown[]) => this.call(method as keyof EngineBackend & string, args)]),
    ) as unknown as EngineApi;
    return {
      ...forward,
      hello: async (): Promise<EngineInfo> => {
        await this.ready;
        return this.worker.call('hello');
      },
      startListening: (source, mode, options) => this.startListening(conn, source, mode, options),
      finishSession: (id) => this.finish(id),
      cancelSession: (id) => this.cancel(id),
      analyze: (input, mode, page) => this.analyze(input, mode, page),
    };
  }

  private async call(method: keyof EngineBackend & string, args: unknown[]): Promise<unknown> {
    await this.ready;
    try {
      return await (this.worker.call as (m: string, ...a: unknown[]) => Promise<unknown>)(method, ...args);
    } catch (error) {
      throw toEngineError(error);
    }
  }

  // MARK: live sessions

  async startListening(conn: UiConnection | null, source: CaptureSource, mode: ListenMode, options: ListenOptions): Promise<string> {
    await this.ready;
    const { platform } = this.options;
    const directive = platform.takeDirective();
    if (directive) {
      // A forced mock outcome: scripted, no audio needed.
      return this.call('startLive', [source, mode, options, { kind: 'none', sampleRate: 0, label: '', directive }]) as Promise<string>;
    }
    if (source === 'SystemAudio') throw new EngineError('Platform', 'Computer audio is not available in this browser yet.', 'CantCapture');
    const release = this.worker.hold();
    let opened: OpenedSource | null = null;
    try {
      const settings = await platform.settings().catch(() => ({ micPriority: [], lowPower: 'auto' as const }));
      // Start the Worker while the capture opens: both take a moment.
      const warm = this.worker.ensure().catch(() => undefined);
      opened = source === 'Tab' ? await platform.openTab(conn) : await platform.openMic(conn, settings.micPriority);
      await warm;
      const buffered: PcmChunk[] = [];
      let sessionId: string | null = null;
      const capture: LiveCapture = { source: opened.source, via: opened.via, frames: 0, stop: () => undefined };
      const offPcm = opened.source.onPcm((chunk) => {
        capture.frames += chunk.samples.length;
        if (sessionId) this.worker.pushPcm(sessionId, chunk);
        else buffered.push(chunk);
      });
      const offEvent = opened.source.onEvent((event) => {
        if (event.type === 'ended' && event.reason !== 'stopped' && sessionId) void this.captureEnded(sessionId, event.reason);
      });
      capture.stop = () => {
        offPcm();
        offEvent();
        capture.source.stop();
      };
      const sampleRate = (opened.source as { sampleRate?: number }).sampleRate ?? buffered[0]?.sampleRate ?? 48_000;
      sessionId = (await this.call('startLive', [source, mode, options, { kind: opened.source.kind, sampleRate, label: opened.tab?.title ?? opened.via, directive: null }])) as string;
      for (const chunk of buffered.splice(0)) this.worker.pushPcm(sessionId, chunk);
      if (!this.worker.sessions.includes(sessionId)) {
        // It ended before we got here (a very fast outcome).
        capture.stop();
        return sessionId;
      }
      this.live.set(sessionId, capture);
      void this.options.registry.put({
        id: sessionId,
        kind: mode === 'Hum' ? 'Hum' : source === 'Microphone' ? 'Listen' : 'SystemAudio',
        source,
        startedAt: Date.now(),
        capture: { via: opened.via, tabId: opened.tab?.id ?? null, url: opened.tab?.url ?? null },
      });
      return sessionId;
    } catch (error) {
      opened?.source.stop();
      throw toEngineError(error);
    } finally {
      release();
    }
  }

  async finish(sessionId: string): Promise<void> {
    // Stop the audio first: what was heard is what gets identified.
    this.stopCapture(sessionId);
    await this.call('finishSession', [sessionId]);
  }

  async cancel(sessionId: string): Promise<void> {
    this.stopCapture(sessionId);
    await this.call('cancelSession', [sessionId]);
  }

  private async captureEnded(sessionId: string, reason: string) {
    this.stopCapture(sessionId);
    await this.call('captureEnded', [sessionId, reason]).catch(() => undefined);
  }

  private stopCapture(sessionId: string) {
    const capture = this.live.get(sessionId);
    if (!capture) return;
    this.live.delete(sessionId);
    capture.stop();
  }

  // MARK: analyses

  async analyze(input: string, mode: AnalyzeMode, page: PageContext | null): Promise<string> {
    await this.ready;
    let context = page;
    const directive = this.options.platform.takeDirective();
    if (page?.tab_id != null || directive) {
      const facts = page?.tab_id != null && !directive ? await this.options.platform.pageFacts?.(page.tab_id).catch(() => null) : null;
      context = {
        tab_id: page?.tab_id ?? null,
        url: page?.url ?? null,
        title: page?.title ?? null,
        facts: { ...(page?.facts ?? {}), ...(facts ?? {}), ...(directive ? { mockDirective: directive } : {}) },
      };
    }
    return (await this.call('analyze', [input, mode, context])) as string;
  }

  // MARK: events and activity

  private sessionEvent(sessionId: string, event: SessionEvent) {
    if (typeof event === 'object' && 'Started' in event) {
      this.kinds.set(sessionId, { id: sessionId, kind: event.Started.kind, source: null, started_at_ms: Date.now() });
      this.options.platform.keepalive?.(true);
    }
    this.broadcast('session', { sessionId, event });
    if (isTerminal(event)) {
      this.stopCapture(sessionId);
      void this.options.registry.remove(sessionId);
      const info = this.kinds.get(sessionId);
      this.kinds.delete(sessionId);
      this.reportActivity({ id: sessionId, kind: info?.kind ?? ('Listen' as SessionKind), event });
      if (this.kinds.size === 0) this.options.platform.keepalive?.(false);
    } else if (typeof event === 'object' && 'Started' in event) {
      this.reportActivity(null);
    }
  }

  private reportActivity(ended: HostActivity['ended']) {
    if (this.closed) return;
    const active = [...this.kinds.values()].sort((a, b) => b.started_at_ms - a.started_at_ms);
    this.options.platform.activity({ active, ended, surfaces: this.ui.size });
  }

  /** Nothing left: no pages, no sessions, no Worker. */
  private maybeIdle() {
    if (this.closed || this.ui.size > 0 || this.live.size > 0 || this.worker.busy || this.worker.state !== 'down') return;
    this.options.platform.idle();
  }
}

/**
 * Low power (extension plan §2.7): the setting, or on "auto" the a05-1 thresholds
 * (≤ 4 CPUs or < 4 GiB). Firefox has no `deviceMemory`, so Firefox for Android counts
 * as low-end at ≤ 8 CPUs.
 */
export function resolveLowPower(setting: HostSettings['lowPower'], android: boolean, cpus: number, memoryGiB: number | null): boolean {
  if (setting === 'on') return true;
  if (setting === 'off') return false;
  if (cpus <= 4) return true;
  if (memoryGiB !== null && memoryGiB < 4) return true;
  return android && memoryGiB === null && cpus <= 8;
}
