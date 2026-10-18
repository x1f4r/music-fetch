import type { PcmChunk } from '../audio/types';
import type { CaptureHost } from '../capture/types';
import { RpcClient, serveRpc } from '../messaging/bus';
import type { EngineTopics } from '../messaging/protocol';
import { EngineError, type CaptureStatus } from './client';
import { isTerminal } from './events';
import type { BackendConfig, EngineBackend } from './host-backend';
import { channelPort, type ChannelPort } from './host-ports';
import { CAPTURE_METHODS, type PcmMessage, type WorkerInit, type WorkerReply } from './host-worker-serve';
import type { SessionEvent } from './types';

/** The parts of `Worker` the host uses (a fake in unit tests). */
export interface WorkerLike {
  postMessage(message: unknown, transfer: Transferable[]): void;
  terminate(): void;
  addEventListener(type: 'message' | 'error' | 'messageerror', listener: (event: Event) => void): void;
}

export type WorkerState = 'down' | 'starting' | 'up' | 'recycling';

export interface WorkerEngineOptions {
  spawn(): WorkerLike;
  config(): BackendConfig;
  /** The host's capture side, served to the engine. */
  capture: CaptureHost;
  /** Recycle after this long without sessions, calls or holds (plan §3.1: 10 min, 2 in low power). */
  idleMs(): number;
  onSessionEvent(sessionId: string, event: SessionEvent): void;
  onLibraryChanged(): void;
  /** The engine reported a page capture's state (the mock does for scripted mixes). */
  onCaptureStatus?(sessionId: string, status: CaptureStatus): void;
  onState?(state: WorkerState, reason: string): void;
  /** Default 30 s (wasm init plus the database open on a slow phone). */
  startTimeoutMs?: number;
  /** Default 60 s per call. */
  callTimeoutMs?: number;
  /** Cooperative shutdown budget before terminate. Default 5 s. */
  shutdownTimeoutMs?: number;
}

function withTimeout<T>(promise: Promise<T>, ms: number, what: string): Promise<T> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new EngineError('Timeout', `${what} took longer than ${Math.round(ms / 1000)} s.`)), ms);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error: unknown) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

interface Running {
  worker: WorkerLike;
  rpc: RpcClient<EngineBackend, EngineTopics>;
  engine: ChannelPort;
  ports: ChannelPort[];
}

/**
 * The engine Worker as the host owns it (extension plan §3.1 "Lifecycle"): started on
 * the first call, recycled after the idle timeout (wasm memory never shrinks: about
 * 70 MB once the melody index is loaded), shut down cooperatively (close → terminate,
 * S3: Chromium's terminate doesn't interrupt a long write), and restarted after a crash.
 * Sessions that were running when it died are failed for the UI; the engine itself
 * marks them failed in its store on the next start.
 */
export class WorkerEngine {
  private running: Running | null = null;
  private starting: Promise<Running> | null = null;
  private stopping: Promise<void> | null = null;
  private readonly active = new Set<string>();
  private pending = 0;
  private holds = 0;
  private idleTimer: ReturnType<typeof setTimeout> | null = null;
  private currentState: WorkerState = 'down';
  /** Worker starts so far (for diagnostics and tests). */
  starts = 0;

  constructor(private readonly options: WorkerEngineOptions) {}

  get state(): WorkerState {
    return this.currentState;
  }

  /** Sessions the engine is running. */
  get sessions(): string[] {
    return [...this.active];
  }

  /** Something still needs the engine (sessions, calls in flight, holds). */
  get busy(): boolean {
    return this.active.size > 0 || this.pending > 0 || this.holds > 0;
  }

  /** Call a backend method, starting the Worker if needed. */
  async call<K extends keyof EngineBackend & string>(method: K, ...args: Parameters<Extract<EngineBackend[K], (...a: never[]) => unknown>>): Promise<Awaited<ReturnType<Extract<EngineBackend[K], (...a: never[]) => unknown>>>> {
    this.pending++;
    this.touch();
    try {
      const running = await this.ensure();
      return (await (running.rpc.call as (m: string, ...a: unknown[]) => Promise<unknown>)(method, ...args)) as never;
    } finally {
      this.pending--;
      this.touch();
    }
  }

  /** Audio for a session. Dropped while no Worker runs (a session can't exist then). */
  pushPcm(sessionId: string, chunk: PcmChunk): void {
    const engine = this.running?.engine;
    if (!engine) return;
    const message: PcmMessage = { t: 'mf:pcm', sessionId, chunk };
    engine.send(message, [chunk.samples.buffer]);
  }

  /** Keep the Worker while the returned release isn't called (a capture is being set up). */
  hold(): () => void {
    this.holds++;
    this.touch();
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.holds--;
      this.touch();
    };
  }

  /** Start the Worker if it isn't running. */
  async ensure(): Promise<Running> {
    if (this.stopping) await this.stopping;
    if (this.running) return this.running;
    this.starting ??= this.start().finally(() => {
      this.starting = null;
    });
    return this.starting;
  }

  private setState(state: WorkerState, reason: string) {
    this.currentState = state;
    this.options.onState?.(state, reason);
  }

  private async start(): Promise<Running> {
    this.setState('starting', 'call');
    const worker = this.options.spawn();
    const engine = new MessageChannel();
    const capture = new MessageChannel();
    const enginePort = channelPort(engine.port1, 'engine');
    const capturePort = channelPort(capture.port1, 'capture');
    const rpc = new RpcClient<EngineBackend, EngineTopics>(enginePort, { timeoutMs: this.options.callTimeoutMs ?? 60_000 });
    rpc.on('session', ({ sessionId, event }) => this.sessionEvent(sessionId, event));
    rpc.on('library:changed', () => this.options.onLibraryChanged());
    rpc.on('capture', ({ sessionId, status }) => this.options.onCaptureStatus?.(sessionId, status));
    serveRpc(capturePort, this.options.capture, CAPTURE_METHODS);
    const running: Running = { worker, rpc, engine: enginePort, ports: [enginePort, capturePort] };

    const ready = new Promise<void>((resolve, reject) => {
      worker.addEventListener('message', (event) => {
        const reply = (event as MessageEvent<WorkerReply>).data;
        if (reply?.type === 'mf:worker.ready') resolve();
        else if (reply?.type === 'mf:worker.failed') reject(new EngineError('Internal', reply.message));
      });
      worker.addEventListener('error', (event) => {
        const message = (event as ErrorEvent).message || 'The engine stopped.';
        if (this.running === running) this.crash(message);
        else reject(new EngineError('Internal', message));
      });
    });
    const init: WorkerInit = { type: 'mf:worker.init', config: this.options.config(), engine: engine.port2, capture: capture.port2 };
    worker.postMessage(init, [engine.port2, capture.port2]);
    try {
      await withTimeout(ready, this.options.startTimeoutMs ?? 30_000, 'Starting the engine');
    } catch (error) {
      this.teardown(running);
      this.setState('down', 'start failed');
      throw error;
    }
    this.starts++;
    this.running = running;
    this.setState('up', 'started');
    this.touch();
    return running;
  }

  private sessionEvent(sessionId: string, event: SessionEvent) {
    if (typeof event === 'object' && 'Started' in event) this.active.add(sessionId);
    if (isTerminal(event)) this.active.delete(sessionId);
    this.touch();
    this.options.onSessionEvent(sessionId, event);
  }

  private teardown(running: Running) {
    try {
      running.worker.terminate();
    } catch {
      // already gone
    }
    for (const port of running.ports) port.hangUp();
  }

  /** The Worker died (a wasm panic aborts it): fail what was running, start again on the next call. */
  private crash(message: string) {
    const running = this.running;
    if (!running) return;
    this.running = null;
    this.teardown(running);
    const failed = [...this.active];
    this.active.clear();
    this.setState('down', `crashed: ${message}`);
    for (const id of failed) this.options.onSessionEvent(id, { Failed: { code: 'Internal', message: 'The engine stopped.' } });
  }

  /**
   * Shut the Worker down (cooperatively: the backend finishes its writes and closes its
   * database, then the Worker is terminated). Running sessions are failed, so call this
   * only when idle unless the user asked.
   */
  recycle(reason = 'idle'): Promise<void> {
    if (!this.running) return this.stopping ?? Promise.resolve();
    this.stopping ??= (async () => {
      const running = this.running!;
      this.setState('recycling', reason);
      try {
        await withTimeout(running.rpc.call('shutdown'), this.options.shutdownTimeoutMs ?? 5000, 'Closing the engine');
      } catch {
        // terminate anyway
      }
      this.running = null;
      this.teardown(running);
      const failed = [...this.active];
      this.active.clear();
      for (const id of failed) this.options.onSessionEvent(id, { Failed: { code: 'Internal', message: 'The engine was restarted.' } });
      this.setState('down', reason);
    })().finally(() => {
      this.stopping = null;
    });
    return this.stopping;
  }

  /** Re-arm the idle timer after any change. */
  private touch() {
    if (this.idleTimer) clearTimeout(this.idleTimer);
    this.idleTimer = null;
    if (!this.running || this.busy) return;
    this.idleTimer = setTimeout(() => {
      this.idleTimer = null;
      if (this.running && !this.busy) void this.recycle('idle');
    }, this.options.idleMs());
  }
}
