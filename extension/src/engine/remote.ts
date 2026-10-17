import { RpcClient, RpcError, type PortLike } from '../messaging/bus';
import type { EngineTopics } from '../messaging/protocol';
import { ENGINE_METHODS, EngineError, type EngineApi, type EngineClient, type EngineListener } from './client';

export interface RemoteEngineOptions {
  /** Start the engine host if needed (a control message to the background). */
  ensureHost(): Promise<void>;
  /** Open a Port to the host. */
  connect(): PortLike;
  /** Connection attempts before a call fails. Default 4. */
  attempts?: number;
  /** Delay before the n-th retry is n × this. Default 150 ms. */
  retryDelayMs?: number;
  /** Per-call timeout. Default 15 s. */
  timeoutMs?: number;
}

function toEngineError(error: unknown): EngineError {
  if (error instanceof EngineError) return error;
  if (error instanceof RpcError) return new EngineError(error.code as EngineError['code'], error.message);
  return new EngineError('Unknown', error instanceof Error ? error.message : String(error));
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * The engine as UI pages see it: every `EngineApi` call travels over a Port to the
 * engine host. The connection is opened on first use, re-established after the host
 * goes away (Worker recycle, offscreen document closed, event page unloaded), and the
 * subscribers keep receiving events across reconnects.
 */
export class RemoteEngineClient implements EngineClient {
  private rpc: RpcClient<EngineApi, EngineTopics> | null = null;
  private opening: Promise<RpcClient<EngineApi, EngineTopics>> | null = null;
  private readonly listeners = new Set<EngineListener>();
  private disposed = false;

  constructor(private readonly options: RemoteEngineOptions) {}

  get connected(): boolean {
    return this.rpc !== null && !this.rpc.closed;
  }

  subscribe(listener: EngineListener): () => void {
    this.listeners.add(listener);
    if (!this.connected) void this.connection().catch(() => undefined);
    return () => this.listeners.delete(listener);
  }

  /** Close the connection; later calls reconnect. */
  disconnect(): void {
    this.rpc?.close();
    this.rpc = null;
  }

  /** Close for good (the page is going away). */
  dispose(): void {
    this.disposed = true;
    this.listeners.clear();
    this.disconnect();
  }

  /** Call an engine method by name (the typed methods below forward here). */
  async invoke(method: keyof EngineApi, args: unknown[]): Promise<unknown> {
    const rpc = await this.connection();
    try {
      return await (rpc.call as (m: string, ...a: unknown[]) => Promise<unknown>)(method, ...args);
    } catch (error) {
      throw toEngineError(error);
    }
  }

  private connection(): Promise<RpcClient<EngineApi, EngineTopics>> {
    if (this.rpc && !this.rpc.closed) return Promise.resolve(this.rpc);
    if (this.disposed) return Promise.reject(new EngineError('Disconnected', 'The engine client was closed.'));
    this.opening ??= this.open().finally(() => {
      this.opening = null;
    });
    return this.opening;
  }

  private async open(): Promise<RpcClient<EngineApi, EngineTopics>> {
    const attempts = this.options.attempts ?? 4;
    const delay = this.options.retryDelayMs ?? 150;
    let lastError: unknown = null;
    for (let attempt = 0; attempt < attempts; attempt++) {
      if (attempt > 0) await sleep(delay * attempt);
      let rpc: RpcClient<EngineApi, EngineTopics> | null = null;
      try {
        await this.options.ensureHost();
        rpc = new RpcClient<EngineApi, EngineTopics>(this.options.connect(), { timeoutMs: this.options.timeoutMs });
        // The handshake fails fast when nobody listens on the other end.
        await rpc.call('hello');
        this.attach(rpc);
        return rpc;
      } catch (error) {
        lastError = error;
        rpc?.close();
      }
    }
    throw toEngineError(lastError);
  }

  private attach(rpc: RpcClient<EngineApi, EngineTopics>) {
    this.rpc = rpc;
    rpc.on('session', ({ sessionId, event }) => {
      for (const listener of this.listeners) listener.sessionEvent?.(sessionId, event);
    });
    rpc.on('library:changed', () => {
      for (const listener of this.listeners) listener.libraryChanged?.();
    });
    rpc.onDisconnect(() => {
      if (this.rpc === rpc) this.rpc = null;
      for (const listener of this.listeners) listener.connectionChanged?.(false);
      // Keep event subscribers attached: reconnect once the host is back.
      if (this.listeners.size > 0 && !this.disposed) {
        setTimeout(() => void this.connection().catch(() => undefined), 250);
      }
    });
    for (const listener of this.listeners) listener.connectionChanged?.(true);
  }
}

// Every EngineApi method forwards to the host.
export interface RemoteEngineClient extends EngineApi {}
for (const method of ENGINE_METHODS) {
  Object.defineProperty(RemoteEngineClient.prototype, method, {
    configurable: true,
    writable: true,
    value: function (this: RemoteEngineClient, ...args: unknown[]) {
      return this.invoke(method, args);
    },
  });
}
