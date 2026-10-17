/**
 * A small typed request/response and event bus over `runtime.Port`s (extension plan
 * §3.1 "Messaging"). One side serves an object's methods; the other calls them by
 * name and receives events by topic.
 *
 * Messages are plain data. Chromium sends them with structured clone (manifest
 * `message_serialization`), Firefox always does; nothing here relies on more than JSON.
 */

/** The parts of `runtime.Port` the bus uses; in-memory ports implement it in tests. */
export interface PortLike {
  readonly name: string;
  postMessage(message: unknown): void;
  disconnect(): void;
  onMessage: EventLike<(message: unknown) => void>;
  onDisconnect: EventLike<() => void>;
}

export interface EventLike<F> {
  addListener(callback: F): void;
  removeListener(callback: F): void;
}

type Wire =
  | { mf: 1; t: 'req'; id: number; method: string; args: unknown[] }
  | { mf: 1; t: 'res'; id: number; ok: true; value: unknown }
  | { mf: 1; t: 'res'; id: number; ok: false; error: WireError }
  | { mf: 1; t: 'evt'; topic: string; payload: unknown };

interface WireError {
  code: string;
  message: string;
}

/** A failed call. `code` is the remote error's code, or Disconnected / Timeout / Unknown. */
export class RpcError extends Error {
  constructor(
    readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = 'RpcError';
  }
}

type AnyFunction = (...args: never[]) => unknown;
type MethodKeys<M> = { [K in keyof M]: M[K] extends AnyFunction ? K : never }[keyof M] & string;
type ArgsOf<F> = F extends (...args: infer A) => unknown ? A : never;
type ResultOf<F> = F extends (...args: never[]) => infer R ? Awaited<R> : never;

function isWire(message: unknown): message is Wire {
  return typeof message === 'object' && message !== null && (message as { mf?: unknown }).mf === 1;
}

function safePost(port: PortLike, message: Wire): boolean {
  try {
    port.postMessage(message);
    return true;
  } catch {
    // The other end went away; onDisconnect cleans up.
    return false;
  }
}

function toWireError(error: unknown): WireError {
  if (error && typeof error === 'object') {
    const { code, message } = error as { code?: unknown; message?: unknown };
    return {
      code: typeof code === 'string' ? code : 'Internal',
      message: typeof message === 'string' ? message : String(error),
    };
  }
  return { code: 'Internal', message: String(error) };
}

export interface RpcClientOptions {
  /** Reject calls that get no answer in time. Default 15 s; 0 waits forever. */
  timeoutMs?: number;
}

/** Calls methods of `M` on the other end of `port` and receives the topics of `T`. */
export class RpcClient<M extends object, T extends object = Record<string, never>> {
  private nextId = 1;
  private readonly pending = new Map<
    number,
    { resolve: (value: unknown) => void; reject: (error: RpcError) => void; timer?: ReturnType<typeof setTimeout> }
  >();
  private readonly topics = new Map<string, Set<(payload: unknown) => void>>();
  private readonly disconnectListeners = new Set<() => void>();
  private isClosed = false;

  constructor(
    readonly port: PortLike,
    private readonly options: RpcClientOptions = {},
  ) {
    port.onMessage.addListener(this.handleMessage);
    port.onDisconnect.addListener(this.handleDisconnect);
  }

  get closed(): boolean {
    return this.isClosed;
  }

  call<K extends MethodKeys<M>>(method: K, ...args: ArgsOf<M[K]>): Promise<ResultOf<M[K]>> {
    if (this.isClosed) return Promise.reject(new RpcError('Disconnected', 'The connection is closed.'));
    const id = this.nextId++;
    return new Promise<ResultOf<M[K]>>((resolve, reject) => {
      const timeoutMs = this.options.timeoutMs ?? 15_000;
      const timer =
        timeoutMs > 0
          ? setTimeout(() => {
              this.pending.delete(id);
              reject(new RpcError('Timeout', `No answer to ${method}.`));
            }, timeoutMs)
          : undefined;
      this.pending.set(id, { resolve: resolve as (value: unknown) => void, reject, timer });
      if (!safePost(this.port, { mf: 1, t: 'req', id, method, args })) {
        this.settle(id)?.reject(new RpcError('Disconnected', 'The connection is closed.'));
      }
    });
  }

  on<K extends keyof T & string>(topic: K, callback: (payload: T[K]) => void): () => void {
    let set = this.topics.get(topic);
    if (!set) this.topics.set(topic, (set = new Set()));
    set.add(callback as (payload: unknown) => void);
    return () => set.delete(callback as (payload: unknown) => void);
  }

  onDisconnect(callback: () => void): () => void {
    this.disconnectListeners.add(callback);
    return () => this.disconnectListeners.delete(callback);
  }

  close(): void {
    if (this.isClosed) return;
    try {
      this.port.disconnect();
    } catch {
      // already gone
    }
    this.handleDisconnect();
  }

  private settle(id: number) {
    const entry = this.pending.get(id);
    if (!entry) return undefined;
    this.pending.delete(id);
    if (entry.timer) clearTimeout(entry.timer);
    return entry;
  }

  private readonly handleMessage = (message: unknown) => {
    if (!isWire(message)) return;
    if (message.t === 'res') {
      const entry = this.settle(message.id);
      if (!entry) return;
      if (message.ok) entry.resolve(message.value);
      else entry.reject(new RpcError(message.error.code, message.error.message));
    } else if (message.t === 'evt') {
      for (const callback of this.topics.get(message.topic) ?? []) {
        try {
          callback(message.payload);
        } catch (error) {
          console.error('event listener failed', error);
        }
      }
    }
  };

  private readonly handleDisconnect = () => {
    if (this.isClosed) return;
    this.isClosed = true;
    this.port.onMessage.removeListener(this.handleMessage);
    this.port.onDisconnect.removeListener(this.handleDisconnect);
    for (const id of [...this.pending.keys()]) {
      this.settle(id)?.reject(new RpcError('Disconnected', 'The connection closed before an answer arrived.'));
    }
    for (const callback of this.disconnectListeners) callback();
    this.disconnectListeners.clear();
  };
}

export interface ServedPort<T extends object> {
  readonly port: PortLike;
  emit<K extends keyof T & string>(topic: K, payload: T[K]): void;
  close(): void;
}

/**
 * Answers calls arriving on `port` with `handlers[method](...args)`. Only `methods` are
 * callable, so a page can never reach anything else on the handler object.
 */
export function serveRpc<M extends object, T extends object = Record<string, never>>(
  port: PortLike,
  handlers: M,
  methods: readonly MethodKeys<M>[],
  onClose?: () => void,
): ServedPort<T> {
  const allowed = new Set<string>(methods);
  let open = true;

  const handleMessage = async (message: unknown) => {
    if (!isWire(message) || message.t !== 'req') return;
    const { id, method, args } = message;
    if (!allowed.has(method)) {
      safePost(port, { mf: 1, t: 'res', id, ok: false, error: { code: 'Unknown', message: `Unknown method ${method}.` } });
      return;
    }
    try {
      const fn = (handlers as Record<string, (...a: unknown[]) => unknown>)[method]!;
      const value = await fn.apply(handlers, Array.isArray(args) ? args : []);
      if (open) safePost(port, { mf: 1, t: 'res', id, ok: true, value: value === undefined ? null : value });
    } catch (error) {
      if (open) safePost(port, { mf: 1, t: 'res', id, ok: false, error: toWireError(error) });
    }
  };

  const close = () => {
    if (!open) return;
    open = false;
    port.onMessage.removeListener(handleMessage);
    port.onDisconnect.removeListener(close);
    onClose?.();
  };

  port.onMessage.addListener(handleMessage);
  port.onDisconnect.addListener(close);

  return {
    port,
    emit(topic, payload) {
      if (open && !safePost(port, { mf: 1, t: 'evt', topic, payload })) close();
    },
    close() {
      try {
        port.disconnect();
      } catch {
        // already gone
      }
      close();
    },
  };
}
