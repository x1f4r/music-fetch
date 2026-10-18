/**
 * Requests from the isolated-world agent to the MAIN-world page script and events back,
 * over `window.postMessage` with JSON strings (strings cross Firefox's Xray boundary
 * cleanly) and a per-injection nonce. The page can read the nonce, so nothing that
 * crosses here is trusted beyond being a hint or a harmless player command.
 */

export const PAGE_CHANNEL = 'mf-page';

export type PageOp = 'hello' | 'facts' | 'mark' | 'seek' | 'play' | 'pause' | 'quality' | 'state' | 'reveal';

interface Envelope {
  ch: typeof PAGE_CHANNEL;
  nonce: string;
  /** Request id; absent for events. */
  id?: number;
  dir: 'req' | 'res' | 'evt';
  op?: PageOp;
  args?: unknown;
  ok?: boolean;
  value?: unknown;
  event?: PageEvent;
}

export type PageEvent = { type: 'navigated'; url: string } | { type: 'ad'; showing: boolean };

function parse(data: unknown, nonce: string): Envelope | null {
  if (typeof data !== 'string' || !data.startsWith('{"ch":"mf-page"')) return null;
  try {
    const envelope = JSON.parse(data) as Envelope;
    return envelope.nonce === nonce ? envelope : null;
  } catch {
    return null;
  }
}

function post(win: Window, envelope: Envelope) {
  win.postMessage(JSON.stringify(envelope), win.location.origin === 'null' ? '*' : win.location.origin);
}

/** The agent's side. */
export class PageBridge {
  private nextId = 1;
  private readonly pending = new Map<number, (envelope: Envelope) => void>();
  private readonly listeners = new Set<(event: PageEvent) => void>();

  constructor(
    private readonly win: Window,
    readonly nonce: string,
  ) {
    win.addEventListener('message', this.onMessage);
  }

  private readonly onMessage = (event: MessageEvent) => {
    if (event.source !== this.win) return;
    const envelope = parse(event.data, this.nonce);
    if (!envelope) return;
    if (envelope.dir === 'res' && envelope.id !== undefined) this.pending.get(envelope.id)?.(envelope);
    else if (envelope.dir === 'evt' && envelope.event) for (const listener of this.listeners) listener(envelope.event);
  };

  /** Ask the page script; rejects when it doesn't answer (not injected, or the page blocks it). */
  call<T>(op: PageOp, args?: unknown, timeoutMs = 1500): Promise<T> {
    const id = this.nextId++;
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`The page script did not answer ${op}.`));
      }, timeoutMs);
      this.pending.set(id, (envelope) => {
        clearTimeout(timer);
        this.pending.delete(id);
        if (envelope.ok) resolve(envelope.value as T);
        else reject(new Error(String(envelope.value)));
      });
      post(this.win, { ch: PAGE_CHANNEL, nonce: this.nonce, id, dir: 'req', op, args });
    });
  }

  onEvent(listener: (event: PageEvent) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  close() {
    this.win.removeEventListener('message', this.onMessage);
    this.listeners.clear();
  }
}

/** The page script's side: answer requests with `handlers`. */
export function servePage(win: Window, nonce: string, handlers: Partial<Record<PageOp, (args: unknown) => unknown>>): { emit(event: PageEvent): void; close(): void } {
  const onMessage = async (event: MessageEvent) => {
    if (event.source !== win) return;
    const envelope = parse(event.data, nonce);
    if (!envelope || envelope.dir !== 'req' || envelope.id === undefined || !envelope.op) return;
    const handler = handlers[envelope.op];
    let ok = true;
    let value: unknown;
    try {
      if (!handler) throw new Error(`No ${envelope.op} here.`);
      value = await handler(envelope.args);
    } catch (error) {
      ok = false;
      value = error instanceof Error ? error.message : String(error);
    }
    post(win, { ch: PAGE_CHANNEL, nonce, id: envelope.id, dir: 'res', ok, value: value ?? null });
  };
  win.addEventListener('message', onMessage);
  return {
    emit: (event) => post(win, { ch: PAGE_CHANNEL, nonce, dir: 'evt', event }),
    close: () => win.removeEventListener('message', onMessage),
  };
}
