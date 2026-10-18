import { browser } from 'wxt/browser';
import type { ControlRequest, ControlResponse, ControlType } from './protocol';

type ControlReply<K extends ControlType> = { ok: true; value: ControlResponse<K> } | { ok: false; message: string; reason?: string };

/** A control message the background answered with an error; `reason` as the handler threw it (e.g. a `CaptureReason`). */
export class ControlError extends Error {
  constructor(
    message: string,
    readonly reason?: string,
  ) {
    super(message);
    this.name = 'ControlError';
  }
}

/** Send a control message to the background and wait for its answer. */
export async function sendControl<K extends ControlType>(request: ControlRequest<K>): Promise<ControlResponse<K>> {
  const reply = (await browser.runtime.sendMessage(request)) as ControlReply<K> | undefined;
  if (!reply) throw new Error(`Nobody answered ${request.type}.`);
  if (!reply.ok) throw new ControlError(reply.message, reply.reason);
  return reply.value;
}

export type ControlHandlers = {
  [K in ControlType]?: (request: ControlRequest<K>) => Promise<ControlResponse<K>>;
};

/**
 * Answer control messages in this context (the background). Messages of other types
 * are left alone so other listeners can answer them.
 */
export function onControl(handlers: ControlHandlers): () => void {
  const listener = (message: unknown, sender: { id?: string }, sendResponse: (reply: unknown) => void) => {
    if (sender.id !== browser.runtime.id) return undefined;
    const type = (message as { type?: unknown } | null)?.type;
    if (typeof type !== 'string' || !(type in handlers)) return undefined;
    const handler = handlers[type as ControlType] as ((request: unknown) => Promise<unknown>) | undefined;
    if (!handler) return undefined;
    handler(message).then(
      (value) => sendResponse({ ok: true, value }),
      (error: unknown) => {
        const reason = (error as { reason?: unknown } | null)?.reason;
        sendResponse({ ok: false, message: error instanceof Error ? error.message : String(error), ...(typeof reason === 'string' ? { reason } : {}) });
      },
    );
    // Keep the channel open for the async answer (works in Chromium and Firefox).
    return true;
  };
  browser.runtime.onMessage.addListener(listener as never);
  return () => browser.runtime.onMessage.removeListener(listener as never);
}
