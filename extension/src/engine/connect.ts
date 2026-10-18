import { browser } from 'wxt/browser';
import { servePageMic } from '../audio/page-mic';
import type { PortLike } from '../messaging/bus';
import { sendControl } from '../messaging/control';
import { ENGINE_PORT } from '../messaging/protocol';
import { RemoteEngineClient } from './remote';

let shared: RemoteEngineClient | null = null;

/** The engine client for this UI page (popup, side panel, sidebar, app tab). */
export function engineClient(): RemoteEngineClient {
  shared ??= new RemoteEngineClient({
    ensureHost: async () => {
      await sendControl({ type: 'mf:host.ensure' });
    },
    connect: () => browser.runtime.connect({ name: ENGINE_PORT }) as unknown as PortLike,
    // Firefox captures the microphone in this page: its background never can (S2).
    ...(import.meta.env.FIREFOX
      ? { onMicRequest: (request) => void servePageMic(request, (name) => browser.runtime.connect({ name }) as unknown as PortLike) }
      : {}),
  });
  return shared;
}
