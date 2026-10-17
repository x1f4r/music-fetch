import { browser } from 'wxt/browser';
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
  });
  return shared;
}
