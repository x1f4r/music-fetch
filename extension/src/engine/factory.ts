import { browser } from 'wxt/browser';
import type { EngineClient } from './client';
import { serveEngineHost, type EngineHost, type HostRuntime } from './host';
import { MockEngine } from './mock/engine';
import { localStoragePersistence } from './mock/persistence';

/** localStorage key a developer sets to force the next live session's outcome. */
export const MOCK_DIRECTIVE_KEY = 'mf.mock.next';

/**
 * The engine the host serves. Until the wasm engine exists (extension plan E7, X2) this
 * is the mock; X2 replaces it with the engine Worker client.
 */
export function createHostEngine(): EngineClient {
  return new MockEngine({
    persistence: localStoragePersistence(),
    seed: true,
    directive: () => {
      try {
        const value = globalThis.localStorage?.getItem(MOCK_DIRECTIVE_KEY) ?? null;
        if (value) globalThis.localStorage.removeItem(MOCK_DIRECTIVE_KEY);
        return value;
      } catch {
        return null;
      }
    },
  });
}

/** Start serving the engine in this context (offscreen document or Firefox event page). */
export function startEngineHost(): EngineHost {
  return serveEngineHost(createHostEngine(), browser.runtime as unknown as HostRuntime);
}
