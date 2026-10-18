import type { BackendFactory } from './host-backend';
import { openKv } from './host-idb';
import { kvPersistence, MockBackend } from './host-mock-backend';

/**
 * The engine the engine Worker runs. This is the one place the wasm engine (extension
 * plan E7) swaps in: `createWasmBackend(env)` over `mfcore-wasm`'s `Engine`, which keeps
 * its library in SQLite on OPFS instead of the mock's IndexedDB snapshot.
 */
export const createBackend: BackendFactory = async (env) => {
  const kv = await openKv('mf-mock-engine');
  return new MockBackend(env, await kvPersistence(kv));
};
