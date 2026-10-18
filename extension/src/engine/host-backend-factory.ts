import type { BackendFactory } from './host-backend';
import { openKv } from './host-idb';
import { kvPersistence, MockBackend } from './host-mock-backend';
import { WasmBackend, type WasmModule } from './host-wasm-backend';

/**
 * The engine the engine Worker runs: the wasm engine (`core/crates/mfcore-wasm`, built by
 * `npm run build:engine`: its glue into `./wasm/`, the module into `public/engine/`) over
 * SQLite on OPFS. Builds without it, or with `WXT_MF_ENGINE=mock`, run the mock engine
 * instead (UI work, demos, screenshots).
 */
const engineModule = import.meta.glob<WasmModule>('./wasm/mfcore_wasm.js');
/** Copied as is from `public/` (an asset import would inline it in some bundles). */
export const ENGINE_WASM_PATH = '/engine/mfcore.wasm';

export const createBackend: BackendFactory = async (env) => {
  const load = engineModule['./wasm/mfcore_wasm.js'];
  if (import.meta.env.WXT_MF_ENGINE === 'mock' || !load) {
    if (import.meta.env.MODE === 'production' && import.meta.env.WXT_MF_ENGINE !== 'mock') console.warn('Music Fetch: no engine module in this build; running the mock engine');
    const kv = await openKv('mf-mock-engine');
    return new MockBackend(env, await kvPersistence(kv));
  }
  const wasm = await load();
  await wasm.default({ module_or_path: new URL(ENGINE_WASM_PATH, self.location.origin) });
  return WasmBackend.create(wasm, env, {
    // e2e builds: the test's stub recognizer (until the test names it, an address where
    // nothing listens: tests never reach the real service), paced for tests, and no
    // catalog lookups.
    ...(import.meta.env.MODE === 'e2e'
      ? { shazam_base_url: env.recognizerUrl ?? 'http://127.0.0.1:9', test_pacing_ms: 300, offline_catalog: true, log_level: 'info' }
      : {}),
  });
};
