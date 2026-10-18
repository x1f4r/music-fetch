import type { Plugin } from 'vite';
import { defineConfig } from 'wxt';
import { buildManifest } from './src/platform/manifest';

/**
 * Unminified bundles keep `//#region` comments naming each module, and WXT's virtual
 * entrypoints name it by absolute path. Keep the module names (they help store review)
 * but drop the build machine's directory.
 */
function relativeModulePaths(): Plugin {
  const root = `${process.cwd()}/`;
  return {
    name: 'mf-relative-module-paths',
    renderChunk(code) {
      return code.includes(root) ? { code: code.split(root).join(''), map: null } : null;
    },
  };
}

// https://wxt.dev/api/config.html
export default defineConfig({
  modules: ['@wxt-dev/module-svelte'],
  manifestVersion: 3,
  // Explicit imports everywhere: easier to read, and the same code runs under Vitest.
  imports: false,
  manifest: ({ browser, mode }) => buildManifest({ browser, mode }),
  zip: {
    artifactTemplate: 'MusicFetch-{{version}}-{{browser}}.zip',
    sourcesTemplate: 'MusicFetch-{{version}}-sources.zip',
  },
  vite: () => ({
    plugins: [relativeModulePaths()],
    build: {
      // Readable bundles for store review; the engine wasm is the size that matters.
      minify: false,
      sourcemap: false,
    },
  }),
});
