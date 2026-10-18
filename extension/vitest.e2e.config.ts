import { defineConfig } from 'vitest/config';

// End-to-end smoke tests: load the built extension (.output/) into real browsers.
// Run `npm run e2e` (builds first). Browsers come from CHROMIUM_BIN / FIREFOX_BIN.
export default defineConfig({
  test: {
    environment: 'node',
    include: ['tests/e2e/**/*.e2e.ts'],
    // Generous: browsers start slowly on a busy test machine.
    testTimeout: 600_000,
    hookTimeout: 240_000,
    fileParallelism: false,
  },
});
