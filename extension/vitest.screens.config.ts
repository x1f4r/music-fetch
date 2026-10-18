import { defineConfig } from 'vitest/config';

// Screenshots and the accessibility audit of every demo state (tests/screens). Run after `npm run build:chrome`.
export default defineConfig({
  test: {
    environment: 'node',
    include: ['tests/screens/**/*.screens.ts'],
    testTimeout: 1_800_000,
    hookTimeout: 240_000,
    // One browser at a time on the shared test machine.
    fileParallelism: false,
  },
});
