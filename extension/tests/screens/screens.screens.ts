import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import puppeteer, { type Browser } from 'puppeteer-core';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { DEMO_STATES, demoUrl } from '../../src/ui/demo/states';
import { OUTPUT, tempProfile } from '../e2e/support';

/**
 * Screenshots of every demo state, light and dark, at 2× (like the design renders), into
 * `.screens/` with a `manifest.json` for the contact sheet. Headless Chromium with the
 * built extension: `npm run build:chrome && npm run screens`. Filter with SCREENS=<id,…>.
 */
const CHROMIUM = process.env.CHROMIUM_BIN ?? '/usr/bin/chromium';
const OUT = resolve(process.env.SCREENS_DIR ?? '.screens');
const ONLY = (process.env.SCREENS ?? '').split(',').filter(Boolean);
const THEMES = (process.env.SCREENS_THEMES ?? 'light,dark').split(',') as ('light' | 'dark')[];
const SCALE = Number(process.env.SCREENS_SCALE ?? 2);

describe.skipIf(!existsSync(CHROMIUM) || !existsSync(OUTPUT.chromium))('demo states', () => {
  let browser: Browser;
  let id = '';
  const shots: { id: string; title: string; theme: string; file: string; prototype: string | null; problems: string | null; width: number; height: number }[] = [];

  beforeAll(async () => {
    mkdirSync(OUT, { recursive: true });
    browser = await puppeteer.launch({
      executablePath: CHROMIUM,
      headless: true,
      pipe: true,
      enableExtensions: [OUTPUT.chromium],
      userDataDir: tempProfile('screens'),
      protocolTimeout: 60_000,
      args: ['--no-first-run', '--no-default-browser-check', '--mute-audio', '--disable-gpu', '--use-angle=swiftshader', '--hide-scrollbars', '--lang=en-US'],
    });
    const deadline = Date.now() + 120_000;
    while (!id && Date.now() < deadline) {
      id = [...(await browser.extensions()).values()].find((e) => e.name === 'Music Fetch')?.id ?? '';
      if (!id) await new Promise((done) => setTimeout(done, 300));
    }
    if (!id) throw new Error('the extension did not load');
  });

  afterAll(async () => {
    writeFileSync(join(OUT, 'manifest.json'), JSON.stringify(shots, null, 1));
    await browser?.close();
  });

  it('renders every state', async () => {
    const states = DEMO_STATES.filter((s) => ONLY.length === 0 || ONLY.includes(s.id));
    const failures: string[] = [];
    for (const state of states) {
      for (const theme of THEMES) {
        // A fresh page per shot: nothing (open menus, timers) carries over between states.
        const page = await browser.newPage();
        page.setDefaultTimeout(20_000);
        try {
          await page.setViewport({ width: state.width, height: state.height, deviceScaleFactor: SCALE });
          await page.emulateMediaFeatures([{ name: 'prefers-color-scheme', value: theme }]);
          await page.goto(`chrome-extension://${id}/${demoUrl(state, { theme, still: true })}`);
          try {
            await page.waitForSelector('html[data-demo-ready]', { timeout: 15_000 });
          } catch {
            failures.push(`${state.id}/${theme}: never ready`);
          }
          const problems = await page.$eval('html', (html) => html.dataset.demoProblems ?? null);
          if (problems) failures.push(`${state.id}/${theme}: ${problems}`);
          const file = `${state.id}-${theme}.png`;
          await page.screenshot({ path: join(OUT, file) as `${string}.png` });
          shots.push({ id: state.id, title: state.title, theme, file, prototype: state.prototype ?? null, problems, width: state.width, height: state.height });
        } catch (error) {
          failures.push(`${state.id}/${theme}: ${String(error).slice(0, 200)}`);
        } finally {
          await page.close().catch(() => undefined);
        }
      }
    }
    expect(failures).toEqual([]);
  });
});
