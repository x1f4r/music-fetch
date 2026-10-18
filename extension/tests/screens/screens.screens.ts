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
/** Top of each stage slot in the 380 × 560 popup (design spec §4). */
const POPUP_SLOTS: Record<string, number> = { '.top': 10, '.chip-slot': 52, '.listen': 196, '.state:not(.two)': 312, '.sources': 352, '.recovery': 404, '.footer': 512 };

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
        page.setDefaultTimeout(60_000);
        try {
          await page.setViewport({ width: state.width, height: state.height, deviceScaleFactor: SCALE });
          await page.emulateMediaFeatures([{ name: 'prefers-color-scheme', value: theme }]);
          // The test machine is shared: a slow navigation gets one more try.
          const url = `chrome-extension://${id}/${demoUrl(state, { theme, still: true })}`;
          const ready = async () => {
            await page.goto(url);
            await page.waitForSelector('html[data-demo-ready]', { timeout: 45_000 });
          };
          await ready().catch(ready);
          const problems = await page.$eval('html', (html) => html.dataset.demoProblems ?? null);
          if (problems) failures.push(`${state.id}/${theme}: ${problems}`);
          // The popup's slots stay where the design puts them in every state (design spec §4).
          const tops = await page.evaluate((slots) => {
            const out: Record<string, number | null> = {};
            for (const selector of Object.keys(slots)) {
              const el = document.querySelector(`.popup > ${selector}`);
              out[selector] = el ? Math.round(el.getBoundingClientRect().top) : null;
            }
            const root = document.querySelector('main, .app') as HTMLElement | null;
            out.overflow = root ? root.scrollWidth - root.clientWidth : 0;
            return out;
          }, POPUP_SLOTS);
          for (const [selector, top] of Object.entries(tops)) {
            if (selector === 'overflow') {
              if ((top ?? 0) > 1) failures.push(`${state.id}/${theme}: ${top}px sideways overflow`);
            } else if (top !== null && Math.abs(top - POPUP_SLOTS[selector]!) > 1) {
              failures.push(`${state.id}/${theme}: ${selector} at ${top}, design ${POPUP_SLOTS[selector]}`);
            }
          }
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
