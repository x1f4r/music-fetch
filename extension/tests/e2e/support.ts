import { mkdirSync, mkdtempSync } from 'node:fs';
import { createServer, type Server } from 'node:http';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import type { Page } from 'puppeteer-core';

/** Built extensions, from `npm run build`. */
export const OUTPUT = {
  chromium: resolve('.output/chrome-mv3'),
  firefox: resolve('.output/firefox-mv3'),
};

/** Screenshots for a human look; not committed. */
export const ARTIFACTS = resolve('.e2e-artifacts');
mkdirSync(ARTIFACTS, { recursive: true });

export function tempProfile(name: string): string {
  return mkdtempSync(join(tmpdir(), `mf-e2e-${name}-`));
}

/**
 * How long one step may take. The test machine is shared with long CPU jobs, where a
 * browser can need tens of seconds for a navigation; raise with E2E_STEP_MS.
 */
export const STEP_MS = Number(process.env.E2E_STEP_MS ?? 90_000);

/** Headful when a display is available (the Legion's Xvfb via `mf-x`), else headless. */
export const HEADLESS = !process.env.DISPLAY || process.env.HEADLESS === '1';

/** A page on 127.0.0.1 for the action to run on ("Analyze this page" needs an http page). */
export async function startPageServer(): Promise<{ url: string; close(): Promise<void> }> {
  const server: Server = createServer((_req, res) => {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
    res.end('<!doctype html><title>Music Fetch test page</title><h1>A page with no music</h1>');
  });
  await new Promise<void>((done) => server.listen(0, '127.0.0.1', done));
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('no port');
  return {
    url: `http://127.0.0.1:${address.port}/`,
    close: () => new Promise((done) => server.close(() => done())),
  };
}

export async function text(page: Page, selector: string): Promise<string> {
  await page.waitForSelector(selector, { timeout: STEP_MS });
  return page.$eval(selector, (el) => (el.textContent ?? '').trim());
}

/**
 * Wait until `selector`'s text contains `expected`. Polls on a timer, not on animation
 * frames (a popup or a background tab gets few or none on a loaded machine), and says
 * what it saw when it gives up.
 */
export async function waitForText(page: Page, selector: string, expected: string): Promise<void> {
  try {
    await page.waitForFunction((sel, want) => document.querySelector(sel)?.textContent?.includes(want) ?? false, { timeout: STEP_MS, polling: 100 }, selector, expected);
  } catch (error) {
    const seen = await page.$eval(selector, (el) => el.textContent?.trim() ?? '').catch(() => '(no element)');
    throw new Error(`${selector} never read "${expected}" (last: "${seen}")`, { cause: error });
  }
}

/** The next live session in the mock engine (shared localStorage of the extension origin). */
export async function mockNext(page: Page, directive: 'hold' | 'match' | 'nomatch'): Promise<void> {
  await page.evaluate((value) => localStorage.setItem('mf.mock.next', value), directive);
}

/**
 * The flow both browsers must pass with the mock engine: the first-run provider panel,
 * Skip, the stage, a listen session that matches, and the result's "Open in" button.
 *
 * The session is held open until the test presses Stop (`hold`), so "Listening" is a
 * state to wait for rather than a 4-second window a loaded machine can miss.
 */
export async function popupFlow(popup: Page, label: string): Promise<{ title: string; openIn: string; chip: string | null }> {
  popup.setDefaultTimeout(STEP_MS);
  popup.setDefaultNavigationTimeout(STEP_MS);
  await popup.waitForSelector('.first-run h1');
  if ((await text(popup, '.first-run h1')) !== 'Where do you listen?') throw new Error('no provider panel');
  await popup.screenshot({ path: join(ARTIFACTS, `${label}-1-provider.png`) });

  await popup.click('.first-run .acts .btn.text'); // Skip
  await popup.waitForSelector('.listen .disc');
  const idle = await text(popup, '#stage-state');
  if (idle !== "Identify what's playing") throw new Error(`unexpected idle line: ${idle}`);
  const chip = await popup.$eval('.chip-slot', (el) => el.textContent?.trim() || null);
  await popup.screenshot({ path: join(ARTIFACTS, `${label}-2-idle.png`) });

  await mockNext(popup, 'hold');
  await popup.click('.listen .disc');
  await waitForText(popup, '#stage-state', 'Listening');
  await popup.waitForSelector('.listen.live');
  await popup.screenshot({ path: join(ARTIFACTS, `${label}-3-listening.png`) });
  await popup.click('.listen .disc'); // Stop: identify what was heard
  await popup.waitForSelector('.hero .ttl');
  const title = await text(popup, '.hero .ttl');
  const openIn = await text(popup, '.split .main');
  await popup.screenshot({ path: join(ARTIFACTS, `${label}-4-result.png`) });
  return { title, openIn, chip };
}
