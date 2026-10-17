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
  await page.waitForSelector(selector, { timeout: 20_000 });
  return page.$eval(selector, (el) => (el.textContent ?? '').trim());
}

/**
 * The flow both browsers must pass with the mock engine: the first-run provider panel,
 * Skip, the stage, a listen session that matches, and the result's "Open in" button.
 */
export async function popupFlow(popup: Page, label: string): Promise<{ title: string; openIn: string; chip: string | null }> {
  await popup.waitForSelector('.first-run h1', { timeout: 20_000 });
  if ((await text(popup, '.first-run h1')) !== 'Where do you listen?') throw new Error('no provider panel');
  await popup.screenshot({ path: join(ARTIFACTS, `${label}-1-provider.png`) });

  await popup.click('.first-run .acts .btn.text'); // Skip
  await popup.waitForSelector('.listen .disc', { timeout: 20_000 });
  const idle = await text(popup, '#stage-state');
  if (idle !== "Identify what's playing") throw new Error(`unexpected idle line: ${idle}`);
  const chip = await popup.$eval('.chip-slot', (el) => el.textContent?.trim() || null);
  await popup.screenshot({ path: join(ARTIFACTS, `${label}-2-idle.png`) });

  await popup.click('.listen .disc');
  await popup.waitForFunction(() => document.querySelector('#stage-state')?.textContent?.includes('Listening'), { timeout: 10_000 });
  await popup.screenshot({ path: join(ARTIFACTS, `${label}-3-listening.png`) });
  await popup.waitForSelector('.hero .ttl', { timeout: 20_000 });
  const title = await text(popup, '.hero .ttl');
  const openIn = await text(popup, '.split .main');
  await popup.screenshot({ path: join(ARTIFACTS, `${label}-4-result.png`) });
  return { title, openIn, chip };
}
