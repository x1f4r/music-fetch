import { createHmac } from 'node:crypto';
import { existsSync } from 'node:fs';
import { createServer as createHttpServer, type Server } from 'node:http';
import { createServer } from 'node:net';
import { join } from 'node:path';
import puppeteer, { type Browser, type Page } from 'puppeteer-core';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import webExt from 'web-ext';
import { ARTIFACTS, HEADLESS, tempProfile } from './support';
import { E2E_OUTPUT, RUNTIME_E2E, rpc, sleep } from './runtime-support';


/** A screenshot for a human look; Firefox can't capture extension pages (privileged), so it may be missing. */
async function shot(page: Page, path: string): Promise<void> {
  await page.screenshot({ path }).catch(() => undefined);
}

/**
 * Settings in both browsers on the e2e builds (X10, X11): every pane on the real host and
 * mock engine, and the Services flow against a local ACRCloud stub that checks the request
 * signature the way ACRCloud does. The keys must work, must never land in storage.sync or
 * in clear anywhere, and the vault key must not be extractable.
 */

const CHROMIUM = process.env.CHROMIUM_BIN ?? '/usr/bin/chromium';
const FIREFOX = process.env.FIREFOX_BIN ?? '/usr/bin/firefox';
const ACCESS_KEY = 'e2e-access-key-7c1d';
const SECRET = 'E2E-acr-secret-Zq81-not-a-real-key';
const WRONG = 'E2E-wrong-secret-Wm4';

interface AcrStub {
  host: string;
  requests: { accessKey: string; ok: boolean }[];
  close(): Promise<void>;
}

/** ACRCloud's identify endpoint, verifying `signature` with the known secret (core acrcloud.rs). */
async function startAcrStub(): Promise<AcrStub> {
  const requests: AcrStub['requests'] = [];
  const server: Server = createHttpServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on('data', (c: Buffer) => chunks.push(c));
    req.on('end', async () => {
      if (req.url !== '/v1/identify' || req.method !== 'POST') return void res.writeHead(404).end();
      const form = await new Response(Buffer.concat(chunks), { headers: { 'content-type': String(req.headers['content-type']) } }).formData();
      const accessKey = String(form.get('access_key'));
      const toSign = `POST\n/v1/identify\n${accessKey}\naudio\n1\n${String(form.get('timestamp'))}`;
      const expected = createHmac('sha1', SECRET).update(toSign).digest('base64');
      const ok = accessKey === ACCESS_KEY && form.get('signature') === expected && form.get('sample') instanceof Blob;
      requests.push({ accessKey, ok });
      res.writeHead(200, { 'content-type': 'application/json', 'access-control-allow-origin': '*' });
      res.end(JSON.stringify(ok ? { status: { code: 1001, msg: 'No result', version: '1.0' } } : { status: { code: 3014, msg: 'invalid signature', version: '1.0' } }));
    });
  });
  await new Promise<void>((done) => server.listen(0, '127.0.0.1', done));
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('no port');
  return { host: `127.0.0.1:${address.port}`, requests, close: () => new Promise((done) => server.close(() => done())) };
}

type Ext = {
  storage: {
    sync: { get(k: null): Promise<Record<string, unknown>>; set(v: unknown): Promise<void> };
    local: { get(k: null): Promise<Record<string, unknown>> };
  };
};

/** What the browser keeps about the keys: both storage areas and the IndexedDB vault, as text. */
async function storedText(page: Page): Promise<{ sync: string; local: string; vault: string; extractable: boolean | null }> {
  return page.evaluate(async () => {
    const g = globalThis as unknown as { browser?: Ext; chrome?: Ext };
    const api = (g.browser ?? g.chrome)!;
    const db = await new Promise<IDBDatabase>((resolve, reject) => {
      const req = indexedDB.open('mf-secrets');
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
    const get = (name: string) =>
      new Promise<unknown>((resolve) => {
        if (!db.objectStoreNames.contains('vault')) return resolve(undefined);
        const req = db.transaction('vault').objectStore('vault').get(name);
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => resolve(undefined);
      });
    const sealed = (await get('providers')) as { data?: ArrayBuffer } | undefined;
    const key = (await get('key')) as CryptoKey | undefined;
    const bytes = sealed?.data ? new Uint8Array(sealed.data) : new Uint8Array();
    db.close();
    return {
      sync: JSON.stringify(await api.storage.sync.get(null)),
      local: JSON.stringify(await api.storage.local.get(null)),
      vault: `${new TextDecoder().decode(bytes)} ${String.fromCharCode(...bytes)}`,
      extractable: key ? key.extractable : null,
    };
  });
}

/** One browser's pass through every pane. `go` switches the app tab's route. */
function settingsFlow(name: string, page: () => Page, go: (hash: string) => Promise<void>, stub: () => AcrStub) {
  const status = (p: Page, provider: string) =>
    p.evaluate((id) => document.querySelector(`#svc-${id}-title`)?.closest('section')?.querySelector('.svc-status')?.textContent?.trim() ?? '', provider);

  async function waitStatus(p: Page, provider: string, text: string) {
    const deadline = Date.now() + 30_000;
    let last = '';
    while (Date.now() < deadline) {
      last = await status(p, provider);
      if (last.includes(text)) return;
      await sleep(150);
    }
    throw new Error(`${provider} status never read "${text}" (last "${last}")`);
  }

  async function fill(p: Page, selector: string, value: string) {
    await p.waitForSelector(selector, { timeout: 20_000 });
    await p.click(selector, { count: 3 });
    await p.type(selector, value);
  }

  it(`${name}: saves ACRCloud humming keys, checks them against the stub and keeps them encrypted`, async () => {
    const p = page();
    await go('#/settings/services');
    await p.waitForSelector('#svc-acrHumming-title ~ .group .add-keys', { timeout: 20_000 });
    await p.click('#svc-acrHumming-title ~ .group .add-keys');
    await fill(p, '#svc-acrHumming-host', stub().host);
    await fill(p, '#svc-acrHumming-key', ACCESS_KEY);
    await fill(p, '#svc-acrHumming-secret', SECRET);
    await p.click('#svc-acrHumming-title + form button[type="submit"]');
    await waitStatus(p, 'acrHumming', 'Works. Hum something to try it.');
    expect(stub().requests.at(-1)).toEqual({ accessKey: ACCESS_KEY, ok: true });
    await shot(p, join(ARTIFACTS, `settings-${name}-services.png`));

    const stored = await storedText(p);
    for (const area of [stored.sync, stored.local, stored.vault]) expect(area).not.toContain(SECRET);
    expect(stored.sync).not.toContain(ACCESS_KEY);
    expect(stored.vault).not.toContain(ACCESS_KEY);
    expect(stored.extractable).toBe(false);
    // The page shows only the summary.
    const text = await p.evaluate(() => document.querySelector('.form')?.textContent ?? '');
    expect(text).toContain('••••7c1d');
    expect(text).not.toContain(SECRET);
  });

  it(`${name}: says when ACRCloud rejects the keys, and removes them`, async () => {
    const p = page();
    await p.click('#svc-acrHumming-title ~ .group .svc-actions .btn.text'); // Change
    await fill(p, '#svc-acrHumming-key', ACCESS_KEY);
    await fill(p, '#svc-acrHumming-secret', WRONG);
    await p.click('#svc-acrHumming-title + form button[type="submit"]');
    await waitStatus(p, 'acrHumming', "ACRCloud didn't accept these keys.");
    expect(stub().requests.at(-1)?.ok).toBe(false);
    expect((await storedText(p)).vault).not.toContain(WRONG);
    await p.evaluate(() => ([...document.querySelectorAll('#svc-acrHumming-title ~ .group .btn')].find((b) => b.textContent?.trim() === 'Remove') as HTMLElement).click());
    await p.waitForSelector('#svc-acrHumming-title ~ .group .add-keys', { timeout: 15_000 });
  });

  it(`${name}: walks the melody library through download, check and remove`, async () => {
    const p = page();
    await go('#/settings/melody');
    const button = (label: string) =>
      p.waitForFunction((l) => [...document.querySelectorAll('.melody-row .btn')].some((b) => b.textContent?.trim() === l), { timeout: 60_000, polling: 150 }, label);
    const press = (label: string) =>
      p.evaluate((l) => ([...document.querySelectorAll('.melody-row .btn')].find((b) => b.textContent?.trim() === l) as HTMLElement).click(), label);
    await p.waitForSelector('.melody-row .btn', { timeout: 20_000 });
    if (await p.evaluate(() => [...document.querySelectorAll('.melody-row .btn')].some((b) => b.textContent?.trim() === 'Download'))) {
      await press('Download');
      await p.waitForSelector('.melody-row [role="progressbar"]', { timeout: 20_000 });
    }
    await button('Check for updates');
    expect(await p.evaluate(() => document.querySelector('.melody-row .sub')?.textContent ?? '')).toMatch(/songs · \d+ MB · updated/);
    await press('Check for updates');
    await button('Up to date');
    await press('Remove');
    await button('Download');
    await shot(p, join(ARTIFACTS, `settings-${name}-melody.png`));
  });

  it(`${name}: applies low power at once`, async () => {
    const p = page();
    await go('#/settings/listening');
    await p.waitForSelector('.seg[role="radiogroup"] [role="radio"]', { timeout: 20_000 });
    const choose = (label: string) =>
      p.evaluate((l) => ([...document.querySelectorAll('.seg[role="radiogroup"] [role="radio"]')].find((b) => b.textContent?.trim() === l) as HTMLElement).click(), label);
    await choose('On');
    await p.waitForFunction(() => document.querySelector('.seg [aria-checked="true"]')?.textContent?.trim() === 'On', { timeout: 10_000 });
    const deadline = Date.now() + 10_000;
    while (Date.now() < deadline && !(await rpc<{ lowPower: boolean }>(p, 'mf-host', 'state')).lowPower) await sleep(200);
    expect((await rpc<{ lowPower: boolean }>(p, 'mf-host', 'state')).lowPower).toBe(true);
    await choose('Off');
    await sleep(500);
    expect((await rpc<{ lowPower: boolean }>(p, 'mf-host', 'state')).lowPower).toBe(false);
    await choose('Auto');
  });

  it(`${name}: shows storage, privacy, shortcuts and the licenses`, async () => {
    const p = page();
    await go('#/settings/storage');
    await p.waitForFunction(() => /Library .* · Cache .* · Melody/.test(document.querySelector('.form')?.textContent ?? ''), { timeout: 20_000 });
    await go('#/settings/privacy');
    await p.waitForFunction(() => document.querySelector('.form')?.textContent?.includes('Short audio fingerprints, to Shazam'), { timeout: 20_000 });
    await go('#/settings/about');
    await p.waitForFunction(() => document.querySelector('.licenses')?.textContent?.includes('svelte'), { timeout: 20_000 });
    await shot(p, join(ARTIFACTS, `settings-${name}-about.png`));
  });
}

async function freePort(): Promise<number> {
  return new Promise((done, fail) => {
    const probe = createServer();
    probe.once('error', fail);
    probe.listen(0, '127.0.0.1', () => {
      const address = probe.address();
      probe.close(() => (typeof address === 'object' && address ? done(address.port) : fail(new Error('no port'))));
    });
  });
}

describe.skipIf(!RUNTIME_E2E || !existsSync(CHROMIUM) || !existsSync(E2E_OUTPUT.chromium))('Chromium settings', () => {
  let browser: Browser;
  let stub: AcrStub;
  let app: Page;
  let id = '';

  beforeAll(async () => {
    stub = await startAcrStub();
    browser = await puppeteer.launch({
      executablePath: CHROMIUM,
      headless: HEADLESS,
      pipe: true,
      enableExtensions: [E2E_OUTPUT.chromium],
      userDataDir: tempProfile('settings-chromium'),
      defaultViewport: { width: 1280, height: 860 },
      args: ['--no-first-run', '--no-default-browser-check', '--disable-gpu', '--mute-audio'],
    });
    const deadline = Date.now() + 180_000;
    while (!id && Date.now() < deadline) {
      id = [...(await browser.extensions()).values()].find((e) => e.name === 'Music Fetch')?.id ?? '';
      if (!id) await sleep(500);
    }
    if (!id) throw new Error('the extension did not load');
    app = (await (await browser.waitForTarget((t) => t.url().startsWith(`chrome-extension://${id}/app.html#/welcome`), { timeout: 60_000 })).asPage())!;
    await app.waitForSelector('.welcome', { timeout: 30_000 });
    await app.evaluate(() => (globalThis as unknown as { chrome: Ext }).chrome.storage.sync.set({ providerDecision: { provider: 'spotify', skipped: true } }));
  });

  afterAll(async () => {
    await browser?.close();
    await stub?.close();
  });

  settingsFlow('chromium', () => app, async (hash) => {
    await app.evaluate((h) => void (location.hash = h), hash);
    await app.waitForSelector(`.form[data-pane="${hash.split('/').pop()}"]`, { timeout: 20_000 });
  }, () => stub);

  it('chromium: lists the shortcuts the browser assigned', async () => {
    await app.evaluate(() => void (location.hash = '#/settings/shortcuts'));
    await app.waitForFunction(() => document.querySelector('.form')?.textContent?.includes('Identify this tab'), { timeout: 20_000 });
    expect(await app.evaluate(() => document.querySelector('.form')?.textContent ?? '')).toContain('Alt+Shift+I');
  });
});

describe.skipIf(!RUNTIME_E2E || !existsSync(FIREFOX) || !existsSync(E2E_OUTPUT.firefox))('Firefox settings', () => {
  let runner: { exit(): Promise<void> } | null = null;
  let browser: Browser;
  let stub: AcrStub;
  let tab: Page;

  beforeAll(async () => {
    stub = await startAcrStub();
    const port = await freePort();
    process.env.MOZ_REMOTE_ALLOW_SYSTEM_ACCESS = '1';
    runner = await webExt.cmd.run(
      {
        sourceDir: E2E_OUTPUT.firefox,
        target: ['firefox-desktop'],
        firefox: FIREFOX,
        args: [`--remote-debugging-port=${port}`, ...(HEADLESS ? ['-headless'] : []), '--window-size=1280,900'],
        pref: { 'browser.shell.checkDefaultBrowser': false, 'datareporting.policy.dataSubmissionEnabled': false, 'media.volume_scale': '0.0' },
        startUrl: ['about:blank'],
        noReload: true,
        noInput: true,
        browserConsole: false,
      },
      { shouldExitProgram: false },
    );
    browser = await puppeteer.connect({ browserWSEndpoint: `ws://127.0.0.1:${port}/session`, protocol: 'webDriverBiDi' });
    const target = await browser.waitForTarget((t) => /^moz-extension:\/\/[^/]+\/app\.html#\/welcome/.test(t.url()), { timeout: 60_000 });
    tab = (await target.asPage())!;
    await tab.waitForSelector('.welcome', { timeout: 30_000 });
    await tab.evaluate(() => (globalThis as unknown as { browser: Ext }).browser.storage.sync.set({ providerDecision: { provider: 'spotify', skipped: true } }));
  });

  afterAll(async () => {
    await browser?.disconnect();
    await runner?.exit();
    await stub?.close();
  });

  settingsFlow('firefox', () => tab, async (hash) => {
    await tab.evaluate((h) => void (location.hash = h), hash);
    await tab.waitForSelector(`.form[data-pane="${hash.split('/').pop()}"]`, { timeout: 20_000 });
  }, () => stub);
});
