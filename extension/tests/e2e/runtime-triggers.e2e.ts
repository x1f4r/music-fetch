import { existsSync } from 'node:fs';
import { createServer } from 'node:net';
import { join } from 'node:path';
import puppeteer, { type Browser, type Page } from 'puppeteer-core';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import webExt from 'web-ext';
import { ARTIFACTS, HEADLESS, tempProfile } from './support';
import { E2E_OUTPUT, RUNTIME_E2E, sleep, startFixtureServer, type FixtureServer } from './runtime-support';

/**
 * Shortcuts and context menus (X7) in both browsers, on the e2e builds: the test invokes
 * the extension on a tab with the toolbar action where a real user would (Chromium's tab
 * capture needs that), then dispatches the command or menu click to the background as the
 * browser does (`mf:e2e.trigger`; WebDriver can press neither a browser shortcut nor a
 * native context menu). The answer must arrive as a notification, with the badge showing
 * the session while it listens. Only local fixture pages and synthetic songs.
 */

const CHROMIUM = process.env.CHROMIUM_BIN ?? '/usr/bin/chromium';
const FIREFOX = process.env.FIREFOX_BIN ?? '/usr/bin/firefox';
const MENUS = ['mf-identify-tab', 'mf-identify-link', 'mf-identify-audio', 'mf-identify-video'];

interface Note {
  id: string;
  title: string;
  message: string;
  buttons: string[];
  at: number;
}

type Ext = {
  runtime: { sendMessage(m: unknown): Promise<unknown> };
  storage: { session: { get(k: string): Promise<Record<string, unknown>>; remove(k: string): Promise<void> }; sync: { set(v: unknown): Promise<void> } };
  action: { getBadgeText(d: unknown): Promise<string> };
  tabs: { query(q: unknown): Promise<{ id: number; url?: string }[]>; update(id: number, p: unknown): Promise<unknown>; create(p: unknown): Promise<{ id: number }> };
  contextMenus?: { update(id: string, p: unknown): Promise<void> };
  menus?: { update(id: string, p: unknown): Promise<void> };
};

async function trigger(driver: Page, message: { command?: string; click?: Record<string, unknown>; tabId?: number }): Promise<void> {
  const reply = (await driver.evaluate(async (m) => {
    const g = globalThis as unknown as { browser?: Ext; chrome?: Ext };
    return (g.browser ?? g.chrome)!.runtime.sendMessage({ type: 'mf:e2e.trigger', ...m });
  }, message)) as { ok: boolean; message?: string } | undefined;
  if (!reply?.ok) throw new Error(`trigger failed: ${JSON.stringify(reply)}`);
}

async function notes(driver: Page): Promise<Note[]> {
  return driver.evaluate(async () => {
    const g = globalThis as unknown as { browser?: Ext; chrome?: Ext };
    return ((await (g.browser ?? g.chrome)!.storage.session.get('mfE2eNotifications')).mfE2eNotifications as Note[] | undefined) ?? [];
  });
}

/** The first notification after `since` that `match` accepts. */
async function waitForNote(driver: Page, since: number, match: (note: Note) => boolean, timeoutMs = 60_000): Promise<Note> {
  const deadline = Date.now() + timeoutMs;
  let seen: Note[] = [];
  while (Date.now() < deadline) {
    seen = (await notes(driver)).filter((n) => n.at >= since);
    const found = seen.find(match);
    if (found) return found;
    await sleep(250);
  }
  throw new Error(`no matching notification; saw ${JSON.stringify(seen)}`);
}

async function badge(driver: Page): Promise<string> {
  return driver.evaluate(async () => {
    const g = globalThis as unknown as { browser?: Ext; chrome?: Ext };
    return (g.browser ?? g.chrome)!.action.getBadgeText({});
  });
}

/** Waits until the badge reads `text` at least once (it may clear again quickly). */
async function sawBadge(driver: Page, text: string, timeoutMs = 20_000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if ((await badge(driver).catch(() => '')) === text) return true;
    await sleep(100);
  }
  return false;
}

async function menusInstalled(driver: Page): Promise<string[]> {
  return driver.evaluate(
    async (ids) => {
      const g = globalThis as unknown as { browser?: Ext; chrome?: Ext };
      const api = (g.browser ?? g.chrome)!;
      const menus = api.contextMenus ?? api.menus!;
      const found: string[] = [];
      for (const id of ids) {
        try {
          await menus.update(id, {});
          found.push(id);
        } catch {
          // not created
        }
      }
      return found;
    },
    MENUS,
  );
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

describe.skipIf(!RUNTIME_E2E || !existsSync(CHROMIUM) || !existsSync(E2E_OUTPUT.chromium))('Chromium shortcuts and menus', () => {
  let browser: Browser;
  let server: FixtureServer;
  let id = '';
  let app: Page;

  beforeAll(async () => {
    server = await startFixtureServer();
    browser = await puppeteer.launch({
      executablePath: CHROMIUM,
      headless: HEADLESS,
      pipe: true,
      enableExtensions: [E2E_OUTPUT.chromium],
      userDataDir: tempProfile('triggers-chromium'),
      defaultViewport: null,
      args: ['--no-first-run', '--no-default-browser-check', '--disable-gpu', '--window-size=1280,860', '--autoplay-policy=no-user-gesture-required'],
    });
    const deadline = Date.now() + 180_000;
    while (!id && Date.now() < deadline) {
      id = [...(await browser.extensions()).values()].find((e) => e.name === 'Music Fetch')?.id ?? '';
      if (!id) await sleep(500);
    }
    if (!id) throw new Error('the extension did not load');
    const welcome = (await (await browser.waitForTarget((t) => t.url().startsWith(`chrome-extension://${id}/app.html#/welcome`), { timeout: 60_000 })).asPage())!;
    await welcome.waitForSelector('.welcome', { timeout: 30_000 });
    await welcome.evaluate(() => (globalThis as unknown as { chrome: Ext }).chrome.storage.sync.set({ providerDecision: { provider: 'spotify', skipped: true } }));
    await welcome.close();
    app = await browser.newPage();
    await app.goto(`chrome-extension://${id}/app.html#/library`);
    await app.waitForSelector('.app');
  });

  afterAll(async () => {
    await browser?.close();
    await server?.close();
  });

  async function playingTab(path: string): Promise<Page> {
    const page = await browser.newPage();
    await page.goto(`${server.origin}${path}`);
    await page.waitForFunction(() => {
      const el = document.querySelector('audio, video') as HTMLMediaElement | null;
      return el && !el.paused && el.currentTime > 0.3;
    }, { timeout: 20_000 });
    return page;
  }

  /** The toolbar action on `page`, as the user's click: it invokes the extension on that tab. The popup is closed again. */
  async function invoke(page: Page): Promise<number> {
    await page.bringToFront();
    const extension = (await browser.extensions()).get(id)!;
    await extension.triggerAction(page);
    const popup = (await (await browser.waitForTarget((t) => t.url() === `chrome-extension://${id}/popup.html`, { timeout: 15_000 })).asPage())!;
    await popup.waitForSelector('.listen .disc', { timeout: 20_000 });
    await popup.close();
    await page.bringToFront();
    return tabIdOf(page.url());
  }

  async function tabIdOf(url: string): Promise<number> {
    return app.evaluate(async (u) => (await (globalThis as unknown as { chrome: Ext }).chrome.tabs.query({})).find((t) => t.url === u)!.id, url);
  }

  it('creates its context menus', async () => {
    expect(await menusInstalled(app)).toEqual(MENUS);
  });

  it('identifies a tab from the shortcut: the badge while listening, then a notification', async () => {
    const page = await playingTab('/pages/generic.html?src=/media/song-b.wav&autoplay');
    const tabId = await invoke(page);
    const since = Date.now();
    await trigger(app, { command: 'identify-tab', tabId });
    expect(await sawBadge(app, '•')).toBe(true);
    const note = await waitForNote(app, since, (n) => n.id.startsWith('mf-') && !n.id.startsWith('mf-problem'));
    expect(note.title).toBe('Test Signal B — Music Fetch Test Signals');
    expect(note.buttons).toEqual(['Open in Spotify', 'Pin']);
    await page.screenshot({ path: join(ARTIFACTS, 'triggers-chromium-tab.png') });
    await page.close();
  });

  it('identifies a tab from the page menu', async () => {
    const page = await playingTab('/pages/generic.html?src=/media/song-c.wav&autoplay');
    const tabId = await invoke(page);
    const since = Date.now();
    await trigger(app, { click: { menuItemId: 'mf-identify-tab', pageUrl: page.url() }, tabId });
    const note = await waitForNote(app, since, (n) => n.title.startsWith('Test Signal C'));
    expect(note.buttons).toEqual(['Open in Spotify', 'Pin']);
    await page.close();
  });

  it('identifies a playing video from its menu', async () => {
    const page = await playingTab('/pages/generic.html?src=/media/song-d.wav&autoplay');
    const tabId = await invoke(page);
    const since = Date.now();
    await trigger(app, { click: { menuItemId: 'mf-identify-audio', srcUrl: `${server.origin}/media/song-d.wav`, pageUrl: page.url() }, tabId });
    expect((await waitForNote(app, since, (n) => n.title.startsWith('Test Signal D'))).title).toContain('Music Fetch Test Signals');
    await page.close();
  });

  it('answers a catalog link from its menu', async () => {
    const since = Date.now();
    await trigger(app, { click: { menuItemId: 'mf-identify-link', linkUrl: 'https://open.spotify.com/track/4PTG3Z6ehGkBFwjybzWkR8' } });
    const note = await waitForNote(app, since, (n) => n.title.includes(' — '));
    expect(note.buttons).toEqual(['Open in Spotify', 'Pin']);
  });

  it('opens a video link in the app’s Open and analyze flow', async () => {
    const link = 'https://www.youtube.com/watch?v=mf-e2e';
    await trigger(app, { click: { menuItemId: 'mf-identify-link', linkUrl: link } });
    await app.waitForFunction((u) => decodeURIComponent(location.hash).includes(u), { timeout: 15_000 }, link);
    expect(decodeURIComponent(await app.evaluate(() => location.hash))).toContain('#/open?url=');
    await app.goto(`chrome-extension://${id}/app.html#/library`);
  });

  it('says why when the extension was never invoked on the tab', async () => {
    const page = await playingTab('/pages/generic.html?src=/media/song-e.wav&autoplay');
    const tabId = await tabIdOf(page.url());
    const since = Date.now();
    await trigger(app, { command: 'identify-tab', tabId });
    const note = await waitForNote(app, since, (n) => n.id.startsWith('mf-problem-'));
    expect(note).toMatchObject({ title: 'Music Fetch', message: 'Click the Music Fetch button on this tab, then try again.' });
    await page.close();
  });

  it('hums from the shortcut: without a panel gesture the app tab starts humming', async () => {
    await app.evaluate(() => localStorage.setItem('mf.mock.next', 'hold'));
    const page = await playingTab('/pages/generic.html?src=/media/song-f.wav&autoplay');
    const tabId = await tabIdOf(page.url());
    await trigger(app, { command: 'hum', tabId });
    await app.waitForFunction(() => location.hash === '#/listen', { timeout: 15_000 });
    await app.waitForFunction(() => document.querySelector('.sources [aria-checked="true"]')?.textContent?.trim() === 'Hum' && document.querySelector('.listen.live'), {
      timeout: 30_000,
      polling: 200,
    });
    await app.screenshot({ path: join(ARTIFACTS, 'triggers-chromium-hum.png') });
    await app.click('.listen .disc');
    await page.close();
  });
});

describe.skipIf(!RUNTIME_E2E || !existsSync(FIREFOX) || !existsSync(E2E_OUTPUT.firefox))('Firefox shortcuts and menus', () => {
  let runner: { exit(): Promise<void> } | null = null;
  let browser: Browser;
  let server: FixtureServer;
  let tab: Page;

  beforeAll(async () => {
    server = await startFixtureServer();
    const port = await freePort();
    process.env.MOZ_REMOTE_ALLOW_SYSTEM_ACCESS = '1';
    runner = await webExt.cmd.run(
      {
        sourceDir: E2E_OUTPUT.firefox,
        target: ['firefox-desktop'],
        firefox: FIREFOX,
        args: [`--remote-debugging-port=${port}`, ...(HEADLESS ? ['-headless'] : [])],
        pref: {
          'browser.shell.checkDefaultBrowser': false,
          'datareporting.policy.dataSubmissionEnabled': false,
          'media.autoplay.default': 0,
          'media.autoplay.blocking_policy': 0,
        },
        startUrl: [`${server.origin}/pages/generic.html?src=/media/song-a.wav&autoplay`],
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
    await server?.close();
  });

  /** Make the tab showing `prefix` the active one (what "This tab" means); its id. */
  async function activate(prefix: string): Promise<number> {
    return tab.evaluate(async (p) => {
      const api = (globalThis as unknown as { browser: Ext }).browser;
      const found = (await api.tabs.query({})).find((t) => t.url?.startsWith(p));
      if (!found) throw new Error(`no tab ${p}`);
      await api.tabs.update(found.id, { active: true });
      return found.id;
    }, prefix);
  }

  it('creates its menus', async () => {
    expect(await menusInstalled(tab)).toEqual(MENUS);
  });

  it('identifies a tab from the shortcut through element capture', async () => {
    const tabId = await activate(`${server.origin}/pages/generic.html`);
    await sleep(1000);
    const since = Date.now();
    await trigger(tab, { command: 'identify-tab', tabId });
    expect(await sawBadge(tab, '•')).toBe(true);
    const note = await waitForNote(tab, since, (n) => n.id.startsWith('mf-') && !n.id.startsWith('mf-problem'));
    expect(note.title).toBe('Test Signal A — Music Fetch Test Signals');
  });

  it('identifies a playing audio element from its menu', async () => {
    const tabId = await activate(`${server.origin}/pages/generic.html`);
    const since = Date.now();
    await trigger(tab, { click: { menuItemId: 'mf-identify-audio', srcUrl: `${server.origin}/media/song-a.wav` }, tabId });
    expect((await waitForNote(tab, since, (n) => n.title.startsWith('Test Signal A'))).buttons).toBeDefined();
  });

  it('answers a catalog link from its menu', async () => {
    const since = Date.now();
    await trigger(tab, { click: { menuItemId: 'mf-identify-link', linkUrl: 'https://open.spotify.com/track/4PTG3Z6ehGkBFwjybzWkR8' } });
    expect((await waitForNote(tab, since, (n) => n.title.includes(' — '))).id).toMatch(/^mf-/);
  });

  it('says why when nothing plays in the tab', async () => {
    const blank = await tab.evaluate(async (u) => (await (globalThis as unknown as { browser: Ext }).browser.tabs.create({ url: u, active: true })).id, `${server.origin}/blank`);
    await sleep(1000);
    const since = Date.now();
    await trigger(tab, { command: 'identify-tab', tabId: blank });
    const note = await waitForNote(tab, since, (n) => n.id.startsWith('mf-problem-'));
    expect(note.message).toBe('Nothing is playing in this tab');
  });

  it('hums from the shortcut: without a sidebar gesture the app tab starts humming', async () => {
    await tab.evaluate(() => localStorage.setItem('mf.mock.next', 'hold'));
    const tabId = await activate(`${server.origin}/pages/generic.html`);
    await trigger(tab, { command: 'hum', tabId });
    // openApp reuses this extension tab: it moves to #/listen and starts humming.
    await tab.waitForFunction(() => location.hash === '#/listen' && document.querySelector('.sources [aria-checked="true"]')?.textContent?.trim() === 'Hum', {
      timeout: 30_000,
      polling: 200,
    });
    await tab.waitForFunction(() => document.querySelector('.listen.live'), { timeout: 30_000, polling: 200 });
    await tab.evaluate(() => (document.querySelector('.listen .disc') as HTMLElement).click());
  });
});
