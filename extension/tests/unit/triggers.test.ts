import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fakeBrowser } from 'wxt/testing/fake-browser';
import { EngineError } from '@/src/engine/client';
import { MockEngine } from '@/src/engine/mock/engine';
import { startNotifier, type NotifierApi } from '@/src/notify/notifier';
import { buildManifest } from '@/src/platform/manifest';
import { settings, skipProviderPanel } from '@/src/settings/settings';
import { installMenus, MENU, menuItems, type MenuItem } from '@/src/triggers/menus';
import { followPendingStart, requestStart, startFromShortcut, takePendingStart } from '@/src/triggers/pending';
import { idleStage } from '@/src/models/stage';
import { problemText, Triggers, type TriggerDeps } from '@/src/triggers/triggers';

function setup(overrides: Partial<TriggerDeps> = {}) {
  const deps = {
    engine: { startListening: vi.fn(async () => 'live-1'), analyze: vi.fn(async () => 'link-1') },
    expect: vi.fn(),
    problem: vi.fn(),
    openApp: vi.fn(),
    requestOrigin: vi.fn(async () => true),
    openPanel: vi.fn(() => Promise.resolve()),
    requestStart: vi.fn(async () => undefined),
    ...overrides,
  } satisfies TriggerDeps;
  return { deps, triggers: new Triggers(deps) };
}

const PAGE_TAB = { id: 7, windowId: 3, url: 'https://www.youtube.com/watch?v=x', title: 'A video', audible: false };

describe('shortcuts and context menus', () => {
  beforeEach(() => fakeBrowser.reset());

  it('identify this tab: a live session the notifier answers', async () => {
    const { deps, triggers } = setup();
    await triggers.command('identify-tab', PAGE_TAB);
    expect(deps.engine.startListening).toHaveBeenCalledOnce();
    expect(deps.expect).toHaveBeenCalledWith('live-1');
    await triggers.menu({ menuItemId: MENU.tab }, PAGE_TAB);
    expect(deps.engine.startListening).toHaveBeenCalledTimes(2);
  });

  it('says why a capture could not start', async () => {
    const cases: [EngineError, string][] = [
      [new EngineError('CaptureNotAllowed', 'x', 'CantCapture'), "Can't listen to this page"],
      [new EngineError('CaptureNotAllowed', 'x', 'NeedsInvocation'), 'Click the Music Fetch button on this tab, then try again.'],
      [new EngineError('NothingPlaying', 'x', 'NothingPlaying'), 'Nothing is playing in this tab'],
      [new EngineError('CaptureBlocked', 'x', 'CaptureBlocked'), 'This tab blocks audio capture. Use the microphone instead.'],
      [new EngineError('CaptureBusy', 'x', 'TabBusy'), 'Another capture is using this tab'],
      [new EngineError('Disconnected', 'x'), "Music Fetch couldn't start. Close this and try again."],
    ];
    for (const [error, text] of cases) {
      const { deps, triggers } = setup({ engine: { startListening: vi.fn(async () => Promise.reject(error)), analyze: vi.fn() } });
      await triggers.command('identify-tab', PAGE_TAB);
      expect(deps.problem).toHaveBeenCalledWith(text);
      expect(deps.expect).not.toHaveBeenCalled();
      expect(problemText(error)).toBe(text);
    }
  });

  it('analyzes catalog links headlessly, without asking for a host', async () => {
    const { deps, triggers } = setup();
    await triggers.menu({ menuItemId: MENU.link, linkUrl: 'https://open.spotify.com/track/abc' }, PAGE_TAB);
    expect(deps.requestOrigin).not.toHaveBeenCalled();
    expect(deps.engine.analyze).toHaveBeenCalledWith('https://open.spotify.com/track/abc', null);
    expect(deps.expect).toHaveBeenCalledWith('link-1');
  });

  it('asks for another site inside the click, before anything is awaited', async () => {
    const { deps, triggers } = setup();
    const done = triggers.menu({ menuItemId: MENU.link, linkUrl: 'https://radio.example.org/show/42' }, PAGE_TAB);
    expect(deps.requestOrigin).toHaveBeenCalledWith('https://radio.example.org/*');
    await done;
    expect(deps.engine.analyze).toHaveBeenCalledWith('https://radio.example.org/show/42', null);

    const refused = setup({ requestOrigin: vi.fn(async () => false) });
    await refused.triggers.menu({ menuItemId: MENU.link, linkUrl: 'https://radio.example.org/show/42' }, PAGE_TAB);
    expect(refused.deps.engine.analyze).not.toHaveBeenCalled();
    expect(refused.deps.problem).toHaveBeenCalledWith("Music Fetch can't read radio.example.org without your permission.");
  });

  it('opens a video link in the app, where its sound can play', async () => {
    const { deps, triggers } = setup();
    await triggers.menu({ menuItemId: MENU.link, linkUrl: 'https://youtu.be/abc' }, PAGE_TAB);
    expect(deps.openApp).toHaveBeenCalledWith(`/open?url=${encodeURIComponent('https://youtu.be/abc')}`);
    expect(deps.engine.analyze).not.toHaveBeenCalled();
  });

  it('identifies audio and video: the playing tab, else the element’s file, else asks to play it', async () => {
    const audible = setup();
    await audible.triggers.menu({ menuItemId: MENU.video, srcUrl: 'blob:https://www.youtube.com/1' }, { ...PAGE_TAB, audible: true });
    expect(audible.deps.engine.startListening).toHaveBeenCalledOnce();

    const file = setup();
    const podcast = { id: 9, windowId: 3, url: 'https://podcast.example.org/ep/1', title: 'Episode 1', audible: false };
    await file.triggers.menu({ menuItemId: MENU.audio, srcUrl: 'https://cdn.example.org/ep1.mp3' }, podcast);
    expect(file.deps.requestOrigin).toHaveBeenCalledWith('https://cdn.example.org/*');
    expect(file.deps.engine.analyze).toHaveBeenCalledWith('https://cdn.example.org/ep1.mp3', { tab_id: 9, url: podcast.url, title: 'Episode 1', facts: null });

    const paused = setup();
    await paused.triggers.menu({ menuItemId: MENU.video, srcUrl: 'blob:https://www.youtube.com/1' }, PAGE_TAB);
    expect(paused.deps.problem).toHaveBeenCalledWith('Play it, then try again.');
  });

  it('hum opens the panel inside the gesture and leaves the request for it', async () => {
    const { deps, triggers } = setup();
    const done = triggers.command('hum', PAGE_TAB);
    expect(deps.openPanel).toHaveBeenCalledWith(3);
    await done;
    expect(deps.requestStart).toHaveBeenCalledWith('Hum');
    expect(deps.openApp).not.toHaveBeenCalled();

    const none = setup({ openPanel: vi.fn(() => null) });
    await none.triggers.command('hum', PAGE_TAB);
    expect(none.deps.openApp).toHaveBeenCalledWith('/listen');
    const refused = setup({ openPanel: vi.fn(() => Promise.reject(new Error('no gesture'))) });
    await refused.triggers.command('hum', PAGE_TAB);
    expect(refused.deps.openApp).toHaveBeenCalledWith('/listen');
  });

  it('creates the menus after removing old ones, on web pages only', async () => {
    const created: MenuItem[] = [];
    const order: string[] = [];
    const api = {
      removeAll: vi.fn(async () => void order.push('removeAll')),
      create: vi.fn((item: MenuItem, done?: () => void) => {
        order.push(item.id);
        created.push(item);
        done?.();
      }),
    };
    expect(await installMenus(api)).toBe(4);
    expect(order[0]).toBe('removeAll');
    expect(created.map((i) => i.title)).toEqual(['Identify music in this tab', 'Identify this link', 'Identify this audio', 'Identify this video']);
    expect(menuItems().every((i) => i.documentUrlPatterns?.every((p) => p.startsWith('http')))).toBe(true);
    expect(await installMenus(undefined)).toBe(0);
  });

  it('declares the commands for both browsers', () => {
    for (const browser of ['chrome', 'firefox']) {
      const commands = buildManifest({ browser, mode: 'production' }).commands as Record<string, { suggested_key?: { default: string } }>;
      expect(Object.keys(commands).sort()).toEqual(['_execute_action', 'hum', 'identify-tab']);
      expect(commands['identify-tab']!.suggested_key!.default).toBe('Alt+Shift+I');
    }
  });
});

describe('the hum request', () => {
  beforeEach(() => fakeBrowser.reset());

  it('is taken once, and only while fresh', async () => {
    await requestStart('Hum', 1000);
    expect(await takePendingStart(2000)).toEqual({ source: 'Hum', at: 1000 });
    expect(await takePendingStart(2000)).toBeNull();
    await requestStart('Hum', 1000);
    expect(await takePendingStart(60_000)).toBeNull();
  });

  it('starts on the first press and stops on the second', () => {
    const listen = { stage: idleStage('Tab'), toggle: vi.fn(async () => undefined), listenWith: vi.fn(async () => undefined) };
    startFromShortcut(listen, 'Hum');
    expect(listen.listenWith).toHaveBeenCalledWith('Hum');
    startFromShortcut({ ...listen, stage: { ...idleStage('Hum'), phase: 'listening', sessionId: 's' } }, 'Hum');
    expect(listen.toggle).toHaveBeenCalledOnce();
  });

  it('reaches a panel that is already open', async () => {
    const start = vi.fn();
    const stop = followPendingStart(start);
    await new Promise((done) => setTimeout(done, 10));
    expect(start).not.toHaveBeenCalled();
    await requestStart('Hum');
    await vi.waitFor(() => expect(start).toHaveBeenCalledWith('Hum'));
    stop();
  });
});

describe('notifications for triggers', () => {
  beforeEach(async () => {
    fakeBrowser.reset();
    await skipProviderPanel();
  });

  function notifier(hasUi: boolean) {
    let next: string | null = null;
    const engine = new MockEngine({ timeScale: 0, directive: () => ((value) => ((next = null), value))(next) });
    const api = { setBadge: vi.fn(), notify: vi.fn(), clear: vi.fn() } satisfies NotifierApi;
    const hooks = { hasUi: () => hasUi, openSession: vi.fn(), openUrl: vi.fn(), tryAgain: vi.fn(), openListen: vi.fn() };
    return { engine, api, hooks, notifier: startNotifier(engine, api, hooks), force: (d: string) => (next = d) };
  }

  it('answers a trigger even with a surface open and notifications off', async () => {
    await settings.notifyInBackground.setValue(false);
    const { engine, api, notifier: n } = notifier(true);
    const id = await engine.startListening('Tab', 'Auto', { auto_stop: true });
    n.expect(id);
    await vi.waitFor(() => expect(api.notify).toHaveBeenCalledOnce());
    expect(api.notify.mock.calls[0]![1].buttons).toEqual(['Open in Spotify', 'Pin']);
  });

  it('answers a session that ended before the trigger claimed it', async () => {
    const { engine, api, notifier: n } = notifier(true);
    const id = await engine.analyze('https://open.spotify.com/track/abc', 'Auto', null);
    await new Promise((done) => setTimeout(done, 50));
    expect(api.notify).not.toHaveBeenCalled();
    n.expect(id);
    await vi.waitFor(() => expect(api.notify).toHaveBeenCalledOnce());
    // A link to one song reads like a match, with Open in and Pin.
    const [, content] = api.notify.mock.calls[0]!;
    expect(content.title).toMatch(/ — /);
    expect(content.buttons).toEqual(['Open in Spotify', 'Pin']);
  });

  it('turns a failed trigger session into a problem that opens Music Fetch', async () => {
    const { engine, api, hooks, notifier: n, force } = notifier(false);
    force('fail');
    const id = await engine.startListening('Tab', 'Auto', { auto_stop: true });
    n.expect(id);
    await vi.waitFor(() => expect(api.notify).toHaveBeenCalledOnce());
    const [noteId, content] = api.notify.mock.calls[0]!;
    expect(noteId).toMatch(/^mf-problem-/);
    expect(content.title).toBe('Music Fetch');
    await n.clicked(noteId);
    expect(hooks.openListen).toHaveBeenCalledOnce();
    expect(api.clear).toHaveBeenCalledWith(noteId);
  });

  it('shows a problem straight away', async () => {
    const { api, notifier: n } = notifier(true);
    n.problem("Can't listen to this page");
    expect(api.notify).toHaveBeenCalledWith(expect.stringMatching(/^mf-problem-/), { title: 'Music Fetch', message: "Can't listen to this page", buttons: [] });
  });
});
