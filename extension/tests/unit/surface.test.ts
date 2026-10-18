import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fakeBrowser } from 'wxt/testing/fake-browser';
import { tm } from '@/src/i18n';
import { recoveryActions } from '@/src/models/stage';
import { settings, useMemorySettings } from '@/src/settings/settings';
import { DemoEngine, demoPlatform } from '@/src/ui/demo/runtime';
import { DECIDED, demoState, type DemoState } from '@/src/ui/demo/states';
import { isMediaPageLink, ListenSurface } from '@/src/ui/surface.svelte';

async function surface(id: string, patch: Partial<DemoState> = {}) {
  const state = { ...demoState(id)!, ...patch };
  useMemorySettings(state.settings ?? DECIDED);
  const engine = new DemoEngine({ ...state, live: undefined });
  const view = new ListenSurface(engine, demoPlatform(state, null));
  await view.start();
  return { engine, view };
}

describe('the stage around the page', () => {
  beforeEach(() => fakeBrowser.reset());

  it('tells media page links from streaming links', () => {
    expect(isMediaPageLink('https://www.youtube.com/watch?v=x')).toBe(true);
    expect(isMediaPageLink('https://youtu.be/x')).toBe(true);
    expect(isMediaPageLink('https://music.youtube.com/watch?v=x')).toBe(false);
    expect(isMediaPageLink('https://www.youtube.com/playlist?list=PL1')).toBe(false);
    expect(isMediaPageLink('https://open.spotify.com/track/1')).toBe(false);
    expect(isMediaPageLink('https://soundcloud.com/a/sets/b')).toBe(false);
  });

  it('offers to open a pasted video link on its page', async () => {
    const { view } = await surface('popup-plays-on-page');
    const open = vi.spyOn(view.platform, 'openUrl');
    view.submit('https://www.youtube.com/watch?v=demo-video');
    expect(tm(view.listen.stage.line)).toBe('This link plays on its page.');
    expect(recoveryActions(view.listen.stage)).toEqual([{ action: 'openAndAnalyze', primary: true }]);
    await view.openAndAnalyze(view.listen.stage.input!);
    expect(open).toHaveBeenCalledWith('https://www.youtube.com/watch?v=demo-video');
    expect(view.listen.stage.phase).toBe('idle');
  });

  it('asks before reading a site the engine may not read, then analyzes', async () => {
    const { view } = await surface('popup-allow-host');
    await view.analyzePage();
    expect(tm(view.listen.stage.line)).toBe('Allow Music Fetch to read radio.example.org?');
    await view.allowHost('https://radio.example.org/live');
    await vi.waitFor(() => expect(view.listen.stage.kind).toBe('Link'));
    expect(view.listen.stage.line.key).not.toBe('page.allowHost');
  });

  it('asks to check a short video, and remembers "Don’t ask again"', async () => {
    const { view } = await surface('popup-check-video');
    await view.analyzePage();
    expect(view.ask?.kind).toBe('short');
    await view.answerAsk('listen', true);
    expect(view.ask).toBeNull();
    expect((await settings.analyzeChoice.getValue())['YouTube:short']).toBe('listen');
    expect(view.listen.source).toBe('Tab');
    await view.listen.cancel();
    await view.analyzePage();
    expect(view.ask).toBeNull();
  });

  it('asks whole or quick for a long video, and analyzes a whole one in the background', async () => {
    const { view } = await surface('popup-ask');
    await view.analyzePage();
    expect(view.ask?.kind).toBe('long');
    await view.answerAsk('whole', false);
    expect(view.listen.stage.phase).toBe('idle');
    await vi.waitFor(() => expect(view.listen.background?.status).toBe('Running'));
  });

  it('says a streaming page answered from its own metadata', async () => {
    const { view } = await surface('popup-from-page');
    await view.analyzePage();
    await vi.waitFor(() => expect(view.listen.stage.phase).toBe('matched'), { timeout: 3000 });
    expect(view.resultFromPage).toBe(true);
    expect(view.resultContext).toBe('From the page · open.spotify.com');
  });
});

describe('Firefox for Android', () => {
  beforeEach(() => fakeBrowser.reset());

  it('hands Hum and Microphone to the app and closes the popup', async () => {
    const state = demoState('mobile-hum')!;
    useMemorySettings(state.settings!);
    const engine = new DemoEngine(state);
    const platform = demoPlatform(state, null);
    const open = vi.spyOn(platform, 'openUrl').mockResolvedValue();
    const close = vi.spyOn(platform, 'close');
    const start = vi.spyOn(engine, 'startListening');
    const melody = vi.spyOn(engine, 'melodyIndexStatus');
    const view = new ListenSurface(engine, platform, { closeOnHandOff: true });
    await view.start();
    expect(view.listen.source).toBe('Hum');
    expect(view.listen.inApp).toBe(true);
    await view.listen.toggle();
    expect(open).toHaveBeenCalledWith(expect.stringMatching(/^intent:\/\/listen\?source=hum#Intent;scheme=musicfetch;/));
    expect(close).toHaveBeenCalledTimes(1);
    // Not the melody library prompt, not a session: the app does the listening.
    expect(start).not.toHaveBeenCalled();
    expect(melody).not.toHaveBeenCalled();
    expect(view.listen.stage.phase).toBe('idle');
    await view.listen.listenWith('Microphone');
    expect(open).toHaveBeenLastCalledWith(expect.stringMatching(/^intent:\/\/listen\?source=mic#/));
  });

  it('keeps This tab in the extension', async () => {
    const state = demoState('mobile-idle')!;
    useMemorySettings(state.settings!);
    const engine = new DemoEngine(state);
    const platform = demoPlatform(state, null);
    const open = vi.spyOn(platform, 'openUrl');
    const view = new ListenSurface(engine, platform, { closeOnHandOff: true });
    await view.start();
    expect(view.listen.source).toBe('Tab');
    expect(view.listen.inApp).toBe(false);
    await view.listen.toggle();
    expect(open).not.toHaveBeenCalled();
    expect(view.listen.stage.sessionId).not.toBeNull();
    await view.listen.cancel();
  });

  it('listens in the extension on desktop Firefox', async () => {
    const { view } = await surface('popup-hum', { live: undefined });
    expect(view.listen.inApp).toBe(false);
  });
});
