import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fakeBrowser } from 'wxt/testing/fake-browser';
import { MockEngine } from '@/src/engine/mock/engine';
import { startNotifier, type NotifierApi } from '@/src/notify/notifier';
import { settings, skipProviderPanel } from '@/src/settings/settings';

function setup(hasUi = false) {
  let next: string | null = null;
  const engine = new MockEngine({ timeScale: 0, directive: () => ((value) => ((next = null), value))(next) });
  const api = { setBadge: vi.fn(), notify: vi.fn(), clear: vi.fn() } satisfies NotifierApi;
  const hooks = { hasUi: () => hasUi, openSession: vi.fn(), openUrl: vi.fn(), tryAgain: vi.fn() };
  const notifier = startNotifier(engine, api, hooks);
  return { engine, api, hooks, notifier, force: (d: string) => (next = d) };
}

describe('the background notifier', () => {
  beforeEach(async () => {
    fakeBrowser.reset();
    await skipProviderPanel();
  });

  it('shows a dot while listening, then a notification with Open in and Pin', async () => {
    const { engine, api, hooks, notifier } = setup();
    const id = await engine.startListening('Tab', 'Auto', { auto_stop: true });
    expect(api.setBadge).toHaveBeenCalledWith('•', expect.any(String));
    await vi.waitFor(() => expect(api.notify).toHaveBeenCalledOnce());
    const [noteId, content] = api.notify.mock.calls[0]!;
    expect(content.buttons).toEqual(['Open in Spotify', 'Pin']);
    expect(api.setBadge).toHaveBeenLastCalledWith('', expect.any(String));
    await notifier.clicked(noteId, 0);
    expect(hooks.openUrl).toHaveBeenCalledWith(expect.stringContaining('spotify.com'));
    void id;
  });

  it('stays quiet while a surface shows the session, or when turned off', async () => {
    const shown = setup(true);
    await shown.engine.startListening('Tab', 'Auto', { auto_stop: true });
    await new Promise((done) => setTimeout(done, 30));
    expect(shown.api.notify).not.toHaveBeenCalled();

    await settings.notifyInBackground.setValue(false);
    const off = setup(false);
    await off.engine.startListening('Tab', 'Auto', { auto_stop: true });
    await new Promise((done) => setTimeout(done, 30));
    expect(off.api.notify).not.toHaveBeenCalled();
  });

  it('offers Try again after no match', async () => {
    const { engine, api, hooks, notifier, force } = setup();
    force('nomatch');
    const id = await engine.startListening('Microphone', 'Auto', { auto_stop: true });
    await vi.waitFor(() => expect(api.notify).toHaveBeenCalledOnce());
    await notifier.clicked(api.notify.mock.calls[0]![0], 0);
    expect(hooks.tryAgain).toHaveBeenCalledWith(id);
  });
});
