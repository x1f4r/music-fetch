import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fakeBrowser } from 'wxt/testing/fake-browser';
import { BackgroundNotifications } from '@/src/engine/host-activity';
import { MockEngine } from '@/src/engine/mock/engine';
import type { HostActivity } from '@/src/messaging/protocol';
import type { SessionEvent } from '@/src/engine/types';
import { skipProviderPanel } from '@/src/settings/settings';

/** Activity reports as the host sends them, from a mock engine's events. */
function hostLike(surfaces: () => number) {
  const engine = new MockEngine({ timeScale: 0 });
  const api = { setBadge: vi.fn(), notify: vi.fn(), clear: vi.fn() };
  const notifications = new BackgroundNotifications({ session: (id) => engine.session(id), setPinned: (id, p) => engine.setPinned(id, p) }, api);
  engine.subscribe({
    sessionEvent: (sessionId: string, event: SessionEvent) => {
      if (typeof event === 'object' && 'Level' in event) return;
      const activity: HostActivity = { active: [], ended: null, surfaces: surfaces(), events: [{ sessionId, event }] };
      notifications.report(activity);
    },
  });
  return { engine, api };
}

describe('the background notifications', () => {
  beforeEach(async () => {
    fakeBrowser.reset();
    await skipProviderPanel();
  });

  it('badge while a session runs and notify when it ends with no page open', async () => {
    const { engine, api } = hostLike(() => 0);
    await engine.startListening('Tab', 'Auto', { auto_stop: true });
    expect(api.setBadge).toHaveBeenCalledWith('•', expect.any(String));
    await vi.waitFor(() => expect(api.notify).toHaveBeenCalledOnce());
    expect(api.notify.mock.calls[0]![1].buttons).toEqual(['Open in Spotify', 'Pin']);
    expect(api.setBadge).toHaveBeenLastCalledWith('', expect.any(String));
  });

  it('stay quiet while a page shows the session', async () => {
    const { engine, api } = hostLike(() => 1);
    await engine.startListening('Tab', 'Auto', { auto_stop: true });
    await new Promise((done) => setTimeout(done, 50));
    expect(api.notify).not.toHaveBeenCalled();
  });
});
