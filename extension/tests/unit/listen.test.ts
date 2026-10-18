import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fakeBrowser } from 'wxt/testing/fake-browser';
import { MockEngine } from '@/src/engine/mock/engine';
import { tm } from '@/src/i18n';
import { settings } from '@/src/settings/settings';
import { ListenController } from '@/src/ui/listen.svelte';

function controller(directive: string | null = null) {
  let next = directive;
  const engine = new MockEngine({
    timeScale: 0,
    directive: () => {
      const value = next;
      next = null;
      return value;
    },
  });
  return { engine, listen: new ListenController(engine, { recentLimit: 5 }) };
}

describe('the listen controller', () => {
  beforeEach(() => fakeBrowser.reset());

  it('starts with the remembered source and follows a session to its result', async () => {
    await settings.lastSource.setValue('Microphone');
    const { listen } = controller();
    await listen.start(null);
    expect(listen.source).toBe('Microphone');
    await listen.toggle();
    expect(listen.stage.sessionId).not.toBeNull();
    await vi.waitFor(() => expect(listen.stage.phase).toBe('matched'), { timeout: 2000 });
    expect(listen.stage.track?.title).toBeTruthy();
    await vi.waitFor(() => expect(listen.recent[0]?.status).toBe('Matched'));
    listen.stop();
  });

  it('prefers the audible tab, and remembers a chosen source', async () => {
    const { listen } = controller();
    await listen.start('Tab');
    expect(listen.source).toBe('Tab');
    await listen.chooseSource('Hum');
    expect(await settings.lastSource.getValue()).toBe('Hum');
  });

  it('stops capture early with the button', async () => {
    const { engine, listen } = controller();
    await listen.start('Tab');
    await listen.toggle();
    await vi.waitFor(() => expect(listen.stage.phase).toBe('listening'));
    await listen.toggle(); // Stop: identify what was heard
    await vi.waitFor(() => expect(listen.stage.phase).toBe('matched'), { timeout: 2000 });
    expect(engine.eventsOf(listen.stage.sessionId!).filter((e) => typeof e === 'object' && 'Level' in e).length).toBeLessThan(10);
  });

  it('turns rejected input into a state line', async () => {
    const { listen } = controller();
    await listen.start('Tab');
    await listen.analyze('ftp://example.com/a');
    expect(listen.stage.phase).toBe('failed');
    expect(tm(listen.stage.line)).toBe("This link isn't supported.");
    listen.reset();
    expect(listen.stage.phase).toBe('idle');
  });

  it('shows how much of a playlist was identified', async () => {
    const { listen } = controller();
    await listen.start('Tab');
    await listen.analyze('https://open.spotify.com/playlist/abc');
    await vi.waitFor(() => expect(listen.stage.phase).toBe('completed'), { timeout: 2000 });
    expect(tm(listen.stage.line)).toBe('5 of 5 identified');
  });

  it('picks up a session that is still running when the popup reopens', async () => {
    const { engine, listen } = controller();
    const id = await engine.startListening('Microphone', 'Hum', { auto_stop: true });
    await listen.start(null);
    expect(listen.stage.sessionId).toBe(id);
    expect(listen.stage.source).toBe('Hum');
    await listen.cancel();
    expect(listen.stage.phase).toBe('idle');
  });
});
