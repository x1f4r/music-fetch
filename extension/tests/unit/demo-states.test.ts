import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fakeBrowser } from 'wxt/testing/fake-browser';
import { checkEventContract } from '@/src/engine/events';
import type { SessionEvent } from '@/src/engine/types';
import { settings, useMemorySettings } from '@/src/settings/settings';
import { parseRoute } from '@/src/ui/router.svelte';
import { DEMO_LIVE_ID, DemoEngine, demoPlatform } from '@/src/ui/demo/runtime';
import { DEMO_STATES, demoState, demoUrl } from '@/src/ui/demo/states';

describe('demo states', () => {
  beforeEach(() => fakeBrowser.reset());

  it('have unique ids and open on a real page and route', () => {
    const ids = DEMO_STATES.map((s) => s.id);
    expect(new Set(ids).size).toBe(ids.length);
    for (const state of DEMO_STATES) {
      expect(demoUrl(state, { theme: 'dark' })).toMatch(/^(popup|sidepanel|app)\.html\?demo=[a-z0-9-]+&theme=dark/);
      if (state.route) expect(parseRoute(`#${state.route}`).name).not.toBe(state.route === '/listen' ? 'nothing' : 'listen');
    }
  });

  it('replay a live session that keeps the core’s event contract', async () => {
    for (const state of DEMO_STATES.filter((s) => s.live?.events.some((e) => typeof e === 'object' && 'Finished' in e))) {
      const engine = new DemoEngine(state);
      const events: SessionEvent[] = [];
      engine.subscribe({ sessionEvent: (id, event) => id === DEMO_LIVE_ID && events.push(event) });
      const [active] = await engine.activeSessions();
      expect(active?.id).toBe(DEMO_LIVE_ID);
      await vi.waitFor(() => expect(events.length).toBeGreaterThan(1));
      expect(checkEventContract(events), state.id).toEqual([]);
    }
  });

  it('finish a held live session when the user stops it', async () => {
    const engine = new DemoEngine(demoState('popup-listening')!);
    const events: SessionEvent[] = [];
    engine.subscribe({ sessionEvent: (_id, event) => events.push(event) });
    await engine.activeSessions();
    await vi.waitFor(() => expect(events.length).toBeGreaterThan(1));
    await engine.finishSession(DEMO_LIVE_ID);
    expect(checkEventContract(events)).toEqual([]);
    expect(events.at(-1)).toMatchObject({ Finished: { outcome: { Matched: {} } } });
  });

  it('report a running mix’s progress and capture state', async () => {
    const engine = new DemoEngine(demoState('panel-mix')!);
    const capture = vi.fn();
    engine.subscribe({ captureStatus: capture });
    await engine.session('demo-mix-running');
    await vi.waitFor(() => expect(capture).toHaveBeenCalledWith('demo-mix-running', { state: 'capturing', rate: 4, muted: true }));
    expect((await engine.session('demo-mix-running'))!.entry.status).toBe('Running');
  });

  it('keep settings in memory, away from the browser’s storage', async () => {
    useMemorySettings({ 'sync:providerDecision': { provider: 'tidal', skipped: false } });
    try {
      expect((await settings.providerDecision.getValue())?.provider).toBe('tidal');
      const seen = vi.fn();
      const stop = settings.autoStop.watch(seen);
      await settings.autoStop.setValue(false);
      expect(seen).toHaveBeenCalledWith(false);
      stop();
      expect(await fakeBrowser.storage.sync.get()).toEqual({});
    } finally {
      // Back to the browser's storage for the other tests in this worker.
      useMemorySettings(null);
    }
  });

  it('stand in for the browser with the state’s tab and capabilities', async () => {
    const android = demoPlatform(demoState('mobile-idle')!, null);
    expect(android.capabilities.android).toBe(true);
    expect(android.capabilities.panel).toBeNull();
    const firefox = demoPlatform(demoState('sidebar-narrow')!, null);
    expect(firefox.capabilities.panel).toBe('sidebar');
    expect((await demoPlatform(demoState('panel-ask')!, null).pageFacts({ id: 1, windowId: 1, url: '', title: '', audible: true }))?.media[0]?.duration).toBe(5412);
  });
});
