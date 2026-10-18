import { afterEach, describe, expect, it, vi } from 'vitest';
import { EngineError } from '@/src/engine/client';
import { CaptureError, EngineHost, resolveLowPower, type HostApi } from '@/src/engine/host-core';
import { memoryKv } from '@/src/engine/host-idb';
import { renderSong, SYNTHETIC_SONGS } from '@/src/engine/host-mock-audio';
import { SessionRegistry } from '@/src/engine/host-registry';
import { RemoteEngineClient } from '@/src/engine/remote';
import { RpcClient } from '@/src/messaging/bus';
import { ENGINE_PORT, HOST_PORT } from '@/src/messaging/protocol';
import type { SessionEvent } from '@/src/engine/types';
import { fakePlatform, fakeRuntime, mockFactory, NO_CAPTURE } from './support/host-fakes';

const hosts: EngineHost[] = [];

function setup(options: { idleMs?: number; platform?: Parameters<typeof fakePlatform>[1] } = {}) {
  const kv = memoryKv();
  const registryKv = memoryKv();
  const platform = fakePlatform(mockFactory(kv), options.platform);
  const { runtime, connect } = fakeRuntime();
  const host = new EngineHost({
    platform,
    runtime,
    capture: NO_CAPTURE,
    registry: new SessionRegistry(Promise.resolve(registryKv)),
    idleMs: options.idleMs ?? 60_000,
  });
  hosts.push(host);
  const client = () => new RemoteEngineClient({ ensureHost: async () => undefined, connect: () => connect(ENGINE_PORT), timeoutMs: 5000 });
  const hostApi = () => new RpcClient<HostApi>(connect(HOST_PORT), { timeoutMs: 5000 });
  return { host, platform, client, hostApi, kv, registryKv, connect };
}

function name(event: SessionEvent): string {
  return typeof event === 'string' ? event : Object.keys(event)[0]!;
}

afterEach(async () => {
  for (const host of hosts.splice(0)) {
    await host.worker.recycle('test end');
    host.close();
  }
});

describe('the engine host', () => {
  it('starts the engine Worker on the first call and serves the engine to pages', async () => {
    const { client, platform } = setup();
    await expect(client().hello()).resolves.toMatchObject({ kind: 'mock' });
    expect(platform.workers).toHaveLength(1);
  });

  it('identifies a synthetic song from live tab audio, and stops the capture at the end', async () => {
    const { client, platform } = setup();
    const ui = client();
    const events: string[] = [];
    let matched = '';
    ui.subscribe({
      sessionEvent: (_id, event) => {
        events.push(name(event));
        if (typeof event === 'object' && 'Finished' in event && event.Finished.outcome !== 'Waiting' && 'Matched' in event.Finished.outcome) {
          matched = event.Finished.outcome.Matched.track.title;
        }
      },
    });
    const id = await ui.startListening('Tab', 'Auto', { auto_stop: true });
    const source = platform.sources[0]!;
    source.play(renderSong(SYNTHETIC_SONGS[2]!, 48_000, 9000, 1000));
    await vi.waitFor(() => expect(matched).toBe('Test Signal C'), { timeout: 5000 });
    expect(events).toContain('Level');
    expect(source.stopped).toBe(true);
    const detail = await ui.session(id);
    expect(detail?.entry.status).toBe('Matched');
  });

  it('keeps a session running when the page that started it closes', async () => {
    const { client, platform, host } = setup();
    const popup = client();
    const id = await popup.startListening('Microphone', 'Auto', { auto_stop: true });
    popup.dispose();
    await vi.waitFor(() => expect(host.surfaces).toBe(0));
    platform.sources[0]!.play(renderSong(SYNTHETIC_SONGS[0]!, 48_000, 9000));
    // Ended with no page open: the background gets what it needs for a notification.
    await vi.waitFor(() => expect(platform.activities.find((a) => a.ended?.id === id)?.surfaces).toBe(0), { timeout: 5000 });
    const reopened = client();
    expect((await reopened.session(id))?.entry.status).toBe('Matched');
  });

  it('recycles the Worker without losing the library', async () => {
    const { client, hostApi } = setup();
    const ui = client();
    const id = await ui.analyze('https://open.spotify.com/track/7qiZfU4dY1lWllzX7mPBI3', 'Auto', null);
    await vi.waitFor(async () => expect((await ui.session(id))?.entry.status).toBe('Completed'), { timeout: 5000 });
    const before = await ui.library({ kinds: [], search: null, pinned_only: false, limit: 50, offset: 0 });
    const state = await hostApi().call('recycle');
    expect(state.worker).toBe('down');
    const after = await ui.library({ kinds: [], search: null, pinned_only: false, limit: 50, offset: 0 });
    expect(after).toEqual(before);
    expect((await hostApi().call('state')).workerStarts).toBe(2);
  });

  it('recycles an idle Worker on its own and reports the host idle', async () => {
    const { client, host, platform } = setup({ idleMs: 50 });
    const ui = client();
    await ui.hello();
    ui.dispose();
    await vi.waitFor(() => expect(host.worker.state).toBe('down'), { timeout: 3000 });
    await vi.waitFor(() => expect(platform.idles).toBeGreaterThan(0));
  });

  it('fails the running sessions when the Worker crashes, and starts a new one', async () => {
    const { client, platform } = setup();
    const ui = client();
    const failed: string[] = [];
    ui.subscribe({ sessionEvent: (id, event) => name(event) === 'Failed' && failed.push(id) });
    const id = await ui.startListening('Tab', 'Auto', { auto_stop: true });
    platform.workers[0]!.crash();
    await vi.waitFor(() => expect(failed).toEqual([id]));
    expect(platform.sources[0]!.stopped).toBe(true);
    await expect(ui.hello()).resolves.toMatchObject({ kind: 'mock' });
    expect(platform.workers).toHaveLength(2);
    // The new engine found the session interrupted.
    expect((await ui.session(id))?.entry.status).toBe('Failed');
  });

  it('plays a directive session without opening any capture', async () => {
    const { client, platform } = setup();
    platform.directive = 'nomatch';
    const ui = client();
    const id = await ui.startListening('Tab', 'Auto', { auto_stop: true });
    expect(platform.sources).toHaveLength(0);
    await vi.waitFor(async () => expect((await ui.session(id))?.entry.status).toBe('NoMatch'), { timeout: 5000 });
  });

  it('rejects with the capture reason when the tab can’t be captured', async () => {
    const { client } = setup({
      platform: {
        openTab: async () => {
          throw new CaptureError('CantCapture', "Can't listen to this page.");
        },
      },
    });
    const error = await client()
      .startListening('Tab', 'Auto', { auto_stop: true })
      .catch((e: unknown) => e);
    expect(error).toBeInstanceOf(EngineError);
    expect(error).toMatchObject({ code: 'CaptureNotAllowed', reason: 'CantCapture' });
  });

  it('identifies what was heard when the capture ends by itself', async () => {
    const { client, platform } = setup();
    const ui = client();
    const id = await ui.startListening('Tab', 'Auto', { auto_stop: true });
    platform.sources[0]!.play(renderSong(SYNTHETIC_SONGS[5]!, 48_000, 5000));
    platform.sources[0]!.end();
    await vi.waitFor(async () => expect((await ui.session(id))?.entry.status).toBe('Matched'), { timeout: 5000 });
    expect((await ui.session(id))?.entry.title).toBe('Test Signal F');
  });

  it('remembers what was running when the host went away', async () => {
    const kv = memoryKv();
    const first = new SessionRegistry(Promise.resolve(kv));
    await first.put({ id: 'gone', kind: 'Link', source: null, startedAt: 1, capture: { via: 'tab', tabId: 3, url: 'https://example.org/' } });
    const next = new SessionRegistry(Promise.resolve(kv));
    expect((await next.takeInterrupted()).map((e) => e.id)).toEqual(['gone']);
    expect(await next.takeInterrupted()).toEqual([]);
  });
});

describe('low power', () => {
  it('follows the setting, else the a05-1 thresholds', () => {
    expect(resolveLowPower('on', false, 16, 32)).toBe(true);
    expect(resolveLowPower('off', true, 2, 1)).toBe(false);
    expect(resolveLowPower('auto', false, 4, 16)).toBe(true);
    expect(resolveLowPower('auto', false, 8, 2)).toBe(true);
    expect(resolveLowPower('auto', false, 8, 8)).toBe(false);
    expect(resolveLowPower('auto', true, 8, null)).toBe(true);
    expect(resolveLowPower('auto', false, 8, null)).toBe(false);
  });
});
