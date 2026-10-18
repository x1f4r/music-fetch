import { afterEach, describe, expect, it, vi } from 'vitest';
import { CaptureCoordinator, type TabsBridge } from '@/src/capture/coordinator';
import { addCoverage, coverageGaps, coveredMs } from '@/src/capture/types';
import { EngineHost } from '@/src/engine/host-core';
import { memoryKv } from '@/src/engine/host-idb';
import { SessionRegistry } from '@/src/engine/host-registry';
import { RemoteEngineClient } from '@/src/engine/remote';
import { createPortPair } from '@/src/messaging/memory-port';
import { ENGINE_PORT, type TabInfo } from '@/src/messaging/protocol';
import type { MediaClock } from '@/src/audio/media-clock';
import { FakeAgent, FakeTabSource } from './support/fake-agent';
import { fakePlatform, fakeRuntime, mockFactory } from './support/host-fakes';

const TAB: TabInfo = { id: 42, windowId: 1, url: 'http://127.0.0.1:8000/mix.html', title: 'Fake mix', audible: false, capture: 'ok' };

function setup(agent = new FakeAgent(), via: 'element' | 'tab' = 'element') {
  let coordinator: CaptureCoordinator;
  const injected: number[] = [];
  const tabs: TabsBridge = {
    ...(via === 'tab' ? { streamId: async () => 'fake-stream' } : {}),
    resolve: async (tabId) => ({ ...TAB, id: tabId ?? TAB.id }),
    inject: async (tabId) => {
      injected.push(tabId);
      const [agentSide, hostSide] = createPortPair('mf-capture');
      agent.serve(agentSide);
      coordinator.acceptAgent(Object.assign(hostSide, { sender: { tab: { id: tabId } } }));
    },
  };
  const platform = fakePlatform(mockFactory(memoryKv()), { target: via === 'tab' ? 'chromium' : 'firefox' });
  let host: EngineHost | null = null;
  coordinator = new CaptureCoordinator({
    target: via === 'tab' ? 'chromium' : 'firefox',
    ...(via === 'tab'
      ? {
          openTabCapture: async (_id: string, clock: MediaClock | null) => {
            agent.tab = new FakeTabSource(clock);
            const stats = { chunks: 0, starts: [] as number[], clock };
            agent.tab.onPcm((c) => {
              stats.chunks++;
              if (!c.continuous) stats.starts.push(Math.round(c.mediaMs!));
            });
            (globalThis as unknown as { __tabStats: unknown[] }).__tabStats ??= [];
            (globalThis as unknown as { __tabStats: unknown[] }).__tabStats.push(stats);
            return agent.tab;
          },
        }
      : {}),
    android: false,
    tabs,
    pushPcm: (id, chunk) => host?.worker.pushPcm(id, chunk),
    captureEvent: (id, event) => void host?.worker.call('captureEvent', id, event),
    onCoverage: (id, _tab, coverage) => host?.coverage(id, coverage),
  });
  const { runtime, connect } = fakeRuntime();
  const registry = new SessionRegistry(Promise.resolve(memoryKv()), 0);
  host = new EngineHost({ platform, runtime, capture: coordinator, registry, idleMs: 60_000 });
  const client = new RemoteEngineClient({ ensureHost: async () => undefined, connect: () => connect(ENGINE_PORT), timeoutMs: 10_000 });
  return { host, client, coordinator, agent, injected, registry };
}

let cleanup: (() => Promise<void>)[] = [];
afterEach(async () => {
  for (const c of cleanup.splice(0)) await c();
});

describe('coverage', () => {
  it('merges ranges and finds the gaps', () => {
    let c = addCoverage([], [0, 1000]);
    c = addCoverage(c, [3000, 4000]);
    c = addCoverage(c, [1020, 2000]);
    expect(c).toEqual([
      [0, 2000],
      [3000, 4000],
    ]);
    expect(coveredMs(c)).toBe(3000);
    expect(coverageGaps(c, 0, 6000)).toEqual([
      [2000, 3000],
      [4000, 6000],
    ]);
  });
});

describe('the capture coordinator', () => {
  it('listens a mix through at 4× and the mock engine segments it within 2 s of the truth', async () => {
    const { host, client, agent, injected } = setup();
    cleanup.push(async () => {
      await host.worker.recycle('test');
      host.close();
    });
    const id = await client.analyze('http://127.0.0.1:8000/mix.html', 'Mix', { tab_id: 42, url: TAB.url, title: 'Fake mix', facts: null });
    await vi.waitFor(async () => expect((await client.session(id))?.entry.status).toBe('Completed'), { timeout: 10_000 });
    const detail = (await client.session(id))!;
    expect(detail.segments.map((s) => s.track?.title)).toEqual(['Alpha Signal', 'Bravo Signal', 'Charlie Signal']);
    const ends = detail.segments.map((s) => s.end_ms);
    expect(Math.abs(ends[0]! - 20_000)).toBeLessThanOrEqual(2000);
    expect(Math.abs(ends[1]! - 40_000)).toBeLessThanOrEqual(2000);
    expect(injected).toEqual([42]);
    // The scan ran muted at 4× with pitch correction off, and the player was put back.
    expect(agent.restored).toBeGreaterThan(0);
    expect(agent.rate).toBe(1);
    expect(agent.muted).toBe(false);
  });

  it('stamps Chromium tab capture by the page clock, with jittery blocks, within 2 s', async () => {
    const agent = new FakeAgent();
    agent.jitterMs = 60;
    agent.seed = Number(process.env.MF_SEED ?? 7);
    const { host, client } = setup(agent, 'tab');
    cleanup.push(async () => {
      await host.worker.recycle('test');
      host.close();
    });
    const id = await client.analyze('http://127.0.0.1:8000/mix.html', 'Mix', { tab_id: 42, url: TAB.url, title: 'Fake mix', facts: null });
    await vi.waitFor(async () => expect((await client.session(id))?.entry.status).toBe('Completed'), { timeout: 10_000 });
    const detail = (await client.session(id))!;
    const stats = ((globalThis as unknown as { __tabStats?: { chunks: number; starts: number[]; clock: MediaClock }[] }).__tabStats ?? []).map((t) => ({ chunks: t.chunks, starts: t.starts.slice(0, 12), dropped: t.clock.dropped, runs: t.clock.runs }));
    const where = `${JSON.stringify(detail.segments.map((s) => [s.track?.title, s.start_ms, s.end_ms]))} ${JSON.stringify(stats)}`;
    expect(detail.segments.map((s) => s.track?.title), where).toEqual(['Alpha Signal', 'Bravo Signal', 'Charlie Signal']);
    expect(Math.abs(detail.segments[0]!.end_ms - 20_000), where).toBeLessThanOrEqual(2000);
    expect(Math.abs(detail.segments[1]!.end_ms - 40_000), where).toBeLessThanOrEqual(2000);
    // Tab capture keeps the element unmuted: the capture follows its volume (S1).
    expect(agent.prepared).toBeNull();
    expect(agent.tab?.stopped).toBe(true);
  });

  it('checks a short video with three survey windows', async () => {
    const short = new FakeAgent([[(await import('@/src/engine/host-mock-audio')).SYNTHETIC_SONGS[3]!, 60_000]]);
    const { host, client } = setup(short);
    cleanup.push(async () => {
      await host.worker.recycle('test');
      host.close();
    });
    const id = await client.analyze('http://127.0.0.1:8000/song.html', 'Auto', { tab_id: 42, url: TAB.url, title: 'A video', facts: null });
    await vi.waitFor(async () => expect((await client.session(id))?.entry.status).toBe('Completed'), { timeout: 10_000 });
    expect((await client.session(id))!.segments.map((s) => s.track?.title)).toEqual(['Delta Signal']);
  });

  it('fails the analysis when the tab goes away mid-capture', async () => {
    const agent = new FakeAgent(undefined, 50);
    const { host, client, registry } = setup(agent);
    cleanup.push(async () => {
      await host.worker.recycle('test');
      host.close();
    });
    const id = await client.analyze('http://127.0.0.1:8000/mix.html', 'Mix', { tab_id: 42, url: TAB.url, title: 'Fake mix', facts: null });
    await vi.waitFor(() => expect(agent.positionMs).toBeGreaterThan(5000), { timeout: 5000 });
    // The registry knows where this analysis got to (a resumed one picks up there).
    const entry = registry.get(id);
    expect(entry?.capture).toMatchObject({ tabId: 42 });
    expect(entry!.coverage.at(-1)![1]).toBeGreaterThan(3000);
    agent.served!.close();
    await vi.waitFor(async () => expect((await client.session(id))?.entry.status).toBe('Failed'), { timeout: 5000 });
  });
});
