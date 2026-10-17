import { describe, expect, it, vi } from 'vitest';
import { EngineError } from '@/src/engine/client';
import { serveEngineHost, type HostRuntime, type IncomingPort } from '@/src/engine/host';
import { MockEngine } from '@/src/engine/mock/engine';
import { RemoteEngineClient } from '@/src/engine/remote';
import type { EventLike, PortLike } from '@/src/messaging/bus';
import { createPortPair } from '@/src/messaging/memory-port';
import { ENGINE_PORT } from '@/src/messaging/protocol';

const ORIGIN = 'chrome-extension://abcdefghijklmnop/';

/** A fake `runtime` whose `connect` hands the other end of a memory port to `onConnect`. */
function fakeRuntime() {
  const listeners = new Set<(port: IncomingPort) => void>();
  const onConnect: EventLike<(port: IncomingPort) => void> = {
    addListener: (f) => listeners.add(f),
    removeListener: (f) => listeners.delete(f),
  };
  const runtime: HostRuntime = { id: 'abcdefghijklmnop', getURL: (path) => new URL(path, ORIGIN).href, onConnect };
  const connect = (url = `${ORIGIN}popup.html`): PortLike => {
    const [ui, host] = createPortPair(ENGINE_PORT);
    const incoming = Object.assign(host, { sender: { id: runtime.id, url } });
    for (const listener of listeners) listener(incoming);
    return ui;
  };
  return { runtime, connect, listeners };
}

describe('the remote engine client', () => {
  it('forwards calls to the host and events back', async () => {
    const engine = new MockEngine({ timeScale: 0 });
    const { runtime, connect } = fakeRuntime();
    serveEngineHost(engine, runtime);
    const client = new RemoteEngineClient({ ensureHost: async () => undefined, connect: () => connect() });

    await expect(client.hello()).resolves.toMatchObject({ kind: 'mock' });
    const events: string[] = [];
    client.subscribe({ sessionEvent: (_id, event) => events.push(typeof event === 'string' ? event : Object.keys(event)[0]!) });
    const id = await client.startListening('Tab', 'Auto', { auto_stop: true });
    await vi.waitFor(() => expect(events.at(-1)).toBe('Finished'), { timeout: 2000 });
    expect(events[0]).toBe('Started');
    const detail = await client.session(id);
    expect(detail?.entry.status).toBe('Matched');
  });

  it('turns host errors into EngineErrors', async () => {
    const { runtime, connect } = fakeRuntime();
    serveEngineHost(new MockEngine({ timeScale: 0 }), runtime);
    const client = new RemoteEngineClient({ ensureHost: async () => undefined, connect: () => connect() });
    const error = await client.analyze('not a link at all', 'Auto', null).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(EngineError);
    expect(error).toMatchObject({ code: 'InvalidInput' });
  });

  it('reconnects after the host goes away', async () => {
    const { runtime, connect } = fakeRuntime();
    const engine = new MockEngine({ timeScale: 0 });
    let host = serveEngineHost(engine, runtime);
    const ensureHost = vi.fn(async () => undefined);
    const client = new RemoteEngineClient({ ensureHost, connect: () => connect(), retryDelayMs: 1 });
    const states: boolean[] = [];
    client.subscribe({ connectionChanged: (up) => states.push(up) });
    await client.hello();
    host.close();
    await vi.waitFor(() => expect(states).toContain(false));
    host = serveEngineHost(engine, runtime);
    await expect(client.isLowPower()).resolves.toBe(false);
    expect(ensureHost.mock.calls.length).toBeGreaterThanOrEqual(2);
    host.close();
  });

  it('fails with Disconnected after the retries when no host answers', async () => {
    const client = new RemoteEngineClient({
      ensureHost: async () => undefined,
      connect: () => createPortPair()[0],
      attempts: 2,
      retryDelayMs: 1,
      timeoutMs: 20,
    });
    await expect(client.hello()).rejects.toBeInstanceOf(EngineError);
  });

  it('refuses ports from other origins', () => {
    const { runtime, connect, listeners } = fakeRuntime();
    const host = serveEngineHost(new MockEngine({ timeScale: 0 }), runtime);
    expect(listeners.size).toBe(1);
    connect('https://evil.example/page.html');
    expect(host.connections).toBe(0);
    connect();
    expect(host.connections).toBe(1);
  });
});
