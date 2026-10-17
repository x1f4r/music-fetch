import { describe, expect, it, vi } from 'vitest';
import { RpcClient, RpcError, serveRpc } from '@/src/messaging/bus';
import { createPortPair } from '@/src/messaging/memory-port';

interface Api {
  add(a: number, b: number): Promise<number>;
  fail(): Promise<void>;
  slow(): Promise<string>;
  secret(): Promise<string>;
}
interface Topics {
  tick: { n: number };
}

function setup(options: { timeoutMs?: number } = {}) {
  const [clientPort, serverPort] = createPortPair('mf-test');
  const handlers: Api = {
    add: async (a, b) => a + b,
    fail: async () => {
      throw Object.assign(new Error('nope'), { code: 'InvalidInput' });
    },
    slow: () => new Promise((resolve) => setTimeout(() => resolve('late'), 50)),
    secret: async () => 'should not be reachable',
  };
  const served = serveRpc<Api, Topics>(serverPort, handlers, ['add', 'fail', 'slow']);
  const client = new RpcClient<Api, Topics>(clientPort, options);
  return { client, served };
}

describe('the message bus', () => {
  it('calls methods and returns their values', async () => {
    const { client } = setup();
    await expect(client.call('add', 2, 3)).resolves.toBe(5);
  });

  it('carries the error code of a failed call', async () => {
    const { client } = setup();
    const error = await client.call('fail').catch((e: unknown) => e);
    expect(error).toBeInstanceOf(RpcError);
    expect(error).toMatchObject({ code: 'InvalidInput', message: 'nope' });
  });

  it('only exposes the listed methods', async () => {
    const { client } = setup();
    await expect(client.call('secret')).rejects.toMatchObject({ code: 'Unknown' });
  });

  it('delivers events by topic', async () => {
    const { client, served } = setup();
    const seen = vi.fn();
    client.on('tick', seen);
    served.emit('tick', { n: 1 });
    served.emit('tick', { n: 2 });
    await vi.waitFor(() => expect(seen).toHaveBeenCalledTimes(2));
    expect(seen).toHaveBeenLastCalledWith({ n: 2 });
  });

  it('rejects pending calls when the other end goes away', async () => {
    const { client, served } = setup();
    const pending = client.call('slow');
    served.close();
    await expect(pending).rejects.toMatchObject({ code: 'Disconnected' });
    expect(client.closed).toBe(true);
    await expect(client.call('add', 1, 1)).rejects.toMatchObject({ code: 'Disconnected' });
  });

  it('times out calls nobody answers', async () => {
    const { client } = setup({ timeoutMs: 10 });
    await expect(client.call('slow')).rejects.toMatchObject({ code: 'Timeout' });
  });

  it('ignores messages that are not its own', async () => {
    const [clientPort, serverPort] = createPortPair();
    const client = new RpcClient<Api>(clientPort);
    serverPort.postMessage({ hello: 'world' });
    serverPort.postMessage({ mf: 1, t: 'res', id: 999, ok: true, value: 1 });
    serveRpc<Api>(serverPort, { add: async (a: number, b: number) => a * b } as unknown as Api, ['add']);
    await expect(client.call('add', 3, 4)).resolves.toBe(12);
  });
});
