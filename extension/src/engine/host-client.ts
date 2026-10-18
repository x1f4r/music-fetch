import { browser } from 'wxt/browser';
import { RpcClient, type PortLike } from '../messaging/bus';
import { sendControl } from '../messaging/control';
import { HOST_PORT } from '../messaging/protocol';
import type { PageFacts } from '../page/facts';
import type { HostApi } from './host-core';

/** One call to the engine host's own API (`HOST_PORT`) from an extension page. */
export async function hostCall<K extends keyof HostApi & string>(method: K, args: Parameters<HostApi[K]>, timeoutMs = 10_000): Promise<Awaited<ReturnType<HostApi[K]>>> {
  await sendControl({ type: 'mf:host.ensure' });
  const rpc = new RpcClient<HostApi>(browser.runtime.connect({ name: HOST_PORT }) as unknown as PortLike, { timeoutMs });
  try {
    return (await (rpc.call as (m: string, ...a: unknown[]) => Promise<unknown>)(method, ...args)) as never;
  } finally {
    rpc.close();
  }
}

/** The capture coordinator's view of a tab's page (site adapter facts and media), or null. */
export async function tabPageFacts(tabId: number): Promise<PageFacts | null> {
  return ((await hostCall('pageFacts', [tabId], 6000).catch(() => null)) as PageFacts | null) ?? null;
}
