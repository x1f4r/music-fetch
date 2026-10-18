import { browser } from 'wxt/browser';
import { defineContentScript } from 'wxt/utils/define-content-script';
import { startAgent } from '@/src/capture/agent';
import type { PortLike } from '@/src/messaging/bus';

/**
 * The capture agent (isolated world, extension plan §3.2): player control, element
 * capture and the media clock for the engine host. Injected on demand only
 * (`registration: 'runtime'`) into the tab the user asked about, so it needs no host
 * permission beyond that tab. Injecting it again reconnects the same agent.
 */
export default defineContentScript({
  matches: [],
  registration: 'runtime',
  main() {
    startAgent({ win: window, doc: document, connect: (name) => browser.runtime.connect({ name }) as unknown as PortLike });
  },
});
