import { browser } from 'wxt/browser';
import { defineContentScript } from 'wxt/utils/define-content-script';
import { isPageFactsMessage, readPageFacts, type PageFacts } from '@/src/page/facts';

/** Messages the extension sends to the capture script in a tab. */
export interface CaptureRequest {
  type: 'mf:capture.facts';
}

/**
 * Isolated-world capture script (extension plan §3.2): element capture and the media
 * control bridge arrive with tasks X3/X4. For now it answers "what is on this page" with
 * the generic facts, merged with the MAIN-world script's when that one was injected.
 * Injected on demand only (`registration: 'runtime'`), so it needs no host permission
 * beyond the tab the user invoked the extension on.
 */
export default defineContentScript({
  matches: [],
  registration: 'runtime',
  main(ctx) {
    const nonce = crypto.randomUUID();
    document.documentElement.dataset.mfNonce = nonce;
    let pageFacts: PageFacts | null = null;

    ctx.addEventListener(window, 'message', (event: MessageEvent) => {
      if (event.source === window && isPageFactsMessage(event.data, nonce)) pageFacts = event.data.facts;
    });

    const onMessage = (message: unknown) => {
      if ((message as Partial<CaptureRequest> | null)?.type !== 'mf:capture.facts') return undefined;
      const local = readPageFacts(document, undefined);
      return Promise.resolve({ ...local, nowPlaying: pageFacts?.nowPlaying ?? null });
    };
    browser.runtime.onMessage.addListener(onMessage);
    ctx.onInvalidated(() => browser.runtime.onMessage.removeListener(onMessage));
  },
});
