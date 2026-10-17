import { defineContentScript } from 'wxt/utils/define-content-script';
import { PAGE_FACTS_MESSAGE, readPageFacts, type PageFactsMessage } from '@/src/page/facts';

/**
 * MAIN-world page script (extension plan §3.3): reads what only the page can see
 * (`navigator.mediaSession`, player globals) and hands it to the isolated capture script.
 * Never in the manifest: the capture coordinator injects it on demand with
 * `scripting.executeScript` after the user asks, so no site is touched otherwise.
 * Site adapters and player control arrive with task X4.
 */
export default defineContentScript({
  matches: [],
  registration: 'runtime',
  world: 'MAIN',
  main() {
    const nonce = document.documentElement.dataset.mfNonce;
    if (!nonce) return;
    delete document.documentElement.dataset.mfNonce;
    const message: PageFactsMessage = { type: PAGE_FACTS_MESSAGE, nonce, facts: readPageFacts(document, navigator.mediaSession) };
    window.postMessage(message, window.location.origin);
  },
});
