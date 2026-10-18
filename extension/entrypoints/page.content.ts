import { defineContentScript } from 'wxt/utils/define-content-script';
import { startPageScript } from '@/src/page/main-world';

/**
 * The page script (MAIN world, extension plan §3.3): site adapters that read the
 * page's own objects and control its player. Never in the manifest: the host injects it
 * right after the capture agent, whose nonce it takes from the document.
 */
export default defineContentScript({
  matches: [],
  registration: 'runtime',
  world: 'MAIN',
  main() {
    const nonce = document.documentElement.dataset.mfNonce;
    if (!nonce) return;
    delete document.documentElement.dataset.mfNonce;
    startPageScript(window, document, nonce);
  },
});
