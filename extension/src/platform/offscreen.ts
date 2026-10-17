import { browser, type Browser } from 'wxt/browser';

const OFFSCREEN_PATH = '/offscreen.html';

let creating: Promise<void> | null = null;

/**
 * Chromium only: make sure the offscreen document that hosts the engine exists. The
 * service worker only routes; it never holds the engine (extension plan §3.1).
 * Idle recycling of the document is the engine-host task (X2).
 */
export async function ensureOffscreenHost(): Promise<void> {
  const url = browser.runtime.getURL(OFFSCREEN_PATH);
  const contexts = await browser.runtime.getContexts({
    contextTypes: ['OFFSCREEN_DOCUMENT' as Browser.runtime.ContextType],
    documentUrls: [url],
  });
  if (contexts.length > 0) return;
  creating ??= browser.offscreen
    .createDocument({
      url: OFFSCREEN_PATH,
      reasons: ['USER_MEDIA', 'WORKERS'] as Browser.offscreen.Reason[],
      justification: 'Runs the music recognition engine and captures tab or microphone audio.',
    })
    .catch((error: unknown) => {
      // Another context created it in the meantime.
      if (!String(error).includes('single offscreen')) throw error;
    })
    .finally(() => {
      creating = null;
    });
  await creating;
}
