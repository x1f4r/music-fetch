import { browser } from 'wxt/browser';
import { defineBackground } from 'wxt/utils/define-background';
import { startEngineHost } from '@/src/engine/factory';
import { onControl } from '@/src/messaging/control';
import { ensureOffscreenHost } from '@/src/platform/offscreen';
import { needsProviderPanel, settings } from '@/src/settings/settings';

/**
 * Chromium: the service worker routes (control messages, the offscreen engine host,
 * side panel behaviour). Firefox: the event page is also the engine host, because
 * Firefox has no offscreen documents (extension plan §3.1).
 */
export default defineBackground(() => {
  const firefox = import.meta.env.FIREFOX;
  if (firefox) startEngineHost();

  onControl({
    'mf:host.ensure': async () => {
      if (firefox) return { host: 'background' };
      await ensureOffscreenHost();
      return { host: 'offscreen' };
    },
  });

  browser.runtime.onInstalled.addListener(({ reason }) => {
    if (reason !== 'install') return;
    void settings.providerDecision.getValue().then((decision) => {
      if (needsProviderPanel(decision)) void browser.tabs.create({ url: browser.runtime.getURL('/app.html#/welcome') });
    });
  });

  if (!firefox) {
    const apply = (open: boolean | null) =>
      browser.sidePanel?.setPanelBehavior({ openPanelOnActionClick: Boolean(open) }).catch(() => undefined);
    void settings.sidePanelOnClick.getValue().then(apply);
    settings.sidePanelOnClick.watch(apply);
  }
});
