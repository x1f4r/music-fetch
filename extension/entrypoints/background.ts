import { browser } from 'wxt/browser';
import { defineBackground } from 'wxt/utils/define-background';
import { listenToNotificationClicks, showActivity } from '@/src/engine/host-activity';
import { startFirefoxHost } from '@/src/engine/host-firefox';
import { injectAgent, resolveTab, tabStream } from '@/src/engine/host-tabs';
import { onControl } from '@/src/messaging/control';
import { installHeaderRules } from '@/src/net/rules';
import { closeOffscreenHost, ensureOffscreenHost } from '@/src/platform/offscreen';
import { needsProviderPanel, settings } from '@/src/settings/settings';

/**
 * Chromium: the service worker routes. It starts and closes the offscreen engine host,
 * does what offscreen documents can't (tabs, scripting, tab capture stream ids,
 * settings, badge, notifications) and installs the header rules. It never holds the
 * engine and never does network I/O itself (extension plan §2.6, §3.1).
 *
 * Firefox: the event page is also the engine host, because Firefox has no offscreen
 * documents.
 */
export default defineBackground(() => {
  const firefox = import.meta.env.FIREFOX;
  if (firefox) startFirefoxHost();

  onControl({
    'mf:host.ensure': async () => {
      if (firefox) return { host: 'background' };
      await ensureOffscreenHost();
      return { host: 'offscreen' };
    },
    'mf:tab.resolve': ({ tabId }) => resolveTab(tabId),
    'mf:tab.stream': ({ tabId }) => tabStream(tabId),
    'mf:tab.inject': async ({ tabId }) => {
      await injectAgent(tabId);
      return { injected: true };
    },
    'mf:host.activity': async ({ active, ended, surfaces }) => {
      await showActivity({ active, ended, surfaces });
      return null;
    },
    'mf:host.idle': async () => ({ closed: firefox ? false : await closeOffscreenHost() }),
    'mf:host.settings': async () => {
      const [micPriority, lowPower] = await Promise.all([settings.micPriority.getValue(), settings.lowPower.getValue()]);
      return { micPriority: micPriority.map((m) => m.id), lowPower };
    },
    'mf:net.ensure': async () => ({ rules: await installHeaderRules() }),
  });

  listenToNotificationClicks();
  // Session rules don't survive a browser restart: install them on every start.
  void installHeaderRules().catch((error: unknown) => console.warn('header rules', error));

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
