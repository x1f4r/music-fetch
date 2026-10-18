import { browser } from 'wxt/browser';
import { defineBackground } from 'wxt/utils/define-background';
import { BackgroundNotifications } from '@/src/engine/host-activity';
import type { HostApi } from '@/src/engine/host-core';
import { startFirefoxHost } from '@/src/engine/host-firefox';
import { injectAgent, resolveTab, tabStream } from '@/src/engine/host-tabs';
import { RpcClient, type PortLike } from '@/src/messaging/bus';
import { onControl } from '@/src/messaging/control';
import { HOST_PORT } from '@/src/messaging/protocol';
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
  const host = firefox ? startFirefoxHost() : null;

  // Badge and notifications. Firefox's host lives here; Chromium's is the offscreen
  // document, reached over a short-lived HOST_PORT (no surface, no long connection).
  const hostCall = async <K extends 'session' | 'setPinned'>(method: K, ...args: Parameters<HostApi[K]>): Promise<Awaited<ReturnType<HostApi[K]>>> => {
    // Inline build constants (not `firefox`) so Firefox bundles drop the offscreen calls.
    if (!import.meta.env.FIREFOX) await ensureOffscreenHost();
    const rpc = new RpcClient<HostApi>(browser.runtime.connect({ name: HOST_PORT }) as unknown as PortLike, { timeoutMs: 10_000 });
    try {
      return (await (rpc.call as (m: string, ...a: unknown[]) => Promise<unknown>)(method, ...args)) as never;
    } finally {
      rpc.close();
    }
  };
  const notifications = new BackgroundNotifications({
    session: (id) => (host ? host.hostSession(id) : hostCall('session', id)),
    setPinned: (id, pinned) => (host ? host.hostSetPinned(id, pinned) : hostCall('setPinned', id, pinned)),
  });
  notifications.listenToClicks();
  host?.onActivity((activity) => notifications.report(activity));

  onControl({
    'mf:host.ensure': async () => {
      if (import.meta.env.FIREFOX) return { host: 'background' };
      await ensureOffscreenHost();
      return { host: 'offscreen' };
    },
    'mf:tab.resolve': ({ tabId }) => resolveTab(tabId),
    'mf:tab.stream': ({ tabId }) => tabStream(tabId),
    'mf:tab.inject': async ({ tabId }) => {
      await injectAgent(tabId);
      return { injected: true };
    },
    'mf:host.activity': async ({ active, ended, surfaces, events }) => {
      notifications.report({ active, ended, surfaces, events });
      return null;
    },
    'mf:host.idle': async () => ({ closed: import.meta.env.FIREFOX ? false : await closeOffscreenHost() }),
    'mf:host.settings': async () => {
      const [micPriority, lowPower] = await Promise.all([settings.micPriority.getValue(), settings.lowPower.getValue()]);
      if (import.meta.env.MODE !== 'e2e') return { micPriority: micPriority.map((m) => m.id), lowPower };
      const { mfE2eRecognizer } = (await browser.storage.local.get('mfE2eRecognizer')) as { mfE2eRecognizer?: string };
      return { micPriority: micPriority.map((m) => m.id), lowPower, e2eRecognizer: mfE2eRecognizer ?? null };
    },
    'mf:net.ensure': async () => ({ rules: await installHeaderRules() }),
  });

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
