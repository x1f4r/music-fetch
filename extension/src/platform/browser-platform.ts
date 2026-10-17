import { browser } from 'wxt/browser';
import type { Platform } from '../ui/context';
import { probeCapabilities, type ProbeApi } from './capabilities';
import { activeTab, openApp, openPanel } from './surfaces';

/** The real browser behind `Platform`, for the popup, side panel, sidebar and app tab. */
export function browserPlatform(): Platform {
  return {
    capabilities: probeCapabilities(browser as unknown as ProbeApi, navigator.userAgent),
    activeTab,
    openApp,
    openPanel,
    async openUrl(url) {
      await browser.tabs.create({ url });
    },
    async copyText(text) {
      await navigator.clipboard.writeText(text);
    },
    close() {
      window.close();
    },
  };
}
