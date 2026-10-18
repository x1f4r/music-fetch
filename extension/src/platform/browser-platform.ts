import { browser } from 'wxt/browser';
import type { Platform } from '../ui/context';
import { probeCapabilities, TARGET, type ProbeApi } from './capabilities';
import { activeTab, openApp, openPanel } from './surfaces';

/** The real browser behind `Platform`, for the popup, side panel, sidebar and app tab. */
export function browserPlatform(): Platform {
  const capabilities = probeCapabilities(browser as unknown as ProbeApi, navigator.userAgent);
  return {
    capabilities,
    activeTab,
    // The capture coordinator (task X4) reads facts in the page; until then nothing is known.
    pageFacts: async () => null,
    async hostAccess(url) {
      try {
        const origin = `${new URL(url).origin}/*`;
        return (await browser.permissions.contains({ origins: [origin] })) ? 'granted' : 'needed';
      } catch {
        return 'granted';
      }
    },
    requestHost(url) {
      // No await before this call: permissions.request needs the click's user gesture.
      return browser.permissions.request({ origins: [`${new URL(url).origin}/*`] }).catch(() => false);
    },
    openApp,
    openPanel,
    async openUrl(url) {
      await browser.tabs.create({ url });
    },
    async copyText(text) {
      await navigator.clipboard.writeText(text);
    },
    async share(data) {
      if (!capabilities.android || typeof navigator.share !== 'function') return false;
      try {
        await navigator.share(data);
        return true;
      } catch {
        return false;
      }
    },
    async download(filename, mime, text) {
      const url = URL.createObjectURL(new Blob([text], { type: mime }));
      try {
        const anchor = Object.assign(document.createElement('a'), { href: url, download: filename });
        document.body.append(anchor);
        anchor.click();
        anchor.remove();
      } finally {
        setTimeout(() => URL.revokeObjectURL(url), 10_000);
      }
    },
    async openSiteSettings() {
      if (TARGET === 'firefox') {
        // Firefox doesn't let extensions open about:preferences; its permission panel is in the address bar.
        await browser.tabs.create({ url: 'https://support.mozilla.org/kb/how-manage-your-camera-and-microphone-permissions' });
        return;
      }
      const site = encodeURIComponent(browser.runtime.getURL('/').replace(/\/$/, ''));
      await browser.tabs.create({ url: `chrome://settings/content/siteDetails?site=${site}` });
    },
    async openShortcuts() {
      const commands = browser.commands as unknown as { openShortcutSettings?: () => Promise<void> };
      if (TARGET === 'firefox' && commands?.openShortcutSettings) return commands.openShortcutSettings();
      await browser.tabs.create({ url: 'chrome://extensions/shortcuts' });
    },
    async shortcuts() {
      try {
        const commands = await browser.commands.getAll();
        return commands.map((c) => ({ name: c.name ?? '', shortcut: c.shortcut ?? '' }));
      } catch {
        return [];
      }
    },
    async microphones() {
      try {
        const devices = await navigator.mediaDevices.enumerateDevices();
        return devices
          .filter((d) => d.kind === 'audioinput' && d.deviceId !== 'default' && d.deviceId !== 'communications')
          .map((d) => ({ id: d.deviceId, label: d.label, inUse: false }));
      } catch {
        return [];
      }
    },
    close() {
      window.close();
    },
  };
}
