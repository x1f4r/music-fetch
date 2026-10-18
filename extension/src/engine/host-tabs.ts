import { browser } from 'wxt/browser';
import type { TabInfo } from '../messaging/protocol';
import { CaptureError } from './host-core';

/** Pages no extension may capture or script: browser pages, extension pages, stores. */
const RESTRICTED_SCHEMES = /^(chrome|chrome-extension|chrome-untrusted|chrome-search|edge|brave|opera|vivaldi|about|moz-extension|resource|view-source|devtools|data|javascript):/i;
const STORE_HOSTS = new Set(['chromewebstore.google.com', 'chrome.google.com', 'addons.mozilla.org', 'microsoftedge.microsoft.com']);

/** Whether "This tab" may work on `url` (unknown URLs are tried). */
export function captureVerdict(url: string | null | undefined): 'ok' | 'CantCapture' {
  if (!url) return 'ok';
  if (RESTRICTED_SCHEMES.test(url)) return 'CantCapture';
  try {
    const parsed = new URL(url);
    if (STORE_HOSTS.has(parsed.hostname) && (parsed.hostname !== 'chrome.google.com' || parsed.pathname.startsWith('/webstore'))) return 'CantCapture';
  } catch {
    return 'CantCapture';
  }
  return 'ok';
}

type ChromeTabCapture = {
  getMediaStreamId(options: { targetTabId?: number }): Promise<string>;
  getCapturedTabs(): Promise<{ tabId: number; status: string }[]>;
};

function tabCapture(): ChromeTabCapture | undefined {
  return (browser as unknown as { tabCapture?: ChromeTabCapture }).tabCapture;
}

/**
 * A tab by id, or the active tab of the last focused window: the one under the popup,
 * the side panel, a shortcut or a context menu.
 */
export async function resolveTab(tabId?: number): Promise<TabInfo> {
  const tab = tabId !== undefined ? await browser.tabs.get(tabId) : (await browser.tabs.query({ active: true, lastFocusedWindow: true }))[0];
  if (!tab || tab.id === undefined || tab.id < 0) throw new CaptureError('TabGone', 'There is no tab to listen to.');
  let capture: TabInfo['capture'] = captureVerdict(tab.url);
  if (capture === 'ok') {
    const captured = await tabCapture()?.getCapturedTabs().catch(() => []);
    if (captured?.some((c) => c.tabId === tab.id && c.status === 'active')) capture = 'TabBusy';
  }
  return { id: tab.id, windowId: tab.windowId ?? -1, url: tab.url ?? null, title: tab.title ?? null, audible: Boolean(tab.audible), capture };
}

/** Inject the capture agent (isolated world), then the page script (MAIN world). */
export async function injectAgent(tabId: number): Promise<void> {
  await browser.scripting.executeScript({ target: { tabId }, files: ['/content-scripts/capture.js'] });
  try {
    await browser.scripting.executeScript({ target: { tabId }, files: ['/content-scripts/page.js'], world: 'MAIN' } as Parameters<typeof browser.scripting.executeScript>[0]);
  } catch (error) {
    // The agent still works on the DOM alone (no site adapter).
    console.warn('page script not injected', error);
  }
}

/**
 * Chromium: a `tabCapture` stream id for the offscreen document. It needs the extension
 * to have been invoked on the tab (toolbar button, shortcut, menu; S1) and works once
 * per tab at a time.
 */
export async function tabStream(tabId?: number): Promise<{ streamId: string; tab: TabInfo }> {
  const tab = await resolveTab(tabId);
  if (tab.capture !== 'ok') throw new CaptureError(tab.capture, tab.capture === 'TabBusy' ? 'Another capture is using this tab.' : "Can't listen to this page.");
  const api = tabCapture();
  if (!api) throw new CaptureError('CantCapture', 'This browser has no tab capture.');
  try {
    return { streamId: await api.getMediaStreamId({ targetTabId: tab.id }), tab };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (/active stream/i.test(message)) throw new CaptureError('TabBusy', 'Another capture is using this tab.');
    if (/invoked|activeTab|permission|user gesture/i.test(message)) throw new CaptureError('NeedsInvocation', 'Click the Music Fetch button on this tab first.');
    throw new CaptureError('CantCapture', message);
  }
}
