import { browser, type Browser } from 'wxt/browser';
import { TARGET } from './capabilities';

/** Routes of the full-page app tab (extension plan §5.1). */
export type AppRoute =
  | '/listen'
  | '/library'
  | `/session/${string}`
  | `/settings/${string}`
  | '/welcome'
  | '/grant-mic'
  | `/grant-mic?state=${string}`
  | `/open?url=${string}`;

export function appUrl(route: AppRoute = '/listen'): string {
  return browser.runtime.getURL(`/app.html#${route}`);
}

/** Open the app tab at `route`, reusing an open one (found through `runtime.getContexts`). */
export async function openApp(route: AppRoute = '/listen'): Promise<void> {
  const url = appUrl(route);
  try {
    const contexts = await browser.runtime.getContexts({ contextTypes: ['TAB' as Browser.runtime.ContextType] });
    const open = contexts.find((c) => c.documentUrl?.startsWith(browser.runtime.getURL('/app.html')) && c.tabId >= 0);
    if (open) {
      await browser.tabs.update(open.tabId, { active: true, url });
      if (open.windowId >= 0) await browser.windows.update(open.windowId, { focused: true });
      return;
    }
  } catch {
    // getContexts is missing (older browser): open a new tab.
  }
  await browser.tabs.create({ url });
}

/**
 * Open the side panel (Chromium) or sidebar (Firefox desktop). Must run directly in a
 * click handler: both browsers require a user gesture, so nothing may be awaited before
 * this call. `windowId` is read ahead of time for that reason.
 */
export function openPanel(windowId: number | undefined): Promise<void> {
  if (TARGET === 'firefox') {
    const sidebar = (browser as unknown as { sidebarAction?: { open(): Promise<void> } }).sidebarAction;
    return sidebar ? sidebar.open() : Promise.reject(new Error('No sidebar in this browser.'));
  }
  if (windowId === undefined) return Promise.reject(new Error('No window to open the side panel in.'));
  return browser.sidePanel.open({ windowId });
}

export interface ActiveTab {
  id: number | undefined;
  windowId: number | undefined;
  /** Only known while the extension holds `activeTab` for it (popup open, shortcut). */
  url: string | undefined;
  title: string | undefined;
  audible: boolean;
}

export async function activeTab(): Promise<ActiveTab | null> {
  try {
    const [tab] = await browser.tabs.query({ active: true, currentWindow: true });
    if (!tab) return null;
    return { id: tab.id, windowId: tab.windowId, url: tab.url, title: tab.title, audible: Boolean(tab.audible) };
  } catch {
    return null;
  }
}

/** The page can be analysed: an http(s) page, not a browser or store page. */
export function isAnalyzablePage(url: string | undefined): boolean {
  if (!url) return false;
  try {
    const parsed = new URL(url);
    if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') return false;
    return !['chromewebstore.google.com', 'chrome.google.com', 'addons.mozilla.org', 'microsoftedge.microsoft.com'].includes(
      parsed.hostname,
    );
  } catch {
    return false;
  }
}
