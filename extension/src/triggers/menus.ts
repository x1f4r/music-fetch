import { t } from '../i18n';

/**
 * Context menus (extension plan §5.1, X7): "Identify music in this tab" on a page and on
 * the toolbar button, "Identify this link" on links, "Identify this audio/video" on media
 * elements. `contextMenus` in Chromium, `menus` in Firefox desktop (the same calls);
 * Firefox for Android has neither.
 */
export const MENU = {
  tab: 'mf-identify-tab',
  link: 'mf-identify-link',
  audio: 'mf-identify-audio',
  video: 'mf-identify-video',
} as const;

export type MenuId = (typeof MENU)[keyof typeof MENU];

/** Web pages only: browser pages and files can't be captured or read. */
const WEB = ['http://*/*', 'https://*/*'];

export interface MenuItem {
  id: MenuId;
  title: string;
  contexts: string[];
  documentUrlPatterns?: string[];
  targetUrlPatterns?: string[];
}

export function menuItems(): MenuItem[] {
  return [
    // `action`: also on the toolbar button's own menu, where it works on any web page.
    { id: MENU.tab, title: t('menu.identifyTab'), contexts: ['page', 'frame', 'selection', 'action'], documentUrlPatterns: WEB },
    { id: MENU.link, title: t('menu.identifyLink'), contexts: ['link'], documentUrlPatterns: WEB, targetUrlPatterns: WEB },
    { id: MENU.audio, title: t('menu.identifyAudio'), contexts: ['audio'], documentUrlPatterns: WEB },
    { id: MENU.video, title: t('menu.identifyVideo'), contexts: ['video'], documentUrlPatterns: WEB },
  ];
}

/** The parts of `contextMenus` / `menus` used here. */
export interface MenusApi {
  removeAll(): Promise<void> | void;
  create(properties: MenuItem, callback?: () => void): unknown;
}

/**
 * (Re)create the items. Chromium keeps menus across service-worker restarts but not
 * across updates, Firefox's event page forgets nothing either; removing first makes it
 * safe to call on every install, update and browser start.
 */
export async function installMenus(api: MenusApi | undefined): Promise<number> {
  if (!api) return 0;
  await api.removeAll();
  const items = menuItems();
  await Promise.all(
    items.map(
      (item) =>
        new Promise<void>((resolve) => {
          // The callback reads lastError so a duplicate id never surfaces as an unchecked error.
          api.create(item, () => {
            void (globalThis as { chrome?: { runtime?: { lastError?: unknown } } }).chrome?.runtime?.lastError;
            resolve();
          });
        }),
    ),
  );
  return items.length;
}
