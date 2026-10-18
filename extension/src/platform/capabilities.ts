/**
 * What this browser can do, probed once per page. Surfaces use it to decide what to
 * offer (e.g. the side panel button), never the user agent alone.
 */

export type Target = 'chromium' | 'firefox';

/** The build target. WXT replaces `import.meta.env.FIREFOX` at build time. */
export const TARGET: Target = import.meta.env.FIREFOX ? 'firefox' : 'chromium';

export interface Capabilities {
  target: Target;
  /** Firefox for Android: no sidebar, menus or shortcuts; the popup is full screen. */
  android: boolean;
  /** Where the persistent surface lives, if any. */
  panel: 'sidePanel' | 'sidebar' | null;
  /** Chromium: the engine host is an offscreen document; Firefox: the event page. */
  engineHost: 'offscreen' | 'background';
  /** `tabCapture` for "This tab" (Chromium); Firefox captures page elements instead. */
  tabCapture: boolean;
  /** Context menus (`contextMenus` on Chromium, `menus` on Firefox desktop). */
  menus: boolean;
  /** Keyboard shortcuts (`commands`); absent on Firefox for Android. */
  shortcuts: boolean;
  /**
   * Where listening by microphone and humming run. Firefox for Android never gives
   * extension pages the microphone (S2: NotAllowedError in the popup and in tabs, no
   * prompt), so there both hand off to the Music Fetch app.
   */
  microphone: 'extension' | 'app';
}

/** The parts of the `browser` namespace the probe looks at. */
export interface ProbeApi {
  sidePanel?: unknown;
  sidebarAction?: unknown;
  offscreen?: unknown;
  tabCapture?: unknown;
  contextMenus?: unknown;
  menus?: unknown;
  commands?: unknown;
}

export function probeCapabilities(api: ProbeApi, userAgent: string, target: Target = TARGET): Capabilities {
  const android = /\bAndroid\b/i.test(userAgent);
  const panel = api.sidePanel ? 'sidePanel' : api.sidebarAction && !android ? 'sidebar' : null;
  return {
    target,
    android,
    panel,
    engineHost: target === 'chromium' ? 'offscreen' : 'background',
    tabCapture: target === 'chromium' && Boolean(api.tabCapture),
    menus: !android && Boolean(api.contextMenus ?? api.menus),
    shortcuts: !android && Boolean(api.commands),
    microphone: android ? 'app' : 'extension',
  };
}
