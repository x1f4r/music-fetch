import { getContext, setContext } from 'svelte';
import type { EngineClient } from '../engine/client';
import type { PageFacts } from '../page/facts';
import type { Capabilities } from '../platform/capabilities';
import type { ActiveTab, AppRoute } from '../platform/surfaces';

/**
 * What a surface needs from the browser, behind one interface so components run in unit
 * tests and demo states with a fake (and the same components serve the popup, panel and
 * app tab).
 */
export interface Platform {
  capabilities: Capabilities;
  activeTab(): Promise<ActiveTab | null>;
  /**
   * What the page in `tab` says about its media (duration, now playing), for the analyze
   * sheet. Null when unknown; the capture coordinator (X4) provides it.
   */
  pageFacts(tab: ActiveTab): Promise<PageFacts | null>;
  /** Open (or focus) the app tab at `route`. */
  openApp(route?: AppRoute): Promise<void>;
  /** Open the side panel or sidebar; call straight from a click handler. */
  openPanel(windowId: number | undefined): Promise<void>;
  /** Open a link in a new tab. */
  openUrl(url: string): Promise<void>;
  copyText(text: string): Promise<void>;
  /** The system share sheet where there is one (Firefox for Android); false if not. */
  share(data: { title: string; text: string; url?: string }): Promise<boolean>;
  /** Save a file (exports) through the browser's downloads. */
  download(filename: string, mime: string, text: string): Promise<void>;
  /** The browser's site settings for this extension (microphone), where it can open them. */
  openSiteSettings(): Promise<void>;
  /** The browser's keyboard shortcut settings. */
  openShortcuts(): Promise<void>;
  /** Close this surface (the popup after handing off to another one; the grant tab). */
  close(): void;
}

export interface SurfaceContext {
  engine: EngineClient;
  platform: Platform;
}

const KEY = Symbol('mf.surface');

export function provideSurface(context: SurfaceContext): void {
  setContext(KEY, context);
}

export function useSurface(): SurfaceContext {
  const context = getContext<SurfaceContext | undefined>(KEY);
  if (!context) throw new Error('No surface context: wrap the component in a surface.');
  return context;
}
