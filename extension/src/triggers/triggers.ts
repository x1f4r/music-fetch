import { EngineError, type CaptureErrorCode } from '../engine/client';
import type { ErrorCode, PageContext } from '../engine/types';
import { t } from '../i18n';
import { errorLine } from '../models/stage';
import { isEngineHost, originPattern } from '../net/permissions';
import { isMediaPageLink } from '../platform/media-links';
import { isAnalyzablePage } from '../platform/surfaces';
import type { ListenChoice } from '../settings/settings';
import { MENU } from './menus';

/**
 * Shortcuts and context menus (extension plan §5.3 step 4, X7): identify without opening
 * a surface. The engine host does the work as for the popup; the toolbar badge shows it
 * listening and a notification brings the answer (or says why it couldn't start).
 *
 * - `identify-tab` (Alt+Shift+I) and "Identify music in this tab": live "This tab". The
 *   shortcut or menu click is the invocation tab capture needs on Chromium (S1).
 * - "Identify this link": catalog and page links are analyzed headlessly; a video or audio
 *   page link opens the app tab's "Open and analyze" flow, because its sound only plays on
 *   its page. Other sites need their host permission, asked inside the click.
 * - "Identify this audio/video": the tab if it is playing; else the element's file when it
 *   has one (a podcast's `<audio src>`); else "Play it, then try again".
 * - `hum` (Alt+Shift+H): opens the side panel or sidebar, which starts humming.
 */

/** A tab as the browser passes it to command and menu listeners. */
export interface TriggerTab {
  id?: number;
  windowId?: number;
  url?: string;
  title?: string;
  audible?: boolean;
}

/** What a context-menu click says (Chromium's and Firefox's `OnClickData`). */
export interface MenuClick {
  menuItemId: string | number;
  linkUrl?: string;
  srcUrl?: string;
  pageUrl?: string;
  mediaType?: string;
}

/** The engine as a trigger uses it: one call over a short-lived connection. */
export interface TriggerEngine {
  startListening(): Promise<string>;
  analyze(input: string, page: PageContext | null): Promise<string>;
}

export interface TriggerDeps {
  engine: TriggerEngine;
  /** A session a trigger started: notify when it ends, whatever is open. */
  expect(sessionId: string): void;
  /** A trigger that couldn't start: a notification with the reason. */
  problem(message: string): void;
  /** Open the app tab (`#/open?url=…`). */
  openApp(route: `/open?url=${string}` | '/listen'): void;
  /** `permissions.request` for an origin pattern: must be called before anything is awaited. */
  requestOrigin(pattern: string): Promise<boolean>;
  /** Open the side panel or sidebar for `windowId`, inside the gesture; false where there is none. */
  openPanel(windowId: number | undefined): Promise<void> | null;
  requestStart(source: ListenChoice): Promise<void>;
}

/** The notification line for an engine error or a capture problem. */
export function problemText(error: unknown): string {
  if (error instanceof EngineError) {
    if (error.reason === 'NeedsInvocation') return t('notify.needsInvocation');
    if (error.reason === 'MicUnavailable') return t('notify.micUnavailable');
    const known = error.code !== 'Disconnected' && error.code !== 'Timeout' && error.code !== 'Unknown';
    if (known) return t(errorLine(error.code as ErrorCode | CaptureErrorCode));
    return t('error.engine');
  }
  return t('error.other');
}

export class Triggers {
  constructor(private readonly deps: TriggerDeps) {}

  /** A keyboard command (`commands.onCommand`). */
  command(name: string, tab: TriggerTab | undefined): Promise<void> {
    if (name === 'identify-tab') return this.identifyTab();
    if (name === 'hum') return this.hum(tab);
    return Promise.resolve();
  }

  /** A context-menu click (`contextMenus.onClicked` / `menus.onClicked`). */
  menu(click: MenuClick, tab: TriggerTab | undefined): Promise<void> {
    switch (click.menuItemId) {
      case MENU.tab:
        return this.identifyTab();
      case MENU.link:
        return click.linkUrl ? this.identifyLink(click.linkUrl) : Promise.resolve();
      case MENU.audio:
      case MENU.video:
        return this.identifyMedia(click, tab);
      default:
        return Promise.resolve();
    }
  }

  /** "This tab", live, as from the popup; the answer arrives as a notification. */
  async identifyTab(): Promise<void> {
    try {
      this.deps.expect(await this.deps.engine.startListening());
    } catch (error) {
      this.deps.problem(problemText(error));
    }
  }

  identifyLink(url: string): Promise<void> {
    if (isMediaPageLink(url)) {
      // Its sound plays only on its page: the app tab asks how to analyze it.
      this.deps.openApp(`/open?url=${encodeURIComponent(url)}`);
      return Promise.resolve();
    }
    return this.analyze(url, null);
  }

  identifyMedia(click: MenuClick, tab: TriggerTab | undefined): Promise<void> {
    if (tab?.audible) return this.identifyTab();
    const src = click.srcUrl ?? '';
    if (/^https?:/i.test(src)) {
      const page: PageContext = { tab_id: tab?.id ?? null, url: tab?.url ?? click.pageUrl ?? null, title: tab?.title ?? null, facts: null };
      return this.analyze(src, isAnalyzablePage(page.url ?? undefined) ? page : null);
    }
    this.deps.problem(t('notify.playFirst'));
    return Promise.resolve();
  }

  /** Analyze a link headlessly; sites outside the engine's hosts are asked for in the click. */
  private analyze(url: string, page: PageContext | null): Promise<void> {
    const pattern = isEngineHost(url) ? null : originPattern(url);
    // No await before this call: the browser needs the click's user gesture.
    const granted = pattern ? this.deps.requestOrigin(pattern) : Promise.resolve(true);
    return (async () => {
      if (!(await granted.catch(() => false))) {
        this.deps.problem(t('notify.hostRefused', new URL(url).hostname));
        return;
      }
      try {
        this.deps.expect(await this.deps.engine.analyze(url, page));
      } catch (error) {
        this.deps.problem(problemText(error));
      }
    })();
  }

  /** The side panel or sidebar, humming. */
  hum(tab: TriggerTab | undefined): Promise<void> {
    // Inside the gesture: open first, then leave the request the panel picks up.
    const opened = this.deps.openPanel(tab?.windowId);
    return (async () => {
      await this.deps.requestStart('Hum');
      if (!opened) {
        this.deps.openApp('/listen');
        return;
      }
      await opened.catch(() => this.deps.openApp('/listen'));
    })();
  }
}
