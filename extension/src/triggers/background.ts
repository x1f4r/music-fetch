import { browser } from 'wxt/browser';
import { EngineError, type EngineApi } from '../engine/client';
import type { EngineHost } from '../engine/host-core';
import { RpcClient, RpcError, type PortLike } from '../messaging/bus';
import { ENGINE_PORT, type CaptureReason } from '../messaging/protocol';
import type { Notifier } from '../notify/notifier';
import { ensureOffscreenHost } from '../platform/offscreen';
import { openApp, openPanel } from '../platform/surfaces';
import { installMenus, type MenusApi } from './menus';
import { requestStart } from './pending';
import { Triggers, type MenuClick, type TriggerEngine, type TriggerTab } from './triggers';

/**
 * Wires shortcuts and context menus in the background (X7). Call it synchronously at the
 * top of the background script, so the listeners exist when an event wakes it up.
 *
 * Chromium: the engine host is the offscreen document; a trigger opens a short-lived
 * engine connection, starts the session and closes it again (sessions outlive their
 * pages). Firefox: the host lives in this event page and is called directly.
 */
export function startTriggers(options: { host: EngineHost | null; notifier: Notifier }): Triggers {
  const api = browser as unknown as {
    contextMenus?: MenusApi & { onClicked: { addListener(cb: (info: MenuClick, tab?: TriggerTab) => void): void } };
    menus?: MenusApi & { onClicked: { addListener(cb: (info: MenuClick, tab?: TriggerTab) => void): void } };
  };
  const menus = api.contextMenus ?? api.menus;
  const triggers = new Triggers({
    engine: options.host ? hostEngine(options.host) : remoteEngine(),
    expect: (id) => options.notifier.expect(id),
    problem: (message) => options.notifier.problem(message),
    openApp: (route) => void openApp(route),
    requestOrigin: (pattern) => browser.permissions.request({ origins: [pattern] }).catch(() => false),
    openPanel: (windowId) => {
      try {
        // Chromium wants a window; -2 is the current one (chrome.windows.WINDOW_ID_CURRENT).
        return openPanel(windowId ?? (import.meta.env.FIREFOX ? undefined : -2));
      } catch {
        return null;
      }
    },
    requestStart: (source) => requestStart(source),
  });

  browser.commands?.onCommand.addListener((command: string, tab?: TriggerTab) => void triggers.command(command, tab));
  menus?.onClicked.addListener((info, tab) => void triggers.menu(info, tab));
  browser.runtime.onInstalled.addListener(() => void installMenus(menus).catch((error: unknown) => console.warn('menus', error)));
  browser.runtime.onStartup?.addListener(() => void installMenus(menus).catch(() => undefined));
  exposeForTests(triggers);
  return triggers;
}

/** Live "This tab" and links, always with auto-stop: there is no Stop button to press. */
const LISTEN_OPTIONS = { auto_stop: true };

function hostEngine(host: EngineHost): TriggerEngine {
  return {
    startListening: () => host.startListening(null, 'Tab', 'Auto', LISTEN_OPTIONS),
    analyze: (input, page) => host.analyze(input, 'Auto', page),
  };
}

function remoteEngine(): TriggerEngine {
  const call = async <K extends 'startListening' | 'analyze'>(method: K, ...args: Parameters<EngineApi[K]>): Promise<string> => {
    await ensureOffscreenHost();
    const rpc = new RpcClient<EngineApi>(browser.runtime.connect({ name: ENGINE_PORT }) as unknown as PortLike, { timeoutMs: 45_000 });
    try {
      return (await (rpc.call as (m: string, ...a: unknown[]) => Promise<unknown>)(method, ...args)) as string;
    } catch (error) {
      if (error instanceof RpcError) throw new EngineError(error.code as EngineError['code'], error.message, error.reason as CaptureReason | undefined);
      throw error;
    } finally {
      // The session goes on in the host; this background holds no connection.
      rpc.close();
    }
  };
  return {
    startListening: () => call('startListening', 'Tab', 'Auto', LISTEN_OPTIONS),
    analyze: (input, page) => call('analyze', input, 'Auto', page),
  };
}

/**
 * e2e builds only: the end-to-end tests dispatch a command or a menu click as the
 * browser would (`globalThis.mfTriggers`, through the service worker or a control
 * message), after invoking the extension on the tab with the toolbar action.
 */
function exposeForTests(triggers: Triggers) {
  if (import.meta.env.MODE !== 'e2e') return;
  (globalThis as unknown as { mfTriggers?: Triggers }).mfTriggers = triggers;
  // Firefox's WebDriver BiDi can't reach the event page: the tests send this from an extension page.
  browser.runtime.onMessage.addListener(((message: unknown, sender: { id?: string; url?: string }, sendResponse: (reply: unknown) => void) => {
    const m = message as { type?: string; command?: string; click?: MenuClick; tabId?: number } | null;
    if (m?.type !== 'mf:e2e.trigger' || sender.id !== browser.runtime.id || !sender.url?.startsWith(browser.runtime.getURL('/'))) return undefined;
    void (async () => {
      const tab = m.tabId !== undefined ? ((await browser.tabs.get(m.tabId)) as TriggerTab) : undefined;
      await (m.command ? triggers.command(m.command, tab) : m.click ? triggers.menu(m.click, tab) : Promise.resolve());
      sendResponse({ ok: true });
    })().catch((error: unknown) => sendResponse({ ok: false, message: String(error) }));
    return true;
  }) as never);
}
