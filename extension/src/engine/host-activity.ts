import { browser } from 'wxt/browser';
import type { HostActivity } from '../messaging/protocol';
import { startNotifier, type Notifier, type NotifierApi } from '../notify/notifier';
import { appUrl, openApp } from '../platform/surfaces';
import type { EngineClient, EngineListener } from './client';

/** What the background asks the host for when it builds a notification. */
export interface ActivityBridge {
  session: EngineClient['session'];
  setPinned: EngineClient['setPinned'];
}

/**
 * The background's badge and notifications (`src/notify/notifier.ts`), fed by the engine
 * host's activity reports. The host can't badge or notify itself (Chromium's offscreen
 * document has only `runtime`), and the background must not hold a long-lived engine
 * connection: Chromium stops the service worker when idle. So the host reports every
 * session's start, phase, progress and end (`mf:host.activity`), and this replays them
 * into the notifier as the engine events it expects.
 */
export class BackgroundNotifications {
  private readonly listeners = new Set<EngineListener>();
  private surfaces = 0;
  readonly notifier: Notifier;

  constructor(bridge: ActivityBridge, api: NotifierApi = browserNotifierApi()) {
    const engine = {
      subscribe: (listener: EngineListener) => {
        this.listeners.add(listener);
        return () => this.listeners.delete(listener);
      },
      session: bridge.session,
      setPinned: bridge.setPinned,
    } as unknown as EngineClient;
    this.notifier = startNotifier(engine, api, {
      hasUi: () => this.surfaces > 0,
      openSession: (id) => void browser.tabs.create({ url: appUrl(`/session/${id}`) }),
      openUrl: (url) => void browser.tabs.create({ url }),
      tryAgain: () => void openApp('/listen'),
    });
  }

  /** Clicks on our notifications and their buttons. */
  listenToClicks(): void {
    browser.notifications?.onClicked.addListener((id) => void this.notifier.clicked(id));
    browser.notifications?.onButtonClicked?.addListener((id, button) => void this.notifier.clicked(id, button));
  }

  report(activity: HostActivity): void {
    this.surfaces = activity.surfaces;
    for (const { sessionId, event } of activity.events ?? []) {
      for (const listener of this.listeners) listener.sessionEvent?.(sessionId, event);
    }
  }
}

/** The notifier's view of the browser: the action badge and system notifications. */
export function browserNotifierApi(): NotifierApi {
  const firefox = import.meta.env.FIREFOX;
  return {
    setBadge(text, color) {
      void browser.action?.setBadgeBackgroundColor({ color }).catch(() => undefined);
      void browser.action?.setBadgeText({ text }).catch(() => undefined);
    },
    notify(id, content) {
      void browser.notifications
        ?.create(id, {
          type: 'basic',
          iconUrl: browser.runtime.getURL('/icon/128.png'),
          title: content.title,
          message: content.message,
          // Firefox shows no buttons; a click opens the session.
          ...(!firefox && content.buttons.length ? { buttons: content.buttons.map((title) => ({ title })) } : {}),
        })
        .catch(() => undefined);
    },
    clear(id) {
      void browser.notifications?.clear(id).catch(() => undefined);
    },
  };
}
