import { browser } from 'wxt/browser';
import { t } from '../i18n';
import type { HostActivity } from '../messaging/protocol';
import { appUrl } from '../platform/surfaces';
import { variantOf, type SessionEvent } from './types';

/** Toolbar badge: a dot while something runs (extension plan §5.1 "Badge"). */
export async function showBadge(activity: HostActivity): Promise<void> {
  const action = browser.action;
  if (!action?.setBadgeText) return;
  const running = activity.active.length > 0;
  await action.setBadgeBackgroundColor?.({ color: '#1FA48A' }).catch(() => undefined);
  await action.setBadgeText({ text: running ? '•' : '' }).catch(() => undefined);
}

/** What a finished session's notification says, or null when it says nothing (canceled). */
export function notificationFor(event: SessionEvent): { title: string; message: string } | null {
  if (variantOf(event) === 'Canceled' || typeof event === 'string') return null;
  if ('Failed' in event) return { title: t('app.name'), message: event.Failed.message || t('error.other') };
  if (!('Finished' in event)) return null;
  const outcome = event.Finished.outcome;
  if (outcome === 'Waiting') return { title: t('app.name'), message: t('state.offline') };
  if ('Matched' in outcome) return { title: outcome.Matched.track.title, message: outcome.Matched.track.artist };
  if ('Completed' in outcome) return { title: t('app.name'), message: t('state.identified', outcome.Completed.identified, outcome.Completed.total) };
  if ('Candidates' in outcome) return { title: t('app.name'), message: t('library.candidates', t('source.hum'), outcome.Candidates.count) };
  return { title: t('app.name'), message: t('state.noMatch') };
}

const NOTIFICATION_PREFIX = 'mf-session:';

/**
 * A session ended while no page was open (the popup closed mid-analysis, a shortcut or
 * menu started it): say so in a notification that opens the session.
 */
export async function notifyEnded(activity: HostActivity): Promise<void> {
  if (!activity.ended || activity.surfaces > 0) return;
  const content = notificationFor(activity.ended.event);
  if (!content || !browser.notifications) return;
  await browser.notifications
    .create(`${NOTIFICATION_PREFIX}${activity.ended.id}`, {
      type: 'basic',
      iconUrl: browser.runtime.getURL('/icon/128.png'),
      title: content.title,
      message: content.message,
    })
    .catch(() => undefined);
}

/** Clicking a session notification opens the session in the app tab. */
export function listenToNotificationClicks(): void {
  browser.notifications?.onClicked.addListener((id) => {
    if (!id.startsWith(NOTIFICATION_PREFIX)) return;
    void browser.tabs.create({ url: appUrl(`/session/${id.slice(NOTIFICATION_PREFIX.length)}`) });
    void browser.notifications.clear(id);
  });
}

export async function showActivity(activity: HostActivity): Promise<void> {
  await Promise.all([showBadge(activity), notifyEnded(activity)]);
}
