import type { LibraryEntry, Outcome } from '../engine/types';
import type { Message } from '../i18n';

/**
 * What the background shows when a session ends while no surface is open (extension plan
 * §5.1 "Notification" and "Badge"): pure, so the background only posts what this returns.
 */
export interface NotificationContent {
  title: Message;
  message: Message | string;
  /** Chromium shows up to two buttons; Firefox shows none (a click opens the session). */
  buttons: Message[];
  /** What a click on the notification (or its first button) does. */
  action: 'openInProvider' | 'openSession' | 'tryAgain';
}

/** `providerName` is the default provider's display name ("Spotify"). */
export function resultNotification(entry: LibraryEntry, outcome: Outcome, providerName: string): NotificationContent | null {
  if (outcome === 'Waiting') {
    return { title: { key: 'notify.offline' }, message: entry.title, buttons: [], action: 'openSession' };
  }
  if ('Matched' in outcome) {
    const { title, artist, album } = outcome.Matched.track;
    return {
      title: { key: 'notify.matched', args: [title, artist] },
      message: album ?? '',
      buttons: [
        { key: 'result.openIn', args: [providerName] },
        { key: 'result.pin' },
      ],
      action: 'openInProvider',
    };
  }
  if ('Candidates' in outcome) {
    const best = entry.track ?? null;
    const guess = best ? { key: 'notify.bestGuess' as const, args: [best.title, best.artist] } : { key: 'notify.seeAll' as const };
    return { title: guess, message: { key: 'notify.seeAll' }, buttons: [{ key: 'notify.seeAll' }], action: 'openSession' };
  }
  if ('Completed' in outcome) {
    const { identified, total } = outcome.Completed;
    return { title: { key: 'notify.done', args: [identified, total] }, message: entry.title, buttons: [], action: 'openSession' };
  }
  return { title: { key: 'notify.noMatch' }, message: entry.title, buttons: [{ key: 'action.tryAgain' }], action: 'tryAgain' };
}

export interface Badge {
  /** Empty clears the badge. Chromium shows about four characters. */
  text: string;
  /** Background colour: the tonal primary (light scheme), legible on both toolbars. */
  color: string;
}

export const BADGE_COLOR = '#006b5a';

/**
 * The toolbar badge: a dot while listening or analyzing, the percentage while a mix is
 * listened through, nothing when idle.
 */
export function badgeFor(state: { capturing: boolean; working: boolean; mixFraction: number | null }): Badge {
  if (state.mixFraction !== null) return { text: `${Math.min(99, Math.floor(state.mixFraction * 100))}%`, color: BADGE_COLOR };
  if (state.capturing || state.working) return { text: '•', color: BADGE_COLOR };
  return { text: '', color: BADGE_COLOR };
}
