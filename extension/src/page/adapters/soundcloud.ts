import { emptyFacts, mediaSessionNowPlaying, pickElement, text, type Entry, type SiteAdapter } from '../site';

interface Hydratable {
  hydratable?: string;
  data?: Record<string, unknown>;
}

interface ScSound {
  title?: string;
  user?: { username?: string };
  duration?: number;
  full_duration?: number;
  permalink_url?: string;
  description?: string;
}

const VERSION = '2026.10';

function hydration(win: Window): Hydratable[] {
  const h = (win as Window & { __sc_hydration?: unknown }).__sc_hydration;
  return Array.isArray(h) ? (h as Hydratable[]) : [];
}

function entry(sound: ScSound): Entry | null {
  if (!sound.title) return null;
  return { title: sound.title, artist: sound.user?.username ?? null, url: sound.permalink_url ?? null, durationMs: sound.full_duration ?? sound.duration ?? null };
}

export const soundcloud: SiteAdapter = {
  id: 'soundcloud',
  version: VERSION,
  // The player is a detached `new Audio()`: only the play hook sees it.
  element: (_win, doc, played) => pickElement([...played, ...doc.querySelectorAll<HTMLMediaElement>('audio')]),
  facts(win, doc) {
    const h = hydration(win);
    const sound = h.find((x) => x.hydratable === 'sound')?.data as ScSound | undefined;
    const set = h.find((x) => x.hydratable === 'playlist')?.data as (ScSound & { tracks?: ScSound[] }) | undefined;
    const main = sound ?? set;
    const badgeTitle = doc.querySelector<HTMLAnchorElement>('.playbackSoundBadge__titleLink');
    const badgeArtist = doc.querySelector('.playbackSoundBadge__lightLink');
    const nowPlaying = badgeTitle
      ? { title: text(badgeTitle.title) ?? text(badgeTitle.textContent) ?? '', artist: text(badgeArtist?.textContent) ?? '', album: '' }
      : mediaSessionNowPlaying(win);
    return {
      ...emptyFacts('soundcloud', VERSION),
      title: main?.title ?? null,
      artist: main?.user?.username ?? null,
      durationMs: main?.full_duration ?? main?.duration ?? null,
      description: main?.description ?? null,
      entries: (set?.tracks ?? []).map(entry).filter((e): e is Entry => e !== null),
      nowPlaying: nowPlaying?.title ? nowPlaying : null,
    };
  },
  reveal(_win, doc) {
    const control = doc.querySelector<HTMLElement>('.playControl, .sc-button-play');
    if (!control) return false;
    const playing = control.classList.contains('playing') || control.getAttribute('aria-pressed') === 'true';
    // Pause and play again, or just play: either way the player calls play() on its element.
    if (playing) control.click();
    control.click();
    return true;
  },
};
