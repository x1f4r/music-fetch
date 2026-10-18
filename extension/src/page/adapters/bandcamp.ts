import { emptyFacts, pickElement, type Entry, type SiteAdapter } from '../site';

const VERSION = '2026.10';

interface TrAlbum {
  artist?: string;
  current?: { title?: string; artist?: string | null };
  trackinfo?: { title?: string; duration?: number; title_link?: string; file?: Record<string, string> | null; artist?: string | null }[];
  url?: string;
}

/** The release data Bandcamp puts on every album and track page (`data-tralbum`). */
export function tralbum(doc: Document): TrAlbum | null {
  const raw = doc.querySelector('script[data-tralbum]')?.getAttribute('data-tralbum');
  if (!raw) return null;
  try {
    return JSON.parse(raw) as TrAlbum;
  } catch {
    return null;
  }
}

export const bandcamp: SiteAdapter = {
  id: 'bandcamp',
  version: VERSION,
  element: (_win, doc, played) => pickElement([...doc.querySelectorAll<HTMLMediaElement>('audio'), ...played]),
  facts(win, doc) {
    const data = tralbum(doc);
    const tracks = data?.trackinfo ?? [];
    const origin = win.location.origin;
    const entries: Entry[] = tracks
      .filter((t) => t.title)
      .map((t) => ({
        title: t.title!,
        artist: t.artist ?? data?.artist ?? null,
        url: t.title_link ? new URL(t.title_link, origin).href : null,
        durationMs: t.duration ? Math.round(t.duration * 1000) : null,
      }));
    const single = tracks.length === 1 ? tracks[0] : undefined;
    return {
      ...emptyFacts('bandcamp', VERSION),
      title: data?.current?.title ?? null,
      artist: data?.current?.artist ?? data?.artist ?? null,
      durationMs: single?.duration ? Math.round(single.duration * 1000) : entries.reduce((sum, e) => sum + (e.durationMs ?? 0), 0) || null,
      entries: tracks.length > 1 ? entries : [],
      // The 128 kbps stream: the engine can fetch and decode it (Direct), no capture needed.
      mediaUrl: single?.file?.['mp3-128'] ?? null,
    };
  },
};
