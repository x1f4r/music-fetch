import { emptyFacts, finite, meta, pickElement, type SiteAdapter } from '../site';

const VERSION = '2026.10';

/** VODs (`/videos/<id>`) can be listened through; a channel page is a live stream. */
export function twitchIsLive(path: string): boolean {
  return !/^\/videos\/\d+/.test(path) && !/\/clip\//.test(path) && !/^\/[^/]+\/clip\//.test(path);
}

export const twitch: SiteAdapter = {
  id: 'twitch',
  version: VERSION,
  element: (_win, doc, played) => pickElement([...doc.querySelectorAll<HTMLMediaElement>('video'), ...played]),
  facts(win, doc) {
    const video = doc.querySelector('video');
    return {
      ...emptyFacts('twitch', VERSION),
      title: meta(doc, 'og:title') ?? doc.title,
      artist: meta(doc, 'og:site_name') === 'Twitch' ? (win.location.pathname.split('/')[1] ?? null) : null,
      description: meta(doc, 'og:description'),
      live: twitchIsLive(win.location.pathname),
      durationMs: video ? (finite(video.duration) ?? 0) * 1000 || null : null,
    };
  },
};
