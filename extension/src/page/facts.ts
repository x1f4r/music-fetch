/**
 * What a page says about its music, read in the page (extension plan §3.3). The site
 * adapters (YouTube, SoundCloud, Mixcloud, Twitch, Bandcamp, streaming now-playing) are
 * task X4; this is the generic reader they build on and the message shape they share.
 */

export type AdapterId = 'youtube' | 'soundcloud' | 'mixcloud' | 'twitch' | 'bandcamp' | 'streaming' | 'generic';

export interface NowPlaying {
  title: string;
  artist: string;
  album: string;
}

export interface MediaFact {
  tag: 'audio' | 'video';
  /** Seconds; null while unknown or for live streams. */
  duration: number | null;
  currentTime: number;
  paused: boolean;
  muted: boolean;
}

export interface PageFacts {
  adapter: AdapterId;
  url: string;
  title: string;
  /** Open Graph title and type, when the page declares them. */
  ogTitle: string | null;
  ogType: string | null;
  /** `navigator.mediaSession.metadata`: an exact answer on streaming sites (DRM-proof). */
  nowPlaying: NowPlaying | null;
  media: MediaFact[];
}

const STREAMING = /(^|\.)(open\.spotify\.com|music\.apple\.com|deezer\.com|tidal\.com|music\.amazon\.[a-z.]+)$/;

export function adapterFor(hostname: string): AdapterId {
  const host = hostname.toLowerCase();
  if (/(^|\.)(youtube\.com|youtu\.be)$/.test(host)) return 'youtube';
  if (/(^|\.)soundcloud\.com$/.test(host)) return 'soundcloud';
  if (/(^|\.)mixcloud\.com$/.test(host)) return 'mixcloud';
  if (/(^|\.)twitch\.tv$/.test(host)) return 'twitch';
  if (/(^|\.)bandcamp\.com$/.test(host)) return 'bandcamp';
  if (STREAMING.test(host)) return 'streaming';
  return 'generic';
}

function meta(doc: Document, property: string): string | null {
  return doc.querySelector<HTMLMetaElement>(`meta[property="${property}"], meta[name="${property}"]`)?.content?.trim() || null;
}

export function readPageFacts(doc: Document, mediaSession: MediaSession | undefined): PageFacts {
  const metadata = mediaSession?.metadata;
  const nowPlaying = metadata && metadata.title ? { title: metadata.title, artist: metadata.artist, album: metadata.album } : null;
  const media = [...doc.querySelectorAll<HTMLMediaElement>('audio, video')].map((element) => ({
    tag: element.tagName === 'VIDEO' ? ('video' as const) : ('audio' as const),
    duration: Number.isFinite(element.duration) ? element.duration : null,
    currentTime: element.currentTime,
    paused: element.paused,
    muted: element.muted,
  }));
  return {
    adapter: adapterFor(doc.location?.hostname ?? ''),
    url: doc.location?.href ?? '',
    title: doc.title,
    ogTitle: meta(doc, 'og:title'),
    ogType: meta(doc, 'og:type'),
    nowPlaying,
    media,
  };
}

/** The MAIN-world script posts facts to the isolated capture script with this message. */
export const PAGE_FACTS_MESSAGE = 'mf:page-facts';

export interface PageFactsMessage {
  type: typeof PAGE_FACTS_MESSAGE;
  /**
   * A per-injection nonce so stray page messages are ignored. The page can read it, so
   * facts stay hints: the engine checks them against its own fetch (plan §3.3).
   */
  nonce: string;
  facts: PageFacts;
}

export function isPageFactsMessage(data: unknown, nonce: string): data is PageFactsMessage {
  const message = data as Partial<PageFactsMessage> | null;
  return typeof message === 'object' && message !== null && message.type === PAGE_FACTS_MESSAGE && message.nonce === nonce && typeof message.facts === 'object';
}
