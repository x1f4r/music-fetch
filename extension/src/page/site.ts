import type { AdapterId, NowPlaying } from './facts';

/**
 * Site adapters (extension plan §3.3) run in the page's MAIN world, where the players'
 * own objects live (`movie_player`, `ytInitialData`, `__sc_hydration`). They read what
 * the page knows about its music and control the player through the site's API where it
 * has one. Everything they read is a hint: the engine prefers its own fetch and parsers.
 *
 * Fragile selectors stay inside one adapter each, behind its `version`, and every adapter
 * is covered by fixture tests on hand-written pages shaped like the site's.
 */

export interface Chapter {
  title: string;
  startMs: number;
}

export interface Entry {
  title: string;
  artist: string | null;
  url: string | null;
  durationMs: number | null;
}

/** What a site says about the media on the page. */
export interface SiteFacts {
  adapter: AdapterId;
  /** The adapter's version (bumped whenever its selectors change). */
  version: string;
  title: string | null;
  /** Uploader, artist or channel. */
  artist: string | null;
  durationMs: number | null;
  live: boolean;
  /** Free text that may hold a tracklist (a description, a set's notes). */
  description: string | null;
  chapters: Chapter[];
  /** Tracks of a set, album or playlist, in order. */
  entries: Entry[];
  /** A direct audio URL (Bandcamp's stream): the engine may fetch it instead of capturing. */
  mediaUrl: string | null;
  nowPlaying: NowPlaying | null;
}

export interface SiteAdapter {
  readonly id: AdapterId;
  readonly version: string;
  /**
   * The player's media element, if the adapter knows it. `played` holds every element
   * that started playing since the page script arrived, also detached ones
   * (`new Audio()`), which `querySelector` can't see.
   */
  element(win: Window, doc: Document, played: readonly HTMLMediaElement[]): HTMLMediaElement | null;
  facts(win: Window, doc: Document): Partial<SiteFacts>;
  /** Seek through the site's player; false when the element should be used instead. */
  seek?(win: Window, ms: number): boolean;
  play?(win: Window, doc: Document): boolean;
  pause?(win: Window, doc: Document): boolean;
  /** Lower the video quality for a scan; returns what to restore. */
  lowQuality?(win: Window, on: boolean, restore: string | null): string | null;
  /** An ad is playing instead of the content. */
  ad?(win: Window, doc: Document): boolean;
  /** The adapter's own idea of the duration (a player API beats `element.duration` during ads). */
  durationMs?(win: Window): number | null;
  /** Events that mean the page navigated in place (SPA). */
  readonly navigationEvents?: readonly string[];
  /** Get a detached player element to play once, so it can be found (SoundCloud's `new Audio()`). */
  reveal?(win: Window, doc: Document): boolean;
}

export function emptyFacts(adapter: AdapterId, version: string): SiteFacts {
  return {
    adapter,
    version,
    title: null,
    artist: null,
    durationMs: null,
    live: false,
    description: null,
    chapters: [],
    entries: [],
    mediaUrl: null,
    nowPlaying: null,
  };
}

export function text(value: unknown): string | null {
  if (typeof value === 'string') return value.trim() || null;
  if (value && typeof value === 'object') {
    const v = value as { simpleText?: unknown; runs?: { text?: unknown }[] };
    if (typeof v.simpleText === 'string') return v.simpleText.trim() || null;
    if (Array.isArray(v.runs)) return v.runs.map((r) => (typeof r.text === 'string' ? r.text : '')).join('').trim() || null;
  }
  return null;
}

/** "1:02:03", "02:03" or "123" seconds → ms. */
export function clockToMs(value: string | null | undefined): number | null {
  if (!value) return null;
  const parts = value.trim().split(':').map((p) => Number(p));
  if (parts.length === 0 || parts.some((p) => !Number.isFinite(p) || p < 0)) return null;
  return parts.reduce((acc, p) => acc * 60 + p, 0) * 1000;
}

/** ISO 8601 durations as in JSON-LD (`PT1H2M3S`) → ms. */
export function isoDurationMs(value: unknown): number | null {
  if (typeof value !== 'string') return null;
  const m = /^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?$/.exec(value.trim());
  if (!m) return null;
  const [, d, h, min, s] = m;
  const ms = ((Number(d ?? 0) * 24 + Number(h ?? 0)) * 60 + Number(min ?? 0)) * 60_000 + Number(s ?? 0) * 1000;
  return ms > 0 ? Math.round(ms) : null;
}

/** Every object under `root` (depth-first, bounded) that has `key`. */
export function findKey(root: unknown, key: string, limit = 2000): Record<string, unknown>[] {
  const out: Record<string, unknown>[] = [];
  const stack: unknown[] = [root];
  let seen = 0;
  while (stack.length && seen < 200_000 && out.length < limit) {
    const node = stack.pop();
    seen++;
    if (!node || typeof node !== 'object') continue;
    if (Array.isArray(node)) {
      for (let i = node.length - 1; i >= 0; i--) stack.push(node[i]);
      continue;
    }
    const record = node as Record<string, unknown>;
    if (key in record) out.push(record);
    for (const value of Object.values(record)) if (value && typeof value === 'object') stack.push(value);
  }
  return out;
}

/** JSON-LD blocks of the page, flattened (`@graph` included). */
export function jsonLd(doc: Document): Record<string, unknown>[] {
  const out: Record<string, unknown>[] = [];
  for (const script of doc.querySelectorAll('script[type="application/ld+json"]')) {
    try {
      const value = JSON.parse(script.textContent ?? 'null') as unknown;
      const items = Array.isArray(value) ? value : [value];
      for (const item of items) {
        if (!item || typeof item !== 'object') continue;
        const graph = (item as { '@graph'?: unknown })['@graph'];
        if (Array.isArray(graph)) out.push(...(graph.filter((g) => g && typeof g === 'object') as Record<string, unknown>[]));
        else out.push(item as Record<string, unknown>);
      }
    } catch {
      // malformed block
    }
  }
  return out;
}

export function meta(doc: Document, property: string): string | null {
  return doc.querySelector<HTMLMetaElement>(`meta[property="${property}"], meta[name="${property}"]`)?.content?.trim() || null;
}

/** The playing element, else the one with the longest media. */
export function pickElement(candidates: Iterable<HTMLMediaElement>): HTMLMediaElement | null {
  const list = [...candidates];
  const playing = list.find((e) => !e.paused && !e.ended && e.readyState > 2);
  if (playing) return playing;
  const withMedia = list.filter((e) => e.currentSrc || e.src || e.srcObject);
  return withMedia.sort((a, b) => (finite(b.duration) ?? 0) - (finite(a.duration) ?? 0))[0] ?? list[0] ?? null;
}

export function finite(value: number): number | null {
  return Number.isFinite(value) && value > 0 ? value : null;
}

/** `navigator.mediaSession.metadata` as `NowPlaying`. */
export function mediaSessionNowPlaying(win: Window): NowPlaying | null {
  const metadata = win.navigator?.mediaSession?.metadata;
  return metadata?.title ? { title: metadata.title, artist: metadata.artist ?? '', album: metadata.album ?? '' } : null;
}
