import { clockToMs, emptyFacts, findKey, mediaSessionNowPlaying, pickElement, text, type Chapter, type Entry, type SiteAdapter } from '../site';

/** The parts of YouTube's player element API the adapter uses (MAIN world only). */
interface YtPlayer {
  seekTo?(seconds: number, allowSeekAhead: boolean): void;
  getCurrentTime?(): number;
  getDuration?(): number;
  pauseVideo?(): void;
  playVideo?(): void;
  getPlayerResponse?(): unknown;
  getPlaybackQuality?(): string;
  setPlaybackQualityRange?(min: string, max?: string): void;
}

interface YtWindow extends Window {
  ytInitialPlayerResponse?: unknown;
  ytInitialData?: unknown;
}

const VERSION = '2026.10';

function player(doc: Document): YtPlayer | null {
  return (doc.getElementById('movie_player') as unknown as YtPlayer | null) ?? null;
}

interface VideoDetails {
  videoId?: string;
  title?: string;
  author?: string;
  lengthSeconds?: string;
  shortDescription?: string;
  isLive?: boolean;
  isLiveContent?: boolean;
}

/** The current video's player response: the player's own after SPA navigation, else the page's. */
function playerResponse(win: YtWindow, doc: Document): { videoDetails?: VideoDetails } | null {
  try {
    const own = player(doc)?.getPlayerResponse?.();
    if (own && typeof own === 'object') return own as { videoDetails?: VideoDetails };
  } catch {
    // not ready
  }
  const initial = win.ytInitialPlayerResponse;
  return initial && typeof initial === 'object' ? (initial as { videoDetails?: VideoDetails }) : null;
}

/** Chapters from the player bar (`chapterRenderer`), else the description's key moments. */
export function youtubeChapters(data: unknown): Chapter[] {
  const bar = findKey(data, 'chapterRenderer')
    .map((c) => c.chapterRenderer as { title?: unknown; timeRangeStartMillis?: unknown })
    .map((c) => ({ title: text(c.title) ?? '', startMs: Number(c.timeRangeStartMillis) }))
    .filter((c) => c.title && Number.isFinite(c.startMs));
  if (bar.length) return dedupe(bar);
  return dedupe(
    findKey(data, 'macroMarkersListItemRenderer')
      .map((m) => m.macroMarkersListItemRenderer as { title?: unknown; timeDescription?: unknown })
      .map((m) => ({ title: text(m.title) ?? '', startMs: clockToMs(text(m.timeDescription)) ?? NaN }))
      .filter((c) => c.title && Number.isFinite(c.startMs)),
  );
}

function dedupe(chapters: Chapter[]): Chapter[] {
  const seen = new Set<string>();
  return chapters.filter((c) => (seen.has(`${c.startMs}`) ? false : (seen.add(`${c.startMs}`), true))).sort((a, b) => a.startMs - b.startMs);
}

/** Playlist entries on a playlist page (`playlistVideoRenderer`) or beside a video (`playlistPanelVideoRenderer`). */
export function youtubeEntries(data: unknown): Entry[] {
  const rows = [...findKey(data, 'playlistVideoRenderer').map((r) => r.playlistVideoRenderer), ...findKey(data, 'playlistPanelVideoRenderer').map((r) => r.playlistPanelVideoRenderer)];
  const seen = new Set<string>();
  const entries: Entry[] = [];
  for (const raw of rows) {
    const r = raw as { videoId?: string; title?: unknown; shortBylineText?: unknown; lengthText?: unknown; lengthSeconds?: unknown };
    if (!r.videoId || seen.has(r.videoId)) continue;
    seen.add(r.videoId);
    const seconds = Number(r.lengthSeconds);
    entries.push({
      title: text(r.title) ?? '',
      artist: text(r.shortBylineText),
      url: `https://www.youtube.com/watch?v=${r.videoId}`,
      durationMs: Number.isFinite(seconds) && seconds > 0 ? seconds * 1000 : clockToMs(text(r.lengthText)),
    });
  }
  return entries.filter((e) => e.title);
}

export const youtube: SiteAdapter = {
  id: 'youtube',
  version: VERSION,
  element: (_win, doc, played) =>
    doc.querySelector<HTMLVideoElement>('#movie_player video.html5-main-video, #movie_player video, ytmusic-player video, #player-container-id video') ??
    pickElement([...doc.querySelectorAll('video'), ...played]),
  facts(win, doc) {
    const w = win as YtWindow;
    const details = playerResponse(w, doc)?.videoDetails ?? {};
    const seconds = Number(details.lengthSeconds);
    let data = w.ytInitialData;
    // After an in-page navigation ytInitialData may describe the previous video.
    const current = findKey(data, 'currentVideoEndpoint')[0] as { currentVideoEndpoint?: { watchEndpoint?: { videoId?: string } } } | undefined;
    const dataVideo = current?.currentVideoEndpoint?.watchEndpoint?.videoId;
    if (details.videoId && dataVideo && dataVideo !== details.videoId) data = null;
    return {
      ...emptyFacts('youtube', VERSION),
      title: details.title ?? null,
      artist: details.author ?? null,
      durationMs: Number.isFinite(seconds) && seconds > 0 ? seconds * 1000 : null,
      live: details.isLive === true,
      description: details.shortDescription ?? null,
      chapters: youtubeChapters(data),
      entries: youtubeEntries(data),
      nowPlaying: win.location.hostname.startsWith('music.') ? mediaSessionNowPlaying(win) : null,
    };
  },
  seek(win, ms) {
    const p = player(win.document);
    if (!p?.seekTo) return false;
    p.seekTo(ms / 1000, true);
    return true;
  },
  play(_win, doc) {
    const p = player(doc);
    if (!p?.playVideo) return false;
    p.playVideo();
    return true;
  },
  pause(_win, doc) {
    const p = player(doc);
    if (!p?.pauseVideo) return false;
    p.pauseVideo();
    return true;
  },
  lowQuality(win, on, restore) {
    const p = player(win.document);
    if (!p?.setPlaybackQualityRange) return null;
    if (on) {
      const previous = p.getPlaybackQuality?.() ?? null;
      p.setPlaybackQualityRange('tiny', 'tiny');
      return previous;
    }
    const q = restore && restore !== 'unknown' ? restore : 'auto';
    p.setPlaybackQualityRange(q, q);
    return null;
  },
  ad: (_win, doc) => doc.querySelector('#movie_player.ad-showing, .html5-video-player.ad-showing, .html5-video-player.ad-interrupting') !== null,
  durationMs(win) {
    const d = player(win.document)?.getDuration?.();
    return typeof d === 'number' && Number.isFinite(d) && d > 0 ? d * 1000 : null;
  },
  navigationEvents: ['yt-navigate-finish'],
};
