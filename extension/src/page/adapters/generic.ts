import { emptyFacts, isoDurationMs, jsonLd, mediaSessionNowPlaying, meta, pickElement, text, type SiteAdapter, type SiteFacts } from '../site';

const MEDIA_TYPES = new Set(['VideoObject', 'AudioObject', 'MusicRecording', 'PodcastEpisode', 'Episode', 'MusicVideoObject']);

function typesOf(item: Record<string, unknown>): string[] {
  const t = item['@type'];
  return Array.isArray(t) ? t.map(String) : t ? [String(t)] : [];
}

function nameOf(value: unknown): string | null {
  if (Array.isArray(value)) return value.map(nameOf).filter(Boolean).join(', ') || null;
  if (value && typeof value === 'object') return text((value as { name?: unknown }).name);
  return text(value);
}

/** JSON-LD media objects and Open Graph, the facts any page may declare. */
export function genericFacts(win: Window, doc: Document): Partial<SiteFacts> {
  const facts: Partial<SiteFacts> = { nowPlaying: mediaSessionNowPlaying(win) };
  const item = jsonLd(doc).find((i) => typesOf(i).some((t) => MEDIA_TYPES.has(t)));
  if (item) {
    facts.title = text(item.name);
    facts.artist = nameOf(item.byArtist ?? item.author ?? item.creator);
    facts.durationMs = isoDurationMs(item.duration);
    facts.description = text(item.description);
    const url = text(item.contentUrl);
    if (url && /\.(mp3|m4a|aac|ogg|opus|flac|wav)(\?|$)/i.test(url)) facts.mediaUrl = url;
    facts.live = Boolean((item.publication as { isLiveBroadcast?: unknown } | undefined)?.isLiveBroadcast);
  }
  facts.title ??= meta(doc, 'og:title');
  facts.description ??= meta(doc, 'og:description') ?? meta(doc, 'description');
  facts.mediaUrl ??= meta(doc, 'og:audio') ?? meta(doc, 'og:audio:url');
  const seconds = Number(meta(doc, 'music:duration') ?? meta(doc, 'video:duration') ?? NaN);
  if (!facts.durationMs && Number.isFinite(seconds) && seconds > 0) facts.durationMs = seconds * 1000;
  return facts;
}

export const generic: SiteAdapter = {
  id: 'generic',
  version: '1',
  element: (_win, doc, played) => pickElement([...doc.querySelectorAll<HTMLMediaElement>('audio, video'), ...played.filter((e) => !e.isConnected)]),
  facts: (win, doc) => ({ ...emptyFacts('generic', '1'), ...genericFacts(win, doc) }),
};
