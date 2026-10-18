import type { CaptureStatus } from '../engine/client';
import type {
  Candidate,
  ErrorCode,
  Evidence,
  LibraryEntry,
  Phase,
  Segment,
  SessionDetail,
  SessionEvent,
  Tier,
  Track,
} from '../engine/types';
import type { Message, MessageKey } from '../i18n';
import { serviceName } from '../providers/services';

/**
 * One session as the session views show it (DESIGN §6.3–6.6, extension plan §5.6): the
 * stored detail, updated live from the session's events while it runs. Pure, so every
 * state of a mix or playlist can be built in tests and demo states.
 */
export interface SessionView {
  detail: SessionDetail;
  running: boolean;
  phase: Phase | null;
  fraction: number | null;
  progress: { analyzed_ms: number; total_ms: number; eta_ms: number | null } | null;
  /** The newest `Item` event (playlists: which entry is being resolved). */
  item: { index: number; count: number; title: string | null } | null;
  capture: CaptureStatus | null;
  error: { code: ErrorCode; message: string } | null;
}

export type SessionShape = 'single' | 'candidates' | 'playlist' | 'mix' | 'empty';

/** Media at least this long is a mix: it gets the timeline (DESIGN §6.5). */
export const MIX_MIN_MS = 10 * 60_000;

export function sessionView(detail: SessionDetail): SessionView {
  return {
    detail,
    running: detail.entry.status === 'Running',
    phase: null,
    fraction: null,
    progress: null,
    item: null,
    capture: null,
    error: null,
  };
}

/** Upsert a segment by id, keeping the list in item and time order (as the store does). */
function upsert(segments: Segment[], segment: Segment): Segment[] {
  const next = segments.filter((s) => s.id !== segment.id);
  next.push(segment);
  return next.sort((a, b) => a.item_index - b.item_index || a.start_ms - b.start_ms);
}

/** Fold one live event of this session into the view. */
export function reduceSession(view: SessionView, event: SessionEvent): SessionView {
  const detail = view.detail;
  if (event === 'Canceled') return { ...view, running: false, detail: { ...detail, entry: { ...detail.entry, status: 'Canceled' } } };
  if ('Started' in event) return { ...view, running: true };
  if ('Phase' in event) return { ...view, phase: event.Phase.phase, fraction: event.Phase.fraction };
  if ('Progress' in event) return { ...view, progress: event.Progress };
  if ('Item' in event) return { ...view, item: event.Item };
  if ('Source' in event) {
    const info = event.Source.info;
    const entry: LibraryEntry = {
      ...detail.entry,
      title: info.title ?? detail.entry.title,
      subtitle: info.uploader ?? info.site ?? detail.entry.subtitle,
      duration_ms: info.duration_ms ?? detail.entry.duration_ms,
      source_url: info.url ?? detail.entry.source_url,
      artwork_url: info.thumbnail_url ?? detail.entry.artwork_url,
    };
    return { ...view, detail: { ...detail, entry, source: info } };
  }
  if ('Segment' in event) return { ...view, detail: { ...detail, segments: upsert(detail.segments, event.Segment.segment) } };
  if ('SegmentRemoved' in event) {
    return { ...view, detail: { ...detail, segments: detail.segments.filter((s) => s.id !== event.SegmentRemoved.id) } };
  }
  if ('Candidates' in event) return { ...view, detail: { ...detail, candidates: event.Candidates.candidates } };
  if ('Failed' in event) {
    return { ...view, running: false, error: event.Failed, detail: { ...detail, entry: { ...detail.entry, status: 'Failed' } } };
  }
  if ('Finished' in event) {
    const outcome = event.Finished.outcome;
    const status =
      outcome === 'Waiting'
        ? 'Waiting'
        : 'Matched' in outcome
          ? 'Matched'
          : 'Candidates' in outcome
            ? 'Candidates'
            : 'Completed' in outcome
              ? 'Completed'
              : 'NoMatch';
    const entry = { ...detail.entry, status } as LibraryEntry;
    if (typeof outcome === 'object' && 'Matched' in outcome) entry.track = outcome.Matched.track;
    if (typeof outcome === 'object' && 'Completed' in outcome) entry.track_count = outcome.Completed.identified;
    return { ...view, running: false, phase: null, fraction: null, detail: { ...detail, entry } };
  }
  return view;
}

const isMusic = (s: Segment) => s.kind === 'Track' || s.kind === 'Unidentified';

/** The total length a mix's timeline spans. */
export function mixDuration(detail: SessionDetail): number {
  const fromSegments = Math.max(0, ...detail.segments.map((s) => s.end_ms));
  return detail.source?.duration_ms ?? detail.entry.duration_ms ?? fromSegments;
}

export function sessionShape(view: SessionView): SessionShape {
  const { detail } = view;
  const items = detail.source?.item_count ?? 1;
  if (items > 1) return 'playlist';
  if (mixDuration(detail) >= MIX_MIN_MS && detail.entry.kind !== 'Hum' && detail.entry.kind !== 'Lyrics') return 'mix';
  const tracks = detail.segments.filter((s) => s.track);
  if (detail.entry.track || tracks.length === 1) return 'single';
  if (detail.candidates.length > 0) return 'candidates';
  if (tracks.length > 1) return 'mix';
  return 'empty';
}

/** The track a single-result session shows. */
export function singleTrack(view: SessionView): Track | null {
  return view.detail.entry.track ?? view.detail.segments.find((s) => s.track)?.track ?? null;
}

// MARK: clock

/** "4:12", "1:01:45". */
export function clock(ms: number): string {
  const s = Math.max(0, Math.floor(ms / 1000));
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const ss = String(s % 60).padStart(2, '0');
  return h > 0 ? `${h}:${String(m).padStart(2, '0')}:${ss}` : `${m}:${ss}`;
}

// MARK: timeline

export type StripClass = 'confirmed' | 'likely' | 'possible' | 'unknown' | 'speech';

export interface StripSegment {
  id: string;
  cls: StripClass;
  /** Fractions of the whole length. */
  left: number;
  width: number;
}

export interface Timeline {
  totalMs: number;
  segments: StripSegment[];
  /** How far the analysis got (0..1) while it runs; null when done. */
  frontier: number | null;
  ticks: { label: string; at: number }[];
}

const TIER_CLASS: Record<Tier, StripClass> = { Confirmed: 'confirmed', Likely: 'likely', Possible: 'possible', Unknown: 'unknown' };

export function stripClass(segment: Segment): StripClass | null {
  if (segment.kind === 'Silence') return null;
  if (segment.kind === 'Speech') return 'speech';
  if (segment.kind === 'Unidentified' || !segment.track) return 'unknown';
  return TIER_CLASS[segment.tier];
}

export function timeline(view: SessionView): Timeline | null {
  const totalMs = mixDuration(view.detail);
  if (totalMs <= 0) return null;
  const segments: StripSegment[] = [];
  for (const segment of view.detail.segments) {
    const cls = stripClass(segment);
    if (!cls || segment.item_index !== 0) continue;
    segments.push({
      id: segment.id,
      cls,
      left: segment.start_ms / totalMs,
      width: Math.max(0, segment.end_ms - segment.start_ms) / totalMs,
    });
  }
  const frontier = view.running && view.progress ? Math.min(1, view.progress.analyzed_ms / Math.max(1, view.progress.total_ms)) : null;
  const ticks = [0, 0.25, 0.5, 0.75, 1].map((at) => ({ at, label: clock(at * totalMs) }));
  return { totalMs, segments, frontier, ticks };
}

// MARK: tracklist

export type TrackAction = 'play' | 'confirm' | 'humIt' | 'none';

export interface MixRow {
  segment: Segment;
  cue: string;
  /** "ID" rows (unidentified music). */
  unknown: boolean;
  /** From the tracklist, beyond what was heard so far: shown at 70 % (DESIGN "provisional rows"). */
  provisional: boolean;
  /** Possible rows carry the tag and ✓ ✕. */
  possible: boolean;
  /** Seconds of an unidentified part, for its subtitle. */
  lengthLabel: string;
}

/** Only page or link attribution backs this segment (a tracklist line, not audio). */
function hintOnly(evidence: Evidence[]): boolean {
  return evidence.length > 0 && evidence.every((e) => e.source === 'PageHint' || e.source === 'Metadata');
}

export function mixRows(view: SessionView): MixRow[] {
  const frontierMs = view.running && view.progress ? view.progress.analyzed_ms : Number.POSITIVE_INFINITY;
  return view.detail.segments
    .filter((s) => isMusic(s) && s.item_index === 0)
    .map((segment) => {
      const unknown = segment.kind === 'Unidentified' || !segment.track;
      const provisional = !unknown && segment.start_ms >= frontierMs && hintOnly(segment.evidence);
      return {
        segment,
        cue: clock(segment.start_ms),
        unknown,
        provisional,
        possible: !unknown && segment.tier === 'Possible',
        lengthLabel: clock(segment.end_ms - segment.start_ms),
      };
    });
}

export interface PlaylistRow {
  index: number;
  title: string;
  artist: string;
  duration: string | null;
  track: Track | null;
  segment: Segment | null;
  /** Not resolved yet (still running): title and artist come from the page's list. */
  pending: boolean;
  /** The entry being resolved right now. */
  resolving: boolean;
}

export function playlistRows(view: SessionView): PlaylistRow[] {
  const byItem = new Map<number, Segment>();
  for (const segment of view.detail.segments) if (segment.track && !byItem.has(segment.item_index)) byItem.set(segment.item_index, segment);
  const hints = view.detail.source?.hints ?? [];
  const count = Math.max(view.detail.source?.item_count ?? 0, hints.length, ...[...byItem.keys()].map((i) => i + 1));
  const rows: PlaylistRow[] = [];
  let resolvingMarked = false;
  for (let index = 0; index < count; index++) {
    const segment = byItem.get(index) ?? null;
    const track = segment?.track ?? null;
    const hint = hints[index] ?? null;
    const shown = track ?? hint;
    if (!shown && !view.running) continue;
    const pending = !track;
    const resolving = pending && view.running && !resolvingMarked;
    if (resolving) resolvingMarked = true;
    rows.push({
      index,
      title: shown?.title ?? '',
      artist: shown?.artist ?? '',
      duration: shown?.duration_ms ? clock(shown.duration_ms) : null,
      track,
      segment,
      pending,
      resolving,
    });
  }
  return rows;
}

// MARK: status row

export interface StatusRow {
  message: Message;
  spinner: boolean;
  action: 'stop' | 'resume' | null;
  /** 0..1 for the small bar (playlists resolving). */
  progress: number | null;
}

export function identifiedCounts(detail: SessionDetail): { identified: number; total: number } {
  const music = detail.segments.filter(isMusic);
  const items = detail.source?.item_count ?? 1;
  if (items > 1) {
    const resolved = new Set(detail.segments.filter((s) => s.track).map((s) => s.item_index));
    return { identified: resolved.size, total: items };
  }
  return { identified: music.filter((s) => s.track).length, total: music.length };
}

const minutes = (ms: number) => Math.round(ms / 60_000);

export function statusRow(view: SessionView, shape: SessionShape = sessionShape(view)): StatusRow | null {
  const { identified, total } = identifiedCounts(view.detail);
  if (!view.running) {
    if (shape !== 'mix' && shape !== 'playlist') return null;
    if (view.detail.entry.status === 'Canceled') return { message: { key: 'mix.stopped', args: [identified] }, spinner: false, action: null, progress: null };
    return { message: { key: 'mix.identified', args: [identified, total] }, spinner: false, action: null, progress: null };
  }
  const capture = view.capture;
  if (capture?.state === 'ad') return { message: { key: 'mix.ad' }, spinner: true, action: 'stop', progress: null };
  if (capture?.state === 'paused') return { message: { key: 'mix.paused' }, spinner: false, action: 'resume', progress: null };
  if (capture?.state === 'tabClosed') return { message: { key: 'mix.tabClosed' }, spinner: false, action: 'resume', progress: null };
  if (shape === 'playlist') {
    const count = view.detail.source?.item_count ?? total;
    return { message: { key: 'mix.resolving', args: [identified, count] }, spinner: true, action: 'stop', progress: count ? identified / count : null };
  }
  if (view.progress && view.progress.total_ms > 0) {
    const done = minutes(view.progress.analyzed_ms);
    const all = Math.max(1, minutes(view.progress.total_ms));
    const message: Message = capture?.rate
      ? { key: 'mix.listenThrough', args: [done, all, String(capture.rate)] }
      : { key: 'mix.listenThroughPlain', args: [done, all] };
    return { message, spinner: true, action: 'stop', progress: null };
  }
  return { message: { key: phaseLine(view.phase) }, spinner: true, action: 'stop', progress: view.fraction };
}

export function phaseLine(phase: Phase | null): MessageKey {
  switch (phase) {
    case 'Resolving':
      return 'state.resolving';
    case 'Downloading':
      return 'state.downloading';
    case 'Decoding':
    case 'Segmenting':
      return 'state.preparing';
    case 'Recognizing':
    case 'MatchingMelody':
    case 'Finishing':
      return 'mix.analyzing';
    case 'SearchingLyrics':
      return 'state.searchingLyrics';
    default:
      return 'state.resolving';
  }
}

// MARK: details

export interface Fact {
  key: MessageKey;
  value: string;
  tnum?: boolean;
}

/** Album, Released, Length, ISRC: whichever the track has. */
export function trackFacts(track: Track): Fact[] {
  const facts: Fact[] = [];
  if (track.album) facts.push({ key: 'result.album', value: track.album });
  if (track.release_year) facts.push({ key: 'result.released', value: String(track.release_year), tnum: true });
  if (track.duration_ms) facts.push({ key: 'result.length', value: clock(track.duration_ms), tnum: true });
  if (track.isrc) facts.push({ key: 'result.isrc', value: track.isrc, tnum: true });
  return facts;
}

/** What identified a segment, for "Matched by" (DESIGN §7 result.matchedBy.*). */
export function matchedBy(evidence: readonly Evidence[], startedAtMs = 0): Message | null {
  const best = [...evidence].sort((a, b) => b.score - a.score)[0];
  if (!best) return null;
  switch (best.source) {
    case 'Shazam': {
      const seconds = Math.round((best.at_ms - startedAtMs) / 1000);
      return seconds > 0 && seconds < 60 ? { key: 'result.matchedBy.shazam', args: [seconds] } : { key: 'result.matchedBy.shazamPlain' };
    }
    case 'AudD':
      return { key: 'result.matchedBy.audd' };
    case 'AcrCloud':
    case 'AcrHumming':
      return { key: 'result.matchedBy.acr' };
    case 'Melody':
      return { key: 'result.matchedBy.melody' };
    case 'Lyrics':
      return { key: 'result.matchedBy.lyrics' };
    case 'Metadata':
      return { key: 'result.matchedBy.metadata' };
    case 'PageHint':
      return { key: 'result.matchedBy.page' };
    case 'User':
      return { key: 'result.matchedBy.user' };
  }
}

function hostOf(url: string | null | undefined): string | null {
  if (!url) return null;
  try {
    return new URL(url).hostname.replace(/^(www|m)\./, '');
  } catch {
    return null;
  }
}

/** "This tab · youtube.com", "Microphone", "A link · open.spotify.com". */
export function heardSource(detail: SessionDetail): Message {
  const entry = detail.entry;
  const host = hostOf(entry.source_url ?? detail.source?.url);
  switch (entry.kind) {
    case 'SystemAudio':
      return host ? { key: 'result.heard.tab', args: [host] } : { key: 'result.heard.tabPlain' };
    case 'Listen':
      return { key: 'result.heard.mic' };
    case 'Hum':
      return { key: 'result.heard.hum' };
    case 'Link':
      return host ? { key: 'result.heard.link', args: [host] } : { key: 'result.heard.file' };
    case 'File':
      return { key: 'result.heard.file' };
    case 'Lyrics':
      return { key: 'result.heard.lyrics' };
  }
}

/** Evidence in a few words: "3 Shazam matches", "Page attribution only". */
export function evidenceSummary(segment: Segment): Message | null {
  if (segment.evidence.some((e) => e.source === 'User')) return { key: 'mix.evidence.user' };
  const shazam = segment.evidence.filter((e) => e.source === 'Shazam').length;
  if (shazam > 1) return { key: 'mix.evidence.shazam.n', args: [shazam] };
  if (shazam === 1) return { key: 'mix.evidence.shazam.one' };
  if (segment.evidence.some((e) => e.source === 'Melody')) return { key: 'mix.evidence.melody' };
  if (hintOnly(segment.evidence)) return { key: 'mix.evidence.hint' };
  return null;
}

export function tierLabel(segment: Segment): MessageKey {
  if (segment.kind === 'Unidentified' || !segment.track) return 'mix.tierUnknown';
  return ({ Confirmed: 'mix.tierConfirmed', Likely: 'mix.tierLikely', Possible: 'mix.tierPossible', Unknown: 'mix.tierUnknown' } as const)[segment.tier];
}

/** Where in the song a hum matched, from the melody source's detail (e.g. "Chorus, bar 1"). */
export function candidateWhere(candidate: Candidate): string | null {
  const melody = candidate.sources.find((s) => s.kind === 'Melody');
  return melody?.detail ?? null;
}

/** The site name of a session's source ("YouTube"), else its host. */
export function sourceSite(detail: SessionDetail): string | null {
  const service = detail.source?.service;
  if (service && service !== 'Web') return serviceName(service);
  return detail.source?.site ?? hostOf(detail.entry.source_url);
}

/** The deep link that plays a mix at `startMs` on its site (YouTube `t=`, SoundCloud `#t=`). */
export function cueLink(detail: SessionDetail, segment: Segment): string | null {
  if (segment.source_url?.startsWith('http')) return segment.source_url;
  const url = detail.source?.url ?? detail.entry.source_url;
  if (!url) return null;
  try {
    const parsed = new URL(url);
    if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') return null;
    const seconds = Math.floor(segment.start_ms / 1000);
    if (/(^|\.)(youtube\.com|youtu\.be)$/.test(parsed.hostname)) {
      parsed.searchParams.set('t', `${seconds}s`);
      return parsed.href;
    }
    parsed.hash = `t=${clock(segment.start_ms)}`;
    return parsed.href;
  } catch {
    return null;
  }
}

/** The tracklist as text, one "cue  Artist – Title" per line (Copy tracklist). */
export function tracklistText(view: SessionView): string {
  const shape = sessionShape(view);
  if (shape === 'playlist') {
    return playlistRows(view)
      .filter((r) => r.track)
      .map((r) => `${r.index + 1}. ${r.artist} – ${r.title}`)
      .join('\n');
  }
  return mixRows(view)
    .filter((r) => r.segment.track)
    .map((r) => `${r.cue}  ${r.segment.track!.artist} – ${r.segment.track!.title}`)
    .join('\n');
}
