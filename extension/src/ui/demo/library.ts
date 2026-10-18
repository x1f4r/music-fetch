import type { MockSnapshot } from '../../engine/mock/persistence';
import type {
  Candidate,
  Evidence,
  LibraryEntry,
  MelodyAlignment,
  Segment,
  SessionDetail,
  SessionKind,
  SessionStatus,
  SourceInfo,
  Tier,
  Track,
} from '../../engine/types';
import { emptyTrack } from '../../engine/types';
import { contour, MELODY_SECONDS } from '../../engine/mock/melody';
import { searchLinks } from '../../providers/search';
import { COVERS, type CoverName } from './covers';

/**
 * The library behind the demo states: the sessions the design prototypes show (a pinned
 * DJ set, a playlist, hums with candidates, songs heard through the tab and the
 * microphone, a no-match, one waiting for the network), dated relative to today.
 */

interface TrackInput {
  title: string;
  artist: string;
  artists?: string[];
  album?: string;
  year?: number;
  length?: string;
  isrc?: string;
  cover: CoverName | null;
}

function ms(clock: string): number {
  const parts = clock.split(':').map(Number);
  return parts.reduce((total, part) => total * 60 + part, 0) * 1000;
}

export function demoTrack(input: TrackInput): Track {
  const artists = input.artists ?? [input.artist];
  return emptyTrack({
    title: input.title,
    artist: input.artist,
    artists,
    album: input.album ?? null,
    release_year: input.year ?? null,
    duration_ms: input.length ? ms(input.length) : null,
    isrc: input.isrc ?? null,
    artwork_url: input.cover ? COVERS[input.cover] : null,
    links: searchLinks({ title: input.title, artist: input.artist, artists }),
    identity_key: `demo:${input.artist.toLowerCase()}|${input.title.toLowerCase()}`,
  });
}

export const TRACKS = {
  houdini: demoTrack({ title: 'Houdini', artist: 'Dua Lipa', album: 'Radical Optimism', year: 2024, length: '3:05', isrc: 'GBAHT2300520', cover: 'houdini' }),
  takeonme: demoTrack({ title: 'Take On Me', artist: 'a-ha', album: 'Hunting High and Low', year: 1985, length: '3:45', cover: 'takeonme' }),
  dancingqueen: demoTrack({ title: 'Dancing Queen', artist: 'ABBA', album: 'Arrival', year: 1976, length: '3:51', cover: 'dancingqueen' }),
  africa: demoTrack({ title: 'Africa', artist: 'Toto', album: 'Toto IV', year: 1982, length: '4:55', cover: 'africa' }),
  dontstop: demoTrack({ title: "Don't Stop Me Now", artist: 'Queen', album: 'Jazz', year: 1978, length: '3:29', cover: 'dontstop' }),
  blinding: demoTrack({ title: 'Blinding Lights', artist: 'The Weeknd', album: 'After Hours', year: 2020, length: '3:20', cover: 'blinding' }),
  espresso: demoTrack({ title: 'Espresso', artist: 'Sabrina Carpenter', album: "Short n' Sweet", year: 2024, length: '2:55', isrc: 'USUM72401234', cover: 'espresso' }),
  nights: demoTrack({ title: 'Nights', artist: 'Frank Ocean', album: 'Blonde', year: 2016, length: '5:07', cover: 'nights' }),
  mega: demoTrack({ title: 'Megalovania', artist: 'Toby Fox', album: 'Undertale Soundtrack', year: 2015, length: '2:36', cover: 'mega' }),
  heart: demoTrack({ title: 'My Heart Will Go On', artist: 'Céline Dion', album: "Let's Talk About Love", year: 1997, length: '4:40', cover: 'heart' }),
  wakemeup: demoTrack({ title: 'Wake Me Up', artist: 'Avicii', album: 'True', year: 2013, length: '4:07', cover: 'wakemeup' }),
} as const;

/** Today at `hh:mm` local time, or `daysAgo` days before. */
export function at(clock: string, daysAgo = 0, now = Date.now()): number {
  const [h = 0, m = 0] = clock.split(':').map(Number);
  const d = new Date(now);
  d.setDate(d.getDate() - daysAgo);
  d.setHours(h, m, 0, 0);
  return d.getTime();
}

function info(overrides: Partial<SourceInfo>): SourceInfo {
  return { url: null, title: null, uploader: null, thumbnail_url: null, duration_ms: null, service: null, site: null, item_count: 1, hints: [], ...overrides };
}

function evidence(source: Evidence['source'], at_ms: number, score = 0.95, detail: string | null = null): Evidence {
  return { source, at_ms, score, detail };
}

function trackSegment(id: string, track: Track, overrides: Partial<Segment> = {}): Segment {
  return {
    id,
    item_index: 0,
    start_ms: 0,
    end_ms: track.duration_ms ?? 0,
    kind: 'Track',
    track,
    confidence: 0.96,
    tier: 'Confirmed',
    evidence: [],
    alternates: [],
    source_url: null,
    ...overrides,
  };
}

interface SessionInput {
  id: string;
  kind: SessionKind;
  status: SessionStatus;
  created: number;
  title: string;
  subtitle?: string | null;
  artwork?: string | null;
  track?: Track | null;
  trackCount?: number;
  duration?: number | null;
  url?: string | null;
  pinned?: boolean;
  source?: SourceInfo | null;
  segments?: Segment[];
  candidates?: Candidate[];
}

function session(input: SessionInput): SessionDetail {
  const entry: LibraryEntry = {
    id: input.id,
    kind: input.kind,
    status: input.status,
    created_at_ms: input.created,
    title: input.title,
    subtitle: input.subtitle ?? input.track?.artist ?? null,
    artwork_url: input.artwork ?? input.track?.artwork_url ?? null,
    track: input.track ?? null,
    track_count: input.trackCount ?? (input.track ? 1 : 0),
    duration_ms: input.duration ?? null,
    source_url: input.url ?? null,
    pinned: input.pinned ?? false,
  };
  return { entry, source: input.source ?? null, segments: input.segments ?? [], candidates: input.candidates ?? [], transcript: null };
}

/** A heard song: one Shazam-confirmed segment. */
function heard(id: string, kind: SessionKind, track: Track, created: number, options: Partial<SessionInput> = {}): SessionDetail {
  return session({
    id,
    kind,
    status: 'Matched',
    created,
    title: track.title,
    track,
    segments: [trackSegment('seg-1', track, { end_ms: 8000, evidence: [evidence('Shazam', 8000, 0.97, 'offset 1:14')] })],
    ...options,
  });
}

// MARK: hums

/** The hum (with a voice's wander) and the song's line, as the overlay draws them. */
export function alignment(seed: number, drift = 0.6): MelodyAlignment {
  const query = contour({ from: 0, to: MELODY_SECONDS, stepMs: 40, seed: 11 + seed, jitter: 0.5, vibrato: 0.15 });
  const reference = contour({ from: 0, to: MELODY_SECONDS, stepMs: 40, seed: 1, jitter: 0, vibrato: 0, slide: 0.02 }).map((v, i) =>
    Number.isFinite(v) ? v + drift * Math.sin((i + seed * 17) / 14) : v,
  );
  return { frame_ms: 40, query, reference };
}

export function humCandidates(): Candidate[] {
  const tiers: [Track, Candidate['tier'], number, string | null][] = [
    [TRACKS.takeonme, 'Strong', 0.82, 'Chorus, bar 1'],
    [TRACKS.dancingqueen, 'Possible', 0.46, null],
    [TRACKS.africa, 'Possible', 0.41, null],
    [TRACKS.dontstop, 'LongShot', 0.17, null],
  ];
  return tiers.map(([track, tier, score, detail], i) => ({
    track,
    tier,
    score,
    sources: [{ kind: 'Melody', score, detail }],
    alignment: alignment(i, 0.4 + i * 0.5),
  }));
}

// MARK: the mix

interface MixLine {
  cue: string;
  title?: string;
  artist?: string;
  artists?: string[];
  cover?: CoverName;
  tier?: Tier;
  album?: string;
  year?: number;
  kind?: 'speech' | 'unknown';
}

const MIX_LINES: MixLine[] = [
  { cue: '0:00', title: "Marea (We've Lost Dancing)", artist: 'Fred again..', artists: ['Fred again..', 'The Blessed Madonna'], cover: 'marea', tier: 'Confirmed', year: 2021 },
  { cue: '4:12', title: 'Kyle (I Found You)', artist: 'Fred again..', cover: 'kyle', tier: 'Likely', year: 2021 },
  { cue: '8:30', title: 'Jungle', artist: 'Fred again..', cover: 'jungle', tier: 'Confirmed', year: 2022 },
  { cue: '12:45', kind: 'unknown' },
  { cue: '15:05', title: 'Lights Out', artist: 'Fred again..', artists: ['Fred again..', 'Romy', 'HAAi'], cover: 'lightsout', tier: 'Confirmed', year: 2021 },
  { cue: '21:05', kind: 'speech' },
  { cue: '22:20', title: 'Delilah (pull me out of this)', artist: 'Fred again..', cover: 'delilah', tier: 'Likely', year: 2022 },
  { cue: '27:24', title: 'Turn On The Lights again..', artist: 'Fred again..', artists: ['Fred again..', 'Swedish House Mafia'], cover: 'turnon', tier: 'Confirmed', year: 2022 },
  { cue: '33:00', title: 'Rumble', artist: 'Skrillex', artists: ['Skrillex', 'Fred again..', 'Flowdan'], cover: 'rumble', tier: 'Possible', year: 2023 },
  { cue: '38:24', title: 'Levitating', artist: 'Dua Lipa', cover: 'levitating', tier: 'Confirmed', year: 2020 },
  { cue: '44:54', title: 'Starboy', artist: 'The Weeknd', artists: ['The Weeknd', 'Daft Punk'], cover: 'starboy', tier: 'Likely', year: 2016 },
  { cue: '50:36', title: 'Danielle (smile on my face)', artist: 'Fred again..', cover: 'kyle', tier: 'Confirmed', year: 2023 },
  { cue: '58:30', kind: 'unknown' },
  { cue: '1:01:45', title: 'Baby again..', artist: 'Fred again..', artists: ['Fred again..', 'Skrillex', 'Four Tet'], cover: 'jungle', tier: 'Confirmed', album: 'Baby again..', year: 2023 },
  { cue: '1:08:48', title: 'leavemealone', artist: 'Fred again..', artists: ['Fred again..', 'Baby Keem'], cover: 'turnon', tier: 'Likely', year: 2023 },
  { cue: '1:17:00', title: 'adore u', artist: 'Fred again..', artists: ['Fred again..', 'Obongjayar'], cover: 'delilah', tier: 'Confirmed', year: 2023 },
];

export const MIX_URL = 'https://www.youtube.com/watch?v=c0-hvBcPbdc';
export const MIX_TITLE = 'Fred again.. | Boiler Room: London';
export const MIX_LENGTH = ms('1:30:12');

function mixSegments(options: { analyzedMs?: number; provisionalFrom?: number } = {}): Segment[] {
  const analyzed = options.analyzedMs ?? Number.POSITIVE_INFINITY;
  const segments: Segment[] = [];
  MIX_LINES.forEach((line, i) => {
    const start = ms(line.cue);
    const end = i + 1 < MIX_LINES.length ? ms(MIX_LINES[i + 1]!.cue) : MIX_LENGTH;
    const id = `mix-${i + 1}`;
    const base = { id, item_index: 0, start_ms: start, end_ms: end, alternates: [], source_url: null };
    const provisional = start >= analyzed;
    if (provisional && line.kind) return;
    if (line.kind === 'speech') {
      segments.push({ ...base, end_ms: start + 50_000, kind: 'Speech', track: null, confidence: 0, tier: 'Unknown', evidence: [] });
      return;
    }
    if (line.kind === 'unknown') {
      segments.push({ ...base, kind: 'Unidentified', track: null, confidence: 0, tier: 'Unknown', evidence: [] });
      return;
    }
    const track = demoTrack({
      title: line.title!,
      artist: line.artist!,
      artists: line.artists,
      album: line.album ?? line.title,
      year: line.year,
      cover: line.cover ?? null,
    });
    const shazams = line.tier === 'Confirmed' ? 3 : line.tier === 'Likely' ? 2 : 1;
    segments.push({
      ...base,
      kind: 'Track',
      track,
      tier: provisional ? 'Possible' : (line.tier ?? 'Confirmed'),
      confidence: provisional ? 0.3 : line.tier === 'Possible' ? 0.45 : line.tier === 'Likely' ? 0.72 : 0.95,
      evidence: provisional
        ? [evidence('PageHint', start, 0.3, 'tracklist in the description')]
        : Array.from({ length: shazams }, (_, k) => evidence('Shazam', start + 40_000 + k * 30_000, 0.9)),
    });
  });
  return segments;
}

export function mixSession(options: { running?: boolean; pinned?: boolean } = {}): SessionDetail {
  const running = options.running ?? false;
  const analyzedMs = running ? ms('23:00') : undefined;
  const segments = mixSegments({ analyzedMs });
  const identified = segments.filter((s) => s.track && s.start_ms < (analyzedMs ?? Infinity)).length;
  return session({
    id: running ? 'demo-mix-running' : 'demo-mix',
    kind: 'Link',
    status: running ? 'Running' : 'Completed',
    created: at('13:05'),
    title: MIX_TITLE,
    subtitle: 'YouTube',
    artwork: COVERS.boiler,
    trackCount: identified,
    duration: MIX_LENGTH,
    url: MIX_URL,
    pinned: options.pinned ?? !running,
    source: info({ url: MIX_URL, title: MIX_TITLE, uploader: 'Boiler Room', thumbnail_url: COVERS.boiler, duration_ms: MIX_LENGTH, service: 'YouTube', site: 'YouTube' }),
    segments,
  });
}

// MARK: the playlist

const PLAYLIST: [string, string][] = [
  ['Taste', '2:37'],
  ['Please Please Please', '3:06'],
  ['Good Graces', '3:05'],
  ['Sharpest Tool', '3:18'],
  ['Coincidence', '3:10'],
  ['Bed Chem', '2:51'],
  ['Espresso', '2:55'],
  ['Dumb & Poetic', '3:14'],
  ['Slim Pickins', '2:38'],
  ['Juno', '3:43'],
  ['Lie To Girls', '3:12'],
  ["Don't Smile", '3:28'],
];

export const PLAYLIST_URL = 'https://open.spotify.com/album/demo';

export function playlistSession(options: { resolved?: number } = {}): SessionDetail {
  const resolved = options.resolved ?? PLAYLIST.length;
  const tracks = PLAYLIST.map(([title, length]) =>
    title === 'Espresso'
      ? TRACKS.espresso
      : demoTrack({ title, artist: 'Sabrina Carpenter', album: "Short n' Sweet", year: 2024, length, cover: 'shortnsweet' }),
  );
  const running = resolved < tracks.length;
  return session({
    id: running ? 'demo-playlist-running' : 'demo-playlist',
    kind: 'Link',
    status: running ? 'Running' : 'Completed',
    created: at('22:10', 1),
    title: "Short n' Sweet",
    subtitle: 'Sabrina Carpenter',
    artwork: COVERS.shortnsweet,
    trackCount: resolved,
    duration: tracks.reduce((sum, t) => sum + (t.duration_ms ?? 0), 0),
    url: PLAYLIST_URL,
    source: info({
      url: PLAYLIST_URL,
      title: "Short n' Sweet",
      uploader: 'Sabrina Carpenter',
      thumbnail_url: COVERS.shortnsweet,
      service: 'Spotify',
      site: 'Spotify',
      item_count: tracks.length,
      hints: tracks.map((t) => ({ ...t, artwork_url: null, links: [] })),
    }),
    segments: tracks.slice(0, resolved).map((track, i) =>
      trackSegment(`item-${i + 1}`, track, { item_index: i, end_ms: track.duration_ms ?? 0, confidence: 1, evidence: [evidence('Metadata', 0, 1)] }),
    ),
  });
}

// MARK: the whole library

export function demoSessions(): SessionDetail[] {
  const houdini = heard('demo-houdini', 'SystemAudio', TRACKS.houdini, at('14:32'), { url: 'https://www.youtube.com/', pinned: true });
  const takeonme = session({
    id: 'demo-hum',
    kind: 'Hum',
    status: 'Candidates',
    created: at('11:48'),
    title: 'Take On Me',
    subtitle: 'a-ha',
    artwork: TRACKS.takeonme.artwork_url,
    candidates: humCandidates(),
  });
  return [
    houdini,
    mixSession(),
    takeonme,
    heard('demo-blinding', 'Listen', TRACKS.blinding, at('09:41')),
    heard('demo-espresso', 'SystemAudio', TRACKS.espresso, at('08:12'), { url: 'https://www.tiktok.com/' }),
    playlistSession(),
    session({
      id: 'demo-mega',
      kind: 'Hum',
      status: 'Matched',
      created: at('20:03', 1),
      title: TRACKS.mega.title,
      track: TRACKS.mega,
      segments: [trackSegment('seg-1', TRACKS.mega, { evidence: [evidence('Melody', 9000, 0.81, 'transposed −2')] })],
    }),
    session({ id: 'demo-nomatch', kind: 'Listen', status: 'NoMatch', created: at('18:30', 1), title: 'No match', subtitle: null }),
    session({
      id: 'demo-heart',
      kind: 'Hum',
      status: 'Matched',
      created: at('12:12', 1),
      title: TRACKS.heart.title,
      track: TRACKS.heart,
      segments: [trackSegment('seg-1', TRACKS.heart, { evidence: [evidence('Melody', 11000, 0.77)] })],
    }),
    heard('demo-nights', 'Listen', TRACKS.nights, at('09:12', 1), { pinned: true }),
    session({ id: 'demo-waiting', kind: 'Listen', status: 'Waiting', created: at('19:20', 2), title: 'Microphone', subtitle: null }),
    heard('demo-wake', 'SystemAudio', TRACKS.wakemeup, at('17:02', 2), { url: 'https://soundcloud.com/' }),
  ].sort((a, b) => b.entry.created_at_ms - a.entry.created_at_ms);
}

export interface DemoLibraryOptions {
  /** Which sessions: everything, nothing (first run, empty library), or a chosen list. */
  sessions?: 'all' | 'none' | SessionDetail[];
  melody?: 'none' | 'installed' | 'update';
}

export const DEMO_MELODY = { version: '2026.10.1', songs: 41_234, bytes: 55_200_000 } as const;

export function demoSnapshot(options: DemoLibraryOptions = {}): MockSnapshot {
  const sessions = options.sessions === 'none' ? [] : options.sessions === 'all' || !options.sessions ? demoSessions() : options.sessions;
  const melody = options.melody ?? 'installed';
  return {
    version: 1,
    sessions,
    lowPower: false,
    melody:
      melody === 'none'
        ? { installed: false, version: null, song_count: 0, size_bytes: 0, available_version: null, download_bytes: DEMO_MELODY.bytes }
        : {
            installed: true,
            version: DEMO_MELODY.version,
            song_count: DEMO_MELODY.songs,
            size_bytes: DEMO_MELODY.bytes,
            available_version: melody === 'update' ? '2026.11.1' : null,
            download_bytes: melody === 'update' ? 58_000_000 : null,
          },
  };
}
