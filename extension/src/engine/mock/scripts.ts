import type {
  Candidate,
  ErrorCode,
  NoMatchReason,
  PitchFrame,
  Segment,
  Service,
  SessionEvent,
  SourceInfo,
  Tier,
  Track,
} from '../types';

/**
 * A recorded or authored event stream. The line format is the `mf` CLI's JSONL output
 * (`{"wall_ms": …, "event": …}`), so streams recorded from the native engine can be
 * replayed by the mock unchanged (`fromRecording`).
 */
export interface TimedEvent {
  wall_ms: number;
  event: SessionEvent;
}

/**
 * How the mock plays one session. `capture` plays while the session listens; it ends
 * on its own (the engine decided it heard enough) or when the UI calls
 * `finishSession`. `outcome` then plays with times relative to its start. The engine
 * emits `Started` itself, so scripts never contain it.
 */
export interface MockScript {
  capture: TimedEvent[];
  outcome: TimedEvent[];
  /** Repeat `capture` until the UI calls `finishSession` (the user stops). */
  loop?: boolean;
}

const at = (wall_ms: number, event: SessionEvent): TimedEvent => ({ wall_ms, event });

/** Levels at about 30 Hz with a music-like envelope (or near silence). */
function levels(fromMs: number, toMs: number, loudness: number): TimedEvent[] {
  const out: TimedEvent[] = [];
  for (let t = fromMs; t < toMs; t += 33) {
    const beat = 0.5 + 0.5 * Math.sin((t / 500) * Math.PI) ** 2;
    const rms = loudness === 0 ? 0 : Math.min(1, loudness * (0.75 + 0.25 * beat));
    out.push(at(t, { Level: { rms, peak: Math.min(1, rms * 1.25) } }));
  }
  return out;
}

/** A short hummed phrase as MIDI notes (pitch, onset ms, duration ms). */
const HUM_PHRASE: [number, number, number][] = [
  [62, 0, 380],
  [64, 400, 380],
  [67, 800, 380],
  [64, 1200, 380],
  [71, 1600, 700],
  [71, 2350, 380],
  [69, 2750, 900],
];

function hummedMidi(tMs: number): number | null {
  const phraseMs = 4000;
  const local = tMs % phraseMs;
  const note = HUM_PHRASE.find(([, onset, dur]) => local >= onset && local < onset + dur);
  if (!note) return null;
  return note[0] + 0.15 * Math.sin(tMs / 40);
}

/** Pitch batches about 10 per second, each with three frames. */
function pitch(fromMs: number, toMs: number): TimedEvent[] {
  const out: TimedEvent[] = [];
  for (let t = fromMs; t < toMs; t += 100) {
    const frames: PitchFrame[] = [0, 33, 66].map((d) => {
      const midi = hummedMidi(t + d);
      return { t_ms: t + d, midi, confidence: midi === null ? 0.1 : 0.9 };
    });
    out.push(at(t, { Pitch: { frames } }));
  }
  return out;
}

function sortByTime(events: TimedEvent[]): TimedEvent[] {
  return events.map((e, i) => [e, i] as const).sort((a, b) => a[0].wall_ms - b[0].wall_ms || a[1] - b[1]).map(([e]) => e);
}

function trackSegment(id: string, track: Track, options: Partial<Segment> = {}): Segment {
  return {
    id,
    item_index: 0,
    start_ms: 0,
    end_ms: 0,
    kind: 'Track',
    track,
    confidence: 0.97,
    tier: 'Confirmed',
    evidence: [],
    alternates: [],
    source_url: null,
    ...options,
  };
}

function unidentifiedSegment(id: string, start_ms: number, end_ms: number): Segment {
  return {
    id,
    item_index: 0,
    start_ms,
    end_ms,
    kind: 'Unidentified',
    track: null,
    confidence: 0,
    tier: 'Unknown',
    evidence: [],
    alternates: [],
    source_url: null,
  };
}

function source(overrides: Partial<SourceInfo>): SourceInfo {
  return {
    url: null,
    title: null,
    uploader: null,
    thumbnail_url: null,
    duration_ms: null,
    service: null,
    site: null,
    item_count: 1,
    hints: [],
    ...overrides,
  };
}

/** Microphone or tab listening that matches `track` after `captureMs`. */
export function matchScript(track: Track, options: { captureMs?: number; rateLimited?: boolean } = {}): MockScript {
  const captureMs = options.captureMs ?? (options.rateLimited ? 6500 : 4200);
  const capture = [at(0, { Phase: { phase: 'Listening', fraction: null } }), ...levels(0, captureMs, 0.62)];
  if (options.rateLimited) capture.push(at(4000, { Notice: { notice: { RateLimited: { retry_in_ms: 2000 } } } }));
  const segment = trackSegment('seg-1', track, {
    start_ms: 0,
    end_ms: captureMs,
    evidence: [{ source: 'Shazam', at_ms: captureMs, score: 0.97, detail: 'offset 1:14, skew 0.1%' }],
  });
  return {
    capture: sortByTime(capture),
    outcome: [
      at(0, { Phase: { phase: 'Recognizing', fraction: null } }),
      at(350, { Segment: { segment } }),
      at(400, { Finished: { outcome: { Matched: { track } } } }),
    ],
  };
}

/** Listening that ends without a match. */
export function noMatchScript(reason: NoMatchReason): MockScript {
  const silent = reason === 'CaptureSilent';
  const capture = [
    at(0, { Phase: { phase: 'Listening', fraction: null } }),
    ...levels(0, 7000, silent ? 0 : reason === 'TooQuiet' ? 0.04 : 0.5),
  ];
  if (silent) capture.push(at(3000, { Notice: { notice: 'CaptureSilent' } }));
  if (reason === 'TooQuiet') capture.push(at(2500, { Notice: { notice: 'MoveCloser' } }));
  return {
    capture: sortByTime(capture),
    outcome: [
      at(0, { Phase: { phase: 'Recognizing', fraction: null } }),
      at(500, { Finished: { outcome: { NoMatch: { reason } } } }),
    ],
  };
}

/** Listening while offline: the capture is kept and the session waits for the network. */
export function waitingScript(captureMs = 4200): MockScript {
  return {
    capture: sortByTime([at(0, { Phase: { phase: 'Listening', fraction: null } }), ...levels(0, captureMs, 0.6)]),
    outcome: [
      at(0, { Phase: { phase: 'Recognizing', fraction: null } }),
      at(600, { Finished: { outcome: 'Waiting' } }),
    ],
  };
}

/** A session that fails with `code`. */
export function failedScript(code: ErrorCode, message: string, afterMs = 600): MockScript {
  return {
    capture: [],
    outcome: [
      at(0, { Phase: { phase: 'Resolving', fraction: null } }),
      at(afterMs, { Failed: { code, message } }),
    ],
  };
}

function alignment(offset: number): Candidate['alignment'] {
  const query: number[] = [];
  const reference: number[] = [];
  for (let t = 0; t < 4000; t += 40) {
    const midi = hummedMidi(t);
    query.push(midi === null ? Number.NaN : midi - 66);
    reference.push(midi === null ? Number.NaN : Math.round(midi) - 66 + offset * Math.sin(t / 300));
  }
  return { frame_ms: 40, query, reference };
}

/** Humming: candidates when the melody index is installed, else a "missing index" no match. */
export function humScript(candidates: Track[], options: { indexInstalled: boolean; captureMs?: number }): MockScript {
  const captureMs = options.captureMs ?? 7000;
  const capture = sortByTime([
    at(0, { Phase: { phase: 'Listening', fraction: null } }),
    at(700, { Notice: { notice: 'ImpressionDetected' } }),
    at(1800, { Notice: { notice: 'KeepGoing' } }),
    ...levels(0, captureMs, 0.45),
    ...pitch(0, captureMs),
  ]);
  if (!options.indexInstalled) {
    return {
      capture,
      outcome: [
        at(0, { Notice: { notice: 'MelodyIndexMissing' } }),
        at(100, { Phase: { phase: 'SearchingLyrics', fraction: null } }),
        at(600, { Finished: { outcome: { NoMatch: { reason: 'NotFound' } } } }),
      ],
    };
  }
  const tiers: Candidate['tier'][] = ['Strong', 'Possible', 'LongShot'];
  const scored: Candidate[] = candidates.slice(0, 3).map((track, i) => ({
    track,
    score: [0.82, 0.41, 0.17][i] ?? 0.1,
    tier: tiers[i] ?? 'LongShot',
    sources: [{ kind: 'Melody', score: [0.82, 0.41, 0.17][i] ?? 0.1, detail: i === 0 ? 'transposed −2, tempo ×0.9' : null }],
    alignment: alignment(i * 0.6),
  }));
  return {
    capture,
    outcome: [
      at(0, { Phase: { phase: 'MatchingMelody', fraction: null } }),
      at(400, { Candidates: { candidates: scored.slice(1), is_final: false } }),
      at(900, { Candidates: { candidates: scored, is_final: true } }),
      at(950, { Finished: { outcome: { Candidates: { count: scored.length } } } }),
    ],
  };
}

/** A typed lyric search. */
export function lyricsScript(text: string, candidates: Track[]): MockScript {
  if (candidates.length === 0) {
    return {
      capture: [],
      outcome: [
        at(0, { Phase: { phase: 'SearchingLyrics', fraction: null } }),
        at(500, { Finished: { outcome: { NoMatch: { reason: 'NotFound' } } } }),
      ],
    };
  }
  const scored: Candidate[] = candidates.slice(0, 2).map((track, i) => ({
    track,
    score: i === 0 ? 0.74 : 0.38,
    tier: i === 0 ? 'Strong' : 'Possible',
    sources: [{ kind: 'Lyrics', score: i === 0 ? 0.74 : 0.38, detail: `“${text.trim().slice(0, 60)}”` }],
    alignment: null,
  }));
  return {
    capture: [],
    outcome: [
      at(0, { Phase: { phase: 'SearchingLyrics', fraction: null } }),
      at(600, { Candidates: { candidates: scored, is_final: true } }),
      at(650, { Finished: { outcome: { Candidates: { count: scored.length } } } }),
    ],
  };
}

/** A streaming-service track link: metadata only, fast. */
export function trackLinkScript(track: Track, url: string, service: Service | null, site: string): MockScript {
  return {
    capture: [],
    outcome: [
      at(0, { Phase: { phase: 'Resolving', fraction: null } }),
      at(300, { Source: { info: source({ url, title: track.title, uploader: track.artist, service, site }) } }),
      at(450, {
        Segment: {
          segment: trackSegment('seg-1', track, {
            confidence: 1,
            evidence: [{ source: 'Metadata', at_ms: 0, score: 1, detail: null }],
          }),
        },
      }),
      at(500, { Finished: { outcome: { Completed: { identified: 1, total: 1 } } } }),
    ],
  };
}

/** An album or playlist link: one Track segment per entry. */
export function collectionScript(tracks: Track[], url: string, service: Service | null, site: string, title: string): MockScript {
  const outcome: TimedEvent[] = [
    at(0, { Phase: { phase: 'Resolving', fraction: null } }),
    at(350, { Source: { info: source({ url, title, uploader: site, service, site, item_count: tracks.length }) } }),
  ];
  tracks.forEach((track, i) => {
    outcome.push(
      at(400 + i * 60, {
        Segment: {
          segment: trackSegment(`seg-${i + 1}`, track, {
            item_index: i,
            confidence: 1,
            evidence: [{ source: 'Metadata', at_ms: 0, score: 1, detail: null }],
          }),
        },
      }),
    );
  });
  outcome.push(
    at(450 + tracks.length * 60, {
      Finished: { outcome: { Completed: { identified: tracks.length, total: tracks.length } } },
    }),
  );
  return { capture: [], outcome };
}

/** A single video or audio page: download, decode, recognize. */
export function mediaScript(track: Track | null, url: string, service: Service | null, site: string, title: string): MockScript {
  const duration = track?.duration_ms ?? 213_000;
  const outcome: TimedEvent[] = [
    at(0, { Phase: { phase: 'Resolving', fraction: null } }),
    at(400, {
      Source: { info: source({ url, title, uploader: track?.artist ?? null, duration_ms: duration, service, site, hints: track ? [track] : [] }) },
    }),
  ];
  for (let i = 0; i <= 5; i++) outcome.push(at(500 + i * 250, { Phase: { phase: 'Downloading', fraction: i / 5 } }));
  outcome.push(at(1800, { Phase: { phase: 'Decoding', fraction: null } }));
  outcome.push(at(2100, { Phase: { phase: 'Recognizing', fraction: null } }));
  for (let i = 1; i <= 3; i++) {
    outcome.push(at(2100 + i * 400, { Progress: { analyzed_ms: Math.round((duration * i) / 3), total_ms: duration, eta_ms: (3 - i) * 400 } }));
  }
  const segment: Segment = track
    ? trackSegment('seg-1', track, {
        end_ms: duration,
        evidence: [
          { source: 'Shazam', at_ms: Math.round(duration * 0.3), score: 0.96, detail: null },
          { source: 'PageHint', at_ms: 0, score: 0.8, detail: 'YouTube "Music" card' },
        ],
        source_url: url,
      })
    : unidentifiedSegment('seg-1', 0, duration);
  outcome.push(at(3400, { Segment: { segment } }));
  outcome.push(at(3500, { Finished: { outcome: { Completed: { identified: track ? 1 : 0, total: 1 } } } }));
  return { capture: [], outcome };
}

/** A long DJ mix: segments across the timeline in every tier, one stitch merge. */
export function mixScript(tracks: Track[], url: string, service: Service | null, site: string, title: string): MockScript {
  const minutes = 60;
  const total = minutes * 60_000;
  const count = 10;
  const length = total / count;
  const tiers: (Tier | 'Unidentified')[] = [
    'Confirmed',
    'Confirmed',
    'Likely',
    'Possible',
    'Confirmed',
    'Unidentified',
    'Confirmed',
    'Likely',
    'Confirmed',
    'Possible',
  ];
  const outcome: TimedEvent[] = [
    at(0, { Phase: { phase: 'Resolving', fraction: null } }),
    at(400, { Source: { info: source({ url, title, uploader: 'Mock DJ', duration_ms: total, service, site }) } }),
  ];
  for (let i = 0; i <= 4; i++) outcome.push(at(500 + i * 300, { Phase: { phase: 'Downloading', fraction: i / 4 } }));
  outcome.push(at(1900, { Phase: { phase: 'Segmenting', fraction: null } }));
  outcome.push(at(2200, { Phase: { phase: 'Recognizing', fraction: null } }));
  let t = 2300;
  let identified = 0;
  tiers.forEach((tier, i) => {
    const start = Math.round(i * length);
    const end = Math.round((i + 1) * length);
    const track = tracks[i % tracks.length]!;
    const base: Segment =
      tier === 'Unidentified'
        ? unidentifiedSegment(`seg-${i + 1}`, start, end)
        : trackSegment(`seg-${i + 1}`, track, {
            start_ms: start,
            end_ms: end,
            tier,
            confidence: tier === 'Confirmed' ? 0.95 : tier === 'Likely' ? 0.7 : 0.45,
            evidence: [{ source: 'Shazam', at_ms: start + 60_000, score: 0.9, detail: null }],
            source_url: `${url}${url.includes('?') ? '&' : '?'}t=${Math.round(start / 1000)}`,
          });
    if (base.kind === 'Track') identified++;
    if (i === 4) {
      // Found in two halves first, then stitched into one.
      const half = Math.round(start + length / 2);
      outcome.push(at(t, { Segment: { segment: { ...base, end_ms: half } } }));
      outcome.push(at(t + 200, { Segment: { segment: { ...base, id: 'seg-5b', start_ms: half } } }));
      outcome.push(at(t + 500, { SegmentRemoved: { id: 'seg-5b' } }));
      outcome.push(at(t + 520, { Segment: { segment: base } }));
      t += 600;
    } else {
      outcome.push(at(t, { Segment: { segment: base } }));
    }
    outcome.push(at(t + 50, { Progress: { analyzed_ms: end, total_ms: total, eta_ms: (count - i - 1) * 600 } }));
    t += 600;
  });
  outcome.push(at(t, { Phase: { phase: 'Finishing', fraction: null } }));
  outcome.push(at(t + 300, { Finished: { outcome: { Completed: { identified, total: count } } } }));
  return { capture: [], outcome };
}

/** The melody index download (not a library session). */
export function melodyInstallScript(): MockScript {
  const outcome: TimedEvent[] = [];
  for (let i = 0; i <= 10; i++) outcome.push(at(i * 300, { Phase: { phase: 'Downloading', fraction: i / 10 } }));
  outcome.push(at(3200, { Finished: { outcome: { Completed: { identified: 1, total: 1 } } } }));
  return { capture: [], outcome };
}

/**
 * A stream recorded from the native engine (`mf … > events.jsonl`, one `{wall_ms,
 * event}` per line). `Started` is dropped because the mock emits its own.
 */
export function fromRecording(lines: readonly TimedEvent[]): MockScript {
  const events = lines.filter((line) => typeof line.event === 'string' || !('Started' in line.event));
  const start = events[0]?.wall_ms ?? 0;
  return { capture: [], outcome: events.map((line) => ({ wall_ms: line.wall_ms - start, event: line.event })) };
}

/** Parse JSONL text from the `mf` CLI. */
export function parseRecording(jsonl: string): TimedEvent[] {
  return jsonl
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const parsed = JSON.parse(line) as { wall_ms?: number; event: SessionEvent };
      return { wall_ms: parsed.wall_ms ?? 0, event: parsed.event };
    });
}
