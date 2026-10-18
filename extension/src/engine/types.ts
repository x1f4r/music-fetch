/**
 * Engine records and enums.
 *
 * The core's own types are generated from its serde types into `types.gen.ts`
 * (`core/crates/mfcore-wasm/tests/types.rs`), which is exactly what the wasm facade passes:
 *
 * - field names stay snake_case;
 * - enums are externally tagged: unit variants are strings (`"Canceled"`), data variants
 *   are single-key objects (`{ Segment: { segment } }`);
 * - `Option<T>` is `T | null`; `u64`/`i64` are plain numbers;
 * - `PitchFrame.midi` is NaN when unvoiced (JSON transports carry null).
 *
 * This file adds what only the browser has and the helpers the UI uses.
 */

import type { CaptureSource as CoreCaptureSource } from './types.gen';

export type {
  AnalyzeMode,
  Candidate,
  CandidateSource,
  CandidateSourceKind,
  CandidateTier,
  ErrorCode,
  Evidence,
  EvidenceSource,
  ExportFormat,
  LibraryEntry,
  LibraryQuery,
  ListenMode,
  ListenOptions,
  LyricLine,
  Lyrics,
  MelodyAlignment,
  MelodyIndexStatus,
  NoMatchReason,
  Notice,
  Outcome,
  PageChapter,
  PageMedia,
  Phase,
  PitchFrame,
  Segment,
  SegmentKind,
  Service,
  ServiceLink,
  SessionDetail,
  SessionEvent,
  SessionKind,
  SessionStatus,
  SourceInfo,
  SpeechTranscript,
  StorageUsage,
  Tier,
  Track,
} from './types.gen';
import type { LibraryQuery, Track } from './types.gen';

/**
 * `Tab` is the browser's "This tab" source (extension plan §2.1). The host captures it;
 * the engine hears it as `SystemAudio` (a recording of what plays).
 */
export type CaptureSource = CoreCaptureSource | 'Tab';

/** What the page told us (extension plan §2.8 `PageContext`); filled by the page adapters. */
export interface PageContext {
  tab_id: number | null;
  url: string | null;
  title: string | null;
  /** Adapter facts (now playing, tracklist text, durations), opaque to the UI. */
  facts: Record<string, unknown> | null;
}

/** The keys of an externally tagged enum value: the variant name. */
export function variantOf<T extends string | Record<string, unknown>>(value: T): VariantName<T> {
  return (typeof value === 'string' ? value : Object.keys(value)[0]) as VariantName<T>;
}

export type VariantName<T> = T extends string ? T : keyof T & string;

/** The payload of an externally tagged variant, or undefined when `value` is another variant. */
export function payloadOf<T extends string | Record<string, unknown>, K extends VariantName<T>>(
  value: T,
  variant: K,
): VariantPayload<T, K> | undefined {
  if (typeof value === 'string') return undefined;
  return (value as Record<string, unknown>)[variant] as VariantPayload<T, K> | undefined;
}

export type VariantPayload<T, K extends string> = T extends Record<K, infer P> ? P : never;

/** The query the library views use when they want everything. */
export function libraryQuery(overrides: Partial<LibraryQuery> = {}): LibraryQuery {
  return { kinds: [], search: null, pinned_only: false, limit: 200, offset: 0, ...overrides };
}

/** A track with every optional field empty. */
export function emptyTrack(overrides: Partial<Track> = {}): Track {
  return {
    title: '',
    artist: '',
    artists: [],
    album: null,
    isrc: null,
    duration_ms: null,
    release_year: null,
    genre: null,
    artwork_url: null,
    preview_url: null,
    links: [],
    ids: {},
    identity_key: '',
    ...overrides,
  };
}
