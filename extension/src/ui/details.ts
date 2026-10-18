import type { Candidate, LibraryEntry, Segment, SessionDetail, Track } from '../engine/types';
import type { Fact } from '../models/session';

/** What the app tab's details column shows. */
export type DetailSubject =
  | { kind: 'track'; track: Track; heard?: Fact[]; sessionId?: string }
  | { kind: 'candidate'; candidate: Candidate; sessionId?: string }
  | { kind: 'segment'; segment: Segment; detail: SessionDetail }
  | { kind: 'item'; track: Track }
  | { kind: 'session'; detail: SessionDetail }
  | { kind: 'many'; entries: LibraryEntry[]; onpin: () => void; ondelete: () => void };
