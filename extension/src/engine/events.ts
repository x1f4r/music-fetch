import { payloadOf, variantOf, type SessionEvent } from './types';

export type EventName = SessionEvent extends infer E ? (E extends string ? E : keyof E & string) : never;

export function eventName(event: SessionEvent): EventName {
  return variantOf(event) as EventName;
}

export function isTerminal(event: SessionEvent): boolean {
  const name = eventName(event);
  return name === 'Finished' || name === 'Failed' || name === 'Canceled';
}

/** The payload of `event` when it is the `name` variant. */
export function eventPayload<N extends Exclude<EventName, 'Canceled'>>(
  event: SessionEvent,
  name: N,
): Extract<SessionEvent, Record<N, unknown>>[N] | undefined {
  return payloadOf(event as Exclude<SessionEvent, 'Canceled'>, name as never) as never;
}

/**
 * Checks one session's event stream against the guarantees in docs/CORE_API.md
 * ("Events"). Returns human-readable violations; empty means the stream is valid.
 * Used on the mock's streams now and on recorded engine streams later.
 */
export function checkEventContract(events: readonly SessionEvent[]): string[] {
  const problems: string[] = [];
  if (events.length === 0) return ['no events'];
  if (eventName(events[0]!) !== 'Started') problems.push('the first event is not Started');
  const terminals = events.map((e, i) => (isTerminal(e) ? i : -1)).filter((i) => i >= 0);
  if (terminals.length !== 1) problems.push(`expected exactly one terminal event, got ${terminals.length}`);
  else if (terminals[0] !== events.length - 1) problems.push('the terminal event is not the last one');
  if (events.slice(1).some((e) => eventName(e) === 'Started')) problems.push('Started appears more than once');

  let analyzed = 0;
  let lastCandidatesFinal: boolean | null = null;
  const segments = new Set<string>();
  for (const event of events) {
    const progress = eventPayload(event, 'Progress');
    if (progress) {
      if (progress.analyzed_ms < analyzed) problems.push('Progress.analyzed_ms went backwards');
      analyzed = progress.analyzed_ms;
    }
    const segment = eventPayload(event, 'Segment');
    if (segment) segments.add(segment.segment.id);
    const removed = eventPayload(event, 'SegmentRemoved');
    if (removed) {
      if (!segments.has(removed.id)) problems.push(`SegmentRemoved for unknown segment ${removed.id}`);
      segments.delete(removed.id);
    }
    const candidates = eventPayload(event, 'Candidates');
    if (candidates) lastCandidatesFinal = candidates.is_final;
    const finished = eventPayload(event, 'Finished');
    if (finished && variantOf(finished.outcome) === 'Candidates' && lastCandidatesFinal !== true) {
      problems.push('Finished(Candidates) is not preceded by a final Candidates event');
    }
  }
  return problems;
}
