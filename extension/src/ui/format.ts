import type { SessionDetail } from '../engine/types';
import { t, tm, type Message } from '../i18n';
import { dayLabel } from '../models/library';
import { heardSource, matchedBy, type Fact } from '../models/session';

/** A message or a plain string, as models return them; parts are joined with " · ". */
export function text(value: Message | string | readonly (Message | string)[]): string {
  if (Array.isArray(value)) return value.map((part) => text(part)).filter(Boolean).join(' · ');
  return typeof value === 'string' ? value : tm(value as Message);
}

export function clockTime(ms: number): string {
  return new Date(ms).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' });
}

/** "Today, 2:32 PM", "Yesterday, 9:12 AM", "Thursday, 16 October, 7:20 PM". */
export function when(ms: number, now: number = Date.now()): string {
  return `${text(dayLabel(ms, now))}, ${clockTime(ms)}`;
}

/** The "Heard" rows of a result: source, when, matched by. */
export function heardRows(detail: SessionDetail, now: number = Date.now()): Fact[] {
  const rows: Fact[] = [
    { key: 'result.source', value: tm(heardSource(detail)) },
    { key: 'result.when', value: when(detail.entry.created_at_ms, now) },
  ];
  const segment = detail.segments.find((s) => s.track);
  const by = segment ? matchedBy(segment.evidence) : null;
  if (by) rows.push({ key: 'result.matchedBy', value: tm(by) });
  return rows;
}

/** "Open in Spotify" for the trailing button of a recent row. */
export function openInLabel(name: string): string {
  return t('result.openIn', name);
}
