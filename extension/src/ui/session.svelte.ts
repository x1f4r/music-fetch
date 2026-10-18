import type { EngineClient } from '../engine/client';
import type { ExportFormat, Segment, Track } from '../engine/types';
import { reduceSession, sessionShape, sessionView, tracklistText, type SessionShape, type SessionView } from '../models/session';

/**
 * One session on screen (app tab, side panel): loads the stored detail, follows its live
 * events and capture status, and runs the session's actions (pin, corrections, export).
 */
export class SessionController {
  view: SessionView | null = $state(null);
  missing = $state(false);
  /** The selected tracklist row or candidate (the details column shows it). */
  selectedId: string | null = $state(null);

  private stop: (() => void) | null = null;
  private reloading = false;

  constructor(
    private readonly engine: EngineClient,
    readonly id: string,
  ) {}

  get shape(): SessionShape | null {
    return this.view ? sessionShape(this.view) : null;
  }

  get selectedSegment(): Segment | null {
    return this.view?.detail.segments.find((s) => s.id === this.selectedId) ?? null;
  }

  async start(): Promise<void> {
    this.stop = this.engine.subscribe({
      sessionEvent: (sessionId, event) => {
        if (sessionId !== this.id || !this.view) return;
        this.view = reduceSession(this.view, event);
      },
      captureStatus: (sessionId, status) => {
        if (sessionId === this.id && this.view) this.view = { ...this.view, capture: status };
      },
      libraryChanged: () => void this.reload(),
    });
    await this.reload();
  }

  dispose(): void {
    this.stop?.();
    this.stop = null;
  }

  /** Reload the stored detail, keeping what only live events know (progress, capture). */
  async reload(): Promise<void> {
    if (this.reloading) return;
    this.reloading = true;
    try {
      const detail = await this.engine.session(this.id).catch(() => null);
      this.missing = detail === null;
      if (!detail) {
        this.view = null;
        return;
      }
      const previous = this.view;
      const fresh = sessionView(detail);
      this.view = previous ? { ...fresh, phase: previous.phase, fraction: previous.fraction, progress: previous.progress, item: previous.item, capture: previous.capture, running: fresh.running } : fresh;
    } finally {
      this.reloading = false;
    }
  }

  async setPinned(pinned: boolean): Promise<void> {
    if (this.view) this.view = { ...this.view, detail: { ...this.view.detail, entry: { ...this.view.detail.entry, pinned } } };
    await this.engine.setPinned(this.id, pinned);
  }

  async confirm(segment: Segment): Promise<void> {
    if (segment.track) await this.engine.correctSegment(this.id, segment.id, segment.track);
    await this.reload();
  }

  async reject(segment: Segment): Promise<void> {
    await this.engine.rejectSegment(this.id, segment.id);
    await this.reload();
  }

  async confirmCandidate(track: Track): Promise<void> {
    await this.engine.confirmCandidate(this.id, track);
    await this.reload();
  }

  async stopAnalysis(): Promise<void> {
    await this.engine.cancelSession(this.id).catch(() => undefined);
  }

  async export(format: ExportFormat): Promise<string> {
    return this.engine.exportSession(this.id, format);
  }

  tracklist(): string {
    return this.view ? tracklistText(this.view) : '';
  }
}

export const EXPORT_FORMATS: { format: ExportFormat; ext: string; mime: string; label: 'export.text' | 'export.csv' | 'export.cue' | 'export.json' }[] = [
  { format: 'Text', ext: 'txt', mime: 'text/plain', label: 'export.text' },
  { format: 'Csv', ext: 'csv', mime: 'text/csv', label: 'export.csv' },
  { format: 'Cue', ext: 'cue', mime: 'application/x-cue', label: 'export.cue' },
  { format: 'Json', ext: 'json', mime: 'application/json', label: 'export.json' },
];

/** A file name from a session title: letters, digits, spaces and dashes. */
export function exportFileName(title: string, ext: string): string {
  const base = title.replace(/[^\p{L}\p{N} ._-]+/gu, ' ').replace(/\s+/g, ' ').trim().slice(0, 80) || 'Music Fetch';
  return `${base}.${ext}`;
}
