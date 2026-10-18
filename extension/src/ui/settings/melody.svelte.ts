import type { EngineClient } from '../../engine/client';
import type { MelodyIndexStatus } from '../../engine/types';
import { MELODY_DOWNLOAD_BYTES } from '../listen.svelte';

/**
 * Settings › Melody (extension plan §5.8, design `app-settings-melody`): the melody
 * library in five states (not installed, downloading, installed, update available,
 * failed), plus the encoder companion file that comes with newer indexes. One download
 * and one size: the encoder is part of the library, not a separate choice.
 */
/** "Check for updates": idle, running, nothing newer, or the manifest couldn't be read. */
export type CheckPhase = 'idle' | 'checking' | 'upToDate' | 'failed';

export type MelodyView =
  | { phase: 'loading' }
  | { phase: 'none'; bytes: number }
  | { phase: 'downloading'; fraction: number; bytes: number }
  | { phase: 'installed'; songs: number; bytes: number; updated: string | null; encoder: boolean; check: CheckPhase }
  | { phase: 'update'; bytes: number }
  | { phase: 'failed' };

/** Bytes on disk: the index and, when installed, its encoder file. */
export function installedBytes(status: MelodyIndexStatus): number {
  return status.size_bytes + (status.encoder_installed ? (status.encoder_bytes ?? 0) : 0);
}

/** Bytes a download (or update) fetches: the index and the encoder file when the manifest names one. */
export function downloadBytes(status: MelodyIndexStatus | null): number {
  if (!status) return MELODY_DOWNLOAD_BYTES;
  const index = status.download_bytes ?? (status.installed ? status.size_bytes : MELODY_DOWNLOAD_BYTES);
  return index + (status.encoder_bytes && !status.encoder_installed ? status.encoder_bytes : 0);
}

/**
 * "updated October 1" from an index version like `2026.10.1` (or `2026.10.18-android`):
 * the year only when it isn't this year. Null when the version isn't a date.
 */
export function indexDate(version: string | null, now = new Date(), locale?: string): string | null {
  const match = /^v?(\d{4})\.(\d{1,2})\.(\d{1,2})/.exec(version ?? '');
  if (!match) return null;
  const [year, month, day] = [Number(match[1]), Number(match[2]), Number(match[3])];
  if (month < 1 || month > 12 || day < 1 || day > 31) return null;
  const date = new Date(Date.UTC(year, month - 1, day, 12));
  const options: Intl.DateTimeFormatOptions = { month: 'long', day: 'numeric', timeZone: 'UTC', ...(year !== now.getFullYear() ? { year: 'numeric' } : {}) };
  return new Intl.DateTimeFormat(locale, options).format(date);
}

export function melodyView(status: MelodyIndexStatus | null, extra: { downloading: number | null; failed: boolean; check: CheckPhase }): MelodyView {
  if (extra.downloading !== null) return { phase: 'downloading', fraction: extra.downloading, bytes: downloadBytes(status) };
  if (extra.failed) return { phase: 'failed' };
  if (!status) return { phase: 'loading' };
  if (!status.installed) return { phase: 'none', bytes: downloadBytes(status) };
  if (status.available_version) return { phase: 'update', bytes: downloadBytes(status) };
  return {
    phase: 'installed',
    songs: status.song_count,
    bytes: installedBytes(status),
    updated: indexDate(status.version),
    encoder: Boolean(status.encoder_installed),
    check: extra.check,
  };
}

export class MelodyController {
  status: MelodyIndexStatus | null = $state(null);
  downloading: number | null = $state(null);
  failed = $state(false);
  check: CheckPhase = $state('idle');
  private installId: string | null = null;
  private unsubscribe: (() => void) | null = null;

  constructor(private readonly engine: EngineClient) {}

  get view(): MelodyView {
    return melodyView(this.status, { downloading: this.downloading, failed: this.failed, check: this.check });
  }

  async start(): Promise<void> {
    this.unsubscribe = this.engine.subscribe({
      sessionEvent: (id, event) => {
        if (id !== this.installId || (typeof event !== 'object' && event !== 'Canceled')) return;
        if (typeof event === 'object' && 'Phase' in event && event.Phase.phase === 'Downloading') this.downloading = event.Phase.fraction ?? 0;
        if (typeof event === 'object' && 'Failed' in event) this.failed = true;
        if (event === 'Canceled' || (typeof event === 'object' && ('Finished' in event || 'Failed' in event))) {
          this.downloading = null;
          this.installId = null;
          void this.refresh();
        }
      },
    });
    await this.refresh();
  }

  dispose(): void {
    this.unsubscribe?.();
    this.unsubscribe = null;
  }

  async refresh(): Promise<void> {
    this.status = await this.engine.melodyIndexStatus().catch(() => this.status);
  }

  /** Download, Update and Try again. */
  async download(): Promise<void> {
    this.downloading = 0;
    this.failed = false;
    this.check = 'idle';
    try {
      this.installId = await this.engine.installMelodyIndex();
    } catch {
      this.installId = null;
      this.downloading = null;
      this.failed = true;
    }
  }

  async cancel(): Promise<void> {
    const id = this.installId;
    this.installId = null;
    this.downloading = null;
    if (id) await this.engine.cancelSession(id).catch(() => undefined);
    await this.refresh();
  }

  async checkForUpdates(): Promise<void> {
    this.check = 'checking';
    try {
      this.status = await this.engine.checkMelodyIndex();
      this.check = this.status.available_version ? 'idle' : 'upToDate';
    } catch {
      this.check = 'failed';
    }
  }

  async remove(): Promise<void> {
    await this.engine.removeMelodyIndex().catch(() => undefined);
    this.check = 'idle';
    await this.refresh();
  }
}
