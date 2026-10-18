import type { ExportFormat, SessionDetail } from '../types';

function clock(ms: number): string {
  const total = Math.floor(ms / 1000);
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = total % 60;
  const mm = String(m).padStart(2, '0');
  const ss = String(s).padStart(2, '0');
  return h > 0 ? `${h}:${mm}:${ss}` : `${mm}:${ss}`;
}

function csvField(value: string): string {
  return /[",\n]/.test(value) ? `"${value.replace(/"/g, '""')}"` : value;
}

/** Simplified versions of the core's exporters (`store/export.rs`), for the mock only. */
export function exportDetail(detail: SessionDetail, format: ExportFormat): string {
  const tracks = detail.segments.filter((s) => s.kind === 'Track' && s.track);
  switch (format) {
    case 'Text':
      return tracks.map((s) => `[${clock(s.start_ms)}] ${s.track!.artist} - ${s.track!.title}`).join('\n') + '\n';
    case 'Csv':
      return (
        ['start,end,artist,title,album,isrc,tier']
          .concat(
            tracks.map((s) =>
              [clock(s.start_ms), clock(s.end_ms), s.track!.artist, s.track!.title, s.track!.album ?? '', s.track!.isrc ?? '', s.tier]
                .map(csvField)
                .join(','),
            ),
          )
          .join('\n') + '\n'
      );
    case 'Json':
      return JSON.stringify(detail, null, 2);
    case 'Cue': {
      const lines = [`TITLE "${detail.entry.title.replace(/"/g, "'")}"`, 'FILE "audio" WAVE'];
      tracks.forEach((s, i) => {
        const frames = Math.floor((s.start_ms % 1000) * 0.075);
        const minutes = Math.floor(s.start_ms / 60_000);
        const seconds = Math.floor((s.start_ms % 60_000) / 1000);
        lines.push(
          `  TRACK ${String(i + 1).padStart(2, '0')} AUDIO`,
          `    TITLE "${s.track!.title.replace(/"/g, "'")}"`,
          `    PERFORMER "${s.track!.artist.replace(/"/g, "'")}"`,
          `    INDEX 01 ${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}:${String(frames).padStart(2, '0')}`,
        );
      });
      return lines.join('\n') + '\n';
    }
  }
}
