import { writeFileSync } from 'node:fs';
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import { longVersion, NOTE_MS, renderSong, SYNTHETIC_ARTIST, SYNTHETIC_SONGS, type SyntheticSong } from '../../src/engine/host-mock-audio';

/**
 * A stand-in for Shazam's `discovery/v5` endpoint that recognizes the synthetic test
 * songs (`host-mock-audio.ts`) from the signatures the real engine sends, the way the
 * real service does: the query's spectral peaks vote for a song and a time offset
 * against reference signatures of every song (made with the engine's own generator).
 * Nothing leaves the machine; nothing copyrighted plays.
 */

/** The engine module's signature tools (`Engine.shazamSignature`, `Engine.decodeSignature`). */
export interface SignatureTools {
  shazamSignature(samples: Int16Array): Uint8Array;
  decodeSignature(signature: Uint8Array): { sample_rate: number; sample_count: number; bands: [number, number, number][][] };
}

const RATE = 16_000;
/** Signature frames are 8 ms (hop 128 at 16 kHz). */
const FRAME_MS = 8;
/**
 * The songs are known in their long versions (`longVersion`): the 32 s test songs are
 * their first 32 s, so they are recognized too (a loop restarts the offset).
 */
const KNOWN_SECONDS = 150;
/** References: 12 s windows every 4 s over a loop and a bit (every query clip fits one). */
const WINDOW_MS = 12_000;
const WINDOW_STEP_MS = 4_000;
/** Peaks within this many 1/64 bins (≈ 4 Hz) and 2 frames count as the same. */
const FBIN_TOLERANCE = 32;
const DELTA_BIN = 2;
/**
 * A match needs this many agreeing peaks and half again as many as any other song (all
 * songs use the same 16 tones, so chance agreements are common).
 */
const MIN_VOTES = 12;
const MIN_LEAD = 1.5;

interface Peak {
  frame: number;
  fbin: number;
}

function peaksOf(tools: SignatureTools, signature: Uint8Array, offsetFrames = 0): Peak[] {
  return tools.decodeSignature(signature).bands.flatMap((band) => band.map(([frame, fbin]) => ({ frame: frame + offsetFrames, fbin })));
}

function int16(samples: Float32Array): Int16Array {
  const out = new Int16Array(samples.length);
  for (let i = 0; i < samples.length; i++) out[i] = Math.max(-32767, Math.min(32767, Math.round(samples[i]! * 32767)));
  return out;
}

export interface SongMatch {
  song: SyntheticSong;
  /** Where in the song (its 32 s loop) the query starts, in ms. */
  offsetMs: number;
  votes: number;
  runnerUp: number;
}

/** Reference peaks of every test song, by frequency bucket. */
export class SongIndex {
  private readonly songs: { song: SyntheticSong; buckets: Map<number, number[]> }[] = [];

  constructor(private readonly tools: SignatureTools) {
    for (const short of SYNTHETIC_SONGS) {
      const song = longVersion(short, KNOWN_SECONDS);
      const lengthMs = song.notes.length * NOTE_MS;
      const audio = renderSong(song, RATE, lengthMs);
      const buckets = new Map<number, number[]>();
      for (let start = 0; start + WINDOW_MS <= lengthMs; start += WINDOW_STEP_MS) {
        const window = int16(audio.subarray((start / 1000) * RATE, ((start + WINDOW_MS) / 1000) * RATE));
        for (const peak of peaksOf(tools, tools.shazamSignature(window), start / FRAME_MS)) {
          const key = Math.round(peak.fbin / FBIN_TOLERANCE);
          const frames = buckets.get(key) ?? [];
          frames.push(peak.frame);
          buckets.set(key, frames);
        }
      }
      this.songs.push({ song: short, buckets });
    }
  }

  /** The song a signature was made from, if its peaks agree on one. */
  match(signature: Uint8Array): SongMatch | null {
    const query = peaksOf(this.tools, signature);
    const results = this.songs.map(({ song, buckets }) => {
      const votes = new Map<number, number>();
      for (const q of query) {
        const key = Math.round(q.fbin / FBIN_TOLERANCE);
        const seen = new Set<number>();
        for (const k of [key - 1, key, key + 1]) {
          for (const frame of buckets.get(k) ?? []) {
            const delta = Math.floor((frame - q.frame) / DELTA_BIN);
            // One vote per query peak and offset.
            if (seen.has(delta)) continue;
            seen.add(delta);
            votes.set(delta, (votes.get(delta) ?? 0) + 1);
          }
        }
      }
      let best = 0;
      let bestDelta = 0;
      for (const [delta, count] of votes) {
        // Neighbouring offset bins belong to the same alignment.
        const total = count + (votes.get(delta - 1) ?? 0) + (votes.get(delta + 1) ?? 0);
        if (total > best) [best, bestDelta] = [total, delta];
      }
      return { song, votes: best, offsetMs: bestDelta * DELTA_BIN * FRAME_MS };
    });
    results.sort((a, b) => b.votes - a.votes);
    const [top, second] = results;
    if (!top || top.votes < MIN_VOTES || top.votes < MIN_LEAD * (second?.votes ?? 0)) return null;
    return { song: top.song, offsetMs: top.offsetMs, votes: top.votes, runnerUp: second?.votes ?? 0 };
  }
}

/** Shazam's answer for a recognized test song (the fields the engine reads). */
export function shazamMatch(match: SongMatch): unknown {
  return {
    matches: [{ id: match.song.id, offset: match.offsetMs / 1000, timeskew: 0, frequencyskew: 0 }],
    track: { key: match.song.id, title: match.song.title, subtitle: SYNTHETIC_ARTIST, type: 'MUSIC' },
    timestamp: Date.now(),
    timezone: 'Europe/Berlin',
    tagid: '00000000-0000-4000-8000-000000000000',
  };
}

export const NO_MATCH = { matches: [], timestamp: 0, timezone: 'Europe/Berlin', tagid: '00000000-0000-4000-8000-000000000000' };

export interface RecognizerQuery {
  at: number;
  song: string | null;
  votes: number;
  runnerUp: number;
}

/** Answer one request body. */
export function recognize(index: SongIndex, body: string): { answer: unknown; query: Omit<RecognizerQuery, 'at'> } {
  const uri = (JSON.parse(body) as { signature?: { uri?: string } }).signature?.uri ?? '';
  const signature = new Uint8Array(Buffer.from(uri.slice(uri.indexOf(',') + 1), 'base64'));
  const match = index.match(signature);
  return {
    answer: match ? shazamMatch(match) : NO_MATCH,
    query: { song: match?.song.title ?? null, votes: match?.votes ?? 0, runnerUp: match?.runnerUp ?? 0 },
  };
}

/**
 * Answer a `discovery/v5` request on `req`/`res` (any other path: false, untouched).
 * CORS is open: the engine runs in an extension Worker.
 */
export function handleRecognizer(index: SongIndex, queries: RecognizerQuery[], req: IncomingMessage, res: ServerResponse): boolean {
  const path = new URL(req.url ?? '/', 'http://127.0.0.1').pathname;
  if (!path.startsWith('/discovery/v5/')) return false;
  const cors = { 'access-control-allow-origin': '*', 'access-control-allow-headers': '*', 'access-control-allow-methods': 'POST, OPTIONS' };
  if (req.method === 'OPTIONS') {
    res.writeHead(204, cors).end();
    return true;
  }
  const chunks: Buffer[] = [];
  req.on('data', (c: Buffer) => chunks.push(c));
  req.on('end', () => {
    try {
      const body = Buffer.concat(chunks).toString('utf8');
      if (process.env.MF_STUB_DUMP) writeFileSync(process.env.MF_STUB_DUMP, body);
      const { answer, query } = recognize(index, body);
      queries.push({ at: Date.now(), ...query });
      res.writeHead(200, { ...cors, 'content-type': 'application/json' }).end(JSON.stringify(answer));
    } catch (error) {
      res.writeHead(400, cors).end(String(error));
    }
  });
  return true;
}

export interface StubRecognizer {
  origin: string;
  queries: RecognizerQuery[];
  close(): Promise<void>;
}

/** The recognizer on its own port (unit tests; the e2e fixture server mounts `handleRecognizer`). */
export async function startStubRecognizer(tools: SignatureTools): Promise<StubRecognizer> {
  const index = new SongIndex(tools);
  const queries: RecognizerQuery[] = [];
  const server: Server = createServer((req, res) => {
    if (!handleRecognizer(index, queries, req, res)) res.writeHead(404).end();
  });
  await new Promise<void>((done) => server.listen(0, '127.0.0.1', done));
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('no port');
  return {
    origin: `http://127.0.0.1:${address.port}`,
    queries,
    close: () =>
      new Promise((done) => {
        server.closeAllConnections?.();
        server.close(() => done());
      }),
  };
}
