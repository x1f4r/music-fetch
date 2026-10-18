// Node smoke test of the browser engine: a live session on a WAV file against a
// local stub recognizer (no network, in-memory storage).
//
//   node core/crates/mfcore-wasm/smoke/live-smoke.mjs [--pkg DIR] [--wav FILE] [--stub URL]
//
//   --pkg   the JS glue (default: extension/src/engine/wasm, see build.sh)
//   --wasm  the module (default: extension/public/engine/mfcore.wasm)
//   --wav   16-bit PCM WAV to listen to (default: 12 s of synthetic music)
//   --stub  a running stub recognizer that answers Shazam's discovery path at its root
//   --mf-stub  a running `mf dev stub-recognizer` (all services routed to it)
//           without either, a built-in stub answers every query with the canned
//           "Get Lucky" match
//
// Exit code 0 when the session ends matched, 1 otherwise.
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const repo = path.resolve(here, '../../../..');
const args = Object.fromEntries(
  process.argv.slice(2).reduce((pairs, arg, i, all) => (arg.startsWith('--') ? [...pairs, [arg.slice(2), all[i + 1]]] : pairs), []),
);
const pkg = path.resolve(args.pkg ?? path.join(repo, 'extension/src/engine/wasm'));

const t0 = performance.now();
const wasm = await import(pathToFileURL(path.join(pkg, 'mfcore_wasm.js')).href);
const bytes = fs.readFileSync(path.resolve(args.wasm ?? path.join(repo, 'extension/public/engine/mfcore.wasm')));
const compiled = await WebAssembly.compile(bytes);
const tCompile = performance.now();
wasm.initSync({ module: compiled });
const tInit = performance.now();

// ---- stub recognizer
let stubUrl = args.stub;
let server = null;
let queries = 0;
if (!stubUrl && !args['mf-stub']) {
  const match = fs.readFileSync(path.join(repo, 'core/crates/mfcore/src/shazam/testdata/match_get_lucky.json'));
  server = http.createServer((req, res) => {
    const headers = { 'access-control-allow-origin': '*', 'access-control-allow-headers': '*' };
    if (req.method === 'OPTIONS') return res.writeHead(204, headers).end();
    if (!req.url.startsWith('/discovery/')) return res.writeHead(404, headers).end();
    req.resume();
    req.on('end', () => {
      queries++;
      res.writeHead(200, { ...headers, 'content-type': 'application/json' }).end(match);
    });
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  stubUrl = `http://127.0.0.1:${server.address().port}`;
}

// ---- audio
function synthWav(file, seconds = 12, rate = 44_100) {
  // A chord progression with a little noise: "music" to the level and silence checks.
  const n = seconds * rate;
  const data = Buffer.alloc(44 + n * 2);
  const chords = [[261.6, 329.6, 392.0], [220.0, 261.6, 329.6], [174.6, 220.0, 261.6], [196.0, 246.9, 293.7]];
  let seed = 7;
  for (let i = 0; i < n; i++) {
    const t = i / rate;
    const chord = chords[Math.floor(t / 1.5) % chords.length];
    let v = chord.reduce((sum, f) => sum + Math.sin(2 * Math.PI * f * t), 0) / 6;
    seed = (seed * 1103515245 + 12345) & 0x7fffffff;
    v += ((seed / 0x7fffffff) - 0.5) * 0.05;
    data.writeInt16LE(Math.max(-32767, Math.min(32767, Math.round(v * 32767))), 44 + i * 2);
  }
  data.write('RIFF', 0); data.writeUInt32LE(36 + n * 2, 4); data.write('WAVE', 8); data.write('fmt ', 12);
  data.writeUInt32LE(16, 16); data.writeUInt16LE(1, 20); data.writeUInt16LE(1, 22); data.writeUInt32LE(rate, 24);
  data.writeUInt32LE(rate * 2, 28); data.writeUInt16LE(2, 32); data.writeUInt16LE(16, 34); data.write('data', 36);
  data.writeUInt32LE(n * 2, 40);
  fs.writeFileSync(file, data);
}

function readWav(file) {
  const b = fs.readFileSync(file);
  if (b.toString('ascii', 0, 4) !== 'RIFF' || b.toString('ascii', 8, 12) !== 'WAVE') throw new Error(`${file} is not a WAV file`);
  let pos = 12, rate = 0, channels = 0, bits = 0;
  while (pos + 8 <= b.length) {
    const id = b.toString('ascii', pos, pos + 4), size = b.readUInt32LE(pos + 4);
    if (id === 'fmt ') { channels = b.readUInt16LE(pos + 10); rate = b.readUInt32LE(pos + 12); bits = b.readUInt16LE(pos + 22); }
    if (id === 'data') {
      if (bits !== 16) throw new Error('only 16-bit PCM WAV files are supported');
      const frames = Math.floor(size / 2 / channels);
      const mono = new Float32Array(frames);
      for (let i = 0; i < frames; i++) {
        let sum = 0;
        for (let c = 0; c < channels; c++) sum += b.readInt16LE(pos + 8 + (i * channels + c) * 2);
        mono[i] = sum / channels / 32768;
      }
      return { samples: mono, rate };
    }
    pos += 8 + size + (size & 1);
  }
  throw new Error(`${file} has no audio data`);
}

let wavFile = args.wav;
if (!wavFile) {
  wavFile = path.join(os.tmpdir(), `mf-live-smoke-${process.pid}.wav`);
  synthWav(wavFile);
}
const audio = readWav(wavFile);

// ---- the session
const events = [];
const engine = await wasm.Engine.create(
  args['mf-stub']
    ? { storage: 'memory', test_stub: args['mf-stub'], test_pacing_ms: 200, log_level: 'warn' }
    : { storage: 'memory', shazam_base_url: stubUrl, offline_catalog: true, log_level: 'warn' },
  { onEvent: (id, event) => events.push({ id, event, at: performance.now() }) },
);
const tEngine = performance.now();
const live = engine.startListening('Microphone', 'Recording', { auto_stop: true });
const chunk = Math.round(audio.rate / 50); // 20 ms
const tick = () => new Promise((resolve) => setTimeout(resolve, 0));
for (let i = 0; i < audio.samples.length && live.isCapturing(); i += chunk) {
  live.pushPcm(audio.samples.subarray(i, i + chunk), audio.rate, 1);
  // Faster than real time, but never more than a second ahead of the analysis.
  do await tick(); while (live.backlogMs() > 1000);
}
live.finish();
const terminal = (e) => e === 'Canceled' || (typeof e === 'object' && ('Finished' in e || 'Failed' in e));
const deadline = performance.now() + 30_000;
while (!events.some((e) => terminal(e.event)) && performance.now() < deadline) await new Promise((r) => setTimeout(r, 20));
const tDone = performance.now();
server?.close();
if (!args.wav) fs.rmSync(wavFile, { force: true });

const last = events.at(-1)?.event;
const kinds = events.map((e) => (typeof e.event === 'string' ? e.event : Object.keys(e.event)[0]));
const counts = kinds.reduce((m, k) => ((m[k] = (m[k] ?? 0) + 1), m), {});
const matched = last?.Finished?.outcome?.Matched?.track;
const report = {
  ok: Boolean(matched),
  outcome: last,
  title: matched ? `${matched.artist} - ${matched.title}` : null,
  events: counts,
  first: kinds[0],
  queries: server ? queries : undefined,
  audio_s: +(audio.samples.length / audio.rate).toFixed(2),
  ms: { compile: +(tCompile - t0).toFixed(1), init: +(tInit - tCompile).toFixed(1), engine: +(tEngine - tInit).toFixed(1), session: +(tDone - tEngine).toFixed(1) },
  module_mb: +(bytes.length / 1048576).toFixed(2),
  memory_mb: +(wasm.Engine.memoryBytes() / 1048576).toFixed(1),
  core: wasm.Engine.coreVersion(),
};
console.log(JSON.stringify(report, null, 2));
await engine.close();
process.exit(report.ok && report.first === 'Started' ? 0 : 1);
