# melody-index

Offline builder for the Music Fetch melody index. It produces the JSONL described in
[`docs/CORE_API.md`](../../docs/CORE_API.md) ("Melody builder input"), which `mf index build`
turns into the shipped `melody-index.mfi`, plus `stats.json` and two evaluation sets.

It runs on a Linux GPU host (any CUDA GPU with ≥ 8 GB; set `MFIDX_HOST` to its ssh name). Data never goes into git.

## Layout on the GPU host

```
~/melody-build/
  code/      this directory (rsynced by scripts/deploy.sh), .venv from uv
  data/      inputs: billboard/all.json, lmd/ (clean_midi, lmd_matched, metadata), models/rmvpe.pt, chad/
  cache/     raw chart pages and API responses (kworb, Apple RSS, Deezer charts, CHAD metadata)
  state/     build.sqlite3 (all stage state, WAL) and the shared Deezer rate-limiter files
  logs/      <stage>.log (appending) and <stage>.out (stdout of the last pipeline run)
  out/       melody-songs-<date>.jsonl, stats.json, famous.json
  eval/      synthetic/ (wav + ground-truth.json), chad/ (wav + manifest.json)
```

`MFIDX_ROOT` overrides the root.

## Setup

```sh
scripts/deploy.sh                       # rsync code, install uv if missing, uv sync (Python 3.12, CUDA torch)
```

Inputs (once):

| File | Source |
|---|---|
| `data/billboard/all.json` | `music-fetch-data/datasets/billboard/all.json` (mhollingshead/billboard-hot-100) |
| `data/lmd/{clean_midi.tar.gz,match_scores.json,md5_to_paths.json,unique_tracks.txt}` | `music-fetch-data/datasets/lmd/` |
| `data/lmd/lmd_matched.tar.gz` | http://hog.ee.columbia.edu/craffel/lmd/lmd_matched.tar.gz |
| `data/models/rmvpe.pt` | https://huggingface.co/lj1995/VoiceConversionWebUI/resolve/main/rmvpe.pt |
| `data/chad/chad_hummings_subset.tar.gz` | https://huggingface.co/datasets/amanteur/CHAD_hummings |

Unpack `clean_midi.tar.gz` and `lmd_matched.tar.gz` inside `data/lmd/` and the CHAD archive
inside `data/chad/`.

## Running

Everything:

```sh
ssh "$MFIDX_HOST"
cd ~/melody-build/code
setsid nohup scripts/pipeline.sh > ~/melody-build/logs/pipeline.out 2>&1 < /dev/null &
```

From your workstation:

```sh
scripts/status.sh                       # counts, songs/min, ETA per stage, last log lines, GPU
scripts/remote.sh mfidx show "Adele" "Hello"   # resolution and notes of one song
scripts/fetch-results.sh                # copy out/ and eval/ to ~/Documents/projects/music-fetch-data/
```

Single stages (`uv run mfidx <stage> --help`):

| Command | Stage |
|---|---|
| `mfidx universe [--refresh] [--max-songs 60000]` | 1. chart sources -> deduplicated songs with popularity |
| `mfidx resolve [--limit N] [--retry-not-found]` | 2. Deezer track per song |
| `mfidx years` | release dates for songs without a chart year (`/track/{id}`) |
| `mfidx extract [--limit N] [--follow]` | 3. previews -> RMVPE -> notes -> phrases |
| `mfidx renote` | recompute preview phrases from stored f0 (no network) |
| `mfidx fulltrack [--top 3000] [--interval 12.5]` | 3b. full-track melodies of the most popular songs (YouTube) |
| `mfidx eval-references [--groups F]` | f0 of the CHAD original videos, for coverage analysis only |
| `mfidx midi [--step catalog\|link\|extract]` | 4. Lakh MIDI melodies |
| `mfidx validate` / `mfidx famous` | MIDI vs preview DTW agreement (all songs / report on well-known songs) |
| `mfidx export [--date D]` | 5. JSONL + stats.json |
| `mfidx eval-synth [--n 300]` / `mfidx eval-chad` | 6. evaluation sets |
| `mfidx sanity` | RMVPE on a synthetic voice with a known melody |
| `mfidx status` | progress, throughput, ETA |

Every stage is idempotent and resumable: work items are rows in `state/build.sqlite3`, and a
rerun only picks up rows that are missing or failed (up to 3 attempts).

## How it works

**1. Universe.** Billboard Hot 100 history (chart points = sum of `101 - position` over all
weeks), kworb Spotify totals (all-time top, global and 21 country weekly totals), Apple Music
most-played top 100 for 39 storefronts and Deezer (global chart, genre charts, the "Top
<country>" playlists of the Deezer Charts account). Entries are merged when they share a
normalized title (feat./with credits, remaster/version/live decorations and " - …" suffixes
removed; a looser key also drops every bracketed group) and any credited artist. Popularity is
a noisy-or `1 - Π(1 - w·s)` of per-source strengths, each monotone in its signal: Billboard
points with a recency factor, log global streams, the best country (log streams) and best
Apple/Deezer rank plus a breadth term per source (number of markets). The score is turned into
a rank percentile and the top 60k songs are kept.

**2. Deezer.** The fielded search syntax (`artist:"…" track:"…"`) returns nothing any more, so
the resolver issues plain queries (artist + title, then without brackets, then title alone) and
verifies candidates with fuzzy title and artist similarity, penalizing karaoke/cover/tribute
uploads and preferring candidates with a preview and a higher Deezer rank. Chart entries that
already carry a Deezer id skip the search. Search results already carry the ISRC and a signed
preview URL; `mfidx years` fetches `/track/{id}` for release dates of songs without a chart
year. All api.deezer.com traffic of all processes shares one limiter (≤ 40 requests per
5.2 s, `flock`ed state file) with exponential backoff on quota errors (code 4), 429 and 5xx.

**3. Preview melodies.** The preview URL is signed and expires after ~15 min, so the extractor
uses it only when it is younger than 10 min and fetches `/track/{id}` again otherwise. The MP3
is decoded in memory by ffmpeg to 16 kHz mono and dropped right after inference; audio never
touches the disk. RMVPE (the reference architecture, `rmvpe.pt` loaded with `strict=True`)
runs batched on the GPU at a 10 ms hop. The f0 track and per-frame confidence are stored
(zlib, ~4 KB per song) so notes can be recomputed with `mfidx renote`.

Notes: voicing at confidence > 0.04, single-frame confidence dips (< 0.72 × the peak on both
sides) become breaks so repeated notes survive, voiced islands < 50 ms dropped, 5-frame median,
octave errors folded against a 0.5 s local median, stable-pitch segmentation (0.75 semitone
change sustained for 30 ms), notes < 80 ms absorbed by a touching neighbour or dropped,
touching notes within 0.5 semitone merged. Phrases split at rests ≥ 400 ms, notes more than
15 semitones from the phrase median dropped (bass or riff pickups), phrases > 20 s split at
their longest rest, phrases need ≥ 6 notes, exact repeats removed. `start_ms` is the
phrase start inside the preview (the preview's offset in the song is not known).

**3b. Full tracks.** A preview covers about 30 s; people often hum a verse, the intro or the
bridge. For the `--top` most popular resolved songs, `mfidx fulltrack` finds the studio recording
on YouTube: one flat search for "artist - title", then the most plausible result by title and
artist similarity, a duration within max(12 s, 8 %) of Deezer's, no live/cover/remix/sped-up/…
words that the song title lacks, preferring "- Topic" art tracks and VEVO uploads. The audio is
streamed through ffmpeg into memory, RMVPE runs in 30.72 s chunks with 1.28 s of context, and
only the f0 track, confidences and notes (same parameters as the previews) are stored in
`track_melody`; audio never touches the disk. Downloads are paced (`--interval`, ±20 % jitter)
and the stage pauses 30 min, then longer, when YouTube asks for a sign-in. YouTube flagged the
build host after about 400 videos in 90 minutes at 12.5 s; 25 s has been sustainable. It needs
a current yt-dlp (`MFIDX_YTDLP`, e.g. a separate `uv venv` with `yt-dlp[default]`) and uses
Node as the JS runtime when Deno is missing. Export adds these melodies as `"audio"` phrases.

`mfidx eval-references` stores the same data for the original videos of the CHAD hums in
`eval_reference` (never exported). It shows where each Deezer preview sits in the song and
how much of each hummed section it covers.

**4. MIDI.** clean_midi (artist/title from the path) and lmd_matched (MSD id ->
`unique_tracks.txt`, best match score per file) are linked to universe songs by the same keys.
Songs that are not in the universe but have ≥ 2 distinct clean_midi transcriptions are added
as MIDI-only songs (lmd_matched counts are not used for this: its automatic matching assigns
hundreds of files to some obscure tracks). The melody track is chosen per file by: karaoke syllables aligned with note
onsets, track-name regex (melody/vocal/lead/… vs bass/drum/pad/chord/…), monophony, register
(median pitch near 67), range ≤ 24, 1–8 notes/s, share of stepwise motion (arpeggios and riffs
leap), activity, General MIDI program. lmd_matched files need an alignment score ≥ 0.6. A
second track is kept when it scores within 0.75. The track is made monophonic (skyline) and
split into phrases like the previews; `start_ms` is the phrase's position in the song.

**Validation.** For songs with both sources every preview phrase is aligned inside each MIDI
melody by subsequence DTW (10 fps, steps (1,1)/(1,2)/(2,1) with a penalty on the slanted
ones, octave-folded cost, three best transpositions by pitch-class histogram). Agreement is
then measured strictly on the warping path: the share of query frames within half a semitone
of the reference after one constant offset. A chance baseline (previews against other songs'
MIDI) sets the threshold at its median: MIDI melodies that do no better than a random song are
dropped, and the best-agreeing file/track is exported. `mfidx famous` reports agreement for
well-known songs next to the chance level.

**Vocal separation** is not used by default. `mfidx separation` compares RMVPE on the mix
with RMVPE on a separated vocal stem against MIDI melodies for well-known songs. It needs
audio-separator in its own venv:

```sh
cd ~/melody-build && uv venv sepenv --python 3.12
uv pip install --python sepenv/bin/python "audio-separator[gpu]" audioread
```

On 20 songs the stem raised mean agreement from 0.74 to 0.76–0.78 (better for 12 of 20) at
2–4 GPU-seconds per preview with audio-separator's per-file processing, i.e. 35–70 GPU-hours
for the whole universe, so the default build runs on the mix.

**5. Export.** One line per song with at least one phrase (preview phrases first, then full
track, then MIDI),
popularity re-ranked over the exported songs. `stats.json` has counts by source and origin,
notes per song and coverage by decade.

**6. Evaluation.** `eval/synthetic`: 300 random songs, an 8–14 s excerpt of one source, hum-like
rendering (sine or triangle with weak harmonics, 4.5–6.5 Hz vibrato, attack/release envelope,
glides between touching notes), octave folded to a humming register, key shift ±6 semitones,
tempo ×0.75–1.3, ±30 cents jitter per note, up to 12 % dropped notes, a short synthetic room
and pink noise at 15–25 dB SNR; `ground-truth.json` lists line, song key, source, excerpt
position and every parameter. `eval/chad`: the CHAD humming subset; each group's original
YouTube video is looked up through oEmbed, its title parsed into artist/title and matched to
the JSONL; `manifest.json` lists (wav, song key) for matched songs.

Song keys in manifests are `deezer:<id>` when the song has a Deezer id, else
`key:<artist>::<title>` from the normalized key.

## Etiquette and licences

- Every request uses the neutral User-Agent `MusicFetch-index-builder/1.0`; no personal data.
- Chart pages are fetched once per few days (cached), 1–2 s apart.
- Preview audio is never stored. Only derived pitch data is kept.
- Lakh MIDI is CC BY 4.0, but the compositions are copyrighted; CHAD is CC BY-NC 4.0 (evaluation
  only).

## Tests

```sh
scripts/remote.sh pytest     # on the GPU host (also checks rmvpe.pt and the mel filterbank against librosa)
```
