# Engine and pipelines

How `Engine` and the session pipelines (`core/crates/mfcore/src/{engine.rs,pipeline/**}`)
work. The module APIs they build on are in the module files; the FFI contract is in
[CORE_API.md](CORE_API.md).

## Engine

```
Engine
├─ config: EngineConfig
├─ runtime: tokio multi-thread runtime ("mf-core", 4 workers) + a blocking pool
├─ http: reqwest::Client (util::http)
├─ store: Arc<Store>                         data_dir/musicfetch.sqlite3
├─ shazam: Arc<ShazamClient>                 one serial queue per engine
├─ credentials: RwLock<Credentials>          snapshot per session at start
├─ pitch: OnceLock<Arc<PitchTracker>>        created on first use
├─ melody: RwLock<Option<Arc<MelodyIndex>>>  data_dir/melody/melody-index.mfi
├─ media: Option<Arc<dyn MediaBackend>>      app-supplied, else YtDlpSubprocess(config.media_tools)
├─ decoder / speech: Option<Arc<dyn …>>
└─ sessions: Mutex<HashMap<id, CancelToken>>
```

- `Engine::new` creates directories, opens the store, calls `recover_interrupted`, opens
  the melody index when present (a corrupt index is moved aside, not fatal), and
  deletes leftover `cache_dir/sessions/*` from earlier runs.
- Every session gets a `CancelToken`; `cancel()` cancels it, kills child work and makes
  the emitter emit `Canceled` exactly once.
- Credentials are snapshotted when a session starts.
- Nothing in the engine may panic across the FFI. Wrap session bodies so a panic becomes
  `Failed { Internal }` and is logged.

## Emitter (`pipeline/emitter.rs`)

Wraps the observer and the store for one session:

- Guarantees `Started` first and exactly one terminal event (`Finished`, `Failed`,
  `Canceled`); drops everything after the terminal event.
- Throttles `Level` to ≤ 30/s and batches `Pitch` to ≤ 10/s.
- Persists as it emits: `Source` → session title/source; `Segment` → `upsert_segment`;
  `SegmentRemoved` → `remove_segment`; `Candidates` → `set_candidates`; terminal →
  status, primary track, artwork, error.
- Fills `Track.identity_key` (identity::identity_key) on every track before emitting.
- Observer calls happen on the calling worker thread; never hold locks while calling out.

## Live sessions (`pipeline/live.rs`)

Threads: `push_pcm` copies the bytes into a bounded channel (drop the oldest chunks
beyond 5 s of backlog) and returns. One dedicated worker thread per live session runs
the DSP loop; network calls run on the runtime.

DSP loop per chunk:

1. `StreamResampler` → 16 kHz mono f32; append to a 30 s ring buffer and to the full
   capture buffer (≤ 30 s, kept for impression analysis and the hum recording).
2. Level: RMS/peak per 33 ms → `Level`.
3. Digital silence: 3 s of |x| < 1e-5 → `Notice::CaptureSilent` once.
4. `StreamingPitch` → frames → `Pitch` (batched) and `ImpressionDetector::push`.

Recording queries (modes `Auto` and `Recording`, and all `SystemAudio` sessions):

- Query times (seconds of captured audio): 4, 8, 13, 20, 28. Clip = newest
  min(elapsed, 12) s, converted to i16. Skip a query if the clip RMS is below
  −50 dBFS (record "too quiet").
- `ShazamClient::recognize(.., Priority::Interactive)`. At most one in flight; if one is
  still running when the next time arrives, run it when the first returns.
- A match → `catalog::enrich` (≤ 3 s timeout, keep what arrived) → `Finished(Matched)`.
  Stop capture processing immediately.
- Paid providers (if configured): at 10 s without a match, send the newest 10 s as WAV
  to AudD and/or ACRCloud fingerprint, once, in parallel with Shazam. Their match ends
  the session the same way.
- `Notice::MoveCloser` once at 6 s if the median RMS since start is below −40 dBFS.
- `Notice::RateLimited` when Shazam reports a cool-down; keep listening (paid providers
  and impression detection still work).
- 30 s without a match → finish (see "Finishing").

Impression rules (mode `Hum`, or `Auto` after the detector says impression):

- `Notice::ImpressionDetected` once (Auto only). Stop issuing recording queries.
- Auto-stop when voiced ≥ 6 s and the last 1.5 s are unvoiced/silent, or 20 s after the
  impression started, or on `finish()`.
- `Notice::KeepGoing` if the user stops with < 3 s voiced.

Finishing:

- Impression (or Hum) with ≥ 2.5 s voiced → impression analysis (below), session kind
  becomes `Hum`.
- Otherwise: if the last recording query is older than 4 s and there is ≥ 3 s of
  audible audio, run one final query; then `NoMatch` with a reason:
  `CaptureSilent` (silent notice and no audible audio), `TooQuiet` (median RMS < −45
  dBFS), `TooShort` (< 3 s audible), `MostlySpeech` (speech-like frames > 60 %: voiced,
  low pitch stability, syllabic energy modulation 3–8 Hz), else `NotFound`.

## Impression analysis (`pipeline/hum.rs`)

Input: the captured 16 kHz audio (≤ 30 s), its pitch frames, the language. Used by live
sessions and by `analyze(.., AnalyzeMode::Impression)` on files.

Run concurrently:

1. Melody — `notes::query_from_frames`; if the index is installed,
   `matcher::search(index, query, 10)`. If not installed, `Notice::MelodyIndexMissing`.
2. Lyrics — if a `SpeechToText` is available: write a temp WAV, `transcribe` (timeout
   15 s), drop hypotheses with < 3 real words (filler like "la", "na", "oh", "hmm" does
   not count), then `lyrics::search` (timeout 12 s). Store the best transcript.
3. ACRCloud humming — if configured: newest ≤ 12 s as 8 kHz WAV (timeout 10 s).

Emit `Candidates { is_final: false }` as each source returns (after fusion), then the
final list.

Fusion:

- Map melody matches to tracks: title/artist/isrc from `SongMeta`, ids
  `{"melody": song_id, "deezer": …}`.
- Group by `identity::same_track`. For a lyric or ACR candidate that is in the melody
  index (`find_song`), add `matcher::score_song` as melody evidence.
- Combined score = 1 − Π(1 − wᵢ·sᵢ) with w = melody 1.0, lyrics 0.95, acr 0.9; +0.1
  (capped at 0.99) when melody and lyrics both support it.
- Tier: Strong ≥ 0.75, Possible ≥ 0.35, else LongShot. Keep ≤ 5 with score ≥ 0.05.
- Enrich the kept tracks (artwork, preview, links) concurrently with a 4 s overall
  budget; emit what is ready.
- No candidates → `NoMatch(NotFound)`; else `Finished(Candidates { count })`.
- Keep the capture as `data_dir/recordings/<session>.wav` (16 kHz mono) so the user can
  retry after installing or updating the melody index. Delete it with the session.

## Recognizer call budget

Rate limits from recognizers were the main failure of 0.4. Rules:

- One `ShazamClient` per engine. A token bucket shared by all sessions: capacity 4,
  refill one token per 9 s (≈ 6.7 calls/min sustained), and at least 2 s between calls.
  Interactive calls (live listening, a single short file) may use the whole bucket;
  batch calls (mixes, playlists) only take a token while ≥ 2 remain, so a person
  listening never waits behind a mix.
- Any 429 / non-JSON 200 opens the breaker (30 s doubling to 60 min) **and** halves the
  refill rate for the next hour. Sessions pause and resume; they never fail because of a
  limit (they finish with what they have only after the batch wait cap).
- Every result is cached persistently (signature hash), so re-analysing the same media
  costs nothing.
- Most mix and playlist tracks should be identified **without** Shazam: tracklist hints
  verified by local fingerprint matching, and metadata for playlist entries. Shazam fills
  the gaps. Target: ≤ 25 Shazam calls per hour of mix audio when a timestamped tracklist
  exists, ≤ 70 without one.

## Analysis sessions (`pipeline/analyze.rs`)

1. Validate synchronously: trimmed input; URL (`links::looks_like_url`, first URL in
   shared text) → `Link`; an existing regular file → `File`; else `InvalidInput`.
2. `Started`, then for links `Phase(Resolving)` and `links::resolve`:
   - `Resolution::Tracks` → `Source`; enrich tracks (4 at a time); one `Segment` per
     track (`kind Track`, `start = end = 0`, `item_index` = position, confidence 0.99,
     `Confirmed`, evidence `Metadata`) → `Finished(Completed { n, n })`.
   - `Resolution::Media` → `Source` (with hints); continue.
3. Media: unless `direct`, `MediaBackend::probe` → `media::parse_probe` → refresh
   `Source` (title, uploader, thumbnail, duration, item count). For media ≥ 8 min the
   probe also asks for the top comments (pinned first) to find tracklists.
4. Playlists: see "Playlists" below.
5. Single media: `Phase(Downloading, fraction)` → `download_audio` (or a direct HTTP
   download with the SSRF guard and a 2 GiB cap) into `cache_dir/sessions/<id>/` →
   `Phase(Decoding)` → `audio::decode::decode_file` → delete the download.
6. Strategy (see below): `Impression` mode → impression analysis on the first 30 s;
   otherwise **survey → single or mix**.
7. Delete `cache_dir/sessions/<id>/` at the end in every outcome.

### Hints (`pipeline/hints.rs`)

Collected before any recognizer call:

- Chapters (yt-dlp), timestamped lines in the description, timestamped tracklists in
  the top/pinned comments (uploader and pinned comments first), YouTube "Music" cards
  and Shorts sound (no times), TikTok sound, `track`/`artist` fields from the extractor.
- Tracklist parser: handles `00:00 Artist - Title`, `[0:00] Artist – Title (Remix)`,
  `1. Artist - Title [Label] 03:45`, `01 | Artist - Title`, en/em dashes, `ID - ID`,
  `w/` mashups, `vs.`/`x` collaborations, labels in brackets, emoji/bullets. A tracklist
  is accepted when ≥ 3 entries have strictly increasing times inside the duration;
  among sources pick the one with the most valid entries.
- Version qualifiers (remix, edit, extended, live, acoustic, VIP, bootleg, rework,
  sped up, slowed) are kept: they select the exact recording.

### Local fingerprint matching (`fingerprint/`)

A landmark matcher (constellation hashing) built on the Shazam peak extractor:

- References: previews of candidate tracks (Deezer 30 s, iTunes/Apple 30–90 s, decoded in
  memory, never stored), keyed by identity.
- Hashes: anchor peak paired with up to 5 later peaks within 0.08–1.5 s and a bounded
  frequency span; hash = (f1, f2, Δt). Matching votes on (reference, time offset); a
  match needs ≥ 10 aligned hashes and a clear lead over the runner-up.
- DJ tempo/pitch changes: search speed factors 0.94–1.06 (resample, vinyl-style) and Δt
  scale factors (key-lock) in 1 % steps around the hinted region only.
- Uses: verify tracklist hints, confirm "same track still playing", and detect a track
  that is played twice — all without recognizer calls.

### Survey: one song or many?

Never decide by duration alone. A single song must never be split.

1. Priors: extractor `track`/`artist` (YouTube Music, Topic channels, SoundCloud) → a
   single track; `catalog::find` on the title ("Artist - Title …") with a duration
   within ±12 % → strong single prior; mix keywords in title/description (mix, set,
   live set, DJ, Boiler Room, tracklist, compilation, playlist, radio, episode,
   "full album", "1 hour") and a timestamped tracklist → strong mix prior.
2. Media < 15 min without a mix prior: survey probes at 20 %, 50 % and 80 % (12 s
   windows, Interactive priority for media < 10 min, else Batch):
   - All matched probes are the same identity → **single**, even when offsets jump
     (loops, extended edits, repeated choruses). One segment `[0, duration]`, tier from
     agreement (2+ probes → Confirmed).
   - Probes disagree → **mix** (keep the survey results).
   - No probe matches → two more probes (35 %, 65 %), then paid providers; still nothing
     → page hints as `Possible`, else one `Unidentified` segment. Never several
     `Unidentified` pieces for a short file.
3. Media ≥ 15 min, or a strong mix prior → **mix** directly (with the tracklist, if any).

### Mix strategy (`pipeline/mix.rs`)

1. `Phase(Segmenting)`: `features::compute(pcm, 1000)`; labels (silence / speech /
   music, global percentile normalization, 5-frame majority smoothing); novelty curve
   (Foote, 24 s checkerboard kernel; 12 s below 20 min) and its peaks as candidate
   change points. Emit placeholder segments so the timeline is complete from the start.
2. **Hinted pass** (timestamped tracklist): for each entry, `catalog::find` (keep the
   version qualifier) → previews → local match within [tᵢ − 30 s, tᵢ₊₁ + 30 s] (speed and
   key-lock search). Verified → segment starting at the refined time, `Confirmed`,
   evidence `PageHint` + local match. Not verified → one Shazam probe at
   tᵢ + clamp((tᵢ₊₁ − tᵢ)/2, 20 s, 60 s): same track → `Confirmed`; a different track →
   trust the audio (`Likely`) and flag the hint as wrong; nothing → `Possible` from the
   hint. Untimed hints (YouTube "Music" cards) become the candidate set for local
   matching across the whole mix before any probing.
3. **Adaptive probing** for everything still unexplained, in time order:
   - A match of X at window start T with Shazam offset o and skew s gives the start
     Sₓ = T − o/(1 + s) and, with the track duration D (enrichment), the latest end
     Eₓ = Sₓ + D/(1 + s).
   - Next probe at min(Eₓ − 15 s, T + 150 s), at least T + 30 s; without D, T + 90 s.
   - Two consecutive matches X (at T₁) and Y (at T₂): the boundary is Y's start
     estimate when it falls in [T₁, T₂], else X's end estimate when it does, else the
     strongest novelty peak in [T₁, T₂]; snap to a novelty peak within ±8 s.
   - Unexplained gap ≥ 30 s between Eₓ and Sᵧ, or T₂ − T₁ > 4 min → probe the middle (a
     short track may sit there). Repeat until no gap qualifies or the budget
     (duration / 45 s + 10 probes) is spent.
   - Before probing a stretch, try local matching against the previews of tracks already
     identified in this mix (replays) — free.
4. Unresolved music regions: one more probe at a different position, then paid
   providers (≤ 30 calls) if configured; then untimed hints.
5. **Stitch** (`pipeline/stitch.rs`, ported from 0.4): bridge Track(X) → short gap →
   Track(X) (bridge budget by longer neighbour: < 10 s → 2 s, < 60 s → 5 s, < 600 s → 8 s,
   else 19 s); merge adjacent same-identity tracks when their start estimates agree
   within 5 s or the gap ≤ allowance (< 10 s → 2 s, < 60 s → 5 s, < 600 s → 12 s, else
   30 s); a replay of X with a different start estimate stays a separate segment; clamp
   overlaps; drop zero-length segments; never merge different ISRCs or different
   versions (remix vs original).
6. Tiers: ≥ 2 agreeing probes or hint + local match → Confirmed 0.95; one tight-skew
   probe → Likely 0.88; loose skew → Likely 0.72 (Possible if it contradicts a
   neighbour); hint only → Possible 0.55; nothing → Unknown.
7. `Progress` after every step: `analyzed_ms` = end of the furthest explained stretch;
   ETA from remaining probes × measured time per probe (including pacing).
8. `source_url` of each segment = `links::deep_link(source_url, start_ms)`.

### Playlists

- Entries in order (cap 300; notice when truncated), `Item` events, `item_index` = entry.
- **Metadata first:** extractor `track`/`artist`, or a title parsed as "Artist - Title"
  without risky qualifiers (live, cover, remix, sped up, slowed, nightcore, karaoke,
  instrumental, 8D), matched by `catalog::find` with a duration within ±8 s (± 25 s for
  "official video") → segment `Likely` 0.9, evidence `Metadata`, no audio needed.
- Otherwise (or when the qualifier says it is a different version): download the
  entry's audio and run the survey (an entry ≥ 15 min becomes a nested mix).
- Up to 2 downloads in parallel; recognizer calls go through the shared budget.

## Evaluation (`tools/eval/mixes`)

A manifest of real cases with ground truth, run with `mf eval mixes`:

- DJ mixes and compilations with 5, ~15, ~30 and 50+ songs whose YouTube description or
  pinned comment has a timestamped tracklist; long single songs (7–12 min, extended
  mixes, songs with spoken interludes or long silent breaks); short and long YouTube
  playlists.
- Metrics per case: track recall and precision (identity match, version-aware and
  version-agnostic), start-time error (median and p90), splits (a true track covered by
  > 1 predicted segment), merges (a predicted segment covering > 1 true track), single
  songs split (must be 0), Shazam calls per hour.
- Two modes: **blind** (hints disabled — measures recognition and segmentation) and
  **hinted** (the product default).
- Live recognizer traffic for evaluation runs from a non-primary IP with ≥ 9 s spacing;
  all responses are cached on disk keyed by signature hash, so algorithm iterations
  replay from the cache.

## Caching

- Recording results: store cache namespace `shazam`, key = SHA-256 of the signature
  bytes; value = serialized match or `null`; TTL 30 days (match) / 1 day (no match).
- Enrichment: namespace `enrich`, key = identity key, TTL 30 days.
- Lyrics backends and catalog lookups keep their own in-memory caches.

## Melody index installation

`install_melody_index`: fetch the manifest → stream the file into
`data_dir/melody/.download` with `Phase(Downloading, fraction)` → verify SHA-256 →
atomic rename to `melody-index.mfi` + `manifest.json` → open → swap into the engine →
`Finished(Completed { 1, 1 })`. Cancel deletes the partial file. `check_melody_index`
fetches the manifest and fills `available_version` / `download_bytes`.

## Library operations

- `correct_segment` sets the track, confidence 1.0, tier Confirmed, adds `User` evidence,
  moves the previous track to alternates (≤ 5) and returns the segment.
- `confirm_candidate` stores the chosen track as the session's primary track and moves
  that candidate to rank 0 with `Strong`.
- `delete_sessions` cancels running sessions first, then deletes rows and recordings.
- `clear_cache` removes `cache_dir/sessions/*` for sessions that are not running.
- `storage_usage` sums the database files, cache dir and melody index.
