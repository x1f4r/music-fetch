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

## Analysis sessions (`pipeline/analyze.rs`)

1. Validate synchronously: trimmed input; URL (`links::looks_like_url`, first URL in
   shared text) → `Link`; an existing regular file → `File`; else `InvalidInput`.
2. `Started`, then for links `Phase(Resolving)` and `links::resolve`:
   - `Resolution::Tracks` → `Source`; enrich tracks (4 at a time); one `Segment` per
     track (`kind Track`, `start = end = 0`, `item_index` = position, confidence 0.99,
     `Confirmed`, evidence `Metadata`) → `Finished(Completed { n, n })`.
   - `Resolution::Media` → `Source` (with hints); continue.
3. Media: unless `direct`, `MediaBackend::probe` → `media::parse_probe` → refresh
   `Source` (title, uploader, thumbnail, duration, item count).
   - Playlist (`entries` non-empty): process entries in order (cap 200; notice when
     truncated) as items: `Item { index, count, title }`, then the single-media path with
     `item_index = index`. A failing entry becomes an `Unidentified` segment with the
     error in its evidence detail; the session continues.
4. Single media: `Phase(Downloading, fraction)` → `download_audio` (or a direct HTTP
   download with the SSRF guard and a 2 GiB cap) into `cache_dir/sessions/<id>/` →
   `Phase(Decoding)` → `audio::decode::decode_file` → delete the download.
5. Strategy: `Impression` mode → impression analysis on the first 30 s. `Mix` mode, or
   duration ≥ 240 s, or ≥ 2 chapters, or ≥ 3 description timestamps → mix. Otherwise
   single.
6. Delete `cache_dir/sessions/<id>/` at the end in every outcome.

### Single strategy

- Rank 12 s windows (hop 6 s) by energy and spectral richness; pick up to 3, at least
  12 s apart, preferring the middle of the file.
- Query them in order with `Priority::Interactive` until: two probes agree
  (`Confirmed`, 0.95), or one probe has max(|timeskew|, |freqskew|) ≤ 0.002 (`Likely`,
  0.88). A single loose-skew match after all probes → `Likely` 0.72.
- No Shazam match → paid providers on the best window if configured.
- Still nothing → use page hints (YouTube "Music" card, TikTok sound) as a `Possible`
  segment (0.55, evidence `PageHint`) if present; else an `Unidentified` segment.
- One segment spanning `[0, duration]`. `Finished(Completed { identified, 1 })`.

### Mix strategy (`pipeline/mix.rs`)

1. `Phase(Segmenting)`: `features::compute(pcm, 1000)`.
2. Frame labels with global (whole-file percentile) normalization: silence
   (RMS < −50 dBFS), speech (voice-band energy, zcr, low chroma strength, syllabic
   modulation), music otherwise. Smooth labels with a 5-frame majority filter.
3. Boundaries: Foote novelty on the self-similarity of z-scored [chroma ‖ mfcc ‖ flatness]
   with a 24 s checkerboard kernel (12 s for media < 20 min); peaks above the 80th
   percentile and ≥ 20 s apart (8 s for < 20 min); plus chapter starts and description
   timestamps snapped to the nearest frame; plus label changes lasting ≥ 8 s.
4. Regions: consecutive spans between boundaries with their dominant label. Music
   regions shorter than 15 s merge into the neighbour with the closest feature mean.
5. Probe plan, in time order: the middle 12 s of every music region; regions > 4 min
   also get probes at 25 % and 75 %. Emit an `Unidentified` (or `Speech`/`Silence`)
   segment for every region up front so the timeline is complete from the start.
6. Run probes with `Priority::Batch`. On a match at window start W with Shazam offset o:
   - Enrich once per identity (cache by identity) for duration.
   - Projected span = [W − o, W − o + duration] ∩ mix. Skip pending probes of later
     regions whose middle lies inside the projected span minus 20 s; keep one
     verification probe if the skipped stretch is > 3 min.
   - Emit/replace segments: the region becomes `Track`; adjacent regions inside the
     projected span with no contradicting evidence join it (`SegmentRemoved` for the
     merged ones).
7. Unresolved music regions after the pass: one more probe at a different position
   (start + 30 %); then paid providers (budget: 30 calls per session) if configured;
   then description/chapter hints (`catalog::find` on "Artist - Title" text near the
   region start → `Possible`, evidence `PageHint`).
8. Final stitch (`pipeline/stitch.rs`, ported from 0.4):
   - Bridge `Track(X) → {Speech|Silence|Unidentified} → Track(Y)` when `same_track(X, Y)`
     and the bridge ≤ budget: longer neighbour < 10 s → 2 s, < 60 s → 5 s, < 600 s →
     8 s, else 19 s.
   - Merge adjacent same-identity tracks when the gap ≤ allowance: < 10 s → 2 s,
     < 60 s → 5 s, < 600 s → 12 s, else 30 s.
   - Merge rule: end = max, confidence = max, evidence = union, alternates deduped.
   - Clamp overlaps (end = next.start), drop zero-length segments.
   - Never merge different ISRCs.
9. Tiers: ≥ 2 agreeing probes → Confirmed 0.95; one tight-skew probe → Likely 0.88;
   loose skew → Likely 0.72 (Possible if it contradicts a neighbour); hint only →
   Possible 0.55; nothing → Unknown.
10. `Progress` after every probe: `analyzed_ms` = end of the furthest processed region;
    ETA = remaining probes × measured average probe time (including spacing).
11. Rate limits: on `RateLimited`, emit the notice and wait (Batch allows 10 min). If the
    breaker stays open longer, finish with what was found (`Completed`) and a notice.
12. `source_url` of each segment = `links::deep_link(source_url, start_ms)`.

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
