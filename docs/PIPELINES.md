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
  listening never waits behind a mix. The client's own queue spaces interactive
  requests ≥ 3.5 s and batch requests ≥ 9 s (+ up to 1 s jitter).
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

1. Validate synchronously: trimmed input; an absolute path or `file://` URL → `File`
   when it is an existing regular file (else `InvalidInput`); a URL
   (`links::extract_url`, first URL in shared text) → `Link`; else `InvalidInput`.
2. `Started`, then for links `Phase(Resolving)` and `links::resolve` (when the page
   itself cannot be fetched, the media backend still gets the link):
   - `Resolution::Tracks` → `Source`; at once one `Segment` per track from the page's
     metadata (`kind Track`, `start = end = 0`, `item_index` = position, confidence 0.99,
     `Confirmed`, evidence `Metadata`, search links for every service), then each
     segment again under the same id as its enrichment arrives (8 lookups in flight,
     the catalogs' rate limiters pace them) → `Finished(Completed { n, n })`. A
     single track gets every catalog; collections get Deezer only (ISRC or search;
     iTunes just for tracks Deezer misses, and only while nothing else waits for
     it; tracks read from Deezer itself are not looked up again). A 50-track
     Spotify playlist lists every track within a fraction of a second and
     finishes in about 8 s.
   - `Resolution::Media` → `Source` (with hints); continue.
3. Media: unless `direct`, `MediaBackend::probe` → `media::parse_probe` → refresh
   `Source` (title, uploader, thumbnail, duration, item count). For media ≥ 8 min the
   probe also asks for the top comments (pinned first) to find tracklists; when the
   page did not tell the duration, a long media without a tracklist in its chapters or
   description is probed once more with comments.
4. Playlists: see "Playlists" below.
5. Single media: `Phase(Downloading, fraction)` → `download_audio` (or a direct HTTP
   download with the SSRF guard and a 2 GiB cap) into `cache_dir/sessions/<id>/`
   (a failed backend download is tried twice more, 1.5 s and 4 s later, each time
   into a fresh folder; cancellation stops a running download at once) →
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
  `w/` mashups, `vs.`/`x` collaborations, labels in brackets, emoji/bullets, invisible
  marks, `Track 01 - Title`, and lists written `Title (Original Mix) - Artist` (turned
  around when only the left side carries a version). Times written as lengths
  (`1. Song 03:45`, not increasing) are accumulated when they add up to 60–105 % of the
  duration. A tracklist is accepted when ≥ 3 entries have strictly increasing times
  inside the duration and fewer than half read like song sections ("Intro", "Chorus");
  among sources pick the one with the most valid entries (ties: chapters, description,
  pinned, uploader, other comments). When most entries have no artist (a live set, an
  album), the performer named at the start of the media title is used.
- Version qualifiers (remix, edit, extended, live, acoustic, VIP, bootleg, rework,
  sped up, slowed) are kept: they select the exact recording. `hints::version_key`
  reduces a title to what distinguishes a recording ("Tiësto Remix" → `tiesto remix`;
  "Extended Mix", "Radio Edit", "Original Mix", remasters → the original); two
  versions are compatible when they share a kind (remix, live, …) and, when both name
  people or places, one of those (style words such as trap, dubstep or house name
  nobody: "Thrizzo Trap Remix" is not "Eff3cts Trap Remix"). `same_recording` = no
  ISRC/provider-id conflict + same song + compatible version, and is what stitching and
  deduplication use. `same_song` also accepts a version credited to its remixers in
  one place and to someone else in another ("Argy, Omnya – Aria" and "Habib Alberto –
  Aria [Argy & Omnya RMX]").

### Local fingerprint matching (`fingerprint/`)

A landmark matcher (constellation hashing) built on the Shazam peak extractor:

- References: previews of candidate tracks (Deezer 30 s, iTunes/Apple 30–90 s, decoded
  in the session work directory and deleted at once, never kept), keyed by identity.
- Hashes: anchor peak paired with up to 6 later peaks within 0.08–1.5 s and ±63
  log-frequency buckets; hash = (log f1 bucket, log f2 − log f1, Δt / 16 ms), buckets of
  1.25 %. Matching votes on (reference, offset) in 32 ms buckets, counting the bucket
  and its two neighbours. A match needs ≥ 20 aligned hashes, ≥ 1.2 % of the reference's
  hashes, a 1.5× lead over any other reference and 10× the chance level.
- DJ tempo/pitch changes: the query's peaks are mapped into reference coordinates for
  speed factors 0.94–1.06 in 0.5 % steps, vinyl-style (time and frequency) and key-lock
  (time only); log-frequency differences are pitch-invariant, so no audio is
  resampled. Searches stay inside the region where the track must play.
- Mix peaks are computed per minute of media on first use and kept for the session.
- Uses: verify tracklist hints, confirm a recognizer answer by finding the track's
  preview where it must play (which also shows more of where it plays), and find
  tracks already identified (continuing or replayed) before spending a probe — all
  without recognizer calls.

### Survey: one song or many?

Never decide by duration alone. A single song must never be split.

1. Priors: extractor `track`/`artist` (YouTube Music, Topic channels, SoundCloud) → a
   strong single prior; `catalog::find` on the title ("Artist - Title …", noise such as
   "(Official Video)" removed, Topic/VEVO channel as the artist) with a duration within
   ±12 % → strong single prior (outside it, a weak one). A timestamped tracklist, or
   title words songs practically never use (DJ set, live set, Boiler Room, Cercle,
   tracklist, compilation, megamix, full album, b2b, "1 hour", episode, …) → mix prior.
   "Mix" or "set" alone is weak ("Little Mix", "Set Fire to the Rain") and ignored.
2. Media < 15 min without a mix prior (or ≥ 15 min with a strong single prior whose
   catalog duration fits): survey probes at 50 %, 20 % and 80 % (12 s windows moved off
   quiet spots, Interactive priority for media < 10 min, else Batch):
   - A strong prior confirmed by the first probe → **single** (one call).
   - All matched probes name the same song (any version, any offset: loops, extended
     edits, repeated choruses) → **single**. One segment `[0, duration]`, the recording
     most probes name (the prior's version first); tier from agreement (2+ probes or a
     confirmed page prior → Confirmed).
   - Probes disagree → two more probes (35 %, 65 %); then **mix** only if two songs each
     have ≥ 2 answers that do not alternate in time, or their start estimates line up
     as consecutive songs. Answers that alternate (A B A A B) are one recording known
     under two names (a DJ set or compilation containing the song, a record sampling
     it): a single of the page's song, else of the recording whose answers place its
     start near the media's. A clear majority (≥ 3) with stray single answers
     (samples, interpolations), or a song the page names, stays a single; the strays
     become alternates. Ambiguity never splits.
   - The page against the verdict: one recognizer answer naming another song (the
     other probes silent or disagreeing) does not outweigh the page's song, and
     neither does a cover by another artist when the artist's own channel uploaded
     the media (channel named like the artist, Topic, VEVO); the page's song is
     `Likely` from the artist's channel, else `Possible`, with the answer as first
     alternate. The page names a version (live, remix) the recognizer only knows in
     its original, or whose versions the answers disagree on (none named twice): the
     page's version, the recognizer's as first alternate.
   - No probe matches → two more probes (35 %, 65 %), then paid providers; still nothing
     → the page's song as `Possible`, else one `Unidentified` segment. Never several
     `Unidentified` pieces for a short file.
3. Media ≥ 15 min, a mix prior, or `AnalyzeMode::Mix` → **mix** directly (with the
   tracklist, if any; survey probes already made seed it).

### Mix strategy (`pipeline/mix.rs`)

1. `Phase(Segmenting)`: `features::compute(pcm, 1000)`; labels (silence / speech /
   music, global percentile normalization, 5-frame majority smoothing); novelty curve
   (Foote, 24 s checkerboard kernel; 12 s below 20 min) and its peaks as candidate
   change points. Emit placeholder segments so the timeline is complete from the start.
2. **Hinted pass** (timestamped tracklist): for each entry, the version-aware catalog
   lookup (`analyze::services::find_version`: a remix never resolves to its original) →
   preview → local match within [tᵢ − 30 s, tᵢ₊₁ + 30 s] (speed and key-lock search).
   Verified → segment [tᵢ, tᵢ₊₁], `Confirmed`, evidence `PageHint` 0.95. Not verified →
   one Shazam probe at tᵢ + clamp((tᵢ₊₁ − tᵢ)/2, 20 s, 60 s): same song → `Confirmed`,
   with the recognizer's exact version; another song → a second probe at ¾ of the
   entry, and the audio overrules the tracklist only when both probes name the same
   recording with agreeing start estimates (a sampled vocal heard twice does not follow
   the original's timeline; an answer naming the previous or next entry is the
   transition); otherwise the hint stays, `Possible`, with the answer as alternate;
   nothing → `Possible` from the hint. `ID` entries and talk ("Interview with …",
   "Episode … (Intro)") get one probe and otherwise become `Unidentified` or `Speech`.
   When three of the last six settled entries were overruled, the tracklist no longer
   describes the media (re-edited upload, shifted times): entries from the first of
   them on are dropped, their probes kept, and the rest is analysed blind. A list
   written "Title - Artist" throughout is turned around first when the catalog knows
   at least two of its first four entries only that way. Untimed hints (YouTube
   "Music" cards) become the candidate set for local matching across the whole mix
   before any probing.
3. **Adaptive probing** for everything still unexplained, in time order (probe starts
   snap to a 2 s grid so a re-analysis hits the answer cache):
   - A match of X at window start T with Shazam offset o and skew s gives the start
     Sₓ = T − o/(1 + s) and, with the track duration D (enrichment), the latest end
     Eₓ = Sₓ + D/(1 + s). Only the middle of a 12 s window counts as surely X.
   - Next probe at Eₓ + 10 s (just past X's latest end, where the next track must play)
     when that is at most 4 min ahead, else T + 150 s; at least T + 30 s; without D, or
     when X was heard past Eₓ (wrong duration), T + 90 s. No match → T + 25 s, twice;
     further unanswered probes in a row double the step up to 150 s (interviews,
     unreleased tracks and long breakdowns do not eat the budget; gap probing looks
     between them later).
   - Each new recording's preview is fetched and looked for between Sₓ and Eₓ; a hit
     confirms the answer for free and shows more of where X plays. Before each probe the
     previews of everything identified so far are looked for around T (continuing or
     replayed tracks cost nothing).
   - Two consecutive recordings X (heard until T₁) and Y (from T₂): a hard cut (Eₓ and
     Sᵧ within 8 s: albums, compilations) puts the boundary at Sᵧ. Otherwise the change
     lies in the transition [max(T₁, Sᵧ), min(T₂, Eₓ)] — DJs blend for a minute or more
     and published tracklists mark the takeover rather than the first bar of Y (on the
     evaluation mixes Sᵧ came 40–110 s before the published time) — and transitions
     longer than 40 s get up to two refining probes in their middle (whoever is heard
     there narrows the stretch), within 60 probes per hour of media; the boundary is
     the middle of what remains. The novelty curve is logged for evaluation but not
     used: it placed changes no better than the middle of the transition.
   - Gaps: X's end and Y's start ≥ 30 s apart → probe the middle; an unseen stretch of
     ≥ 60 s before Sᵧ (unless X ends exactly at Sᵧ, i.e. played in full) → probe 20 s
     before Sᵧ, where a short hidden track would sit; T₂ − T₁ > 4 min → probe the
     middle. Repeat until nothing qualifies or the budget (duration / 45 s + 10 probes)
     is spent.
   - Shazam reports how fast (timeskew) and how high (frequencyskew) the query plays
     against its reference. Playback produces neither, both together (turntable,
     pitch fader) or tempo alone (key lock), within ±8.5 %; any other combination
     (within 0.15 %) means the recognizer heard a sample inside another record. Such
     an answer never overrules a tracklist entry and, when nothing else backs it up,
     is dropped (measured on the evaluation mixes: 11 of 16 wrong answers, 1 of ~60
     right ones).
   - A recording heard once and not confirmed locally is dropped without a probe when
     its own start estimate would make it overlap an established recording (heard twice
     or found locally) for over a minute — longer than any transition; that is how
     misidentified samples and unreleased tracks show up. Otherwise it gets one more
     probe 10–40 s away inside the stretch it must occupy: the same recording confirms
     it, another version of the same song replaces it, anything else is kept as a
     neighbour (and checked for overlap again), nothing keeps it as it is.
   - What counts as backing: a second answer of the same recording whose start
     estimate agrees, from a window at least 12 s away (overlapping windows hear the
     same audio; a window within 6 s of an earlier probe is never asked again), or the
     preview found inside the stretch the play must occupy. Two answers at a speed and
     pitch no player produces do not back each other up (a sampled vocal repeats).
   - Refining probes only decide which of the two neighbours dominates; a third
     recording heard in the middle of a blend is ignored (the blend confusing the
     recognizer far more often than a hidden track).
   - Answers of one play that name different versions of a song (a radio edit and the
     extended mix, a remix and its original sharing most of their audio) become one
     recording — the version most answers name, local matches counting twice — so a
     play is never split by version; a song heard only after the other ended (an
     album's reprise, a replay) stays separate.
   - A play heard over 15 s past the end its catalog duration gives (the catalog
     knows a shorter edit) has no known end; a recording without a known end is
     taken to last 90 s past its last hearing when nothing else was heard after it,
     instead of being stretched over minutes nobody heard it in.
4. Unresolved music regions ≥ 45 s: one more probe at a different position, then paid
   providers (≤ 30 calls) if configured.
5. **Stitch** (`pipeline/stitch.rs`, ported from 0.4): bridge Track(X) → short gap →
   Track(X) (bridge budget by longer neighbour: < 10 s → 2 s, < 60 s → 5 s, < 600 s → 8 s,
   else 19 s); merge adjacent same-identity tracks when their start estimates agree
   within 5 s or the gap ≤ allowance (< 10 s → 2 s, < 60 s → 5 s, < 600 s → 12 s, else
   30 s); a replay of X with a different start estimate stays a separate segment; clamp
   overlaps; drop zero-length segments; never merge different ISRCs or different
   versions (remix vs original).
6. Tiers (`stitch::grade`): ≥ 2 agreeing probes or local confirmations, or a hint
   verified locally or by a probe → Confirmed 0.95; one tight-skew probe → Likely
   0.88; loose skew → Likely 0.72; hint only → Possible 0.55; nothing → Unknown. A lone
   answer inside one continuous play of another recording (same start estimate on both
   sides) is dropped as a sample or misidentification.
7. `Progress` after every step: `analyzed_ms` = end of the furthest explained stretch;
   ETA from remaining probes × measured time per probe (including pacing).
8. `source_url` of each segment = `links::deep_link(source_url, start_ms)`.

### Playlists

- Entries in order (cap 300; truncation is logged — there is no `Notice` for it yet),
  `Item` events, `item_index` = entry; segment ids are prefixed `e<index>-`.
- **Metadata first:** a title parsed as "Artist - Title" (or the title with a Topic /
  VEVO channel as the artist) without risky qualifiers (live, cover, remix, sped up,
  slowed, nightcore, karaoke, instrumental, 8D, reverb, mashup, bootleg, acoustic,
  edit, VIP, flip, rework, tribute), matched by the version-aware catalog lookup with a
  duration within ±8 s (±25 s for official videos, audio, lyric videos and visualizers) → segment `[0, duration]`, `Likely`
  0.9, evidence `Metadata`, no audio needed. "Live" counts only as a qualifier
  ("(Live …)", "Live at/in/from …"), not inside a title ("Live Forever").
- Otherwise (or when the qualifier says it is a different version): download the
  entry's audio and run the survey (an entry ≥ 15 min becomes a nested mix). A failing
  entry becomes one `Unidentified` segment; the playlist goes on.
- Up to 2 entries in flight; recognizer calls go through the shared budget.

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
- `mf eval mixes tools/eval/mixes/manifest.json [--mode blind|hinted|both] [--case id]
  [--cache-db path]` (see `tools/eval/mixes/README.md`); `tools/eval/mixes/run-remote.sh`
  runs it on `$MF_EVAL_HOST`. `mf eval mix-draft <url>` drafts a case's ground truth
  from its tracklist. Reports (Markdown, HTML, JSON) go to `--report-dir`, never into
  the repository.

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

## Melody encoder (`melody/encoder.rs`)

An optional recall stage for humming search: a small network (tools/melody-encoder, about
820k parameters) maps 8 s of the 20 fps query contour to a 64-d unit vector, and the songs
whose reference windows lie closest join the matcher's rerank. It finds songs the
matcher's own retrieval misses; the rerank and the score model still decide the answer.

**Companion file.** `melody-encoder-<tier>.mfe` (format 1) belongs to one melody index:
its header names the index by FNV-1a 64 over the index's TRCK section and its track count,
and `EncoderIndex::open` refuses any other index ("belongs to another melody index"). A
missing, damaged or foreign file disables the stage; the matcher runs as without it.
Sections (offset, length) in a 192-byte header, each 64-byte aligned:

| section | content |
|---|---|
| MODL | the encoder as ONNX: `features` (batch, 3, 160) → (batch, 64) unit vectors |
| PQCB | sub-quantizers × 16 × (64 / sub-quantizers) f32 centroids |
| TWIN | (tracks + 1) × u32: first window of each track (windows ordered by track, start) |
| WSTA | windows × u16: start in reference hops |
| CODE | ⌈windows / 32⌉ blocks of sub-quantizers × 16 B: byte j of sub-quantizer s holds window j's code (low nibble) and window j + 16's (high) |
| META | version string |

The header also carries the window length (160), the reference and query hops (40 frames),
the least voiced share (300 ‰) and the most query windows (8). Only the tier's most
popular songs get windows; the others keep an empty TWIN range and are left to the
matcher, which still searches every song.

**Query path** (`EncoderIndex::search`):

1. Windows: the contour trimmed to its voiced span, 160 frames every 40 with the last one
   aligned to the end, kept when ≥ 30 % voiced; a shorter query is one window padded with
   rests. More than the budget's windows are thinned evenly.
2. Features per frame (spec: `melenc.latency.featurize_np`, reproduced bit for bit): pitch
   minus the window's voiced median (lower middle value), rests holding the previous pitch
   and leading rests the median, ÷ 6 clipped to ±2; the voicing flag; the held pitch
   change ÷ 3 clipped to ±2.
3. Embedding with tract; plans for batches of 1, 2, 4 and 8 with concrete shapes, built
   on first use (the model is parsed on the first search).
4. Fast scan per query window: the 64-d query against each sub-quantizer's 16 centroids
   gives a table, quantized to bytes with one scale (as faiss does); every block of 32
   coded windows is summed with `tbl` (NEON: Apple Silicon, Android arm64) or `pshufb`
   (SSSE3: Intel Macs, Android x86_64), or a scalar loop elsewhere. The nearest
   `window_neighbours` windows are rescored exactly from the float table.
5. Songs: each query window's best similarity per song, summed over query windows (or
   the best one, `Pooling::Max`); the top `songs` become `EncoderHit`s with their best
   window pair.
6. `EncoderHit::hint` → `matcher::search_with_hints` / `candidates_with_hints` with the
   budget's `HintOptions`: one placement per (tempo, shift) of `offsets`, where the two
   windows' centres meet, unless retrieval already placed the track there. Hinted
   placements skip the pool cut, carry no votes and compete in the quick stage like any
   other placement. `votes` adds a retrieval pass restricted to the hinted tracks so
   their placements carry index votes (about +110 ms per search on an M4 core; off in
   every budget until retrieval can do it in its first pass); `reserve` sends the first
   N hits to the full rerank even when the quick stage cuts them (off).

Measured on the held-out CHAD half with the c2k model (popularity prior off): top-1
15.3 → 17.1 %, top-10 19.1 → 22.2 % with the Android budget; the encoder stage itself
takes about 4 ms per search on an M4 core and the extra rerank about 11 ms.

Budgets (`EncoderBudget`):

| | Android | Mac | low power |
|---|---|---|---|
| query windows | 6 | 8 | 4 |
| windows kept per query window | 1000 | 1000 | 500 |
| songs | 200 | 500 | 100 |
| placements per hit (tempo, shift frames) | (1, −20), (1, 0), (1, +20) | the same plus (0.8, 0), (1.25, 0) | (1, 0) |

**Build.** `mf index encoder build --index <tier>.mfi --model encoder.onnx --top 30000
--sub-quantizers 32 --out melody-encoder-android.mfe` embeds every window with the
app's featurizer and tract plan (threads `--threads`), trains the 4-bit quantizer by
k-means on 300k sampled windows and writes the file atomically. `--sub-quantizers 64`
gives 32 B per window (the Mac file). `mf index encoder info <file> --index <tier>.mfi`
checks a file against its index. The file must be rebuilt whenever its index is rebuilt
(the digest changes); a new model only needs a new encoder file.

**Release.** The encoder file is published in the same release as its index (tag
`melody-index-2`) and named in that index's manifest under `encoder` (`url`, `sha256`,
`bytes`, `format`, `min_core`); `scripts/package-melody-index.sh <mfi> <version> <base>
<file.mfe>` checks it against the index and writes the entry. Cores that do not know the
field ignore it (`Manifest` has no `deny_unknown_fields`); `Manifest::usable_encoder`
skips entries of another format or for a newer core.

## Library operations

- `correct_segment` sets the track, confidence 1.0, tier Confirmed, adds `User` evidence,
  moves the previous track to alternates (≤ 5) and returns the segment.
- `confirm_candidate` stores the chosen track as the session's primary track and moves
  that candidate to rank 0 with `Strong`.
- `delete_sessions` cancels running sessions first, then deletes rows and recordings.
- `clear_cache` removes `cache_dir/sessions/*` for sessions that are not running.
- `storage_usage` sums the database files, cache dir and melody index.
