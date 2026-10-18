# mfcore API contract

The FFI surface is defined in Rust (`core/crates/mfcore/src/{engine,model,config,platform,error}.rs`)
and generated with UniFFI 0.32.2:

- Swift module `MusicFetchCore` (C module `MfCoreFFI`), built by `scripts/build-core-apple.sh`.
- Kotlin package `app.musicfetch.core`, built by `scripts/build-core-android.sh`.

This document describes the behaviour the types cannot express.

## Lifecycle

1. The app creates one `Engine` at launch with `EngineConfig` and optional platform
   services. Construction opens the database, marks sessions left `Running` by a crash
   as `Failed`, and opens the melody index if installed. It is cheap enough for the
   main thread (< 50 ms) but apps should create it off the main thread anyway.
2. `set_credentials` may be called at any time; running sessions keep the
   credentials they started with.
3. Sessions (`LiveSession`, `AnalysisSession`) run on the engine's worker threads. A
   session handle can be dropped by the app without cancelling the session; call
   `cancel()` to stop it.

## Events

- Every session emits exactly one `Started` first and exactly one terminal event last:
  `Finished`, `Failed` or `Canceled`. After `cancel()` the session emits `Canceled`
  (only stray `Level`/`Pitch` events may precede it) and is stored with status `Canceled`.
- Events are delivered on engine threads, in order per session. Observers must not
  block; hop to the UI thread.
- `Level` ~30 Hz and `Pitch` ~10 Hz are only emitted by live sessions.
- `Segment` events are upserts keyed by `Segment.id`. `SegmentRemoved` follows when
  stitching merges segments. The final segment list equals `Engine::session(id).segments`.
- `Candidates` may arrive several times (`is_final = false` while sources are still
  running); the last one has `is_final = true` and precedes `Finished`.
- `Progress.total_ms` is the source duration; `analyzed_ms` only grows.

## Live sessions

- `push_pcm(data, sample_rate, channels)`: `data` is interleaved little-endian f32.
  Any rate (8–192 kHz) and channel count. The call copies into a queue and returns
  immediately; it never blocks on analysis. Kotlin must pass a direct `ByteBuffer`.
- Changing `sample_rate`/`channels` mid-session is allowed (the resampler restarts).
- `finish()` means the user stopped capture: the core analyzes what it has and emits
  the outcome. Calling `push_pcm` after `finish()` is ignored.
- The core decides when it is done (match found, impression auto-stop, 30 s limit) and
  emits `Finished`; the app should stop capture when it sees a terminal event.
- `CaptureSource::SystemAudio` always uses `ListenMode::Recording` rules.
- If 3 s of pushed audio are digital silence the core emits `Notice::CaptureSilent`
  (permission denied or the source app blocks capture); the session continues until
  the app stops it or the 30 s limit.
- `Finished { Waiting }`: the recognizer could not be reached at all (no network). The
  session is stored with status `Waiting` and `Engine::retry_waiting` identifies it
  later; the app says it was saved for later instead of "no match". (The live pipeline
  does not produce it yet; the apps already handle it.)

### Humming, whistling, singing

- `start_listening_with(source, mode, ListenOptions { auto_stop, live_candidates, transcripts })`;
  `start_listening` uses the defaults (`auto_stop` on, `live_candidates` and `transcripts` off).
- While the user hums (`ListenMode::Hum`, or `Auto` after `Notice::ImpressionDetected`)
  the core searches the melody index every 2 s (4 s in low-power mode) once 4 s are
  voiced. With `auto_stop` the session stops by itself as soon as an answer is clear (a
  top answer scoring ≥ 0.8 that the previous search also had on top), after a phrase
  (≥ 8 s voiced, then a 1.5 s pause), after a 5 s pause, or at 30 s. Without it, only
  `finish()` and the 30 s limit stop it.
- `live_candidates: true` makes each search's melody candidates arrive as
  `Candidates { is_final: false }` while the phase is still `Listening`. They are
  provisional: melody only, not enriched (no artwork yet), and each list replaces the
  previous one. The final list (`is_final = true`, after `Phase::MatchingMelody`,
  with lyrics/ACRCloud and enrichment) follows when capture ends.
- `Notice::KeepGoing` means the hum is too short to answer from: the core sends it once
  during a pause before 4 s are voiced, and when capture ends with less than 4 s voiced
  (then the outcome is `NoMatch(TooShort)` rather than a guess).
- Candidate tiers: melody scores read as the chance the answer is right. On real hums
  (CHAD, 58k songs) `Strong` (≥ 0.75) answers are right about 19 times in 20, `Possible`
  (0.35–0.75) about 6 in 10, `LongShot` rarely; interim lists while humming are
  calibrated the same way.

What the apps need to adopt progressive results (until then they keep today's flow; the
early confident stop already shortens their sessions):

1. Start hum and auto sessions with `start_listening_with(.., ListenOptions { auto_stop:
   <setting>, live_candidates: true }, ..)`.
2. While `Phase::Listening`, treat `Candidates { is_final: false }` as a live preview:
   show the top one or two under the listening UI ("Sounds like …" with its tier), and
   do not switch to the result screen or announce it as a result. Replace the preview
   on every new list; clear it when a list is empty.
3. Switch to the candidates/result screen only on the final list (`is_final = true`) or
   on `Finished`. Accessibility announcements: once, for the final list.
4. On `Notice::KeepGoing` show a short "Keep humming" hint during capture; after a
   `NoMatch(TooShort)` offer "Hum again".
5. Stop capture (and the microphone indicator) on the terminal event as today; with
   `auto_stop` the core may finish well before the user stops humming.

### Sung words

Takes sung with words are also found by their lyrics: the engine runs its lyric search
on what the speech recognizer heard (when it holds at least 3 distinct real words, not "la la"
or "hmm") and fuses those candidates with the melody (and ACRCloud) candidates on one
calibrated scale; a song both find gets a bonus. There are two ways to hand over the
words; the first is preferred because it costs no time after capture:

1. **Push transcripts while the user sings.** Start the session with
   `ListenOptions { transcripts: true, .. }`, feed the same microphone audio to the
   on-device recognizer, and send each result with `LiveSession::push_transcript`. Each
   call replaces the previous one; send partial results as they arrive (`is_final:
   false`) and the recognizer's last result with `is_final: true`, also when it arrives
   after capture stopped (`finish()` or an auto-stop). When capture ends the engine
   waits up to 2 s for the final transcript, then uses the newest one. It does not call
   `SpeechToText` for such a session. Transcripts pushed without `transcripts: true`
   are used too, but nothing is waited for.
2. **Let the engine ask.** Without pushed transcripts the engine calls the
   `SpeechToText` service (if the app supplied one) with a WAV of the capture after
   capture ends.

`SpeechTranscript { text, alternatives, language, confidence, is_final }`: `alternatives` are
further hypotheses, best first (up to 4 are used); `language` is the recognizer's BCP-47
tag (None: `EngineConfig.language`); `confidence` in 0..=1 when the recognizer reports
one (None, 0 or negative mean unknown; a low confidence lowers the lyric candidates'
scores, to 0.4× at 0). The best hypothesis is stored as the session's `transcript`.

Swift (macOS: SFSpeechRecognizer with `SFSpeechAudioBufferRecognitionRequest`, or
SpeechAnalyzer on macOS 26, fed the buffers that go to `pushPcm`):

```swift
let options = ListenOptions(autoStop: true, liveCandidates: true, transcripts: true)
let session = try engine.startListeningWith(source: .microphone, mode: .hum, options: options, observer: observer)
// For every recognizer result (partial ones too):
session.pushTranscript(transcript: SpeechTranscript(
    text: result.bestTranscription.formattedString,
    alternatives: result.transcriptions.dropFirst().map(\.formattedString),
    language: "en-US",
    confidence: nil,            // or the mean segment confidence of a final result
    isFinal: result.isFinal))
```

Kotlin (Android 13+: `SpeechRecognizer` with `EXTRA_PARTIAL_RESULTS` and the capture fed
through `EXTRA_AUDIO_SOURCE`, so it does not compete for the microphone; below Android 13
there is no shared-audio recognizer and hum sessions run on melody alone):

```kotlin
val options = ListenOptions(autoStop = true, liveCandidates = true, transcripts = true)
val session = engine.startListeningWith(CaptureSource.MICROPHONE, ListenMode.HUM, options, observer)
// onPartialResults / onResults:
val texts = bundle.getStringArrayList(SpeechRecognizer.RESULTS_RECOGNITION).orEmpty()
val scores = bundle.getFloatArray(SpeechRecognizer.CONFIDENCE_SCORES)
session.pushTranscript(SpeechTranscript(
    text = texts.firstOrNull().orEmpty(),
    alternatives = texts.drop(1),
    language = "en-US",
    confidence = scores?.firstOrNull()?.takeIf { it > 0f },
    isFinal = isFinalResult,
))
```

## Analysis sessions

- `analyze(input, mode, observer)`: `input` is a URL or an absolute file path.
  Validation errors (empty input, unsupported scheme, missing file) are returned
  synchronously as `MfError`; everything after that arrives as events.
- Links to streaming services finish fast with `Segment`s of kind `Track` (one per
  track, `start_ms = end_ms = 0` for collections, and `item_index` = position).
- Media sessions emit `Source`, then `Phase` changes, `Progress`, `Segment`s as they
  are found, and `Finished { Completed { identified, total } }`.

## Platform services

| Trait | macOS | Android | When the core calls it |
|---|---|---|---|
| `MediaBackend` | not supplied: the core runs the bundled yt-dlp via `EngineConfig.media_tools` | Chaquopy wrapper | links that are not streaming metadata |
| `PlatformDecoder` | AVFoundation | MediaExtractor + MediaCodec | symphonia fails on a file |
| `SpeechToText` | SpeechTranscriber (macOS 26), SFSpeechRecognizer (macOS 15) | on-device SpeechRecognizer | impression sessions without pushed transcripts ("Sung words") |

All three are called from engine worker threads, may block, must honour
`TaskHandle.is_canceled()` and must return `PlatformError::Canceled` when canceled.

## Errors

`MfError` is returned synchronously for invalid calls. Runtime failures inside a
session become a `Failed { code, message }` event. Messages are short, user-presentable
English sentences without stack traces or URLs containing secrets.

## Melody index

- Manifest (`EngineConfig.melody_index_manifest_url`), JSON:
  `{"version": "2026.11.1", "url": "https://…/melody-index.mfi", "sha256": "…", "bytes": 12345678, "songs": 41234, "min_core": "1.1.0", "format": 2}`
- One manifest per tier in the format's release: Android apps read
  `…/melody-index-2/manifest-android.json` (the phone tier), Mac apps
  `…/melody-index-2/manifest-mac.json` (the large tier); `scripts/package-melody-index.sh`
  writes them with `TIER=android|mac`.
- Installed under `data_dir/melody/` as `melody-index.mfi` + `manifest.json`, plus
  `melody-encoder.mfe` when the index has an encoder file.
- File format 2 (one continuous contour per song and source, window and note indexes; see
  `melody/index.rs`). Each format is published under its own release tag,
  `melody-index-<format>`, with `min_core` set to the first core that reads it (format 2:
  1.1.0) and `format` naming it; the apps point at the tag of their core's format. A
  manifest of another format (one without `format` is format 1) fails with
  `IndexUnavailable` before anything is downloaded, and an installed index of another
  format is deleted at startup, so the app offers the download again.
- Without an index, impression sessions still run lyrics and ACRCloud humming and emit
  `Notice::MelodyIndexMissing`.
- Optional `encoder` object: the index's learned-encoder file (`melody/encoder.rs`,
  docs/PIPELINES.md "Melody encoder"), e.g.
  `"encoder": {"url": "https://…/melody-encoder-android.mfe", "sha256": "…", "bytes": 13000000, "format": 1, "min_core": "1.1.0"}`.
  It belongs to exactly that index file (it names the index by a digest of its track
  table). Cores ignore entries they cannot use (`Manifest::usable_encoder`), and the
  index works without it.
- `install_melody_index` downloads the encoder file after the index, in the same session
  (one progress bar over both files), checks its SHA-256 and that it belongs to the new
  index, and swaps it in; searches use it from then on. If only the encoder fails, the
  index is still installed and the session finishes normally. An index that is already
  current but lacks its encoder (or has an older one) gets just the encoder file. A new
  index replaces the old encoder, `remove_melody_index` deletes both, and a file that
  does not belong to the installed index is deleted at startup.
- `MelodyIndexStatus`: `encoder_installed` says the encoder is in use; `size_bytes`
  covers both files; `download_bytes` is what `install_melody_index` would fetch (index
  and encoder). When only the encoder is missing, `available_version` is the installed
  version and `download_bytes` the encoder's size, so the app's usual "Download" offer
  covers it with no new UI. `storage_usage().melody_index_bytes` includes it.

## Melody builder input (JSONL)

One song per line, produced by `tools/melody-index` and consumed by `mf index build`:

```json
{"title": "Song", "artist": "Artist", "isrc": "USUM71703861", "deezer_id": 123, "year": 2017,
 "popularity": 0.83,
 "phrases": [{"source": "preview", "start_ms": 48000,
              "notes": [[64.1, 0, 310], [66.0, 320, 280]]}]}
```

- `notes`: `[pitch, onset_ms, dur_ms]`, pitch in fractional MIDI numbers, onset relative to
  the phrase start, sorted by onset, monophonic (no overlaps).
- `source`: `"preview"` (30 s audio preview), `"audio"` (full track audio) or `"midi"`.
- `start_ms`: the phrase's position in its source. The phrases of one source are placed on
  one timeline (rests between them stay rests), so a hum may span several; phrases that
  overlap in time (two MIDI parts) become separate timelines.
- `popularity` in 0..=1; `isrc`, `deezer_id`, `year` may be null.
- Optional `contours`: frame melodies, e.g.
  `[{"source": "preview", "fps": 20, "pitch": [61.25, 61.5, null, …]}]` with fractional
  MIDI pitches and `null` for unvoiced frames (other rates are resampled to 20 fps). A source
  with a contour ignores its phrases.
