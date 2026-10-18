# Music Fetch 1.0 architecture

Music Fetch identifies music from four kinds of input and runs natively on macOS and
Android:

| Input | Examples | How it is identified |
|---|---|---|
| A recording | microphone, system audio, local files, any link with media | Shazam-compatible fingerprints (free), optional AudD / ACRCloud |
| An impression | humming, whistling, singing | on-device melody matching against a melody index, sung-lyrics search, optional ACRCloud humming |
| A streaming link | Spotify, Apple Music, Deezer, Tidal, Amazon Music, SoundCloud, YouTube Music | page metadata, then cross-matched to every service |
| A long mix | DJ sets, radio shows, compilations, playlists | local segmentation plus scheduled fingerprint probes, stitched into a timeline |

Everything that is not platform-specific lives in one Rust crate, `mfcore`, compiled
into both apps. The apps own capture, speech recognition, credentials, the media
download backend and the UI.

```
┌──────────── macOS app (SwiftUI, macOS 15+) ───────────┐  ┌──────── Android app (Compose, API 29+) ────────┐
│ AVAudioEngine mic · Core Audio process tap            │  │ AudioRecord mic · AudioPlaybackCapture         │
│ SpeechTranscriber · Keychain · bundled Python+yt-dlp  │  │ SpeechRecognizer · Keystore · Chaquopy+yt-dlp  │
│ Views (Liquid Glass)                                  │  │ Views (Material 3 Expressive)                  │
└──────────────┬────────────────────────────────────────┘  └───────────────┬────────────────────────────────┘
               │ UniFFI (Swift)                                            │ UniFFI (Kotlin/JNA)
┌──────────────┴───────────────────────────────────────────────────────────┴────────────────────────────────┐
│ mfcore (Rust)                                                                                              │
│ engine · pipeline (live, hum, analyze, mix, stitch) · shazam · melody · lyrics · providers · links ·       │
│ catalog · identity · media (yt-dlp contract, decode) · audio (resample, features) · store (SQLite)         │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Repository layout

```
core/                      Rust workspace (rust-toolchain.toml pins 1.99.0)
  crates/mfcore/           the engine; crate-type lib + staticlib + cdylib; UniFFI proc-macros
  crates/mf-cli/           `mf` developer CLI: recognize, hum, resolve, index, eval
  crates/uniffi-bindgen/   pinned bindgen binaries (uniffi =0.32.2)
apple/                     macOS app
  project.yml              XcodeGen spec (app target: Swift 6, default MainActor isolation)
  MusicFetchCore/          local Swift package: MfCoreFFI.xcframework + generated mfcore.swift (Swift 5 mode)
  MusicFetch/              app sources, resources, Info.plist, entitlements
android/                   Gradle project (AGP 9.4.1, Kotlin 2.4.21, Compose BOM 2026.09.00 + material3 1.5.0-beta01)
  core/                    Android library: generated Kotlin bindings + jniLibs (gitignored, built by script)
  app/                     the app (Chaquopy 17.1 for yt-dlp)
tools/melody-index/        offline melody-index builder (Python; runs on a GPU machine)
scripts/                   build-core-apple.sh, build-core-android.sh, build-macos-app.sh, build-android-app.sh, release.sh
docs/                      this file, CORE_API.md
```

Versions are pinned in `core/Cargo.toml` (`[workspace.dependencies]`),
`android/gradle/libs.versions.toml` and `apple/project.yml`.

## Core principles

1. **One engine, in-process.** No servers, no subprocess per excerpt, no polling. The UI
   receives push events through a foreign `SessionObserver`.
2. **Honest confidence.** Every result carries a calibrated `confidence` (0–1) and a
   display `tier`. Fingerprint matches are tiered by skew and agreement; melody matches
   always show a ranked list.
3. **Free first.** Shazam-compatible fingerprints, the melody index and lyric search need
   no account. AudD and ACRCloud are optional and only called when configured.
4. **Respect remote services.** One serial Shazam queue per install with spacing, jitter,
   429 circuit breaker and result caching. Every HTTP client sends a neutral
   `MusicFetch/<version>` user agent (never personal data).
5. **Bounded resources.** Long media is decoded once to a 16 kHz mono PCM temp file and
   read by window; never hold more than a few minutes of PCM in memory on Android.
6. **Cancellation is absolute.** Every session has a cancel token checked between steps;
   cancelling kills child processes and makes later status writes no-ops.

## mfcore modules

| Module | Responsibility |
|---|---|
| `model` | All FFI-visible records and enums (Track, Segment, Candidate, SessionEvent, ...) |
| `error` | `MfError` (FFI error enum) and internal error helpers |
| `config` | `EngineConfig`, `Credentials`, `MediaToolsConfig` |
| `engine` | `Engine` object: owns runtime, HTTP client, store, Shazam queue, melody index; starts sessions |
| `events` | `SessionObserver` foreign trait, event emitter with throttling |
| `platform` | Foreign traits implemented by the apps: `MediaBackend`, `PlatformDecoder`, `SpeechToText` |
| `util` | HTTP client factory, rate limiter, cancel token, time helpers, temp files |
| `audio` | PCM buffers, resampling (rubato), downmix, decode (symphonia → PCM file), frame features |
| `shazam` | Clean-room signature generator and client (rate limit, circuit breaker, cache) |
| `providers` | AudD, ACRCloud (fingerprint and humming projects) |
| `melody` | Pitch tracking (SwiftF0 ONNX via tract, whistle path), note segmentation, index format, matcher |
| `lyrics` | Sung-lyric search fan-out (iTunes, Genius lyric search, YouTube Music, Deezer), LRCLIB lyrics, alignment re-rank |
| `links` | URL canonicalization, shortener resolution, SSRF guard, site resolvers, generic media discovery |
| `catalog` | Cross-matching and enrichment: Deezer/iTunes lookup, ISRC, artwork, previews, service links |
| `identity` | Fuzzy keys, tiered identity, ISRC veto (ported golden tests) |
| `media` | yt-dlp contract: probe JSON parsing, subprocess backend (macOS/CLI), download orchestration |
| `pipeline` | `live` (mic/system listening), `hum` (impression analysis), `analyze` (links/files), `mix` (long media), `stitch` |
| `store` | SQLite (rusqlite, bundled): sessions, segments, candidates, provider cache, track cache |

The exact FFI surface is specified in [CORE_API.md](CORE_API.md). Internal module APIs
are free to evolve as long as that surface holds.

## Recognition pipelines

### Live listening (microphone, system audio)

1. The app pushes interleaved f32 PCM (any rate, any channel count) into
   `LiveSession::push_pcm`. The core downmixes and resamples to 16 kHz mono and keeps a
   rolling 30 s buffer.
2. Every ~33 ms the core emits `Level`; every ~100 ms it emits batched `Pitch` frames
   from the streaming SwiftF0 tracker.
3. `ListenMode::Auto` (default) and `Recording`: Shazam queries on the newest audio at
   t ≈ 4 s, 8 s, 13 s, 20 s, 28 s (clip length = min(elapsed, 12 s)). A match ends the
   session (`Outcome::Matched`). AudD/ACRCloud, when configured, are tried once if
   Shazam has not matched by 10 s.
4. In parallel, a voice-melody detector (voicing ratio, monophony, spectral flatness)
   decides whether the input is someone humming/singing/whistling. In `Auto`, once that
   is likely, the session switches to impression rules: the melody is searched every
   2 s while the user hums, and capture stops once that search is confident, after
   ≥ 8 s voiced audio followed by 1.5 s of silence, or at 30 s. Less than 4 s voiced
   asks the user to keep going instead of answering.
5. `ListenMode::Hum` skips Shazam and only runs impression analysis.
6. Without a recording match, impression analysis runs on the captured audio (see
   below). With too little signal the outcome is `NoMatch` with a reason code
   (`TooQuiet`, `TooShort`, `Noisy`, `MostlySpeech`, `CaptureSilent`).

### Impression analysis (hum / whistle / sing)

Run in parallel, then fused by identity:

- **Melody:** f0 (SwiftF0; whistle path above ~1.2 kHz via the half- or quarter-speed trick) → median
  filter, octave fixing, gap handling → notes + 20 fps contour → retrieval on continuous
  per-song contours by key-invariant window vectors (inverted multi-index, alignment
  voting) and by note n-grams → quick DTW + rhythm-free note alignment → linear scaling →
  subsequence DTW with key search → calibrated score fusion (stage evidence, reference
  length, popularity, and how conclusive the hum itself is) → one answer per work.
  Requires the melody index (downloadable).
- **Lyrics:** if speech is present, the app's `SpeechToText` transcribes the captured
  audio (macOS SpeechTranscriber, Android on-device recognizer). The core searches
  6–10-word windows across iTunes, Genius lyric search, YouTube Music and Deezer,
  fetches lyrics for the top candidates from LRCLIB and re-ranks by fuzzy word
  alignment.
- **ACRCloud humming** when a humming project is configured.

Output: 1–5 `Candidate`s, each with sources, a calibrated score, a tier
(`Strong`, `Possible`, `LongShot`) and, for melody matches, a `MelodyAlignment`
(query and reference contours) for the "your hum vs. the song" overlay.

### Links and files

1. `links::resolve` canonicalizes the URL (tracking stripped, shorteners resolved with
   the SSRF guard on every hop) and routes by host:
   - Spotify, Apple Music, Deezer, Tidal, Amazon Music: metadata → `Track`s directly
     (track, album, playlist). No audio needed.
   - TikTok: page JSON music attribution (Apple/Spotify IDs) as a hint, then audio.
   - YouTube: "Music" cards / Shorts sound as hints, then audio.
   - SoundCloud, Vimeo, Instagram, X, Reddit, Twitch, Bandcamp, Mixcloud and any other
     page: media through the `MediaBackend` (yt-dlp) or generic page discovery.
2. Audio: download best audio (m4a preferred) → decode once to a 16 kHz mono PCM file.
3. Duration < 4 min and no chapters → **single** strategy: up to 3 energy-ranked 12 s
   probes until a confident match (2 agreeing probes, or 1 match with small skew).
4. Otherwise → **mix** strategy: features at 1 s hops → novelty boundaries + chapter /
   description timestamps → music/speech/silence labels → probe queue in time order →
   Shazam offset + track duration project coverage → skip covered regions → extra
   probes for unresolved music → stitch (bridge rules ported from 0.4) → tiers.
5. Playlists: entries are processed in order as child items of one session.

### Confidence and tiers

| Evidence | confidence | Segment tier |
|---|---|---|
| ≥ 2 agreeing probes or providers | 0.95 | Confirmed |
| 1 Shazam match, max(|timeskew|, |frequencyskew|) ≤ 0.002 | 0.88 | Likely |
| 1 Shazam match with larger skew (DJ pitch) | 0.72 | Likely |
| ACRCloud score s (70–100) | s/100 × 0.95 | Likely if ≥ 0.8 else Possible |
| AudD single match | 0.8 | Likely |
| Streaming-link metadata | 0.99 | Confirmed |
| Page attribution hint only (no audio match) | 0.55 | Possible |
| User correction | 1.0 | Confirmed |

## Platform layer contracts

- **MediaBackend** (`probe`, `download_audio`): macOS uses the core's built-in subprocess
  backend pointed at the bundled Python + yt-dlp + `qjs`; Android implements the trait
  with Chaquopy. The CLI uses the subprocess backend with system `yt-dlp`.
- **PlatformDecoder** (`decode_to_wav`): fallback when symphonia cannot decode a file
  (WebM/Opus on macOS, HE-AAC, odd containers). AVFoundation / MediaExtractor+MediaCodec.
- **SpeechToText** (`transcribe`): blocking, returns up to 5 hypotheses. Optional.
- **Credentials** live in Keychain / Android Keystore-encrypted storage and are passed to
  `Engine::set_credentials`; the core never persists them.

## Data

- `data_dir/musicfetch.sqlite3`: sessions, segments, candidates, track cache, provider
  cache (positive results 30 days, negative 1 day). WAL, foreign keys, busy timeout.
- `data_dir/melody/`: melody index file + manifest. Downloaded on demand from the
  `melody-index-<format>` GitHub release of the index format the core reads
  (`melody-index-2` since 1.1; 1.0.x reads `melody-index-1`), verified by SHA-256.
- `cache_dir/sessions/<id>/`: downloads and PCM files, removed when a session finishes
  unless the user pins it.

## Build and release

- `scripts/build-core-apple.sh` → `apple/MusicFetchCore/MfCoreFFI.xcframework` + Swift bindings.
- `scripts/build-core-android.sh` → `android/core/src/main/jniLibs/{arm64-v8a,x86_64}` + Kotlin bindings.
- `scripts/build-macos-app.sh` → `dist/Music Fetch.app` (universal arm64 + x86_64, macOS 15+,
  ad-hoc signed with the stable designated requirement `identifier "local.musicfetch.app"`)
  and a `ditto` zip.
- `scripts/build-android-app.sh` → signed release APK.
- CI (GitHub Actions): Rust fmt/clippy/test on macOS; Android assembleRelease (unsigned)
  on Ubuntu; macOS app build on macos-26.
