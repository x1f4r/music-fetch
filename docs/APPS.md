# App architecture

Both apps are thin native shells around `mfcore`: capture, platform services, state and
views. Visual design and copy: [design/DESIGN.md](design/DESIGN.md).

## Shared structure

| Concern | macOS (Swift 6, SwiftUI, macOS 26) | Android (Kotlin, Compose, API 29+) |
|---|---|---|
| Engine host | `EngineHost` actor: builds `EngineConfig`, creates `Engine` off the main thread, owns platform services | `EngineHost` singleton in `MusicFetchApp` (Application) |
| Event bridge | `ObserverBridge: SessionObserver` → `AsyncStream<SessionEvent>` consumed on the MainActor | `ObserverBridge` → `callbackFlow`/`Channel` collected in `viewModelScope` |
| Microphone | `AVAudioEngine` input tap (hardware format, no voice processing) → `pushPcm` | `AudioRecord` (UNPROCESSED → VOICE_RECOGNITION → MIC), 48 kHz float, in a `microphone` foreground service |
| System audio | Core Audio process tap (`CATapDescription` global tap excluding own process) + private aggregate device | `AudioPlaybackCapture` via `MediaProjection` in a `mediaProjection` foreground service |
| Media backend | none: `EngineConfig.media_tools` points at the bundled Python + yt-dlp + qjs (`MediaRuntime`) | `ChaquopyMediaBackend` (Python 3.13 + yt-dlp + bundled `libqjs.so`) |
| Decoder fallback | `AVFoundationDecoder` (AVAssetReader → 16-bit mono WAV) | `MediaCodecDecoder` (MediaExtractor + MediaCodec) |
| Speech | `SpeechTranscriberService` (SpeechAnalyzer/SpeechTranscriber, on-device; DictationTranscriber for other locales) | `AndroidSpeechToText` (`createOnDeviceSpeechRecognizer`, `EXTRA_AUDIO_SOURCE` on API 33+) |
| Credentials | Keychain (generic passwords, service `local.musicfetch.app.credentials`) | DataStore with values encrypted by an Android Keystore AES-GCM key |
| Settings | `@AppStorage` / `UserDefaults` | DataStore preferences |
| Background work | not needed (app keeps running) | link/file analysis in a foreground service (`dataSync`, `mediaProcessing` on API 35+) with a progress notification |
| Entry points | main window, `MenuBarExtra` (`.window`), Services/Share, drag & drop, ⌘V, URL scheme `musicfetch://analyze?url=` | launcher, share sheet (`ACTION_SEND` text/audio/video), QS tile, Glance widget, `musicfetch://` deep link |

## State models

One observable model per screen, fed by engine events. Names are the same on both
platforms.

- `ListenModel` — `phase: idle | listening(source, mode) | analyzing | result`;
  live `level`, `pitchTrace` (ring of the last 8 s of `PitchFrame`s), `impression: Bool`,
  `notice`, `elapsed`; result: `matched(Track) | candidates([Candidate]) | noMatch(reason)`;
  actions `start(source, mode)`, `stop()`, `cancel()`, `retryAsHum()`.
- `SessionModel` — for link/file/lyrics sessions and library detail: `source`,
  `phase`/`fraction`, `progress` (analyzed/total/ETA), ordered `segments` (upsert by id,
  removal), `candidates`, `items` (playlist entries), `outcome`, `error`; actions
  `cancel()`, `export(format)`, `correct(segment, track)`, `confirm(candidate)`.
- `LibraryModel` — paged `LibraryEntry` list grouped by day, filter (All, Heard, Hummed,
  Links, Mixes), search text, pin/delete; refreshes when sessions finish.
- `SettingsModel` — default service, credentials, melody index status/install/remove,
  storage usage/clear cache, platform toggles.

Rules:

- Engine calls that can block (`Engine.new`, `library`, `session`, exports) run off the
  main thread.
- Each live/analysis session handle is kept by its model; `cancel()` on screen exit only
  for live sessions (link analyses continue in the background and land in the library).
- The UI never parses strings from the engine: it switches on enums (`Phase`, `Notice`,
  `NoMatchReason`, `ErrorCode`) and maps them to localized copy.
- Opening a service link prefers the user's default service; fall back to the first
  available link.

## Permissions

- Microphone: pre-prompt sheet once, then the system prompt; denied → inline banner
  with "Open System Settings".
- System audio (macOS): explain once, then start the tap (the system prompt appears on
  start). Silent capture (engine `Notice::CaptureSilent`) → banner with "Open System
  Settings" (Privacy & Security → Screen & System Audio Recording).
- System audio (Android): one-line explanation, then the MediaProjection consent each
  session; silent capture → "This app blocks audio capture. Use the microphone instead."
- Notifications (Android 13+): requested the first time a background analysis starts.

## Builds

- macOS: `scripts/build-macos-app.sh` (core → runtime → XcodeGen → xcodebuild → bundle
  runtime → ad-hoc sign with the stable designated requirement → zip).
- Android: `scripts/build-android-app.sh` (runtime → core → Gradle assembleRelease →
  signed APK).
