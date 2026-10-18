# App architecture

Both apps are thin native shells around `mfcore`: capture, platform services, state and
views. Visual design and copy: [design/DESIGN.md](design/DESIGN.md).

## Shared structure

| Concern | macOS (Swift 6, SwiftUI, macOS 15+, universal; Liquid Glass on macOS 26) | Android (Kotlin, Compose, API 29+) |
|---|---|---|
| Engine host | `EngineHost` actor: builds `EngineConfig`, creates `Engine` off the main thread, owns platform services | `EngineHost` singleton in `MusicFetchApp` (Application) |
| Event bridge | `ObserverBridge: SessionObserver` → `AsyncStream<SessionEvent>` consumed on the MainActor | `ObserverBridge` → `callbackFlow`/`Channel` collected in `viewModelScope` |
| Microphone | `AVAudioEngine` input tap (hardware format, no voice processing) → `pushPcm` | `AudioRecord` (UNPROCESSED → VOICE_RECOGNITION → MIC), 48 kHz float, in a `microphone` foreground service |
| System audio | Core Audio process tap (`CATapDescription` global tap excluding own process) + private aggregate device | `AudioPlaybackCapture` via `MediaProjection` in a `mediaProjection` foreground service |
| Media backend | none: `EngineConfig.media_tools` points at the bundled Python + yt-dlp + qjs (`MediaRuntime`) | `ChaquopyMediaBackend` (Python 3.13 + yt-dlp + bundled `libqjs.so`) |
| Decoder fallback | `AVFoundationDecoder` (AVAssetReader → 16-bit mono WAV) | `MediaCodecDecoder` (MediaExtractor + MediaCodec) |
| Speech | `SpeechTranscriberService` (macOS 26: SpeechAnalyzer/SpeechTranscriber, DictationTranscriber for other locales; macOS 15: `SFSpeechRecognizer`; on-device only) | `AndroidSpeechToText` (`createOnDeviceSpeechRecognizer`, `EXTRA_AUDIO_SOURCE` on API 33+) |
| Credentials | Keychain (generic passwords, service `local.musicfetch.app.credentials`) | DataStore with values encrypted by an Android Keystore AES-GCM key |
| Settings | `@AppStorage` / `UserDefaults` | DataStore preferences |
| Background work | not needed (app keeps running) | link/file analysis in a foreground service (`dataSync`, `mediaProcessing` on API 35+) with a progress notification |
| Entry points | main window, `MenuBarExtra` (`.window`), Services/Share, drag & drop, ⌘V, URL scheme `musicfetch://analyze?url=` | launcher, share sheet (`ACTION_SEND` text/audio/video), QS tile, Glance widget, `musicfetch://` deep link |

## State models

One observable model per screen, fed by engine events. Names are the same on both
platforms.

- `ListenModel` — `phase: idle | listening(source, mode) | analyzing | result`;
  live `level`, `pitchTrace` (ring of the last 8 s of `PitchFrame`s), `impression: Bool`,
  `notice`, `elapsed`; result: `matched(Track) | candidates([Candidate]) |
  noMatch(reason) | offline` (`Outcome::Waiting`: saved to identify once online);
  actions `start(source, mode)`, `stop()`, `cancel()`, `retryAsHum()`, `select(source)`
  (a source toggle: chooses the method without listening and leaves a finished state).
  On macOS the toggles stay on the stage in every state; recovery actions (Try again,
  Hum it, Type a lyric, …) sit in their own row below them. Try again repeats the last
  method, or analyzes the same link or file again.
- `SessionModel` — for link/file/lyrics sessions and library detail: `source`,
  `phase`/`fraction`, `progress` (analyzed/total/ETA), ordered `segments` (upsert by id,
  removal), `candidates`, `items` (playlist entries), `outcome`, `error`; actions
  `cancel()`, `export(format)`, `correct(segment, track)`, `confirm(candidate)`.
- `LibraryModel` — paged `LibraryEntry` list grouped by day, filter (All, Heard, Hummed,
  Links, Mixes, Pinned), search text, pin/delete; refreshes when sessions finish. Pinning
  takes any number of sessions at once (`setPinned(ids, pinned:)`, `togglePinned(ids)`:
  pins all unless all are pinned), updates the lists at once, writes to the store off
  the main thread and registers Undo.
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

## macOS window

- One inspector, attached to the `NavigationSplitView`, owns its toggle in its own
  toolbar, so the toolbar keeps every content item left of the inspector divider.
  Below 1060 pt (832 pt without the sidebar) it folds away and comes back when the
  window widens, without changing the preference.
- The toolbar's Listen is a split button: listen again with the current method, or
  choose another method, a link, a file or a lyric from its menu. Results on the Listen
  pane have a back button (⌘[, Esc); the sidebar's Listen row also returns to the stage.
- Pin is the only way to keep sessions (no separate favorites).

## Screenshots and layout checks (macOS)

- `apple/MusicFetchUITests` launches every demo state (`-MFDemoState`, with
  `-MFWindowSize`, `-MFAppearance`, `-MFReduceMotion`, `-MFInspector`) and attaches
  window screenshots and toolbar geometry checks (no toolbar item on a split divider,
  Listen present, one center line). The Screenshots workflow runs it on macOS 26 and
  macOS 15 runners and publishes a contact sheet (`screenshots` artifact, `index.html`),
  built by `scripts/screenshot-sheet.py`. Never run the UI tests on a Mac someone is
  using: they launch the app.
- Debug builds also have an offscreen check, `apple/tools/render-offscreen.sh`: it lays
  the main window out in windows that are never shown (`-MFRender`), at 900–1440 pt,
  with and without the inspector, and fails on the same geometry rules. It runs a copy of
  the Debug app under its own bundle identifier, so the installed app's preferences and
  permissions are never touched. Liquid Glass doesn't draw offscreen; its PNGs only show
  layout.
- Demo states run on `FakeEngine` and keep everything they change (inspector, Settings
  pane, provider, source) in memory.

## Permissions

- Microphone: pre-prompt sheet once, then the system prompt; denied → inline banner
  with "Open System Settings".
- System audio (macOS): explain once, then start the tap (the system prompt appears on
  start). Silent capture (engine `Notice::CaptureSilent`) → banner with "Open System
  Settings" (Privacy & Security → Screen & System Audio Recording).
- System audio (Android): one-line explanation, then the MediaProjection consent each
  session; silent capture → "This app blocks audio capture. Use the microphone instead."
- Notifications (Android 13+): requested the first time a background analysis starts.

## Android Quick Settings tiles

Two tiles, so the user adds whichever they want to the quick settings shade:

- **Identify song** (microphone, `ListenMode::Auto`: plays, hums, whistles and singing).
- **Identify phone audio** (`CaptureSource::SystemAudio`).

A tap goes through an invisible trampoline activity (`startActivityAndCollapse(PendingIntent)`),
which starts the capture foreground service (`microphone` or, after the per-session
MediaProjection consent, `mediaProjection`) and finishes immediately. The tile shows
`STATE_ACTIVE` while capturing; tapping it again stops. The ongoing notification says
what is being captured ("Listening with the microphone" / "Listening to phone audio"),
shows elapsed time and has a Stop action. When the session ends the notification is
replaced by the result: matched track (actions "Open in <default service>", "Open"),
best impression candidate ("Best guess: … · See all"), or the no-match reason with
"Try again". Tapping it opens the session in the app.

## Opening links

The user picks **one preferred provider**; every result's primary button opens the track
there ("Open in YouTube Music").

- **Startup panel:** on launch, while no provider decision is stored, the app shows a
  "Where do you listen?" panel with the most common services (Spotify, YouTube Music,
  Apple Music, Tidal, Amazon Music, Deezer, SoundCloud; "More…" for the full list and a
  custom provider), pre-selecting an installed app when one is detected (Android via
  `PackageManager`; macOS via installed apps such as Music, Spotify). Choosing stores the
  provider. **Skip** stores the decision with Spotify as the provider. Either way the panel
  never appears again. The decision is written only when the user chooses or skips, so a
  crash, quit or restart before that (e.g. around permission prompts) shows the panel
  again on the next launch. The provider stays editable in Settings → General → "Open
  songs in".
- **Switch per tap:** the primary button is a split button. Its chevron opens a menu of
  all other providers; choosing one opens the track there once without changing the
  setting. Exact links first, search links marked as such.
- **Providers:** everything `catalog::provider_links` returns — Spotify, Apple Music,
  YouTube Music, YouTube, Deezer, Tidal, Amazon Music, SoundCloud, Qobuz, Pandora,
  Audiomack, Anghami, JioSaavn, Boomplay, KKBOX, Yandex Music, NetEase Cloud Music,
  Bandcamp, Shazam, Last.fm, Genius — plus a **custom provider**: a name and a URL
  template with `{artist}`, `{title}`, `{query}`, `{isrc}` placeholders, for any other
  app or site.
- Link choice for a provider: the exact track link when known, else the provider's
  search link for "artist title" (version qualifiers kept).
- Android: if the provider's app is installed (known package per provider, declared in
  `<queries>`), launch `ACTION_VIEW` with `setPackage(...)` so it opens directly in that
  app; Spotify search uses `spotify:search:`. Otherwise open the https URL normally.
- macOS: `NSWorkspace.open` (universal links hand off to installed apps); Apple Music
  links open in Music via the `music://` form.
- Segment deep links (`Segment.source_url`, e.g. YouTube `&t=`) open the source at the
  timestamp.

## Privacy

The apps are distributed to testers and the repository is public:

- No analytics, telemetry or crash upload. No accounts.
- Network requests carry no personal data; the core uses a neutral user agent.
- Credentials live only in Keychain / Keystore-encrypted storage, are never logged,
  exported or included in diagnostics.
- No personal identifiers, machine names, local paths, tokens or keys in the source,
  scripts, docs, fixtures or build outputs. Release builds run a secret scan (gitleaks)
  and a grep for personal strings before publishing.

## Builds

- macOS: `scripts/build-macos-app.sh` (core → runtime → XcodeGen → xcodebuild → bundle
  runtime → ad-hoc sign with the stable designated requirement → zip).
- Android: `scripts/build-android-app.sh` (runtime → core → Gradle assembleRelease →
  signed APK).
