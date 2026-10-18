# Music Fetch for macOS

SwiftUI app for macOS 15 and later on top of the Rust engine (`MusicFetchCore`), one
universal binary for Apple silicon and Intel Macs. On macOS 26 it uses Liquid Glass and
SpeechAnalyzer; before that, native materials, bordered button styles and
`SFSpeechRecognizer` (`App/Compat.swift`, `Views/Components/GlassCompat.swift`).
Architecture: [docs/APPS.md](../docs/APPS.md); screens and copy:
[docs/design/DESIGN.md](../docs/design/DESIGN.md).

## Build

```sh
scripts/build-macos-app.sh            # core, media runtime, Release build, signing, zip → dist/
scripts/privacy-check.sh dist         # before sharing a build
```

The engine, the app and the bundled runtime (CPython, yt-dlp, QuickJS) are universal
(arm64 + x86_64); the build checks every Mach-O for both architectures and a macOS 15
deployment target. Every configuration, Debug included, is signed with the designated
requirement in `Signing.rqs`, so microphone and system-audio permissions survive rebuilds.

For day-to-day work run `scripts/build-core-apple.sh` once, then
`xcodegen generate --spec apple/project.yml` and build the `MusicFetch` scheme in Xcode.
Unit tests (`MusicFetchTests`) run with the scheme's Test action; they host the app on
the demo engine, never the real library.

## Layout

| Folder | Contents |
|---|---|
| `MusicFetch/App` | `@main` scenes, `AppModel` (engine, navigation, app-wide actions), menus, dock menu and Services, App Intents, demo states |
| `MusicFetch/Engine` | `EngineHost` (engine creation), `MusicEngine` (engine surface), `ObserverBridge`, `FakeEngine` and its fixtures |
| `MusicFetch/Models` | `ListenModel`, `SessionModel`, `LibraryModel`, `SettingsModel`, `LyricSearchModel`, copy and formatting |
| `MusicFetch/Platform` | capture (microphone, Core Audio process tap), AVFoundation decoder, speech, Keychain, links, previews, notifications |
| `MusicFetch/Views` | Listen stage, result, candidates, mix and collections, library, inspector, settings, menu bar extra |

## Demo states

Launch with `-MFDemoState <state>` to run on `FakeEngine` with scripted data (nothing
touches the real library, network or Keychain), e.g. for screenshots:

```sh
open -n "dist/Music Fetch.app" --args -MFDemoState mix
```

States: `idle`, `listening`, `hum`, `hum-answers`, `matching`, `result`, `candidates`,
`candidates-unsure`, `mix`, `collection`, `library`, `session`, `noMatch`,
`captureSilent`, `downloading`, `prePrompt`, `speechPrePrompt`, `micDenied`,
`melodyPrompt`, `lyrics`, `failed`, `providers`, `providers-custom`, `settings-general`,
`settings-general-custom`, `settings-services`, `settings-melody`,
`settings-melody-model`, `settings-melody-speech-off`, `settings-melody-downloading`,
`settings-storage`, `settings-about`.
Add `-NSRequiresAquaSystemAppearance YES` for the light appearance.

Debug builds also accept `-MFLegacyUI YES`, which takes every pre-macOS 26 branch (no
Liquid Glass, `SFSpeechRecognizer`), to check the macOS 15 fallbacks on a newer Mac.

## Sung words

While someone hums or sings, the microphone audio also goes to the on-device speech
recognizer and its text to the engine's lyric search (`SungWordsTranscriber`). macOS 26
needs no permission (SpeechAnalyzer); macOS 15 explains speech recognition on the stage
before the first hum, once. To see that again:

```sh
tccutil reset SpeechRecognition local.musicfetch.app
defaults delete local.musicfetch.app speechPermissionAsked
```

## Testing the melody library download

The app reads the published manifest. To try a local index, serve a folder with
`manifest.json` and the `.mfi` file on the loopback interface and point the app at it:

```sh
defaults write local.musicfetch.app MFMelodyIndexManifestURL http://127.0.0.1:8796/manifest.json
defaults delete local.musicfetch.app MFMelodyIndexManifestURL   # back to the release
```

Only `https://` URLs and `http://` on 127.0.0.1 or localhost are accepted.
