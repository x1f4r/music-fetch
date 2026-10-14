# Music Fetch for macOS

SwiftUI app for macOS 26 on top of the Rust engine (`MusicFetchCore`). Architecture:
[docs/APPS.md](../docs/APPS.md); screens and copy: [docs/design/DESIGN.md](../docs/design/DESIGN.md).

## Build

```sh
scripts/build-macos-app.sh            # core, media runtime, Release build, signing, zip → dist/
scripts/privacy-check.sh dist         # before sharing a build
```

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

States: `idle`, `listening`, `hum`, `matching`, `result`, `candidates`, `mix`,
`collection`, `library`, `session`, `noMatch`, `captureSilent`, `downloading`,
`prePrompt`, `micDenied`, `melodyPrompt`, `lyrics`, `failed`, `settings-general`,
`settings-services`, `settings-melody`, `settings-melody-downloading`,
`settings-storage`, `settings-about`. Add `-NSRequiresAquaSystemAppearance YES` for the
light appearance.

## Testing the melody library download

The app reads the published manifest. To try a local index, serve a folder with
`manifest.json` and the `.mfi` file on the loopback interface and point the app at it:

```sh
defaults write local.musicfetch.app MFMelodyIndexManifestURL http://127.0.0.1:8796/manifest.json
defaults delete local.musicfetch.app MFMelodyIndexManifestURL   # back to the release
```

Only `https://` URLs and `http://` on 127.0.0.1 or localhost are accepted.
