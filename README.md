# Music Fetch

Find out what's playing — or what's stuck in your head — on macOS and Android.

- **Listen.** Identify a song playing nearby (microphone) or on the device itself (system
  audio on the Mac, app audio on Android).
- **Hum, whistle or sing.** No recording needed: Music Fetch matches the melody and any
  words you sing, and shows its best guesses with an honest match label and a "your hum
  vs. the song" overlay.
- **Paste any link.** YouTube, Spotify, Apple Music, Deezer, Tidal, Amazon Music,
  SoundCloud, TikTok, Instagram, Mixcloud, Bandcamp and most other pages with audio or
  video. Streaming links resolve instantly; videos are analysed.
- **Mixes and playlists.** DJ sets, radio shows and compilations become a tracklist with
  timestamps, linked to every service. Playlists are identified entry by entry.
- **Open anywhere.** Every result links to Spotify, Apple Music, YouTube Music, YouTube,
  Deezer, Tidal, Amazon Music and SoundCloud; on Android your chosen app opens directly.

Everything runs on your device. There are no accounts, no analytics and no tracking.

## Install

Downloads are on the [Releases](https://github.com/x1f4r/music-fetch/releases) page.

### macOS (macOS 15 Sequoia or later, Apple silicon and Intel)

1. Download `MusicFetch-<version>-macOS-universal.zip`, open it and move **Music Fetch** to
   Applications.
2. The app is not notarized. The first time you open it, macOS says it cannot verify
   the developer. Open **System Settings → Privacy & Security**, scroll down and click
   **Open Anyway**, then confirm.
3. Music Fetch asks for the microphone when you first listen, and for **System Audio
   Recording** when you first use *This Mac*.

### Android (Android 10 or later)

1. Download `MusicFetch-<version>-android.apk` on your phone and open it.
2. Allow installing apps from your browser or file manager when Android asks.
3. Optional: add the **Identify song** and **Identify phone audio** tiles to Quick
   Settings (swipe down twice → edit).

## Humming needs the melody library

Recognising a hummed tune uses a melody library of popular songs (about 50 MB). The app
offers to download it the first time you hum, and you can manage it in Settings.

## Optional services

Recognition works without any account. If you have keys for
[AudD](https://audd.io) or [ACRCloud](https://www.acrcloud.com) (including an ACRCloud
project with humming recognition), add them in Settings → Services to widen coverage.
Keys are stored in the macOS Keychain / Android Keystore and never leave your device
except in requests to that service.

## How it works

One engine written in Rust powers both apps through UniFFI:

- recordings are matched with Shazam-compatible audio fingerprints;
- hummed melodies are pitch-tracked on device and matched against the melody library;
  sung words are transcribed on device and searched as lyrics;
- links are resolved from page metadata where possible and otherwise downloaded with
  [yt-dlp](https://github.com/yt-dlp/yt-dlp), which ships inside the apps;
- mixes are segmented locally, and tracklists in descriptions or comments are verified
  against the audio, so only a few recognition requests are needed per hour of audio.

Architecture and design notes are in [`docs/`](docs).

## Build from source

Requirements: macOS with Xcode 26, [rustup](https://rustup.rs), XcodeGen; for Android
also JDK 21, the Android SDK with NDK r29, `cargo-ndk` and Python 3.13.

```sh
scripts/build-macos-app.sh        # dist/Music Fetch.app and a zip
scripts/build-android-app.sh      # dist/MusicFetch-<version>-android.apk
cd core && cargo test --workspace # engine tests
```

## Privacy

- Audio is processed on your device. Short audio fingerprints (not audio) are sent to the
  recognition service; links you paste are fetched from their sites; song titles are
  looked up in public catalogues (Deezer, iTunes, LRCLIB) to add artwork, links and
  lyrics.
- Requests carry a neutral `MusicFetch/<version>` user agent and no identifiers.
- Your history stays in a local database. Delete sessions from the Library at any time.

## License

MIT. Third-party components and their licenses are listed in
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
