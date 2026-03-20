# Music Fetch

Music Fetch is a local-first music recognition tool for noisy web video, playlists, and local media files. It exposes the same engine through:

- `music-fetch analyze` for direct CLI analysis
- `music-fetch serve` for a local HTTP API
- `music-fetch tui` for an interactive Textual terminal UI
- `Music Fetch.app` for a native SwiftUI macOS interface

It is built for macOS first and assumes `ffmpeg` and `yt-dlp` are available. Input ingestion is platform-agnostic and follows whatever sites `yt-dlp` supports, which typically includes YouTube, Instagram, TikTok, Vimeo, and many other public video hosts. Optional helpers improve accuracy:

- `vibra` for free-first unofficial Shazam matching
- `audio-separator` for vocal/instrumental source separation
- `fpcalc` from Chromaprint for local catalog matching
- AudD and ACRCloud credentials for official hosted recognition

## Install

```bash
uv sync
music-fetch doctor
```

For optional helpers:

```bash
./scripts/bootstrap_macos.sh
```

Build the native macOS app bundle:

```bash
./scripts/build_macos_app.sh
open "dist/Music Fetch.app"
```

Install or refresh the app in `/Applications`:

```bash
./scripts/build_macos_app.sh --install
open "/Applications/Music Fetch.app"
```

## Usage

Analyze a URL or local file:

```bash
music-fetch analyze "https://youtube.com/shorts/-OvmHgojXLw"
music-fetch analyze ~/Downloads/example.mp4 --json
```

Run the local API:

```bash
music-fetch serve --host 127.0.0.1 --port 7766
```

Launch the TUI:

```bash
music-fetch tui
```

Launch the macOS app:

```bash
open "dist/Music Fetch.app"
```

Or, after installation:

```bash
open "/Applications/Music Fetch.app"
```

Import a local catalog:

```bash
music-fetch catalog import ~/Music
```

## Provider order

The default provider chain is:

1. `local_catalog` if configured and `fpcalc` is available
2. `vibra` if installed
3. `audd` if token configured
4. `acrcloud` if host/key/secret configured

The engine scans overlapping windows from the original mix and, when available, the instrumental stem. Short single-track items use aggressive early-stop logic, while long videos and mixes switch to a clustered long-mix mode with larger request budgets, excerpt caching, and repeated-hit fusion into timeline segments.

## Notes

- The native app bundle is built from the SwiftUI sources in `macos/` and shells out to the local `music-fetch` backend in `.venv/bin/music-fetch`.
- Local catalog fingerprinting needs clips long enough for Chromaprint to produce a fingerprint; very short clips such as one-second tones are expected to fail import.
- `yt-dlp` is used for YouTube, Shorts, playlists, Instagram, TikTok, Vimeo, and other supported streaming sites. If a provider changes extractor behavior, update `yt-dlp` first.
- `deno` is installed alongside `yt-dlp` so the JS challenge helpers used by some sites, especially YouTube, are available.
