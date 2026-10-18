# Synthetic fixtures and the stub recognizer

Test music that contains no recorded audio, and a local stand-in for every
service the engine talks to when it identifies it. Tests (native, wasm, the
browser extension's end-to-end suites) use them instead of commercial songs and
the public recognizer.

## Fixtures

```sh
python3 tools/fixtures/synth_songs.py --out /tmp/mf-fixtures          # 12 songs, 2 mixes, 6 hums (44.1 kHz, ~320 MB, ~15 s)
python3 tools/fixtures/synth_songs.py --out /tmp/mf-fixtures --quick  # 4 short songs, 1 mix, 2 hums (16 kHz, tests)
```

Needs numpy. The songs are generated from note lists (a through-composed lead,
bass, a pad and a drum kit per song), so every truth is exact:

- `catalog.json`: the catalog songs (title, artist, ISRC, Deezer, Apple and
  Shazam ids, preview window, synced lyrics) and two unreleased "ID" tracks that
  no catalog knows.
- `mixes/<id>.wav`: DJ-style mixes with crossfades of 20–60 s, one track sped up
  vinyl-style (+3 % speed and pitch), one slowed down key-locked (−3 % tempo), one
  replay and one ID track. `<id>.truth.json` has every track's start, takeover
  (mid-crossfade) and end time, its position in the song, speed and pitch;
  `<id>.tracklist.txt` is a YouTube-style description; `mixes.json` lists the mixes
  in the `tools/eval/mixes` manifest format (plus the exact truth).
- `hums/hum-*.wav`: someone humming 12 s of a song's lead, transposed, off tempo,
  with vibrato, glides, intonation errors and breath noise; `hums.json` is the truth.
- `melody.jsonl`: melody-index builder input with the exact lead notes.

## Stub recognizer

```sh
mf dev stub-recognizer --refs /tmp/mf-fixtures [--port 47700] [--ready-file ready.json] [-v]
```

Listens on 127.0.0.1 only and prints one line when ready:

```json
{"url": "http://127.0.0.1:47700", "endpoints": {"routes": {"https://amp.shazam.com": "http://127.0.0.1:47700/shazam", …}, "isolate": "http://127.0.0.1:47700/blocked"}, "manifest": "http://127.0.0.1:47700/melody/manifest.json", "songs": 12}
```

| Route | Stands in for |
|---|---|
| `/shazam/discovery/v5/…` | Shazam: decodes the posted signature and matches its peaks against the songs with the engine's landmark matcher (speed and pitch changes up to ±8 %); answers in Shazam's shape (offset, time and frequency skew) or "no match" |
| `/deezer/…`, `/dzcdn/…` | Deezer API (track, ISRC, search, album, a playlist of every song) and its previews |
| `/itunes/…`, `/itunes-audio/…` | iTunes lookup and search, previews |
| `/lrclib/api/get`, `/lrclib/api/search` | LRCLIB with the synced lyrics |
| `/melody/manifest.json`, `/melody/index.mfi` | the melody index, built at start from `melody.jsonl` (also under `/github/…/manifest.json`) |
| `/media/<path>` | any fixture file, with Range support |
| `/__requests`, `/__reset`, `/__endpoints` | what the stub received (method, path, the headers the header policy sets), clear it, the endpoint map |
| `/blocked?url=…` | the sink for requests to every other host (403) |

`--throttle-first N --retry-after S` answers the first N recognizer requests
with HTTP 429, for back-off tests.

Point the engine at it with the endpoint map (`util::endpoints::TestEndpoints`):

- `mf listen|analyze|session|index … --stub <url>` (or `MF_STUB_URL`) installs
  the map, uses the stub's melody manifest and, against the stub only, a fast
  recognizer pacing (`--real-pacing` keeps the public one);
- `mf shazam <wav> --stub <url>` asks the stub;
- the browser extension's test builds embed the same JSON.

The map can only point at loopback addresses, and with `isolate` set every
other request goes to the stub's sink, so a test run never leaves the machine.
`cargo test -p mf-cli --test stub` runs the whole loop (it skips without numpy).
