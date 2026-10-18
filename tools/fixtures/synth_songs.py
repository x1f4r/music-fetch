#!/usr/bin/env python3
"""Synthetic test music: songs, DJ-style mixes and hums. No recorded audio.

Everything is generated from note lists (lead melody, bass, pad and drums), so
the fixtures are free to commit, share and publish, and every truth is exact:
the songs' notes, where each track sits in a mix, its speed and pitch there.
`mf dev stub-recognizer --refs <out>` serves the catalog as a recognizer,
catalog, lyrics and melody-index stand-in (see tools/fixtures/README.md).

Usage:
    python3 tools/fixtures/synth_songs.py --out <dir> [--quick] [--rate 44100]

Writes into <out>:
    catalog.json        catalog songs (ids, titles, previews, lyrics) and the
                        unreleased "ID" tracks, which no catalog knows
    melody.jsonl        melody-index builder input: the exact lead notes
    songs/*.wav         the songs (16-bit mono)
    mixes/<id>.wav      DJ-style mixes; <id>.truth.json and <id>.tracklist.txt
                        (a YouTube-style description with timestamps)
    mixes.json          the mixes as an eval manifest (tools/eval/mixes format,
                        plus exact times, offsets, speed and pitch)
    hums/*.wav          hummed excerpts of six songs; hums.json is the truth
    fixtures.json       generator version, parameters and SHA-256 of every file

The same arguments give the same files (numpy is the only dependency; random
choices use Python's seeded generator, noise a counter-based hash).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import struct
import sys
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

GENERATOR = "synth_songs.py 1"

TITLE_A = ["Copper", "Velvet", "Paper", "Glass", "Hollow", "Silver", "Neon", "Quiet", "Amber", "Cobalt",
           "Linen", "Static", "Marble", "Saffron"]
TITLE_B = ["Harbor", "Comets", "Signal", "Orchard", "Lanterns", "Tide", "Circuit", "Meadow", "Arcade",
           "Satellite", "Ember", "Atlas", "Ferris Wheel", "Monsoon"]
ARTISTS = ["Northbound Echo", "The Lumen Club", "Polar Motel", "Sine Parade", "Night Bus Choir",
           "Tape Garden", "Kite District", "Velour Engines"]
ALBUMS = ["Test Pressing", "Signal Path", "Room Tone", "Long Exposure"]
GENRES = ["Electronic", "Pop", "Dance", "Indie"]
WORDS = ["light", "over", "the", "harbor", "we", "run", "under", "copper", "skies", "slow", "and", "bright",
         "waves", "fold", "into", "static", "night", "keeps", "turning", "home", "again", "paper", "kites",
         "follow", "every", "signal", "down", "to", "glass", "rivers"]

# Scales as semitone steps from the root.
SCALES = {
    "major": [0, 2, 4, 5, 7, 9, 11],
    "minor": [0, 2, 3, 5, 7, 8, 10],
    "dorian": [0, 2, 3, 5, 7, 9, 10],
    "mixolydian": [0, 2, 4, 5, 7, 9, 10],
}
# One bar of lead rhythm (beats); every pattern fills four beats.
RHYTHMS = [
    [1, 1, 1, 1], [2, 1, 1], [1.5, 0.5, 2], [1, 0.5, 0.5, 2], [3, 1], [0.5, 0.5, 1, 2], [1, 1, 2],
    [2, 2], [1, 2, 1], [0.5, 1, 0.5, 2], [1.5, 1.5, 1], [4],
]
PROGRESSIONS = [[0, 4, 5, 3], [0, 5, 3, 4], [5, 3, 0, 4], [0, 3, 4, 4], [0, 2, 3, 4], [3, 4, 0, 5],
                [0, 3, 0, 4], [5, 4, 3, 4]]


def rng_for(seed: int, name: str) -> random.Random:
    return random.Random(f"mf-synth/{seed}/{name}")


def noise(n: int, key: int) -> np.ndarray:
    """Uniform noise in [-1, 1): splitmix64 of the sample index (identical everywhere)."""
    x = np.arange(n, dtype=np.uint64) + np.uint64(key & 0xFFFFFFFFFFFF) * np.uint64(1 << 16)
    x = x * np.uint64(0x9E3779B97F4A7C15)
    x ^= x >> np.uint64(30)
    x = x * np.uint64(0xBF58476D1CE4E5B9)
    x ^= x >> np.uint64(27)
    x = x * np.uint64(0x94D049BB133111EB)
    x ^= x >> np.uint64(31)
    return (x >> np.uint64(11)).astype(np.float64) / float(1 << 53) * 2.0 - 1.0


def midi_hz(m: float) -> float:
    return 440.0 * 2.0 ** ((m - 69.0) / 12.0)


@dataclass
class Note:
    beat: float  # onset in beats
    beats: float  # length in beats
    midi: float


@dataclass
class Song:
    id: str
    index: int
    title: str
    artist: str
    album: str
    year: int
    genre: str
    bpm: float
    seconds: float
    catalog: bool
    lead: list[Note] = field(default_factory=list)
    bass: list[Note] = field(default_factory=list)
    pad: list[Note] = field(default_factory=list)
    kicks: list[float] = field(default_factory=list)
    snares: list[float] = field(default_factory=list)
    hats: list[tuple[float, float]] = field(default_factory=list)
    harmonics: list[float] = field(default_factory=list)
    phases: list[float] = field(default_factory=list)
    # Drum kit: kick base and sweep (Hz), snare tone (Hz), hat decay (s).
    kit: tuple[float, float, float, float] = (50.0, 100.0, 190.0, 0.015)
    lyrics: list[tuple[int, str]] = field(default_factory=list)

    @property
    def beat_s(self) -> float:
        return 60.0 / self.bpm


def compose(seed: int, sid: str, index: int, seconds: float, catalog: bool) -> Song:
    """A through-composed song: the lead never repeats, so every window has one offset."""
    rng = rng_for(seed, sid)
    title = f"{TITLE_A[index % len(TITLE_A)]} {TITLE_B[(index * 5 + 3) % len(TITLE_B)]}"
    song = Song(
        id=sid,
        index=index,
        title=title,
        artist=ARTISTS[index % len(ARTISTS)],
        album=ALBUMS[index % len(ALBUMS)],
        year=2019 + index % 7,
        genre=GENRES[index % len(GENRES)],
        bpm=round(rng.uniform(92.0, 136.0), 2),
        seconds=seconds,
        catalog=catalog,
    )
    scale = SCALES[rng.choice(sorted(SCALES))]
    root = rng.randint(57, 66)
    song.harmonics = [1.0] + [rng.uniform(0.15, 0.7) / math.sqrt(k) for k in range(2, 7)]
    song.phases = [rng.uniform(0, 2 * math.pi) for _ in range(6)]
    song.kit = (rng.uniform(40, 70), rng.uniform(70, 150), rng.uniform(150, 260), rng.uniform(0.01, 0.03))
    swing = rng.uniform(0.0, 0.08)
    bars = int(math.ceil(seconds / (4 * song.beat_s))) + 1

    def degree_midi(d: int, octave_base: int) -> int:
        return octave_base + 12 * (d // 7) + scale[d % 7]

    degree = rng.randint(4, 9)
    progression = rng.choice(PROGRESSIONS)
    for bar in range(bars):
        if bar % 8 == 0:
            progression = rng.choice(PROGRESSIONS)
        chord = progression[bar % 4]
        t = bar * 4.0
        # Pad: the chord's triad, one bar, an octave below the lead.
        for k in (0, 2, 4):
            song.pad.append(Note(t, 4.0, degree_midi(chord + k, root - 12)))
        # Bass: the root on 1 and 3, root, fifth or octave on 2 and 4.
        for beat in range(4):
            if beat % 2 == 0:
                m = degree_midi(chord, root - 24)
            else:
                m = degree_midi(chord + rng.choice([0, 4, 7]), root - 24)
            song.bass.append(Note(t + beat, 0.9, m))
        # Drums.
        song.kicks += [t, t + 2.0] + ([t + 3.5] if rng.random() < 0.3 else [])
        song.snares += [t + 1.0, t + 3.0]
        for eighth in range(8):
            off = 0.5 * eighth + (swing if eighth % 2 else 0.0)
            song.hats.append((t + off, 0.7 if eighth % 2 == 0 else 0.45))
        # Lead: one rhythm per bar, a random walk over two octaves of the scale.
        if bar < 2 or bar >= bars - 1:
            continue  # drums-only intro and outro bar
        beat = t
        for length in rng.choice(RHYTHMS):
            if rng.random() < 0.1 and length <= 1.0:
                beat += length  # rest
                continue
            step = rng.choices([-4, -3, -2, -1, 1, 2, 3, 4, 0], [2, 3, 15, 28, 28, 15, 3, 2, 4])[0]
            degree += step
            if degree < 0 or degree > 14:
                degree -= 2 * step
            degree = min(max(degree, 0), 14)
            song.lead.append(Note(beat, length, degree_midi(degree, root + 12)))
            beat += length
    # Synced lyrics, one line every two bars (for the lyrics stub).
    line_rng = rng_for(seed, sid + "/lyrics")
    for bar in range(2, bars - 2, 2):
        at = int(round(bar * 4 * song.beat_s * 1000))
        if at >= seconds * 1000 - 4000:
            break
        song.lyrics.append((at, " ".join(line_rng.choice(WORDS) for _ in range(line_rng.randint(3, 6)))))
    return song


def tone(n: int, rate: int, freqs, harmonics, phases, env: np.ndarray) -> np.ndarray:
    """Harmonic tone; `freqs` is a constant (Hz) or a per-sample array."""
    t = np.arange(n) / rate
    if np.isscalar(freqs):
        phase = 2 * np.pi * float(freqs) * t
        nyquist_ok = lambda k: k * float(freqs) < rate * 0.45  # noqa: E731
    else:
        phase = 2 * np.pi * np.cumsum(freqs) / rate
        top = float(np.max(freqs)) if len(freqs) else 0.0
        nyquist_ok = lambda k: k * top < rate * 0.45  # noqa: E731
    out = np.zeros(n)
    for k, (a, p) in enumerate(zip(harmonics, phases), start=1):
        if a > 0 and nyquist_ok(k):
            out += a * np.sin(k * phase + p)
    return out * env


def adsr(n: int, rate: int, attack: float, decay: float, sustain: float, release: float) -> np.ndarray:
    t = np.arange(n) / rate
    dur = n / rate
    env = np.where(t < attack, t / max(attack, 1e-6), sustain + (1 - sustain) * np.exp(-(t - attack) / max(decay, 1e-6)))
    tail = np.clip((dur - t) / max(release, 1e-6), 0.0, 1.0)
    return env * tail


def render(song: Song, start_s: float, length_s: float, rate: int, tempo: float = 1.0, pitch: float = 1.0) -> np.ndarray:
    """`length_s` seconds of output from song time `start_s`, played `tempo` times faster
    (more notes per second) with every frequency multiplied by `pitch`. Vinyl-style
    speed changes (tempo == pitch) resample the audio, as a turntable does; key-locked
    ones (pitch == 1) play the notes faster at their own pitch."""
    if tempo != 1.0 and abs(tempo - pitch) < 1e-9:
        src = render(song, start_s, length_s * tempo, rate)
        t = np.arange(int(round(length_s * rate))) * tempo
        return np.interp(t, np.arange(len(src)), src)
    n = int(round(length_s * rate))
    out = np.zeros(n + rate)  # room for release tails
    beat_s = song.beat_s

    def place(t_song: float, dur_song: float) -> tuple[int, int] | None:
        t_out = (t_song - start_s) / tempo
        dur = dur_song / tempo
        if t_out + dur < 0 or t_out >= length_s:
            return None
        return int(round(t_out * rate)), max(1, int(round(dur * rate)))

    def add(at: int, sig: np.ndarray) -> None:
        a, b = at, at + len(sig)
        lo, hi = max(a, 0), min(b, len(out))
        if lo < hi:
            out[lo:hi] += sig[lo - a:hi - a]

    for note in song.lead:
        pos = place(note.beat * beat_s, note.beats * beat_s - 0.02)
        if pos:
            at, m = pos
            env = adsr(m, rate, 0.025, 0.08, 0.75, 0.05)
            add(at, 0.30 * tone(m, rate, midi_hz(note.midi) * pitch, song.harmonics, song.phases, env))
    for note in song.bass:
        pos = place(note.beat * beat_s, note.beats * beat_s)
        if pos:
            at, m = pos
            env = adsr(m, rate, 0.015, 0.15, 0.6, 0.04)
            add(at, 0.26 * tone(m, rate, midi_hz(note.midi) * pitch, [1.0, 0.55, 0.3], [0.0, 0.5, 1.0], env))
    for note in song.pad:
        pos = place(note.beat * beat_s, note.beats * beat_s)
        if pos:
            at, m = pos
            env = adsr(m, rate, 0.15, 0.5, 0.8, 0.2)
            add(at, 0.06 * tone(m, rate, midi_hz(note.midi) * pitch, [1.0, 0.3], [0.3, 1.1], env))
    key = song.index * 7919 + 17
    for beat in song.kicks:
        pos = place(beat * beat_s, 0.25)
        if pos:
            at, m = pos
            t = np.arange(m) / rate
            freq = (song.kit[0] + song.kit[1] * np.exp(-t / 0.03)) * pitch
            add(at, 0.5 * np.sin(2 * np.pi * np.cumsum(freq) / rate) * np.exp(-t / 0.12))
    for i, beat in enumerate(song.snares):
        pos = place(beat * beat_s, 0.2)
        if pos:
            at, m = pos
            t = np.arange(m) / rate
            body = np.sin(2 * np.pi * song.kit[2] * pitch * t) * np.exp(-t / 0.04)
            add(at, 0.22 * noise(m, key + 3 * i) * np.exp(-t / 0.05) + 0.18 * body)
    for i, (beat, gain) in enumerate(song.hats):
        pos = place(beat * beat_s, 0.06)
        if pos:
            at, m = pos
            t = np.arange(m) / rate
            w = noise(m + 1, key + 100_003 + 5 * i)
            add(at, 0.05 * gain * (w[1:] - w[:-1]) * np.exp(-t / song.kit[3]))
    return out[:n]


def fade(sig: np.ndarray, rate: int, fade_in: float, fade_out: float) -> np.ndarray:
    sig = sig.copy()
    a, b = int(fade_in * rate), int(fade_out * rate)
    if a:
        sig[:a] *= np.linspace(0, 1, a)
    if b:
        sig[-b:] *= np.linspace(1, 0, b)
    return sig


def write_wav(path: Path, sig: np.ndarray, rate: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pcm = np.clip(np.round(sig * 32767.0), -32768, 32767).astype("<i2").tobytes()
    header = b"RIFF" + struct.pack("<I", 36 + len(pcm)) + b"WAVEfmt " + struct.pack(
        "<IHHIIHH", 16, 1, 1, rate, rate * 2, 2, 16) + b"data" + struct.pack("<I", len(pcm))
    path.write_bytes(header + pcm)


def normalize(sig: np.ndarray, peak: float = 0.89) -> np.ndarray:
    top = float(np.max(np.abs(sig))) if len(sig) else 0.0
    return sig * (peak / top) if top > 0 else sig


def clock(ms: float) -> str:
    s = int(round(ms / 1000.0))
    return f"{s // 3600}:{s % 3600 // 60:02d}:{s % 60:02d}" if s >= 3600 else f"{s // 60}:{s % 60:02d}"


@dataclass
class Entry:
    song: str
    start_s: float  # song time at the entry's first sample
    length_s: float  # output seconds
    crossfade_s: float = 0.0  # overlap with the previous entry
    tempo: float = 1.0
    pitch: float = 1.0


def build_mix(mix_id: str, title: str, entries: list[Entry], songs: dict[str, Song], rate: int, out: Path):
    for i, e in enumerate(entries):
        nxt = entries[i + 1].crossfade_s if i + 1 < len(entries) else 0.0
        if e.crossfade_s + nxt > e.length_s:
            raise ValueError(f"{mix_id}: entry {i} ({e.song}) is shorter than its two crossfades")
        if e.start_s + e.length_s * e.tempo > songs[e.song].seconds - 2.0:
            raise ValueError(f"{mix_id}: entry {i} ({e.song}) runs past the end of the song")
    starts = []
    t = 0.0
    for i, e in enumerate(entries):
        if i:
            t -= e.crossfade_s
        starts.append(t)
        t += e.length_s
    total = t
    mix = np.zeros(int(round(total * rate)) + rate)
    truth = []
    for i, (e, at) in enumerate(zip(entries, starts)):
        song = songs[e.song]
        sig = render(song, e.start_s, e.length_s, rate, e.tempo, e.pitch)
        a = int(round(e.crossfade_s * rate))
        nxt = entries[i + 1].crossfade_s if i + 1 < len(entries) else 0.0
        b = int(round(nxt * rate))
        env = np.ones(len(sig))
        if a:
            env[:a] = np.sin(np.linspace(0, np.pi / 2, a))
        elif i == 0:
            env[: rate // 2] = np.linspace(0, 1, rate // 2)
        if b:
            env[-b:] = np.cos(np.linspace(0, np.pi / 2, b))
        else:
            env[-rate:] *= np.linspace(1, 0, rate)
        k = int(round(at * rate))
        mix[k:k + len(sig)] += sig * env
        # The tracklist time is where the new track takes over (mid-crossfade).
        main = at + e.crossfade_s / 2
        truth.append({
            "song": song.id,
            "title": song.title,
            "artist": song.artist,
            "id": not song.catalog,
            "start_ms": int(round(at * 1000)),
            "main_ms": int(round(main * 1000)),
            "end_ms": int(round((at + e.length_s) * 1000)),
            "song_start_ms": int(round(e.start_s * 1000)),
            "speed": round(e.tempo - 1.0, 4),
            "pitch": round(e.pitch - 1.0, 4),
            "replay": any(x.song == e.song for x in entries[:i]),
        })
    mix = normalize(mix[: int(round(total * rate))])
    write_wav(out / "mixes" / f"{mix_id}.wav", mix, rate)
    lines = [f"{title}", "", "Tracklist:"]
    for tr in truth:
        name = "ID - ID" if tr["id"] else f"{tr['artist']} - {tr['title']}"
        lines.append(f"{clock(tr['main_ms'])} {name}")
    (out / "mixes" / f"{mix_id}.tracklist.txt").write_text("\n".join(lines) + "\n")
    doc = {"id": mix_id, "title": title, "file": f"mixes/{mix_id}.wav", "duration_ms": int(round(total * 1000)),
           "tracks": truth}
    (out / "mixes" / f"{mix_id}.truth.json").write_text(json.dumps(doc, indent=1) + "\n")
    return doc


def hum(song: Song, rng: random.Random, start_s: float, length_s: float, rate: int, key: int):
    """A person humming the lead line: transposed, off-tempo, with vibrato, glides,
    intonation errors, breath noise and a hum-like timbre."""
    transpose = rng.choice([-7, -5, -4, -3, 2, 3])
    tempo = rng.uniform(0.9, 1.08)
    beat_s = song.beat_s
    notes = [n for n in song.lead if start_s <= n.beat * beat_s < start_s + length_s]
    out_len = length_s / tempo + 0.6
    n = int(round(out_len * rate))
    freq = np.zeros(n)
    amp = np.zeros(n)
    t = np.arange(n) / rate
    prev = None
    for note in notes:
        a = int(round((note.beat * beat_s - start_s) / tempo * rate))
        m = int(round((note.beats * beat_s - 0.05) / tempo * rate))
        b = min(n, a + m)
        if a >= n or b <= a:
            continue
        target = midi_hz(note.midi + transpose + rng.gauss(0.0, 0.25))
        seg = np.full(b - a, target)
        glide = min(int(0.04 * rate), b - a)
        if prev is not None and glide > 0:
            seg[:glide] = np.geomspace(prev, target, glide)
        local = t[a:b] - t[a]
        vib = 1.0 + (2 ** (0.3 / 12) - 1) * np.sin(2 * np.pi * 5.5 * local) * np.clip((local - 0.15) / 0.2, 0, 1)
        freq[a:b] = seg * vib
        amp[a:b] = adsr(b - a, rate, 0.04, 0.2, 0.8, 0.06)
        prev = target
    voiced = amp > 0
    freq[~voiced] = 150.0
    sig = tone(n, rate, freq, [1.0, 0.28, 0.1], [0.0, 0.4, 0.9], amp * 0.5)
    sig += 0.012 * noise(n, key)  # room and breath
    return normalize(sig, 0.7), transpose, tempo


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--rate", type=int, default=None, help="sample rate (44100; --quick: 16000)")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--quick", action="store_true", help="4 short songs, one short mix, two hums (tests)")
    args = ap.parse_args()
    rate = args.rate or (16000 if args.quick else 44100)
    out: Path = args.out
    out.mkdir(parents=True, exist_ok=True)

    n_songs, seconds, n_ids, n_hums = (4, 75.0, 1, 2) if args.quick else (12, 180.0, 2, 6)
    songs: dict[str, Song] = {}
    for i in range(n_songs):
        sid = f"song-{i + 1:02d}"
        songs[sid] = compose(args.seed, sid, i, seconds, True)
    for i in range(n_ids):
        sid = f"id-{i + 1:02d}"
        songs[sid] = compose(args.seed, sid, 40 + i, seconds, False)

    catalog = {"version": 1, "generator": GENERATOR, "sample_rate": rate, "songs": [], "unreleased": []}
    for song in songs.values():
        sig = fade(render(song, 0.0, song.seconds, rate), rate, 0.05, 1.0)
        path = out / "songs" / f"{song.id}.wav"
        write_wav(path, normalize(sig), rate)
        entry = {
            "id": song.id,
            "title": song.title,
            "artist": song.artist,
            "album": song.album,
            "year": song.year,
            "genre": song.genre,
            "duration_ms": int(round(song.seconds * 1000)),
            "bpm": song.bpm,
            "file": f"songs/{song.id}.wav",
        }
        if song.catalog:
            n = song.index + 1
            entry.update({
                "isrc": f"ZZMF0{song.year % 100:02d}{n:05d}",
                "deezer_id": 900_000_000 + n,
                "apple_id": 1_900_000_000 + n,
                "shazam_key": str(990_000_000 + n),
                "preview_start_ms": int(round(min(30.0, song.seconds / 3) * 1000)),
                "preview_ms": int(min(30_000, song.seconds * 1000 // 2)),
                "lyrics": [[ms, text] for ms, text in song.lyrics],
            })
            catalog["songs"].append(entry)
        else:
            catalog["unreleased"].append(entry)
    (out / "catalog.json").write_text(json.dumps(catalog, indent=1) + "\n")

    with open(out / "melody.jsonl", "w") as f:
        for entry in catalog["songs"]:
            song = songs[entry["id"]]
            ms = song.beat_s * 1000.0
            notes = [[round(n.midi, 2), round(n.beat * ms, 1), round(n.beats * ms - 20.0, 1)] for n in song.lead]
            f.write(json.dumps({
                "title": song.title, "artist": song.artist, "isrc": entry["isrc"], "deezer_id": entry["deezer_id"],
                "year": song.year, "popularity": 0.5,
                "phrases": [{"source": "midi", "start_ms": 0, "notes": notes}],
            }) + "\n")

    if args.quick:
        mixes = [("synth-mix-q", "Synthetic Mix Q (test)", [
            Entry("song-01", 5.0, 50.0),
            Entry("song-02", 10.0, 50.0, 20.0, 1.03, 1.03),
            Entry("id-01", 10.0, 40.0, 15.0),
            Entry("song-03", 8.0, 50.0, 20.0, 0.97, 1.0),
            Entry("song-01", 50.0, 22.0, 12.0),
        ])]
    else:
        mixes = [
            ("synth-mix-a", "Synthetic Mix A (test fixture)", [
                Entry("song-01", 30.0, 110.0),
                Entry("song-02", 20.0, 120.0, 30.0),
                Entry("song-03", 40.0, 100.0, 45.0, 1.03, 1.03),
                Entry("id-01", 20.0, 90.0, 25.0),
                Entry("song-04", 15.0, 110.0, 60.0, 0.97, 1.0),
                Entry("song-02", 100.0, 70.0, 20.0),
                Entry("song-05", 30.0, 120.0, 40.0),
                Entry("song-06", 50.0, 110.0, 30.0),
            ]),
            ("synth-mix-b", "Synthetic Mix B (test fixture)", [
                Entry("song-07", 10.0, 120.0),
                Entry("song-08", 30.0, 110.0, 35.0),
                Entry("song-09", 20.0, 100.0, 50.0),
                Entry("song-10", 40.0, 115.0, 25.0, 0.97, 0.97),
                Entry("id-02", 30.0, 80.0, 30.0),
                Entry("song-11", 20.0, 110.0, 40.0, 1.03, 1.0),
                Entry("song-09", 115.0, 60.0, 20.0),
                Entry("song-12", 25.0, 120.0, 35.0),
            ]),
        ]
    manifest = {"cases": []}
    for mix_id, title, entries in mixes:
        doc = build_mix(mix_id, title, entries, songs, rate, out)
        manifest["cases"].append({
            "id": mix_id,
            "kind": "mix",
            "file": doc["file"],
            "note": "synthetic; crossfades, one replay, one ID, speed and pitch changes",
            "tracks": [{"t": clock(t["main_ms"]), "artist": t["artist"], "title": t["title"], **({"id": True} if t["id"] else {})}
                       for t in doc["tracks"]],
            "truth": doc["tracks"],
        })
    (out / "mixes.json").write_text(json.dumps(manifest, indent=1) + "\n")

    hums = []
    for i, entry in enumerate(catalog["songs"][:n_hums]):
        song = songs[entry["id"]]
        rng = rng_for(args.seed, f"hum/{song.id}")
        length = 12.0
        onsets = [n.beat * song.beat_s for n in song.lead if 10.0 <= n.beat * song.beat_s <= song.seconds - length - 5]
        start = rng.choice(onsets[: max(1, len(onsets) // 2)])
        sig, transpose, tempo = hum(song, rng, start, length, rate, 500_000 + i)
        name = f"hum-{i + 1:02d}"
        write_wav(out / "hums" / f"{name}.wav", sig, rate)
        hums.append({"file": f"hums/{name}.wav", "song": song.id, "title": song.title, "artist": song.artist,
                     "excerpt_start_ms": int(round(start * 1000)), "excerpt_ms": int(length * 1000),
                     "transpose": transpose, "tempo": round(tempo, 4)})
    (out / "hums" / "hums.json").write_text(json.dumps(hums, indent=1) + "\n")

    files = sorted(p for p in out.rglob("*") if p.is_file() and p.name != "fixtures.json")
    info = {
        "generator": GENERATOR,
        "args": {"rate": rate, "seed": args.seed, "quick": args.quick},
        "numpy": np.__version__,
        "files": {str(p.relative_to(out)): sha256(p) for p in files},
    }
    (out / "fixtures.json").write_text(json.dumps(info, indent=1) + "\n")
    print(f"wrote {len(files)} files to {out} ({len(catalog['songs'])} songs, {len(mixes)} mixes, {len(hums)} hums)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
