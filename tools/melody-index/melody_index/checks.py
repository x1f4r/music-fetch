"""Spot checks: RMVPE on a synthetic voice, printing a song's notes, and the
MIDI-vs-preview agreement report for well-known songs."""

from __future__ import annotations

import json
import random
from typing import Any

import numpy as np

from . import db as dbm
from .config import Paths
from .normalize import match_keys
from .notes import NoteParams, f0_to_notes, midi_to_hz
from .validate import _midi_tracks, agreement

NOTE_NAMES = ["C", "C#", "D", "D#", "E", "F", "F#", "G", "G#", "A", "A#", "B"]

FAMOUS = [
    ("The Beatles", "Let It Be"),
    ("The Beatles", "Hey Jude"),
    ("The Beatles", "Yesterday"),
    ("Queen", "Bohemian Rhapsody"),
    ("Michael Jackson", "Billie Jean"),
    ("Michael Jackson", "Beat It"),
    ("ABBA", "Dancing Queen"),
    ("Whitney Houston", "I Will Always Love You"),
    ("Toto", "Africa"),
    ("a-ha", "Take On Me"),
    ("Bon Jovi", "Livin' On A Prayer"),
    ("Eagles", "Hotel California"),
    ("Celine Dion", "My Heart Will Go On"),
    ("Cyndi Lauper", "Girls Just Want To Have Fun"),
    ("Survivor", "Eye Of The Tiger"),
    ("Lionel Richie", "Hello"),
    ("Backstreet Boys", "I Want It That Way"),
    ("Britney Spears", "...Baby One More Time"),
    ("Ricky Martin", "Livin' La Vida Loca"),
    ("Eric Clapton", "Tears In Heaven"),
    ("The Police", "Every Breath You Take"),
    ("Wham!", "Careless Whisper"),
    ("Simon & Garfunkel", "The Sound Of Silence"),
    ("Madonna", "Like A Virgin"),
    ("Elton John", "Your Song"),
    ("Stevie Wonder", "I Just Called To Say I Love You"),
    ("Bryan Adams", "Summer Of '69"),
    ("Madonna", "Like A Prayer"),
    ("Prince", "Purple Rain"),
    ("U2", "With Or Without You"),
    ("Bill Withers", "Lean On Me"),
    ("Ben E. King", "Stand By Me"),
    ("John Lennon", "Imagine"),
    ("Coldplay", "Yellow"),
    ("Oasis", "Wonderwall"),
    ("Nirvana", "Smells Like Teen Spirit"),
    ("Spice Girls", "Wannabe"),
    ("Aqua", "Barbie Girl"),
    ("Gloria Gaynor", "I Will Survive"),
    ("Bee Gees", "Stayin' Alive"),
    ("Journey", "Don't Stop Believin'"),
    ("Kansas", "Dust In The Wind"),
]


def note_name(p: float) -> str:
    n = int(round(p))
    cents = int(round((p - n) * 100))
    return (
        f"{NOTE_NAMES[n % 12]}{n // 12 - 1}{cents:+d}"
        if abs(cents) >= 15
        else f"{NOTE_NAMES[n % 12]}{n // 12 - 1}"
    )


def voice_like(notes: list[tuple[float, float]], sr: int = 16000, seed: int = 3) -> np.ndarray:
    """Harmonic-rich tone with vibrato and a vowel-ish spectral tilt; ``notes`` = (midi, seconds)."""
    rng = np.random.default_rng(seed)
    f = np.concatenate([np.full(int(d * sr), midi_to_hz(p)) for p, d in notes])
    t = np.arange(len(f)) / sr
    f = f * 2 ** (0.25 * np.sin(2 * np.pi * 5.5 * t) / 12)
    phase = 2 * np.pi * np.cumsum(f) / sr
    wave = sum((0.8**h) * np.sin(h * phase) for h in range(1, 12))
    env = np.ones_like(t)
    edges = np.cumsum([int(d * sr) for _, d in notes])
    for e in edges[:-1]:
        env[max(0, e - 640) : e] = 0.0  # 40 ms consonant-like break between notes
    audio = wave * env
    audio += 0.01 * rng.standard_normal(len(audio))
    return (0.3 * audio / np.max(np.abs(audio))).astype(np.float32)


def rmvpe_synthetic(paths: Paths) -> dict[str, Any]:
    """Known melody in, notes out: checks weights, mel front end and decoding together."""
    from .rmvpe import Rmvpe

    model = Rmvpe(paths.rmvpe_weights)
    melody = [
        (60, 0.5),
        (60, 0.5),
        (67, 0.5),
        (67, 0.5),
        (69, 0.5),
        (69, 0.5),
        (67, 1.0),
        (65, 0.5),
        (65, 0.5),
        (64, 0.5),
        (64, 0.5),
        (62, 0.5),
        (62, 0.5),
        (60, 1.0),
    ]
    audio = voice_like(melody)
    ((f0, conf),) = model.f0([audio], threshold=0.01)
    notes = f0_to_notes(f0, conf, NoteParams())
    expected = [p for p, _ in melody]
    got = [n.pitch for n in notes]
    voiced = f0[conf > 0.04]
    # Frame-level check: every voiced frame within 50 cents of the sung note.
    target = np.concatenate([np.full(int(d * 100), p) for p, d in melody])
    k = min(len(target), len(f0))
    v = (conf[:k] > 0.04) & (f0[:k] > 0)
    err = np.abs(69 + 12 * np.log2(np.maximum(f0[:k][v], 1e-6) / 440) - target[v])
    return {
        "expected_notes": [note_name(p) for p in expected],
        "detected_notes": [note_name(p) for p in got],
        "exact_sequence": [round(p) for p in got] == expected,
        "voiced_frames": int(voiced.size),
        "frame_accuracy_50c": round(float(np.mean(err <= 0.5)), 4) if err.size else 0.0,
        "median_abs_cents": round(float(np.median(err) * 100), 1) if err.size else None,
    }


def find_song(conn, artist: str, title: str):
    keys = match_keys(artist, title)
    for r in conn.execute(
        "SELECT id, key, title, artist, aliases FROM songs WHERE in_universe=1 AND title LIKE ?",
        (f"%{title.split()[-1]}%",),
    ):
        pairs = [(r["artist"], r["title"])] + [
            tuple(a) for a in json.loads(r["aliases"] or "[]") if len(a) == 2
        ]
        if any(match_keys(a, t) & keys for a, t in pairs):
            return r
    return None


def show_song(paths: Paths, artist: str, title: str, max_phrases: int = 4) -> str:
    conn = dbm.connect(paths.db)
    s = find_song(conn, artist, title)
    if s is None:
        return f"not in universe: {artist} - {title}"
    lines = [f"{s['artist']} - {s['title']} (song {s['id']})"]
    d = conn.execute("SELECT * FROM deezer WHERE song_id=?", (s["id"],)).fetchone()
    if d:
        lines.append(
            f"  deezer {d['status']} id={d['deezer_id']} isrc={d['isrc']} '{d['dz_artist']} - {d['dz_title']}'"
            f" match={d['match_score']} preview={d['has_preview']}"
        )
    p = conn.execute(
        "SELECT status, phrases, voiced_ratio FROM preview_melody WHERE song_id=?", (s["id"],)
    ).fetchone()
    if p:
        phrases = json.loads(p["phrases"] or "[]")
        lines.append(
            f"  preview: {p['status']}, {len(phrases)} phrases, voiced {p['voiced_ratio']}"
        )
        for ph in phrases[:max_phrases]:
            lines.append(
                f"    @{ph['start_ms'] / 1000:5.1f}s "
                + " ".join(note_name(n[0]) for n in ph["notes"][:24])
            )
    for m in conn.execute(
        "SELECT md5, phrases, tracks FROM midi_melody WHERE song_id=? AND status='ok'", (s["id"],)
    ):
        phrases = json.loads(m["phrases"])
        tracks = json.loads(m["tracks"])
        lines.append(
            f"  midi {m['md5'][:8]}: {len(phrases)} phrases, track '{tracks[0]['name']}'"
            f" program {tracks[0]['program']} score {tracks[0]['score']}"
        )
        for ph in [x for x in phrases if x.get("track", 0) == 0][:max_phrases]:
            lines.append(
                f"    @{ph['start_ms'] / 1000:5.1f}s "
                + " ".join(note_name(n[0]) for n in ph["notes"][:24])
            )
    return "\n".join(lines)


def famous(paths: Paths, want: int = 20, null_per_song: int = 5, seed: int = 11) -> dict[str, Any]:
    """MIDI vs preview DTW agreement for well-known songs, with a wrong-song baseline."""
    conn = dbm.connect(paths.db)
    rng = random.Random(seed)
    others = conn.execute(
        "SELECT song_id, phrases FROM midi_melody WHERE status='ok' ORDER BY RANDOM() LIMIT 400"
    ).fetchall()
    rows = []
    for artist, title in FAMOUS:
        if len(rows) >= want:
            break
        s = find_song(conn, artist, title)
        if s is None:
            continue
        p = conn.execute(
            "SELECT phrases FROM preview_melody WHERE song_id=? AND status='ok'", (s["id"],)
        ).fetchone()
        mids = conn.execute(
            "SELECT md5, phrases FROM midi_melody WHERE song_id=? AND status='ok'", (s["id"],)
        ).fetchall()
        if not p or not mids:
            continue
        preview = json.loads(p["phrases"])
        best: dict[str, Any] | None = None
        for m in mids:
            for track, phrases in _midi_tracks(json.loads(m["phrases"])).items():
                res = agreement(preview, phrases)
                if res["agreement"] is not None and (
                    best is None or res["agreement"] > best["agreement"]
                ):
                    best = {**res, "md5": m["md5"], "track": track}
        if best is None:
            continue
        null = []
        for o in rng.sample(others, min(null_per_song, len(others))):
            if o["song_id"] == s["id"]:
                continue
            tr = _midi_tracks(json.loads(o["phrases"]))
            res = agreement(preview, tr.get(0) or next(iter(tr.values())))
            if res["agreement"] is not None:
                null.append(res["agreement"])
        rows.append(
            {
                "song": f"{s['artist']} - {s['title']}",
                "midi_files": len(mids),
                **best,
                "null_mean": round(float(np.mean(null)), 4) if null else None,
                "null_max": round(float(np.max(null)), 4) if null else None,
            }
        )
    ag = [r["agreement"] for r in rows]
    nulls = [r["null_mean"] for r in rows if r["null_mean"] is not None]
    return {
        "songs": rows,
        "mean_agreement": round(float(np.mean(ag)), 4) if ag else None,
        "median_agreement": round(float(np.median(ag)), 4) if ag else None,
        "mean_null": round(float(np.mean(nulls)), 4) if nulls else None,
        "songs_above_null_max": sum(
            1 for r in rows if r["null_max"] is not None and r["agreement"] > r["null_max"]
        ),
    }
