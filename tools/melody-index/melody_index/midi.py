"""Stage 4: melody references from the Lakh MIDI Dataset.

``catalog`` lists clean_midi (artist/title from the path) and lmd_matched
(artist/title via the MSD track id), ``link`` attaches files to universe songs
by normalized key (and adds MIDI-only songs that many people transcribed),
``extract`` picks the melody track(s) and turns them into phrases.

Melody track recipe: karaoke lyric syllables aligned with note onsets, track
name, monophony, register, range, note rate, activity and General MIDI program;
the winner is made monophonic with a skyline and split at rests.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import signal
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np

from . import db as dbm
from .config import Paths
from .normalize import match_keys, song_key
from .notes import Note, NoteParams, dedupe_phrases, phrases_from_notes

log = logging.getLogger("mfidx.midi")

POS_NAME_RE = re.compile(
    r"melod|vocal|voice|vox|lead|sing|canto|stimme|melodie|solo|tune|theme", re.I
)
NEG_NAME_RE = re.compile(
    r"bass|drum|perc|pad|chord|string|harm|backing|bvox|choir|rhythm|accomp|arp|kick|snare", re.I
)
DUP_SUFFIX_RE = re.compile(r"\.\d+$")
MIDI_ONLY_MIN_CLEAN_VERSIONS = (
    2  # distinct clean_midi transcriptions of a song outside the universe
)
MATCHED_MIN_SCORE = 0.6  # lmd_matched alignment confidence needed to trust the MSD artist/title
MIDI_PRM = NoteParams(min_note_ms=40, phrase_gap_ms=400, min_phrase_notes=6, max_phrase_ms=20000)


def program_bonus(program: int) -> float:
    table = [
        (0, 7, 0.0),
        (8, 15, -0.3),
        (16, 23, -0.2),
        (24, 31, -0.3),
        (32, 39, -3.0),
        (40, 47, 0.3),
        (48, 51, -0.7),
        (52, 54, 1.0),
        (55, 55, -1.0),
        (56, 63, 0.4),
        (64, 71, 0.8),
        (72, 79, 0.8),
        (80, 87, 0.8),
        (88, 95, -1.5),
        (96, 103, -1.5),
        (104, 111, 0.0),
        (112, 119, -2.0),
        (120, 127, -3.0),
    ]
    for lo, hi, b in table:
        if lo <= program <= hi:
            return b
    return 0.0


@dataclass
class TrackFeatures:
    index: int
    name: str
    program: int
    n_notes: int
    mono: float
    median: float
    range: float
    nps: float
    active: float
    lyric_match: float
    lyric_n: int
    stepwise: float
    score: float = 0.0


def track_features(
    index: int,
    name: str,
    program: int,
    notes: list[tuple[float, float, int]],
    song_end: float,
    syllables: np.ndarray,
) -> TrackFeatures:
    """``notes``: (start_s, end_s, pitch)."""
    starts = np.array([n[0] for n in notes])
    ends = np.array([n[1] for n in notes])
    pitches = np.array([n[2] for n in notes], dtype=float)
    ev_t = np.concatenate([starts, ends])
    ev_d = np.concatenate([np.ones_like(starts), -np.ones_like(ends)])
    order = np.lexsort((ev_d, ev_t))  # ends before starts at equal times
    ev_t, ev_d = ev_t[order], ev_d[order]
    count = np.cumsum(ev_d)[:-1]
    span = np.diff(ev_t)
    sounding = float(span[count >= 1].sum())
    single = float(span[count == 1].sum())
    mono = single / sounding if sounding > 0 else 0.0
    # Melodies move mostly by step; arpeggios and riffs leap.
    line = pitches[np.argsort(starts, kind="stable")]
    steps = np.abs(np.diff(line))
    stepwise = float(np.mean(steps <= 2)) if steps.size else 0.0
    lyric_match = 0.0
    if syllables.size:
        on = np.sort(starts)
        pos = (
            np.clip(np.searchsorted(on, syllables), 1, len(on) - 1)
            if len(on) > 1
            else np.zeros(len(syllables), int)
        )
        near = np.minimum(
            np.abs(on[pos] - syllables), np.abs(on[np.maximum(pos - 1, 0)] - syllables)
        )
        lyric_match = float(np.mean(near <= 0.06))
    return TrackFeatures(
        index=index,
        name=name or "",
        program=int(program),
        n_notes=len(notes),
        mono=round(mono, 3),
        median=float(np.median(pitches)),
        range=float(np.percentile(pitches, 95) - np.percentile(pitches, 5)),
        nps=len(notes) / sounding if sounding > 0 else 0.0,
        active=sounding / song_end if song_end > 0 else 0.0,
        lyric_match=round(lyric_match, 3),
        lyric_n=int(syllables.size),
        stepwise=round(stepwise, 3),
    )


def melody_score(f: TrackFeatures) -> float:
    s = 0.0
    if f.lyric_n >= 20:
        s += 4.0 * f.lyric_match
    if POS_NAME_RE.search(f.name):
        s += 2.0
    if NEG_NAME_RE.search(f.name):
        s -= 3.0
    s += 1.0 if f.mono >= 0.8 else -5.0 * (0.8 - f.mono)
    s += 1.0 - min(2.5, abs(f.median - 67.0) / 8.0)
    s += 0.5 if f.range <= 24 else -0.5
    s += 0.5 if 1.0 <= f.nps <= 8.0 else -0.5
    s += max(-1.5, min(1.0, 3.0 * (f.stepwise - 0.45)))
    if f.active < 0.1:
        s -= 1.5
    elif f.active > 0.9:
        s -= 1.0
    else:
        s += 0.5 if 0.2 <= f.active <= 0.85 else -0.5
    s += program_bonus(f.program)
    s += 0.02 * (f.median - 60)
    return round(s, 3)


def skyline(notes: list[tuple[float, float, int]]) -> list[Note]:
    """Monophonic line: highest pitch at each onset, earlier notes cut at the next onset."""
    ns = sorted(notes, key=lambda n: (round(n[0], 3), -n[2]))
    out: list[list[float]] = []
    for start, end, pitch in ns:
        if out and abs(start - out[-1][0]) < 0.01:
            continue
        if out and out[-1][1] > start:
            out[-1][1] = start
        out.append([start, end, pitch])
    return [
        Note(float(p), int(round(s * 1000)), int(round((e - s) * 1000)))
        for s, e, p in out
        if e - s >= 0.03
    ]


def _syllable_times(pm) -> np.ndarray:
    times = [ly.time for ly in getattr(pm, "lyrics", [])]
    texts = [t for t in getattr(pm, "text_events", []) if not t.text.startswith("@")]
    if len(texts) > 20:
        times += [t.time for t in texts]
    return np.unique(np.round(np.array(times, dtype=float), 3)) if times else np.zeros(0)


class _Timeout(Exception):
    pass


def _alarm(signum, frame):  # noqa: ARG001
    raise _Timeout()


def analyze_file(path: str, timeout: int = 30) -> dict[str, Any]:
    """Melody phrases of one MIDI file (runs in a worker process)."""
    import pretty_midi

    signal.signal(signal.SIGALRM, _alarm)
    signal.alarm(timeout)
    try:
        pm = pretty_midi.PrettyMIDI(path)
        end = float(pm.get_end_time())
        if not 20.0 <= end <= 1500.0:
            return {"status": "skip", "error": f"duration {end:.0f}s"}
        syl = _syllable_times(pm)
        feats: list[TrackFeatures] = []
        notes_by_idx: dict[int, list[tuple[float, float, int]]] = {}
        for i, inst in enumerate(pm.instruments):
            if inst.is_drum or len(inst.notes) < 16:
                continue
            notes = [
                (float(n.start), float(n.end), int(n.pitch)) for n in inst.notes if n.end > n.start
            ]
            if len(notes) < 16:
                continue
            f = track_features(i, inst.name, inst.program, notes, end, syl)
            f.score = melody_score(f)
            feats.append(f)
            notes_by_idx[i] = notes
        if not feats:
            return {"status": "empty", "error": "no melodic tracks"}
        feats.sort(key=lambda f: f.score, reverse=True)
        chosen = [feats[0]]
        if len(feats) > 1 and feats[1].score >= feats[0].score - 0.75 and feats[1].score > 1.0:
            chosen.append(feats[1])
        phrases: list[dict[str, Any]] = []
        for rank, f in enumerate(chosen):
            line = skyline(notes_by_idx[f.index])
            for ph in dedupe_phrases(phrases_from_notes(line, MIDI_PRM)):
                d = ph.to_json("midi")
                d["track"] = rank
                phrases.append(d)
        return {
            "status": "ok" if phrases else "empty",
            "tracks": [asdict(f) for f in chosen],
            "candidates": len(feats),
            "phrases": phrases,
            "confidence": chosen[0].score,
        }
    except _Timeout:
        return {"status": "error", "error": "timeout"}
    except Exception as e:  # noqa: BLE001 - malformed MIDI files are common in Lakh
        return {"status": "error", "error": f"{type(e).__name__}: {e}"[:200]}
    finally:
        signal.alarm(0)


# --------------------------------------------------------------------------- catalog


def _md5(path: Path) -> str:
    return hashlib.md5(path.read_bytes()).hexdigest()


def catalog(paths: Paths) -> dict[str, Any]:
    conn = dbm.connect(paths.db)
    lmd = paths.lmd
    rows: dict[str, tuple] = {}
    clean = lmd / "clean_midi"
    n_clean = 0
    if clean.is_dir():
        for artist_dir in sorted(p for p in clean.iterdir() if p.is_dir()):
            for f in sorted(artist_dir.glob("*.mid")):
                title = DUP_SUFFIX_RE.sub("", f.stem)
                md5 = _md5(f)
                rows[md5] = (
                    md5,
                    str(f.relative_to(lmd)),
                    "clean",
                    artist_dir.name,
                    title,
                    None,
                    None,
                )
                n_clean += 1
    matched = lmd / "lmd_matched"
    n_matched = 0
    if matched.is_dir():
        tracks: dict[str, tuple[str, str]] = {}
        with open(lmd / "unique_tracks.txt", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                parts = line.rstrip("\n").split("<SEP>")
                if len(parts) == 4:
                    tracks[parts[0]] = (parts[2], parts[3])
        scores = json.loads((lmd / "match_scores.json").read_text())
        for f in matched.rglob("*.mid"):
            msd, md5 = f.parent.name, f.stem
            if msd not in tracks:
                continue
            score = float((scores.get(msd) or {}).get(md5, 0.0))
            prev = rows.get(md5)
            if prev is not None and (prev[2] == "clean" or (prev[6] or 0) >= score):
                continue
            artist, title = tracks[msd]
            rows[md5] = (md5, str(f.relative_to(lmd)), "matched", artist, title, msd, score)
            n_matched += 1
    with dbm.Tx(conn):
        conn.executemany(
            "INSERT INTO midi_files(md5, path, origin, artist, title, msd_id, match_score) VALUES (?, ?, ?, ?, ?, ?, ?)"
            " ON CONFLICT(md5) DO UPDATE SET path=excluded.path, origin=excluded.origin, artist=excluded.artist,"
            " title=excluded.title, msd_id=excluded.msd_id, match_score=excluded.match_score",
            list(rows.values()),
        )
    info = {"clean_files": n_clean, "matched_files": n_matched, "distinct_md5": len(rows)}
    log.info("midi catalog: %s", info)
    return info


def link(paths: Paths) -> dict[str, Any]:
    """Attach MIDI files to universe songs; add MIDI-only songs that were transcribed
    several times in clean_midi (a popularity proxy that, unlike lmd_matched, does not
    depend on automatic audio matching)."""
    conn = dbm.connect(paths.db)
    conn.execute("UPDATE songs SET in_universe=0 WHERE origin='midi'")
    index: dict[tuple[str, str, str], tuple[float, int]] = {}
    for r in conn.execute(
        "SELECT id, title, artist, aliases, score FROM songs WHERE in_universe=1"
    ):
        pairs = [(r["artist"], r["title"])] + [
            tuple(a) for a in json.loads(r["aliases"] or "[]") if len(a) == 2
        ]
        for artist, title in pairs:
            for k in match_keys(artist, title):
                if k not in index or index[k][0] < r["score"]:
                    index[k] = (r["score"], r["id"])
    files = conn.execute(
        "SELECT md5, artist, title, origin, match_score FROM midi_files"
    ).fetchall()
    linked: list[tuple[str, int]] = []
    unlinked: dict[str, list[Any]] = defaultdict(list)
    for f in files:
        if not f["artist"] or not f["title"]:
            continue
        if f["origin"] == "matched" and (f["match_score"] or 0.0) < MATCHED_MIN_SCORE:
            continue
        hits = [index[k] for k in match_keys(f["artist"], f["title"]) if k in index]
        words = f["artist"].split()
        if not hits and len(words) == 2:
            # clean_midi often files people as "Surname Firstname" ("Jackson Michael").
            hits = [
                index[k] for k in match_keys(f"{words[1]} {words[0]}", f["title"]) if k in index
            ]
        if hits:
            linked.append((f["md5"], max(hits)[1]))
        else:
            unlinked[song_key(f["artist"], f["title"])].append(f)

    midi_only = {}
    for key, members in unlinked.items():
        clean = [m for m in members if m["origin"] == "clean"]
        if (
            len(clean) >= MIDI_ONLY_MIN_CLEAN_VERSIONS
            and not key.startswith("::")
            and not key.endswith("::")
        ):
            midi_only[key] = (clean, members)
    with dbm.Tx(conn):
        for key, (clean, members) in midi_only.items():
            artist, title = clean[0]["artist"], clean[0]["title"]
            score = 0.25 * min(1.0, len(clean) / 6.0)
            conn.execute(
                "INSERT INTO songs(key, title, artist, year, score, popularity, signals, aliases, origin, in_universe)"
                " VALUES (?, ?, ?, NULL, ?, 0, ?, ?, 'midi', 1) ON CONFLICT(key) DO UPDATE SET in_universe=1,"
                " score=MAX(songs.score, excluded.score)",
                (
                    key,
                    title,
                    artist,
                    score,
                    json.dumps({"midi_versions": len(clean)}),
                    json.dumps(
                        sorted({(m["artist"], m["title"]) for m in members})[:12],
                        ensure_ascii=False,
                    ),
                ),
            )
            sid = conn.execute("SELECT id FROM songs WHERE key=?", (key,)).fetchone()["id"]
            linked.extend((m["md5"], sid) for m in members)
        conn.execute("UPDATE midi_files SET key=NULL")
        conn.executemany(
            "UPDATE midi_files SET key=(SELECT key FROM songs WHERE id=?) WHERE md5=?",
            [(sid, md5) for md5, sid in linked],
        )
        conn.execute("UPDATE midi_melody SET song_id=NULL")
        conn.executemany(
            "UPDATE midi_melody SET song_id=? WHERE md5=?", [(sid, md5) for md5, sid in linked]
        )
    info = {
        "files": len(files),
        "linked_files": len(linked),
        "songs_with_midi": len({s for _, s in linked}),
        "midi_only_songs": len(midi_only),
    }
    log.info("midi link: %s", info)
    return info


def extract(paths: Paths, workers: int | None = None, limit: int | None = None) -> dict[str, Any]:
    conn = dbm.connect(paths.db)
    dbm.stage_started(conn, "midi")
    rows = conn.execute(
        "SELECT f.md5, f.path, s.id AS song_id FROM midi_files f JOIN songs s ON s.key=f.key"
        " LEFT JOIN midi_melody m ON m.md5=f.md5 WHERE m.md5 IS NULL ORDER BY s.score DESC"
        + (" LIMIT ?" if limit else ""),
        ((limit,) if limit else ()),
    ).fetchall()
    workers = workers or max(1, (os.cpu_count() or 4) - 4)
    t0 = time.time()
    done = ok = 0
    buffer: list[tuple] = []

    def flush() -> None:
        nonlocal buffer
        if buffer:
            with dbm.Tx(conn):
                conn.executemany(
                    "INSERT OR REPLACE INTO midi_melody(md5, song_id, status, tracks, phrases, n_phrases, n_notes,"
                    " confidence, error, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    buffer,
                )
            buffer = []

    with ProcessPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(analyze_file, str(paths.lmd / r["path"])): r for r in rows}
        for fut in as_completed(futs):
            r = futs[fut]
            try:
                res = fut.result()
            except Exception as e:  # noqa: BLE001 - a crashed worker should not stop the stage
                res = {"status": "error", "error": f"{type(e).__name__}: {e}"[:200]}
            phrases = res.get("phrases") or []
            buffer.append(
                (
                    r["md5"],
                    r["song_id"],
                    res["status"],
                    json.dumps(res.get("tracks") or []),
                    json.dumps(phrases, separators=(",", ":")),
                    len(phrases),
                    sum(len(p["notes"]) for p in phrases),
                    res.get("confidence"),
                    res.get("error"),
                    time.time(),
                )
            )
            done += 1
            ok += res["status"] == "ok"
            if len(buffer) >= 200:
                flush()
            if done % 1000 == 0:
                log.info(
                    "midi extract: %d/%d (%d ok), %.0f files/min",
                    done,
                    len(rows),
                    ok,
                    done / (time.time() - t0) * 60,
                )
    flush()
    info = {"files": done, "ok": ok, "seconds": round(time.time() - t0, 1)}
    if not limit:
        dbm.stage_finished(conn, "midi", info)
    log.info("midi extract finished: %s", info)
    return info
