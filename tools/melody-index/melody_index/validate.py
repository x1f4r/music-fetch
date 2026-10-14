"""MIDI vs preview melody agreement by subsequence DTW.

Both melodies are rendered to 10 fps contours (rests removed). Every preview
phrase is aligned anywhere inside the MIDI melody with steps (1,1), (1,2) and
(2,1) (the last two penalized), for the three transpositions whose pitch-class
histograms agree best, using an octave-folded cost. Agreement is the share of
query frames on the warping path within half a semitone of the reference after
one constant offset, so octave slips and wrong notes both count against it.
"""

from __future__ import annotations

import json
import logging
import random
import time
from concurrent.futures import ProcessPoolExecutor
from typing import Any

import numpy as np

from . import db as dbm
from .config import Paths
from .notes import notes_to_contour

log = logging.getLogger("mfidx.validate")

FPS = 10
CLIP = 3.0
TOLERANCE = 0.5
STEP_PENALTY = 0.3
INF = 1e18


def phrase_contour(phrase: dict[str, Any], fps: int = FPS, absolute: bool = False) -> np.ndarray:
    start = phrase["start_ms"] if absolute else 0
    notes = [(p, o + start, d) for p, o, d in phrase["notes"]]
    return notes_to_contour(notes, fps)


def song_contour(phrases: list[dict[str, Any]], fps: int = FPS) -> np.ndarray:
    """Whole melody (absolute times), voiced frames only."""
    notes = [(p, ph["start_ms"] + o, d) for ph in phrases for p, o, d in ph["notes"]]
    c = notes_to_contour(sorted(notes, key=lambda n: n[1]), fps)
    return c[~np.isnan(c)]


def folded(x: np.ndarray) -> np.ndarray:
    return np.abs(np.mod(x + 6.0, 12.0) - 6.0)


def candidate_shifts(q: np.ndarray, r: np.ndarray, top: int = 3) -> list[int]:
    """Transpositions whose pitch-class histograms correlate best (``q - k`` matches ``r``)."""
    hq = np.bincount(np.mod(np.round(q).astype(int), 12), minlength=12).astype(float)
    hr = np.bincount(np.mod(np.round(r).astype(int), 12), minlength=12).astype(float)
    corr = [float(np.dot(np.roll(hq, -k), hr)) for k in range(12)]
    return sorted(range(12), key=lambda k: -corr[k])[:top]


def subseq_dtw(
    q: np.ndarray, r: np.ndarray, shifts: list[int] | None = None
) -> tuple[float, float, int]:
    """(agreement, mean cost, offset in semitones) of ``q`` aligned anywhere inside ``r``.

    The alignment uses an octave-folded cost (robust to octave displacement); agreement
    is then measured strictly on the warping path: the share of query frames within
    ``TOLERANCE`` semitones of the reference after one constant offset (the path median).
    """
    n, m = len(q), len(r)
    if n < 4 or m < 4:
        return 0.0, CLIP, 0
    ks = np.array(shifts if shifts is not None else candidate_shifts(q, r), dtype=float)
    s = len(ks)
    c = np.minimum(folded(q[None, :, None] - r[None, None, :] - ks[:, None, None]), CLIP)
    choice = np.zeros((s, n, m), dtype=np.int8)
    inf1 = np.full((s, 1), INF)
    inf2 = np.full((s, 2), INF)
    D_prev2 = np.full((s, m), INF)
    D_prev = c[:, 0].copy()
    for i in range(1, n):
        o1 = np.concatenate([inf1, D_prev[:, :-1]], axis=1)
        o2 = np.concatenate([inf2, D_prev[:, :-2]], axis=1) + STEP_PENALTY
        if i >= 2:
            o3 = np.concatenate([inf1, D_prev2[:, :-1] + c[:, i - 1, 1:]], axis=1) + STEP_PENALTY
        else:
            o3 = np.full((s, m), INF)
        stack = np.stack([o1, o2, o3])
        arg = np.argmin(stack, axis=0)
        choice[:, i] = arg
        D_cur = np.take_along_axis(stack, arg[None], axis=0)[0] + c[:, i]
        D_prev2, D_prev = D_prev, D_cur
    flat = int(np.argmin(D_prev))
    ki, j = divmod(flat, m)
    cost = float(D_prev[ki, j])
    if cost >= INF / 2:
        return 0.0, CLIP, 0
    qi: list[int] = []
    rj: list[int] = []
    i = n - 1
    while i >= 0 and j >= 0:
        qi.append(i)
        rj.append(j)
        if i == 0:
            break
        step = choice[ki, i, j]
        if step == 0:
            i, j = i - 1, j - 1
        elif step == 1:
            i, j = i - 1, j - 2
        else:
            qi.append(i - 1)
            rj.append(j)
            i, j = i - 2, j - 1
    diff = q[np.array(qi)] - r[np.array(rj)]
    offset = float(np.median(diff))
    good = float(np.mean(np.abs(diff - offset) <= TOLERANCE))
    return good, cost / n, int(round(offset))


def agreement(
    preview_phrases: list[dict[str, Any]], midi_phrases: list[dict[str, Any]]
) -> dict[str, Any]:
    """Frame-weighted agreement of all preview phrases against one MIDI melody."""
    ref = song_contour(midi_phrases)
    total = good = cost = 0.0
    shifts: list[int] = []
    for ph in preview_phrases:
        q = phrase_contour(ph)
        q = q[~np.isnan(q)]
        if len(q) < 10:
            continue
        a, c, k = subseq_dtw(q, ref)
        total += len(q)
        good += a * len(q)
        cost += c * len(q)
        shifts.append(k)
    if total == 0:
        return {"agreement": None, "cost": None, "shift": None, "frames": 0}
    shift = max(set(shifts), key=shifts.count)
    return {
        "agreement": round(good / total, 4),
        "cost": round(cost / total, 4),
        "shift": shift,
        "frames": int(total),
    }


def _job(args: tuple) -> tuple:
    song_id, md5, track, preview, midi = args
    res = agreement(preview, midi)
    return song_id, md5, track, res


def _midi_tracks(phrases: list[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    by: dict[int, list[dict[str, Any]]] = {}
    for ph in phrases:
        by.setdefault(int(ph.get("track", 0)), []).append(ph)
    return by


def run(paths: Paths, workers: int = 12) -> dict[str, Any]:
    """Agreement for every song with both preview and MIDI phrases."""
    conn = dbm.connect(paths.db)
    dbm.stage_started(conn, "validate")
    rows = conn.execute(
        "SELECT p.song_id, p.phrases AS pp, m.md5, m.phrases AS mp FROM preview_melody p"
        " JOIN midi_melody m ON m.song_id=p.song_id LEFT JOIN midi_validation v ON v.song_id=p.song_id AND v.md5=m.md5"
        " WHERE p.status='ok' AND m.status='ok' AND v.md5 IS NULL"
    ).fetchall()
    jobs = []
    for r in rows:
        preview = json.loads(r["pp"])
        for track, phrases in _midi_tracks(json.loads(r["mp"])).items():
            jobs.append((r["song_id"], r["md5"], track, preview, phrases))
    t0 = time.time()
    out = []
    with ProcessPoolExecutor(max_workers=workers) as pool:
        for song_id, md5, track, res in pool.map(_job, jobs, chunksize=4):
            out.append(
                (
                    song_id,
                    md5,
                    track,
                    res["agreement"],
                    res["cost"],
                    res["shift"],
                    res["frames"],
                    time.time(),
                )
            )
            if len(out) % 500 == 0:
                log.info("validate: %d/%d", len(out), len(jobs))
    with dbm.Tx(conn):
        conn.executemany(
            "INSERT OR REPLACE INTO midi_validation(song_id, md5, track, agreement, cost, shift, frames, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            out,
        )
    null = null_agreement(conn, workers=workers)
    p95 = float(np.percentile(null, 95)) if null else None
    # Preview-vs-MIDI agreement is noisy (instrumental passages, arrangement changes), so
    # only melodies that do no better than a random song's (the null median) are rejected.
    threshold = round(max(0.4, float(np.median(null)) if null else 0.45), 4)
    total = conn.execute("SELECT COUNT(*) FROM midi_validation").fetchone()[0]
    info = {
        "pairs_new": len(out),
        "pairs_total": total,
        "seconds": round(time.time() - t0, 1),
        "null_samples": len(null),
        "null_median": round(float(np.median(null)), 4) if null else None,
        "null_p95": round(p95, 4) if p95 is not None else None,
        "threshold": threshold,
    }
    dbm.stage_finished(conn, "validate", info)
    log.info("validate: %s", info)
    return info


def null_agreement(conn, samples: int = 200, seed: int = 7, workers: int = 12) -> list[float]:
    """Agreement of previews against MIDI melodies of *other* songs (chance level)."""
    rng = random.Random(seed)
    previews = conn.execute(
        "SELECT song_id, phrases FROM preview_melody WHERE status='ok' LIMIT 5000"
    ).fetchall()
    midis = conn.execute(
        "SELECT song_id, phrases FROM midi_melody WHERE status='ok' LIMIT 5000"
    ).fetchall()
    if not previews or not midis:
        return []
    jobs = []
    for _ in range(samples):
        p = rng.choice(previews)
        m = rng.choice(midis)
        if p["song_id"] == m["song_id"]:
            continue
        tracks = _midi_tracks(json.loads(m["phrases"]))
        jobs.append(
            (
                p["song_id"],
                "",
                0,
                json.loads(p["phrases"]),
                tracks.get(0) or next(iter(tracks.values())),
            )
        )
    with ProcessPoolExecutor(max_workers=workers) as pool:
        res = [r[3]["agreement"] for r in pool.map(_job, jobs, chunksize=4)]
    return sorted(a for a in res if a is not None)
