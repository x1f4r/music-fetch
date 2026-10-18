"""Contour-domain hum simulation (20 fps, MIDI semitones, NaN = unvoiced).

A query is rendered from a *target melody* living on a reference track's timeline: the source
array plus an affine map from reference frames to source indices (identity for self pairs, the
MIDI placement for MIDI -> preview pairs). The simulator applies what people and pitch trackers
do to a melody: global tempo x0.7-1.4 with local warps, per-note pitch errors and interval
slips, key drift, dropped and inserted notes, vibrato, glides and legato, breaths, octave slips
and frame jitter. It also returns, per query frame, the reference frame it came from.
"""

import numpy as np
from scipy.ndimage import median_filter, uniform_filter1d

FPS = 20
W = 160  # window frames (8 s)


def hold_fill(p):
    """Forward-fill NaNs (leading NaNs take the first voiced value)."""
    v = np.isfinite(p)
    if not v.any():
        return np.zeros_like(p)
    idx = np.where(v, np.arange(len(p)), 0)
    np.maximum.accumulate(idx, out=idx)
    out = p[idx]
    first = np.argmax(v)
    out[:first] = p[first]
    return out


def humanize(p, rng):
    """What a person would hum from an audio-extracted melody: octave slips and short excursions
    (bass pickups, accompaniment leaks) folded back to the running melody, fragments merged."""
    v = np.isfinite(p)
    if v.sum() < 8:
        return p
    f = hold_fill(p)
    med = median_filter(f, size=31, mode="nearest")
    d = f - med
    k = np.round(d / 12)
    fold = (k != 0) & (np.abs(d - 12 * k) < 2.5)
    f = np.where(fold, f - 12 * k, f)
    d = f - med
    far = np.abs(d) > rng.uniform(6, 9)
    f = np.where(far, med, f)
    if rng.random() < 0.8:
        f = median_filter(f, size=int(rng.choice([3, 5, 5, 7])), mode="nearest")
    out = np.where(v, f, np.nan).astype(np.float32)
    return out


def smooth_noise(n, rng, width):
    x = rng.normal(0, 1, n + 2 * width)
    x = uniform_filter1d(x, size=max(1, width), mode="nearest")
    x = uniform_filter1d(x, size=max(1, width), mode="nearest")
    x = x[width : width + n]
    s = x.std()
    return x / s if s > 0 else x


def notes_of(q):
    """Segment ids: a new note where voicing changes or the rounded pitch changes."""
    v = np.isfinite(q)
    r = np.where(v, np.round(np.where(v, q, 0) * 2) / 2, -99)
    change = np.ones(len(q), bool)
    change[1:] = r[1:] != r[:-1]
    return np.cumsum(change) - 1, v


def simulate(src, a, b, start, rng, length=W, tempo=None, strength=1.0, tracker=True):
    """Render a query of `length` frames.

    src: source array (target melody); reference frame x maps to source index a + b * x.
    start: reference frame where the hum begins. Returns (query, ref_pos) where ref_pos[i] is
    the reference frame of query frame i. tracker=False leaves out what a pitch tracker adds
    (octave slips, junk frames, frame jitter), for rendering audio.
    """
    s = strength
    tau = tempo if tempo is not None else float(np.exp(rng.uniform(np.log(0.7), np.log(1.4))))
    warp = np.exp(0.10 * s * rng.uniform(0.3, 1.0) * smooth_noise(length, rng, int(rng.integers(15, 50))))
    step = 1.0 / (tau * warp)
    ref_pos = start + np.concatenate([[0.0], np.cumsum(step[:-1])])
    si = np.round(a + b * ref_pos).astype(np.int64)
    inside = (si >= 0) & (si < len(src))
    q = np.full(length, np.nan, np.float32)
    q[inside] = src[si[inside]]

    seg, v = notes_of(q)
    n_notes = seg[-1] + 1
    # per-note pitch errors, interval slips, cumulative key drift
    err = rng.normal(0, rng.uniform(0.05, 0.4) * s, n_notes)
    slip = rng.random(n_notes) < rng.uniform(0, 0.10) * s
    err += slip * rng.choice([-2, -1, 1, 2], n_notes, p=[0.15, 0.35, 0.35, 0.15])
    jumps = (rng.random(n_notes) < rng.uniform(0, 0.06) * s) * rng.choice([-1, -0.5, 0.5, 1], n_notes)
    err += np.cumsum(jumps)
    q = q + err[seg]
    # dropped notes: merge into the previous note or go silent
    drop = rng.random(n_notes) < rng.uniform(0, 0.08) * s
    if drop.any():
        filled = hold_fill(q)
        for k in np.flatnonzero(drop):
            m = seg == k
            first = np.argmax(m)
            if rng.random() < 0.5 and first > 0 and np.isfinite(q[first - 1]):
                q[m & v] = filled[first - 1]
            else:
                q[m] = np.nan
    # slow drift
    q = q + rng.uniform(0, 0.6) * s * smooth_noise(length, rng, int(rng.integers(40, 120)))
    # ornaments at note starts
    starts = np.flatnonzero(np.diff(np.concatenate([[-1], seg])) != 0)
    for i in starts:
        if rng.random() < 0.04 * s and np.isfinite(q[i]):
            L = int(rng.integers(1, 4))
            q[i : i + L] = q[i] + rng.choice([-2, -1, 1, 2])
    # legato: fill short rests
    if rng.random() < 0.6:
        vv = np.isfinite(q)
        filled = hold_fill(q)
        gap_max = int(rng.integers(2, 8))
        i = 0
        while i < length:
            if vv[i]:
                i += 1
                continue
            j = i
            while j < length and not vv[j]:
                j += 1
            if i > 0 and j < length and j - i <= gap_max:
                q[i:j] = filled[i - 1]
            i = j
    # glides: low-pass within voiced runs
    if rng.random() < 0.7:
        vv = np.isfinite(q)
        k = int(rng.choice([2, 3]))
        f = uniform_filter1d(hold_fill(q), size=k, mode="nearest")
        q = np.where(vv, f, np.nan).astype(np.float32)
    # vibrato
    if rng.random() < 0.5:
        t = np.arange(length) / FPS
        q = q + rng.uniform(0.1, 0.5) * np.sin(2 * np.pi * rng.uniform(4.5, 6.5) * t + rng.uniform(0, 6.3))
    # breaths / tracker dropouts
    for _ in range(int(rng.integers(0, 4))):
        if rng.random() < 0.6 * s:
            i = int(rng.integers(0, length))
            q[i : i + int(rng.integers(1, 8))] = np.nan
    if not tracker:
        return (q + rng.uniform(-6, 6)).astype(np.float32), ref_pos
    # octave slips
    if rng.random() < 0.06 * s:
        i = int(rng.integers(0, length))
        q[i : i + int(rng.integers(3, 20))] += rng.choice([-12, 12])
    # junk frames
    if rng.random() < 0.08 * s:
        idx = rng.integers(0, length, int(rng.integers(1, 4)))
        q[idx] = np.nanmedian(q) + rng.uniform(-24, 24, len(idx)) if np.isfinite(q).any() else np.nan
    # frame jitter, key
    q = q + rng.normal(0, rng.uniform(0.02, 0.2), length) + rng.uniform(-6, 6)
    return q.astype(np.float32), ref_pos


def ref_window(track, start, length=W):
    out = np.full(length, np.nan, np.float32)
    a = max(0, start)
    b = min(len(track), start + length)
    if b > a:
        out[a - start : b - start] = track[a:b]
    return out
