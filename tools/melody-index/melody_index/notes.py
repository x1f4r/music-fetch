"""From an f0 track (10 ms hop) to notes and phrases, plus shared phrase helpers.

Pipeline: voicing gate -> salience dips become breaks (repeated notes) -> drop
tiny voiced islands -> median smoothing -> octave-error folding against a local
median -> stable-pitch segmentation with a persistence rule -> short-note
absorption -> merge of touching near-identical neighbours -> phrases split at
rests.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
from numpy.lib.stride_tricks import sliding_window_view
from scipy.ndimage import median_filter

FRAME_MS = 10


@dataclass(frozen=True)
class NoteParams:
    voicing: float = 0.04
    dip_ratio: float = 0.72
    dip_window: int = 5
    min_island_ms: int = 50
    median_frames: int = 5
    octave_window_ms: int = 500
    split_semitones: float = 0.75
    split_frames: int = 3
    min_note_ms: int = 80
    merge_semitones: float = 0.5
    merge_gap_ms: int = 0
    phrase_gap_ms: int = 400
    outlier_semitones: float = 15.0
    min_phrase_notes: int = 6
    max_phrase_ms: int = 20000
    pitch_lo: float = 36.0
    pitch_hi: float = 96.0


DEFAULT_PARAMS = NoteParams()


@dataclass
class Note:
    pitch: float
    onset_ms: int
    dur_ms: int

    @property
    def end_ms(self) -> int:
        return self.onset_ms + self.dur_ms


@dataclass
class Phrase:
    start_ms: int
    notes: list[Note] = field(default_factory=list)

    def to_json(self, source: str, **extra) -> dict:
        """CORE_API shape: onsets relative to the phrase start."""
        d = {
            "source": source,
            "start_ms": int(self.start_ms),
            "notes": [
                [round(float(n.pitch), 2), int(n.onset_ms - self.start_ms), int(n.dur_ms)]
                for n in self.notes
            ],
        }
        d.update(extra)
        return d


def hz_to_midi(f0: np.ndarray) -> np.ndarray:
    f = np.asarray(f0, dtype=np.float64)
    out = np.full(f.shape, np.nan)
    v = f > 0
    out[v] = 69.0 + 12.0 * np.log2(f[v] / 440.0)
    return out


def midi_to_hz(p: np.ndarray | float) -> np.ndarray | float:
    return 440.0 * 2.0 ** ((np.asarray(p) - 69.0) / 12.0)


def runs(mask: np.ndarray) -> list[tuple[int, int]]:
    """[start, end) index ranges where ``mask`` is true."""
    m = np.concatenate([[False], np.asarray(mask, dtype=bool), [False]])
    d = np.diff(m.astype(np.int8))
    starts = np.flatnonzero(d == 1)
    ends = np.flatnonzero(d == -1)
    return list(zip(starts.tolist(), ends.tolist(), strict=True))


def smooth(p: np.ndarray, size: int) -> np.ndarray:
    """Median filter applied inside each voiced run (NaN = unvoiced)."""
    out = p.copy()
    if size <= 1:
        return out
    for s, e in runs(~np.isnan(p)):
        if e - s >= 3:
            out[s:e] = median_filter(p[s:e], size=min(size, e - s), mode="nearest")
    return out


def fix_octaves(p: np.ndarray, window: int) -> np.ndarray:
    """Fold frames that sit an octave (±12) away from the local voiced median."""
    out = p.copy()
    voiced = np.flatnonzero(~np.isnan(p))
    if voiced.size < 5:
        return out
    half = max(1, window // 2)
    vals = p[voiced]
    lo = np.searchsorted(voiced, voiced - half, side="left")
    hi = np.searchsorted(voiced, voiced + half, side="right")
    for i in range(voiced.size):
        med = np.median(vals[lo[i] : hi[i]])
        diff = vals[i] - med
        k = np.round(diff / 12.0)
        if k != 0 and abs(diff - 12.0 * k) < 3.0:
            out[voiced[i]] = vals[i] - 12.0 * k
    return out


def _segment_run(p: np.ndarray, s: int, e: int, prm: NoteParams) -> list[tuple[int, int, float]]:
    notes: list[tuple[int, int, float]] = []
    cur_start = s
    cur = [p[s]]
    j = s + 1
    while j < e:
        ref = float(np.median(cur[-15:]))
        dev = p[j] - ref
        if abs(dev) > prm.split_semitones:
            k_end = j + prm.split_frames
            if k_end <= e:
                w = p[j:k_end]
                same_side = np.all(np.sign(w - ref) == np.sign(dev))
                if (
                    same_side
                    and np.all(np.abs(w - ref) > prm.split_semitones)
                    and np.ptp(w) <= 2 * prm.split_semitones
                ):
                    notes.append((cur_start, j, float(np.median(cur))))
                    cur_start, cur = j, [p[j]]
                    j += 1
                    continue
            j += 1  # transient outlier: skip without moving the reference
            continue
        cur.append(p[j])
        j += 1
    notes.append((cur_start, e, float(np.median(cur))))
    return notes


def _absorb_short(notes: list[list], min_frames: int) -> list[list]:
    """Merge notes shorter than ``min_frames`` into a touching neighbour, else drop them."""
    changed = True
    while changed:
        changed = False
        for i, (s, e, pitch) in enumerate(notes):
            if e - s >= min_frames:
                continue
            prev_ok = i > 0 and notes[i - 1][1] == s
            next_ok = i + 1 < len(notes) and notes[i + 1][0] == e
            if prev_ok and next_ok:
                target = (
                    i - 1 if abs(notes[i - 1][2] - pitch) <= abs(notes[i + 1][2] - pitch) else i + 1
                )
            elif prev_ok:
                target = i - 1
            elif next_ok:
                target = i + 1
            else:
                target = None
            if target is not None:
                notes[target][0] = min(notes[target][0], s)
                notes[target][1] = max(notes[target][1], e)
            del notes[i]
            changed = True
            break
    return notes


def salience_dips(conf: np.ndarray, ratio: float, window: int) -> np.ndarray:
    """Frames whose confidence falls below ``ratio`` x the peak on both sides: articulation
    breaks (consonants, re-attacks) that the network bridges with a continuous f0."""
    c = np.asarray(conf, dtype=np.float64)
    n = len(c)
    if n < 3 or ratio <= 0:
        return np.zeros(n, dtype=bool)
    win = sliding_window_view(np.pad(c, (window, window)), window)
    left = win[:n].max(axis=1)
    right = win[window + 1 : window + 1 + n].max(axis=1)
    prev = np.concatenate([[np.inf], c[:-1]])
    nxt = np.concatenate([c[1:], [np.inf]])
    return (c < ratio * np.minimum(left, right)) & (c <= prev) & (c <= nxt)


def f0_to_notes(
    f0: np.ndarray, conf: np.ndarray | None = None, prm: NoteParams = DEFAULT_PARAMS
) -> list[Note]:
    """Notes (absolute onsets in ms) from an f0 track at 10 ms per frame."""
    f = np.asarray(f0, dtype=np.float64).copy()
    if conf is not None:
        c = np.asarray(conf, dtype=np.float64)
        f[c <= prm.voicing] = 0.0
        f[salience_dips(c, prm.dip_ratio, prm.dip_window)] = 0.0
    p = hz_to_midi(f)
    p[(p < prm.pitch_lo - 12) | (p > prm.pitch_hi + 12)] = np.nan
    min_island = max(1, prm.min_island_ms // FRAME_MS)
    for s, e in runs(~np.isnan(p)):
        if e - s < min_island:
            p[s:e] = np.nan
    p = smooth(p, prm.median_frames)
    p = fix_octaves(p, prm.octave_window_ms // FRAME_MS)
    p = smooth(p, 3)

    raw: list[list] = []
    for s, e in runs(~np.isnan(p)):
        raw.extend([list(n) for n in _segment_run(p, s, e, prm)])
    raw = _absorb_short(raw, max(1, prm.min_note_ms // FRAME_MS))
    # Re-estimate each note's pitch from its (possibly extended) frame span.
    for n in raw:
        seg = p[n[0] : n[1]]
        seg = seg[~np.isnan(seg)]
        if seg.size:
            n[2] = float(np.median(seg))

    merged: list[list] = []
    gap = prm.merge_gap_ms // FRAME_MS
    for n in raw:
        if (
            merged
            and 0 <= n[0] - merged[-1][1] <= gap
            and abs(n[2] - merged[-1][2]) < prm.merge_semitones
        ):
            a, b = merged[-1], n
            wa, wb = a[1] - a[0], b[1] - b[0]
            a[2] = (a[2] * wa + b[2] * wb) / max(1, wa + wb)
            a[1] = b[1]
        else:
            merged.append(list(n))
    out = []
    for s, e, pitch in merged:
        if prm.pitch_lo <= pitch <= prm.pitch_hi and (e - s) * FRAME_MS >= prm.min_note_ms:
            out.append(Note(round(pitch, 2), s * FRAME_MS, (e - s) * FRAME_MS))
    return out


def make_monophonic(notes: list[Note]) -> list[Note]:
    """Sorted, non-overlapping notes (later onsets cut earlier notes short)."""
    ns = sorted(notes, key=lambda n: (n.onset_ms, -n.pitch))
    out: list[Note] = []
    for n in ns:
        if out and n.onset_ms == out[-1].onset_ms:
            continue  # same onset: keep the highest pitch (skyline)
        if out and out[-1].end_ms > n.onset_ms:
            out[-1] = Note(out[-1].pitch, out[-1].onset_ms, n.onset_ms - out[-1].onset_ms)
        out.append(Note(n.pitch, n.onset_ms, n.dur_ms))
    return [n for n in out if n.dur_ms > 0]


def _split_long(notes: list[Note], max_ms: int) -> list[list[Note]]:
    if len(notes) < 2 or notes[-1].end_ms - notes[0].onset_ms <= max_ms:
        return [notes]
    gaps = [notes[i + 1].onset_ms - notes[i].end_ms for i in range(len(notes) - 1)]
    # Split at the largest rest in the middle 80% so both halves stay useful.
    lo, hi = max(1, len(notes) // 10), max(2, len(notes) - len(notes) // 10)
    cut = max(range(lo - 1, hi - 1), key=lambda i: (gaps[i], -abs(i - len(notes) / 2)))
    return _split_long(notes[: cut + 1], max_ms) + _split_long(notes[cut + 1 :], max_ms)


def phrases_from_notes(notes: list[Note], prm: NoteParams = DEFAULT_PARAMS) -> list[Phrase]:
    """Split at rests ≥ ``phrase_gap_ms``; keep phrases with ≥ ``min_phrase_notes`` notes."""
    ns = make_monophonic(notes)
    groups: list[list[Note]] = []
    for n in ns:
        if groups and n.onset_ms - groups[-1][-1].end_ms < prm.phrase_gap_ms:
            groups[-1].append(n)
        else:
            groups.append([n])
    out: list[Phrase] = []
    for g in groups:
        if prm.outlier_semitones > 0 and len(g) >= 3:
            # Notes far from the phrase's register are another instrument (bass, a lead riff).
            med = float(np.median([n.pitch for n in g]))
            g = [n for n in g if abs(n.pitch - med) <= prm.outlier_semitones]
        for part in _split_long(g, prm.max_phrase_ms):
            if len(part) >= prm.min_phrase_notes:
                out.append(Phrase(part[0].onset_ms, part))
    return out


def dedupe_phrases(phrases: list[Phrase], tolerance: float = 0.5) -> list[Phrase]:
    """Drop phrases that repeat an earlier one (same length, every pitch within ``tolerance``)."""
    kept: list[Phrase] = []
    for ph in phrases:
        pitches = np.array([n.pitch for n in ph.notes])
        dup = False
        for k in kept:
            if len(k.notes) == len(ph.notes):
                kp = np.array([n.pitch for n in k.notes])
                if np.all(np.abs(kp - pitches) <= tolerance):
                    dup = True
                    break
        if not dup:
            kept.append(ph)
    return kept


def notes_to_contour(
    notes: list[tuple[float, int, int]] | list[Note], fps: int = 20, start_ms: int = 0
) -> np.ndarray:
    """Frame contour in MIDI semitones (NaN = rest) from notes with absolute or relative onsets."""
    items = [(n.pitch, n.onset_ms, n.dur_ms) if isinstance(n, Note) else tuple(n) for n in notes]
    if not items:
        return np.zeros(0)
    end = max(o + d for _, o, d in items) - start_ms
    out = np.full(int(np.ceil(end * fps / 1000.0)) + 1, np.nan)
    for p, o, d in items:
        a = int(round((o - start_ms) * fps / 1000.0))
        b = max(a + 1, int(round((o - start_ms + d) * fps / 1000.0)))
        out[a:b] = p
    return out


def voiced_ratio(f0: np.ndarray, conf: np.ndarray, threshold: float) -> float:
    if len(f0) == 0:
        return 0.0
    return float(np.mean((np.asarray(f0) > 0) & (np.asarray(conf) > threshold)))
