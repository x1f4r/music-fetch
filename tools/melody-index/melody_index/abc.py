"""A small ABC-notation reader for single-voice tunes (the evergreen list).

Supported: the ``L:`` (unit length), ``Q:`` (tempo, "1/4=96" or "96"), ``K:`` (major and minor
keys) header fields; notes with accidentals (``^ ^^ _ __ =``), octave marks (``'`` and ``,``),
lengths (``2``, ``/``, ``/2``, ``3/2``), rests (``z``, ``x``), ties (``-``), broken rhythm
(``>``, ``<``), triplets (``(3``), bar lines (accidentals last until the bar line) and ``|``
variants. Chords keep their top note; grace notes, decorations, chord symbols and comments are
skipped. Middle C is ``C`` (MIDI 60).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from fractions import Fraction

_SHARPS = "FCGDAEB"
_KEYS = {
    "C": 0, "G": 1, "D": 2, "A": 3, "E": 4, "B": 5, "F#": 6, "C#": 7,
    "F": -1, "Bb": -2, "Eb": -3, "Ab": -4, "Db": -5, "Gb": -6, "Cb": -7,
}
_MINOR = {
    "A": 0, "E": 1, "B": 2, "F#": 3, "C#": 4, "G#": 5, "D#": 6,
    "D": -1, "G": -2, "C": -3, "F": -4, "Bb": -5, "Eb": -6,
}
_STEP = {"C": 0, "D": 2, "E": 4, "F": 5, "G": 7, "A": 9, "B": 11}
_NOTE_RE = re.compile(r"(\^\^|\^|__|_|=)?([A-Ga-gzx])([',]*)(\d*)(/*)(\d*)")


class AbcError(ValueError):
    pass


@dataclass
class AbcNote:
    pitch: int | None  # MIDI; None = rest
    start: Fraction  # in whole notes
    length: Fraction


def key_signature(key: str) -> dict[str, int]:
    """Letter -> semitone shift for a key such as "G", "Bb", "Em" or "F#m"."""
    k = key.strip().split()[0] if key.strip() else "C"
    minor = k.endswith("m") and not k.endswith("maj")
    name = k[:-1] if minor else k
    table = _MINOR if minor else _KEYS
    if name not in table:
        raise AbcError(f"unsupported key {key!r}")
    n = table[name]
    sig = dict.fromkeys(_STEP, 0)
    if n > 0:
        for letter in _SHARPS[:n]:
            sig[letter] = 1
    else:
        for letter in reversed(_SHARPS[len(_SHARPS) + n :]):
            sig[letter] = -1
    return sig


def _length(num: str, slashes: str, den: str) -> Fraction:
    n = Fraction(int(num)) if num else Fraction(1)
    if slashes:
        d = int(den) if den else 2 ** len(slashes)
        n /= d
    return n


def parse(text: str) -> tuple[list[AbcNote], dict[str, str]]:
    """Notes (with rests) and header fields of one tune."""
    fields: dict[str, str] = {}
    body: list[str] = []
    for raw in text.splitlines():
        line = raw.split("%", 1)[0].rstrip()
        if not line.strip():
            continue
        m = re.match(r"^\s*([A-Za-z]):(.*)$", line)
        if m and m.group(1) in "XTCMLQKRSOZNHBDGIWw":
            fields[m.group(1)] = m.group(2).strip()
            continue
        body.append(line)
    unit = Fraction(fields.get("L", "1/8"))
    sig = key_signature(fields.get("K", "C"))
    s = " ".join(body)
    s = re.sub(r'"[^"]*"', " ", s)  # chord symbols / annotations
    s = re.sub(r"![^!]*!", " ", s)  # decorations
    s = re.sub(r"\{[^}]*\}", " ", s)  # grace notes
    s = re.sub(r"\[([^\]|:]*)\]", lambda m: _top_of_chord(m.group(1)), s)  # chords -> top note
    notes: list[AbcNote] = []
    t = Fraction(0)
    bar_acc: dict[tuple[str, int], int] = {}
    tuplet: list[Fraction] = []  # remaining time factors
    broken: Fraction | None = None
    tie = False
    i = 0
    while i < len(s):
        c = s[i]
        if c in " \t":
            i += 1
            continue
        if c in "|:[]":
            bar_acc.clear()
            i += 1
            continue
        if c == "(" and i + 1 < len(s) and s[i + 1].isdigit():
            p = int(s[i + 1])
            q = {2: 3, 3: 2, 4: 3, 6: 2}.get(p, 2)
            tuplet = [Fraction(q, p)] * p
            i += 2
            continue
        if c in "()":
            i += 1
            continue
        if c == "-":
            tie = True
            i += 1
            continue
        if c in "><":
            n = 1
            while i + n < len(s) and s[i + n] == c:
                n += 1
            if not notes:
                raise AbcError("broken rhythm without a note before it")
            prev = notes[-1]
            factor = Fraction(1, 2**n)
            if c == ">":
                extra = prev.length * (1 - factor)
                prev.length += extra
                broken = factor
            else:
                cut = prev.length * (1 - factor)
                prev.length -= cut
                broken = 2 - factor
            t = prev.start + prev.length
            i += n
            continue
        m = _NOTE_RE.match(s, i)
        if not m:
            raise AbcError(f"cannot read {s[i:i + 12]!r}")
        acc, letter, octave, num, slashes, den = m.groups()
        length = _length(num, slashes, den) * unit
        if broken is not None:
            length *= broken
            broken = None
        if tuplet:
            length *= tuplet.pop(0)
        if letter in "zx":
            notes.append(AbcNote(None, t, length))
            t += length
            tie = False
            i = m.end()
            continue
        upper = letter.upper()
        base = 60 + _STEP[upper] + (12 if letter.islower() else 0)
        base += 12 * octave.count("'") - 12 * octave.count(",")
        key = (upper, base // 12)
        if acc:
            shift = {"^": 1, "^^": 2, "_": -1, "__": -2, "=": 0}[acc]
            bar_acc[key] = shift
        else:
            shift = bar_acc.get(key, sig[upper])
        pitch = base + shift
        if tie and notes and notes[-1].pitch == pitch:
            notes[-1].length += length
        else:
            notes.append(AbcNote(pitch, t, length))
        tie = False
        t += length
        i = m.end()
    return notes, fields


def _top_of_chord(inner: str) -> str:
    """The highest note of a chord written as [CEG] (keeps its length)."""
    found = list(_NOTE_RE.finditer(inner))
    if not found:
        return " "

    def pitch(m: re.Match) -> int:
        letter, octave = m.group(2), m.group(3)
        if letter in "zx":
            return -1
        return _STEP[letter.upper()] + (12 if letter.islower() else 0) + 12 * octave.count("'") - 12 * octave.count(",")

    return max(found, key=pitch).group(0)


def tempo_ms_per_whole(fields: dict[str, str], default_bpm: float = 100.0) -> float:
    """Milliseconds of a whole note from ``Q:`` ("1/4=96" or "96" quarter beats per minute)."""
    q = fields.get("Q", "").strip()
    beat, bpm = Fraction(1, 4), default_bpm
    if q:
        m = re.match(r"^(?:(\d+/\d+)\s*=\s*)?(\d+(?:\.\d+)?)", q)
        if not m:
            raise AbcError(f"cannot read tempo {q!r}")
        if m.group(1):
            beat = Fraction(m.group(1))
        bpm = float(m.group(2))
    return float(60000.0 / bpm / beat)


def to_notes(text: str, default_bpm: float = 100.0) -> list[tuple[float, int, int]]:
    """(MIDI pitch, onset ms, duration ms) of the tune's notes; rests leave gaps."""
    notes, fields = parse(text)
    whole = tempo_ms_per_whole(fields, default_bpm)
    out = []
    for n in notes:
        if n.pitch is None:
            continue
        out.append((float(n.pitch), int(round(float(n.start) * whole)), max(1, int(round(float(n.length) * whole)))))
    return out
