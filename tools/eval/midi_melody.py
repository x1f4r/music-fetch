"""MIDI to monophonic melody notes for the melody-index builder input.

`melody_notes(path, pick_track)` returns `[[pitch, onset_ms, dur_ms], ...]` (sorted,
monophonic). With `pick_track=False` every non-drum note is merged (ground-truth
MIDIs are monophonic already); with `pick_track=True` the most melody-like track is
chosen first with a crude heuristic (lyric events, track name, monophony, register,
note density, General MIDI program). Overlaps are resolved by the skyline rule
(highest pitch wins).
"""

import re
import statistics

import mido

POSITIVE = re.compile(r"melod|vocal|voice|vox|lead|sing|canto|stimme|melodie|theme|tune", re.I)
NEGATIVE = re.compile(r"bass|drum|perc|pad|chord|string|harm|backing|bvox|choir|accomp|rhythm|kick|snare|hat|cymbal", re.I)
# Voice, solo instruments, brass, reeds, pipes, synth leads.
LEAD_PROGRAMS = set(range(52, 55)) | set(range(56, 80)) | set(range(80, 88))


def _tempo_map(mid):
    """[(tick, microseconds per beat)] from all tracks."""
    changes = []
    for track in mid.tracks:
        tick = 0
        for msg in track:
            tick += msg.time
            if msg.type == "set_tempo":
                changes.append((tick, msg.tempo))
    changes.sort()
    if not changes or changes[0][0] != 0:
        changes.insert(0, (0, 500000))
    return changes


def _tick_to_ms(changes, tpb):
    starts = []
    ms = 0.0
    for i, (tick, tempo) in enumerate(changes):
        if i:
            prev_tick, prev_tempo = changes[i - 1]
            ms += (tick - prev_tick) * prev_tempo / tpb / 1000.0
        starts.append((tick, ms, tempo))

    def convert(t):
        lo, hi = 0, len(starts) - 1
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if starts[mid][0] <= t:
                lo = mid
            else:
                hi = mid - 1
        tick, base, tempo = starts[lo]
        return base + (t - tick) * tempo / tpb / 1000.0

    return convert


def _track_notes(track, convert):
    """Notes of one track as (onset_ms, end_ms, pitch, channel); plus program and lyric count."""
    tick = 0
    active = {}
    notes = []
    programs = []
    lyrics = 0
    for msg in track:
        tick += msg.time
        if msg.type in ("lyrics",) or (msg.type == "text" and msg.text and not msg.text.startswith("@")):
            lyrics += 1
        if msg.type == "program_change":
            programs.append(msg.program)
        if msg.type == "note_on" and msg.velocity > 0:
            key = (msg.channel, msg.note)
            if key in active:
                start = active.pop(key)
                notes.append((convert(start), convert(tick), msg.note, msg.channel))
            active[key] = tick
        elif msg.type == "note_off" or (msg.type == "note_on" and msg.velocity == 0):
            key = (msg.channel, msg.note)
            if key in active:
                start = active.pop(key)
                notes.append((convert(start), convert(tick), msg.note, msg.channel))
    return notes, programs, lyrics


def _skyline(notes):
    """Monophonic sequence: at every onset the highest sounding note wins; a note ends at the next onset."""
    if not notes:
        return []
    notes = sorted(notes, key=lambda n: (n[0], -n[2]))
    out = []
    for onset, end, pitch, _ in notes:
        if out and abs(out[-1][0] - onset) < 1.0:
            continue  # same onset: the higher note came first
        if out and out[-1][0] + out[-1][2] > onset:
            prev = out[-1]
            if pitch < prev[1] and end <= prev[0] + prev[2]:
                continue  # a lower note inside a sounding higher one
            out[-1] = (prev[0], prev[1], max(onset - prev[0], 1.0))
        out.append((onset, pitch, max(end - onset, 1.0)))
    return [[float(p), round(o, 1), round(d, 1)] for o, p, d in out if d >= 30.0]


def _monophony(notes):
    if not notes:
        return 0.0
    events = sorted([(n[0], 1) for n in notes] + [(n[1], -1) for n in notes])
    level = 0
    last = events[0][0]
    poly = total = 0.0
    for t, d in events:
        if level > 0:
            total += t - last
            if level > 1:
                poly += t - last
        level += d
        last = t
    return 1.0 - poly / total if total else 0.0


def _score(name, notes, programs, lyrics, song_ms):
    if not notes or all(n[3] == 9 for n in notes):
        return None
    notes = [n for n in notes if n[3] != 9]
    if len(notes) < 20:
        return None
    pitches = [n[2] for n in notes]
    med = statistics.median(pitches)
    span = max(n[1] for n in notes) - min(n[0] for n in notes)
    active = sum(n[1] - n[0] for n in notes) / max(song_ms, 1.0)
    density = len(notes) / max(span / 1000.0, 1.0)
    p5, p95 = sorted(pitches)[len(pitches) // 20], sorted(pitches)[-1 - len(pitches) // 20]
    score = 0.0
    if lyrics >= 10:
        score += 4
    if name and POSITIVE.search(name):
        score += 3
    if name and NEGATIVE.search(name):
        score -= 5
    if programs and programs[0] in LEAD_PROGRAMS:
        score += 1
    if programs and programs[0] in range(32, 40):  # basses
        score -= 3
    mono = _monophony(notes)
    score += 2 if mono >= 0.9 else 1 if mono >= 0.8 else -2
    score += 1 if 55 <= med <= 84 else -2
    score += 1 if p95 - p5 <= 24 else -1
    score += 1 if 1.0 <= density <= 8.0 else -1
    score += 1 if 0.2 <= active <= 0.9 else 0
    return score


def melody_notes(path, pick_track=True, max_ms=360_000.0):
    mid = mido.MidiFile(path, clip=True)
    if mid.type == 2 or mid.ticks_per_beat <= 0:
        return []
    convert = _tick_to_ms(_tempo_map(mid), mid.ticks_per_beat)
    tracks = []
    for track in mid.tracks:
        notes, programs, lyrics = _track_notes(track, convert)
        tracks.append((track.name, notes, programs, lyrics))
    every = [n for _, notes, _, _ in tracks for n in notes if n[3] != 9]
    if not every:
        return []
    song_ms = max(n[1] for n in every)
    if pick_track:
        # Type-0 files keep all channels in one track: split them by channel.
        if len(tracks) == 1 or sum(1 for t in tracks if t[1]) == 1:
            only = next(t for t in tracks if t[1])
            channels = sorted({n[3] for n in only[1]})
            tracks = [(only[0], [n for n in only[1] if n[3] == c], [], only[3]) for c in channels]
        scored = []
        for name, notes, programs, lyrics in tracks:
            s = _score(name, notes, programs, lyrics, song_ms)
            if s is not None:
                scored.append((s, name, notes))
        if not scored:
            return []
        scored.sort(key=lambda x: -x[0])
        if scored[0][0] < 3:
            return []
        chosen = [n for n in scored[0][2] if n[3] != 9]
    else:
        chosen = every
    chosen = [n for n in chosen if n[0] < max_ms]
    return _skyline(chosen)
