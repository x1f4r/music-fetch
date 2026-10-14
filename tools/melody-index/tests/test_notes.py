import numpy as np

from melody_index.notes import (
    Note,
    NoteParams,
    Phrase,
    dedupe_phrases,
    f0_to_notes,
    fix_octaves,
    make_monophonic,
    midi_to_hz,
    notes_to_contour,
    phrases_from_notes,
    runs,
    salience_dips,
)


def track(notes, gap_frames=0, vibrato=0.0, jitter=0.0, seed=0):
    """f0 (Hz) at 10 ms frames for (midi, frames) notes, with optional vibrato (semitones)."""
    rng = np.random.default_rng(seed)
    out = []
    for p, n in notes:
        t = np.arange(n) / 100.0
        semis = p + vibrato * np.sin(2 * np.pi * 5.5 * t) + rng.normal(0, jitter, n)
        out.append(midi_to_hz(semis))
        out.append(np.zeros(gap_frames))
    return np.concatenate(out)


def test_runs():
    assert runs(np.array([0, 1, 1, 0, 1], bool)) == [(1, 3), (4, 5)]
    assert runs(np.zeros(3, bool)) == []


def test_clean_melody_is_recovered_exactly():
    melody = [(60, 30), (62, 30), (64, 30), (65, 30), (67, 60), (65, 20), (64, 30)]
    notes = f0_to_notes(track(melody))
    assert [round(n.pitch) for n in notes] == [p for p, _ in melody]
    assert notes[0].onset_ms == 0
    assert notes[4].dur_ms == 600


def test_vibrato_and_jitter_do_not_split_notes():
    melody = [(69, 80), (71, 80), (72, 100)]
    notes = f0_to_notes(track(melody, vibrato=0.35, jitter=0.08, seed=1))
    assert [round(n.pitch) for n in notes] == [69, 71, 72]


def test_repeated_notes_split_by_gaps():
    melody = [(64, 25), (64, 25), (64, 25)]
    notes = f0_to_notes(track(melody, gap_frames=6))
    assert len(notes) == 3


def test_short_blips_and_octave_errors_are_removed():
    f0 = track([(62, 40), (74, 4), (62, 40)])  # 40 ms octave jump inside a held note
    notes = f0_to_notes(f0)
    assert [round(n.pitch) for n in notes] == [62]
    assert notes[0].dur_ms == 840


def test_octave_folding_against_local_median():
    p = np.full(100, 60.0)
    p[40:52] = 72.0
    fixed = fix_octaves(p, 50)
    assert np.allclose(fixed, 60.0)


def test_low_confidence_frames_are_unvoiced():
    f0 = track([(60, 50), (62, 50)])
    conf = np.full(len(f0), 0.9)
    conf[50:] = 0.01
    notes = f0_to_notes(f0, conf, NoteParams(voicing=0.04))
    assert [round(n.pitch) for n in notes] == [60]


def test_phrases_split_at_rests_and_need_six_notes():
    ns = [Note(60 + i, i * 300, 250) for i in range(6)]
    ns += [Note(70, 6 * 300 + 1000 + i * 300, 250) for i in range(3)]
    ph = phrases_from_notes(ns)
    assert len(ph) == 1 and len(ph[0].notes) == 6
    js = ph[0].to_json("preview")
    assert js["start_ms"] == 0 and js["notes"][1] == [61.0, 300, 250]


def test_phrase_onsets_are_relative_and_monophonic():
    ns = [Note(60, 1000 + i * 200, 300) for i in range(8)]  # overlapping
    ph = phrases_from_notes(ns)[0].to_json("midi")
    assert ph["start_ms"] == 1000
    for (_, o1, d1), (_, o2, _) in zip(ph["notes"], ph["notes"][1:], strict=False):
        assert o1 + d1 <= o2


def test_long_phrases_are_split():
    ns = [Note(60 + (i % 5), i * 400, 380) for i in range(80)]  # 32 s without a rest
    ph = phrases_from_notes(ns, NoteParams(max_phrase_ms=20000))
    assert len(ph) >= 2
    assert all(p.notes[-1].end_ms - p.notes[0].onset_ms <= 20000 for p in ph)


def test_skyline_keeps_highest_at_same_onset():
    ns = make_monophonic([Note(60, 0, 500), Note(67, 0, 500), Note(64, 400, 300)])
    assert [(n.pitch, n.onset_ms, n.dur_ms) for n in ns] == [(67, 0, 400), (64, 400, 300)]


def test_dedupe_identical_phrases():
    a = Phrase(0, [Note(60 + i, i * 100, 90) for i in range(6)])
    b = Phrase(5000, [Note(60.2 + i, 5000 + i * 120, 90) for i in range(6)])
    c = Phrase(9000, [Note(65 + i, 9000 + i * 100, 90) for i in range(6)])
    assert len(dedupe_phrases([a, b, c])) == 2


def test_contour_rendering():
    c = notes_to_contour([(60.0, 0, 100), (62.0, 200, 100)], fps=20)
    assert c[0] == 60.0 and c[1] == 60.0 and np.isnan(c[2]) and c[4] == 62.0


def test_salience_dip_splits_repeated_notes():
    f0 = track([(60, 50), (60, 50), (67, 50)])
    conf = np.full(len(f0), 0.9)
    conf[49] = 0.3  # re-attack: the network keeps f0 continuous but confidence dips
    notes = f0_to_notes(f0, conf)
    assert [round(n.pitch) for n in notes] == [60, 60, 67]
    assert notes[0].dur_ms == 490 and notes[1].onset_ms == 500


def test_salience_dips_ignore_smooth_changes():
    conf = np.linspace(0.9, 0.5, 50)
    assert not salience_dips(conf, 0.5, 5).any()


def test_far_outliers_are_dropped_from_phrases():
    ns = [Note(67 + (i % 3), i * 300, 250) for i in range(8)] + [Note(38, 8 * 300, 250)]
    ph = phrases_from_notes(ns)
    assert len(ph) == 1 and all(n.pitch > 60 for n in ph[0].notes)
