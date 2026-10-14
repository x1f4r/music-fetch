import numpy as np

from melody_index.validate import agreement, candidate_shifts, folded, subseq_dtw


def _phrase(pitches, start=0, dur=300, step=320):
    return {
        "source": "x",
        "start_ms": start,
        "notes": [[float(p), i * step, dur] for i, p in enumerate(pitches)],
    }


TUNE = [60, 62, 64, 60, 60, 62, 64, 60, 64, 65, 67, 64, 65, 67, 67, 69, 67, 65, 64, 60]
OTHER = [70, 69, 70, 72, 74, 72, 70, 69, 67, 65, 67, 69, 70, 72, 70, 69, 67, 66, 67, 62]


def test_folded_distance():
    assert np.allclose(folded(np.array([0.0, 12.0, -12.0, 1.0, 11.0, 6.0])), [0, 0, 0, 1, 1, 6])


def test_shift_candidates_find_transposition():
    q = np.array(TUNE, float) + 5
    assert 5 in candidate_shifts(q, np.array(TUNE, float))


def test_subsequence_dtw_finds_excerpt_with_tempo_change():
    ref = np.repeat(np.array(OTHER + TUNE + OTHER, float), 4)
    q = np.repeat(np.array(TUNE[4:16], float) + 12, 3)  # octave up, 25% faster
    a, cost, k = subseq_dtw(q, ref)
    assert a > 0.95 and cost < 0.2 and k == 12


def test_agreement_separates_same_and_different_melodies():
    midi = [_phrase(OTHER, 0), _phrase(TUNE, 8000), _phrase(OTHER, 16000)]
    same = [_phrase([p + 3 for p in TUNE[:12]], dur=250, step=270)]
    diff = [_phrase([62, 61, 59, 57, 59, 61, 62, 66, 64, 62, 61, 59])]
    a_same = agreement(same, midi)
    a_diff = agreement(diff, midi)
    assert a_same["agreement"] > 0.9 and a_same["shift"] == 3
    assert a_diff["agreement"] < a_same["agreement"]
