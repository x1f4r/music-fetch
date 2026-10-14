import json

import numpy as np

from melody_index.evalset import HumParams, excerpt, expected_key, parse_youtube_title, render


def _rec():
    phrases = [
        {
            "source": "preview",
            "start_ms": s,
            "notes": [[60.0 + i % 5, i * 400, 350] for i in range(12)],
        }
        for s in (0, 6000, 12000)
    ]
    return {"title": "Song", "artist": "Band", "deezer_id": 7, "phrases": phrases}


def test_excerpt_spans_eight_to_fourteen_seconds():
    rng = np.random.default_rng(0)
    for _ in range(20):
        meta, notes = excerpt(_rec(), rng)
        span = notes[-1][1] + notes[-1][2] - notes[0][1]
        assert 6000 <= span <= 14000
        assert meta["source"] == "preview" and len(notes) >= 6


def test_render_is_hum_like_and_bounded():
    rng = np.random.default_rng(1)
    prm = HumParams(
        key_shift=2.0,
        tempo=1.1,
        jitter_cents=30,
        drop_prob=0.05,
        waveform="triangle",
        vibrato_rate=5.5,
        vibrato_cents=30,
        snr_db=20,
        rt60=0.3,
        octave=0,
    )
    audio = render([(57.0, 0.0, 0.4), (59.0, 0.45, 0.4), (60.0, 0.9, 0.8)], prm, rng)
    assert audio.dtype == np.float32
    assert np.max(np.abs(audio)) <= 1.0
    assert 1.5 < len(audio) / 16000 < 3.5
    spec = np.abs(np.fft.rfft(audio[int(0.5 * 16000) : int(0.8 * 16000)]))
    peak_hz = np.argmax(spec) * 16000 / (2 * (len(spec) - 1))
    assert 200 < peak_hz < 280  # around B3 (247 Hz)


def test_expected_key_and_youtube_titles():
    assert expected_key({"deezer_id": 5, "artist": "A", "title": "B"}) == "deezer:5"
    assert (
        expected_key({"deezer_id": None, "artist": "The Band", "title": "Song (Live)"})
        == "key:band::song"
    )
    assert parse_youtube_title("Adele - Hello (Official Music Video)", "AdeleVEVO") == (
        "Adele",
        "Hello",
    )
    assert parse_youtube_title("Shape of You", "Ed Sheeran - Topic") == (
        "Ed Sheeran",
        "Shape of You",
    )
    json.dumps(parse_youtube_title("", ""))
    assert parse_youtube_title("BTS (방탄소년단) 'Dynamite' Official MV", "HYBE LABELS") == (
        "BTS (방탄소년단)",
        "Dynamite",
    )
    assert parse_youtube_title("Joe_Cocker_-_You_Can_Leave_Your_Hat_On_(HQ).mp4", "x") == (
        "Joe Cocker",
        "You Can Leave Your Hat On",
    )
