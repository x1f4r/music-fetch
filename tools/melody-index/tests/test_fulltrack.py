import numpy as np

from melody_index.fulltrack import Song, choose, score_candidate, track_f0


def cand(title, channel, duration, vid="x"):
    return {"id": vid, "title": title, "channel": channel, "duration": duration}


SONG = Song(1, "Hello", "Adele", 296)


def test_prefers_the_art_track_over_live_and_lyric_uploads():
    results = [
        cand("Adele - Hello (Official Music Video)", "Adele", 367, "video"),
        cand("Adele - Hello (Lyrics)", "Rare Vibes", 290, "lyrics"),
        cand("Hello", "Adele - Topic", 296, "topic"),
        cand("Adele - Hello (Live at the NRJ Awards)", "Adele", 308, "live"),
    ]
    picked, score = choose(results, SONG)
    assert picked["id"] == "topic"
    assert 0 < score <= 1
    # A music video with a long intro is too far from the recording's length.
    assert score_candidate(results[0], SONG) == 0.0
    assert score_candidate(results[3], SONG) == 0.0


def test_rejects_other_songs_covers_and_wrong_lengths():
    assert score_candidate(cand("Adele - Someone Like You", "Adele - Topic", 296), SONG) == 0.0
    assert score_candidate(cand("Hello (Piano Cover)", "Some Pianist", 296), SONG) == 0.0
    assert score_candidate(cand("Hello", "Lionel Richie - Topic", 296), SONG) == 0.0
    assert score_candidate(cand("Hello", "Adele - Topic", None), SONG) == 0.0
    assert choose([], SONG) is None
    # A version word that is part of the title itself is fine.
    acoustic = Song(2, "Eastside (Acoustic)", "benny blanco", 180)
    assert score_candidate(cand("Eastside (Acoustic)", "benny blanco - Topic", 181), acoustic) > 0


class FakeModel:
    """Salience that encodes each input sample's value, to check chunk stitching."""

    def salience(self, audios):
        return [np.repeat(a[::160][:, None], 360, axis=1)[: len(a) // 160 + 1] for a in audios]

    def decode(self, salience, threshold):
        v = salience[:, 0].astype(np.float32)
        return v, np.ones_like(v)


def test_long_audio_is_stitched_from_chunks_without_gaps():
    sr = 16000
    audio = np.repeat(np.arange(7000, dtype=np.float32), 160)  # 70 s, value = frame index
    f0, conf = track_f0(FakeModel(), audio, sr)
    assert len(f0) == len(audio) // 160 + 1
    assert np.array_equal(f0[:-1], np.arange(7000, dtype=np.float32))
    assert conf[:-1].min() == 1.0
