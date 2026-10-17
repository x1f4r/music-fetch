import json
import sqlite3
import time

import numpy as np

from melody_index.guard import SourceDisabled, SourceGuard
from melody_index.refs import ACCEPT, contour, placement, window_offset, window_phrases
from melody_index.soundcloud import Candidate, rank_candidates, score_candidate


def melody(n, seed=0):
    rng = np.random.default_rng(seed)
    steps = rng.choice([-2, -1, 0, 1, 2, 3, -3], size=n)
    return (60 + np.cumsum(steps) % 12).astype(np.float32)


def test_placement_finds_the_preview_inside_the_full_track():
    full = np.repeat(melody(60, 1), 5)  # 30 s of 10 fps contour
    full = np.concatenate([np.repeat(melody(80, 2), 5), full, np.repeat(melody(40, 3), 5)])
    preview = full[400:700].copy()
    preview[::7] = np.nan  # unvoiced frames
    share, at = placement(preview, full)
    assert share > 0.95 and abs(at - 40.0) < 0.05
    # The same melody a few semitones up (a sped-up or pitched upload) does not pass.
    assert placement(preview, full + 2.0)[0] < ACCEPT
    # Another song does not pass either.
    other = np.repeat(melody(400, 9), 5)
    assert placement(preview, other)[0] < ACCEPT


def test_contour_is_10_fps_midi_with_unvoiced_frames_as_nan():
    f0 = np.full(1000, 440.0, np.float32)
    conf = np.ones(1000, np.float32)
    conf[500:] = 0.0
    c = contour(f0, conf)
    assert len(c) == 100 and np.allclose(c[:50], 69.0) and np.isnan(c[50:]).all()


def test_window_offset_places_an_overlapping_window_and_refuses_disjoint_ones():
    song = np.repeat(melody(200, 4), 5)  # 100 s
    deezer, apple = song[300:600], song[420:720]  # Apple's window starts 12 s later
    share, off = window_offset(deezer, apple)
    assert share > 0.95 and abs(off - 12.0) < 0.05
    share, off = window_offset(deezer, song[700:1000])
    assert off is None


def test_window_phrases_drop_what_the_deezer_preview_already_has(tmp_path):
    conn = sqlite3.connect(tmp_path / "s.db")
    conn.row_factory = sqlite3.Row
    from melody_index.refs import SCHEMA

    conn.executescript(SCHEMA)
    phrases = [
        {"source": "preview", "start_ms": 0, "notes": [[60, 0, 500], [62, 500, 500]]},  # inside Deezer's
        {"source": "preview", "start_ms": 20000, "notes": [[64, 0, 500], [65, 500, 500]]},  # after it
    ]
    conn.execute(
        "INSERT INTO window_melody(song_id, status, source, phrases, offset_s, updated_at) VALUES (1, 'ok', 'itunes', ?, 12.0, 0)",
        (json.dumps(phrases),),
    )
    out = window_phrases(conn)[1]
    assert len(out) == 1 and out[0]["start_ms"] == 60000 + 20000


def cand(title, user, full, policy="MONETIZE", isrc=None, protocol="hls", snipped=False, duration=None):
    return Candidate(
        id=1, title=title, user=user, duration=duration or full, full_duration=full, policy=policy, isrc=isrc,
        publisher_artist="", authorization="a",
        transcodings=[{"url": "u", "protocol": protocol, "mime": "audio/mp4", "snipped": snipped, "preset": "aac_96k"}],
        url="",
    )


def test_soundcloud_candidates_reject_snippets_drm_versions_and_wrong_lengths():
    s = ("Never Gonna Give You Up", "Rick Astley", 213, "GBARL0600786")
    assert score_candidate(cand("Rick Astley - Never Gonna Give You Up", "pajlada", 213), *s)[1] == "ok"
    assert score_candidate(cand("Never Gonna Give You Up", "Rick Astley", 213, policy="SNIP", duration=30), *s)[1] == "snippet"
    assert score_candidate(cand("Never Gonna Give You Up", "Rick Astley", 214, protocol="ctr-encrypted-hls"), *s)[1] == "protected"
    assert score_candidate(cand("[Nightcore] Rick Astley - Never Gonna Give You Up", "clouD", 171), *s)[1] in ("version", "duration")
    assert score_candidate(cand("Rick Astley - Never Gonna Give You Up (slowed)", "Madam", 213), *s)[1] == "version"
    assert score_candidate(cand("Rick Astley - Never Gonna Give You Up (Daymaan Remix)", "Daymaan", 213), *s)[1] == "version"
    assert score_candidate(cand("Rick Astley - Never Gonna Give You Up", "Bent Fader", 193), *s)[1] == "duration"
    assert score_candidate(cand("Rick Astley - Together Forever", "x", 213), *s)[1] == "title"
    assert score_candidate(cand("Rick Astley - Never Gonna Give You Up ft. MC Nobody", "x", 213), *s)[1] == "feature"
    # A version word in the artist's own name is fine; an ISRC match ranks first.
    assert score_candidate(cand("Lil Nas X - Old Town Road", "someone", 157), "Old Town Road", "Lil Nas X", 157, None)[1] == "ok"
    ranked = rank_candidates(
        [cand("Rick Astley - Never Gonna Give You Up", "pajlada", 213),
         cand("Never Gonna Give You Up", "Rick Astley", 213, isrc="GBARL0600786")], *s)
    assert ranked[0].isrc == "GBARL0600786"


def test_guard_paces_opens_and_disables(tmp_path):
    g = SourceGuard(tmp_path / "x.guard", limit=3, window=0.5, min_gap=0.0, block_pause=0.2, max_blocks=2)
    t = time.time()
    for _ in range(4):
        g.acquire()
    assert time.time() - t >= 0.45  # the fourth request waited for the window
    assert g.blocked("429") == 0.2
    t = time.time()
    g.acquire()
    assert time.time() - t >= 0.15
    g.ok()
    assert g.blocked("429") == 0.2  # ok() reset the count
    assert g.blocked("429") == float("inf")
    try:
        g.acquire()
        raise AssertionError("disabled guard let a request through")
    except SourceDisabled:
        pass
    # State is shared through the file.
    assert SourceGuard(tmp_path / "x.guard", limit=3, window=0.5).summary()["disabled"]
    g.reset()
    g.acquire()


def test_guard_day_limit_caps_requests_per_24_hours(tmp_path):
    g = SourceGuard(tmp_path / "d.guard", limit=100, window=1.0, day_limit=2)
    g.acquire()
    g.acquire()
    stop_after = time.time()
    # The third request would wait for a day: stop it instead of waiting.
    import threading

    from melody_index.guard import Stopped

    ev = threading.Event()
    g.stop = ev
    threading.Timer(0.2, ev.set).start()
    try:
        g.acquire()
        raise AssertionError("third request in a day went through")
    except Stopped:
        pass
    assert time.time() - stop_after < 5
    assert g.summary()["last_24h"] == 2


def test_youtube_block_signals_and_per_video_refusals():
    from melody_index.youtube import classify

    assert classify("ERROR: [youtube] abc: Sign in to confirm you’re not a bot. Use --cookies") == "block"
    assert classify("ERROR: [youtube] abc: Sign in to confirm you're not a bot") == "block"
    assert classify("ERROR: unable to download video data: HTTP Error 429: Too Many Requests") == "block"
    assert classify("WARNING: [youtube] The current session has been rate-limited by YouTube for up to an hour") == "block"
    assert classify("ERROR: [youtube] abc: Video unavailable. This content isn’t available, try again later.") == "block"
    assert classify("ERROR: [youtube] abc: Sign in to confirm your age. This video may be inappropriate for some users.") == "video"
    assert classify("ERROR: [youtube] abc: Private video. Sign in if you've been granted access to this video") == "video"
    assert classify("ERROR: [youtube] abc: Video unavailable. This video has been removed by the uploader") == "video"
    assert classify("WARNING: [youtube] [pot] Error fetching PO Token: Read timed out") is None
    assert classify("") is None
    # A per-video line does not hide a block signal on another line.
    assert classify("ERROR: Private video\nWARNING: HTTP Error 429: Too Many Requests") == "block"


def test_decoded_duration_must_match_the_song():
    from melody_index.refs import duration_ok

    assert duration_ok(214.0, 213) and duration_ok(226.0, 213)
    assert not duration_ok(171.0, 213)  # sped up
    assert not duration_ok(30.0, 213)  # a snippet
    assert duration_ok(300.0, None)


def _plan(tmp_path, sources=("soundcloud", "dailymotion", "youtube", "itunes")):
    from melody_index import db as dbm
    from melody_index.fulltrack import SCHEMA as TRACK_SCHEMA
    from melody_index.refs import SCHEMA, Plan

    conn = dbm.connect(tmp_path / "b.db")
    conn.executescript(TRACK_SCHEMA)
    conn.executescript(SCHEMA)
    for i, score in ((1, 0.9), (2, 0.8), (3, 0.7)):
        conn.execute("INSERT INTO songs(id, key, title, artist, score) VALUES (?, ?, ?, 'A', ?)", (i, f"k{i}", f"T{i}", score))
        conn.execute("INSERT INTO deezer(song_id, status, duration) VALUES (?, 'ok', 200)", (i,))
    return conn, Plan(conn, 10, sources)


def test_plan_hands_a_song_on_after_a_verdict_or_an_error(tmp_path):
    conn, plan = _plan(tmp_path)
    t1 = plan.next("soundcloud")
    assert t1.song_id == 1
    assert plan.next("dailymotion") is None  # nothing has failed on SoundCloud yet
    plan.done(t1, "soundcloud", "error")
    d1 = plan.next("dailymotion")
    assert d1.song_id == 1  # an error hands the song on
    t2 = plan.next("soundcloud")
    plan.done(t2, "soundcloud", "ok")
    assert not any(plan.eligible(t2, s) for s in ("soundcloud", "dailymotion", "youtube", "itunes"))
    # YouTube waits for Dailymotion; iTunes waits only for SoundCloud and Dailymotion.
    assert not plan.eligible(t1, "youtube") and not plan.eligible(t1, "itunes")
    plan.done(d1, "dailymotion", "mismatch")
    assert plan.eligible(t1, "youtube") and plan.eligible(t1, "itunes")


def test_plan_refresh_adds_new_songs(tmp_path):
    conn, plan = _plan(tmp_path)
    conn.execute("INSERT INTO songs(id, key, title, artist, score, origin) VALUES (4, 'k4', 'T4', 'B', 0.95, 'evergreen')")
    conn.execute("INSERT INTO deezer(song_id, status, duration) VALUES (4, 'ok', 100)")
    assert plan.refresh(conn) == 1
    assert plan.next("soundcloud").song_id == 4  # most popular first
