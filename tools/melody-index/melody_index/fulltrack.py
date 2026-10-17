"""Stage 3b: full-track melodies for the most popular songs (GPU).

Previews cover about 30 s of a song, so people humming a verse or the intro
often hum something the index has never seen. For the most popular songs this
stage finds the studio recording on YouTube (flat search, then a strict
title/artist/duration check that prefers "- Topic" art tracks), streams the
audio through ffmpeg into memory, runs RMVPE and stores only the f0 track and
confidences, like the preview stage. Audio never touches the disk.

YouTube tolerates a few hundred videos per hour from one address, so downloads
are paced (``--interval``, default 12.5 s) and the stage backs off for half an
hour when YouTube asks for a sign-in or rate-limits.

``eval_reference`` holds the same data for evaluation videos (the original
videos the CHAD hums refer to). It only serves to measure how much of a hummed
section the index covers and is never exported.
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from . import db as dbm
from .config import Paths
from .extract import DecodeError, decode_audio, pack
from .normalize import NON_ORIGINAL_RE, artist_similarity, fold, title_similarity
from .notes import DEFAULT_PARAMS, dedupe_phrases, f0_to_notes, phrases_from_notes, voiced_ratio

log = logging.getLogger("mfidx.fulltrack")

SCHEMA = """
CREATE TABLE IF NOT EXISTS track_melody (
    song_id      INTEGER PRIMARY KEY,
    status       TEXT NOT NULL,
    video_id     TEXT,
    video_title  TEXT,
    channel      TEXT,
    duration     REAL,
    match_score  REAL,
    f0           BLOB,
    conf         BLOB,
    n_frames     INTEGER,
    phrases      TEXT,
    n_phrases    INTEGER,
    n_notes      INTEGER,
    voiced_ratio REAL,
    attempts     INTEGER NOT NULL DEFAULT 0,
    error        TEXT,
    updated_at   REAL
);
CREATE TABLE IF NOT EXISTS eval_reference (
    ref_id     TEXT PRIMARY KEY,
    video_id   TEXT NOT NULL,
    status     TEXT NOT NULL,
    duration   REAL,
    f0         BLOB,
    conf       BLOB,
    n_frames   INTEGER,
    attempts   INTEGER NOT NULL DEFAULT 0,
    error      TEXT,
    updated_at REAL
);
"""

F0_FLOOR = 0.01
CHUNK_S = 30.72  # RMVPE input chunk (a multiple of 32 frames of 10 ms)
CONTEXT_S = 1.28  # extra audio on each side of a chunk, discarded after inference
MAX_TRACK_S = 900.0
# Video titles that are not the studio recording.
_BAD_VIDEO_RE = re.compile(
    r"\b(?:live|en vivo|ao vivo|concert|tour|cover|karaoke|instrumental|remix|sped[ -]?up|slowed|reverb|"
    r"8d|nightcore|1 hour|10 hours|loop|reaction|tutorial|lesson|piano|guitar|drum|bass boosted|"
    r"extended|mashup|medley|reprise|acapella|a cappella|teaser|trailer|snippet|preview|shorts?)\b",
    re.I,
)


class Blocked(Exception):
    """YouTube asked for a sign-in or rate-limited this address."""


@dataclass
class Song:
    song_id: int
    title: str
    artist: str
    duration: float | None


def ytdlp() -> list[str]:
    """The yt-dlp command (``MFIDX_YTDLP``), with a JS runtime when only Node is installed."""
    cmd = [os.environ.get("MFIDX_YTDLP", "yt-dlp"), "--no-update", "--quiet", "--no-warnings"]
    if shutil.which("deno") is None and shutil.which("node") is not None:
        cmd += ["--js-runtimes", "node"]
    return cmd


def _run(args: list[str], timeout: float, binary: bool = False) -> bytes | str:
    try:
        p = subprocess.run(ytdlp() + args, capture_output=True, timeout=timeout, check=False)
    except subprocess.TimeoutExpired as e:
        raise DecodeError("yt-dlp timed out") from e
    err = p.stderr.decode(errors="replace")
    if p.returncode != 0:
        if re.search(r"sign in to confirm|not a bot|HTTP Error 429|too many requests", err, re.I):
            raise Blocked(err[-300:])
        raise DecodeError(err[-300:] or f"yt-dlp exit {p.returncode}")
    return p.stdout if binary else p.stdout.decode(errors="replace")


def search(query: str, n: int = 10) -> list[dict[str, Any]]:
    """Flat YouTube search results: id, title, duration, channel."""
    out = _run(["--flat-playlist", "-J", f"ytsearch{n}:{query}"], timeout=60)
    data = json.loads(out or "{}")
    return [
        {
            "id": e.get("id"),
            "title": e.get("title") or "",
            "duration": e.get("duration"),
            "channel": e.get("channel") or e.get("uploader") or "",
        }
        for e in data.get("entries") or []
        if e.get("id")
    ]


def score_candidate(c: dict[str, Any], song: Song) -> float:
    """0..1 plausibility that a search result is the song's studio recording; 0 = reject."""
    title, channel = c["title"], c["channel"]
    topic = channel.endswith(" - Topic")
    # Art tracks carry the bare title; other uploads usually "Artist - Title (…)".
    shown = title
    if not topic and " - " in title:
        head, _, tail = title.partition(" - ")
        if artist_similarity(head, song.artist) >= 0.8:
            shown = tail
    ts = title_similarity(shown, song.title)
    if ts < 0.75:
        return 0.0
    artist_src = channel.removesuffix(" - Topic").removesuffix("VEVO")
    a = max(artist_similarity(artist_src, song.artist), artist_similarity(title, song.artist))
    if a < 0.6 and fold(song.artist) not in fold(title + " " + channel):
        return 0.0
    bad = _BAD_VIDEO_RE.search(title) or NON_ORIGINAL_RE.search(title)
    if bad and not re.search(re.escape(bad.group(0)), song.title, re.I):
        return 0.0
    dur = c.get("duration")
    if not dur or dur > MAX_TRACK_S:
        return 0.0
    if song.duration:
        diff = abs(dur - song.duration)
        if diff > max(12.0, 0.08 * song.duration):
            return 0.0
        dscore = 1.0 - diff / max(12.0, 0.08 * song.duration)
    else:
        dscore = 0.5
    return 0.4 * ts + 0.25 * min(1.0, a) + 0.2 * dscore + (0.15 if topic else 0.05 if "vevo" in channel.lower() else 0)


def choose(results: list[dict[str, Any]], song: Song) -> tuple[dict[str, Any], float] | None:
    scored = [(score_candidate(c, song), i, c) for i, c in enumerate(results)]
    scored = [s for s in scored if s[0] > 0]
    if not scored:
        return None
    # Earlier search results break near-ties.
    s, _, c = max(scored, key=lambda t: (t[0] - 0.01 * t[1], -t[1]))
    return c, s


def download_audio(video_id: str) -> np.ndarray:
    """Best audio stream of a video, decoded in memory to 16 kHz mono."""
    data = _run(
        ["-f", "bestaudio[abr<=160]/bestaudio", "-o", "-", f"https://www.youtube.com/watch?v={video_id}"],
        timeout=300,
        binary=True,
    )
    if not data:
        raise DecodeError("empty download")
    return decode_audio(data)


def track_f0(model, audio: np.ndarray, sr: int = 16000) -> tuple[np.ndarray, np.ndarray]:
    """RMVPE f0 and confidence at 10 ms for audio of any length, in overlapping chunks."""
    hop = sr // 100
    n_frames = len(audio) // hop + 1
    chunk, ctx = int(CHUNK_S * sr), int(CONTEXT_S * sr)
    f0 = np.zeros(n_frames, np.float32)
    conf = np.zeros(n_frames, np.float32)
    starts = list(range(0, len(audio), chunk))
    pieces = [audio[max(0, s - ctx) : s + chunk + ctx] for s in starts]
    for i in range(0, len(pieces), 4):
        sal = model.salience(pieces[i : i + 4])
        for s, piece_sal in zip(starts[i : i + 4], sal, strict=True):
            pf0, pconf = model.decode(piece_sal, F0_FLOOR)
            skip = (s - max(0, s - ctx)) // hop
            a = s // hop
            n = min(chunk // hop, n_frames - a, len(pf0) - skip)
            if n > 0:
                f0[a : a + n] = pf0[skip : skip + n]
                conf[a : a + n] = pconf[skip : skip + n]
    return f0, conf


class Pacer:
    """At least ``interval`` seconds (±20 % jitter) between downloads; long pauses when blocked."""

    def __init__(self, interval: float) -> None:
        self.interval = interval
        self.last = 0.0
        self.blocked = 0

    def wait(self) -> None:
        gap = self.interval * random.uniform(0.8, 1.2)
        delay = self.last + gap - time.time()
        if delay > 0:
            time.sleep(delay)
        self.last = time.time()

    def backoff(self) -> None:
        self.blocked += 1
        pause = min(4 * 3600, 1800 * 2 ** (self.blocked - 1))
        log.warning("YouTube blocked this address; pausing %.0f min", pause / 60)
        time.sleep(pause)

    def ok(self) -> None:
        self.blocked = 0


def _connect(paths: Paths):
    conn = dbm.connect(paths.db)
    conn.executescript(SCHEMA)
    return conn


def pending(conn, top: int) -> list[Song]:
    rows = conn.execute(
        "SELECT s.id, s.title, s.artist, d.duration FROM songs s JOIN deezer d ON d.song_id=s.id"
        " WHERE s.in_universe=1 AND d.status='ok' ORDER BY s.score DESC, s.id LIMIT ?",
        (top,),
    ).fetchall()
    done = {
        r["song_id"]
        for r in conn.execute(
            "SELECT song_id FROM track_melody WHERE status IN ('ok', 'empty', 'no_match') OR attempts >= 3"
        )
    }
    return [Song(r["id"], r["title"], r["artist"], r["duration"]) for r in rows if r["id"] not in done]


def _store(conn, song_id: int, **cols: Any) -> None:
    row = {
        "song_id": song_id,
        "status": "error",
        "video_id": None,
        "video_title": None,
        "channel": None,
        "duration": None,
        "match_score": None,
        "f0": None,
        "conf": None,
        "n_frames": None,
        "phrases": None,
        "n_phrases": 0,
        "n_notes": 0,
        "voiced_ratio": None,
        "error": None,
        "updated_at": time.time(),
    }
    row.update(cols)
    conn.execute(
        "INSERT INTO track_melody(song_id, status, video_id, video_title, channel, duration, match_score, f0, conf,"
        " n_frames, phrases, n_phrases, n_notes, voiced_ratio, attempts, error, updated_at) VALUES (:song_id,"
        " :status, :video_id, :video_title, :channel, :duration, :match_score, :f0, :conf, :n_frames, :phrases,"
        " :n_phrases, :n_notes, :voiced_ratio, 1, :error, :updated_at) ON CONFLICT(song_id) DO UPDATE SET"
        " status=excluded.status, video_id=excluded.video_id, video_title=excluded.video_title,"
        " channel=excluded.channel, duration=excluded.duration, match_score=excluded.match_score, f0=excluded.f0,"
        " conf=excluded.conf, n_frames=excluded.n_frames, phrases=excluded.phrases, n_phrases=excluded.n_phrases,"
        " n_notes=excluded.n_notes, voiced_ratio=excluded.voiced_ratio, attempts=track_melody.attempts+1,"
        " error=excluded.error, updated_at=excluded.updated_at",
        row,
    )


def run(paths: Paths, top: int = 3000, interval: float = 12.5, limit: int | None = None) -> dict[str, Any]:
    """Full-track f0 for the ``top`` most popular resolved songs (resumable)."""
    from .rmvpe import Rmvpe

    conn = _connect(paths)
    songs = pending(conn, top)
    if limit is not None:
        songs = songs[:limit]
    log.info("fulltrack: %d songs to do in the top %d", len(songs), top)
    model = Rmvpe(paths.rmvpe_weights)
    pacer = Pacer(interval)
    counts: dict[str, int] = {}
    t0 = time.time()
    for i, song in enumerate(songs):
        status = _one(conn, model, pacer, song)
        counts[status] = counts.get(status, 0) + 1
        if (i + 1) % 25 == 0:
            rate = (i + 1) / max(1e-6, time.time() - t0) * 3600
            log.info("fulltrack: %d/%d %s, %.0f songs/h", i + 1, len(songs), counts, rate)
    return {"songs": len(songs), **counts}


def _one(conn, model, pacer: Pacer, song: Song) -> str:
    while True:
        try:
            results = search(f"{song.artist} - {song.title}")
            picked = choose(results, song)
            if picked is None:
                _store(conn, song.song_id, status="no_match", error=f"{len(results)} results, none plausible")
                return "no_match"
            c, s = picked
            pacer.wait()
            audio = download_audio(c["id"])
            pacer.ok()
            f0, conf = track_f0(model, audio)
            del audio
            notes = f0_to_notes(f0, conf, DEFAULT_PARAMS)
            phrases = [p.to_json("audio") for p in dedupe_phrases(phrases_from_notes(notes, DEFAULT_PARAMS))]
            _store(
                conn,
                song.song_id,
                status="ok" if phrases else "empty",
                video_id=c["id"],
                video_title=c["title"][:300],
                channel=c["channel"][:200],
                duration=c["duration"],
                match_score=round(s, 3),
                f0=pack(f0, "<f2"),
                conf=pack(np.round(conf * 255).clip(0, 255), "u1"),
                n_frames=int(len(f0)),
                phrases=json.dumps(phrases, separators=(",", ":")),
                n_phrases=len(phrases),
                n_notes=sum(len(p["notes"]) for p in phrases),
                voiced_ratio=round(voiced_ratio(f0, conf, DEFAULT_PARAMS.voicing), 4),
            )
            return "ok" if phrases else "empty"
        except Blocked:
            pacer.backoff()
        except (DecodeError, json.JSONDecodeError) as e:
            _store(conn, song.song_id, status="error", error=str(e)[:300])
            return "error"
        except Exception as e:  # noqa: BLE001 - keep the stage alive
            log.exception("fulltrack %s failed", song.song_id)
            _store(conn, song.song_id, status="error", error=f"{type(e).__name__}: {e}"[:300])
            return "error"


def run_eval_references(paths: Paths, groups: Path, interval: float = 12.5) -> dict[str, Any]:
    """f0 of evaluation videos (``groups``: JSON object of {ref_id: {"youtube_id": …}}). Never exported."""
    from .rmvpe import Rmvpe

    conn = _connect(paths)
    items = json.loads(groups.read_text())
    done = {
        r["ref_id"]
        for r in conn.execute("SELECT ref_id FROM eval_reference WHERE status='ok' OR attempts >= 3")
    }
    todo = [(k, v["youtube_id"]) for k, v in items.items() if k not in done and v.get("youtube_id")]
    log.info("eval references: %d to do", len(todo))
    model = Rmvpe(paths.rmvpe_weights)
    pacer = Pacer(interval)
    counts: dict[str, int] = {}
    for ref_id, vid in todo:
        while True:
            try:
                pacer.wait()
                audio = download_audio(vid)
                pacer.ok()
                f0, conf = track_f0(model, audio)
                row = ("ok", len(audio) / 16000, pack(f0, "<f2"), pack(np.round(conf * 255).clip(0, 255), "u1"), len(f0), None)
                break
            except Blocked:
                pacer.backoff()
            except Exception as e:  # noqa: BLE001
                row = ("error", None, None, None, None, f"{type(e).__name__}: {e}"[:300])
                break
        conn.execute(
            "INSERT INTO eval_reference(ref_id, video_id, status, duration, f0, conf, n_frames, attempts, error,"
            " updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?) ON CONFLICT(ref_id) DO UPDATE SET"
            " status=excluded.status, duration=excluded.duration, f0=excluded.f0, conf=excluded.conf,"
            " n_frames=excluded.n_frames, attempts=eval_reference.attempts+1, error=excluded.error,"
            " updated_at=excluded.updated_at",
            (ref_id, vid, row[0], row[1], row[2], row[3], row[4], row[5], time.time()),
        )
        counts[row[0]] = counts.get(row[0], 0) + 1
    return {"references": len(todo), **counts}
