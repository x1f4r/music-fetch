"""YouTube as a slow second source of full recordings, behind a strict circuit breaker.

The search and the candidate choice are the full-track stage's (``fulltrack``): a flat search,
then the most plausible upload by title, artist and duration, preferring "- Topic" art tracks.
Every yt-dlp run takes a slot of one ``SourceGuard``; a sign-in prompt or a 429 pauses YouTube
for a day and a second one in a row disables it until ``mfidx refs --reset youtube``. Several
failed downloads in a row count as a block as well. With ``MFIDX_POT_URL`` (a bgutil PO-token
server on localhost) yt-dlp gets proof-of-origin tokens for the web clients.
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
from typing import Any

import numpy as np

from .config import Paths
from .extract import DecodeError, decode_audio
from .fulltrack import Song, score_candidate, ytdlp
from .guard import SourceGuard
from .sources import Pick

log = logging.getLogger("mfidx.youtube")

_BLOCK_RE = re.compile(
    r"sign in to confirm|not a bot|HTTP Error 429|too many requests|try again later|unusual traffic", re.I
)
MAX_FAILURES_IN_A_ROW = 5


class Blocked(Exception):
    pass


class YouTube:
    Error = DecodeError
    Refused = Blocked

    def __init__(self, paths: Paths, per_hour: int = 40, stop=None) -> None:
        # Two yt-dlp runs per song (search, download).
        self.guard = SourceGuard(
            paths.state / "youtube.guard",
            limit=2 * per_hour,
            window=3600.0,
            min_gap=3600.0 / (2 * per_hour) * 0.8,
            jitter=0.3,
            block_pause=24 * 3600.0,
            max_pause=72 * 3600.0,
            max_blocks=2,
            stop=stop,
        )
        self.guards = (self.guard,)
        self.cmd = ytdlp()
        pot = os.environ.get("MFIDX_POT_URL")
        if pot:
            self.cmd += ["--extractor-args", f"youtubepot-bgutilhttp:base_url={pot}"]
        self.failures = 0
        self.stats = {"runs": 0}

    def _run(self, args: list[str], timeout: float, binary: bool = False) -> bytes | str:
        self.guard.acquire()
        self.stats["runs"] += 1
        try:
            p = subprocess.run(self.cmd + args, capture_output=True, timeout=timeout, check=False)
        except subprocess.TimeoutExpired as e:
            self._failed("yt-dlp timed out")
            raise DecodeError("yt-dlp timed out") from e
        err = p.stderr.decode(errors="replace")
        if _BLOCK_RE.search(err):
            self.failures = 0
            self.guard.blocked(err.strip().splitlines()[-1] if err.strip() else "blocked")
            raise Blocked(err[-300:])
        if p.returncode != 0:
            self._failed(err[-200:])
            raise DecodeError(err[-300:] or f"yt-dlp exit {p.returncode}")
        self.failures = 0
        self.guard.ok()
        return p.stdout if binary else p.stdout.decode(errors="replace")

    def _failed(self, why: str) -> None:
        self.failures += 1
        if self.failures >= MAX_FAILURES_IN_A_ROW:
            self.failures = 0
            self.guard.blocked(f"{MAX_FAILURES_IN_A_ROW} failures in a row: {why}")

    def candidates(self, t, n: int = 10) -> list[Pick]:
        """Plausible uploads of a refs target (``fulltrack``'s choice rules), most plausible first."""
        return [
            Pick(f"youtube:{c['id']}", c["title"], c["channel"], c["duration"], s, c["id"])
            for c, s in self.search(Song(t.song_id, t.title, t.artist, t.duration), n)
        ]

    def fetch(self, p: Pick) -> np.ndarray:
        return self.fetch_audio(p.key)

    def search(self, song: Song, n: int = 10) -> list[tuple[dict[str, Any], float]]:
        out = self._run(["--flat-playlist", "-J", f"ytsearch{n}:{song.artist} - {song.title}"], timeout=90)
        data = json.loads(out or "{}")
        results = [
            {
                "id": e.get("id"),
                "title": e.get("title") or "",
                "duration": e.get("duration"),
                "channel": e.get("channel") or e.get("uploader") or "",
            }
            for e in data.get("entries") or []
            if e.get("id")
        ]
        scored = [(score_candidate(c, song), i, c) for i, c in enumerate(results)]
        scored = [(s - 0.01 * i, c) for s, i, c in scored if s > 0]
        return [(c, round(s, 4)) for s, c in sorted(scored, key=lambda t: -t[0])]

    def fetch_audio(self, video_id: str) -> np.ndarray:
        data = self._run(
            ["-f", "bestaudio[abr<=160]/bestaudio", "-o", "-", f"https://www.youtube.com/watch?v={video_id}"],
            timeout=300,
            binary=True,
        )
        if not data:
            raise DecodeError("empty download")
        return decode_audio(data)
