"""YouTube as the last source of full recordings, slow and behind a strict circuit breaker.

The search and the candidate choice are the full-track stage's (``fulltrack``): a flat search,
then the most plausible upload by title, artist and duration, preferring "- Topic" art tracks.

Every yt-dlp run (a search or a download) takes a slot of one ``SourceGuard``: on average one
run per ``gap`` seconds (default 30, ±30 % jitter, so at most one download a minute) and at most
``per_day`` runs in any 24 hours. The first bot check, sign-in prompt, 429 or rate-limit notice
pauses YouTube for a day; another one right after the pause disables it until
``mfidx refs --reset youtube``. Five failed runs in a row count as a block too. Age-gated and
private videos are per-video errors, not blocks. yt-dlp's warnings are kept (only ``--quiet``)
so that a rate-limit warning is seen. With ``MFIDX_POT_URL`` (a bgutil PO-token server on
localhost) yt-dlp gets proof-of-origin tokens for the web clients.
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
from .normalize import foreign_featured_artist
from .sources import Pick

log = logging.getLogger("mfidx.youtube")

# Per-video refusals that say nothing about this address.
_VIDEO_RE = re.compile(
    r"confirm your age|inappropriate for some users|private video|granted access|members[- ]only|"
    r"video unavailable(?!.*try again later)|has been removed|not available in your country|copyright",
    re.I,
)
# Anything that says this address is being throttled or challenged.
_BLOCK_RE = re.compile(
    r"not a bot|sign in|log in|login required|HTTP Error 429|too many requests|rate[- ]?limit|"
    r"try again later|unusual traffic|captcha|confirm you(?:'|’)?re human|automated",
    re.I,
)
MAX_FAILURES_IN_A_ROW = 5


def classify(stderr: str) -> str | None:
    """"block" when yt-dlp's output carries any throttling or challenge signal, "video" for a
    per-video refusal, None otherwise."""
    for line in stderr.splitlines():
        if _VIDEO_RE.search(line):
            continue
        if _BLOCK_RE.search(line):
            return "block"
    return "video" if _VIDEO_RE.search(stderr) else None


class Blocked(Exception):
    pass


class YouTubeError(Exception):
    pass


class YouTube:
    Error = YouTubeError
    Refused = Blocked
    tries = 2  # candidates downloaded per song: the budget is precious

    def __init__(self, paths: Paths, gap: float = 30.0, per_day: int = 2400, stop=None) -> None:
        self.guard = SourceGuard(
            paths.state / "youtube.guard",
            limit=max(1, int(3600.0 / gap)),
            window=3600.0,
            min_gap=gap,
            jitter=0.3,
            day_limit=per_day,
            block_pause=24 * 3600.0,
            max_pause=72 * 3600.0,
            max_blocks=2,
            stop=stop,
        )
        self.guards = (self.guard,)
        self.cmd = [a for a in ytdlp() if a != "--no-warnings"]
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
            raise YouTubeError("yt-dlp timed out") from e
        err = p.stderr.decode(errors="replace")
        kind = classify(err)
        if kind == "block":
            self.failures = 0
            line = next((ln for ln in err.splitlines() if _BLOCK_RE.search(ln)), "blocked")
            self.guard.blocked(line.strip()[:300])
            raise Blocked(line.strip()[:300])
        if p.returncode != 0:
            if kind != "video":
                self._failed(err[-200:])
            raise YouTubeError(err.strip()[-300:] or f"yt-dlp exit {p.returncode}")
        self.failures = 0
        self.guard.ok()
        return p.stdout if binary else p.stdout.decode(errors="replace")

    def _failed(self, why: str) -> None:
        self.failures += 1
        if self.failures >= MAX_FAILURES_IN_A_ROW:
            self.failures = 0
            self.guard.blocked(f"{MAX_FAILURES_IN_A_ROW} failed runs in a row: {why.strip()[-160:]}")

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
        try:
            data = json.loads(out or "{}")
        except ValueError as e:
            raise YouTubeError("search returned no JSON") from e
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
        scored = [
            (score_candidate(c, song), i, c)
            for i, c in enumerate(results)
            if not foreign_featured_artist(c["title"], song.artist)
        ]
        scored = [(s - 0.01 * i, c) for s, i, c in scored if s > 0]
        return [(c, round(s, 4)) for s, c in sorted(scored, key=lambda t: -t[0])]

    def fetch_audio(self, video_id: str) -> np.ndarray:
        data = self._run(
            ["-f", "bestaudio[abr<=160]/bestaudio", "-o", "-", f"https://www.youtube.com/watch?v={video_id}"],
            timeout=300,
            binary=True,
        )
        if not data:
            raise YouTubeError("empty download")
        return decode_audio(data)
