"""Dailymotion as a source of full recordings: public REST API search, yt-dlp for the stream.

Dailymotion has many music uploads (official videos and re-uploads) and plain HLS streams without
DRM. Its streams always carry video, so the smallest rendition is fetched (into memory, through
yt-dlp's stdout) and only its audio is decoded. The API search and the downloads have one guard
each; an API 403/429, a rate-limit message from the media servers or a run of failed downloads
opens the breaker (a 403 on one stream alone is a per-video failure, e.g. geo restrictions).
"""

from __future__ import annotations

import logging
import re
import subprocess
from typing import Any

import numpy as np
import requests

from .config import Paths
from .extract import DecodeError, decode_audio
from .fulltrack import Song, score_candidate, ytdlp
from .guard import SourceGuard
from .net import session
from .normalize import foreign_featured_artist, primary_artist, title_core
from .sources import Pick, ram_dir, ytdlp_ram_args

log = logging.getLogger("mfidx.dailymotion")

API = "https://api.dailymotion.com/videos"
_BLOCK_RE = re.compile(r"HTTP Error 429|too many requests|rate[- ]?limit", re.I)
MAX_FAILURES_IN_A_ROW = 6


class DailymotionError(Exception):
    pass


class Refused(DailymotionError):
    pass


class Dailymotion:
    Error = DailymotionError
    Refused = Refused
    tries = 2  # candidates downloaded per song

    def __init__(self, paths: Paths, api_per_min: int = 6, downloads_per_hour: int = 60, stop=None) -> None:
        self.http = session(2)
        self.api = SourceGuard(
            paths.state / "dailymotion-api.guard", limit=api_per_min, window=60.0,
            min_gap=60.0 / api_per_min * 0.8, block_pause=6 * 3600.0, max_blocks=3, stop=stop,
        )
        self.media = SourceGuard(
            paths.state / "dailymotion-media.guard", limit=downloads_per_hour, window=3600.0,
            min_gap=3600.0 / downloads_per_hour * 0.7, jitter=0.3, block_pause=6 * 3600.0, max_blocks=3, stop=stop,
        )
        self.guard = self.api
        self.guards = (self.api, self.media)
        self.cmd = ytdlp() + ytdlp_ram_args()
        self.failures = 0

    def search(self, song: Song, limit: int = 15) -> list[dict[str, Any]]:
        q = f"{primary_artist(song.artist)} {title_core(song.title)}"
        for _ in range(4):
            self.api.acquire()
            try:
                r = self.http.get(
                    API,
                    params={"search": q, "fields": "id,title,duration,owner.screenname", "limit": limit},
                    timeout=30,
                )
            except requests.RequestException:
                self.api.soft_error()
                continue
            if r.status_code == 200:
                self.api.ok()
                return [
                    {"id": x["id"], "title": x.get("title") or "", "duration": x.get("duration"),
                     "channel": x.get("owner.screenname") or ""}
                    for x in r.json().get("list") or [] if x.get("id")
                ]
            if r.status_code in (403, 429):
                self.api.blocked(f"api HTTP {r.status_code}")
                raise Refused(f"HTTP {r.status_code}")
            self.api.soft_error()
        raise DailymotionError("search gave up")

    def candidates(self, t) -> list[Pick]:
        song = Song(t.song_id, t.title, t.artist, t.duration)
        scored = [
            (score_candidate(c, song) - 0.01 * i, c)
            for i, c in enumerate(self.search(song))
            if not foreign_featured_artist(c["title"], song.artist)
        ]
        return [
            Pick(f"dailymotion:{c['id']}", c["title"], c["channel"], c["duration"], round(s, 4), c["id"])
            for s, c in sorted(scored, key=lambda x: -x[0]) if s > 0
        ]

    def fetch(self, p: Pick) -> np.ndarray:
        self.media.acquire()
        try:
            proc = subprocess.run(
                self.cmd + ["-f", "worst[acodec!=none]/worst", "-o", "-",
                            f"https://www.dailymotion.com/video/{p.key}"],
                capture_output=True, timeout=300, check=False, cwd=ram_dir(),
            )
        except subprocess.TimeoutExpired as e:
            self._failed("timeout")
            raise DecodeError("yt-dlp timed out") from e
        err = proc.stderr.decode(errors="replace")
        if _BLOCK_RE.search(err):
            self.media.blocked(err.strip().splitlines()[-1][:200] if err.strip() else "blocked")
            raise Refused(err[-200:])
        if proc.returncode != 0 or not proc.stdout:
            self._failed(err[-120:])
            raise DailymotionError(err[-300:] or f"yt-dlp exit {proc.returncode}")
        self.failures = 0
        self.media.ok()
        return decode_audio(proc.stdout)

    def _failed(self, why: str) -> None:
        self.failures += 1
        if self.failures >= MAX_FAILURES_IN_A_ROW:
            self.failures = 0
            self.media.blocked(f"{MAX_FAILURES_IN_A_ROW} failed downloads in a row: {why}")
