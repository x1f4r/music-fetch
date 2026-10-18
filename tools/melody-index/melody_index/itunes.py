"""Apple's 30-s previews as a second window into a song (iTunes Search API, no account).

Apple often previews another part of the song than Deezer: on 54 CHAD songs the two windows
start more than 10 s apart in 35 % of cases, and together they hold the hummed melody of 48 %
of CHAD hums against 35 % for Deezer's alone. The Search API allows about 20 requests a minute;
the guard keeps it at 15. Matching is strict: same title and artist, length within 10 s of
Deezer's, no remix/live/acoustic/... words that the song title lacks.
"""

from __future__ import annotations

import logging
import re
from typing import Any

import numpy as np
import requests

from .config import Paths
from .extract import decode_audio
from .guard import SourceGuard
from .net import session
from .normalize import NON_ORIGINAL_RE, artist_similarity, primary_artist, title_core, title_similarity

log = logging.getLogger("mfidx.itunes")

SEARCH = "https://itunes.apple.com/search"
_VERSION_RE = re.compile(
    r"\b(?:remix|live|acoustic|instrumental|karaoke|cover|sped|slowed|edit|version|mix|demo|"
    r"remaster(?:ed)?|tribute|made famous|in the style|a cappella|acapella|reprise)\b",
    re.I,
)


class ITunesError(Exception):
    pass


class Refused(ITunesError):
    pass


def pick(results: list[dict[str, Any]], title: str, artist: str, duration: float | None) -> dict[str, Any] | None:
    best = None
    for c in results:
        if c.get("kind") != "song" or not c.get("previewUrl"):
            continue
        tn, an = c.get("trackName") or "", c.get("artistName") or ""
        ts, a = title_similarity(tn, title), artist_similarity(an, artist)
        if ts < 0.8 or a < 0.7:
            continue
        bad = _VERSION_RE.search(tn) or NON_ORIGINAL_RE.search(f"{tn} {c.get('collectionName') or ''}")
        if bad and not re.search(re.escape(bad.group(0)), title, re.I):
            continue
        d = (c.get("trackTimeMillis") or 0) / 1000.0
        if duration and abs(d - duration) > 10:
            continue
        s = ts + a - (abs(d - duration) / 20 if duration else 0.0)
        if best is None or s > best[0]:
            best = (s, c)
    return best[1] if best else None


class ITunes:
    Error = ITunesError
    Refused = Refused

    def __init__(self, paths: Paths, per_min: int = 15, country: str = "US", stop=None) -> None:
        self.http = session(2)
        self.country = country
        self.api = SourceGuard(
            paths.state / "itunes-api.guard",
            limit=per_min,
            window=60.0,
            min_gap=60.0 / per_min * 0.8,
            block_pause=6 * 3600.0,
            max_blocks=3,
            stop=stop,
        )
        self.cdn = SourceGuard(
            paths.state / "itunes-cdn.guard", limit=30, window=60.0, min_gap=1.0, block_pause=6 * 3600.0, stop=stop
        )
        self.guard = self.api
        self.guards = (self.api, self.cdn)
        self.stats = {"api": 0, "cdn": 0}

    def _get(self, guard: SourceGuard, url: str, params: dict[str, Any] | None = None) -> requests.Response:
        for _ in range(4):
            guard.acquire()
            try:
                r = self.http.get(url, params=params, timeout=30)
            except requests.RequestException:
                guard.soft_error()
                continue
            if r.status_code == 200:
                guard.ok()
                return r
            if r.status_code in (403, 429):
                guard.blocked(f"HTTP {r.status_code}")
                raise Refused(f"HTTP {r.status_code}")
            if r.status_code == 404:
                raise ITunesError("not found")
            guard.soft_error()
        raise ITunesError("gave up")

    def match(self, title: str, artist: str, duration: float | None) -> dict[str, Any] | None:
        self.stats["api"] += 1
        q = f"{primary_artist(artist)} {title_core(title)}"
        r = self._get(self.api, SEARCH, {"term": q, "entity": "song", "limit": 15, "country": self.country})
        try:
            results = r.json().get("results") or []
        except ValueError as e:
            raise ITunesError("invalid JSON") from e
        return pick(results, title, artist, duration)

    def fetch(self, url: str) -> np.ndarray:
        self.stats["cdn"] += 1
        return decode_audio(self._get(self.cdn, url).content)
