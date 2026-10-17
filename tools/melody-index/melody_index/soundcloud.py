"""SoundCloud as a source of full-length recordings (api-v2 with the public web client id).

Official label uploads on SoundCloud are either 30-s "Go+" snippets (``policy: SNIP``) or
streamed only as encrypted HLS; neither is used. What remains are plain HLS streams (AAC or MP3
segments), mostly re-uploads, so every pick is checked against the Deezer preview melody by the
caller before it is kept. Audio segments are fetched into memory and decoded there.

Requests go through two ``SourceGuard``s: one for api-v2 (search, stream URLs) and one for the
media CDN (playlists and segments). 401/403 on api-v2 first refresh the client id; repeated
refusals and 429s open the breaker.
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from dataclasses import dataclass
from typing import Any

import numpy as np
import requests

from .config import Paths
from .extract import DecodeError, decode_audio
from .guard import SourceGuard
from .net import session
from .normalize import (
    NON_ORIGINAL_RE,
    artist_similarity,
    fold,
    foreign_featured_artist,
    primary_artist,
    title_core,
    title_similarity,
)

log = logging.getLogger("mfidx.soundcloud")

API = "https://api-v2.soundcloud.com"
HOME = "https://soundcloud.com/"
CLIENT_ID_TTL = 6 * 3600.0
MAX_DURATION_S = 900.0
MAX_BYTES = 40_000_000
DURATION_TOLERANCE_S = 10.0
# Uploads that are not the recording (unless the song title says so itself).
_NOT_RECORDING_RE = re.compile(
    r"\b(?:nightcore|sped[ -]?up|slowed|reverb|[0-9]+d(?: audio)?|remix(?:es|ed)?|rmx|bootleg|edits?|flip|"
    r"rework|covers?|vip|mashups?|type beat|acoustic|tiny desk|remake|live|sessions?|unplugged|piano|guitar|"
    r"instrumental|karaoke|bass ?boost(?:ed)?|extended|loop|hours?|reprise|demo|snippet|preview|teaser|"
    r"chopped|screwed|tribute|medley|versions?|ver|overlapped|a ?cappella|acapp?ella|vocals? only|lo-?fi|"
    r"phonk|orchestral|symphonic|choir|amapiano|drill remix|jersey club)\b|\s[x×]\s",
    re.I,
)
# Preferred transcodings, best first (small AAC is plenty for melody extraction).
_PRESET_ORDER = ("aac_96k", "aac_160k", "mp3_1_0", "mp3_0_1", "mp3_standard", "opus_0_0")


class SoundCloudError(Exception):
    pass


class Refused(SoundCloudError):
    """api-v2 refused the client (rate limit or blocked client id)."""


@dataclass
class Candidate:
    id: int
    title: str
    user: str
    duration: float
    full_duration: float
    policy: str
    isrc: str | None
    publisher_artist: str
    authorization: str | None
    transcodings: list[dict[str, Any]]
    url: str
    score: float = 0.0
    reason: str = ""

    @property
    def ref_id(self) -> str:
        return f"soundcloud:{self.id}"

    @property
    def uploader(self) -> str:
        return self.user

    @classmethod
    def from_api(cls, t: dict[str, Any]) -> Candidate:
        pm = t.get("publisher_metadata") or {}
        tr = []
        for x in (t.get("media") or {}).get("transcodings") or []:
            fmt = x.get("format") or {}
            tr.append(
                {
                    "url": x.get("url"),
                    "protocol": fmt.get("protocol"),
                    "mime": fmt.get("mime_type") or "",
                    "snipped": bool(x.get("snipped")),
                    "preset": x.get("preset") or "",
                }
            )
        return cls(
            id=int(t.get("id") or 0),
            title=t.get("title") or "",
            user=(t.get("user") or {}).get("username") or "",
            duration=(t.get("duration") or 0) / 1000.0,
            full_duration=(t.get("full_duration") or t.get("duration") or 0) / 1000.0,
            policy=t.get("policy") or "",
            isrc=(pm.get("isrc") or None),
            publisher_artist=pm.get("artist") or "",
            authorization=t.get("track_authorization"),
            transcodings=tr,
            url=t.get("permalink_url") or "",
        )

    def plain_transcodings(self) -> list[dict[str, Any]]:
        """Unencrypted, unsnipped streams, preferred first."""
        ok = [x for x in self.transcodings if not x["snipped"] and x["protocol"] in ("hls", "progressive")]

        def rank(x: dict[str, Any]) -> tuple[int, int]:
            p = x["preset"]
            return (_PRESET_ORDER.index(p) if p in _PRESET_ORDER else len(_PRESET_ORDER), x["protocol"] != "hls")

        return sorted(ok, key=rank)


def score_candidate(c: Candidate, title: str, artist: str, duration: float | None, isrc: str | None) -> tuple[float, str]:
    """0..1 plausibility that a SoundCloud track is the song's full recording; 0 = reject (with a reason)."""
    shown = c.title
    if " - " in c.title:
        head, _, tail = c.title.partition(" - ")
        if artist_similarity(head, artist) >= 0.8:
            shown = tail
    ts = max(title_similarity(shown, title), title_similarity(c.title, title) if " - " not in c.title else 0.0)
    if ts < 0.75:
        return 0.0, "title"
    a = max(
        artist_similarity(c.user, artist),
        artist_similarity(c.publisher_artist, artist),
        artist_similarity(c.title, artist),
    )
    if a < 0.6 and fold(primary_artist(artist)) not in fold(f"{c.title} {c.user}"):
        return 0.0, "artist"
    # Version words, but not inside the artist's own name ("Lil Nas X - ...").
    probe = fold(c.title)
    for name in (artist, re.split(r"\s+(?:feat\.?|featuring|ft\.?|with|&|and)\s+|,\s*", artist, flags=re.I)[0]):
        probe = probe.replace(fold(name), " ")
    bad = _NOT_RECORDING_RE.search(probe) or NON_ORIGINAL_RE.search(probe)
    if bad and not re.search(rf"\b{re.escape(bad.group(0).strip())}\b", fold(title)):
        return 0.0, "version"
    if foreign_featured_artist(c.title, artist):
        return 0.0, "feature"  # someone else's verse on the original hook
    if not c.full_duration or c.full_duration > MAX_DURATION_S:
        return 0.0, "duration"
    if duration and abs(c.full_duration - duration) > DURATION_TOLERANCE_S:
        return 0.0, "duration"
    if c.policy == "SNIP" or (c.duration and c.full_duration and c.duration + 1 < c.full_duration):
        return 0.0, "snippet"
    if c.policy == "BLOCK":
        return 0.0, "blocked"
    if not c.plain_transcodings():
        return 0.0, "protected"
    isrc_ok = bool(c.isrc and isrc and c.isrc.upper() == isrc.upper())
    dscore = 1.0 - abs(c.full_duration - duration) / DURATION_TOLERANCE_S if duration else 0.5
    return round(0.45 + 0.25 * isrc_ok + 0.1 * ts + 0.1 * min(1.0, a) + 0.1 * dscore, 4), "ok"


def rank_candidates(
    results: list[Candidate], title: str, artist: str, duration: float | None, isrc: str | None
) -> list[Candidate]:
    out = []
    for i, c in enumerate(results):
        c.score, c.reason = score_candidate(c, title, artist, duration, isrc)
        if c.score > 0:
            c.score -= 0.002 * i  # earlier search results break ties
            out.append(c)
    return sorted(out, key=lambda c: -c.score)


class SoundCloud:
    Error = SoundCloudError
    Refused = Refused

    def __init__(self, paths: Paths, api_per_min: int = 12, cdn_per_s: float = 3.0, stop=None) -> None:
        self.paths = paths
        self.http = session(4)
        self.api = SourceGuard(
            paths.state / "soundcloud-api.guard",
            limit=api_per_min,
            window=60.0,
            min_gap=max(1.0, 60.0 / api_per_min * 0.6),
            block_pause=3 * 3600.0,
            max_blocks=4,
            stop=stop,
        )
        self.cdn = SourceGuard(
            paths.state / "soundcloud-cdn.guard",
            limit=max(1, int(cdn_per_s * 10)),
            window=10.0,
            min_gap=0.1,
            block_pause=3 * 3600.0,
            max_blocks=4,
            stop=stop,
        )
        self.guard = self.api
        self.guards = (self.api, self.cdn)
        self._cid_path = paths.state / "soundcloud-client-id.json"
        self._cid: str | None = None
        self._cid_lock = threading.Lock()
        self.stats = {"api": 0, "cdn": 0, "bytes": 0}

    # ------------------------------------------------------------------ client id

    def client_id(self, refresh: bool = False) -> str:
        with self._cid_lock:
            return self._client_id(refresh)

    def _client_id(self, refresh: bool) -> str:
        if not refresh:
            if self._cid:
                return self._cid
            try:
                d = json.loads(self._cid_path.read_text())
                if time.time() - float(d["t"]) < CLIENT_ID_TTL:
                    self._cid = str(d["id"])
                    return self._cid
            except (OSError, ValueError, KeyError):
                pass
        self.api.acquire()
        html = self.http.get(HOME, timeout=30).text
        scripts = re.findall(r'<script[^>]+src="(https://a-v2\.sndcdn\.com/assets/[^"]+\.js)"', html)
        for src in reversed(scripts):
            self.api.acquire()
            m = re.search(r'client_id\s*:\s*"([0-9a-zA-Z]{32})"', self.http.get(src, timeout=30).text)
            if m:
                self._cid = m.group(1)
                self._cid_path.write_text(json.dumps({"id": self._cid, "t": time.time()}))
                log.info("soundcloud: new web client id")
                return self._cid
        raise SoundCloudError("no client id in the web app")

    # ------------------------------------------------------------------ api-v2

    def _api(self, url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        refreshed = False
        last = "no response"
        for _ in range(5):
            p = dict(params or {})
            p["client_id"] = self.client_id()
            self.api.acquire()
            self.stats["api"] += 1
            try:
                r = self.http.get(url, params=p, timeout=30)
            except requests.RequestException as e:
                self.api.soft_error()
                last = type(e).__name__
                continue
            if r.status_code == 200:
                self.api.ok()
                return r.json()
            if r.status_code in (401, 403) and not refreshed:
                # The web app rotates its client id now and then.
                refreshed = True
                self.client_id(refresh=True)
                continue
            if r.status_code in (401, 403, 429):
                self.api.blocked(f"api-v2 HTTP {r.status_code}")
                raise Refused(f"HTTP {r.status_code}")
            if r.status_code == 404:
                raise SoundCloudError("not found")
            last = f"HTTP {r.status_code}"
            self.api.soft_error()
        raise SoundCloudError(f"api-v2 gave up ({last})")

    def search(self, title: str, artist: str, limit: int = 20) -> list[Candidate]:
        q = f"{primary_artist(artist)} {title_core(title)}"
        data = self._api(f"{API}/search/tracks", {"q": q, "limit": limit, "offset": 0, "app_locale": "en"})
        return [Candidate.from_api(t) for t in data.get("collection") or [] if t.get("kind", "track") == "track"]

    def candidates(self, t) -> list[Candidate]:
        """Plausible full uploads of a refs target, most plausible first."""
        return rank_candidates(self.search(t.title, t.artist), t.title, t.artist, t.duration, t.isrc)

    def fetch(self, c: Candidate) -> np.ndarray:
        return self.fetch_audio(c)

    # ------------------------------------------------------------------ media

    def _cdn(self, url: str, timeout: float = 60) -> bytes:
        for _ in range(4):
            self.cdn.acquire()
            self.stats["cdn"] += 1
            try:
                r = self.http.get(url, timeout=timeout)
            except requests.RequestException:
                self.cdn.soft_error()
                continue
            if r.status_code == 200:
                self.stats["bytes"] += len(r.content)
                return r.content
            if r.status_code == 429:
                self.cdn.blocked("media HTTP 429")
                raise Refused("media HTTP 429")
            if r.status_code in (403, 404, 410):
                raise SoundCloudError(f"media HTTP {r.status_code}")
            self.cdn.soft_error()
        raise SoundCloudError("media gave up")

    def fetch_audio(self, c: Candidate) -> np.ndarray:
        """The candidate's full stream, decoded in memory to 16 kHz mono."""
        errors = []
        for tr in c.plain_transcodings():
            params = {"track_authorization": c.authorization} if c.authorization else {}
            try:
                url = self._api(tr["url"], params)["url"]
            except Refused:
                raise
            except SoundCloudError as e:
                errors.append(f"{tr['preset'] or tr['protocol']}: {e}")
                continue
            if tr["protocol"] == "progressive":
                blob = self._cdn(url, timeout=180)
            else:
                blob = self._hls(url)
            try:
                return decode_audio(blob)
            except DecodeError as e:
                errors.append(f"{tr['preset']}: decode {e}"[:120])
        raise SoundCloudError("no usable stream (" + "; ".join(errors)[:250] + ")")

    def _hls(self, url: str) -> bytes:
        playlist = self._cdn(url).decode("utf-8", "replace")
        if "#EXT-X-KEY" in playlist and "METHOD=NONE" not in playlist:
            raise SoundCloudError("encrypted playlist")
        base = url.split("?", 1)[0].rsplit("/", 1)[0]

        def absolute(u: str) -> str:
            return u if u.startswith("http") else f"{base}/{u}"

        parts: list[bytes] = []
        size = 0
        m = re.search(r'#EXT-X-MAP:URI="([^"]+)"', playlist)
        if m:
            parts.append(self._cdn(absolute(m.group(1))))
        for line in playlist.splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                b = self._cdn(absolute(line))
                size += len(b)
                if size > MAX_BYTES:
                    raise SoundCloudError("stream too large")
                parts.append(b)
        if len(parts) < 2:
            raise SoundCloudError("empty playlist")
        return b"".join(parts)
