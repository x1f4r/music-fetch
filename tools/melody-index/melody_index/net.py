"""HTTP helpers: neutral User-Agent, a cross-process rate limiter and the Deezer client."""

from __future__ import annotations

import fcntl
import json
import logging
import random
import threading
import time
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter

from .config import USER_AGENT

log = logging.getLogger("mfidx.net")


def session(pool: int = 16) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {"User-Agent": USER_AGENT, "Accept": "application/json, text/html;q=0.9, */*;q=0.5"}
    )
    adapter = HTTPAdapter(pool_connections=pool, pool_maxsize=pool, max_retries=0)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


class SlidingWindowLimiter:
    """At most ``limit`` acquisitions per ``window`` seconds, shared by every thread and
    process on the host that uses the same state file (``flock``)."""

    def __init__(self, path: Path, limit: int, window: float) -> None:
        self.path = path
        self.limit = limit
        self.window = window
        self._lock = threading.Lock()
        path.parent.mkdir(parents=True, exist_ok=True)

    def _update(self, fn) -> Any:
        with self._lock, open(self.path, "a+") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                f.seek(0)
                raw = f.read()
                try:
                    state = json.loads(raw) if raw.strip() else {}
                except ValueError:
                    state = {}
                result = fn(state, time.time())
                f.seek(0)
                f.truncate()
                f.write(json.dumps(state))
                f.flush()
                return result
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

    def acquire(self) -> None:
        def take(state: dict, now: float) -> float:
            stamps = [t for t in state.get("t", []) if now - t < self.window]
            state["t"] = stamps
            pause = float(state.get("pause_until", 0.0))
            if now < pause:
                return pause - now
            if len(stamps) < self.limit:
                stamps.append(now)
                return 0.0
            return stamps[0] + self.window - now

        while True:
            wait = self._update(take)
            if wait <= 0:
                return
            time.sleep(wait + random.uniform(0.005, 0.05))

    def pause(self, seconds: float) -> None:
        """Make every user of this limiter wait (quota errors)."""

        def apply(state: dict, now: float) -> None:
            state["pause_until"] = max(float(state.get("pause_until", 0.0)), now + seconds)

        self._update(apply)


class DeezerError(Exception):
    pass


class DeezerNotFound(DeezerError):
    pass


class DeezerClient:
    """api.deezer.com with a shared ≤ 40 requests / 5 s budget and backoff on quota errors."""

    BASE = "https://api.deezer.com"

    def __init__(self, state_dir: Path, limit: int = 40, window: float = 5.2) -> None:
        self.limiter = SlidingWindowLimiter(state_dir / "deezer-api.limiter", limit, window)
        self.http = session(32)
        self.requests = 0
        self.quota_hits = 0

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = path if path.startswith("http") else f"{self.BASE}{path}"
        backoff = 2.0
        last = "no response"
        for _ in range(9):
            self.limiter.acquire()
            self.requests += 1
            try:
                r = self.http.get(url, params=params, timeout=20)
            except requests.RequestException as e:
                last = f"{type(e).__name__}"
                time.sleep(backoff)
                backoff = min(backoff * 2, 120)
                continue
            if r.status_code == 429 or r.status_code >= 500:
                last = f"HTTP {r.status_code}"
                self.limiter.pause(backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 120)
                continue
            try:
                data = r.json()
            except ValueError:
                last = "invalid JSON"
                time.sleep(backoff)
                backoff = min(backoff * 2, 120)
                continue
            if isinstance(data, dict) and "error" in data:
                err = data["error"] if isinstance(data["error"], dict) else {}
                code = err.get("code")
                if code == 4:
                    self.quota_hits += 1
                    log.warning("deezer quota exceeded; backing off %.0fs", backoff)
                    self.limiter.pause(backoff)
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 120)
                    continue
                if code == 700:
                    last = "service busy"
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 120)
                    continue
                if code == 800:
                    raise DeezerNotFound(err.get("message", "not found"))
                raise DeezerError(f"{err.get('type', 'error')} {code}: {err.get('message', '')}")
            if not isinstance(data, dict):
                raise DeezerError("unexpected response")
            return data
        raise DeezerError(f"giving up after retries ({last})")

    def search(self, query: str, limit: int = 15) -> list[dict[str, Any]]:
        data = self.get("/search", {"q": query, "limit": limit})
        items = data.get("data") or []
        return [x for x in items if isinstance(x, dict) and x.get("type", "track") == "track"]

    def track(self, track_id: int) -> dict[str, Any]:
        return self.get(f"/track/{int(track_id)}")


class PoliteFetcher:
    """Sequential fetches with a minimum interval per host (chart pages, oEmbed)."""

    def __init__(self, min_interval: float = 1.5) -> None:
        self.http = session(4)
        self.min_interval = min_interval
        self._last: dict[str, float] = {}

    def get(self, url: str, **kw: Any) -> requests.Response:
        host = url.split("/")[2]
        wait = self._last.get(host, 0.0) + self.min_interval - time.time()
        if wait > 0:
            time.sleep(wait)
        backoff = 3.0
        for attempt in range(5):
            try:
                r = self.http.get(url, timeout=60, **kw)
            except requests.RequestException:
                if attempt == 4:
                    raise
                time.sleep(backoff)
                backoff *= 2
                continue
            finally:
                self._last[host] = time.time()
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(backoff)
                backoff *= 2
                continue
            return r
        return r
