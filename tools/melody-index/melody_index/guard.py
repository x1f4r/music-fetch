"""Pacing and a circuit breaker for one remote source, shared by every thread and process.

A ``SourceGuard`` keeps its state in a small JSON file under ``state/`` (locked with ``flock``),
so two crawls on the same host, or a restart, never add up to more than the source's budget:

- ``acquire()`` waits for a slot: at most ``limit`` requests per ``window`` seconds (and at most
  ``day_limit`` per 24 h when set), at least ``min_gap`` seconds (±``jitter``) between two
  requests, and nothing while the breaker is open.
- ``ok()`` after a successful request closes the breaker's count.
- ``blocked(reason)`` opens the breaker: every user of the source waits ``block_pause`` seconds,
  doubled for each further consecutive block (capped at ``max_pause``). After ``max_blocks``
  consecutive blocks the source is disabled until ``reset()``: a crawl never keeps knocking on a
  door that has been shut.
- ``soft_error()`` (timeouts, 5xx) backs off briefly and exponentially, also for every user.
"""

from __future__ import annotations

import fcntl
import json
import logging
import random
import threading
import time
from pathlib import Path
from typing import Any

log = logging.getLogger("mfidx.guard")

DAY = 86400.0


class SourceDisabled(Exception):
    """The source tripped its breaker too often and stays off until reset."""


class Stopped(Exception):
    """The crawl is shutting down."""


class SourceGuard:
    def __init__(
        self,
        path: Path,
        *,
        limit: int,
        window: float,
        min_gap: float = 0.0,
        jitter: float = 0.2,
        day_limit: int | None = None,
        block_pause: float = 6 * 3600.0,
        max_pause: float = 48 * 3600.0,
        max_blocks: int = 3,
        soft_base: float = 5.0,
        soft_max: float = 600.0,
        stop: threading.Event | None = None,
    ) -> None:
        self.path = path
        self.name = path.stem
        self.limit = limit
        self.window = window
        self.min_gap = min_gap
        self.jitter = jitter
        self.day_limit = day_limit
        self.block_pause = block_pause
        self.max_pause = max_pause
        self.max_blocks = max_blocks
        self.soft_base = soft_base
        self.soft_max = soft_max
        self.stop = stop or threading.Event()
        self._lock = threading.Lock()
        path.parent.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ state file

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

    def state(self) -> dict[str, Any]:
        return self._update(lambda s, now: dict(s))

    # ------------------------------------------------------------------ pacing

    def _sleep(self, seconds: float) -> None:
        if self.stop.wait(max(0.0, seconds)):
            raise Stopped(self.name)

    def acquire(self) -> None:
        def take(state: dict, now: float) -> float | str:
            if state.get("disabled"):
                return "disabled"
            pause = float(state.get("pause_until", 0.0))
            if now < pause:
                return pause - now
            stamps = [t for t in state.get("t", []) if now - t < self.window]
            state["t"] = stamps
            gap = float(state.get("next_gap", self.min_gap))
            if stamps and now - stamps[-1] < gap:
                return stamps[-1] + gap - now
            if len(stamps) >= self.limit:
                return stamps[0] + self.window - now
            if self.day_limit is not None:
                day = [t for t in state.get("d", []) if now - t < DAY]
                state["d"] = day
                if len(day) >= self.day_limit:
                    return day[0] + DAY - now
                day.append(now)
            stamps.append(now)
            state["next_gap"] = self.min_gap * random.uniform(1 - self.jitter, 1 + self.jitter)
            state["requests"] = int(state.get("requests", 0)) + 1
            return 0.0

        while True:
            wait = self._update(take)
            if wait == "disabled":
                raise SourceDisabled(self.name)
            if wait <= 0:
                return
            # Long pauses are re-checked every few minutes (a reset or a stop may end them).
            self._sleep(min(float(wait), 300.0) + random.uniform(0.01, 0.1))

    # ------------------------------------------------------------------ outcomes

    def ok(self) -> None:
        def apply(state: dict, now: float) -> None:
            state["blocks"] = 0
            state["soft"] = 0
            state["last_ok"] = now

        self._update(apply)

    def blocked(self, reason: str) -> float:
        """Open the breaker; returns the pause in seconds (inf when the source is now disabled)."""

        def apply(state: dict, now: float) -> float:
            n = int(state.get("blocks", 0)) + 1
            state["blocks"] = n
            state["total_blocks"] = int(state.get("total_blocks", 0)) + 1
            state["last_block"] = now
            state["reason"] = reason[:300]
            if n >= self.max_blocks:
                state["disabled"] = True
                return float("inf")
            pause = min(self.max_pause, self.block_pause * 2 ** (n - 1))
            state["pause_until"] = max(float(state.get("pause_until", 0.0)), now + pause)
            return pause

        pause = self._update(apply)
        if pause == float("inf"):
            log.error("%s: blocked %d times in a row (%s); disabled until reset", self.name, self.max_blocks, reason[:120])
        else:
            log.warning("%s: blocked (%s); pausing %.1f h", self.name, reason[:120], pause / 3600)
        return pause

    def soft_error(self) -> float:
        def apply(state: dict, now: float) -> float:
            n = int(state.get("soft", 0)) + 1
            state["soft"] = n
            pause = min(self.soft_max, self.soft_base * 2 ** (n - 1))
            state["pause_until"] = max(float(state.get("pause_until", 0.0)), now + pause)
            return pause

        return self._update(apply)

    def reset(self) -> None:
        def apply(state: dict, now: float) -> None:
            for k in ("disabled", "blocks", "soft", "pause_until", "reason"):
                state.pop(k, None)

        self._update(apply)

    def summary(self) -> dict[str, Any]:
        s = self.state()
        now = time.time()
        recent = [t for t in s.get("t", []) if now - t < self.window]
        return {
            "requests": s.get("requests", 0),
            "in_window": len(recent),
            "last_24h": sum(1 for t in s.get("d", []) if now - t < DAY) if self.day_limit else None,
            "limit": f"{self.limit}/{self.window:.0f}s" + (f", {self.day_limit}/day" if self.day_limit else ""),
            "paused_for_s": max(0.0, float(s.get("pause_until", 0.0)) - now),
            "blocks_in_a_row": s.get("blocks", 0),
            "total_blocks": s.get("total_blocks", 0),
            "disabled": bool(s.get("disabled")),
            "reason": s.get("reason"),
        }
