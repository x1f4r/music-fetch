"""Stage 3: preview audio -> RMVPE f0 -> notes -> phrases (GPU).

Previews are fetched with a fresh signed URL (they expire after ~15 min),
decoded in memory by ffmpeg to 16 kHz mono and dropped right after inference;
nothing but the f0 track, confidences and phrases is stored.
"""

from __future__ import annotations

import json
import logging
import queue
import subprocess
import threading
import time
import zlib
from dataclasses import dataclass
from typing import Any

import numpy as np
import requests

from . import db as dbm
from .config import Paths
from .net import DeezerClient, DeezerError, DeezerNotFound, SlidingWindowLimiter, session
from .notes import (
    DEFAULT_PARAMS,
    NoteParams,
    dedupe_phrases,
    f0_to_notes,
    phrases_from_notes,
    voiced_ratio,
)

log = logging.getLogger("mfidx.extract")

URL_MAX_AGE = 600.0  # signed preview URLs live ~891 s
F0_FLOOR = 0.01  # store f0 wherever salience exceeds this; the voicing threshold is applied later
MIN_AUDIO_S = 5.0


@dataclass
class Job:
    song_id: int
    deezer_id: int
    url: str | None
    url_time: float | None


class DecodeError(Exception):
    pass


def decode_audio(data: bytes, sr: int = 16000) -> np.ndarray:
    """Any container ffmpeg reads -> float32 mono at ``sr`` (in memory, no temp files)."""
    cmd = [
        "ffmpeg",
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        "pipe:0",
        "-f",
        "f32le",
        "-ac",
        "1",
        "-ar",
        str(sr),
        "pipe:1",
    ]
    try:
        p = subprocess.run(cmd, input=data, capture_output=True, timeout=120, check=False)
    except subprocess.TimeoutExpired as e:
        raise DecodeError("ffmpeg timed out") from e
    if p.returncode != 0:
        raise DecodeError(p.stderr.decode(errors="replace")[-300:])
    return np.frombuffer(p.stdout, dtype="<f4").copy()


def pack(arr: np.ndarray, dtype: str) -> bytes:
    return zlib.compress(np.ascontiguousarray(arr, dtype=dtype).tobytes(), 6)


def unpack(blob: bytes, dtype: str) -> np.ndarray:
    return np.frombuffer(zlib.decompress(blob), dtype=dtype)


def phrases_for(f0: np.ndarray, conf: np.ndarray, prm: NoteParams) -> list[dict[str, Any]]:
    notes = f0_to_notes(f0, conf, prm)
    phrases = dedupe_phrases(phrases_from_notes(notes, prm))
    return [p.to_json("preview") for p in phrases]


class Extractor:
    def __init__(
        self,
        paths: Paths,
        batch: int = 8,
        fetchers: int = 12,
        half: bool = False,
        prm: NoteParams = DEFAULT_PARAMS,
    ) -> None:
        from .rmvpe import Rmvpe

        self.paths = paths
        self.conn = dbm.connect(paths.db)  # writer (main thread)
        self.read_conn = dbm.connect(paths.db)  # feeder thread
        self.client = DeezerClient(paths.state)
        self.cdn = SlidingWindowLimiter(paths.state / "deezer-cdn.limiter", 50, 5.0)
        self.http = session(fetchers * 2)
        self.model = Rmvpe(paths.rmvpe_weights, half=half)
        self.batch = batch
        self.fetchers = fetchers
        self.prm = prm
        self.jobs: queue.Queue[Job | None] = queue.Queue(maxsize=fetchers * 4)
        self.ready: queue.Queue[tuple[Job, np.ndarray] | None] = queue.Queue(maxsize=batch * 3)
        self.results: list[dict[str, Any]] = []
        self.url_updates: list[tuple[Any, ...]] = []
        self.done = 0
        self.ok = 0
        self.t0 = time.time()
        self.gpu_seconds = 0.0
        self.audio_seconds = 0.0
        self._claimed: set[int] = set()  # queued, downloading or waiting for the GPU
        self._unflushed: set[int] = set()  # finished but not yet written
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ work selection

    def pending(self, limit: int) -> list[Job]:
        now = time.time()
        q = (
            "SELECT d.song_id, d.deezer_id, d.preview_url, d.url_fetched_at, d.has_preview FROM deezer d"
            " JOIN songs s ON s.id=d.song_id LEFT JOIN preview_melody m ON m.song_id=d.song_id"
            " WHERE d.status='ok' AND s.in_universe=1"
            " AND (m.song_id IS NULL OR (m.status='error' AND m.attempts<3)) {extra} LIMIT ?"
        )
        with self._lock:
            busy = len(self._claimed)
        fresh = self.read_conn.execute(
            q.format(
                extra="AND d.has_preview=1 AND d.url_fetched_at>? ORDER BY d.url_fetched_at ASC"
            ),
            (now - URL_MAX_AGE, limit + busy),
        ).fetchall()
        rows = list(fresh)
        if len(rows) < limit + busy:
            rows += self.read_conn.execute(
                q.format(extra="ORDER BY s.score DESC"), (limit + busy,)
            ).fetchall()
        out: list[Job] = []
        seen: set[int] = set()
        with self._lock:
            for r in rows:
                sid = r["song_id"]
                if sid in seen or sid in self._claimed or sid in self._unflushed:
                    continue
                seen.add(sid)
                if not r["has_preview"]:
                    self._record(sid, r["deezer_id"], status="no_preview")
                    continue
                self._claimed.add(sid)
                out.append(Job(sid, r["deezer_id"], r["preview_url"], r["url_fetched_at"]))
                if len(out) >= limit:
                    break
        return out

    # ------------------------------------------------------------------------ download

    def _refresh_url(self, job: Job) -> str | None:
        track = self.client.track(job.deezer_id)
        url = track.get("preview") or None
        now = time.time()
        with self._lock:
            self.url_updates.append(
                (
                    url,
                    now,
                    1 if url else 0,
                    track.get("isrc") or None,
                    track.get("release_date") or None,
                    job.song_id,
                )
            )
        job.url, job.url_time = url, now
        return url

    def _download(self, job: Job) -> bytes | None:
        if not job.url or not job.url_time or time.time() - job.url_time > URL_MAX_AGE:
            if self._refresh_url(job) is None:
                return None
        for attempt in range(3):
            self.cdn.acquire()
            try:
                r = self.http.get(job.url, timeout=30)
            except requests.RequestException:
                time.sleep(2 * (attempt + 1))
                continue
            if r.status_code == 200 and r.content:
                return r.content
            if r.status_code in (401, 403, 404, 410):
                if self._refresh_url(job) is None:
                    return None
                continue
            time.sleep(2 * (attempt + 1))
        raise DecodeError("preview download failed")

    def _fetch_loop(self) -> None:
        while True:
            job = self.jobs.get()
            if job is None:
                self.ready.put(None)
                return
            try:
                data = self._download(job)
                if data is None:
                    self._finish(job, status="no_preview")
                    continue
                audio = decode_audio(data)
                del data
                if len(audio) < MIN_AUDIO_S * 16000:
                    self._finish(job, status="error", error="preview too short")
                    continue
                self.ready.put((job, audio))
            except DeezerNotFound:
                self._finish(job, status="no_preview", error="track gone")
            except (DeezerError, DecodeError) as e:
                self._finish(job, status="error", error=str(e)[:300])
            except Exception as e:  # noqa: BLE001 - keep the worker alive
                log.exception("fetch %s failed", job.song_id)
                self._finish(job, status="error", error=f"{type(e).__name__}: {e}"[:300])

    # ---------------------------------------------------------------------- results

    def _record(self, song_id: int, deezer_id: int | None, **cols: Any) -> None:
        row = {
            "song_id": song_id,
            "deezer_id": deezer_id,
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
        self.results.append(row)
        self._unflushed.add(song_id)

    def _finish(self, job: Job, **cols: Any) -> None:
        with self._lock:
            self._record(job.song_id, job.deezer_id, **cols)
            self._claimed.discard(job.song_id)

    def flush(self) -> None:
        with self._lock:
            rows, self.results = self.results, []
            urls, self.url_updates = self.url_updates, []
        if not rows and not urls:
            return
        with dbm.Tx(self.conn):
            self.conn.executemany(
                "UPDATE deezer SET preview_url=?, url_fetched_at=?, has_preview=?, isrc=COALESCE(?, isrc),"
                " release_date=COALESCE(?, release_date) WHERE song_id=?",
                urls,
            )
            self.conn.executemany(
                "INSERT INTO preview_melody(song_id, status, deezer_id, f0, conf, n_frames, phrases, n_phrases,"
                " n_notes, voiced_ratio, attempts, error, updated_at) VALUES (:song_id, :status, :deezer_id, :f0,"
                " :conf, :n_frames, :phrases, :n_phrases, :n_notes, :voiced_ratio, 1, :error, :updated_at)"
                " ON CONFLICT(song_id) DO UPDATE SET status=excluded.status, deezer_id=excluded.deezer_id,"
                " f0=excluded.f0, conf=excluded.conf, n_frames=excluded.n_frames, phrases=excluded.phrases,"
                " n_phrases=excluded.n_phrases, n_notes=excluded.n_notes, voiced_ratio=excluded.voiced_ratio,"
                " attempts=preview_melody.attempts+1, error=excluded.error, updated_at=excluded.updated_at",
                rows,
            )
        with self._lock:
            self._unflushed.difference_update(r["song_id"] for r in rows)
        self.done += len(rows)
        self.ok += sum(1 for r in rows if r["status"] == "ok")

    # -------------------------------------------------------------------------- GPU

    def _infer(self, items: list[tuple[Job, np.ndarray]]) -> None:
        t = time.time()
        sal = self.model.salience([a for _, a in items])
        self.gpu_seconds += time.time() - t
        for (job, audio), s in zip(items, sal, strict=True):
            self.audio_seconds += len(audio) / 16000
            f0, conf = self.model.decode(s, F0_FLOOR)
            phrases = phrases_for(f0, conf, self.prm)
            self._finish(
                job,
                status="ok" if phrases else "empty",
                f0=pack(f0, "<f2"),
                conf=pack(np.round(conf * 255).clip(0, 255), "u1"),
                n_frames=int(len(f0)),
                phrases=json.dumps(phrases, separators=(",", ":")),
                n_phrases=len(phrases),
                n_notes=sum(len(p["notes"]) for p in phrases),
                voiced_ratio=round(voiced_ratio(f0, conf, self.prm.voicing), 4),
            )

    def run(self, limit: int | None = None, follow: bool = False) -> dict[str, Any]:
        if limit is None:
            dbm.stage_started(self.conn, "extract")
        threads = [
            threading.Thread(target=self._fetch_loop, daemon=True) for _ in range(self.fetchers)
        ]
        for th in threads:
            th.start()
        stop = threading.Event()

        def feeder() -> None:
            fed = 0
            idle_since = None
            while not stop.is_set():
                want = self.jobs.maxsize - self.jobs.qsize()
                if limit is not None:
                    if fed >= limit:
                        break
                    want = min(want, limit - fed)
                jobs = self.pending(max(1, want)) if want > 0 else []
                for j in jobs:
                    self.jobs.put(j)
                fed += len(jobs)
                if jobs:
                    idle_since = None
                    continue
                if want <= 0:
                    time.sleep(0.5)
                    continue
                # Nothing to do right now: wait for stage 2 when following it.
                if follow and dbm.stage_status(self.read_conn, "resolve") != "done":
                    time.sleep(10)
                    continue
                with self._lock:
                    in_flight = len(self._claimed)
                if in_flight:
                    time.sleep(1)
                    continue
                idle_since = idle_since or time.time()
                if time.time() - idle_since > 5:
                    break
                time.sleep(1)
            for _ in threads:
                self.jobs.put(None)

        feed = threading.Thread(target=feeder, daemon=True)
        feed.start()
        finished = 0
        last_log = time.time()
        pending_items: list[tuple[Job, np.ndarray]] = []
        while finished < len(threads):
            try:
                item = self.ready.get(timeout=0.5)
            except queue.Empty:
                item = False
            if item is None:
                finished += 1
            elif item is not False:
                pending_items.append(item)
            if len(pending_items) >= self.batch or (pending_items and (item is False or finished)):
                self._infer(pending_items)
                pending_items = []
            if len(self.results) >= 20 or (self.results and item is False):
                self.flush()
            if time.time() - last_log > 60:
                last_log = time.time()
                self._log_progress()
        if pending_items:
            self._infer(pending_items)
        stop.set()
        self.flush()
        self._log_progress()
        info = {
            "processed": self.done,
            "ok": self.ok,
            "seconds": round(time.time() - self.t0, 1),
            "gpu_seconds": round(self.gpu_seconds, 1),
            "audio_seconds": round(self.audio_seconds, 1),
        }
        if limit is None:
            dbm.stage_finished(self.conn, "extract", info)
        return info

    def _log_progress(self) -> None:
        el = max(1e-6, time.time() - self.t0)
        log.info(
            "extract: %d done (%d with phrases), %.1f songs/min, GPU %.1fx realtime, %d API requests",
            self.done,
            self.ok,
            self.done / el * 60,
            self.audio_seconds / max(1e-6, self.gpu_seconds),
            self.client.requests,
        )


def renote(paths: Paths, prm: NoteParams = DEFAULT_PARAMS) -> dict[str, Any]:
    """Recompute phrases from stored f0/confidence (no network, no GPU)."""
    conn = dbm.connect(paths.db)
    rows = conn.execute(
        "SELECT song_id, f0, conf FROM preview_melody WHERE f0 IS NOT NULL"
    ).fetchall()
    updates = []
    for r in rows:
        f0 = unpack(r["f0"], "<f2").astype(np.float32)
        conf = unpack(r["conf"], "u1").astype(np.float32) / 255.0
        phrases = phrases_for(f0, conf, prm)
        updates.append(
            (
                "ok" if phrases else "empty",
                json.dumps(phrases, separators=(",", ":")),
                len(phrases),
                sum(len(p["notes"]) for p in phrases),
                round(voiced_ratio(f0, conf, prm.voicing), 4),
                r["song_id"],
            )
        )
    with dbm.Tx(conn):
        conn.executemany(
            "UPDATE preview_melody SET status=?, phrases=?, n_phrases=?, n_notes=?, voiced_ratio=? WHERE song_id=?",
            updates,
        )
    return {"songs": len(updates)}
