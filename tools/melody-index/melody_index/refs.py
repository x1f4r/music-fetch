"""Stage 3c: verified full-track melodies from several sources, plus second preview windows.

People hum whatever part of a song they remember; a Deezer preview holds about 30 s of it. This
stage finds the whole recording for the most popular songs and keeps its melody, like stage 3b
(``fulltrack``) does from YouTube alone, but from several sources at once:

- ``soundcloud``: api-v2 search, plain (unencrypted) HLS streams; mostly re-uploads.
- ``dailymotion``: public API search, yt-dlp.
- ``youtube``: the full-track stage's search and choice, slowly and behind a strict breaker.
- ``itunes``: not a full track but Apple's 30-s preview, which often covers another part of the
  song; only for songs that end up without a full track.

A song goes to the sources in that order: a source takes it once every source before it has
failed for it (or is switched off). Every full-track pick is checked against the song's Deezer
preview melody (``placement``): the preview's 10 fps RMVPE contour must find itself somewhere in
the full track, without transposition or tempo change, for at least ``ACCEPT`` of its voiced
frames. On the 701 YouTube full tracks of stage 3b this passes 99.3 % of the true pairs and none
of 3,500 pairs with another song's preview (wrong pairs score at most 0.33). Sped-up, pitched,
live, cover and remix uploads fail it; the version words in titles are only a first filter.

Audio is fetched into memory, decoded there and dropped after RMVPE; only f0, confidences and
notes are stored, full tracks in ``track_melody`` (same rows as stage 3b, so the export and
stage 3b see them) and windows in ``window_melody``. ``ref_attempt`` records every outcome per
song and source, which makes the stage resumable and its progress measurable (``mfidx
refs-status``). Order: the ``top`` most popular resolved songs, songs without a validated MIDI
melody first.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import warnings
from dataclasses import dataclass
from typing import Any

import numpy as np
from numpy.lib.stride_tricks import sliding_window_view

from . import db as dbm
from .config import Paths
from .extract import DecodeError, pack, unpack
from .fulltrack import SCHEMA as TRACK_SCHEMA
from .fulltrack import Song, _store as store_track
from .fulltrack import track_f0
from .guard import SourceDisabled, Stopped
from .notes import DEFAULT_PARAMS, dedupe_phrases, f0_to_notes, phrases_from_notes, voiced_ratio

log = logging.getLogger("mfidx.refs")

SCHEMA = """
CREATE TABLE IF NOT EXISTS ref_attempt (
    song_id     INTEGER NOT NULL,
    source      TEXT NOT NULL,
    status      TEXT NOT NULL,
    ref_id      TEXT,
    ref_title   TEXT,
    uploader    TEXT,
    duration    REAL,
    match_score REAL,
    agreement   REAL,
    preview_at  REAL,
    candidates  INTEGER,
    attempts    INTEGER NOT NULL DEFAULT 0,
    error       TEXT,
    updated_at  REAL,
    PRIMARY KEY (song_id, source)
);
CREATE INDEX IF NOT EXISTS ref_attempt_updated ON ref_attempt(updated_at);
CREATE TABLE IF NOT EXISTS window_melody (
    song_id      INTEGER PRIMARY KEY,
    status       TEXT NOT NULL,
    source       TEXT NOT NULL,
    ref_id       TEXT,
    ref_title    TEXT,
    f0           BLOB,
    conf         BLOB,
    n_frames     INTEGER,
    phrases      TEXT,
    n_phrases    INTEGER,
    n_notes      INTEGER,
    voiced_ratio REAL,
    offset_s     REAL,
    agreement    REAL,
    updated_at   REAL
);
"""

FULL_SOURCES = ("soundcloud", "dailymotion", "youtube")
WINDOW_SOURCES = ("itunes",)
ALL_SOURCES = FULL_SOURCES + WINDOW_SOURCES
# Sources whose verdict a window source waits for (YouTube is too slow to wait for).
WINDOW_AFTER = ("soundcloud", "dailymotion")
ACCEPT = 0.45
FINAL = {"ok", "mismatch", "no_match", "unverifiable", "same_window"}
MAX_ERRORS = 3  # an erroring song is retried on later passes up to this many times
MAX_TRIES = 3  # candidates downloaded per song and source
VOICING = 0.04
FPS = 10
WINDOW_MIN_SHIFT_S = 8.0  # a second window that starts closer than this to Deezer's adds nothing
WINDOW_MIN_OVERLAP = 50  # voiced frames (5 s) to place a window against the Deezer preview


# ---------------------------------------------------------------------------- melody checks


def contour(f0: np.ndarray, conf: np.ndarray, voicing: float = VOICING) -> np.ndarray:
    """10 ms f0 + confidence -> 10 fps MIDI pitch (median of each 100 ms), NaN when unvoiced."""
    m = np.where((f0 > 0) & (conf > voicing), 69 + 12 * np.log2(np.maximum(f0, 1.0) / 440.0), np.nan)
    n = len(m) // 10
    if n == 0:
        return np.zeros(0, np.float32)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        return np.nanmedian(m[: n * 10].reshape(n, 10), axis=1).astype(np.float32)


def stored_contour(f0_blob: bytes, conf_blob: bytes) -> np.ndarray:
    f0 = unpack(f0_blob, "<f2").astype(np.float32)
    conf = unpack(conf_blob, "u1").astype(np.float32) / 255.0
    return contour(f0, conf)


def placement(preview: np.ndarray, full: np.ndarray, tolerance: float = 0.6) -> tuple[float, float]:
    """Best share of the preview's voiced frames within ``tolerance`` semitones of the full track
    at one lag (no transposition, no tempo change) and that lag in seconds."""
    voiced = int(np.isfinite(preview).sum())
    if voiced < 20 or len(full) < len(preview):
        return 0.0, 0.0
    win = sliding_window_view(np.nan_to_num(full, nan=1000.0), len(preview))
    hits = (np.abs(win - np.nan_to_num(preview, nan=-1000.0)[None, :]) < tolerance).sum(axis=1) / voiced
    i = int(np.argmax(hits))
    return float(hits[i]), i / FPS


def window_offset(preview: np.ndarray, window: np.ndarray, tolerance: float = 0.6) -> tuple[float, float | None]:
    """Where a second 30-s window starts relative to the Deezer preview, from their overlap.

    Returns (agreement over the overlap, start offset in seconds) or (best agreement, None) when no
    lag overlaps by at least ``WINDOW_MIN_OVERLAP`` voiced frames with agreement >= 0.5.
    """
    if len(window) == 0 or len(preview) == 0:
        return 0.0, None
    pad = np.full(len(window), np.nan, np.float32)
    ref = np.concatenate([pad, preview, pad])
    win = sliding_window_view(ref, len(window))
    w = window[None, :]
    both = np.isfinite(win) & np.isfinite(w)
    overlap = both.sum(axis=1)
    hits = (both & (np.abs(np.nan_to_num(win) - np.nan_to_num(w)) < tolerance)).sum(axis=1)
    share = np.where(overlap > 0, hits / np.maximum(overlap, 1), 0.0)
    ok = overlap >= WINDOW_MIN_OVERLAP
    if not ok.any():
        return 0.0, None
    i = int(np.argmax(np.where(ok, share, -1.0)))
    if share[i] < 0.5:
        return float(share[i]), None
    # Lag i means window frame 0 sits at preview frame i - len(window).
    return float(share[i]), (i - len(window)) / FPS


def phrases_of(f0: np.ndarray, conf: np.ndarray, source: str) -> list[dict[str, Any]]:
    notes = f0_to_notes(f0, conf, DEFAULT_PARAMS)
    return [p.to_json(source) for p in dedupe_phrases(phrases_from_notes(notes, DEFAULT_PARAMS))]


# ---------------------------------------------------------------------------- plan


@dataclass
class Target:
    song_id: int
    title: str
    artist: str
    duration: float | None
    isrc: str | None
    rank: int
    midi: bool

    def song(self) -> Song:
        return Song(self.song_id, self.title, self.artist, self.duration)


def validate_threshold(conn) -> float:
    row = conn.execute("SELECT info FROM stage_runs WHERE stage='validate'").fetchone()
    info = json.loads(row["info"]) if row and row["info"] else {}
    return float(info.get("threshold") or 0.45)


def ranked_songs(conn) -> list[Any]:
    """Resolved universe songs by popularity (rank 0 = most popular)."""
    return conn.execute(
        "SELECT s.id, s.title, s.artist, d.duration, d.isrc FROM songs s JOIN deezer d ON d.song_id=s.id"
        " WHERE s.in_universe=1 AND d.status='ok' ORDER BY s.score DESC, s.id"
    ).fetchall()


def priority(conn, top: int) -> list[Target]:
    """The ``top`` most popular resolved songs; songs without a validated MIDI melody first."""
    thr = validate_threshold(conn)
    validated = {
        r[0] for r in conn.execute("SELECT song_id FROM midi_validation GROUP BY song_id HAVING MAX(agreement) >= ?", (thr,))
    }
    out = [
        Target(r["id"], r["title"], r["artist"], r["duration"], r["isrc"], i, r["id"] in validated)
        for i, r in enumerate(ranked_songs(conn)[:top])
    ]
    out.sort(key=lambda t: (t.midi, t.rank))
    return out


class Plan:
    """Which song each source works on next (in-memory view of the state tables, thread-safe)."""

    def __init__(self, conn, top: int, sources: tuple[str, ...]) -> None:
        self.sources = sources
        self.targets = priority(conn, top)
        self.full = {
            r[0] for r in conn.execute("SELECT song_id FROM track_melody WHERE status IN ('ok', 'empty')")
        }
        self.windows = {r[0] for r in conn.execute("SELECT song_id FROM window_melody")}
        self.state: dict[tuple[int, str], tuple[str, int]] = {
            (r[0], r[1]): (r[2], r[3]) for r in conn.execute("SELECT song_id, source, status, attempts FROM ref_attempt")
        }
        self.claimed: set[tuple[int, str]] = set()
        self.cursor: dict[str, int] = {s: 0 for s in sources}
        self.off: set[str] = set()  # sources that are switched off, disabled or paused for long
        self.lock = threading.Lock()

    def _final(self, song_id: int, source: str) -> bool:
        st = self.state.get((song_id, source))
        return bool(st) and (st[0] in FINAL or st[1] >= MAX_ERRORS)

    def _failed(self, song_id: int, source: str) -> bool:
        """The source has given its verdict and found nothing (or will not run)."""
        if source not in self.sources or source in self.off:
            return True
        st = self.state.get((song_id, source))
        return self._final(song_id, source) and not (st and st[0] == "ok")

    def eligible(self, t: Target, source: str) -> bool:
        if t.song_id in self.full or (t.song_id, source) in self.claimed or self._final(t.song_id, source):
            return False
        if source in FULL_SOURCES:
            before = FULL_SOURCES[: FULL_SOURCES.index(source)]
        else:
            if t.song_id in self.windows:
                return False
            before = WINDOW_AFTER
        return all(self._failed(t.song_id, s) for s in before)

    def next(self, source: str) -> Target | None:
        with self.lock:
            i = self.cursor[source]
            # Songs before the cursor that became eligible later (an earlier source just failed
            # on them) are picked up by a rescan from the start.
            for j in list(range(i, len(self.targets))) + list(range(0, i)):
                t = self.targets[j]
                if self.eligible(t, source):
                    self.claimed.add((t.song_id, source))
                    self.cursor[source] = j + 1
                    return t
            return None

    def done(self, t: Target, source: str, status: str) -> None:
        with self.lock:
            self.claimed.discard((t.song_id, source))
            prev = self.state.get((t.song_id, source), ("", 0))
            self.state[(t.song_id, source)] = (status, prev[1] + 1)
            if status == "ok":
                if source in FULL_SOURCES:
                    self.full.add(t.song_id)
                else:
                    self.windows.add(t.song_id)

    def release(self, t: Target, source: str) -> None:
        with self.lock:
            self.claimed.discard((t.song_id, source))

    def set_off(self, source: str, off: bool) -> None:
        with self.lock:
            (self.off.add if off else self.off.discard)(source)
            if not off:
                return
            # Songs waiting for this source may now go to the next one.
            for s in self.cursor:
                self.cursor[s] = 0

    def pending(self, source: str) -> bool:
        with self.lock:
            return any(self.eligible(t, source) or (t.song_id, source) in self.claimed for t in self.targets)


# ---------------------------------------------------------------------------- the stage


class Refs:
    def __init__(self, paths: Paths, top: int, sources: tuple[str, ...], soundcloud_workers: int = 2,
                 youtube_per_hour: int = 40, half: bool = False) -> None:
        self.paths = paths
        self.top = top
        self.sources = sources
        self.stop = threading.Event()
        conn = self._conn()
        conn.executescript(TRACK_SCHEMA)
        conn.executescript(SCHEMA)
        self.plan = Plan(conn, top, sources)
        self.gpu = threading.Lock()
        self._model = None
        self._half = half
        self.clients: dict[str, Any] = {}
        if "soundcloud" in sources:
            from .soundcloud import SoundCloud

            self.clients["soundcloud"] = SoundCloud(paths, stop=self.stop)
        if "dailymotion" in sources:
            from .dailymotion import Dailymotion

            self.clients["dailymotion"] = Dailymotion(paths, stop=self.stop)
        if "youtube" in sources:
            from .youtube import YouTube

            self.clients["youtube"] = YouTube(paths, per_hour=youtube_per_hour, stop=self.stop)
        if "itunes" in sources:
            from .itunes import ITunes

            self.clients["itunes"] = ITunes(paths, stop=self.stop)
        self.workers = {"soundcloud": soundcloud_workers, "dailymotion": 1, "youtube": 1, "itunes": 1}
        self.counts: dict[str, dict[str, int]] = {s: {} for s in sources}
        self.t0 = time.time()

    def _conn(self):
        return dbm.connect(self.paths.db)

    # ---------------------------------------------------------------- GPU

    def pitch(self, audio: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        with self.gpu:
            if self._model is None:
                from .rmvpe import Rmvpe

                self._model = Rmvpe(self.paths.rmvpe_weights, half=self._half)
            return track_f0(self._model, audio)

    # ---------------------------------------------------------------- bookkeeping

    @staticmethod
    def _attempt(conn, t: Target, source: str, status: str, **cols: Any) -> None:
        row = {
            "song_id": t.song_id, "source": source, "status": status, "ref_id": None, "ref_title": None,
            "uploader": None, "duration": None, "match_score": None, "agreement": None, "preview_at": None,
            "candidates": None, "error": None, "updated_at": time.time(),
        }
        row.update(cols)
        conn.execute(
            "INSERT INTO ref_attempt(song_id, source, status, ref_id, ref_title, uploader, duration, match_score,"
            " agreement, preview_at, candidates, attempts, error, updated_at) VALUES (:song_id, :source, :status,"
            " :ref_id, :ref_title, :uploader, :duration, :match_score, :agreement, :preview_at, :candidates, 1,"
            " :error, :updated_at) ON CONFLICT(song_id, source) DO UPDATE SET status=excluded.status,"
            " ref_id=excluded.ref_id, ref_title=excluded.ref_title, uploader=excluded.uploader,"
            " duration=excluded.duration, match_score=excluded.match_score, agreement=excluded.agreement,"
            " preview_at=excluded.preview_at, candidates=excluded.candidates, attempts=ref_attempt.attempts+1,"
            " error=excluded.error, updated_at=excluded.updated_at",
            row,
        )

    @staticmethod
    def _preview(conn, song_id: int) -> np.ndarray | None:
        r = conn.execute("SELECT f0, conf FROM preview_melody WHERE song_id=? AND status='ok'", (song_id,)).fetchone()
        return stored_contour(r["f0"], r["conf"]) if r and r["f0"] else None

    # ---------------------------------------------------------------- full tracks

    def full_track(self, conn, source: str, t: Target) -> str:
        client = self.clients[source]
        preview = self._preview(conn, t.song_id)
        if preview is None or int(np.isfinite(preview).sum()) < 20:
            self._attempt(conn, t, source, "unverifiable", error="no voiced preview melody to check against")
            return "unverifiable"
        cands = client.candidates(t)
        if not cands:
            self._attempt(conn, t, source, "no_match", candidates=0)
            return "no_match"
        best: dict[str, Any] = {"agreement": -1.0}
        errors: list[str] = []
        for c in cands[:MAX_TRIES]:
            try:
                audio = client.fetch(c)
            except (DecodeError, client.Error) as e:
                errors.append(f"{c.ref_id}: {e}"[:160])
                continue
            seconds = len(audio) / 16000.0
            f0, conf = self.pitch(audio)
            del audio
            share, at = placement(preview, contour(f0, conf))
            info = {
                "ref_id": c.ref_id, "ref_title": c.title[:300], "uploader": c.uploader[:200],
                "duration": round(seconds, 2), "match_score": round(c.score, 4), "agreement": round(share, 4),
                "preview_at": round(at, 1),
            }
            if share >= ACCEPT:
                phrases = phrases_of(f0, conf, "audio")
                store_track(
                    conn, t.song_id, status="ok" if phrases else "empty", video_id=c.ref_id,
                    video_title=c.title[:300], channel=c.uploader[:200], duration=round(seconds, 2),
                    match_score=round(c.score, 3), f0=pack(f0, "<f2"),
                    conf=pack(np.round(conf * 255).clip(0, 255), "u1"), n_frames=int(len(f0)),
                    phrases=json.dumps(phrases, separators=(",", ":")), n_phrases=len(phrases),
                    n_notes=sum(len(p["notes"]) for p in phrases),
                    voiced_ratio=round(voiced_ratio(f0, conf, DEFAULT_PARAMS.voicing), 4),
                )
                self._attempt(conn, t, source, "ok", candidates=len(cands), **info)
                return "ok"
            if share > best["agreement"]:
                best = info
        if best["agreement"] >= 0:
            self._attempt(conn, t, source, "mismatch", candidates=len(cands), error="; ".join(errors)[:300] or None, **best)
            return "mismatch"
        self._attempt(conn, t, source, "error", candidates=len(cands), error="; ".join(errors)[:300])
        return "error"

    # ---------------------------------------------------------------- second windows

    def window(self, conn, source: str, t: Target) -> str:
        client = self.clients[source]
        c = client.match(t.title, t.artist, t.duration)
        if c is None:
            self._attempt(conn, t, source, "no_match")
            return "no_match"
        audio = client.fetch(c["previewUrl"])
        f0, conf = self.pitch(audio)
        del audio
        preview = self._preview(conn, t.song_id)
        share, offset = window_offset(preview, contour(f0, conf)) if preview is not None else (0.0, None)
        title = f"{c.get('artistName')} - {c.get('trackName')}"[:300]
        ref_id = f"itunes:{c.get('trackId')}"
        if offset is not None and abs(offset) < WINDOW_MIN_SHIFT_S:
            self._attempt(conn, t, source, "same_window", ref_id=ref_id, ref_title=title, agreement=round(share, 4),
                          preview_at=round(offset, 1))
            return "same_window"
        phrases = phrases_of(f0, conf, "preview")
        conn.execute(
            "INSERT INTO window_melody(song_id, status, source, ref_id, ref_title, f0, conf, n_frames, phrases,"
            " n_phrases, n_notes, voiced_ratio, offset_s, agreement, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,"
            " ?, ?, ?, ?, ?, ?) ON CONFLICT(song_id) DO UPDATE SET status=excluded.status, source=excluded.source,"
            " ref_id=excluded.ref_id, ref_title=excluded.ref_title, f0=excluded.f0, conf=excluded.conf,"
            " n_frames=excluded.n_frames, phrases=excluded.phrases, n_phrases=excluded.n_phrases,"
            " n_notes=excluded.n_notes, voiced_ratio=excluded.voiced_ratio, offset_s=excluded.offset_s,"
            " agreement=excluded.agreement, updated_at=excluded.updated_at",
            (t.song_id, "ok" if phrases else "empty", source, ref_id, title, pack(f0, "<f2"),
             pack(np.round(conf * 255).clip(0, 255), "u1"), int(len(f0)), json.dumps(phrases, separators=(",", ":")),
             len(phrases), sum(len(p["notes"]) for p in phrases), round(voiced_ratio(f0, conf, VOICING), 4),
             offset, round(share, 4), time.time()),
        )
        self._attempt(conn, t, source, "ok", ref_id=ref_id, ref_title=title, agreement=round(share, 4),
                      preview_at=None if offset is None else round(offset, 1))
        return "ok"

    # ---------------------------------------------------------------- workers

    def _work(self, source: str, n: int) -> None:
        conn = self._conn()
        client = self.clients[source]
        idle = 0
        while not self.stop.is_set():
            states = [g.summary() for g in client.guards]
            disabled = [s for s in states if s["disabled"]]
            off = bool(disabled) or any(s["paused_for_s"] > 900 for s in states)
            if off != (source in self.plan.off):
                self.plan.set_off(source, off)
            if disabled:
                log.error("%s worker %d: source disabled (%s); stopping", source, n, disabled[0]["reason"])
                return
            t = self.plan.next(source)
            if t is None:
                if not self.plan.pending(source) and idle > 3:
                    log.info("%s worker %d: nothing left", source, n)
                    return
                idle += 1
                self.stop.wait(120)
                continue
            idle = 0
            try:
                if source in FULL_SOURCES:
                    status = self.full_track(conn, source, t)
                else:
                    status = self.window(conn, source, t)
            except (Stopped, SourceDisabled):
                self.plan.release(t, source)
                continue
            except client.Refused as e:
                # The breaker is open now; the song is tried again later.
                self.plan.release(t, source)
                log.warning("%s refused (%s)", source, e)
                continue
            except Exception as e:  # noqa: BLE001 - keep the crawl alive
                log.exception("%s %d failed", source, t.song_id)
                self._attempt(conn, t, source, "error", error=f"{type(e).__name__}: {e}"[:300])
                status = "error"
            self.plan.done(t, source, status)
            c = self.counts[source]
            c[status] = c.get(status, 0) + 1

    def _report(self) -> None:
        while not self.stop.wait(600):
            hours = (time.time() - self.t0) / 3600
            parts = []
            for s in self.sources:
                c = self.counts[s]
                done = sum(c.values())
                parts.append(f"{s} {done} ({c.get('ok', 0)} ok, {done / max(hours, 1e-6):.0f}/h)")
            log.info("refs: %s; full tracks %d of %d targets", "; ".join(parts), len(self.plan.full & {
                t.song_id for t in self.plan.targets}), len(self.plan.targets))

    def run(self) -> dict[str, Any]:
        log.info("refs: %d target songs, sources %s", len(self.plan.targets), ",".join(self.sources))
        threads = [threading.Thread(target=self._report, daemon=True, name="report")]
        for s in self.sources:
            for n in range(self.workers.get(s, 1)):
                threads.append(threading.Thread(target=self._work, args=(s, n), name=f"{s}-{n}"))
        for th in threads:
            th.start()
        try:
            for th in threads[1:]:
                while th.is_alive():
                    th.join(5)
        except KeyboardInterrupt:
            log.warning("refs: stopping")
            self.stop.set()
            for th in threads[1:]:
                th.join(60)
        self.stop.set()
        return {"targets": len(self.plan.targets), "counts": self.counts}


def run(paths: Paths, top: int = 15000, sources: tuple[str, ...] = ALL_SOURCES, soundcloud_workers: int = 2,
        youtube_per_hour: int = 40, half: bool = False) -> dict[str, Any]:
    return Refs(paths, top, sources, soundcloud_workers, youtube_per_hour, half).run()


# ---------------------------------------------------------------------------- progress


def _eval_songs(paths: Paths, conn) -> dict[str, list[int]]:
    """Song ids of the evaluation sets, to report their coverage (never to prioritize them)."""
    out: dict[str, list[int]] = {}
    groups = paths.eval / "chad" / "groups.json"
    if groups.exists():
        dz = {int(g["deezer_id"]) for g in json.loads(groups.read_text()).values() if g.get("deezer_id")}
        out["chad"] = [r[0] for r in conn.execute("SELECT song_id, deezer_id FROM deezer WHERE status='ok'") if r[1] in dz]
    user = paths.eval / "user-songs.json"  # [{"artist": …, "title": …}], private, optional
    if user.exists():
        from .normalize import song_key

        keys = {song_key(x["artist"], x["title"]) for x in json.loads(user.read_text())}
        out["user"] = [r[0] for r in conn.execute("SELECT id, key FROM songs") if r[1] in keys]
    return out


def status(paths: Paths, window: float = 3600.0) -> dict[str, Any]:
    conn = dbm.connect(paths.db)
    conn.executescript(TRACK_SCHEMA)
    conn.executescript(SCHEMA)
    now = time.time()
    by_source: dict[str, Any] = {}
    for r in conn.execute("SELECT source, status, COUNT(*) n FROM ref_attempt GROUP BY source, status"):
        by_source.setdefault(r["source"], {"by_status": {}})["by_status"][r["status"]] = r["n"]
    for r in conn.execute(
        "SELECT source, COUNT(*) n, SUM(status='ok') ok FROM ref_attempt WHERE updated_at>? GROUP BY source",
        (now - window,),
    ):
        d = by_source.setdefault(r["source"], {"by_status": {}})
        d["songs_last_hour"] = r["n"]
        d["ok_last_hour"] = r["ok"]
    guards = {}
    for f in sorted(paths.state.glob("*.guard")):
        try:
            g = json.loads(f.read_text() or "{}")
        except (OSError, ValueError):
            continue
        guards[f.stem] = {
            "requests": g.get("requests", 0),
            "recent": len(g.get("t", [])),
            "paused_for_min": round(max(0.0, float(g.get("pause_until", 0)) - now) / 60, 1),
            "blocks_in_a_row": g.get("blocks", 0),
            "total_blocks": g.get("total_blocks", 0),
            "disabled": bool(g.get("disabled")),
            "reason": g.get("reason"),
        }
    full = {}
    for r in conn.execute("SELECT song_id, video_id FROM track_melody WHERE status='ok'"):
        vid = r["video_id"] or ""
        full[r["song_id"]] = vid.split(":", 1)[0] if ":" in vid else "youtube-3b"
    windows = {r[0] for r in conn.execute("SELECT song_id FROM window_melody WHERE status='ok'")}
    ranked = [r["id"] for r in ranked_songs(conn)]
    tiers = {}
    rate_ok = sum(d.get("ok_last_hour") or 0 for s, d in by_source.items() if s in FULL_SOURCES)
    rate_songs = by_source.get("soundcloud", {}).get("songs_last_hour") or 0
    tried = {r[0] for r in conn.execute("SELECT DISTINCT song_id FROM ref_attempt WHERE source='soundcloud'")}
    for n in (1000, 5000, 15000):
        ids = ranked[:n]
        have = sum(1 for i in ids if i in full)
        left = sum(1 for i in ids if i not in full and i not in tried)
        tiers[f"top{n}"] = {
            "full_track": have,
            "full_track_share": round(have / max(1, len(ids)), 3),
            "window_only": sum(1 for i in ids if i not in full and i in windows),
            "not_tried_yet": left,
            "eta_first_pass_h": round(left / rate_songs, 1) if rate_songs else None,
        }
    evals = {}
    rank_of = {i: k for k, i in enumerate(ranked)}
    for name, ids in _eval_songs(paths, conn).items():
        evals[name] = {
            "songs": len(ids),
            "full_track": sum(1 for i in ids if i in full),
            "window": sum(1 for i in ids if i not in full and i in windows),
            "in_top15000": sum(1 for i in ids if rank_of.get(i, 10**9) < 15000),
        }
    return {
        "sources": by_source,
        "full_tracks": {"total": len(full), "by_source": {s: sum(1 for v in full.values() if v == s) for s in set(full.values())}},
        "full_tracks_per_hour": rate_ok,
        "tiers": tiers,
        "eval_coverage": evals,
        "guards": guards,
    }


# ---------------------------------------------------------------------------- export

WINDOW_START_MS = 60_000  # second windows are placed after the Deezer preview, with a gap
PREVIEW_S = 30.0


def window_phrases(conn) -> dict[int, list[dict[str, Any]]]:
    """Phrases of the second windows, without the part they share with the Deezer preview.

    They keep the source "preview" and start at ``WINDOW_START_MS`` so that a matcher that
    places a source's phrases on one timeline never overlaps them with Deezer's.
    """
    out: dict[int, list[dict[str, Any]]] = {}
    if not conn.execute("SELECT name FROM sqlite_master WHERE name='window_melody'").fetchone():
        return out
    for r in conn.execute("SELECT song_id, phrases, offset_s FROM window_melody WHERE status='ok' AND phrases IS NOT NULL"):
        keep = []
        for p in json.loads(r["phrases"]):
            if r["offset_s"] is not None and p["notes"]:
                a = r["offset_s"] + p["start_ms"] / 1000.0
                b = a + (p["notes"][-1][1] + p["notes"][-1][2]) / 1000.0
                inside = max(0.0, min(b, PREVIEW_S) - max(a, 0.0))
                if inside > 0.5 * max(b - a, 1e-3):
                    continue
            keep.append({**p, "start_ms": WINDOW_START_MS + int(p["start_ms"])})
        if keep:
            out[r["song_id"]] = keep
    return out
