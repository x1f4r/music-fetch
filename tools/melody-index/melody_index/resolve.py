"""Stage 2: find the Deezer track for every song (id, ISRC, duration, preview)."""

from __future__ import annotations

import json
import logging
import queue
import re
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

from . import db as dbm
from .config import Paths
from .net import DeezerClient, DeezerError, DeezerNotFound
from .normalize import (
    NON_ORIGINAL_RE,
    artist_similarity,
    primary_artist,
    title_core,
    title_similarity,
    title_variants,
)

log = logging.getLogger("mfidx.resolve")

MIN_TITLE = 0.82
MIN_ARTIST = 0.75
ACCEPT = 0.80
_GROUP_RE = re.compile(r"\s*[\(\[][^\)\]]*[\)\]]")
_VERSION_HINT_RE = re.compile(r"\blive\b|remix|\bedit\b|sped up|slowed|acoustic|version", re.I)


@dataclass
class Target:
    song_id: int
    title: str
    artist: str
    aliases: list[tuple[str, str]]
    deezer_hint: int | None
    pinned: bool = False  # an evergreen item names its Deezer track: take it as is


def queries(t: Target) -> list[str]:
    """Search strings, most specific first (Deezer's fielded syntax no longer works)."""
    out: list[str] = []
    pairs = [(t.artist, t.title)] + [a for a in t.aliases if a != (t.artist, t.title)]
    for artist, title in pairs[:3]:
        pa = primary_artist(artist).replace('"', "")
        core = title_core(title).replace('"', "")
        loose = _GROUP_RE.sub("", core).strip() or core
        for q in (f"{pa} {core}", f"{pa} {loose}"):
            if q not in out:
                out.append(q)
    core = _GROUP_RE.sub("", title_core(t.title)).strip()
    if len(core) >= 4 and core not in out:
        out.append(core)
    return out[:4]


def score(t: Target, cand: dict[str, Any]) -> float:
    """0..~1.05 match quality of a Deezer track object for the target; 0 = reject."""
    c_title = cand.get("title") or ""
    c_short = cand.get("title_short") or c_title
    c_artist = (cand.get("artist") or {}).get("name") or ""
    best = 0.0
    for artist, title in [(t.artist, t.title)] + list(t.aliases):
        ts = max(title_similarity(title, c_title), title_similarity(title, c_short))
        if ts < MIN_TITLE:
            continue
        ar = artist_similarity(artist, c_artist)
        if ar < MIN_ARTIST:
            continue
        best = max(best, 0.6 * ts + 0.4 * ar)
    if best == 0.0:
        return 0.0
    decorated = f"{c_title} {cand.get('title_version') or ''} {c_artist} {(cand.get('album') or {}).get('title') or ''}"
    if NON_ORIGINAL_RE.search(decorated) and not NON_ORIGINAL_RE.search(t.title):
        best -= 0.25
    elif _VERSION_HINT_RE.search(
        f"{c_title} {cand.get('title_version') or ''}"
    ) and not _VERSION_HINT_RE.search(t.title):
        best -= 0.04
    rank = cand.get("rank") or 0
    best += 0.05 * min(1.0, rank / 1_000_000)
    return round(best, 4)


def pick(t: Target, cands: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, float]:
    scored = [(score(t, c), c) for c in cands]
    scored = [(s, c) for s, c in scored if s >= ACCEPT]
    if not scored:
        return None, 0.0
    top = max(s for s, _ in scored)
    # Among near-equal matches prefer one that has a preview, then the most streamed.
    near = [(s, c) for s, c in scored if s >= top - 0.05]
    near.sort(
        key=lambda sc: (bool(sc[1].get("preview")), sc[0], sc[1].get("rank") or 0), reverse=True
    )
    return near[0][1], near[0][0]


def resolve_one(client: DeezerClient, t: Target) -> dict[str, Any]:
    """Returns a row for the ``deezer`` table."""
    row: dict[str, Any] = {"song_id": t.song_id, "updated_at": time.time()}
    chosen: dict[str, Any] | None = None
    match = 0.0
    used_query = None
    if t.deezer_hint:
        try:
            hinted = client.track(t.deezer_hint)
            s = 1.0 if t.pinned else score(t, hinted)
            if s >= ACCEPT - 0.1:
                chosen, match, used_query = hinted, s, f"{'pinned' if t.pinned else 'hint'}:{t.deezer_hint}"
        except DeezerNotFound:
            pass
    if chosen is None:
        for q in queries(t):
            cands = client.search(q)
            cand, s = pick(t, cands)
            if cand is not None and s > match:
                chosen, match, used_query = cand, s, q
            if match >= 0.95:
                break
    if chosen is None:
        row.update(status="not_found", query=json.dumps(queries(t), ensure_ascii=False))
        return row
    full = chosen if "isrc" in chosen else client.track(int(chosen["id"]))
    preview = full.get("preview") or ""
    row.update(
        status="ok",
        deezer_id=int(full["id"]),
        dz_title=full.get("title"),
        dz_artist=(full.get("artist") or {}).get("name"),
        dz_album=(full.get("album") or {}).get("title"),
        duration=full.get("duration"),
        dz_rank=full.get("rank"),
        has_preview=1 if preview else 0,
        isrc=full.get("isrc") or None,
        release_date=full.get("release_date") or (full.get("album") or {}).get("release_date"),
        bpm=full.get("bpm") or None,
        gain=full.get("gain"),
        match_score=match,
        query=used_query,
        preview_url=preview or None,
        url_fetched_at=time.time() if preview else None,
    )
    return row


_COLUMNS = (
    "song_id",
    "status",
    "deezer_id",
    "dz_title",
    "dz_artist",
    "dz_album",
    "duration",
    "dz_rank",
    "has_preview",
    "isrc",
    "release_date",
    "bpm",
    "gain",
    "match_score",
    "query",
    "preview_url",
    "url_fetched_at",
    "error",
    "updated_at",
)


def _store(conn, rows: list[dict[str, Any]]) -> None:
    cols = ", ".join(_COLUMNS)
    marks = ", ".join(f":{c}" for c in _COLUMNS)
    updates = ", ".join(f"{c}=excluded.{c}" for c in _COLUMNS if c != "song_id")
    with dbm.Tx(conn):
        conn.executemany(
            f"INSERT INTO deezer({cols}, attempts) VALUES ({marks}, 1) ON CONFLICT(song_id) DO UPDATE SET"
            f" {updates}, attempts=deezer.attempts+1",
            [{c: r.get(c) for c in _COLUMNS} for r in rows],
        )


def pending(conn, limit: int, max_attempts: int = 3) -> list[Target]:
    rows = conn.execute(
        "SELECT s.id, s.title, s.artist, s.aliases, s.deezer_hint, s.origin FROM songs s"
        " LEFT JOIN deezer d ON d.song_id=s.id"
        " WHERE s.in_universe=1 AND (d.song_id IS NULL OR (d.status='error' AND d.attempts<?))"
        " ORDER BY s.score DESC LIMIT ?",
        (max_attempts, limit),
    ).fetchall()
    out = []
    for r in rows:
        aliases = [
            tuple(a)
            for a in json.loads(r["aliases"] or "[]")
            if isinstance(a, list) and len(a) == 2
        ]
        sides = [(r["artist"], v) for v in title_variants(r["title"])[1:]]
        out.append(
            Target(r["id"], r["title"], r["artist"], sides + aliases, r["deezer_hint"],
                   pinned=r["origin"] == "evergreen" and bool(r["deezer_hint"]))
        )
    return out


def run(
    paths: Paths, workers: int = 6, limit: int | None = None, retry_not_found: bool = False
) -> dict[str, Any]:
    conn = dbm.connect(paths.db)
    dbm.stage_started(conn, "resolve")
    if retry_not_found:
        # After matcher changes: search again for songs that found nothing last time.
        n = conn.execute("DELETE FROM deezer WHERE status='not_found'").rowcount
        log.info("resolve: retrying %d songs that were not found", n)
    client = DeezerClient(paths.state)
    done = 0
    ok = 0
    t0 = time.time()
    results: queue.Queue[dict[str, Any]] = queue.Queue()

    def work(t: Target) -> None:
        try:
            results.put(resolve_one(client, t))
        except DeezerError as e:
            results.put(
                {
                    "song_id": t.song_id,
                    "status": "error",
                    "error": str(e)[:300],
                    "updated_at": time.time(),
                }
            )
        except Exception as e:  # noqa: BLE001 - keep the pool alive, record the failure
            log.exception("resolve %s failed", t.song_id)
            results.put(
                {
                    "song_id": t.song_id,
                    "status": "error",
                    "error": f"{type(e).__name__}: {e}"[:300],
                    "updated_at": time.time(),
                }
            )

    with ThreadPoolExecutor(max_workers=workers) as pool:
        while True:
            batch_size = 200 if limit is None else min(200, limit - done)
            if batch_size <= 0:
                break
            targets = pending(conn, batch_size)
            if not targets:
                break
            futures = [pool.submit(work, t) for t in targets]
            buffer: list[dict[str, Any]] = []
            for f in futures:
                f.result()
                while not results.empty():
                    buffer.append(results.get())
                if len(buffer) >= 25:
                    _store(conn, buffer)
                    done += len(buffer)
                    ok += sum(1 for r in buffer if r.get("status") == "ok")
                    buffer = []
            while not results.empty():
                buffer.append(results.get())
            if buffer:
                _store(conn, buffer)
                done += len(buffer)
                ok += sum(1 for r in buffer if r.get("status") == "ok")
            rate = done / max(1e-6, time.time() - t0) * 60
            log.info(
                "resolve: %d done (%d ok, %.1f%%), %.1f songs/min, %d requests, %d quota hits",
                done,
                ok,
                100 * ok / max(1, done),
                rate,
                client.requests,
                client.quota_hits,
            )
    info = {
        "processed": done,
        "ok": ok,
        "seconds": round(time.time() - t0, 1),
        "requests": client.requests,
    }
    if limit is None:
        dbm.stage_finished(conn, "resolve", info)
    log.info("resolve finished: %s", info)
    return info


def fill_years(paths: Paths, workers: int = 4) -> dict[str, Any]:
    """Release dates for resolved songs without a chart year (search results carry none)."""
    conn = dbm.connect(paths.db)
    client = DeezerClient(paths.state)
    rows = conn.execute(
        "SELECT d.song_id, d.deezer_id FROM deezer d JOIN songs s ON s.id=d.song_id"
        " WHERE d.status='ok' AND d.release_date IS NULL AND s.year IS NULL AND s.in_universe=1"
        " ORDER BY s.score DESC"
    ).fetchall()
    t0 = time.time()

    def fetch(row) -> tuple[str, str | None, int]:
        try:
            track = client.track(int(row["deezer_id"]))
        except DeezerError:
            return "", None, row["song_id"]
        date = track.get("release_date") or (track.get("album") or {}).get("release_date") or ""
        return date, track.get("isrc") or None, row["song_id"]

    done = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for start in range(0, len(rows), 500):
            updates = list(pool.map(fetch, rows[start : start + 500]))
            with dbm.Tx(conn):
                conn.executemany(
                    "UPDATE deezer SET release_date=?, isrc=COALESCE(?, isrc) WHERE song_id=?",
                    updates,
                )
            done += len(updates)
            log.info(
                "years: %d/%d, %.0f songs/min", done, len(rows), done / (time.time() - t0) * 60
            )
    info = {"songs": done, "seconds": round(time.time() - t0, 1), "requests": client.requests}
    log.info("years finished: %s", info)
    return info
