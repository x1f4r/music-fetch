"""Evergreen tunes: themes, classics, children's songs, carols, anthems and memes.

Charts miss most of what people hum without thinking: Happy Birthday, Für Elise, the Tetris
music, the Star Wars theme. ``data/evergreen.toml`` lists them by hand. Each item becomes a song
of origin ``evergreen`` (or lifts the score of the chart song it already is), so the normal
stages resolve it on Deezer, extract its preview and look for a full recording (stage 3c).
Public-domain melodies also carry a hand-written ABC transcription of the part people hum; it is
stored in ``symbolic_melody`` and exported as clean "midi" phrases. Every item records its
licence.

Item fields (``[[item]]``): ``title``, ``artist`` (composer or performer as Deezer would credit
it), ``kind`` ("symbolic": transcription only, "audio": recording only, "both"), ``fame`` (1 =
known to nearly everyone, 2 = very well known, 3 = well known; sets the popularity),
``licence`` and for symbolic items ``abc``; optional ``aliases`` (other titles, e.g. the German
one), ``artists`` (other credits, e.g. "Peter Ilyich Tchaikovsky"), ``category`` and ``deezer``:
a Deezer track id that resolve takes as is (classical recordings are credited to performers,
and the best-ranked match is sometimes a remix), or 0 for "no recording; transcription only".
Recordings come in ``[[group]]`` tables that share ``kind``, ``licence`` and ``category`` and
list their items as ``[title, artist, fame]``, ``[title, artist, fame, [aliases]]`` or inline
tables with the item fields.

Pinning an item to another Deezer track (or to none) clears the song's Deezer, preview and
stage 3c rows, so ``resolve``, ``extract`` and ``refs`` redo them.
"""

from __future__ import annotations

import json
import logging
import time
import tomllib
from dataclasses import replace
from importlib import resources
from pathlib import Path
from typing import Any

from . import db as dbm
from .abc import to_notes
from .config import Paths
from .normalize import song_key
from .notes import DEFAULT_PARAMS, Note, dedupe_phrases, phrases_from_notes

log = logging.getLogger("mfidx.evergreen")

SCHEMA = """
CREATE TABLE IF NOT EXISTS evergreen (
    item_id    TEXT PRIMARY KEY,
    song_id    INTEGER,
    title      TEXT NOT NULL,
    artist     TEXT NOT NULL,
    kind       TEXT NOT NULL,
    category   TEXT,
    fame       INTEGER,
    licence    TEXT NOT NULL,
    created    INTEGER NOT NULL DEFAULT 0,
    updated_at REAL
);
CREATE TABLE IF NOT EXISTS symbolic_melody (
    song_id    INTEGER PRIMARY KEY,
    item_id    TEXT NOT NULL,
    licence    TEXT NOT NULL,
    phrases    TEXT NOT NULL,
    n_phrases  INTEGER,
    n_notes    INTEGER,
    updated_at REAL
);
"""

KINDS = ("symbolic", "audio", "both")
# Popularity score by fame (the chart scores put rank 600 at ~0.99, 1,600 at ~0.95, 3,500 at ~0.85).
FAME_SCORE = {1: 0.99, 2: 0.95, 3: 0.85}
# Symbolic phrases: split at rests and at 12 s, like the matcher's reference phrases.
PHRASE_PARAMS = replace(DEFAULT_PARAMS, max_phrase_ms=12000, min_phrase_notes=5)


def load(path: Path | None = None) -> list[dict[str, Any]]:
    if path is None:
        raw = resources.files("melody_index").joinpath("data/evergreen.toml").read_bytes()
    else:
        raw = path.read_bytes()
    doc = tomllib.loads(raw.decode("utf-8"))
    items = list(doc.get("item", []))
    for g in doc.get("group", []):
        for entry in g.get("items", []):
            it = {"kind": g.get("kind", "audio"), "licence": g.get("licence"), "category": g.get("category")}
            if isinstance(entry, dict):
                it.update(entry)
            elif 3 <= len(entry) <= 4:
                it.update(title=entry[0], artist=entry[1], fame=entry[2])
                if len(entry) == 4:
                    it["aliases"] = list(entry[3])
            else:
                raise ValueError(f"evergreen group {g.get('category')!r}: bad entry {entry!r}")
            items.append(it)
    seen: set[str] = set()
    for it in items:
        for k in ("title", "artist", "kind", "fame", "licence"):
            if not it.get(k):
                raise ValueError(f"evergreen item {it.get('title')!r} lacks {k!r}")
        if it["kind"] not in KINDS:
            raise ValueError(f"evergreen item {it['title']!r}: kind must be one of {KINDS}")
        if int(it["fame"]) not in FAME_SCORE:
            raise ValueError(f"evergreen item {it['title']!r}: fame must be 1, 2 or 3")
        if it["kind"] in ("symbolic", "both") and not it.get("abc"):
            raise ValueError(f"evergreen item {it['title']!r} needs an abc transcription")
        it["id"] = song_key(it["artist"], it["title"])
        if it["id"] in seen:
            raise ValueError(f"duplicate evergreen item {it['title']!r} / {it['artist']!r}")
        seen.add(it["id"])
    return items


def phrases(abc: str) -> list[dict[str, Any]]:
    """Reference phrases ("midi" source) of a transcription."""
    notes = [Note(p, on, dur) for p, on, dur in to_notes(abc)]
    out = dedupe_phrases(phrases_from_notes(notes, PHRASE_PARAMS))
    return [p.to_json("midi") for p in out]


def run(paths: Paths, file: Path | None = None) -> dict[str, Any]:
    items = load(file)
    conn = dbm.connect(paths.db)
    conn.executescript(SCHEMA)
    counts = {"items": len(items), "new_songs": 0, "chart_songs_lifted": 0, "already_listed": 0, "symbolic": 0}
    now = time.time()
    with dbm.Tx(conn):
        for it in items:
            score = FAME_SCORE[int(it["fame"])]
            credits = [it["artist"]] + list(it.get("artists", []))
            titles = [it["title"]] + list(it.get("aliases", []))
            aliases = [[a, t] for a in credits for t in titles if [a, t] != [it["artist"], it["title"]]]
            pin = it.get("deezer")
            row = conn.execute("SELECT id, origin, score FROM songs WHERE key=?", (it["id"],)).fetchone()
            created = 0
            if row is None:
                cur = conn.execute(
                    "INSERT INTO songs(key, title, artist, year, score, popularity, signals, aliases, deezer_hint,"
                    " origin, in_universe) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, 'evergreen', 1)",
                    (it["id"], it["title"], it["artist"], it.get("year"), score,
                     json.dumps({"evergreen": it.get("category")}), json.dumps(aliases, ensure_ascii=False),
                     pin or None),
                )
                song_id, created = cur.lastrowid, 1
                counts["new_songs"] += 1
                if pin == 0:
                    _no_recording(conn, song_id, now)
            else:
                song_id = row["id"]
                if row["origin"] == "evergreen":
                    conn.execute("UPDATE songs SET score=?, in_universe=1, aliases=?, deezer_hint=? WHERE id=?",
                                 (score, json.dumps(aliases, ensure_ascii=False), pin or None, song_id))
                    counts["already_listed"] += 1
                    if pin is not None and _repin(conn, song_id, pin, now):
                        counts["repinned"] = counts.get("repinned", 0) + 1
                else:
                    conn.execute("UPDATE songs SET score=MAX(score, ?), in_universe=1 WHERE id=?", (score, song_id))
                    counts["chart_songs_lifted"] += 1
            conn.execute(
                "INSERT INTO evergreen(item_id, song_id, title, artist, kind, category, fame, licence, created, updated_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(item_id) DO UPDATE SET song_id=excluded.song_id,"
                " title=excluded.title, artist=excluded.artist, kind=excluded.kind, category=excluded.category,"
                " fame=excluded.fame, licence=excluded.licence, updated_at=excluded.updated_at",
                (it["id"], song_id, it["title"], it["artist"], it["kind"], it.get("category"), int(it["fame"]),
                 it["licence"], created, now),
            )
            if it["kind"] in ("symbolic", "both"):
                ph = phrases(it["abc"])
                conn.execute(
                    "INSERT INTO symbolic_melody(song_id, item_id, licence, phrases, n_phrases, n_notes, updated_at)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(song_id) DO UPDATE SET item_id=excluded.item_id,"
                    " licence=excluded.licence, phrases=excluded.phrases, n_phrases=excluded.n_phrases,"
                    " n_notes=excluded.n_notes, updated_at=excluded.updated_at",
                    (song_id, it["id"], it["licence"], json.dumps(ph, separators=(",", ":")), len(ph),
                     sum(len(p["notes"]) for p in ph), now),
                )
                counts["symbolic"] += 1
        # Items taken off the list leave the universe (chart songs keep their lifted score until
        # the next universe run).
        listed = {it["id"] for it in items}
        gone = [r for r in conn.execute("SELECT item_id, song_id FROM evergreen") if r["item_id"] not in listed]
        for r in gone:
            conn.execute("UPDATE songs SET in_universe=0 WHERE id=? AND origin='evergreen'", (r["song_id"],))
            conn.execute("DELETE FROM symbolic_melody WHERE song_id=?", (r["song_id"],))
            conn.execute("DELETE FROM evergreen WHERE item_id=?", (r["item_id"],))
        counts["removed"] = len(gone)
    log.info("evergreen: %s", counts)
    return counts


def _clear(conn, song_id: int) -> None:
    """Forget a song's recording: Deezer match, preview melody and stage 3c results."""
    conn.execute("DELETE FROM deezer WHERE song_id=?", (song_id,))
    conn.execute("DELETE FROM preview_melody WHERE song_id=?", (song_id,))
    for table in ("ref_attempt", "window_melody"):
        if conn.execute("SELECT name FROM sqlite_master WHERE name=?", (table,)).fetchone():
            conn.execute(f"DELETE FROM {table} WHERE song_id=?", (song_id,))
    if conn.execute("SELECT name FROM sqlite_master WHERE name='track_melody'").fetchone():
        conn.execute("DELETE FROM track_melody WHERE song_id=?", (song_id,))


def _no_recording(conn, song_id: int, now: float) -> None:
    """A transcription-only item: a Deezer row that resolve leaves alone."""
    conn.execute(
        "INSERT INTO deezer(song_id, status, attempts, error, updated_at) VALUES (?, 'skipped', 1,"
        " 'evergreen item without a recording', ?)",
        (song_id, now),
    )


def _repin(conn, song_id: int, pin: int, now: float) -> bool:
    """Make the song's Deezer match follow the item's pin; True when something was cleared."""
    row = conn.execute("SELECT status, deezer_id FROM deezer WHERE song_id=?", (song_id,)).fetchone()
    if pin == 0:
        if row is not None and row["status"] == "skipped":
            return False
        _clear(conn, song_id)
        _no_recording(conn, song_id, now)
        return True
    if row is not None and row["status"] == "ok" and row["deezer_id"] == pin:
        return False
    _clear(conn, song_id)
    return True


def symbolic_phrases(conn) -> dict[int, list[dict[str, Any]]]:
    if not conn.execute("SELECT name FROM sqlite_master WHERE name='symbolic_melody'").fetchone():
        return {}
    return {r["song_id"]: json.loads(r["phrases"]) for r in conn.execute("SELECT song_id, phrases FROM symbolic_melody")}
