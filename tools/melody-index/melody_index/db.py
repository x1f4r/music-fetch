"""SQLite build state shared by all stages (WAL, safe for several processes)."""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS chart_entries (
    source      TEXT NOT NULL,
    chart       TEXT NOT NULL,
    artist      TEXT NOT NULL,
    title       TEXT NOT NULL,
    rank        INTEGER,
    peak        INTEGER,
    weeks       INTEGER,
    streams     INTEGER,
    points      REAL,
    first_date  TEXT,
    last_date   TEXT,
    year        INTEGER,
    deezer_id   INTEGER,
    PRIMARY KEY (source, chart, artist, title)
);

CREATE TABLE IF NOT EXISTS songs (
    id          INTEGER PRIMARY KEY,
    key         TEXT NOT NULL UNIQUE,
    title       TEXT NOT NULL,
    artist      TEXT NOT NULL,
    year        INTEGER,
    score       REAL NOT NULL DEFAULT 0,
    popularity  REAL NOT NULL DEFAULT 0,
    signals     TEXT,
    aliases     TEXT,
    deezer_hint INTEGER,
    origin      TEXT NOT NULL DEFAULT 'chart',
    in_universe INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS songs_universe ON songs(in_universe, score DESC);

CREATE TABLE IF NOT EXISTS deezer (
    song_id        INTEGER PRIMARY KEY,
    status         TEXT NOT NULL,
    deezer_id      INTEGER,
    dz_title       TEXT,
    dz_artist      TEXT,
    dz_album       TEXT,
    duration       INTEGER,
    dz_rank        INTEGER,
    has_preview    INTEGER,
    isrc           TEXT,
    release_date   TEXT,
    bpm            REAL,
    gain           REAL,
    match_score    REAL,
    query          TEXT,
    preview_url    TEXT,
    url_fetched_at REAL,
    attempts       INTEGER NOT NULL DEFAULT 0,
    error          TEXT,
    updated_at     REAL
);
CREATE INDEX IF NOT EXISTS deezer_status ON deezer(status, updated_at);

CREATE TABLE IF NOT EXISTS preview_melody (
    song_id      INTEGER PRIMARY KEY,
    status       TEXT NOT NULL,
    deezer_id    INTEGER,
    f0           BLOB,
    conf         BLOB,
    n_frames     INTEGER,
    phrases      TEXT,
    n_phrases    INTEGER,
    n_notes      INTEGER,
    voiced_ratio REAL,
    attempts     INTEGER NOT NULL DEFAULT 0,
    error        TEXT,
    updated_at   REAL
);
CREATE INDEX IF NOT EXISTS preview_melody_status ON preview_melody(status, updated_at);

CREATE TABLE IF NOT EXISTS midi_files (
    md5         TEXT PRIMARY KEY,
    path        TEXT NOT NULL,
    origin      TEXT NOT NULL,
    artist      TEXT,
    title       TEXT,
    key         TEXT,
    msd_id      TEXT,
    match_score REAL
);
CREATE INDEX IF NOT EXISTS midi_files_key ON midi_files(key);

CREATE TABLE IF NOT EXISTS midi_melody (
    md5        TEXT PRIMARY KEY,
    song_id    INTEGER,
    status     TEXT NOT NULL,
    tracks     TEXT,
    phrases    TEXT,
    n_phrases  INTEGER,
    n_notes    INTEGER,
    confidence REAL,
    error      TEXT,
    updated_at REAL
);
CREATE INDEX IF NOT EXISTS midi_melody_song ON midi_melody(song_id);

CREATE TABLE IF NOT EXISTS midi_validation (
    song_id    INTEGER NOT NULL,
    md5        TEXT NOT NULL,
    track      INTEGER NOT NULL,
    agreement  REAL,
    cost       REAL,
    shift      INTEGER,
    frames     INTEGER,
    updated_at REAL,
    PRIMARY KEY (song_id, md5, track)
);

CREATE TABLE IF NOT EXISTS stage_runs (
    stage       TEXT PRIMARY KEY,
    status      TEXT NOT NULL,
    started_at  REAL,
    finished_at REAL,
    info        TEXT
);
"""


def connect(path: Path) -> sqlite3.Connection:
    """Open the state database; creates the schema on first use."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=120, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=120000")
    conn.execute("PRAGMA foreign_keys=OFF")
    conn.executescript(SCHEMA)
    return conn


class Tx:
    """``with Tx(conn):`` runs the block in one IMMEDIATE transaction."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def __enter__(self) -> sqlite3.Connection:
        self.conn.execute("BEGIN IMMEDIATE")
        return self.conn

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self.conn.execute("COMMIT")
        else:
            self.conn.execute("ROLLBACK")


def stage_started(conn: sqlite3.Connection, stage: str, info: dict[str, Any] | None = None) -> None:
    conn.execute(
        "INSERT INTO stage_runs(stage, status, started_at, finished_at, info) VALUES (?, 'running', ?, NULL, ?)"
        " ON CONFLICT(stage) DO UPDATE SET status='running', started_at=excluded.started_at,"
        " finished_at=NULL, info=excluded.info",
        (stage, time.time(), json.dumps(info or {})),
    )


def stage_finished(
    conn: sqlite3.Connection, stage: str, info: dict[str, Any] | None = None
) -> None:
    conn.execute(
        "UPDATE stage_runs SET status='done', finished_at=?, info=? WHERE stage=?",
        (time.time(), json.dumps(info or {}), stage),
    )


def stage_status(conn: sqlite3.Connection, stage: str) -> str | None:
    row = conn.execute("SELECT status FROM stage_runs WHERE stage=?", (stage,)).fetchone()
    return row["status"] if row else None
