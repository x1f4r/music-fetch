from __future__ import annotations

from music_fetch.db import Database


def test_database_runs_latest_schema_migrations(tmp_path) -> None:
    db = Database(tmp_path / "music_fetch.sqlite3")
    assert db.schema_version() == Database.SCHEMA_VERSION
