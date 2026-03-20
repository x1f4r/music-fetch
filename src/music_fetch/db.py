from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .models import (
    DetectedSegment,
    ItemStatus,
    Job,
    JobEvent,
    JobOptions,
    JobStatus,
    ProviderConfig,
    ProviderName,
    ProviderState,
    SourceItem,
    SourceKind,
    SourceMetadata,
)
from .utils import now_iso


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS jobs (
                  id TEXT PRIMARY KEY,
                  status TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  inputs_json TEXT NOT NULL,
                  options_json TEXT NOT NULL,
                  error TEXT
                );
                CREATE TABLE IF NOT EXISTS source_items (
                  id TEXT PRIMARY KEY,
                  job_id TEXT NOT NULL,
                  input_value TEXT NOT NULL,
                  kind TEXT NOT NULL,
                  status TEXT NOT NULL,
                  metadata_json TEXT NOT NULL,
                  local_path TEXT,
                  download_url TEXT,
                  normalized_path TEXT,
                  instrumental_path TEXT,
                  error TEXT,
                  FOREIGN KEY(job_id) REFERENCES jobs(id)
                );
                CREATE TABLE IF NOT EXISTS detected_segments (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  job_id TEXT NOT NULL,
                  source_item_id TEXT NOT NULL,
                  segment_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS provider_configs (
                  name TEXT PRIMARY KEY,
                  enabled INTEGER NOT NULL,
                  config_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS job_events (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  job_id TEXT NOT NULL,
                  level TEXT NOT NULL,
                  message TEXT NOT NULL,
                  created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS local_catalog_tracks (
                  id TEXT PRIMARY KEY,
                  path TEXT NOT NULL,
                  title TEXT,
                  artist TEXT,
                  album TEXT,
                  fingerprint_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS provider_match_cache (
                  cache_key TEXT NOT NULL,
                  provider_name TEXT NOT NULL,
                  candidates_json TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  PRIMARY KEY (cache_key, provider_name)
                );
                CREATE TABLE IF NOT EXISTS pinned_jobs (
                  job_id TEXT PRIMARY KEY,
                  created_at TEXT NOT NULL,
                  FOREIGN KEY(job_id) REFERENCES jobs(id)
                );
                """
            )
            conn.commit()

    def create_job(self, inputs: list[str], options: JobOptions) -> Job:
        timestamp = now_iso()
        job = Job(
            id=str(uuid.uuid4()),
            status=JobStatus.QUEUED,
            created_at=timestamp,
            updated_at=timestamp,
            options=options,
            inputs=inputs,
        )
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO jobs (id, status, created_at, updated_at, inputs_json, options_json, error) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (job.id, job.status, job.created_at, job.updated_at, json.dumps(inputs), job.options.model_dump_json(), None),
            )
            conn.commit()
        return job

    def update_job(self, job_id: str, *, status: JobStatus | None = None, error: str | None = None) -> None:
        updates = ["updated_at = ?"]
        values: list[Any] = [now_iso()]
        if status is not None:
            updates.append("status = ?")
            values.append(status)
        if error is not None:
            updates.append("error = ?")
            values.append(error)
        values.append(job_id)
        with self.connect() as conn:
            conn.execute(f"UPDATE jobs SET {', '.join(updates)} WHERE id = ?", values)
            conn.commit()

    def add_source_items(self, items: list[SourceItem]) -> None:
        with self.connect() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO source_items (
                  id, job_id, input_value, kind, status, metadata_json, local_path, download_url,
                  normalized_path, instrumental_path, error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        item.id,
                        item.job_id,
                        item.input_value,
                        item.kind,
                        item.status,
                        item.metadata.model_dump_json(),
                        item.local_path,
                        item.download_url,
                        item.normalized_path,
                        item.instrumental_path,
                        item.error,
                    )
                    for item in items
                ],
            )
            conn.commit()

    def update_source_item(self, item: SourceItem) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE source_items
                SET status = ?, metadata_json = ?, local_path = ?, download_url = ?, normalized_path = ?,
                    instrumental_path = ?, error = ?
                WHERE id = ?
                """,
                (
                    item.status,
                    item.metadata.model_dump_json(),
                    item.local_path,
                    item.download_url,
                    item.normalized_path,
                    item.instrumental_path,
                    item.error,
                    item.id,
                ),
            )
            conn.commit()

    def replace_segments(self, job_id: str, source_item_id: str, segments: list[DetectedSegment]) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM detected_segments WHERE job_id = ? AND source_item_id = ?", (job_id, source_item_id))
            conn.executemany(
                "INSERT INTO detected_segments (job_id, source_item_id, segment_json) VALUES (?, ?, ?)",
                [(job_id, source_item_id, segment.model_dump_json()) for segment in segments],
            )
            conn.commit()

    def add_event(self, job_id: str, level: str, message: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO job_events (job_id, level, message, created_at) VALUES (?, ?, ?, ?)",
                (job_id, level, message, now_iso()),
            )
            conn.commit()

    def list_events(self, job_id: str, after_id: int = 0) -> list[JobEvent]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT id, job_id, level, message, created_at FROM job_events WHERE job_id = ? AND id > ? ORDER BY id ASC",
                (job_id, after_id),
            ).fetchall()
        return [JobEvent(**dict(row)) for row in rows]

    def get_job(self, job_id: str) -> Job | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if row is None:
            return None
        return Job(
            id=row["id"],
            status=JobStatus(row["status"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            inputs=json.loads(row["inputs_json"]),
            options=JobOptions.model_validate_json(row["options_json"]),
            error=row["error"],
        )

    def list_jobs(self, limit: int = 50) -> list[Job]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        return [
            Job(
                id=row["id"],
                status=JobStatus(row["status"]),
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                inputs=json.loads(row["inputs_json"]),
                options=JobOptions.model_validate_json(row["options_json"]),
                error=row["error"],
            )
            for row in rows
        ]

    def get_source_items(self, job_id: str) -> list[SourceItem]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM source_items WHERE job_id = ? ORDER BY rowid ASC", (job_id,)).fetchall()
        return [
            SourceItem(
                id=row["id"],
                job_id=row["job_id"],
                input_value=row["input_value"],
                kind=SourceKind(row["kind"]),
                status=ItemStatus(row["status"]),
                metadata=SourceMetadata.model_validate_json(row["metadata_json"]),
                local_path=row["local_path"],
                download_url=row["download_url"],
                normalized_path=row["normalized_path"],
                instrumental_path=row["instrumental_path"],
                error=row["error"],
            )
            for row in rows
        ]

    def get_segments(self, job_id: str) -> list[DetectedSegment]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT segment_json FROM detected_segments WHERE job_id = ? ORDER BY source_item_id, id",
                (job_id,),
            ).fetchall()
        return [DetectedSegment.model_validate_json(row["segment_json"]) for row in rows]

    def set_provider_config(self, name: ProviderName, config: ProviderConfig) -> None:
        with self.connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO provider_configs (name, enabled, config_json) VALUES (?, ?, ?)",
                (name, int(config.enabled), json.dumps(config.config)),
            )
            conn.commit()

    def get_provider_configs(self) -> dict[ProviderName, ProviderConfig]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM provider_configs").fetchall()
        return {
            ProviderName(row["name"]): ProviderConfig(enabled=bool(row["enabled"]), config=json.loads(row["config_json"]))
            for row in rows
        }

    def list_provider_states(self, defaults: dict[ProviderName, ProviderState]) -> list[ProviderState]:
        saved = self.get_provider_configs()
        states = []
        for name, state in defaults.items():
            if name in saved:
                state = state.model_copy(update={"enabled": saved[name].enabled, "config": saved[name].config})
            states.append(state)
        return states

    def add_catalog_track(self, track_id: str, path: str, title: str | None, artist: str | None, album: str | None, fingerprint: dict[str, Any]) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO local_catalog_tracks (id, path, title, artist, album, fingerprint_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (track_id, path, title, artist, album, json.dumps(fingerprint)),
            )
            conn.commit()

    def list_catalog_tracks(self) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute("SELECT * FROM local_catalog_tracks ORDER BY artist, title, path").fetchall()

    def get_provider_cache(self, cache_key: str, provider_name: ProviderName) -> str | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT candidates_json FROM provider_match_cache WHERE cache_key = ? AND provider_name = ?",
                (cache_key, provider_name),
            ).fetchone()
        return None if row is None else row["candidates_json"]

    def set_provider_cache(self, cache_key: str, provider_name: ProviderName, candidates_json: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO provider_match_cache (cache_key, provider_name, candidates_json, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (cache_key, provider_name, candidates_json, now_iso()),
            )
            conn.commit()

    def set_job_pinned(self, job_id: str, pinned: bool) -> None:
        with self.connect() as conn:
            if pinned:
                conn.execute(
                    "INSERT OR REPLACE INTO pinned_jobs (job_id, created_at) VALUES (?, ?)",
                    (job_id, now_iso()),
                )
            else:
                conn.execute("DELETE FROM pinned_jobs WHERE job_id = ?", (job_id,))
            conn.commit()

    def is_job_pinned(self, job_id: str) -> bool:
        with self.connect() as conn:
            row = conn.execute("SELECT 1 FROM pinned_jobs WHERE job_id = ?", (job_id,)).fetchone()
        return row is not None

    def list_pinned_job_ids(self) -> set[str]:
        with self.connect() as conn:
            rows = conn.execute("SELECT job_id FROM pinned_jobs").fetchall()
        return {row["job_id"] for row in rows}
