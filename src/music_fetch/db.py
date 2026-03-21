from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .models import (
    ArtifactEntry,
    DetectedSegment,
    DiscoveryState,
    ItemStatus,
    Job,
    JobEvent,
    JobOptions,
    JobStatus,
    ProviderConfig,
    RecognitionMetric,
    ProviderName,
    ProviderState,
    SourceItem,
    SourceKind,
    SourceMetadata,
)
from .utils import now_iso


class Database:
    SCHEMA_VERSION = 4

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
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("CREATE TABLE IF NOT EXISTS app_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            version_row = conn.execute("SELECT value FROM app_meta WHERE key = 'schema_version'").fetchone()
            current_version = int(version_row["value"]) if version_row else 0
            for version in range(current_version + 1, self.SCHEMA_VERSION + 1):
                getattr(self, f"_migrate_to_v{version}")(conn)
            conn.execute(
                "INSERT OR REPLACE INTO app_meta (key, value) VALUES ('schema_version', ?)",
                (str(self.SCHEMA_VERSION),),
            )
            conn.commit()

    def _migrate_to_v1(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS jobs (
              id TEXT PRIMARY KEY,
              status TEXT NOT NULL,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              inputs_json TEXT NOT NULL,
              options_json TEXT NOT NULL,
              error TEXT,
              cancel_requested INTEGER NOT NULL DEFAULT 0
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
        job_columns = {row["name"] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()}
        if "cancel_requested" not in job_columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN cancel_requested INTEGER NOT NULL DEFAULT 0")

    def _migrate_to_v2(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS segment_rows (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              job_id TEXT NOT NULL,
              source_item_id TEXT NOT NULL,
              start_ms INTEGER NOT NULL,
              end_ms INTEGER NOT NULL,
              kind TEXT NOT NULL,
              confidence REAL NOT NULL,
              providers_json TEXT NOT NULL,
              evidence_count INTEGER NOT NULL,
              track_json TEXT,
              alternates_json TEXT NOT NULL,
              repeat_group_id TEXT,
              probe_count INTEGER NOT NULL DEFAULT 0,
              provider_attempts INTEGER NOT NULL DEFAULT 0,
              metadata_hints_json TEXT NOT NULL,
              uncertainty REAL,
              segment_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_segment_rows_job_start ON segment_rows(job_id, source_item_id, start_ms);
            CREATE TABLE IF NOT EXISTS artifact_entries (
              id TEXT PRIMARY KEY,
              category TEXT NOT NULL,
              label TEXT NOT NULL,
              path TEXT NOT NULL,
              size_bytes INTEGER NOT NULL,
              exists_flag INTEGER NOT NULL,
              temporary INTEGER NOT NULL,
              job_id TEXT,
              source_item_id TEXT,
              pinned INTEGER NOT NULL DEFAULT 0,
              updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_artifact_entries_job ON artifact_entries(job_id, category);
            """
        )
        legacy_rows = conn.execute("SELECT id, job_id, source_item_id, segment_json FROM detected_segments ORDER BY id").fetchall()
        for row in legacy_rows:
            segment = DetectedSegment.model_validate_json(row["segment_json"])
            conn.execute(
                """
                INSERT OR IGNORE INTO segment_rows (
                  id, job_id, source_item_id, start_ms, end_ms, kind, confidence, providers_json,
                  evidence_count, track_json, alternates_json, repeat_group_id, probe_count,
                  provider_attempts, metadata_hints_json, uncertainty, segment_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["id"],
                    row["job_id"],
                    row["source_item_id"],
                    segment.start_ms,
                    segment.end_ms,
                    segment.kind,
                    segment.confidence,
                    json.dumps([provider.value for provider in segment.providers]),
                    segment.evidence_count,
                    None if segment.track is None else segment.track.model_dump_json(),
                    json.dumps([track.model_dump(mode="json") for track in segment.alternates]),
                    segment.repeat_group_id,
                    segment.probe_count,
                    segment.provider_attempts,
                    json.dumps(segment.metadata_hints),
                    segment.uncertainty,
                    row["segment_json"],
                ),
            )

    def _migrate_to_v3(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS discovery_state (
              job_id TEXT NOT NULL,
              input_value TEXT NOT NULL,
              cursor INTEGER NOT NULL DEFAULT 0,
              total INTEGER,
              completed INTEGER NOT NULL DEFAULT 0,
              payload_json TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              PRIMARY KEY (job_id, input_value)
            );
            CREATE TABLE IF NOT EXISTS recognition_metrics (
              id TEXT PRIMARY KEY,
              job_id TEXT NOT NULL,
              source_item_id TEXT,
              provider_name TEXT,
              cache_hit INTEGER NOT NULL DEFAULT 0,
              matched INTEGER NOT NULL DEFAULT 0,
              call_count INTEGER NOT NULL DEFAULT 0,
              matched_segments INTEGER NOT NULL DEFAULT 0,
              unresolved_segments INTEGER NOT NULL DEFAULT 0,
              elapsed_ms INTEGER NOT NULL DEFAULT 0,
              payload_json TEXT NOT NULL,
              created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_recognition_metrics_job ON recognition_metrics(job_id, source_item_id, provider_name);
            """
        )

    def _migrate_to_v4(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS catalog_fingerprint_index (
              track_id TEXT NOT NULL,
              bucket TEXT NOT NULL,
              position INTEGER NOT NULL,
              value INTEGER NOT NULL,
              PRIMARY KEY (track_id, bucket, position)
            );
            CREATE INDEX IF NOT EXISTS idx_catalog_fingerprint_bucket ON catalog_fingerprint_index(bucket, value);
            """
        )

    def create_job(self, inputs: list[str], options: JobOptions) -> Job:
        timestamp = now_iso()
        job = Job(
            id=str(uuid.uuid4()),
            status=JobStatus.QUEUED,
            created_at=timestamp,
            updated_at=timestamp,
            options=options,
            inputs=inputs,
            cancel_requested=False,
        )
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO jobs (id, status, created_at, updated_at, inputs_json, options_json, error, cancel_requested) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (job.id, job.status, job.created_at, job.updated_at, json.dumps(inputs), job.options.model_dump_json(), None, 0),
            )
            conn.commit()
        return job

    def update_job(
        self,
        job_id: str,
        *,
        status: JobStatus | None = None,
        error: str | None = None,
        cancel_requested: bool | None = None,
    ) -> None:
        updates = ["updated_at = ?"]
        values: list[Any] = [now_iso()]
        if status is not None:
            updates.append("status = ?")
            values.append(status)
        if error is not None:
            updates.append("error = ?")
            values.append(error)
        if cancel_requested is not None:
            updates.append("cancel_requested = ?")
            values.append(int(cancel_requested))
        values.append(job_id)
        with self.connect() as conn:
            conn.execute(f"UPDATE jobs SET {', '.join(updates)} WHERE id = ?", values)
            conn.commit()

    def request_job_cancel(self, job_id: str) -> None:
        self.update_job(job_id, cancel_requested=True)

    def is_cancel_requested(self, job_id: str) -> bool:
        with self.connect() as conn:
            row = conn.execute("SELECT cancel_requested FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return bool(row["cancel_requested"]) if row else False

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
            conn.execute("DELETE FROM segment_rows WHERE job_id = ? AND source_item_id = ?", (job_id, source_item_id))
            conn.executemany(
                "INSERT INTO detected_segments (job_id, source_item_id, segment_json) VALUES (?, ?, ?)",
                [(job_id, source_item_id, segment.model_dump_json()) for segment in segments],
            )
            conn.executemany(
                """
                INSERT INTO segment_rows (
                  job_id, source_item_id, start_ms, end_ms, kind, confidence, providers_json,
                  evidence_count, track_json, alternates_json, repeat_group_id, probe_count,
                  provider_attempts, metadata_hints_json, uncertainty, segment_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        job_id,
                        source_item_id,
                        segment.start_ms,
                        segment.end_ms,
                        segment.kind,
                        segment.confidence,
                        json.dumps([provider.value for provider in segment.providers]),
                        segment.evidence_count,
                        None if segment.track is None else segment.track.model_dump_json(),
                        json.dumps([track.model_dump(mode="json") for track in segment.alternates]),
                        segment.repeat_group_id,
                        segment.probe_count,
                        segment.provider_attempts,
                        json.dumps(segment.metadata_hints),
                        segment.uncertainty,
                        segment.model_dump_json(),
                    )
                    for segment in segments
                ],
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
        return self._row_to_job(row)

    def list_jobs(self, limit: int = 50) -> list[Job]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        return [self._row_to_job(row) for row in rows]

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
                "SELECT segment_json FROM segment_rows WHERE job_id = ? ORDER BY source_item_id, start_ms, id",
                (job_id,),
            ).fetchall()
        return [DetectedSegment.model_validate_json(row["segment_json"]) for row in rows]

    def replace_artifact_entries(self, job_id: str | None, entries: list[ArtifactEntry]) -> None:
        with self.connect() as conn:
            if job_id is not None:
                conn.execute("DELETE FROM artifact_entries WHERE job_id = ?", (job_id,))
            conn.executemany(
                """
                INSERT OR REPLACE INTO artifact_entries (
                  id, category, label, path, size_bytes, exists_flag, temporary, job_id, source_item_id, pinned, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        entry.id,
                        entry.category,
                        entry.label,
                        entry.path,
                        entry.size_bytes,
                        int(entry.exists),
                        int(entry.temporary),
                        entry.job_id,
                        entry.source_item_id,
                        int(entry.pinned),
                        now_iso(),
                    )
                    for entry in entries
                ],
            )
            conn.commit()

    def get_artifact_entries(self, job_id: str | None = None) -> list[ArtifactEntry]:
        with self.connect() as conn:
            if job_id is None:
                rows = conn.execute("SELECT * FROM artifact_entries ORDER BY job_id, category, label").fetchall()
            else:
                rows = conn.execute("SELECT * FROM artifact_entries WHERE job_id = ? ORDER BY category, label", (job_id,)).fetchall()
        return [
            ArtifactEntry(
                id=row["id"],
                category=row["category"],
                label=row["label"],
                path=row["path"],
                size_bytes=row["size_bytes"],
                exists=bool(row["exists_flag"]),
                temporary=bool(row["temporary"]),
                job_id=row["job_id"],
                source_item_id=row["source_item_id"],
                pinned=bool(row["pinned"]),
            )
            for row in rows
        ]

    def upsert_discovery_state(self, state: DiscoveryState) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO discovery_state (
                  job_id, input_value, cursor, total, completed, payload_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    state.job_id,
                    state.input_value,
                    state.cursor,
                    state.total,
                    int(state.completed),
                    json.dumps(state.payload),
                    state.updated_at,
                ),
            )
            conn.commit()

    def list_discovery_states(self, job_id: str) -> list[DiscoveryState]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM discovery_state WHERE job_id = ? ORDER BY input_value",
                (job_id,),
            ).fetchall()
        return [
            DiscoveryState(
                job_id=row["job_id"],
                input_value=row["input_value"],
                cursor=row["cursor"],
                total=row["total"],
                completed=bool(row["completed"]),
                payload=json.loads(row["payload_json"]),
                updated_at=row["updated_at"],
            )
            for row in rows
        ]

    def add_recognition_metric(self, metric: RecognitionMetric) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO recognition_metrics (
                  id, job_id, source_item_id, provider_name, cache_hit, matched, call_count,
                  matched_segments, unresolved_segments, elapsed_ms, payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    metric.id,
                    metric.job_id,
                    metric.source_item_id,
                    metric.provider_name,
                    int(metric.cache_hit),
                    int(metric.matched),
                    metric.call_count,
                    metric.matched_segments,
                    metric.unresolved_segments,
                    metric.elapsed_ms,
                    json.dumps(metric.payload),
                    metric.created_at,
                ),
            )
            conn.commit()

    def list_recognition_metrics(self, job_id: str) -> list[RecognitionMetric]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM recognition_metrics WHERE job_id = ? ORDER BY created_at, id",
                (job_id,),
            ).fetchall()
        return [
            RecognitionMetric(
                id=row["id"],
                job_id=row["job_id"],
                source_item_id=row["source_item_id"],
                provider_name=None if row["provider_name"] is None else ProviderName(row["provider_name"]),
                cache_hit=bool(row["cache_hit"]),
                matched=bool(row["matched"]),
                call_count=row["call_count"],
                matched_segments=row["matched_segments"],
                unresolved_segments=row["unresolved_segments"],
                elapsed_ms=row["elapsed_ms"],
                payload=json.loads(row["payload_json"]),
                created_at=row["created_at"],
            )
            for row in rows
        ]

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
            conn.execute("DELETE FROM catalog_fingerprint_index WHERE track_id = ?", (track_id,))
            for position, value in enumerate((fingerprint.get("fingerprint") or [])[:120]):
                bucket = f"b{position // 12:02d}"
                conn.execute(
                    "INSERT INTO catalog_fingerprint_index (track_id, bucket, position, value) VALUES (?, ?, ?, ?)",
                    (track_id, bucket, position, int(value)),
                )
            conn.commit()

    def list_catalog_tracks(self) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute("SELECT * FROM local_catalog_tracks ORDER BY artist, title, path").fetchall()

    def shortlist_catalog_track_ids(self, fingerprint: list[int], limit: int = 24) -> list[str]:
        if not fingerprint:
            return []
        with self.connect() as conn:
            scores: dict[str, int] = {}
            for position, value in enumerate(fingerprint[:120]):
                bucket = f"b{position // 12:02d}"
                rows = conn.execute(
                    """
                    SELECT track_id, ABS(value - ?) <= 10 AS close_match
                    FROM catalog_fingerprint_index
                    WHERE bucket = ? AND position = ?
                    """,
                    (int(value), bucket, position),
                ).fetchall()
                for row in rows:
                    if not row["close_match"]:
                        continue
                    scores[row["track_id"]] = scores.get(row["track_id"], 0) + 1
        return [track_id for track_id, _score in sorted(scores.items(), key=lambda item: item[1], reverse=True)[:limit]]

    def get_catalog_tracks_by_ids(self, track_ids: list[str]) -> list[sqlite3.Row]:
        if not track_ids:
            return []
        placeholders = ", ".join("?" for _ in track_ids)
        with self.connect() as conn:
            return conn.execute(
                f"SELECT * FROM local_catalog_tracks WHERE id IN ({placeholders})",
                track_ids,
            ).fetchall()

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

    def schema_version(self) -> int:
        with self.connect() as conn:
            row = conn.execute("SELECT value FROM app_meta WHERE key = 'schema_version'").fetchone()
        return int(row["value"]) if row else 0

    @staticmethod
    def _row_to_job(row: sqlite3.Row) -> Job:
        return Job(
            id=row["id"],
            status=JobStatus(row["status"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            inputs=json.loads(row["inputs_json"]),
            options=JobOptions.model_validate_json(row["options_json"]),
            error=row["error"],
            cancel_requested=bool(row["cancel_requested"]) if "cancel_requested" in row.keys() else False,
        )
