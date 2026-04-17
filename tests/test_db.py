from __future__ import annotations

from pathlib import Path

import pytest

from music_fetch.db import Database
from music_fetch.models import (
    DetectedSegment,
    DiscoveryState,
    ItemStatus,
    JobOptions,
    JobStatus,
    ProviderName,
    RecognitionMetric,
    SegmentKind,
    SourceItem,
    SourceKind,
    SourceMetadata,
    TrackMatch,
)
from music_fetch.utils import now_iso


def test_database_runs_latest_schema_migrations(tmp_path) -> None:
    db = Database(tmp_path / "music_fetch.sqlite3")
    assert db.schema_version() == Database.SCHEMA_VERSION


def test_foreign_keys_pragma_is_enforced(tmp_path) -> None:
    """Cascades only fire when ``PRAGMA foreign_keys = ON`` is applied to the
    live connection. The :class:`Database` must set it on every connect."""
    db = Database(tmp_path / "music_fetch.sqlite3")
    with db.connect() as conn:
        row = conn.execute("PRAGMA foreign_keys").fetchone()
    assert row is not None
    # SQLite returns the PRAGMA as an integer 0/1; row[0] when sqlite3.Row is set.
    assert int(row[0] if not hasattr(row, "keys") else row[0]) == 1


def _seed_job(db: Database, *, job_id: str = "job-a") -> str:
    options = JobOptions()
    job = db.create_job(["/tmp/input.wav"], options)
    # Replace the generated id so tests can assert deterministic rows.
    with db.connect() as conn:
        conn.execute("UPDATE jobs SET id = ? WHERE id = ?", (job_id, job.id))
        conn.commit()
    item = SourceItem(
        id=f"{job_id}-item-1",
        job_id=job_id,
        input_value="/tmp/input.wav",
        kind=SourceKind.LOCAL_FILE,
        status=ItemStatus.SUCCEEDED,
        metadata=SourceMetadata(duration_ms=20_000),
    )
    db.add_source_items([item])
    db.replace_segments(
        job_id,
        item.id,
        [
            DetectedSegment(
                source_item_id=item.id,
                start_ms=0,
                end_ms=12_000,
                kind=SegmentKind.MATCHED_TRACK,
                confidence=0.9,
                providers=[ProviderName.VIBRA],
                evidence_count=1,
                track=TrackMatch(title="Song", artist="Artist"),
                identity_key="fuzzy::artist::song",
                acceptance_gate="G1",
            )
        ],
    )
    db.add_event(job_id, "info", "seed event")
    db.upsert_discovery_state(
        DiscoveryState(
            job_id=job_id,
            input_value="/tmp/input.wav",
            cursor=1,
            completed=True,
            payload={},
            updated_at=now_iso(),
        )
    )
    db.add_recognition_metric(
        RecognitionMetric(
            id=f"{job_id}-metric",
            job_id=job_id,
            provider_name=ProviderName.VIBRA,
            cache_hit=False,
            matched=True,
            call_count=1,
            created_at=now_iso(),
        )
    )
    return job_id


def _child_row_counts(db: Database, job_id: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    with db.connect() as conn:
        for table in (
            "source_items",
            "segment_rows",
            "detected_segments",
            "job_events",
            "recognition_metrics",
            "discovery_state",
            "artifact_entries",
            "pinned_jobs",
        ):
            row = conn.execute(f"SELECT COUNT(*) AS n FROM {table} WHERE job_id = ?", (job_id,)).fetchone()
            counts[table] = row["n"] if row else 0
    return counts


def test_delete_job_cascades_through_every_child_table(tmp_path) -> None:
    db = Database(tmp_path / "music_fetch.sqlite3")
    job_id = _seed_job(db)

    before = _child_row_counts(db, job_id)
    assert before["source_items"] == 1
    assert before["segment_rows"] == 1
    assert before["detected_segments"] == 1
    assert before["job_events"] >= 1
    assert before["recognition_metrics"] == 1
    assert before["discovery_state"] == 1

    assert db.delete_job(job_id) is True

    after = _child_row_counts(db, job_id)
    for table, count in after.items():
        assert count == 0, f"{table} still has {count} orphan(s) for {job_id}"
    assert db.get_job(job_id) is None
    # Idempotent: re-deleting returns False but doesn't raise.
    assert db.delete_job(job_id) is False


def test_delete_job_cascade_preserves_other_jobs(tmp_path) -> None:
    db = Database(tmp_path / "music_fetch.sqlite3")
    keep_id = _seed_job(db, job_id="keep")
    drop_id = _seed_job(db, job_id="drop")

    db.delete_job(drop_id)

    assert db.get_job(keep_id) is not None
    assert db.get_job(drop_id) is None
    # The kept job's children still exist.
    kept_counts = _child_row_counts(db, keep_id)
    assert kept_counts["source_items"] == 1
    assert kept_counts["segment_rows"] == 1


def test_replace_segments_persists_identity_and_gate_columns(tmp_path) -> None:
    db = Database(tmp_path / "music_fetch.sqlite3")
    job_id = _seed_job(db, job_id="obs")
    with db.connect() as conn:
        row = conn.execute(
            "SELECT identity_key, acceptance_gate FROM segment_rows WHERE job_id = ?",
            (job_id,),
        ).fetchone()
    assert row is not None
    assert row["identity_key"] == "fuzzy::artist::song"
    assert row["acceptance_gate"] == "G1"


def test_recognition_metric_round_trips_extended_counters(tmp_path) -> None:
    db = Database(tmp_path / "music_fetch.sqlite3")
    job_id = _seed_job(db, job_id="metric-job")
    db.add_recognition_metric(
        RecognitionMetric(
            id="m-extended",
            job_id=job_id,
            provider_name=None,
            segments_merged=3,
            segments_bridged_across_speech=1,
            repeat_group_reconfirmed=2,
            gate_g3_hits=4,
            created_at=now_iso(),
        )
    )
    metrics = db.list_recognition_metrics(job_id)
    extended = [metric for metric in metrics if metric.id == "m-extended"][0]
    assert extended.segments_merged == 3
    assert extended.segments_bridged_across_speech == 1
    assert extended.repeat_group_reconfirmed == 2
    assert extended.gate_g3_hits == 4
