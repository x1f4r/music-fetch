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


def test_recognition_metric_round_trips_item_summary_payload(tmp_path) -> None:
    db = Database(tmp_path / "music_fetch.sqlite3")
    job_id = _seed_job(db, job_id="summary-job")
    db.add_recognition_metric(
        RecognitionMetric(
            id="m-item-summary",
            job_id=job_id,
            source_item_id=f"{job_id}-item-1",
            provider_name=None,
            matched_segments=2,
            unresolved_segments=1,
            segments_merged=1,
            payload={
                "metric_type": "item_summary",
                "outcome": "item_summary",
                "segment_count": 3,
            },
            created_at=now_iso(),
        )
    )

    metric = next(metric for metric in db.list_recognition_metrics(job_id) if metric.id == "m-item-summary")
    assert metric.payload["metric_type"] == "item_summary"
    assert metric.payload["outcome"] == "item_summary"
    assert metric.payload["segment_count"] == 3
    assert metric.matched_segments == 2
    assert metric.unresolved_segments == 1
    assert metric.segments_merged == 1


def test_recognition_metric_round_trips_outcome_taxonomy_payload(tmp_path) -> None:
    db = Database(tmp_path / "music_fetch.sqlite3")
    job_id = _seed_job(db, job_id="outcome-job")
    attempt_base = {
        "metric_type": "provider_attempt",
        "ledger_version": 1,
        "start_ms": 0,
        "end_ms": 12_000,
        "probe_start_ms": 0,
        "probe_end_ms": 12_000,
        "cache_key": "cache-key",
        "cache_hit": False,
        "provider_call_attempted": False,
        "budget_consumed": 0,
        "budget_exhausted": False,
    }
    decision_base = {
        "metric_type": "provider_decision",
        "ledger_version": 1,
        "start_ms": 0,
        "end_ms": 12_000,
        "probe_start_ms": 0,
        "probe_end_ms": 12_000,
        "cache_hit": False,
        "provider_call_attempted": False,
        "budget_consumed": 0,
        "budget_exhausted": False,
        "skip_reason": "test skip",
    }
    payloads = {
        "cache_hit_matched": {**attempt_base, "outcome": "cache_hit_matched", "cache_hit": True},
        "cache_hit_empty": {**attempt_base, "outcome": "cache_hit_empty", "cache_hit": True},
        "provider_call_matched": {
            **attempt_base,
            "outcome": "provider_call_matched",
            "provider_call_attempted": True,
            "budget_consumed": 1,
        },
        "provider_call_empty": {
            **attempt_base,
            "outcome": "provider_call_empty",
            "provider_call_attempted": True,
            "budget_consumed": 1,
        },
        "provider_error": {
            **attempt_base,
            "outcome": "provider_error",
            "provider_call_attempted": True,
            "budget_consumed": 1,
            "error_type": "ProviderError",
            "error_message": "quota",
        },
        "provider_exception": {
            **attempt_base,
            "outcome": "provider_exception",
            "provider_call_attempted": True,
            "budget_consumed": 1,
            "error_type": "RuntimeError",
            "error_message": "boom",
        },
        "provider_unavailable": {**decision_base, "outcome": "provider_unavailable"},
        "prefer_free_skip": {**decision_base, "outcome": "prefer_free_skip"},
        "budget_exhausted": {
            **attempt_base,
            "outcome": "budget_exhausted",
            "budget_exhausted": True,
            "skip_reason": "provider-call budget exhausted",
        },
    }
    for outcome, payload in payloads.items():
        db.add_recognition_metric(
            RecognitionMetric(
                id=f"m-{outcome}",
                job_id=job_id,
                source_item_id=f"{job_id}-item-1",
                provider_name=ProviderName.VIBRA,
                cache_hit=outcome.startswith("cache_hit"),
                matched=outcome.endswith("matched"),
                call_count=1 if outcome.startswith("provider_call") or outcome in {"provider_error", "provider_exception"} else 0,
                payload=payload,
                created_at=now_iso(),
            )
        )

    metrics = {metric.id: metric for metric in db.list_recognition_metrics(job_id)}
    for outcome in payloads:
        metric = metrics[f"m-{outcome}"]
        assert metric.payload["outcome"] == outcome
    assert metrics["m-provider_error"].payload["error_type"] == "ProviderError"


def test_recognition_metric_validates_provider_ledger_payload() -> None:
    valid_payload = {
        "metric_type": "provider_attempt",
        "ledger_version": 1,
        "outcome": "provider_call_empty",
        "start_ms": 0,
        "end_ms": 12_000,
        "probe_start_ms": 0,
        "probe_end_ms": 12_000,
        "cache_key": "cache-key",
        "cache_hit": False,
        "provider_call_attempted": True,
        "budget_consumed": 1,
        "budget_exhausted": False,
    }
    metric = RecognitionMetric(
        id="valid-ledger",
        job_id="job-1",
        source_item_id="item-1",
        provider_name=ProviderName.VIBRA,
        cache_hit=False,
        matched=False,
        call_count=1,
        payload=valid_payload,
        created_at=now_iso(),
    )
    assert metric.payload["outcome"] == "provider_call_empty"

    with pytest.raises(ValueError, match="Unknown provider_attempt outcome"):
        RecognitionMetric(
            id="bad-ledger",
            job_id="job-1",
            source_item_id="item-1",
            provider_name=ProviderName.VIBRA,
            cache_hit=False,
            matched=False,
            call_count=1,
            payload={**valid_payload, "outcome": "provider_call_typo"},
            created_at=now_iso(),
        )
    inferred = RecognitionMetric(
        id="missing-type",
        job_id="job-1",
        source_item_id="item-1",
        provider_name=ProviderName.VIBRA,
        cache_hit=False,
        matched=False,
        call_count=1,
        payload={key: value for key, value in valid_payload.items() if key != "metric_type"},
        created_at=now_iso(),
    )
    assert inferred.payload["metric_type"] == "provider_attempt"
    inferred_decision = RecognitionMetric(
        id="missing-decision-type",
        job_id="job-1",
        source_item_id="item-1",
        provider_name=None,
        cache_hit=False,
        matched=False,
        call_count=0,
        payload={
            "ledger_version": 1,
            "outcome": "budget_exhausted",
            "start_ms": 0,
            "end_ms": 12_000,
            "probe_start_ms": 0,
            "probe_end_ms": 12_000,
            "cache_hit": False,
            "provider_call_attempted": False,
            "budget_consumed": 0,
            "budget_exhausted": True,
            "skip_reason": "provider-call budget exhausted",
        },
        created_at=now_iso(),
    )
    assert inferred_decision.payload["metric_type"] == "provider_decision"
    item_summary = RecognitionMetric(
        id="item-summary",
        job_id="job-1",
        source_item_id="item-1",
        provider_name=None,
        matched_segments=1,
        payload={"metric_type": "item_summary", "outcome": "item_summary", "segment_count": 1},
        created_at=now_iso(),
    )
    assert item_summary.payload["metric_type"] == "item_summary"
    with pytest.raises(ValueError, match="item_summary metrics must use outcome=item_summary"):
        RecognitionMetric(
            id="bad-item-summary-outcome",
            job_id="job-1",
            source_item_id="item-1",
            payload={"metric_type": "item_summary", "outcome": "other", "segment_count": 1},
            created_at=now_iso(),
        )
    with pytest.raises(ValueError, match="segment_count must be >= 0"):
        RecognitionMetric(
            id="bad-item-summary-count",
            job_id="job-1",
            source_item_id="item-1",
            payload={"metric_type": "item_summary", "outcome": "item_summary", "segment_count": -1},
            created_at=now_iso(),
        )
    with pytest.raises(ValueError, match="tied to a source item"):
        RecognitionMetric(
            id="bad-item-summary-source",
            job_id="job-1",
            payload={"metric_type": "item_summary", "outcome": "item_summary", "segment_count": 1},
            created_at=now_iso(),
        )
    with pytest.raises(ValueError, match="must not be provider-specific"):
        RecognitionMetric(
            id="bad-item-summary-matched",
            job_id="job-1",
            source_item_id="item-1",
            matched=True,
            payload={"metric_type": "item_summary", "outcome": "item_summary", "segment_count": 1},
            created_at=now_iso(),
        )
    with pytest.raises(ValueError, match="must not be provider-specific"):
        RecognitionMetric(
            id="bad-item-summary-provider",
            job_id="job-1",
            source_item_id="item-1",
            provider_name=ProviderName.VIBRA,
            payload={"metric_type": "item_summary", "outcome": "item_summary", "segment_count": 1},
            created_at=now_iso(),
        )
    with pytest.raises(ValueError, match="Unknown recognition outcome"):
        RecognitionMetric(
            id="unknown-outcome",
            job_id="job-1",
            payload={"outcome": "provider_call_typo"},
            created_at=now_iso(),
        )
    with pytest.raises(ValueError, match="must not be marked matched"):
        RecognitionMetric(
            id="bad-flags",
            job_id="job-1",
            source_item_id="item-1",
            provider_name=ProviderName.VIBRA,
            cache_hit=False,
            matched=True,
            call_count=1,
            payload={**valid_payload, "outcome": "provider_call_empty"},
            created_at=now_iso(),
        )
