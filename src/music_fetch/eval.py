from __future__ import annotations

import json
from pathlib import Path
import time

from .models import EvaluationCase, EvaluationCaseResult, EvaluationReport, JobCreate, SegmentKind
from .service import JobManager
from .utils import now_iso


def load_manifest(path: Path) -> list[EvaluationCase]:
    payload = json.loads(path.read_text())
    if isinstance(payload, dict):
        payload = payload.get("cases") or []
    return [EvaluationCase.model_validate(case) for case in payload]


def run_evaluation_manifest(manager: JobManager, manifest_path: Path) -> EvaluationReport:
    cases = load_manifest(manifest_path)
    results: list[EvaluationCaseResult] = []
    for case in cases:
        started_at = time.monotonic()
        job = manager.submit(JobCreate(inputs=[case.input_value]))
        final_job = manager.wait(job.id)
        segments = manager.db.get_segments(job.id)
        metrics = manager.db.list_recognition_metrics(job.id)
        actual_tracks = [segment.track.normalized_key() for segment in segments if segment.kind == SegmentKind.MATCHED_TRACK and segment.track]
        expected_tracks = [value.strip().lower() for value in case.expected_tracks]
        expected_set = set(expected_tracks)
        actual_set = set(actual_tracks)
        true_positive = len(expected_set & actual_set)
        precision = true_positive / max(1, len(actual_set))
        recall = true_positive / max(1, len(expected_set))
        provider_calls = sum(metric.call_count for metric in metrics)
        cache_hits = sum(1 for metric in metrics if metric.cache_hit)
        results.append(
            EvaluationCaseResult(
                case_id=case.id,
                job_id=job.id,
                status=final_job.status,
                runtime_ms=int((time.monotonic() - started_at) * 1000),
                provider_calls=provider_calls,
                cache_hits=cache_hits,
                matched_segments=sum(1 for segment in segments if segment.kind == SegmentKind.MATCHED_TRACK),
                unresolved_segments=sum(1 for segment in segments if segment.kind == SegmentKind.MUSIC_UNRESOLVED),
                precision=precision,
                recall=recall,
                expected_tracks=expected_tracks,
                actual_tracks=actual_tracks,
            )
        )
    summary = {
        "case_count": float(len(results)),
        "avg_precision": sum(result.precision for result in results) / max(1, len(results)),
        "avg_recall": sum(result.recall for result in results) / max(1, len(results)),
        "avg_provider_calls": sum(result.provider_calls for result in results) / max(1, len(results)),
    }
    return EvaluationReport(
        manifest_path=str(manifest_path),
        created_at=now_iso(),
        case_results=results,
        summary=summary,
    )
