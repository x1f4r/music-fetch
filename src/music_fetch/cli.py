from __future__ import annotations

from collections import Counter
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import typer
import uvicorn
from rich.console import Console
from rich.table import Table

from .api import create_api
from .context import create_context
from .doctor import run_doctor
from .eval import run_evaluation_manifest
from .installer import install_dependencies
from .models import AnalysisMode, JobCreate, JobOptions, JobStatus, ProviderName, RecallProfile
from .tui import launch_tui

app = typer.Typer(help="Music Fetch CLI")
catalog_app = typer.Typer(help="Local catalog management")
storage_app = typer.Typer(help="Artifact and storage management")
app.add_typer(catalog_app, name="catalog")
app.add_typer(storage_app, name="storage")
console = Console()
TERMINAL_JOB_STATUSES = {
    JobStatus.SUCCEEDED.value,
    JobStatus.PARTIAL_FAILED.value,
    JobStatus.FAILED.value,
    JobStatus.CANCELED.value,
}
MIN_WATCH_INTERVAL_SECONDS = 0.05
METRICS_SCHEMA_VERSION = 2
METRIC_GATE_FIELDS = (
    "gate_g1_hits",
    "gate_g2_hits",
    "gate_g3_hits",
    "gate_g4_hits",
    "gate_g5_hits",
)


def _job_create(
    inputs: list[str],
    *,
    prefer_separation: bool = True,
    analysis_mode: AnalysisMode = AnalysisMode.AUTO,
    recall_profile: RecallProfile = RecallProfile.MAX_RECALL,
    metadata_hints: bool = True,
    repeat_detection: bool = True,
    max_windows: int = 24,
    max_segments: int = 360,
    max_probes_per_segment: int = 3,
    max_provider_calls: int = 420,
    provider_order: list[ProviderName] | None = None,
) -> JobCreate:
    options = JobOptions(
        prefer_separation=prefer_separation,
        analysis_mode=analysis_mode,
        recall_profile=recall_profile,
        enable_metadata_hints=metadata_hints,
        enable_repeat_detection=repeat_detection,
        max_windows=max_windows,
        max_segments=max_segments,
        max_probes_per_segment=max_probes_per_segment,
        max_provider_calls=max_provider_calls,
        provider_order=provider_order or JobOptions().provider_order,
    )
    return JobCreate(inputs=inputs, options=options)


def _status_value(value: object) -> str:
    return str(getattr(value, "value", value))


def _job_snapshot(context, job_id: str) -> dict[str, object]:
    job = context.db.get_job(job_id)
    if not job:
        raise typer.BadParameter(f"Unknown job: {job_id}")
    return {
        "job": job.model_dump(),
        "items": [item.model_dump() for item in context.db.get_source_items(job_id)],
        "segments": [segment.model_dump() for segment in context.db.get_segments(job_id)],
        "events": [event.model_dump() for event in context.db.list_events(job_id)],
    }


def _format_time_range(start_ms: object, end_ms: object) -> str:
    if start_ms is None or end_ms is None:
        return "-"
    return f"{int(start_ms) / 1000:.1f}s - {int(end_ms) / 1000:.1f}s"


def _format_track(segment: dict[str, object]) -> str:
    track = segment.get("track")
    if isinstance(track, dict) and track:
        title = track.get("title") or "Unknown title"
        artist = track.get("artist") or "Unknown artist"
        return f"{artist} - {title}"
    return _status_value(segment.get("kind", "-")).replace("_", " ")


def _format_confidence(value: object) -> str:
    if value is None:
        return "-"
    return f"{float(value):.2f}"


def _print_job_snapshot(payload: dict[str, object]) -> None:
    job = payload["job"]
    assert isinstance(job, dict)
    items = payload["items"]
    segments = payload["segments"]
    events = payload["events"]
    assert isinstance(items, list)
    assert isinstance(segments, list)
    assert isinstance(events, list)

    console.print(f"[bold]Job {job.get('id', '')}[/bold]")
    console.print(f"Status: {_status_value(job.get('status'))}")
    console.print(f"Created: {job.get('created_at', '-')}")
    console.print(f"Updated: {job.get('updated_at', '-')}")
    console.print(f"Inputs: {len(job.get('inputs') or [])}")
    console.print(f"Items: {len(items)}")
    console.print(f"Segments: {len(segments)}")
    console.print(f"Events: {len(events)}")
    if job.get("error"):
        console.print(f"[red]Error:[/red] {job['error']}")

    input_values = job.get("inputs") or []
    if input_values:
        console.print("\n[bold]Inputs[/bold]")
        for value in input_values:
            console.print(f"- {value}")

    if items:
        table = Table(title="Items")
        table.add_column("Status")
        table.add_column("Kind")
        table.add_column("Title / Input")
        table.add_column("Error")
        for item in items:
            if not isinstance(item, dict):
                continue
            metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
            title = metadata.get("title") if isinstance(metadata, dict) else None
            table.add_row(
                _status_value(item.get("status")),
                _status_value(item.get("kind")),
                str(title or item.get("input_value") or "-"),
                str(item.get("error") or ""),
            )
        console.print(table)

    if segments:
        table = Table(title="Segments")
        table.add_column("Range")
        table.add_column("Result")
        table.add_column("Confidence")
        table.add_column("Providers")
        for segment in segments:
            if not isinstance(segment, dict):
                continue
            providers = segment.get("providers") or []
            table.add_row(
                _format_time_range(segment.get("start_ms"), segment.get("end_ms")),
                _format_track(segment),
                _format_confidence(segment.get("confidence")),
                ", ".join(_status_value(provider) for provider in providers) if isinstance(providers, list) else "-",
            )
        console.print(table)

    if events:
        table = Table(title="Events")
        table.add_column("ID")
        table.add_column("Time")
        table.add_column("Level")
        table.add_column("Message")
        for event in events:
            if not isinstance(event, dict):
                continue
            table.add_row(
                str(event.get("id", "")),
                str(event.get("created_at", "")),
                str(event.get("level", "")),
                str(event.get("message", "")),
            )
        console.print(table)


def _event_id(event: dict[str, object]) -> int:
    try:
        return int(event.get("id") or 0)
    except (TypeError, ValueError):
        return 0


def _model_dump(model: object) -> dict[str, object]:
    dump = getattr(model, "model_dump")
    try:
        return dump(mode="json")
    except TypeError:
        return dump()


def _int_value(value: object) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _bool_value(value: object) -> bool:
    return bool(value)


def _metrics_payload(
    context,
    job_id: str,
    *,
    provider_filters: list[str] | None = None,
    outcome_filters: list[str] | None = None,
    metric_type_filters: list[str] | None = None,
    source_item_id: str | None = None,
    matched: bool | None = None,
    cache_hit: bool | None = None,
    summary_only: bool = False,
) -> dict[str, object]:
    job = context.db.get_job(job_id)
    if not job:
        raise typer.BadParameter(f"Unknown job: {job_id}")
    job_payload = _model_dump(job)
    filters = _metric_filters(
        providers=provider_filters,
        outcomes=outcome_filters,
        metric_types=metric_type_filters,
        source_item_id=source_item_id,
        matched=matched,
        cache_hit=cache_hit,
    )
    rows = _filter_metrics([_model_dump(metric) for metric in context.db.list_recognition_metrics(job_id)], filters)
    payload = {
        "schema_version": METRICS_SCHEMA_VERSION,
        "job_id": job_id,
        "job": _metrics_job_context(job_payload),
        "summary": _summarize_metrics(rows),
    }
    if filters:
        payload["filters"] = filters
    if not summary_only:
        payload["metrics"] = rows
    return payload


def _metrics_job_context(job: dict[str, object]) -> dict[str, object]:
    options = job.get("options") if isinstance(job.get("options"), dict) else {}
    assert isinstance(options, dict)
    return {
        "id": job.get("id"),
        "status": _status_value(job.get("status")),
        "created_at": job.get("created_at"),
        "updated_at": job.get("updated_at"),
        "max_provider_calls": options.get("max_provider_calls"),
        "budget_autoscale": options.get("budget_autoscale"),
        "provider_order": options.get("provider_order") or [],
    }


def _summarize_metrics(rows: list[dict[str, object]]) -> dict[str, object]:
    outcomes: Counter[str] = Counter()
    metric_types: Counter[str] = Counter()
    providers: dict[str, dict[str, object]] = {}
    gate_hits = {field: 0 for field in METRIC_GATE_FIELDS}
    totals = {
        "metrics": 0,
        "provider_calls": 0,
        "provider_call_attempts": 0,
        "cache_hits": 0,
        "matched_metrics": 0,
        "budget_consumed": 0,
        "budget_exhausted": 0,
        "elapsed_ms": 0,
        "matched_segments": 0,
        "unresolved_segments": 0,
    }
    for row in rows:
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        assert isinstance(payload, dict)
        provider = _status_value(row.get("provider_name") or "job")
        provider_summary = providers.setdefault(
            provider,
            {
                "provider": provider,
                "metrics": 0,
                "provider_calls": 0,
                "provider_call_attempts": 0,
                "cache_hits": 0,
                "matched_metrics": 0,
                "budget_consumed": 0,
                "budget_exhausted": 0,
                "elapsed_ms": 0,
                "outcomes": {},
            },
        )

        metric_type, outcome = _metric_type_and_outcome(row, payload)
        outcomes[outcome] += 1
        metric_types[metric_type] += 1

        call_count = _int_value(row.get("call_count"))
        budget_consumed = _int_value(payload.get("budget_consumed"))
        budget_exhausted = _bool_value(payload.get("budget_exhausted")) or outcome == "budget_exhausted"
        provider_call_attempted = _bool_value(payload.get("provider_call_attempted"))
        cache_hit = _bool_value(row.get("cache_hit"))
        matched = _bool_value(row.get("matched"))
        elapsed_ms = _int_value(row.get("elapsed_ms"))

        for target in (totals, provider_summary):
            target["metrics"] = _int_value(target.get("metrics")) + 1
            target["provider_calls"] = _int_value(target.get("provider_calls")) + call_count
            target["provider_call_attempts"] = _int_value(target.get("provider_call_attempts")) + int(provider_call_attempted)
            target["cache_hits"] = _int_value(target.get("cache_hits")) + int(cache_hit)
            target["matched_metrics"] = _int_value(target.get("matched_metrics")) + int(matched)
            target["budget_consumed"] = _int_value(target.get("budget_consumed")) + budget_consumed
            target["budget_exhausted"] = _int_value(target.get("budget_exhausted")) + int(budget_exhausted)
            target["elapsed_ms"] = _int_value(target.get("elapsed_ms")) + elapsed_ms

        totals["matched_segments"] += _int_value(row.get("matched_segments"))
        totals["unresolved_segments"] += _int_value(row.get("unresolved_segments"))
        provider_outcomes = provider_summary["outcomes"]
        assert isinstance(provider_outcomes, dict)
        provider_outcomes[outcome] = _int_value(provider_outcomes.get(outcome)) + 1
        for field in METRIC_GATE_FIELDS:
            gate_hits[field] += _int_value(row.get(field))

    return {
        "totals": totals,
        "outcomes": dict(sorted(outcomes.items())),
        "metric_types": dict(sorted(metric_types.items())),
        "gates": gate_hits,
        "providers": sorted(providers.values(), key=lambda item: str(item["provider"])),
    }


def _metric_filters(
    *,
    providers: list[str] | None,
    outcomes: list[str] | None,
    metric_types: list[str] | None,
    source_item_id: str | None,
    matched: bool | None,
    cache_hit: bool | None,
) -> dict[str, object]:
    filters: dict[str, object] = {}
    provider_values = _normalize_filter_values(providers)
    outcome_values = _normalize_filter_values(outcomes)
    metric_type_values = _normalize_filter_values(metric_types)
    if provider_values:
        filters["providers"] = provider_values
    if outcome_values:
        filters["outcomes"] = outcome_values
    if metric_type_values:
        filters["metric_types"] = metric_type_values
    if source_item_id:
        filters["source_item_id"] = source_item_id
    if matched is not None:
        filters["matched"] = matched
    if cache_hit is not None:
        filters["cache_hit"] = cache_hit
    return filters


def _normalize_filter_values(values: list[str] | None) -> list[str]:
    normalized = sorted({value.strip().lower() for value in values or [] if value.strip()})
    return normalized


def _filter_metrics(rows: list[dict[str, object]], filters: dict[str, object]) -> list[dict[str, object]]:
    providers = set(filters["providers"]) if isinstance(filters.get("providers"), list) else set()
    outcomes = set(filters["outcomes"]) if isinstance(filters.get("outcomes"), list) else set()
    metric_types = set(filters["metric_types"]) if isinstance(filters.get("metric_types"), list) else set()
    source_item_id = filters.get("source_item_id")
    matched_filter = filters.get("matched")
    cache_hit_filter = filters.get("cache_hit")
    filtered: list[dict[str, object]] = []
    for row in rows:
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        assert isinstance(payload, dict)
        metric_type, outcome = _metric_type_and_outcome(row, payload)
        provider = _status_value(row.get("provider_name") or "job").lower()
        if providers and provider not in providers:
            continue
        if outcomes and outcome.lower() not in outcomes:
            continue
        if metric_types and metric_type.lower() not in metric_types:
            continue
        if source_item_id and row.get("source_item_id") != source_item_id:
            continue
        if matched_filter is not None and _bool_value(row.get("matched")) != matched_filter:
            continue
        if cache_hit_filter is not None and _bool_value(row.get("cache_hit")) != cache_hit_filter:
            continue
        filtered.append(row)
    return filtered


def _metric_type_and_outcome(row: dict[str, object], payload: dict[str, object]) -> tuple[str, str]:
    metric_type = str(payload.get("metric_type") or "")
    outcome = str(payload.get("outcome") or "")
    if metric_type or outcome:
        return metric_type or "unknown", outcome or "unknown"
    if _looks_like_item_summary_metric(row, payload):
        return "item_summary", "item_summary"
    return "unknown", "unknown"


def _looks_like_item_summary_metric(row: dict[str, object], payload: dict[str, object]) -> bool:
    if row.get("provider_name") is not None or _int_value(row.get("call_count")) != 0:
        return False
    if row.get("source_item_id") is None:
        return False
    if "segment_count" in payload:
        return True
    summary_counters = (
        "matched_segments",
        "unresolved_segments",
        "segments_merged",
        "segments_bridged_across_speech",
        "repeat_group_reconfirmed",
        "repeat_group_rejected",
        *METRIC_GATE_FIELDS,
    )
    return any(_int_value(payload.get(field) if field == "segment_count" else row.get(field)) for field in summary_counters)


def _print_metrics(payload: dict[str, object]) -> None:
    summary = payload["summary"]
    assert isinstance(summary, dict)
    totals = summary["totals"]
    assert isinstance(totals, dict)
    job = payload["job"]
    assert isinstance(job, dict)
    console.print(f"[bold]Recognition metrics for {payload['job_id']}[/bold]")
    console.print(
        f"Status: {job.get('status')} | "
        f"Max provider calls: {job.get('max_provider_calls')} | "
        f"Budget autoscale: {job.get('budget_autoscale')}"
    )
    filters = payload.get("filters") if isinstance(payload.get("filters"), dict) else {}
    if filters:
        rendered_filters = []
        for key in ("providers", "outcomes", "metric_types"):
            value = filters.get(key)
            if isinstance(value, list) and value:
                rendered_filters.append(f"{key}: {', '.join(str(item) for item in value)}")
        if filters.get("source_item_id"):
            rendered_filters.append(f"source_item_id: {filters['source_item_id']}")
        for key in ("matched", "cache_hit"):
            if key in filters:
                rendered_filters.append(f"{key}: {str(filters[key]).lower()}")
        console.print(f"Filters: {' | '.join(rendered_filters)}")
    table = Table(title="Totals")
    table.add_column("Metric")
    table.add_column("Value")
    for key in (
        "metrics",
        "provider_calls",
        "provider_call_attempts",
        "cache_hits",
        "matched_metrics",
        "budget_consumed",
        "budget_exhausted",
        "elapsed_ms",
        "matched_segments",
        "unresolved_segments",
    ):
        table.add_row(key.replace("_", " "), str(totals.get(key, 0)))
    console.print(table)

    providers = summary["providers"]
    assert isinstance(providers, list)
    if providers:
        provider_table = Table(title="Providers")
        provider_table.add_column("Provider")
        provider_table.add_column("Metrics")
        provider_table.add_column("Calls")
        provider_table.add_column("Cache hits")
        provider_table.add_column("Matched")
        provider_table.add_column("Budget")
        provider_table.add_column("Outcomes")
        for provider in providers:
            if not isinstance(provider, dict):
                continue
            provider_table.add_row(
                str(provider.get("provider", "")),
                str(provider.get("metrics", 0)),
                str(provider.get("provider_calls", 0)),
                str(provider.get("cache_hits", 0)),
                str(provider.get("matched_metrics", 0)),
                f"{provider.get('budget_consumed', 0)} / exhausted {provider.get('budget_exhausted', 0)}",
                ", ".join(f"{key}:{value}" for key, value in sorted((provider.get("outcomes") or {}).items())),
            )
        console.print(provider_table)

    outcomes = summary["outcomes"]
    assert isinstance(outcomes, dict)
    if outcomes:
        outcome_table = Table(title="Outcomes")
        outcome_table.add_column("Outcome")
        outcome_table.add_column("Count")
        for outcome, count in outcomes.items():
            outcome_table.add_row(str(outcome), str(count))
        console.print(outcome_table)
    if not totals.get("metrics"):
        if filters:
            console.print("No recognition metrics match the active filters.")
        else:
            console.print("No recognition metrics found.")


@app.command()
def analyze(
    inputs: list[str],
    json_output: bool = typer.Option(False, "--json"),
    prefer_separation: bool = typer.Option(True, "--prefer-separation/--no-prefer-separation"),
    analysis_mode: AnalysisMode = typer.Option(AnalysisMode.AUTO, "--analysis-mode"),
    recall_profile: RecallProfile = typer.Option(RecallProfile.MAX_RECALL, "--recall-profile"),
    metadata_hints: bool = typer.Option(True, "--metadata-hints/--no-metadata-hints"),
    repeat_detection: bool = typer.Option(True, "--repeat-detection/--no-repeat-detection"),
    max_windows: int = typer.Option(24, "--max-windows"),
    max_segments: int = typer.Option(360, "--max-segments"),
    max_probes_per_segment: int = typer.Option(3, "--max-probes-per-segment"),
    max_provider_calls: int = typer.Option(420, "--max-provider-calls"),
    provider_order: list[ProviderName] | None = typer.Option(None, "--provider-order"),
) -> None:
    context = create_context()
    job = context.manager.submit(
        _job_create(
            inputs,
            prefer_separation=prefer_separation,
            analysis_mode=analysis_mode,
            recall_profile=recall_profile,
            metadata_hints=metadata_hints,
            repeat_detection=repeat_detection,
            max_windows=max_windows,
            max_segments=max_segments,
            max_probes_per_segment=max_probes_per_segment,
            max_provider_calls=max_provider_calls,
            provider_order=provider_order,
        )
    )
    final_job = context.manager.wait(job.id)
    payload = {
        "job": context.db.get_job(final_job.id).model_dump(),
        "items": [item.model_dump() for item in context.db.get_source_items(final_job.id)],
        "segments": [segment.model_dump() for segment in context.db.get_segments(final_job.id)],
        "events": [event.model_dump() for event in context.db.list_events(final_job.id)],
    }
    if json_output:
        console.print_json(json.dumps(payload))
        raise typer.Exit(code=0 if final_job.status != "failed" else 1)

    table = Table(title=f"Job {job.id}")
    table.add_column("Range")
    table.add_column("Segment")
    table.add_column("Confidence")
    for segment in context.db.get_segments(final_job.id):
        if segment.track:
            artist = segment.track.artist or "Unknown artist"
            summary = f"{artist} - {segment.track.title}"
        else:
            summary = segment.kind.value.replace("_", " ")
        table.add_row(
            f"{segment.start_ms/1000:.1f}s - {segment.end_ms/1000:.1f}s",
            summary,
            f"{segment.confidence:.2f}",
        )
    console.print(table)
    if not payload["segments"]:
        console.print("No matches found.")
    raise typer.Exit(code=0 if final_job.status != "failed" else 1)


@app.command("submit")
def submit_job(
    inputs: list[str],
    json_output: bool = typer.Option(False, "--json"),
    prefer_separation: bool = typer.Option(True, "--prefer-separation/--no-prefer-separation"),
    analysis_mode: AnalysisMode = typer.Option(AnalysisMode.AUTO, "--analysis-mode"),
    recall_profile: RecallProfile = typer.Option(RecallProfile.MAX_RECALL, "--recall-profile"),
    metadata_hints: bool = typer.Option(True, "--metadata-hints/--no-metadata-hints"),
    repeat_detection: bool = typer.Option(True, "--repeat-detection/--no-repeat-detection"),
    max_windows: int = typer.Option(24, "--max-windows"),
    max_segments: int = typer.Option(360, "--max-segments"),
    max_probes_per_segment: int = typer.Option(3, "--max-probes-per-segment"),
    max_provider_calls: int = typer.Option(420, "--max-provider-calls"),
    provider_order: list[ProviderName] | None = typer.Option(None, "--provider-order"),
) -> None:
    context = create_context()
    job = context.manager.create_job(
        _job_create(
            inputs,
            prefer_separation=prefer_separation,
            analysis_mode=analysis_mode,
            recall_profile=recall_profile,
            metadata_hints=metadata_hints,
            repeat_detection=repeat_detection,
            max_windows=max_windows,
            max_segments=max_segments,
            max_probes_per_segment=max_probes_per_segment,
            max_provider_calls=max_provider_calls,
            provider_order=provider_order,
        )
    )
    _spawn_worker(job.id)
    payload = {
        "job": context.db.get_job(job.id).model_dump(),
        "items": [item.model_dump() for item in context.db.get_source_items(job.id)],
        "segments": [segment.model_dump() for segment in context.db.get_segments(job.id)],
        "events": [event.model_dump() for event in context.db.list_events(job.id)],
    }
    if json_output:
        console.print_json(json.dumps(payload))
        return
    console.print(job.id)


@app.command("worker", hidden=True)
def worker(job_id: str) -> None:
    context = create_context()
    job = context.db.get_job(job_id)
    if not job:
        raise typer.BadParameter(f"Unknown job: {job_id}")
    context.manager.run_existing_job(job_id)


@app.command("cancel")
def cancel_job(job_id: str, json_output: bool = typer.Option(False, "--json")) -> None:
    context = create_context()
    context.manager.cancel(job_id)
    job = context.db.get_job(job_id)
    payload = {"job_id": job_id, "status": job.status.value if job else "canceled"}
    if json_output:
        console.print_json(json.dumps(payload))
        return
    console.print(f"{job_id}: {payload['status']}")


@app.command()
def serve(
    host: str = typer.Option("127.0.0.1"),
    port: int = typer.Option(7766),
) -> None:
    context = create_context()
    if host not in {"127.0.0.1", "localhost", "::1"} and not context.settings.api_token:
        raise typer.BadParameter("Set MUSIC_FETCH_API_TOKEN before binding the API to a non-loopback host.")
    uvicorn.run(create_api(context), host=host, port=port)


@app.command()
def tui() -> None:
    context = create_context()
    launch_tui(context)


@app.command("library")
def library(
    limit: int = typer.Option(50, "--limit"),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    context = create_context()
    entries = context.manager.list_library_entries(limit=limit)
    if json_output:
        console.print_json(json.dumps([entry.model_dump() for entry in entries]))
        return
    table = Table(title="Music Fetch Library")
    table.add_column("Created")
    table.add_column("Title")
    table.add_column("Status")
    table.add_column("Segments")
    table.add_column("Artifacts")
    for entry in entries:
        table.add_row(
            entry.created_at,
            entry.title,
            entry.status.value,
            str(entry.segment_count),
            _format_size(entry.artifact_size_bytes),
        )
    console.print(table)


@app.command("library-delete")
def library_delete(
    job_id: str,
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    """Permanently delete a library run (files + history)."""
    from .service import JobBusyError

    context = create_context()
    try:
        result = context.manager.delete_job(job_id)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    except JobBusyError as exc:
        console.print(f"[yellow]{exc}[/yellow]")
        raise typer.Exit(code=2) from exc
    if json_output:
        console.print_json(json.dumps(result))
        return
    console.print(f"{job_id}: deleted")
    if result.get("failed_paths"):
        console.print(f"[yellow]Some paths could not be removed ({len(result['failed_paths'])}).[/yellow]")


@app.command("library-prune-zombies")
def library_prune_zombies(json_output: bool = typer.Option(False, "--json")) -> None:
    """Remove library rows whose artifacts are gone."""
    context = create_context()
    result = context.manager.prune_zombie_library_entries()
    if json_output:
        console.print_json(json.dumps(result))
        return
    removed = result.get("removed_job_ids", [])
    console.print(f"Pruned {len(removed)} zombie run(s).")


@app.command("job")
def show_job(
    job_id: str,
    json_output: bool = typer.Option(False, "--json"),
    human: bool = typer.Option(False, "--human"),
) -> None:
    if json_output and human:
        raise typer.BadParameter("Use either --json or --human, not both")
    context = create_context(recover_orphans=False)
    payload = _job_snapshot(context, job_id)
    if human:
        _print_job_snapshot(payload)
        return
    console.print_json(json.dumps(payload))


@app.command("jobs")
def list_jobs(
    limit: int = typer.Option(20, "--limit"),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    if limit < 1:
        raise typer.BadParameter("--limit must be at least 1")
    context = create_context(recover_orphans=False)
    jobs = context.db.list_jobs(limit=limit)
    payload = [job.model_dump() for job in jobs]
    if json_output:
        console.print_json(json.dumps(payload))
        return
    if not payload:
        console.print("No jobs found.")
        return
    table = Table(title="Recent Jobs")
    table.add_column("Created")
    table.add_column("Updated")
    table.add_column("Status")
    table.add_column("Inputs")
    table.add_column("Job ID")
    table.add_column("Error")
    for job in payload:
        table.add_row(
            str(job.get("created_at", "")),
            str(job.get("updated_at", "")),
            _status_value(job.get("status")),
            str(len(job.get("inputs") or [])),
            str(job.get("id", "")),
            str(job.get("error") or ""),
        )
    console.print(table)


@app.command("metrics")
def recognition_metrics(
    job_id: str,
    json_output: bool = typer.Option(False, "--json"),
    provider: list[str] | None = typer.Option(None, "--provider"),
    outcome: list[str] | None = typer.Option(None, "--outcome"),
    metric_type: list[str] | None = typer.Option(None, "--metric-type"),
    source_item_id: str | None = typer.Option(None, "--source-item-id"),
    matched: bool | None = typer.Option(None, "--matched/--unmatched"),
    cache_hit: bool | None = typer.Option(None, "--cache-hit/--cache-miss"),
    summary_only: bool = typer.Option(False, "--summary-only"),
) -> None:
    if summary_only and not json_output:
        raise typer.BadParameter("--summary-only requires --json")
    context = create_context(recover_orphans=False)
    payload = _metrics_payload(
        context,
        job_id,
        provider_filters=provider,
        outcome_filters=outcome,
        metric_type_filters=metric_type,
        source_item_id=source_item_id,
        matched=matched,
        cache_hit=cache_hit,
        summary_only=summary_only,
    )
    if json_output:
        console.print_json(json.dumps(payload))
        return
    _print_metrics(payload)


@app.command("recover-jobs")
def recover_jobs(
    older_than: float = typer.Option(24 * 60 * 60, "--older-than"),
    apply_changes: bool = typer.Option(False, "--apply"),
    reason: str = typer.Option("Marked stale by music-fetch recover-jobs"),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    if older_than < 0:
        raise typer.BadParameter("--older-than must be >= 0")
    context = create_context(recover_orphans=False)
    job_ids = context.db.sweep_orphan_running_jobs(
        reason=reason,
        older_than_seconds=older_than,
        dry_run=not apply_changes,
    )
    payload = {
        "job_ids": job_ids,
        "count": len(job_ids),
        "dry_run": not apply_changes,
        "older_than_seconds": older_than,
    }
    if json_output:
        console.print_json(json.dumps(payload))
        return
    action = "Recovered" if apply_changes else "Would recover"
    console.print(f"{action} {len(job_ids)} stale active job(s).")
    for job_id in job_ids:
        console.print(f"- {job_id}")
    if job_ids and not apply_changes:
        console.print("Run again with --apply to mark these jobs failed.")


@app.command("watch")
def watch_job(
    job_id: str,
    json_output: bool = typer.Option(False, "--json"),
    interval: float = typer.Option(2.0, "--interval"),
    timeout: float | None = typer.Option(None, "--timeout"),
) -> None:
    if interval < MIN_WATCH_INTERVAL_SECONDS:
        raise typer.BadParameter(f"--interval must be >= {MIN_WATCH_INTERVAL_SECONDS:g}")
    if timeout is not None and timeout < 0:
        raise typer.BadParameter("--timeout must be >= 0")

    context = create_context(recover_orphans=False)
    deadline = time.monotonic() + timeout if timeout is not None else None
    last_status: str | None = None
    last_event_id = 0
    payload: dict[str, object] | None = None
    status = ""
    timed_out = False

    while True:
        job_model = context.db.get_job(job_id)
        if not job_model:
            raise typer.BadParameter(f"Unknown job: {job_id}")
        job = job_model.model_dump()
        events = [event.model_dump() for event in context.db.list_events(job_id, after_id=last_event_id)]
        status = _status_value(job.get("status"))

        if not json_output:
            if status != last_status:
                console.print(f"{job_id}: {status}")
            for event in events:
                if not isinstance(event, dict) or _event_id(event) <= last_event_id:
                    continue
                console.print(
                    f"{event.get('created_at', '')} [{event.get('level', '')}] {event.get('message', '')}"
                )

        last_status = status
        for event in events:
            if isinstance(event, dict):
                last_event_id = max(last_event_id, _event_id(event))

        if status in TERMINAL_JOB_STATUSES:
            payload = _job_snapshot(context, job_id)
            break

        now = time.monotonic()
        if deadline is not None and now >= deadline:
            timed_out = True
            payload = _job_snapshot(context, job_id)
            break

        sleep_for = interval
        if deadline is not None:
            sleep_for = min(sleep_for, max(0.0, deadline - now))
        if sleep_for > 0:
            time.sleep(sleep_for)

    if payload is not None and json_output:
        if timed_out:
            payload = {**payload, "timed_out": True}
        console.print_json(json.dumps(payload))
    elif timed_out:
        console.print(f"[yellow]Timed out waiting for {job_id} after {timeout:.1f}s.[/yellow]")

    if timed_out:
        raise typer.Exit(code=2)
    raise typer.Exit(code=1 if status in {JobStatus.FAILED.value, JobStatus.CANCELED.value} else 0)


@app.command("retry")
def retry_segments(
    job_id: str,
    source_item_id: str | None = typer.Option(None, "--source-item-id"),
    json_output: bool = typer.Option(False, "--json"),
    prefer_separation: bool = typer.Option(True, "--prefer-separation/--no-prefer-separation"),
    analysis_mode: AnalysisMode = typer.Option(AnalysisMode.AUTO, "--analysis-mode"),
    recall_profile: RecallProfile = typer.Option(RecallProfile.MAX_RECALL, "--recall-profile"),
    metadata_hints: bool = typer.Option(True, "--metadata-hints/--no-metadata-hints"),
    repeat_detection: bool = typer.Option(True, "--repeat-detection/--no-repeat-detection"),
    max_windows: int = typer.Option(24, "--max-windows"),
    max_segments: int = typer.Option(360, "--max-segments"),
    max_probes_per_segment: int = typer.Option(3, "--max-probes-per-segment"),
    max_provider_calls: int = typer.Option(420, "--max-provider-calls"),
    provider_order: list[ProviderName] | None = typer.Option(None, "--provider-order"),
) -> None:
    context = create_context()
    options = _job_create(
        [],
        prefer_separation=prefer_separation,
        analysis_mode=analysis_mode,
        recall_profile=recall_profile,
        metadata_hints=metadata_hints,
        repeat_detection=repeat_detection,
        max_windows=max_windows,
        max_segments=max_segments,
        max_probes_per_segment=max_probes_per_segment,
        max_provider_calls=max_provider_calls,
        provider_order=provider_order,
    ).options
    result = context.manager.retry_unresolved_segments(job_id, source_item_id=source_item_id, options_override=options)
    payload = {"job_id": job_id, **result}
    if json_output:
        console.print_json(json.dumps(payload))
        return
    console.print(
        f"{job_id}: retried {result['retried_segments']} unresolved segment(s), "
        f"matched {result['matched_segments']}, remaining {result['remaining_unresolved_segments']}"
    )


@app.command("correct")
def correct_segment(
    job_id: str,
    source_item_id: str,
    start_ms: int,
    end_ms: int,
    title: str = typer.Option(..., "--title"),
    artist: str | None = typer.Option(None, "--artist"),
    album: str | None = typer.Option(None, "--album"),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    context = create_context()
    segment = context.manager.correct_segment(
        job_id,
        source_item_id=source_item_id,
        start_ms=start_ms,
        end_ms=end_ms,
        title=title,
        artist=artist,
        album=album,
    )
    if json_output:
        console.print_json(json.dumps({"job_id": job_id, "segment": segment.model_dump(mode="json")}))
        return
    console.print(f"{job_id}: corrected {source_item_id} {start_ms}-{end_ms} -> {title}")


@app.command("export")
def export_job(
    job_id: str,
    format: str = typer.Option("json", "--format"),
    output: str | None = typer.Option(None, "--output"),
) -> None:
    context = create_context()
    filename, content = context.manager.export_job(job_id, export_format=format)
    if output:
        Path(output).expanduser().write_text(content, encoding="utf-8")
        console.print(Path(output).expanduser())
        return
    if format.lower() == "json":
        console.print_json(content)
        return
    console.print(content)


@app.command("eval")
def evaluate(
    manifest: str,
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    context = create_context()
    report = run_evaluation_manifest(context.manager, Path(manifest).expanduser().resolve())
    if json_output:
        console.print_json(report.model_dump_json())
        return
    table = Table(title="Music Fetch Evaluation")
    table.add_column("Case")
    table.add_column("Status")
    table.add_column("Precision")
    table.add_column("Recall")
    table.add_column("Runtime")
    for case in report.case_results:
        table.add_row(case.case_id, case.status.value, f"{case.precision:.2f}", f"{case.recall:.2f}", f"{case.runtime_ms} ms")
    console.print(table)


@catalog_app.command("import")
def import_catalog(paths: list[str]) -> None:
    context = create_context()
    count = context.manager.import_catalog([Path(path).expanduser().resolve() for path in paths])
    console.print(f"Imported {count} tracks into the local catalog.")


@app.command()
def doctor(json_output: bool = typer.Option(False, "--json")) -> None:
    context = create_context()
    checks = run_doctor(context.settings)
    if json_output:
        console.print_json(json.dumps([check.__dict__ for check in checks]))
        return
    table = Table(title="Music Fetch Doctor")
    table.add_column("Check")
    table.add_column("Status")
    table.add_column("Detail")
    for check in checks:
        table.add_row(check.name, "ok" if check.ok else "missing", check.detail)
    console.print(table)


@app.command("install-deps")
def install_deps(
    json_output: bool = typer.Option(False, "--json"),
    include_optional: bool = typer.Option(False, "--include-optional"),
) -> None:
    context = create_context()
    result = install_dependencies(context.settings, include_optional=include_optional)
    if json_output:
        console.print_json(
            json.dumps(
                {
                    "installed": result.installed,
                    "skipped": result.skipped,
                    "failed": result.failed,
                    "checks": [check.__dict__ for check in result.checks],
                }
            )
        )
        raise typer.Exit(code=0 if not result.failed else 1)

    table = Table(title="Dependency Installation")
    table.add_column("Category")
    table.add_column("Values")
    table.add_row("Installed", ", ".join(result.installed) or "-")
    table.add_row("Skipped", ", ".join(result.skipped) or "-")
    table.add_row("Failed", ", ".join(result.failed) or "-")
    console.print(table)
    raise typer.Exit(code=0 if not result.failed else 1)


@storage_app.command("summary")
def storage_summary(
    job_id: str | None = typer.Option(None, "--job-id"),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    context = create_context()
    summary = context.manager.storage_summary(job_id)
    if json_output:
        console.print_json(summary.model_dump_json())
        return
    table = Table(title="Music Fetch Storage")
    table.add_column("Category")
    table.add_column("Count")
    table.add_column("Size")
    for category in summary.categories:
        table.add_row(category.category.value, str(category.count), _format_size(category.size_bytes))
    console.print(table)


@storage_app.command("cleanup")
def storage_cleanup(
    job_id: str | None = typer.Option(None, "--job-id"),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    context = create_context()
    summary = context.manager.cleanup_job_artifacts(job_id) if job_id else context.manager.cleanup_temporary_artifacts()
    if json_output:
        console.print_json(summary.model_dump_json())
        return
    console.print(f"Remaining temporary artifacts: {_format_size(summary.total_size_bytes)}")


@storage_app.command("pin")
def storage_pin(
    job_id: str,
    pinned: bool = typer.Option(True, "--pinned/--unpinned"),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    context = create_context()
    value = context.manager.set_job_pinned(job_id, pinned)
    if json_output:
        console.print_json(json.dumps({"job_id": job_id, "pinned": value}))
        return
    console.print(f"{job_id}: {'pinned' if value else 'unpinned'}")


def _format_size(size_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB"]
    value = float(size_bytes)
    unit = units[0]
    for candidate in units[1:]:
        if value < 1024:
            break
        value /= 1024
        unit = candidate
    if unit == "B":
        return f"{int(value)} {unit}"
    return f"{value:.1f} {unit}"


def _spawn_worker(job_id: str) -> None:
    command = [sys.executable, "-m", "music_fetch", "worker", job_id]
    kwargs = {
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
        "cwd": os.getcwd(),
        "start_new_session": True,
    }
    subprocess.Popen(command, **kwargs)


def run() -> None:
    app()
