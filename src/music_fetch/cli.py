from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import typer
import uvicorn
from rich.console import Console
from rich.table import Table

from .api import create_api
from .context import create_context
from .doctor import run_doctor
from .eval import run_evaluation_manifest
from .installer import install_dependencies
from .models import AnalysisMode, JobCreate, JobOptions, ProviderName, RecallProfile
from .tui import launch_tui

app = typer.Typer(help="Music Fetch CLI")
catalog_app = typer.Typer(help="Local catalog management")
storage_app = typer.Typer(help="Artifact and storage management")
app.add_typer(catalog_app, name="catalog")
app.add_typer(storage_app, name="storage")
console = Console()


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
def show_job(job_id: str, json_output: bool = typer.Option(False, "--json")) -> None:
    context = create_context()
    job = context.db.get_job(job_id)
    if not job:
        raise typer.BadParameter(f"Unknown job: {job_id}")
    payload = {
        "job": job.model_dump(),
        "items": [item.model_dump() for item in context.db.get_source_items(job_id)],
        "segments": [segment.model_dump() for segment in context.db.get_segments(job_id)],
        "events": [event.model_dump() for event in context.db.list_events(job_id)],
    }
    if json_output:
        console.print_json(json.dumps(payload))
        return
    console.print_json(json.dumps(payload))


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
