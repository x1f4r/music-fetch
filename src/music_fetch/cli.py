from __future__ import annotations

import json
from pathlib import Path

import typer
import uvicorn
from rich.console import Console
from rich.table import Table

from .api import create_api
from .context import create_context
from .doctor import run_doctor
from .installer import install_dependencies
from .models import JobCreate
from .tui import launch_tui

app = typer.Typer(help="Music Fetch CLI")
catalog_app = typer.Typer(help="Local catalog management")
storage_app = typer.Typer(help="Artifact and storage management")
app.add_typer(catalog_app, name="catalog")
app.add_typer(storage_app, name="storage")
console = Console()


@app.command()
def analyze(inputs: list[str], json_output: bool = typer.Option(False, "--json")) -> None:
    context = create_context()
    job = context.manager.submit(JobCreate(inputs=inputs))
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
def submit_job(inputs: list[str], json_output: bool = typer.Option(False, "--json")) -> None:
    context = create_context()
    job = context.manager.submit(JobCreate(inputs=inputs))
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


def run() -> None:
    app()
