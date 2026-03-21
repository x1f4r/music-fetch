from __future__ import annotations

import json
from typer.testing import CliRunner

from music_fetch.cli import app


runner = CliRunner()


class DummyJob:
    def __init__(self, job_id: str) -> None:
        self.id = job_id


class DummyManager:
    def create_job(self, payload):
        self.payload = payload
        return DummyJob("job-1")

    def retry_unresolved_segments(self, job_id, source_item_id=None, options_override=None):
        self.retry_payload = (job_id, source_item_id, options_override)
        return {"retried_segments": 2, "matched_segments": 1, "remaining_unresolved_segments": 1}

    def correct_segment(self, job_id, **payload):
        self.correct_payload = (job_id, payload)
        return type("Segment", (), {"model_dump": lambda self, mode=None: {"source_item_id": payload["source_item_id"]}})()

    def export_job(self, job_id, export_format="json"):
        self.export_payload = (job_id, export_format)
        return ("out.txt", "content")


class DummyDb:
    def get_job(self, job_id):
        return type("Job", (), {"model_dump": lambda self: {"id": job_id, "status": "queued"}})()

    def get_source_items(self, job_id):
        return []

    def get_segments(self, job_id):
        return []

    def list_events(self, job_id):
        return []


class DummyContext:
    def __init__(self) -> None:
        self.manager = DummyManager()
        self.db = DummyDb()


def test_submit_spawns_detached_worker(monkeypatch) -> None:
    spawned: dict[str, object] = {}

    monkeypatch.setattr("music_fetch.cli.create_context", lambda: DummyContext())

    def fake_spawn(job_id: str) -> None:
        spawned["job_id"] = job_id

    monkeypatch.setattr("music_fetch.cli._spawn_worker", fake_spawn)

    result = runner.invoke(app, ["submit", "https://example.com/test", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["job"]["id"] == "job-1"
    assert spawned["job_id"] == "job-1"


def test_submit_passes_job_options(monkeypatch) -> None:
    context = DummyContext()
    monkeypatch.setattr("music_fetch.cli.create_context", lambda: context)
    monkeypatch.setattr("music_fetch.cli._spawn_worker", lambda job_id: None)

    result = runner.invoke(
        app,
        [
            "submit",
            "https://example.com/test",
            "--analysis-mode",
            "single_track",
            "--recall-profile",
            "balanced",
            "--no-prefer-separation",
            "--json",
        ],
    )

    assert result.exit_code == 0
    assert context.manager.payload.options.analysis_mode.value == "single_track"
    assert context.manager.payload.options.recall_profile.value == "balanced"
    assert context.manager.payload.options.prefer_separation is False


def test_retry_command_passes_overrides(monkeypatch) -> None:
    context = DummyContext()
    monkeypatch.setattr("music_fetch.cli.create_context", lambda: context)

    result = runner.invoke(
        app,
        ["retry", "job-1", "--source-item-id", "item-1", "--analysis-mode", "long_mix", "--json"],
    )

    assert result.exit_code == 0
    assert context.manager.retry_payload[0] == "job-1"
    assert context.manager.retry_payload[1] == "item-1"
    assert context.manager.retry_payload[2].analysis_mode.value == "long_mix"


def test_correct_and_export_commands(monkeypatch, tmp_path) -> None:
    context = DummyContext()
    monkeypatch.setattr("music_fetch.cli.create_context", lambda: context)

    correct = runner.invoke(
        app,
        ["correct", "job-1", "item-1", "0", "12000", "--title", "Song", "--artist", "Artist", "--json"],
    )
    assert correct.exit_code == 0
    assert context.manager.correct_payload[0] == "job-1"
    assert context.manager.correct_payload[1]["title"] == "Song"

    output = tmp_path / "export.txt"
    export = runner.invoke(app, ["export", "job-1", "--format", "chapters", "--output", str(output)])
    assert export.exit_code == 0
    assert context.manager.export_payload == ("job-1", "chapters")
    assert output.read_text(encoding="utf-8") == "content"
