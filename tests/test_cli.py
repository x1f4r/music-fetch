from __future__ import annotations

import json
from typer.testing import CliRunner

from music_fetch.cli import app


runner = CliRunner()


class DumpModel:
    def __init__(self, payload: dict) -> None:
        self.payload = payload

    def model_dump(self):
        return self.payload


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
    def __init__(self) -> None:
        self.requested_limit: int | None = None
        self.sweep_payload: dict | None = None
        self.event_after_ids: list[int] = []

    def get_job(self, job_id):
        if job_id == "missing":
            return None
        return DumpModel(
            {
                "id": job_id,
                "status": "queued",
                "created_at": "2026-06-20T10:00:00+00:00",
                "updated_at": "2026-06-20T10:01:00+00:00",
                "inputs": ["https://example.com/test"],
                "error": None,
            }
        )

    def list_jobs(self, limit=50):
        self.requested_limit = limit
        return [
            DumpModel(
                {
                    "id": "job-1",
                    "status": "queued",
                    "created_at": "2026-06-20T10:00:00+00:00",
                    "updated_at": "2026-06-20T10:01:00+00:00",
                    "inputs": ["https://example.com/one"],
                    "error": None,
                }
            ),
            DumpModel(
                {
                    "id": "job-2",
                    "status": "succeeded",
                    "created_at": "2026-06-20T09:00:00+00:00",
                    "updated_at": "2026-06-20T09:05:00+00:00",
                    "inputs": ["https://example.com/two"],
                    "error": None,
                }
            ),
        ][:limit]

    def get_source_items(self, job_id):
        return [
            DumpModel(
                {
                    "id": "item-1",
                    "job_id": job_id,
                    "input_value": "https://example.com/test",
                    "kind": "yt_dlp",
                    "status": "succeeded",
                    "metadata": {"title": "Example Clip"},
                    "error": None,
                }
            )
        ]

    def get_segments(self, job_id):
        return [
            DumpModel(
                {
                    "source_item_id": "item-1",
                    "start_ms": 0,
                    "end_ms": 12000,
                    "kind": "matched_track",
                    "confidence": 0.91,
                    "providers": ["vibra"],
                    "track": {"title": "Song", "artist": "Artist"},
                }
            )
        ]

    def list_events(self, job_id, after_id=0):
        self.event_after_ids.append(after_id)
        return [
            DumpModel(
                {
                    "id": 1,
                    "job_id": job_id,
                    "level": "info",
                    "message": "seed event",
                    "created_at": "2026-06-20T10:01:00+00:00",
                }
            )
        ]

    def sweep_orphan_running_jobs(self, **payload):
        self.sweep_payload = payload
        return ["job-stale"]


class WatchDb(DummyDb):
    def __init__(self) -> None:
        super().__init__()
        self.statuses = ["running", "succeeded"]
        self.get_job_calls = 0

    def get_job(self, job_id):
        status = self.statuses[min(self.get_job_calls, len(self.statuses) - 1)]
        self.get_job_calls += 1
        return DumpModel(
            {
                "id": job_id,
                "status": status,
                "created_at": "2026-06-20T10:00:00+00:00",
                "updated_at": "2026-06-20T10:02:00+00:00",
                "inputs": ["https://example.com/test"],
                "error": None,
            }
        )

    def list_events(self, job_id, after_id=0):
        self.event_after_ids.append(after_id)
        events = [
            {
                "id": 1,
                "job_id": job_id,
                "level": "info",
                "message": "started",
                "created_at": "2026-06-20T10:01:00+00:00",
            }
        ]
        if self.get_job_calls >= 2:
            events.append(
                {
                    "id": 2,
                    "job_id": job_id,
                    "level": "info",
                    "message": "finished",
                    "created_at": "2026-06-20T10:02:00+00:00",
                }
            )
        return [DumpModel(event) for event in events if event["id"] > after_id]


class CanceledWatchDb(WatchDb):
    def __init__(self) -> None:
        super().__init__()
        self.statuses = ["canceled"]


class FailedWatchDb(WatchDb):
    def __init__(self) -> None:
        super().__init__()
        self.statuses = ["failed"]


class TimeoutWatchDb(WatchDb):
    def __init__(self) -> None:
        super().__init__()
        self.statuses = ["running"]


class DummyContext:
    def __init__(self, db=None) -> None:
        self.manager = DummyManager()
        self.db = db or DummyDb()


def read_only_context_factory(context):
    calls = []

    def factory(**kwargs):
        calls.append(kwargs)
        return context

    return factory, calls


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


def test_jobs_lists_recent_jobs(monkeypatch) -> None:
    context = DummyContext()
    factory, calls = read_only_context_factory(context)
    monkeypatch.setattr("music_fetch.cli.create_context", factory)

    result = runner.invoke(app, ["jobs", "--limit", "2"])

    assert result.exit_code == 0
    assert calls == [{"recover_orphans": False}]
    assert context.db.requested_limit == 2
    assert "Recent Jobs" in result.stdout
    assert "job-1" in result.stdout
    assert "job-2" in result.stdout
    assert "succeeded" in result.stdout


def test_recover_jobs_dry_run_is_guarded(monkeypatch) -> None:
    context = DummyContext()
    factory, calls = read_only_context_factory(context)
    monkeypatch.setattr("music_fetch.cli.create_context", factory)

    result = runner.invoke(app, ["recover-jobs", "--older-than", "60", "--json"])

    assert result.exit_code == 0
    assert calls == [{"recover_orphans": False}]
    assert context.db.sweep_payload == {
        "reason": "Marked stale by music-fetch recover-jobs",
        "older_than_seconds": 60.0,
        "dry_run": True,
    }
    payload = json.loads(result.stdout)
    assert payload["job_ids"] == ["job-stale"]
    assert payload["dry_run"] is True


def test_recover_jobs_apply_marks_matching_jobs(monkeypatch) -> None:
    context = DummyContext()
    factory, calls = read_only_context_factory(context)
    monkeypatch.setattr("music_fetch.cli.create_context", factory)

    result = runner.invoke(app, ["recover-jobs", "--older-than", "60", "--apply"])

    assert result.exit_code == 0
    assert calls == [{"recover_orphans": False}]
    assert context.db.sweep_payload["dry_run"] is False
    assert "Recovered 1 stale active job(s)." in result.stdout


def test_job_human_view_is_readable(monkeypatch) -> None:
    factory, calls = read_only_context_factory(DummyContext())
    monkeypatch.setattr("music_fetch.cli.create_context", factory)

    result = runner.invoke(app, ["job", "job-1", "--human"])

    assert result.exit_code == 0
    assert calls == [{"recover_orphans": False}]
    assert "Job job-1" in result.stdout
    assert "Status: queued" in result.stdout
    assert "Example Clip" in result.stdout
    assert "Artist - Song" in result.stdout
    assert "seed event" in result.stdout


def test_job_default_json_payload_is_unchanged_shape(monkeypatch) -> None:
    factory, calls = read_only_context_factory(DummyContext())
    monkeypatch.setattr("music_fetch.cli.create_context", factory)

    result = runner.invoke(app, ["job", "job-1"])

    assert result.exit_code == 0
    assert calls == [{"recover_orphans": False}]
    payload = json.loads(result.stdout)
    assert set(payload) == {"job", "items", "segments", "events"}
    assert payload["job"]["id"] == "job-1"
    assert payload["events"][0]["message"] == "seed event"


def test_job_rejects_conflicting_output_modes(monkeypatch) -> None:
    factory, calls = read_only_context_factory(DummyContext())
    monkeypatch.setattr("music_fetch.cli.create_context", factory)

    result = runner.invoke(app, ["job", "job-1", "--json", "--human"])

    assert result.exit_code != 0
    assert calls == []
    assert "Use either --json or --human" in result.output


def test_job_unknown_id_exits_with_error(monkeypatch) -> None:
    factory, calls = read_only_context_factory(DummyContext())
    monkeypatch.setattr("music_fetch.cli.create_context", factory)

    result = runner.invoke(app, ["job", "missing"])

    assert result.exit_code != 0
    assert calls == [{"recover_orphans": False}]
    assert "Unknown job: missing" in result.output


def test_watch_polls_until_terminal_and_outputs_json(monkeypatch) -> None:
    db = WatchDb()
    factory, calls = read_only_context_factory(DummyContext(db=db))
    monkeypatch.setattr("music_fetch.cli.create_context", factory)

    result = runner.invoke(app, ["watch", "job-1", "--json", "--interval", "0.05"])

    assert result.exit_code == 0
    assert calls == [{"recover_orphans": False}]
    assert db.get_job_calls == 3
    assert db.event_after_ids[:2] == [0, 1]
    payload = json.loads(result.stdout)
    assert payload["job"]["status"] == "succeeded"
    assert [event["message"] for event in payload["events"]] == ["started", "finished"]


def test_watch_prints_status_and_event_progress(monkeypatch) -> None:
    db = WatchDb()
    factory, calls = read_only_context_factory(DummyContext(db=db))
    monkeypatch.setattr("music_fetch.cli.create_context", factory)

    result = runner.invoke(app, ["watch", "job-1", "--interval", "0.05"])

    assert result.exit_code == 0
    assert calls == [{"recover_orphans": False}]
    assert "job-1: running" in result.stdout
    assert "started" in result.stdout
    assert "job-1: succeeded" in result.stdout
    assert "finished" in result.stdout


def test_watch_canceled_job_exits_nonzero(monkeypatch) -> None:
    factory, calls = read_only_context_factory(DummyContext(db=CanceledWatchDb()))
    monkeypatch.setattr("music_fetch.cli.create_context", factory)

    result = runner.invoke(app, ["watch", "job-1", "--interval", "0.05"])

    assert result.exit_code == 1
    assert calls == [{"recover_orphans": False}]
    assert "job-1: canceled" in result.stdout


def test_watch_failed_job_exits_nonzero(monkeypatch) -> None:
    factory, calls = read_only_context_factory(DummyContext(db=FailedWatchDb()))
    monkeypatch.setattr("music_fetch.cli.create_context", factory)

    result = runner.invoke(app, ["watch", "job-1", "--json", "--interval", "0.05"])

    assert result.exit_code == 1
    assert calls == [{"recover_orphans": False}]
    assert json.loads(result.stdout)["job"]["status"] == "failed"


def test_watch_timeout_exits_two_and_marks_json(monkeypatch) -> None:
    factory, calls = read_only_context_factory(DummyContext(db=TimeoutWatchDb()))
    monkeypatch.setattr("music_fetch.cli.create_context", factory)

    result = runner.invoke(app, ["watch", "job-1", "--json", "--interval", "0.05", "--timeout", "0"])

    assert result.exit_code == 2
    assert calls == [{"recover_orphans": False}]
    payload = json.loads(result.stdout)
    assert payload["job"]["status"] == "running"
    assert payload["timed_out"] is True


def test_watch_rejects_tight_poll_interval(monkeypatch) -> None:
    factory, calls = read_only_context_factory(DummyContext(db=TimeoutWatchDb()))
    monkeypatch.setattr("music_fetch.cli.create_context", factory)

    result = runner.invoke(app, ["watch", "job-1", "--interval", "0"])

    assert result.exit_code != 0
    assert calls == []
    assert "--interval must be >=" in result.output


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
