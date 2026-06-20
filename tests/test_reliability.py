"""Tests for the v0.4.x backend reliability pass.

Each test targets a footgun fixed in that pass:

* SQLite busy_timeout under concurrent writes
* Subprocess timeout + SIGKILL escalation
* SSRF blocking in the direct-HTTP downloader
* Upload size cap
* Opt-in orphan RUNNING/QUEUED job recovery
* Bulk delete batching with async artifact cleanup
* Defensive provider response parsing
* Cancellation status-write race
"""

from __future__ import annotations

import socket
import sys
import time
from pathlib import Path

import pytest

from music_fetch.config import Settings
from music_fetch.db import Database
from music_fetch.models import (
    DetectedSegment,
    ItemStatus,
    JobOptions,
    JobStatus,
    SegmentKind,
    SourceItem,
    SourceKind,
    SourceMetadata,
)
from music_fetch.providers.acrcloud import ACRCloudProvider
from music_fetch.providers.audd import AudDProvider
from music_fetch.service import JobCanceled, JobManager
from music_fetch.sources import (
    UnsafeURLError,
    _assert_safe_external_host,
    _assert_safe_external_url,
    download_direct_http,
)
from music_fetch.utils import (
    CommandTimeoutError,
    cancel_job_processes,
    command_job_context,
    run_command,
)


# ---------------------------------------------------------------------------
# SQLite busy_timeout
# ---------------------------------------------------------------------------


def test_connect_sets_busy_timeout(tmp_path: Path) -> None:
    """The PRAGMA must be applied to every connection, not just the first.

    Without busy_timeout, a writer that finds the WAL locked raises
    SQLITE_BUSY immediately instead of waiting. With the nested executor
    pools in this app, that's a >50% failure rate under load.
    """
    db = Database(tmp_path / "test.sqlite3")
    with db.connect() as conn:
        row = conn.execute("PRAGMA busy_timeout").fetchone()
    # SQLite returns the configured value in milliseconds.
    assert row[0] >= 1000, f"busy_timeout pragma not applied (got {row[0]})"


# ---------------------------------------------------------------------------
# Subprocess timeout + kill escalation
# ---------------------------------------------------------------------------


def test_run_command_enforces_timeout() -> None:
    """A hung subprocess must be killed, not allowed to block forever."""
    if sys.platform == "win32":
        pytest.skip("POSIX-only signal semantics")
    start = time.monotonic()
    with pytest.raises(CommandTimeoutError):
        run_command(["/bin/sh", "-c", "sleep 30"], timeout=1.0)
    elapsed = time.monotonic() - start
    # Must return within ~timeout + grace (3s default) + tiny epsilon.
    assert elapsed < 8.0, f"run_command did not kill the child promptly ({elapsed:.1f}s)"


def test_cancel_job_processes_escalates_to_sigkill() -> None:
    """A child that ignores SIGTERM must still die via SIGKILL.

    Simulates a torch/audio_separator process that installs its own
    handler — we trap SIGTERM in a sh subshell so it never exits on
    SIGTERM, and verify the escalator kills it within the grace window.
    """
    if sys.platform == "win32":
        pytest.skip("POSIX-only signal semantics")
    # ``trap '' TERM`` swallows SIGTERM; without escalation the child
    # would sleep the full 30s.
    script = "trap '' TERM; sleep 30"

    job_id = "test-job-escalate"

    import threading

    # Start the command on a worker thread so we can cancel it externally.
    def runner() -> None:
        try:
            with command_job_context(job_id):
                run_command(["/bin/sh", "-c", script], timeout=20.0)
        except Exception:
            pass

    thread = threading.Thread(target=runner)
    thread.start()
    time.sleep(0.5)  # let the child install its trap

    start = time.monotonic()
    killed = cancel_job_processes(job_id, grace_seconds=1.0)
    thread.join(timeout=5.0)
    elapsed = time.monotonic() - start

    assert killed >= 1, "cancel_job_processes did not signal any process"
    assert not thread.is_alive(), "subprocess survived SIGKILL escalation"
    # SIGTERM grace + SIGKILL wait should be under 4s.
    assert elapsed < 4.0, f"escalation took too long ({elapsed:.1f}s)"


# ---------------------------------------------------------------------------
# SSRF blocking
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "host",
    [
        "127.0.0.1",
        "localhost",
        "169.254.169.254",  # AWS / GCP metadata
        "0.0.0.0",
        "10.0.0.1",
        "192.168.1.1",
        "[::1]",
    ],
)
def test_assert_safe_external_host_rejects_private_addresses(host: str) -> None:
    with pytest.raises(UnsafeURLError):
        _assert_safe_external_host(host)


def test_assert_safe_external_url_rejects_non_http_scheme() -> None:
    with pytest.raises(UnsafeURLError):
        _assert_safe_external_url("file:///etc/passwd")
    with pytest.raises(UnsafeURLError):
        _assert_safe_external_url("gopher://internal/")


def test_download_direct_http_refuses_loopback(tmp_path: Path) -> None:
    with pytest.raises(UnsafeURLError):
        download_direct_http("http://127.0.0.1:8080/track.mp3", tmp_path / "out.mp3")


def test_download_direct_http_blocks_redirect_to_metadata(monkeypatch, tmp_path: Path) -> None:
    """A 302 from a public host into a metadata IP must be blocked.

    Mocks an httpx client whose first hop is "public", then returns a
    redirect Location pointing at AWS metadata. Without per-hop
    validation an attacker could exfiltrate IAM credentials from a
    host that has them.
    """
    import httpx

    class FakeResponse:
        def __init__(self, *, redirect_to: str | None = None) -> None:
            self.is_redirect = redirect_to is not None
            self.headers = {"location": redirect_to} if redirect_to else {}

        def raise_for_status(self) -> None:
            return None

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def iter_bytes(self):
            yield b""

    class FakeClient:
        def __init__(self, *args, **kwargs):
            self.calls = 0

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def stream(self, method, url):
            self.calls += 1
            return FakeResponse(redirect_to="http://169.254.169.254/latest/meta-data/")

    monkeypatch.setattr("music_fetch.sources.httpx.Client", FakeClient)
    # Bypass the first-hop check so we exercise the redirect-hop check.
    monkeypatch.setattr(
        "music_fetch.sources._assert_safe_external_url",
        lambda url: None if "169.254" not in url else (_ for _ in ()).throw(UnsafeURLError("blocked")),
    )
    with pytest.raises(UnsafeURLError):
        download_direct_http("https://cdn.example.com/file.mp3", tmp_path / "out.mp3")


# ---------------------------------------------------------------------------
# Upload size cap
# ---------------------------------------------------------------------------


def test_upload_endpoint_rejects_oversized_body(tmp_path: Path) -> None:
    """An upload bigger than ``max_upload_bytes`` returns 413."""
    from fastapi.testclient import TestClient

    from music_fetch.api import create_api
    from music_fetch.context import AppContext

    class StubManager:
        def submit(self, payload):
            return type("Job", (), {"id": "job-1", "status": JobStatus.QUEUED})()

    settings = type(
        "Settings",
        (),
        {"api_token": None, "cache_dir": tmp_path, "max_upload_bytes": 16},
    )()
    context = AppContext(settings=settings, db=None, manager=StubManager())
    client = TestClient(create_api(context))

    response = client.post(
        "/v1/uploads",
        files={"file": ("big.wav", b"x" * 1024, "audio/wav")},
    )
    assert response.status_code == 413


# ---------------------------------------------------------------------------
# Orphan job recovery
# ---------------------------------------------------------------------------


def test_sweep_orphan_running_jobs_marks_stuck_rows(tmp_path: Path) -> None:
    """Jobs left in RUNNING after a process crash must be cleaned up.

    Without the sweep the UI keeps showing a phantom spinner forever
    and ``cleanup_temporary_artifacts`` skips the orphan as "still
    active."
    """
    settings = Settings(base_dir=str(tmp_path), max_workers=1)
    db = Database(settings.db_path)
    stale_timestamp = "2000-01-01T00:00:00+00:00"
    # Seed a RUNNING and a QUEUED row directly — simulates a crash mid-flight.
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO jobs (id, status, created_at, updated_at, inputs_json, options_json, error, cancel_requested) "
            "VALUES (?, ?, ?, ?, '[]', '{}', NULL, 0)",
            ("run-1", JobStatus.RUNNING, stale_timestamp, stale_timestamp),
        )
        conn.execute(
            "INSERT INTO jobs (id, status, created_at, updated_at, inputs_json, options_json, error, cancel_requested) "
            "VALUES (?, ?, ?, ?, '[]', '{}', NULL, 0)",
            ("queue-1", JobStatus.QUEUED, stale_timestamp, stale_timestamp),
        )
        conn.execute(
            "INSERT INTO jobs (id, status, created_at, updated_at, inputs_json, options_json, error, cancel_requested) "
            "VALUES (?, ?, datetime('now'), datetime('now'), '[]', '{}', NULL, 0)",
            ("fresh-1", JobStatus.RUNNING),
        )
        conn.execute(
            "INSERT INTO jobs (id, status, created_at, updated_at, inputs_json, options_json, error, cancel_requested) "
            "VALUES (?, ?, datetime('now'), datetime('now'), '[]', '{}', NULL, 0)",
            ("done-1", JobStatus.SUCCEEDED),
        )
        conn.commit()

    swept = db.sweep_orphan_running_jobs(reason="restarted", older_than_seconds=60)
    assert set(swept) == {"run-1", "queue-1"}
    # Already-terminal jobs are left alone.
    assert db.get_job("done-1").status == JobStatus.SUCCEEDED
    # Fresh active jobs are not assumed orphaned.
    assert db.get_job("fresh-1").status == JobStatus.RUNNING
    assert db.get_job("run-1").status == JobStatus.FAILED
    assert db.get_job("run-1").error == "restarted"


def test_jobmanager_does_not_sweep_on_default_construction(tmp_path: Path) -> None:
    settings = Settings(base_dir=str(tmp_path), max_workers=1)
    db = Database(settings.db_path)
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO jobs (id, status, created_at, updated_at, inputs_json, options_json, error, cancel_requested) "
            "VALUES (?, ?, datetime('now'), datetime('now'), '[]', '{}', NULL, 0)",
            ("stuck-1", JobStatus.RUNNING),
        )
        conn.commit()
    JobManager(settings, db)
    assert db.get_job("stuck-1").status == JobStatus.RUNNING


def test_sweep_orphan_running_jobs_dry_run_preserves_rows(tmp_path: Path) -> None:
    settings = Settings(base_dir=str(tmp_path), max_workers=1)
    db = Database(settings.db_path)
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO jobs (id, status, created_at, updated_at, inputs_json, options_json, error, cancel_requested) "
            "VALUES (?, ?, ?, ?, '[]', '{}', NULL, 0)",
            ("stuck-1", JobStatus.RUNNING, "2000-01-01T00:00:00+00:00", "2000-01-01T00:00:00+00:00"),
        )
        conn.commit()

    swept = db.sweep_orphan_running_jobs(older_than_seconds=60, dry_run=True)

    assert swept == ["stuck-1"]
    assert db.get_job("stuck-1").status == JobStatus.RUNNING


def test_jobmanager_sweeps_when_recovery_requested(tmp_path: Path) -> None:
    settings = Settings(base_dir=str(tmp_path), max_workers=1)
    db = Database(settings.db_path)
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO jobs (id, status, created_at, updated_at, inputs_json, options_json, error, cancel_requested) "
            "VALUES (?, ?, ?, ?, '[]', '{}', NULL, 0)",
            ("stuck-1", JobStatus.RUNNING, "2000-01-01T00:00:00+00:00", "2000-01-01T00:00:00+00:00"),
        )
        conn.commit()
    JobManager(settings, db, recover_orphans=True)
    assert db.get_job("stuck-1").status == JobStatus.FAILED


# ---------------------------------------------------------------------------
# Bulk delete batching + async cleanup
# ---------------------------------------------------------------------------


def test_delete_jobs_bulk_uses_single_cascade(tmp_path: Path) -> None:
    """``delete_jobs_bulk`` removes hundreds of rows in one transaction.

    The previous behaviour opened a connection per job. Here we just
    verify the cascade actually wipes the rows and that the call
    returns the count.
    """
    settings = Settings(base_dir=str(tmp_path), max_workers=1)
    db = Database(settings.db_path)
    ids = [f"job-{i}" for i in range(25)]
    with db.connect() as conn:
        for jid in ids:
            conn.execute(
                "INSERT INTO jobs (id, status, created_at, updated_at, inputs_json, options_json, error, cancel_requested) "
                "VALUES (?, ?, datetime('now'), datetime('now'), '[]', '{}', NULL, 0)",
                (jid, JobStatus.SUCCEEDED),
            )
        conn.commit()
    removed = db.delete_jobs_bulk(ids)
    assert removed == len(ids)
    assert db.list_jobs(limit=1000) == []


# ---------------------------------------------------------------------------
# Defensive provider parsing
# ---------------------------------------------------------------------------


def test_audd_provider_handles_missing_title(monkeypatch, tmp_path: Path) -> None:
    """AudD partial-match responses without a ``title`` must not crash.

    Previously ``result["title"]`` KeyError-ed and the worker raised; we
    now return an empty match list and let the engine try the next
    provider.
    """
    import httpx

    clip = tmp_path / "clip.wav"
    clip.write_bytes(b"RIFF")  # content doesn't matter; we mock the post

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            # ``result`` envelope present but with no usable title.
            return {"status": "success", "result": {"artist": "Unknown"}}

    def fake_post(*args, **kwargs):
        return FakeResponse()

    monkeypatch.setattr(httpx, "post", fake_post)
    provider = AudDProvider(token="t")
    assert provider.recognize(clip, 0, 1000) == []


def test_acrcloud_provider_handles_missing_title(monkeypatch, tmp_path: Path) -> None:
    import httpx

    clip = tmp_path / "clip.wav"
    clip.write_bytes(b"RIFF")

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            # ``music`` array present but the top item has no title field.
            return {"metadata": {"music": [{"acrid": "abc"}]}}

    def fake_post(*args, **kwargs):
        return FakeResponse()

    monkeypatch.setattr(httpx, "post", fake_post)
    provider = ACRCloudProvider(host="acr", access_key="k", access_secret="s")
    assert provider.recognize(clip, 0, 1000) == []


# ---------------------------------------------------------------------------
# Cancellation race
# ---------------------------------------------------------------------------


def test_update_job_respects_not_if_status_in(tmp_path: Path) -> None:
    """``not_if_status_in`` guards a status write against a racing cancel.

    This is the core of the cancellation TOCTOU fix: even if the worker
    decides to write RUNNING after a cancel sneaks past the pre-check,
    the WHERE clause vetoes the write so CANCELED sticks.
    """
    settings = Settings(base_dir=str(tmp_path), max_workers=1)
    db = Database(settings.db_path)
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO jobs (id, status, created_at, updated_at, inputs_json, options_json, error, cancel_requested) "
            "VALUES (?, ?, datetime('now'), datetime('now'), '[]', '{}', NULL, 0)",
            ("race-1", JobStatus.CANCELED),
        )
        conn.commit()
    wrote = db.update_job(
        "race-1",
        status=JobStatus.RUNNING,
        not_if_status_in=[JobStatus.CANCELED],
    )
    assert wrote is False
    assert db.get_job("race-1").status == JobStatus.CANCELED


def test_jobcanceled_is_not_caught_by_except_exception() -> None:
    """``JobCanceled`` is a ``BaseException`` so it doesn't get swallowed.

    Previously a generic ``except Exception`` around a worker step could
    catch the sentinel and mis-classify a cancel as a soft failure.
    """
    try:
        try:
            raise JobCanceled()
        except Exception:  # noqa: BLE001 — deliberately testing this shape
            pytest.fail("JobCanceled was caught by `except Exception`")
    except JobCanceled:
        pass
