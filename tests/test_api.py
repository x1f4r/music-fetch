from __future__ import annotations

from fastapi.testclient import TestClient

from music_fetch.api import create_api
from music_fetch.context import AppContext
from music_fetch.models import DetectedSegment, JobCreate, JobStatus, SegmentKind


class DummyManager:
    def __init__(self) -> None:
        self.submitted = []
        self.canceled = []
        self.corrected = []
        self.retried = []

    def submit(self, payload: JobCreate):
        self.submitted.append(payload)
        return type("Job", (), {"id": "job-1", "status": JobStatus.QUEUED})()

    def cancel(self, job_id: str):
        self.canceled.append(job_id)

    def provider_states(self):
        return []

    def set_provider_config(self, name, config):
        return {"name": name, "enabled": config.enabled}

    def import_catalog(self, paths):
        return len(paths)

    def list_library_entries(self, limit=50):
        return []

    def storage_summary(self, job_id=None):
        return {"job_id": job_id, "total_size_bytes": 0}

    def cleanup_job_artifacts(self, job_id, *, strict: bool = False):
        return {"job_id": job_id, "total_size_bytes": 0}

    def cleanup_temporary_artifacts(self):
        return {"job_id": None, "total_size_bytes": 0}

    def set_job_pinned(self, job_id, pinned):
        return pinned

    def delete_job(self, job_id):
        return {"job_id": job_id, "deleted": True, "failed_paths": []}

    def prune_zombie_library_entries(self):
        return {"removed_job_ids": []}

    def correct_segment(self, job_id, **payload):
        self.corrected.append((job_id, payload))
        return DetectedSegment(
            source_item_id=payload["source_item_id"],
            start_ms=payload["start_ms"],
            end_ms=payload["end_ms"],
            kind=SegmentKind.MATCHED_TRACK,
            confidence=1.0,
            providers=[],
            evidence_count=1,
        )

    def retry_unresolved_segments(self, job_id, source_item_id=None, options_override=None):
        self.retried.append((job_id, source_item_id, options_override))
        return {"retried_segments": 1, "matched_segments": 1, "remaining_unresolved_segments": 0}

    def export_job(self, job_id, export_format="json"):
        return (f"{job_id}.{export_format}", "demo")


class DummyDb:
    def get_job(self, job_id):
        if job_id != "job-1":
            return None
        return type("Job", (), {"id": "job-1", "status": JobStatus.QUEUED})()

    def get_source_items(self, job_id):
        return []

    def get_segments(self, job_id):
        return []

    def list_events(self, job_id, after_id=0):
        return []


def test_create_job_endpoint(tmp_path) -> None:
    context = AppContext(settings=type("Settings", (), {"api_token": None})(), db=DummyDb(), manager=DummyManager())
    client = TestClient(create_api(context))
    response = client.post("/v1/jobs", json={"inputs": ["https://example.com/video"], "options": {}})
    assert response.status_code == 200
    assert response.json()["job_id"] == "job-1"


def test_storage_and_library_endpoints() -> None:
    manager = DummyManager()
    context = AppContext(settings=type("Settings", (), {"api_token": None})(), db=DummyDb(), manager=manager)
    client = TestClient(create_api(context))

    library = client.get("/v1/library")
    assert library.status_code == 200
    assert library.json() == {"entries": []}

    storage = client.get("/v1/storage")
    assert storage.status_code == 200
    assert storage.json()["storage"]["total_size_bytes"] == 0

    cleanup = client.delete("/v1/storage", params={"job_id": "job-1"})
    assert cleanup.status_code == 200
    assert cleanup.json()["storage"]["job_id"] == "job-1"

    pin = client.put("/v1/storage/jobs/job-1/pin", json={"pinned": True})
    assert pin.status_code == 200
    assert pin.json() == {"job_id": "job-1", "pinned": True}

    cancel = client.post("/v1/jobs/job-1/cancel")
    assert cancel.status_code == 200
    assert cancel.json()["job_id"] == "job-1"
    assert manager.canceled == ["job-1"]


def test_delete_job_endpoint_success_and_404() -> None:
    """``DELETE /v1/jobs/{id}`` returns the manager result. Unknown job is 404."""
    manager = DummyManager()
    context = AppContext(settings=type("Settings", (), {"api_token": None})(), db=DummyDb(), manager=manager)
    client = TestClient(create_api(context))

    ok = client.delete("/v1/jobs/job-1")
    assert ok.status_code == 200
    body = ok.json()
    assert body["deleted"] is True
    assert body["job_id"] == "job-1"

    # DummyManager.delete_job never raises — simulate a 404 by flipping it.
    def _raise_unknown(job_id):
        raise ValueError(f"Unknown job: {job_id}")

    manager.delete_job = _raise_unknown  # type: ignore[method-assign]
    missing = client.delete("/v1/jobs/unknown")
    assert missing.status_code == 404


def test_delete_job_endpoint_409_on_running_job() -> None:
    from music_fetch.service import JobBusyError

    manager = DummyManager()

    def _raise_busy(job_id):
        raise JobBusyError(f"Job {job_id} is running")

    manager.delete_job = _raise_busy  # type: ignore[method-assign]
    context = AppContext(settings=type("Settings", (), {"api_token": None})(), db=DummyDb(), manager=manager)
    client = TestClient(create_api(context))

    response = client.delete("/v1/jobs/job-1")
    assert response.status_code == 409


def test_delete_library_entry_alias_and_prune_zombies_endpoint() -> None:
    manager = DummyManager()
    context = AppContext(settings=type("Settings", (), {"api_token": None})(), db=DummyDb(), manager=manager)
    client = TestClient(create_api(context))

    alias = client.delete("/v1/library/job-1")
    assert alias.status_code == 200
    assert alias.json()["deleted"] is True

    prune = client.post("/v1/library/prune-zombies")
    assert prune.status_code == 200
    assert prune.json() == {"removed_job_ids": []}


def test_delete_storage_surfaces_failed_paths() -> None:
    """Partial success during cleanup returns HTTP 200 plus ``failed_paths``."""
    from music_fetch.artifact_service import ArtifactCleanupError

    manager = DummyManager()

    def _partial(job_id, *, strict: bool = False):
        raise ArtifactCleanupError(["/tmp/stuck.wav"], "simulated stuck file")

    manager.cleanup_job_artifacts = _partial  # type: ignore[method-assign]
    context = AppContext(settings=type("Settings", (), {"api_token": None})(), db=DummyDb(), manager=manager)
    client = TestClient(create_api(context))

    response = client.delete("/v1/storage", params={"job_id": "job-1"})
    assert response.status_code == 200
    body = response.json()
    assert body["failed_paths"] == ["/tmp/stuck.wav"]


def test_upload_endpoint_sanitizes_filename_and_accepts_options(tmp_path) -> None:
    manager = DummyManager()
    settings = type("Settings", (), {"api_token": None, "cache_dir": tmp_path})()
    context = AppContext(settings=settings, db=DummyDb(), manager=manager)
    client = TestClient(create_api(context))

    response = client.post(
        "/v1/uploads",
        data={"options_json": '{"analysis_mode":"single_track","prefer_separation":false}'},
        files={"file": ("../../bad.wav", b"wav", "audio/wav")},
    )

    assert response.status_code == 200
    assert response.json() == {"job_id": "job-1", "status": "queued"}
    submitted = manager.submitted[0]
    assert submitted.options.analysis_mode.value == "single_track"
    assert submitted.options.prefer_separation is False
    assert ".." not in submitted.inputs[0]


def test_retry_correct_and_export_endpoints() -> None:
    manager = DummyManager()
    context = AppContext(settings=type("Settings", (), {"api_token": None})(), db=DummyDb(), manager=manager)
    client = TestClient(create_api(context))

    retry_response = client.post("/v1/jobs/job-1/segments/retry", json={"source_item_id": "item-1", "options": {"analysis_mode": "long_mix"}})
    assert retry_response.status_code == 200
    assert retry_response.json()["matched_segments"] == 1
    assert manager.retried[0][0] == "job-1"
    assert manager.retried[0][1] == "item-1"
    assert manager.retried[0][2].analysis_mode.value == "long_mix"

    correction_response = client.post(
        "/v1/jobs/job-1/segments/correct",
        json={
            "source_item_id": "item-1",
            "start_ms": 0,
            "end_ms": 12000,
            "title": "Song",
            "artist": "Artist",
        },
    )
    assert correction_response.status_code == 200
    assert correction_response.json()["segment"]["source_item_id"] == "item-1"
    assert manager.corrected[0][1]["title"] == "Song"

    export_response = client.get("/v1/jobs/job-1/export", params={"format": "chapters"})
    assert export_response.status_code == 200
    assert export_response.json() == {
        "job_id": "job-1",
        "format": "chapters",
        "filename": "job-1.chapters",
        "content": "demo",
    }
