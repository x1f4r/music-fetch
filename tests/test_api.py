from __future__ import annotations

from fastapi.testclient import TestClient

from music_fetch.api import create_api
from music_fetch.context import AppContext
from music_fetch.models import JobCreate, JobStatus


class DummyManager:
    def __init__(self) -> None:
        self.submitted = []

    def submit(self, payload: JobCreate):
        self.submitted.append(payload)
        return type("Job", (), {"id": "job-1", "status": JobStatus.QUEUED})()

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

    def cleanup_job_artifacts(self, job_id):
        return {"job_id": job_id, "total_size_bytes": 0}

    def cleanup_temporary_artifacts(self):
        return {"job_id": None, "total_size_bytes": 0}

    def set_job_pinned(self, job_id, pinned):
        return pinned


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
    context = AppContext(settings=type("Settings", (), {"api_token": None})(), db=DummyDb(), manager=DummyManager())
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
