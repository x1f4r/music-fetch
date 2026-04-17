from __future__ import annotations

import asyncio
import json
from pathlib import Path
import tempfile
from typing import Annotated
import uuid

from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from .context import AppContext
from .models import DetectedSegment, JobCreate, JobOptions, ProviderConfig, ProviderName


class ProviderUpdate(BaseModel):
    enabled: bool = True
    config: dict = {}


class PinUpdate(BaseModel):
    pinned: bool


class JobActionResponse(BaseModel):
    job_id: str
    status: str


class SegmentCorrectionRequest(BaseModel):
    source_item_id: str
    start_ms: int
    end_ms: int
    title: str
    artist: str | None = None
    album: str | None = None


class SegmentCorrectionResponse(BaseModel):
    job_id: str
    segment: DetectedSegment


class RetrySegmentsRequest(BaseModel):
    source_item_id: str | None = None
    options: JobOptions | None = None


class RetrySegmentsResponse(BaseModel):
    job_id: str
    retried_segments: int
    matched_segments: int
    remaining_unresolved_segments: int


class ExportResponse(BaseModel):
    job_id: str
    format: str
    filename: str
    content: str


def _safe_upload_name(filename: str | None) -> str:
    raw = (filename or "").strip()
    if not raw:
        return f"upload-{uuid.uuid4().hex}.bin"
    cleaned = Path(raw).name.replace("/", "_").replace("\\", "_").strip()
    return cleaned or f"upload-{uuid.uuid4().hex}.bin"


def create_api(context: AppContext) -> FastAPI:
    app = FastAPI(title="Music Fetch", version="0.3.8")

    def require_auth(authorization: str | None = Header(default=None)) -> None:
        token = context.settings.api_token
        if not token:
            return
        expected = f"Bearer {token}"
        if authorization != expected:
            raise HTTPException(status_code=401, detail="Unauthorized")

    @app.get("/health")
    def health() -> dict:
        return {"ok": True}

    @app.post("/v1/jobs")
    async def create_job(payload: JobCreate, _: None = Depends(require_auth)) -> dict:
        job = context.manager.submit(payload)
        return {"job_id": job.id, "status": job.status}

    @app.post("/v1/jobs/{job_id}/cancel")
    async def cancel_job(job_id: str, _: None = Depends(require_auth)) -> JobActionResponse:
        job = context.db.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        context.manager.cancel(job_id)
        refreshed = context.db.get_job(job_id)
        return JobActionResponse(job_id=job_id, status=(refreshed.status.value if refreshed else "canceled"))

    @app.get("/v1/jobs/{job_id}")
    async def get_job(job_id: str, _: None = Depends(require_auth)) -> dict:
        job = context.db.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return {"job": job, "items": context.db.get_source_items(job_id)}

    @app.get("/v1/jobs/{job_id}/snapshot")
    async def get_job_snapshot(job_id: str, _: None = Depends(require_auth)) -> dict:
        job = context.db.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return {
            "job": job,
            "items": context.db.get_source_items(job_id),
            "segments": context.db.get_segments(job_id),
            "events": context.db.list_events(job_id),
        }

    @app.get("/v1/jobs/{job_id}/results")
    async def get_results(job_id: str, _: None = Depends(require_auth)) -> dict:
        job = context.db.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return {"job": job, "segments": context.db.get_segments(job_id)}

    @app.post("/v1/jobs/{job_id}/segments/correct")
    async def correct_segment(
        job_id: str,
        payload: SegmentCorrectionRequest,
        _: None = Depends(require_auth),
    ) -> SegmentCorrectionResponse:
        try:
            segment = context.manager.correct_segment(
                job_id,
                source_item_id=payload.source_item_id,
                start_ms=payload.start_ms,
                end_ms=payload.end_ms,
                title=payload.title,
                artist=payload.artist,
                album=payload.album,
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return SegmentCorrectionResponse(job_id=job_id, segment=segment)

    @app.post("/v1/jobs/{job_id}/segments/retry")
    async def retry_segments(
        job_id: str,
        payload: RetrySegmentsRequest,
        _: None = Depends(require_auth),
    ) -> RetrySegmentsResponse:
        try:
            result = context.manager.retry_unresolved_segments(
                job_id,
                source_item_id=payload.source_item_id,
                options_override=payload.options,
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return RetrySegmentsResponse(job_id=job_id, **result)

    @app.get("/v1/jobs/{job_id}/export")
    async def export_results(job_id: str, format: str = "json", _: None = Depends(require_auth)) -> ExportResponse:
        try:
            filename, content = context.manager.export_job(job_id, export_format=format)
        except ValueError as exc:
            raise HTTPException(status_code=404 if "Unknown job" in str(exc) else 400, detail=str(exc)) from exc
        return ExportResponse(job_id=job_id, format=format, filename=filename, content=content)

    @app.get("/v1/library")
    async def get_library(limit: int = 50, _: None = Depends(require_auth)) -> dict:
        return {"entries": context.manager.list_library_entries(limit=limit)}

    @app.get("/v1/jobs/{job_id}/events")
    async def stream_events(job_id: str, _: None = Depends(require_auth)) -> StreamingResponse:
        if not context.db.get_job(job_id):
            raise HTTPException(status_code=404, detail="Job not found")

        async def event_stream():
            last_id = 0
            while True:
                events = context.db.list_events(job_id, after_id=last_id)
                for event in events:
                    last_id = event.id
                    yield f"event: {event.level}\ndata: {event.model_dump_json()}\n\n"
                job = context.db.get_job(job_id)
                if job and job.status in {"succeeded", "partial_failed", "failed", "canceled"}:
                    break
                await asyncio.sleep(1.0)

        return StreamingResponse(event_stream(), media_type="text/event-stream")

    @app.post("/v1/catalog/import")
    async def import_catalog(paths: list[str], _: None = Depends(require_auth)) -> dict:
        count = context.manager.import_catalog([Path(path).expanduser().resolve() for path in paths])
        return {"imported": count}

    @app.post("/v1/uploads")
    async def upload_file(
        file: Annotated[UploadFile, File()],
        options_json: Annotated[str | None, Form()] = None,
        _: None = Depends(require_auth),
    ) -> dict:
        upload_dir = context.settings.cache_dir / "uploads"
        upload_dir.mkdir(parents=True, exist_ok=True)
        safe_name = _safe_upload_name(file.filename)
        fd, temp_path = tempfile.mkstemp(prefix="music-fetch-upload-", dir=upload_dir)
        Path(temp_path).chmod(0o600)
        try:
            import os

            os.close(fd)
        except OSError:
            pass
        target = upload_dir / f"{uuid.uuid4().hex}-{safe_name}"
        try:
            with Path(temp_path).open("wb") as handle:
                while True:
                    chunk = await file.read(1024 * 1024)
                    if not chunk:
                        break
                    handle.write(chunk)
            Path(temp_path).replace(target)
        finally:
            try:
                Path(temp_path).unlink(missing_ok=True)
            except OSError:
                pass

        options = JobOptions()
        if options_json:
            try:
                options = JobOptions.model_validate(json.loads(options_json))
            except Exception as exc:
                raise HTTPException(status_code=400, detail=f"Invalid options_json: {exc}") from exc
        job = context.manager.submit(JobCreate(inputs=[str(target)], options=options))
        return {"job_id": job.id, "status": job.status}

    @app.get("/v1/providers")
    async def list_providers(_: None = Depends(require_auth)) -> dict:
        return {"providers": context.manager.provider_states()}

    @app.put("/v1/providers/{provider_name}")
    async def update_provider(provider_name: ProviderName, payload: ProviderUpdate, _: None = Depends(require_auth)) -> dict:
        state = context.manager.set_provider_config(provider_name, ProviderConfig(enabled=payload.enabled, config=payload.config))
        return {"provider": state}

    @app.get("/v1/storage")
    async def get_storage(job_id: str | None = None, _: None = Depends(require_auth)) -> dict:
        return {"storage": context.manager.storage_summary(job_id)}

    @app.delete("/v1/storage")
    async def delete_storage(job_id: str | None = None, _: None = Depends(require_auth)) -> dict:
        if job_id:
            storage = context.manager.cleanup_job_artifacts(job_id)
        else:
            storage = context.manager.cleanup_temporary_artifacts()
        return {"storage": storage}

    @app.put("/v1/storage/jobs/{job_id}/pin")
    async def update_job_pin(job_id: str, payload: PinUpdate, _: None = Depends(require_auth)) -> dict:
        pinned = context.manager.set_job_pinned(job_id, payload.pinned)
        return {"job_id": job_id, "pinned": pinned}

    return app
