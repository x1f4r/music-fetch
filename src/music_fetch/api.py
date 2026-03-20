from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI, File, Header, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from .context import AppContext
from .models import JobCreate, ProviderConfig, ProviderName


class ProviderUpdate(BaseModel):
    enabled: bool = True
    config: dict = {}


class PinUpdate(BaseModel):
    pinned: bool


def create_api(context: AppContext) -> FastAPI:
    app = FastAPI(title="Music Fetch", version="0.2.2")

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

    @app.get("/v1/jobs/{job_id}")
    async def get_job(job_id: str, _: None = Depends(require_auth)) -> dict:
        job = context.db.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return {"job": job, "items": context.db.get_source_items(job_id)}

    @app.get("/v1/jobs/{job_id}/results")
    async def get_results(job_id: str, _: None = Depends(require_auth)) -> dict:
        job = context.db.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return {"job": job, "segments": context.db.get_segments(job_id)}

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
                if job and job.status in {"succeeded", "partial_failed", "failed"}:
                    break
                await asyncio.sleep(1.0)

        return StreamingResponse(event_stream(), media_type="text/event-stream")

    @app.post("/v1/catalog/import")
    async def import_catalog(paths: list[str], _: None = Depends(require_auth)) -> dict:
        count = context.manager.import_catalog([Path(path).expanduser().resolve() for path in paths])
        return {"imported": count}

    @app.post("/v1/uploads")
    async def upload_file(file: Annotated[UploadFile, File()], _: None = Depends(require_auth)) -> dict:
        upload_dir = context.settings.cache_dir / "uploads"
        upload_dir.mkdir(parents=True, exist_ok=True)
        target = upload_dir / file.filename
        target.write_bytes(await file.read())
        job = context.manager.submit(JobCreate(inputs=[str(target)]))
        return {"job_id": job.id}

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
