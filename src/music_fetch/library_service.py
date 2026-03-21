from __future__ import annotations

from pathlib import Path

from .artifact_service import ArtifactService
from .db import Database
from .models import LibraryEntry, SegmentKind


class LibraryQueryService:
    def __init__(self, db: Database, artifact_service: ArtifactService) -> None:
        self.db = db
        self.artifact_service = artifact_service

    def list_library_entries(self, limit: int = 50) -> list[LibraryEntry]:
        jobs = self.db.list_jobs(limit=limit)
        pinned_jobs = self.db.list_pinned_job_ids()
        entries: list[LibraryEntry] = []
        for job in jobs:
            items = self.db.get_source_items(job.id)
            segments = self.db.get_segments(job.id)
            primary_item = items[0] if items else None
            title = (
                primary_item.metadata.title
                or primary_item.metadata.playlist_title
                or (Path(primary_item.input_value).name if primary_item else None)
                or (job.inputs[0] if job.inputs else job.id)
            )
            input_value = primary_item.input_value if primary_item else (job.inputs[0] if job.inputs else job.id)
            summary = self.artifact_service.storage_summary(job.id)
            entries.append(
                LibraryEntry(
                    job_id=job.id,
                    title=title,
                    input_value=input_value,
                    status=job.status,
                    created_at=job.created_at,
                    updated_at=job.updated_at,
                    item_count=len(items),
                    segment_count=len(segments),
                    matched_count=sum(1 for segment in segments if segment.kind == SegmentKind.MATCHED_TRACK),
                    pinned=job.id in pinned_jobs,
                    artifact_size_bytes=summary.total_size_bytes,
                )
            )
        return entries
