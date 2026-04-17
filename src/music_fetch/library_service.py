from __future__ import annotations

from pathlib import Path

from .artifact_service import ArtifactService
from .db import Database
from .models import JobStatus, LibraryEntry, SegmentKind


_TERMINAL_STATUSES = {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELED, JobStatus.PARTIAL_FAILED}


class LibraryQueryService:
    def __init__(self, db: Database, artifact_service: ArtifactService) -> None:
        self.db = db
        self.artifact_service = artifact_service

    def list_library_entries(
        self,
        limit: int = 50,
        *,
        hide_zombies: bool = False,
    ) -> list[LibraryEntry]:
        """Return library entries.

        When ``hide_zombies=True`` we filter out terminal-status, unpinned
        jobs that have no on-disk artifacts — these are "zombie" rows left
        behind by an interrupted cleanup or by a user who cleared storage.
        Default is ``False`` so existing behavior (show everything) is
        preserved; callers that want the clean list opt in.
        """
        jobs = self.db.list_jobs(limit=limit)
        pinned_jobs = self.db.list_pinned_job_ids()
        entries: list[LibraryEntry] = []
        for job in jobs:
            items = self.db.get_source_items(job.id)
            segments = self.db.get_segments(job.id)
            primary_item = items[0] if items else None
            metadata = primary_item.metadata if primary_item else None
            title = (
                (metadata.title if metadata else None)
                or (metadata.playlist_title if metadata else None)
                or (Path(primary_item.input_value).name if primary_item else None)
                or (job.inputs[0] if job.inputs else job.id)
            )
            input_value = primary_item.input_value if primary_item else (job.inputs[0] if job.inputs else job.id)
            summary = self.artifact_service.storage_summary(job.id)
            is_pinned = job.id in pinned_jobs
            is_zombie = (
                hide_zombies
                and not is_pinned
                and job.status in _TERMINAL_STATUSES
                and summary.total_size_bytes == 0
                and len(segments) == 0
            )
            if is_zombie:
                continue
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
                    pinned=is_pinned,
                    artifact_size_bytes=summary.total_size_bytes,
                )
            )
        return entries

    def prune_zombie_entries(self) -> dict:
        """Delete terminal-status unpinned jobs that have no artifacts.

        Returns ``{"removed_job_ids": [...]}``.  This is opt-in cleanup (wired
        to ``POST /v1/library/prune-zombies`` and ``music-fetch library
        prune-zombies``) rather than something we do automatically on every
        read — an in-flight job might look "zombie-y" for a split second
        between creation and first artifact write.
        """
        removed: list[str] = []
        pinned_jobs = self.db.list_pinned_job_ids()
        for job in self.db.list_jobs(limit=10_000):
            if job.id in pinned_jobs:
                continue
            if job.status not in _TERMINAL_STATUSES:
                continue
            summary = self.artifact_service.storage_summary(job.id)
            if summary.total_size_bytes > 0:
                continue
            segments = self.db.get_segments(job.id)
            if segments:
                continue
            if self.db.delete_job(job.id):
                removed.append(job.id)
        # Also sweep orphan cache dirs (the reverse case: files whose job row is gone).
        self.artifact_service.sweep_orphan_cache_dirs()
        return {"removed_job_ids": removed}
