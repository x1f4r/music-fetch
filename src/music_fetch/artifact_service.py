from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

from .config import Settings
from .db import Database
from .models import ArtifactCategory, ArtifactCategorySummary, ArtifactEntry, SourceItem, StorageSummary


class ArtifactService:
    def __init__(self, settings: Settings, db: Database) -> None:
        self.settings = settings
        self.db = db

    def storage_summary(self, job_id: str | None = None) -> StorageSummary:
        entries = self.collect_artifact_entries(job_id)
        self.db.replace_artifact_entries(job_id, entries)
        category_totals: dict[ArtifactCategory, tuple[int, int]] = {}
        for entry in entries:
            count, size = category_totals.get(entry.category, (0, 0))
            category_totals[entry.category] = (count + 1, size + entry.size_bytes)
        categories = [
            ArtifactCategorySummary(category=category, count=count, size_bytes=size)
            for category, (count, size) in sorted(category_totals.items(), key=lambda item: item[0].value)
        ]
        return StorageSummary(
            job_id=job_id,
            auto_clean=not self.settings.retain_artifacts,
            total_size_bytes=sum(entry.size_bytes for entry in entries),
            categories=categories,
            entries=sorted(entries, key=lambda entry: (entry.job_id or "", entry.category.value, entry.label)),
            locations=self.storage_locations(),
        )

    def collect_artifact_entries(self, job_id: str | None = None) -> list[ArtifactEntry]:
        jobs = [self.db.get_job(job_id)] if job_id else self.db.list_jobs(limit=500)
        pinned_jobs = self.db.list_pinned_job_ids()
        entries: list[ArtifactEntry] = []
        seen_paths: set[str] = set()
        for job in jobs:
            if job is None:
                continue
            items = self.db.get_source_items(job.id)
            for item in items:
                for category, label, path, temporary in self.item_artifact_specs(item):
                    normalized = str(path.expanduser())
                    if normalized in seen_paths:
                        continue
                    seen_paths.add(normalized)
                    entry = self.artifact_entry(
                        category=category,
                        label=label,
                        path=path,
                        job_id=job.id,
                        source_item_id=item.id,
                        pinned=job.id in pinned_jobs,
                        temporary=temporary,
                    )
                    if entry:
                        entries.append(entry)
        if job_id is None:
            for entry in self.orphan_recording_entries():
                if entry.path in seen_paths:
                    continue
                seen_paths.add(entry.path)
                entries.append(entry)
            for category, label, path, temporary in [
                (ArtifactCategory.MODEL, "Separationsmodelle", self.settings.cache_dir / "models", False),
                (ArtifactCategory.SUPPORT, "App-Datenbank", self.settings.db_path, False),
                (ArtifactCategory.SUPPORT, "App-Konfiguration", self.settings.config_path, False),
            ]:
                normalized = str(path.expanduser())
                if normalized in seen_paths:
                    continue
                seen_paths.add(normalized)
                entry = self.artifact_entry(category=category, label=label, path=path, temporary=temporary)
                if entry:
                    entries.append(entry)
        return entries

    def cleanup_job_artifacts(self, job_id: str, *, force: bool = True) -> StorageSummary:
        job = self.db.get_job(job_id)
        if not job:
            raise ValueError(f"Unknown job: {job_id}")
        if self.db.is_job_pinned(job_id) and not force:
            return self.storage_summary(job_id)
        entries = self.collect_artifact_entries(job_id)
        self.delete_artifact_entries(entries, skip_pinned=not force)
        self.prune_job_cache_dirs(job_id)
        self.clear_item_artifact_references(job_id)
        self.db.add_event(job_id, "info", "Temporary artifacts removed")
        return self.storage_summary(job_id)

    def cleanup_temporary_artifacts(self) -> StorageSummary:
        for job in self.db.list_jobs(limit=500):
            if self.db.is_job_pinned(job.id):
                continue
            self.cleanup_job_artifacts(job.id, force=False)
        for orphan in self.orphan_recording_entries():
            self.delete_path(Path(orphan.path))
        return self.storage_summary()

    def item_artifact_specs(self, item: SourceItem) -> list[tuple[ArtifactCategory, str, Path, bool]]:
        specs: list[tuple[ArtifactCategory, str, Path, bool]] = []
        source_dir = self.settings.cache_dir / "sources" / item.id
        normalized_dir = self.settings.cache_dir / "normalized" / item.job_id / item.id
        clips_dir = normalized_dir / "clips"
        segment_clips_dir = normalized_dir / "segment-clips"
        stems_dir = normalized_dir / "stems"

        if source_dir.exists():
            specs.append((ArtifactCategory.DOWNLOAD, "Quellcache", source_dir, True))

        if item.input_value:
            input_path = Path(item.input_value).expanduser()
            if self.is_upload_path(input_path):
                specs.append((ArtifactCategory.UPLOAD, "Hochgeladene Datei", input_path, True))
            elif self.is_recording_path(input_path):
                specs.append((ArtifactCategory.RECORDING, "Temporäre Aufnahme", input_path, True))

        if item.local_path:
            local_path = Path(item.local_path).expanduser()
            if self.is_recording_path(local_path):
                specs.append((ArtifactCategory.RECORDING, "Temporäre Aufnahme", local_path, True))
            elif self.is_upload_path(local_path):
                specs.append((ArtifactCategory.UPLOAD, "Hochgeladene Datei", local_path, True))

        if item.normalized_path:
            specs.append((ArtifactCategory.NORMALIZED, "Normalisierte Audiodatei", Path(item.normalized_path), True))
        if stems_dir.exists():
            specs.append((ArtifactCategory.STEM, "Musikstems", stems_dir, True))
        if clips_dir.exists():
            specs.append((ArtifactCategory.EXCERPT, "Probe-Clips", clips_dir, True))
        if segment_clips_dir.exists():
            specs.append((ArtifactCategory.EXCERPT, "Segment-Probes", segment_clips_dir, True))
        return specs

    def orphan_recording_entries(self) -> list[ArtifactEntry]:
        temp_dir = Path(tempfile.gettempdir())
        entries: list[ArtifactEntry] = []
        for pattern, label in [("music-fetch-mic-*.m4a", "Temporäre Mikrofonaufnahme"), ("music-fetch-system-*.m4a", "Temporäre Systemaufnahme")]:
            for path in temp_dir.glob(pattern):
                entry = self.artifact_entry(
                    category=ArtifactCategory.RECORDING,
                    label=label,
                    path=path,
                    temporary=True,
                )
                if entry:
                    entries.append(entry)
        return entries

    def artifact_entry(
        self,
        *,
        category: ArtifactCategory,
        label: str,
        path: Path,
        temporary: bool,
        job_id: str | None = None,
        source_item_id: str | None = None,
        pinned: bool = False,
    ) -> ArtifactEntry | None:
        exists = path.exists()
        if not exists and temporary:
            return None
        resolved = path.expanduser()
        return ArtifactEntry(
            id=f"{job_id or 'global'}:{category.value}:{resolved}",
            category=category,
            label=label,
            path=str(resolved),
            size_bytes=self.path_size(resolved) if exists else 0,
            exists=exists,
            temporary=temporary,
            job_id=job_id,
            source_item_id=source_item_id,
            pinned=pinned,
        )

    def clear_item_artifact_references(self, job_id: str) -> None:
        for item in self.db.get_source_items(job_id):
            local_path = Path(item.local_path).expanduser() if item.local_path else None
            if local_path and (self.is_recording_path(local_path) or self.is_upload_path(local_path) or self.is_path_in_dir(local_path, self.settings.cache_dir)):
                item.local_path = None
            item.normalized_path = None
            item.instrumental_path = None
            self.db.update_source_item(item)

    def delete_artifact_entries(self, entries: list[ArtifactEntry], *, skip_pinned: bool) -> None:
        paths = [
            Path(entry.path)
            for entry in entries
            if entry.temporary and entry.exists and not (skip_pinned and entry.pinned)
        ]
        for path in sorted({path for path in paths}, key=lambda candidate: len(candidate.parts), reverse=True):
            self.delete_path(path)

    def delete_path(self, path: Path) -> None:
        if not path.exists():
            return
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        else:
            try:
                path.unlink()
            except FileNotFoundError:
                return

    def prune_job_cache_dirs(self, job_id: str) -> None:
        for root in [self.settings.cache_dir / "normalized" / job_id, self.settings.cache_dir / "sources"]:
            if not root.exists():
                continue
            if root == self.settings.cache_dir / "sources":
                for child in root.iterdir():
                    if child.is_dir() and not any(child.iterdir()):
                        child.rmdir()
                continue
            for path in sorted([candidate for candidate in root.rglob("*") if candidate.is_dir()], key=lambda candidate: len(candidate.parts), reverse=True):
                if not any(path.iterdir()):
                    path.rmdir()
            if root.exists() and not any(root.iterdir()):
                root.rmdir()

    def path_size(self, path: Path) -> int:
        if not path.exists():
            return 0
        if path.is_file():
            return path.stat().st_size
        return sum(candidate.stat().st_size for candidate in path.rglob("*") if candidate.is_file())

    def is_path_in_dir(self, path: Path, directory: Path) -> bool:
        try:
            path.resolve().relative_to(directory.resolve())
            return True
        except ValueError:
            return False

    def is_recording_path(self, path: Path) -> bool:
        return path.name.startswith("music-fetch-mic-") or path.name.startswith("music-fetch-system-")

    def is_upload_path(self, path: Path) -> bool:
        return self.is_path_in_dir(path, self.settings.cache_dir / "uploads")

    def storage_locations(self) -> dict[str, str]:
        return {
            "cache": str(self.settings.cache_dir),
            "data": str(self.settings.data_dir),
            "config": str(self.settings.config_dir),
            "database": str(self.settings.db_path),
            "temporary_recordings": str(Path(tempfile.gettempdir())),
        }
