from __future__ import annotations

import errno
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from .config import Settings
from .db import Database
from .models import ArtifactCategory, ArtifactCategorySummary, ArtifactEntry, SourceItem, StorageSummary


class ArtifactCleanupError(RuntimeError):
    """Raised when one or more artifact paths could not be removed.

    Carries the list of offending paths so callers can surface them to the UI
    (see ``DELETE /v1/storage`` response ``failed_paths``).
    """

    def __init__(self, failed_paths: list[str], detail: str = "") -> None:
        self.failed_paths = list(failed_paths)
        super().__init__(detail or f"Failed to delete {len(self.failed_paths)} path(s)")


@dataclass
class CleanupReport:
    """Summary of a delete operation. ``failed_paths`` is non-fatal when
    ``non_fatal=True`` (e.g. soft cleanup on shutdown) and raised as
    ``ArtifactCleanupError`` otherwise.
    """

    failed_paths: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failed_paths


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

    def cleanup_job_artifacts(
        self,
        job_id: str,
        *,
        force: bool = True,
        strict: bool = False,
    ) -> StorageSummary:
        """Delete the on-disk artifacts for ``job_id`` and null out DB references
        that pointed at them — without removing the library entry itself.

        Ordering is DB-first → files-second so a crash mid-cleanup leaves the
        database pointing at a clean state; orphan files get reclaimed on the
        next ``cleanup_temporary_artifacts()`` sweep (``sweep_orphan_cache_dirs``).

        When ``strict=True`` (e.g. from explicit user action), partial failures
        raise :class:`ArtifactCleanupError`. When ``strict=False`` (background
        sweeps), failures are swallowed and logged instead.
        """
        job = self.db.get_job(job_id)
        if not job:
            raise ValueError(f"Unknown job: {job_id}")
        if self.db.is_job_pinned(job_id) and not force:
            return self.storage_summary(job_id)
        entries = self.collect_artifact_entries(job_id)
        # Collect paths to delete BEFORE we mutate the DB — clear_* erases the
        # ``normalized_path`` / ``instrumental_path`` references we need to find
        # the files on disk.
        report = CleanupReport()
        # Phase 1: clear DB references. Cheap, reversible, and leaves DB consistent.
        self.clear_item_artifact_references(job_id)
        # Phase 2: delete files. Failures are accumulated, not silently swallowed.
        self.delete_artifact_entries(entries, skip_pinned=not force, report=report)
        self.prune_job_cache_dirs(job_id, report=report)
        self.db.add_event(job_id, "info", "Temporary artifacts removed")
        if report.failed_paths:
            message = f"Cleanup retained {len(report.failed_paths)} path(s): {report.failed_paths[:3]}"
            self.db.add_event(job_id, "warning", message)
            if strict:
                raise ArtifactCleanupError(report.failed_paths, message)
        return self.storage_summary(job_id)

    def delete_job_completely(self, job_id: str, *, strict: bool = True) -> CleanupReport:
        """Remove all artifacts for ``job_id`` AND delete the ``jobs`` row so
        the library entry goes away (T0.2).

        With the v5 schema in place, deleting the parent ``jobs`` row cascades
        through every child table. File deletion happens first; on partial
        failure we still remove the row (the on-disk orphans are reclaimable).
        """
        if not self.db.get_job(job_id):
            return CleanupReport()
        entries = self.collect_artifact_entries(job_id)
        report = CleanupReport()
        # We deliberately skip pinned protection here: callers use this API to
        # _explicitly_ delete a job, and pinned-only-protection should live at
        # the UI/controller layer (confirmation dialog).
        self.delete_artifact_entries(entries, skip_pinned=False, report=report)
        self.prune_job_cache_dirs(job_id, report=report)
        # Schema v5 + PRAGMA foreign_keys=ON cascades the delete through every
        # dependent table.
        self.db.delete_job(job_id)
        if report.failed_paths and strict:
            raise ArtifactCleanupError(report.failed_paths, f"Partial delete of job {job_id}")
        return report

    def cleanup_temporary_artifacts(self) -> StorageSummary:
        for job in self.db.list_jobs(limit=500):
            if self.db.is_job_pinned(job.id):
                continue
            try:
                self.cleanup_job_artifacts(job.id, force=False, strict=False)
            except ValueError:
                continue
        for orphan in self.orphan_recording_entries():
            self.delete_path(Path(orphan.path))
        # Sweep orphan cache directories whose job_id is gone. Guards against
        # the "partial-delete leaves orphan files behind" case.
        self.sweep_orphan_cache_dirs()
        return self.storage_summary()

    def sweep_orphan_cache_dirs(self) -> CleanupReport:
        """Remove ``cache_dir/normalized/<job-id>/`` trees whose job_id is no
        longer in the ``jobs`` table. Called opportunistically by
        :meth:`cleanup_temporary_artifacts` and by the library reconciler.
        """
        report = CleanupReport()
        root = self.settings.cache_dir / "normalized"
        if not root.exists():
            return report
        known_job_ids = {job.id for job in self.db.list_jobs(limit=10_000)}
        for child in root.iterdir():
            if not child.is_dir():
                continue
            if child.name in known_job_ids:
                continue
            self.delete_path(child, report=report)
        return report

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

    def delete_artifact_entries(
        self,
        entries: list[ArtifactEntry],
        *,
        skip_pinned: bool,
        report: CleanupReport | None = None,
    ) -> CleanupReport:
        report = report if report is not None else CleanupReport()
        paths = [
            Path(entry.path)
            for entry in entries
            if entry.temporary and entry.exists and not (skip_pinned and entry.pinned)
        ]
        for path in sorted({path for path in paths}, key=lambda candidate: len(candidate.parts), reverse=True):
            self.delete_path(path, report=report)
        return report

    def delete_path(self, path: Path, *, report: CleanupReport | None = None) -> CleanupReport:
        """Delete ``path`` (file or directory tree).

        Treats ``FileNotFoundError`` as benign (idempotency). Every other error —
        permission denied, busy file, I/O error — is recorded on ``report`` so
        callers can surface or escalate. This replaces the prior
        ``shutil.rmtree(ignore_errors=True)`` silent-failure behavior.
        """
        report = report if report is not None else CleanupReport()
        if not path.exists():
            return report
        if path.is_dir():
            errors: list[tuple[str, OSError]] = []

            def _onerror(func, target, exc_info):  # pragma: no cover - onerror
                exc = exc_info[1] if isinstance(exc_info, tuple) else exc_info
                if isinstance(exc, FileNotFoundError):
                    return
                errors.append((str(target), exc if isinstance(exc, OSError) else OSError(str(exc))))

            # ``onerror`` is the pre-3.12 spelling (``onexc`` is the 3.12+ name).
            # Passing both covers both runtimes; unknown kwargs raise so guard.
            try:
                shutil.rmtree(path, onexc=_onerror)  # type: ignore[arg-type]
            except TypeError:
                shutil.rmtree(path, onerror=_onerror)
            for failed, _exc in errors:
                report.failed_paths.append(failed)
        else:
            try:
                path.unlink()
            except FileNotFoundError:
                return report
            except OSError as exc:  # PermissionError etc.
                if exc.errno == errno.ENOENT:
                    return report
                report.failed_paths.append(str(path))
        return report

    def prune_job_cache_dirs(self, job_id: str, *, report: CleanupReport | None = None) -> CleanupReport:
        report = report if report is not None else CleanupReport()
        for root in [self.settings.cache_dir / "normalized" / job_id, self.settings.cache_dir / "sources"]:
            if not root.exists():
                continue
            if root == self.settings.cache_dir / "sources":
                for child in root.iterdir():
                    if child.is_dir() and not any(child.iterdir()):
                        try:
                            child.rmdir()
                        except OSError:
                            report.failed_paths.append(str(child))
                continue
            for path in sorted([candidate for candidate in root.rglob("*") if candidate.is_dir()], key=lambda candidate: len(candidate.parts), reverse=True):
                if not any(path.iterdir()):
                    try:
                        path.rmdir()
                    except OSError:
                        report.failed_paths.append(str(path))
            if root.exists() and not any(root.iterdir()):
                try:
                    root.rmdir()
                except OSError:
                    report.failed_paths.append(str(root))
        return report

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
