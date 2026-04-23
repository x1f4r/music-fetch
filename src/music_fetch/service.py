from __future__ import annotations

import csv
import io
import json
import shutil
import threading
import tempfile
import time
import uuid
from collections import Counter
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path

from .artifact_service import ArtifactCleanupError, ArtifactService
from .config import Settings
from .db import Database
from .fusion import fuse_candidates
from .library_service import LibraryQueryService
from .links import build_search_links
from .long_mix import SegmentDraft, analyze_long_mix
from .media import (
    build_excerpt_path,
    classify_source,
    clustered_long_mix_windows,
    create_excerpt,
    ensure_local_media,
    fingerprint_cache_key,
    isolate_music,
    MediaToolError,
    metadata_windows,
    normalize_media,
    plan_windows_for_profile,
    probe_duration_ms,
    SourceProfile,
)
from .models import (
    AnalysisMode,
    ArtifactCategory,
    ArtifactCategorySummary,
    ArtifactEntry,
    DetectedSegment,
    DiscoveryState,
    ItemStatus,
    Job,
    JobCreate,
    JobOptions,
    JobStatus,
    LibraryEntry,
    ProviderConfig,
    ProviderName,
    RecognitionMetric,
    ProviderState,
    SegmentKind,
    SourceItem,
    SourceKind,
    StorageSummary,
    TrackCandidate,
    TrackMatch,
    WindowPlan,
)
from .provider_registry import ProviderRegistry
from .providers import ACRCloudProvider, AudDProvider, LocalCatalogProvider, VibraProvider
from .providers.base import BaseProvider, ProviderError
from .sources import SourceResolver
from .utils import cancel_job_processes, command_job_context, now_iso


class JobBusyError(RuntimeError):
    """Raised when an operation refuses to act on a queued/running job.

    Handled by the API layer as HTTP 409 Conflict. Keeps the worker thread
    from having its DB row ripped out mid-process.
    """


class _BudgetCounter:
    """Thread-safe integer budget shared across parallel segment workers.

    ``try_spend(1)`` atomically checks-and-decrements; returns True when the
    caller is authorized to make a provider call. ``try_spend(0)`` is a
    non-consuming check for "is there any budget left?" — useful when a
    worker wants to bail early without burning a slot.
    """

    def __init__(self, initial: int) -> None:
        self._lock = threading.Lock()
        self._remaining = max(0, initial)

    @property
    def remaining(self) -> int:
        with self._lock:
            return self._remaining

    def try_spend(self, cost: int) -> bool:
        with self._lock:
            if self._remaining < cost:
                return False
            self._remaining -= cost
            return True


class JobManager:
    def __init__(self, settings: Settings, db: Database) -> None:
        self.settings = settings
        self.db = db
        self.source_resolver = SourceResolver(settings.cache_dir)
        self.provider_registry = ProviderRegistry(settings, db)
        self.artifact_service = ArtifactService(settings, db)
        self.library_service = LibraryQueryService(db, self.artifact_service)
        self.executor = ThreadPoolExecutor(max_workers=settings.max_workers, thread_name_prefix="music-fetch")
        self._futures: dict[str, Future[None]] = {}
        self._lock = threading.Lock()
        self._provider_call_lock = threading.Lock()
        self._provider_next_call_at: dict[ProviderName, float] = {}

    def submit(self, payload: JobCreate) -> Job:
        job = self.db.create_job(payload.inputs, payload.options)
        self.db.add_event(job.id, "info", "Job created")
        future = self.executor.submit(self._run_job, job.id)
        with self._lock:
            self._futures[job.id] = future
        return job

    def create_job(self, payload: JobCreate) -> Job:
        job = self.db.create_job(payload.inputs, payload.options)
        self.db.add_event(job.id, "info", "Job created")
        return job

    def run_existing_job(self, job_id: str) -> None:
        self._run_job(job_id)

    def submit_payload(self, inputs: list[str]) -> Job:
        return self.submit(JobCreate(inputs=inputs))

    def wait(self, job_id: str, poll_interval: float = 0.5) -> Job:
        while True:
            job = self.db.get_job(job_id)
            if job and job.status in {JobStatus.SUCCEEDED, JobStatus.PARTIAL_FAILED, JobStatus.FAILED, JobStatus.CANCELED}:
                return job
            time.sleep(poll_interval)

    def provider_states(self) -> list[ProviderState]:
        return self.provider_registry.provider_states()

    def set_provider_config(self, name: ProviderName, config: ProviderConfig) -> ProviderState:
        return self.provider_registry.set_provider_config(name, config)

    def import_catalog(self, paths: list[Path]) -> int:
        provider = LocalCatalogProvider(self.settings, self.db)
        return provider.import_paths(paths)

    def list_library_entries(self, limit: int = 50) -> list[LibraryEntry]:
        return self.library_service.list_library_entries(limit=limit)

    def storage_summary(self, job_id: str | None = None) -> StorageSummary:
        return self.artifact_service.storage_summary(job_id)

    def system_resources(self) -> dict:
        from .config import detect_system_resources

        cpu, ram_gb = detect_system_resources()
        with self._lock:
            active = sum(1 for future in self._futures.values() if not future.done())
        return {
            "cpu_count": cpu,
            "ram_gb": round(ram_gb, 2) if ram_gb else 0.0,
            "max_workers": self.settings.max_workers,
            "active_jobs": active,
        }

    def set_job_pinned(self, job_id: str, pinned: bool) -> bool:
        if not self.db.get_job(job_id):
            raise ValueError(f"Unknown job: {job_id}")
        self.db.set_job_pinned(job_id, pinned)
        return pinned

    def cleanup_job_artifacts(self, job_id: str, *, force: bool = True, strict: bool = False) -> StorageSummary:
        return self.artifact_service.cleanup_job_artifacts(job_id, force=force, strict=strict)

    def cleanup_temporary_artifacts(self) -> StorageSummary:
        return self.artifact_service.cleanup_temporary_artifacts()

    def delete_job(self, job_id: str) -> dict:
        """Delete a job entirely: files + library row (T0.2).

        Active jobs are force-canceled first. Canceling a ``Future`` is not
        enough once a worker has started, so this also terminates subprocesses
        registered to the job before removing artifacts and rows.
        """
        job = self.db.get_job(job_id)
        if job is None:
            raise ValueError(f"Unknown job: {job_id}")
        was_active = job.status in {JobStatus.QUEUED, JobStatus.RUNNING}
        if was_active:
            self.cancel(job_id)
        report = self.artifact_service.delete_job_completely(job_id, strict=False)
        with self._lock:
            self._futures.pop(job_id, None)
        return {
            "job_id": job_id,
            "deleted": True,
            "canceled": was_active,
            "failed_paths": list(report.failed_paths),
        }

    def delete_jobs(self, *, job_ids: list[str] | None = None, include_pinned: bool = False) -> dict:
        pinned = self.db.list_pinned_job_ids()
        jobs = self.db.list_jobs(limit=10_000) if job_ids is None else [
            job for job_id in job_ids if (job := self.db.get_job(job_id)) is not None
        ]
        deleted: list[str] = []
        canceled: list[str] = []
        skipped_pinned: list[str] = []
        failed_paths: list[str] = []
        for job in jobs:
            if job.id in pinned and not include_pinned:
                skipped_pinned.append(job.id)
                continue
            result = self.delete_job(job.id)
            deleted.append(job.id)
            if result.get("canceled"):
                canceled.append(job.id)
            failed_paths.extend(result.get("failed_paths") or [])
        return {
            "deleted_job_ids": deleted,
            "canceled_job_ids": canceled,
            "skipped_pinned_job_ids": skipped_pinned,
            "failed_paths": failed_paths,
        }

    def prune_zombie_library_entries(self) -> dict:
        """Remove library entries whose artifacts are gone (T0.5).

        A "zombie" is a terminal-status job row (``SUCCEEDED`` / ``FAILED`` /
        ``CANCELED``) that is unpinned and has no artifacts on disk. Returns a
        summary dict with ``removed_job_ids``.
        """
        return self.library_service.prune_zombie_entries()

    def cancel(self, job_id: str) -> None:
        if not self.db.get_job(job_id):
            raise ValueError(f"Unknown job: {job_id}")
        self.db.request_job_cancel(job_id)
        self.db.update_job(job_id, status=JobStatus.CANCELED)
        self.db.add_event(job_id, "warning", "Cancellation requested")
        killed = cancel_job_processes(job_id)
        if killed:
            self.db.add_event(job_id, "warning", f"Stopped {killed} running subprocess(es)")
        with self._lock:
            future = self._futures.get(job_id)
        if future is not None:
            future.cancel()

    def _is_canceled(self, job_id: str) -> bool:
        job = self.db.get_job(job_id)
        if job is None:
            return True
        return job.cancel_requested or job.status == JobStatus.CANCELED

    def _raise_if_canceled(self, job_id: str) -> None:
        if self._is_canceled(job_id):
            raise RuntimeError("__CANCELLED__")

    def _mark_canceled(self, job_id: str, message: str = "Job canceled") -> None:
        if self.db.get_job(job_id) is None:
            return
        self.db.update_job(job_id, status=JobStatus.CANCELED)
        self.db.add_event(job_id, "warning", message)

    def _providers(self) -> list[BaseProvider]:
        return self.provider_registry.active_providers()

    def _run_job(self, job_id: str) -> None:
        with command_job_context(job_id):
            job = self.db.get_job(job_id)
            if not job:
                return
            if self._is_canceled(job_id):
                self._mark_canceled(job_id, "Job canceled before execution")
                return
            self.db.update_job(job_id, status=JobStatus.RUNNING)
            self.db.add_event(job_id, "info", "Resolving inputs")
            try:
                self._run_job_inner(job_id, job)
            except Exception as exc:
                if str(exc) == "__CANCELLED__" or self._is_canceled(job_id):
                    self._mark_canceled(job_id)
                    return
                if self.db.get_job(job_id) is None:
                    return
                self.db.update_job(job_id, status=JobStatus.FAILED, error=str(exc))
                self.db.add_event(job_id, "error", f"Job failed: {exc}")

    def _run_job_inner(self, job_id: str, job: Job) -> None:
        items: list[SourceItem] = []
        failures = 0
        failure_messages: list[str] = []
        discovered_per_input = {raw: 0 for raw in job.inputs}
        for raw in job.inputs:
            self.db.upsert_discovery_state(
                DiscoveryState(
                    job_id=job_id,
                    input_value=raw,
                    cursor=0,
                    total=None,
                    completed=False,
                    payload={},
                    updated_at=job.updated_at,
                )
            )
        worker_count = max(1, min(self.settings.max_workers, max(1, len(job.inputs))))
        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="music-fetch-item") as item_executor:
            future_map: dict[Future[None], SourceItem] = {}
            for item in self.source_resolver.iter_resolve_inputs(job_id, job.inputs):
                self._raise_if_canceled(job_id)
                items.append(item)
                discovered_per_input[item.input_value] = discovered_per_input.get(item.input_value, 0) + 1
                self.db.add_source_items([item])
                self.db.upsert_discovery_state(
                    DiscoveryState(
                        job_id=job_id,
                        input_value=item.input_value,
                        cursor=discovered_per_input[item.input_value],
                        total=None,
                        completed=False,
                        payload={"latest_item_id": item.id},
                        updated_at=now_iso(),
                    )
                )
                future_map[item_executor.submit(self._process_item, job, item)] = item
            for raw, count in discovered_per_input.items():
                self.db.upsert_discovery_state(
                    DiscoveryState(
                        job_id=job_id,
                        input_value=raw,
                        cursor=count,
                        total=count,
                        completed=True,
                        payload={},
                        updated_at=now_iso(),
                    )
                )
            for future in as_completed(future_map):
                item = future_map[future]
                try:
                    future.result()
                except Exception as exc:
                    if str(exc) == "__CANCELLED__" or self._is_canceled(job_id):
                        item.status = ItemStatus.CANCELED
                        self.db.update_source_item(item)
                        continue
                    failures += 1
                    item.status = ItemStatus.FAILED
                    item.error = str(exc)
                    failure_messages.append(f"{item.input_value}: {exc}")
                    self.db.update_source_item(item)
                    self.db.add_event(job_id, "error", f"{item.input_value}: {exc}")
        if self._is_canceled(job_id):
            self._mark_canceled(job_id)
            return
        if failures == 0:
            status = JobStatus.SUCCEEDED
        elif failures < len(items):
            status = JobStatus.PARTIAL_FAILED
        else:
            status = JobStatus.FAILED
        error_summary = None
        if failure_messages:
            error_summary = failure_messages[0] if len(failure_messages) == 1 else "\n".join(failure_messages[:3])
        if self._is_canceled(job_id):
            self._mark_canceled(job_id)
            return
        self.db.update_job(job_id, status=status, error=error_summary)
        if status in {JobStatus.SUCCEEDED, JobStatus.PARTIAL_FAILED} and not self.settings.retain_artifacts and not self.db.is_job_pinned(job_id):
            try:
                self.cleanup_job_artifacts(job_id, force=False)
            except Exception as exc:
                self.db.add_event(job_id, "warning", f"Artifact cleanup failed: {exc}")
        self.db.add_event(job_id, "info", f"Job finished with status {status}")

    def _process_item(self, job: Job, item: SourceItem) -> None:
        with command_job_context(job.id):
            self._process_item_inner(job, item)

    def _process_item_inner(self, job: Job, item: SourceItem) -> None:
        if self._is_canceled(job.id):
            item.status = ItemStatus.CANCELED
            self.db.update_source_item(item)
            raise RuntimeError("__CANCELLED__")
        item.status = ItemStatus.RUNNING
        self.db.update_source_item(item)
        self.db.add_event(job.id, "info", f"Preparing item {item.metadata.title or item.input_value}")
        if self._has_metadata_only_track(item):
            self.db.add_event(job.id, "info", f"Using metadata-only playlist fallback for {item.metadata.title or item.input_value}")
            self.db.replace_segments(job.id, item.id, self._metadata_only_segments(item))
            item.status = ItemStatus.SUCCEEDED
            self.db.update_source_item(item)
            return
        try:
            if item.kind == SourceKind.LOCAL_FILE and not item.local_path:
                candidate = Path(item.input_value).expanduser().resolve()
                if not candidate.exists():
                    raise MediaToolError(f"Input file does not exist: {candidate}")
                local_media = candidate
            else:
                local_media = ensure_local_media(self.settings, item)
            self._raise_if_canceled(job.id)
        except MediaToolError:
            if self._has_metadata_only_track(item):
                self.db.add_event(job.id, "warning", f"Media unavailable, falling back to metadata for {item.metadata.title or item.input_value}")
                self.db.replace_segments(job.id, item.id, self._metadata_only_segments(item))
                item.status = ItemStatus.SUCCEEDED
                self.db.update_source_item(item)
                return
            raise
        item.local_path = str(local_media)

        normalized_dir = self.settings.cache_dir / "normalized" / job.id / item.id
        normalized = normalize_media(local_media, normalized_dir / "normalized.wav")
        self._raise_if_canceled(job.id)
        item.normalized_path = str(normalized)
        if not item.metadata.duration_ms:
            item.metadata.duration_ms = probe_duration_ms(normalized)
            self._raise_if_canceled(job.id)

        profile = classify_source(
            item.metadata.duration_ms or 0,
            has_playlist_context=item.metadata.playlist_id is not None,
            metadata=item.metadata,
        )
        if (
            job.options.analysis_mode is AnalysisMode.AUTO
            and self._is_recording_source(item)
            and (item.metadata.duration_ms or 0) <= 90_000
        ):
            # Quick mic/system captures are usually "what song is playing right now?"
            # For these, the segmented multi-track path is too conservative.
            profile.strategy = "single_track"
        if job.options.analysis_mode is AnalysisMode.LONG_MIX:
            profile.strategy = "long_mix"
        elif job.options.analysis_mode is AnalysisMode.SINGLE_TRACK:
            profile.strategy = "single_track"
        elif job.options.analysis_mode is AnalysisMode.PLAYLIST_ENTRY and profile.strategy != "long_mix":
            profile.strategy = "multi_track"
        self.db.add_event(job.id, "info", f"Using {profile.strategy} strategy for {item.id}")

        instrumental = None
        if job.options.prefer_separation and profile.use_source_separation:
            self.db.add_event(job.id, "info", f"Separating music stem for {item.id}")
            instrumental = isolate_music(self.settings, normalized, normalized_dir / "stems")
            self._raise_if_canceled(job.id)
            item.instrumental_path = str(instrumental)
        self.db.update_source_item(item)

        if profile.strategy in {"long_mix", "multi_track"}:
            segments = self._process_long_mix_item(job, item, normalized, instrumental)
            self._raise_if_canceled(job.id)
            self.db.replace_segments(job.id, item.id, segments)
            self._record_item_summary_metric(job.id, item.id, segments)
            item.status = ItemStatus.SUCCEEDED
            self.db.update_source_item(item)
            return

        plans = self._select_windows(job, item, normalized, instrumental, profile)
        providers = self._providers()
        candidates: list[TrackCandidate] = []
        excerpts_dir = normalized_dir / "clips"
        remaining_budget = profile.request_budget
        for plan in plans:
            if self._is_canceled(job.id):
                item.status = ItemStatus.CANCELED
                self.db.update_source_item(item)
                raise RuntimeError("__CANCELLED__")
            if remaining_budget <= 0:
                self.db.add_event(job.id, "info", f"Request budget exhausted for {item.id}")
                break
            excerpt_path = build_excerpt_path(excerpts_dir, Path(plan.source_path), plan.start_ms, plan.end_ms, plan.label)
            if not excerpt_path.exists():
                create_excerpt(Path(plan.source_path), plan.start_ms, plan.end_ms, excerpt_path)
                self._raise_if_canceled(job.id)
            for provider in providers:
                if remaining_budget <= 0:
                    break
                state = provider.state()
                if not state.available:
                    continue
                provider_hits = self._recognize_with_cache(job.id, item, provider, excerpt_path, plan.start_ms, plan.end_ms)
                self._raise_if_canceled(job.id)
                remaining_budget -= 1
                if provider_hits:
                    self.db.add_event(job.id, "info", f"{provider.name} matched {provider_hits[0].track.title}")
                candidates.extend(provider_hits)
            if self._should_stop_early(profile, candidates):
                self.db.add_event(job.id, "info", f"Early stop reached for {item.id}")
                break

        segments = fuse_candidates(
            item.id,
            candidates,
            max_gap_ms=job.options.merge_gap_same_track_ms,
        )
        # Even on the single-track path the stitch pass is useful: provider
        # title/artist variance can yield two same-identity segments that only
        # merge once tiered-identity does its thing.
        segments = self._stitch_segment_timeline(segments, options=job.options)
        self._raise_if_canceled(job.id)
        self.db.replace_segments(job.id, item.id, segments)
        self._record_item_summary_metric(job.id, item.id, segments)
        item.status = ItemStatus.SUCCEEDED
        self.db.update_source_item(item)

    def _has_metadata_only_track(self, item: SourceItem) -> bool:
        extra = item.metadata.extra
        title = str(extra.get("track_title") or item.metadata.title or "").strip()
        artist = str(extra.get("track_artist") or "").strip()
        return bool(extra.get("metadata_only") and title and (artist or item.metadata.playlist_id))

    def _metadata_only_segments(self, item: SourceItem) -> list[DetectedSegment]:
        extra = item.metadata.extra
        title = str(extra.get("track_title") or item.metadata.title or item.input_value).strip()
        artist = str(extra.get("track_artist") or "").strip() or None
        album = str(extra.get("track_album") or "").strip() or None
        duration_ms = item.metadata.duration_ms or 30_000
        match = TrackMatch(
            title=title,
            artist=artist,
            album=album,
            external_links=build_search_links(title, artist),
            raw={"source": "metadata_only", "input_value": item.input_value},
        )
        return [
            DetectedSegment(
                source_item_id=item.id,
                start_ms=0,
                end_ms=duration_ms,
                kind=SegmentKind.MATCHED_TRACK,
                confidence=0.52,
                providers=[],
                evidence_count=1,
                track=match,
                metadata_hints=[item.metadata.playlist_title] if item.metadata.playlist_title else [],
                explanation=[
                    "Recovered from playlist metadata without full media analysis.",
                    "Search links were generated from the playlist entry metadata.",
                ],
            )
        ]

    def _process_long_mix_item(self, job: Job, item: SourceItem, normalized: Path, instrumental: Path | None = None) -> list[DetectedSegment]:
        """Scan a long mix segment-by-segment using a coverage-first allocator
        and (optionally) a parallel worker pool.

        Pipeline changes vs the pre-overhaul code:

        - **T1.4**: skip-predicate change — SPEECH_ONLY segments with some
          music content (``music_ratio >= 0.15``) still get probed.
        - **T1.5**: segments run through a ``ThreadPoolExecutor``. Per-provider
          throttling is already thread-safe via ``_throttle_provider``.
        - **T1.6**: budget scales to the number of probe-able segments when
          only free providers are configured; paid providers still honor the
          ``JobOptions.max_provider_calls`` ceiling.
        - **T1.7**: a single retry pass runs over any segment that came back
          ``MUSIC_UNRESOLVED``, reusing ``_retry_segment``.
        - **T2.2**: repeat-group reuse now re-confirms with one cheap free
          probe before adopting the group's match.
        """
        providers = self._providers()
        analysis = analyze_long_mix(normalized, item.metadata, job.options)
        excerpts_dir = normalized.parent / "segment-clips"
        excerpt_source = instrumental or normalized
        probeable: list[SegmentDraft] = []
        probeable_ids: set[int] = set()
        draft_order: dict[int, SegmentDraft] = {}
        for index, draft in enumerate(analysis.segments):
            draft_order[index] = draft
            if self._should_skip_draft(draft):
                # Not probeable — emit as-is.
                continue
            probeable.append(draft)
            probeable_ids.add(id(draft))

        budget = _BudgetCounter(self._effective_budget(job.options, providers, probeable, item))
        repeat_matches: dict[str, TrackCandidate] = {}
        repeat_matches_lock = threading.Lock()
        per_draft_result: dict[int, DetectedSegment] = {}
        repeat_stats = {"reconfirmed": 0, "rejected": 0}

        progress_counter = [0]
        progress_lock = threading.Lock()
        total_segments = len(analysis.segments)

        def process_one(index: int, draft: SegmentDraft) -> DetectedSegment:
            with command_job_context(job.id):
                return process_one_inner(index, draft)

        def process_one_inner(index: int, draft: SegmentDraft) -> DetectedSegment:
            self._raise_if_canceled(job.id)
            candidates: list[TrackCandidate] = []
            provider_attempts = 0
            probe_count = 0
            # --- T2.2: re-confirm reuse before adopting a group's match ---
            if draft.repeat_group_id:
                with repeat_matches_lock:
                    reusable = repeat_matches.get(draft.repeat_group_id)
                if reusable is not None:
                    confirmed = self._reconfirm_reused_match(
                        job,
                        item,
                        draft,
                        reusable,
                        excerpt_source,
                        excerpts_dir,
                        budget,
                        providers,
                    )
                    if confirmed is True:
                        repeat_stats["reconfirmed"] += 1
                        draft.probe_count = 1
                        draft.provider_attempts = 1
                        return self._candidate_to_detected(item.id, draft, reusable, reused=True)
                    if confirmed is False:
                        repeat_stats["rejected"] += 1
                    # confirmed is None → no free provider to verify. Fall through
                    # to full probing so we don't propagate a possibly-wrong match.

            for probe in draft.probe_windows[: job.options.max_probes_per_segment]:
                spend = budget.try_spend(1)
                if not spend:
                    break
                probe_count += 1
                excerpt_path = build_excerpt_path(
                    excerpts_dir,
                    excerpt_source,
                    probe.start_ms,
                    probe.end_ms,
                    f"segment-{probe.reason}",
                )
                if not excerpt_path.exists():
                    create_excerpt(excerpt_source, probe.start_ms, probe.end_ms, excerpt_path)
                    self._raise_if_canceled(job.id)
                for provider in providers:
                    self._raise_if_canceled(job.id)
                    state = provider.state()
                    if not state.available:
                        continue
                    # Prefer-free-providers: once a free provider has produced a
                    # strong match for this probe, skip paid providers for the
                    # rest of the probe loop.
                    if (
                        job.options.prefer_free_providers
                        and self._is_paid_provider(provider)
                        and self._candidates_have_strong_free_hit(candidates)
                    ):
                        continue
                    if not budget.try_spend(0):
                        break
                    provider_hits = self._recognize_with_cache(
                        job.id, item, provider, excerpt_path, probe.start_ms, probe.end_ms
                    )
                    self._raise_if_canceled(job.id)
                    provider_attempts += 1
                    if provider_hits:
                        self.db.add_event(
                            job.id,
                            "info",
                            f"{provider.name} matched {provider_hits[0].track.title} for segment {draft.start_ms}-{draft.end_ms}",
                        )
                    candidates.extend(provider_hits)
                if self._probes_have_strong_match(candidates):
                    break
            draft.probe_count = probe_count
            draft.provider_attempts = provider_attempts
            draft.candidates = candidates
            best = self._pick_segment_candidate(draft)
            with progress_lock:
                progress_counter[0] += 1
                if progress_counter[0] % 12 == 0:
                    self.db.add_event(
                        job.id,
                        "info",
                        f"Processed {progress_counter[0]}/{total_segments} segmented regions for {item.id}",
                    )
            if best:
                if draft.repeat_group_id:
                    with repeat_matches_lock:
                        repeat_matches.setdefault(draft.repeat_group_id, best)
                return self._candidate_to_detected(item.id, draft, best, reused=False)
            unresolved_kind = (
                SegmentKind.SPEECH_ONLY
                if draft.speech_ratio >= 0.70 and draft.music_ratio < 0.35
                else SegmentKind.MUSIC_UNRESOLVED
            )
            draft.kind = unresolved_kind
            return self._draft_to_detected(item.id, draft)

        # --- T1.5: parallel segment loop ---
        workers = self._segment_worker_count(job.options, probeable)
        # Map SegmentDraft identity -> its index in analysis order so the
        # parallel assembly can reconstruct timeline order. Identity-by-id is
        # safe here (drafts are only referenced by this call) and avoids the
        # numpy-array __eq__ ambiguity that plain ``in`` would trigger.
        order_index = {id(draft): index for index, draft in draft_order.items() if id(draft) in probeable_ids}
        if workers <= 1 or len(probeable) <= 1:
            for draft in probeable:
                self._raise_if_canceled(job.id)
                idx = order_index[id(draft)]
                per_draft_result[idx] = process_one(idx, draft)
        else:
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="music-fetch-seg") as pool:
                futures = {pool.submit(process_one, order_index[id(draft)], draft): draft for draft in probeable}
                for future in as_completed(futures):
                    draft = futures[future]
                    idx = order_index[id(draft)]
                    try:
                        per_draft_result[idx] = future.result()
                    except Exception:
                        raise

        # Assemble per-draft results preserving timeline order; non-probeable
        # drafts were skipped above and get emitted here as speech/silence.
        assembled: list[DetectedSegment] = []
        for index, draft in sorted(draft_order.items()):
            if index in per_draft_result:
                assembled.append(per_draft_result[index])
                continue
            assembled.append(self._draft_to_detected(item.id, draft))

        # --- T1.7: retry unresolved segments once if budget permits ---
        if job.options.auto_retry_unresolved:
            unresolved_indices = [
                index
                for index, segment in enumerate(assembled)
                if segment.kind == SegmentKind.MUSIC_UNRESOLVED and budget.remaining > 0
            ]
            for index in unresolved_indices:
                if not budget.try_spend(0):
                    break
                if self._is_canceled(job.id):
                    break
                retried = self._retry_segment(
                    job,
                    item,
                    assembled[index],
                    excerpt_source,
                    providers,
                    job.options,
                )
                assembled[index] = retried

        self._last_segment_counters = {
            "repeat_group_reconfirmed": repeat_stats["reconfirmed"],
            "repeat_group_rejected": repeat_stats["rejected"],
        }
        return self._stitch_segment_timeline(assembled, options=job.options)

    # --- Helpers introduced by the long-mix rewrite ---

    def _should_skip_draft(self, draft: SegmentDraft) -> bool:
        """Skip non-probeable drafts (T1.4).

        SILENCE_OR_FX is always skipped. SPEECH_ONLY is skipped ONLY when
        there is essentially no music content; a podcast with a bed of music
        is still worth a probe.
        """
        if draft.kind == SegmentKind.SILENCE_OR_FX:
            return True
        if draft.kind == SegmentKind.SPEECH_ONLY and draft.music_ratio < 0.15:
            return True
        return False

    def _effective_budget(
        self,
        options: JobOptions,
        providers: list[BaseProvider],
        probeable: list[SegmentDraft],
        item: SourceItem,
    ) -> int:
        """Compute an allowance for provider calls (T1.6).

        Rules:
        - Free-only mode: call count uncapped (so long mixes get full coverage
          when there's no financial cost).
        - Paid mode: ``max(max_provider_calls, segments * 1.3)`` so mixes
          with many segments still get their minimum-one-probe coverage even
          when the hand-configured ceiling is lower.
        - When budget_autoscale is disabled, the old fixed-cap behavior is
          preserved.
        """
        base = options.max_provider_calls
        if not options.budget_autoscale:
            return base if (item.metadata.duration_ms or 0) >= 25 * 60_000 else min(96, base)
        free_only = self._only_free_providers(providers)
        if free_only:
            # Uncap: 2 × probes × segments is a safe ceiling that still prevents
            # an infinite loop if something goes wrong.
            return max(base, 2 * options.max_probes_per_segment * max(1, len(probeable)))
        # Paid providers configured — keep the user-specified ceiling but grow
        # it enough to guarantee coverage.
        return max(base, int(1.3 * len(probeable)) + 60)

    def _only_free_providers(self, providers: list[BaseProvider]) -> bool:
        """True when the available provider set contains no paid API."""
        for provider in providers:
            if not provider.state().available:
                continue
            if self._is_paid_provider(provider):
                return False
        return True

    def _is_paid_provider(self, provider: BaseProvider) -> bool:
        return provider.name in {ProviderName.AUDD, ProviderName.ACRCLOUD}

    def _candidates_have_strong_free_hit(self, candidates: list[TrackCandidate]) -> bool:
        for candidate in candidates:
            if candidate.provider in {ProviderName.LOCAL_CATALOG, ProviderName.VIBRA}:
                if candidate.confidence >= 0.80:
                    return True
        return False

    def _segment_worker_count(self, options: JobOptions, probeable: list[SegmentDraft]) -> int:
        if options.segment_workers and options.segment_workers > 0:
            return max(1, options.segment_workers)
        base = max(2, (self.settings.max_workers or 2) * 2)
        # Don't spin up more workers than segments to probe, with a hard cap
        # because most providers rate-limit aggressively.
        return max(1, min(base, 6, max(1, len(probeable))))

    def _reconfirm_reused_match(
        self,
        job: Job,
        item: SourceItem,
        draft: SegmentDraft,
        reusable: TrackCandidate,
        excerpt_source: Path,
        excerpts_dir: Path,
        budget: "_BudgetCounter",
        providers: list[BaseProvider],
    ) -> bool | None:
        """Verify a cached ``repeat_group`` match with one cheap free probe.

        Returns:
            - ``True``: a free provider returned an identity that merges with
              the existing group match — adopt it.
            - ``False``: a free provider returned a different identity or no
              hit at all — force full probing so the group doesn't propagate a
              wrong guess.
            - ``None``: no free provider is available — caller decides.
        """
        if not draft.probe_windows:
            return None
        free_providers = [p for p in providers if not self._is_paid_provider(p) and p.state().available]
        if not free_providers:
            return None
        if not budget.try_spend(1):
            return None
        probe = draft.probe_windows[len(draft.probe_windows) // 2]
        excerpt_path = build_excerpt_path(
            excerpts_dir, excerpt_source, probe.start_ms, probe.end_ms, "reconfirm"
        )
        if not excerpt_path.exists():
            try:
                create_excerpt(excerpt_source, probe.start_ms, probe.end_ms, excerpt_path)
            except MediaToolError:
                return None
        for provider in free_providers:
            hits = self._recognize_with_cache(
                job.id, item, provider, excerpt_path, probe.start_ms, probe.end_ms
            )
            if not hits:
                continue
            # If any hit matches the group's track by tiered identity, accept.
            if any(hit.track.merges_with(reusable.track) for hit in hits):
                return True
            # A confident, DIFFERENT match means the first group member was
            # wrong; fall through to full probing.
            return False
        return None

    def _pick_segment_candidate(self, draft: SegmentDraft) -> TrackCandidate | None:
        """Layered acceptance gate (T1.3).

        Instead of the previous single compound AND-gate
        (``score >= 0.68 AND music_ratio >= 0.45 AND kind != SPEECH_ONLY``)
        we accept a candidate under the FIRST of these that matches:

        - G1: 2+ distinct providers agree on the top identity.
        - G2: 2+ probes from the same provider agree.
        - G3: single hit with score >= 0.72 — rescues podcasts-with-music where
          ``music_ratio`` is unfairly low.
        - G4: single hit with score >= 0.60 AND music_ratio >= 0.35.
        - G5: single hit with ISRC present and score >= 0.55 — ISRC provenance
          is stronger evidence than raw confidence.

        The winning gate label is stashed on the candidate's ``evidence`` list
        (a string like ``"gate:G3"``) so ``_candidate_to_detected`` can surface
        it on the resulting ``DetectedSegment.acceptance_gate``.
        """
        if not draft.candidates:
            return None
        scores: dict[str, float] = {}
        by_key: dict[str, list[TrackCandidate]] = {}
        for candidate in draft.candidates:
            key = candidate.track.normalized_key()
            by_key.setdefault(key, []).append(candidate)
            scores[key] = scores.get(key, 0.0) + self._candidate_score(candidate, draft)
        top_key = max(scores, key=scores.get)
        top_candidates = by_key[top_key]
        ranked = sorted(
            top_candidates,
            key=lambda candidate: (self._candidate_score(candidate, draft), candidate.confidence),
            reverse=True,
        )
        best = ranked[0]

        gate = self._determine_acceptance_gate(best, top_candidates, draft)
        if gate is None:
            return None
        # Tag the candidate with the gate that accepted it. Using a tuple
        # sentinel in ``evidence`` is noisy; stash on ``raw`` instead.
        best.raw = {**best.raw, "_acceptance_gate": gate}
        return best

    def _determine_acceptance_gate(
        self,
        best: TrackCandidate,
        top_candidates: list[TrackCandidate],
        draft: SegmentDraft,
    ) -> str | None:
        score = self._candidate_score(best, draft)
        distinct_providers = {candidate.provider for candidate in top_candidates}
        # G1 — multi-provider consensus.
        if len(distinct_providers) >= 2:
            return "G1"
        # G2 — 2+ probes from the same provider.
        if len(top_candidates) >= 2:
            return "G2"
        # G3 — strong single hit; music_ratio irrelevant (podcast+music rescue).
        if score >= 0.72:
            return "G3"
        # G4 — moderate single hit with noticeable music content.
        if score >= 0.60 and draft.music_ratio >= 0.35:
            return "G4"
        # G5 — ISRC-backed single hit.
        if best.track.isrc and score >= 0.55:
            return "G5"
        return None

    def _candidate_score(self, candidate: TrackCandidate, draft: SegmentDraft) -> float:
        provider_weight = {
            ProviderName.LOCAL_CATALOG: 0.93,
            ProviderName.ACRCLOUD: 0.90,
            ProviderName.AUDD: 0.87,
            ProviderName.VIBRA: 0.82,
        }.get(candidate.provider, 0.80)
        metadata_bonus = 0.05 if draft.metadata_hints else 0.0
        repeat_bonus = 0.04 if draft.repeat_group_id else 0.0
        evidence_bonus = min(0.08, 0.02 * len(candidate.evidence))
        return min(1.0, candidate.confidence * provider_weight + metadata_bonus + repeat_bonus + evidence_bonus)

    def _draft_explanation(self, draft: SegmentDraft) -> list[str]:
        explanation: list[str] = []
        if draft.kind == SegmentKind.SPEECH_ONLY:
            explanation.append(
                f"Speech-dominant region (speech {draft.speech_ratio:.2f}, music {draft.music_ratio:.2f})."
            )
        elif draft.kind == SegmentKind.SILENCE_OR_FX:
            explanation.append("Low musical content; treated as silence or effects.")
        else:
            explanation.append("Music was detected, but no candidate cleared the evidence threshold.")
        if draft.probe_count or draft.provider_attempts:
            explanation.append(
                f"Tried {draft.probe_count} probe(s) across {draft.provider_attempts} provider attempt(s)."
            )
        if draft.repeat_group_id:
            explanation.append(f"Segment belongs to repeat group {draft.repeat_group_id}.")
        if draft.metadata_hints:
            explanation.append(f"Metadata hints considered: {', '.join(draft.metadata_hints[:3])}.")
        return explanation

    def _candidate_explanation(self, draft: SegmentDraft, candidate: TrackCandidate, *, reused: bool) -> list[str]:
        explanation: list[str] = []
        agreeing = {
            item.provider.value
            for item in draft.candidates
            if item.track.normalized_key() == candidate.track.normalized_key()
        }
        if reused and draft.repeat_group_id:
            explanation.append(f"Matched via repeat-group propagation from {draft.repeat_group_id}.")
        elif agreeing:
            explanation.append(f"Provider agreement: {', '.join(sorted(agreeing))}.")
        explanation.append(
            f"Winning evidence score {self._candidate_score(candidate, draft):.2f} from {candidate.provider.value}."
        )
        if draft.probe_count or draft.provider_attempts:
            explanation.append(
                f"Analyzed {draft.probe_count} probe(s) with {draft.provider_attempts} provider attempt(s)."
            )
        if draft.metadata_hints:
            explanation.append(f"Metadata hints influenced ranking: {', '.join(draft.metadata_hints[:3])}.")
        return explanation

    def _draft_to_detected(self, source_item_id: str, draft: SegmentDraft) -> DetectedSegment:
        return DetectedSegment(
            source_item_id=source_item_id,
            start_ms=draft.start_ms,
            end_ms=draft.end_ms,
            kind=draft.kind,
            confidence=0.0,
            providers=[],
            evidence_count=0,
            track=None,
            repeat_group_id=draft.repeat_group_id,
            probe_count=draft.probe_count,
            provider_attempts=draft.provider_attempts,
            metadata_hints=draft.metadata_hints,
            uncertainty=1.0,
            explanation=self._draft_explanation(draft),
        )

    def _candidate_to_detected(self, source_item_id: str, draft: SegmentDraft, candidate: TrackCandidate, *, reused: bool) -> DetectedSegment:
        alternates = []
        seen = {candidate.track.normalized_key()}
        for alternate in sorted(draft.candidates, key=lambda value: value.confidence, reverse=True):
            key = alternate.track.normalized_key()
            if key in seen:
                continue
            seen.add(key)
            alternates.append(alternate.track)
        confidence = candidate.confidence if not reused else min(0.95, candidate.confidence + 0.03)
        # Lift acceptance-gate label (set by _pick_segment_candidate) + identity
        # key onto the segment for observability. Reused candidates inherit the
        # propagation label "REUSED" so downstream SQL can split these out.
        gate = str((candidate.raw or {}).get("_acceptance_gate") or "") or ("REUSED" if reused else None)
        return DetectedSegment(
            source_item_id=source_item_id,
            start_ms=draft.start_ms,
            end_ms=draft.end_ms,
            kind=SegmentKind.MATCHED_TRACK,
            confidence=confidence,
            providers=[candidate.provider] if not reused else [candidate.provider],
            evidence_count=max(1, len(candidate.evidence)),
            track=candidate.track,
            alternates=alternates,
            repeat_group_id=draft.repeat_group_id,
            probe_count=draft.probe_count,
            provider_attempts=draft.provider_attempts,
            metadata_hints=draft.metadata_hints,
            uncertainty=max(0.0, 1.0 - self._candidate_score(candidate, draft)),
            explanation=self._candidate_explanation(draft, candidate, reused=reused),
            identity_key=candidate.track.normalized_key(),
            acceptance_gate=gate,
        )

    def _stitch_segment_timeline(
        self,
        segments: list[DetectedSegment],
        *,
        options: "JobOptions | None" = None,
    ) -> list[DetectedSegment]:
        """Two-pass stitch (T1.2):

        1. **Bridge pass** — when a short non-MATCHED segment (speech or
           silence) sits between two MATCHED segments of the same identity,
           absorb it into the outer pair. Directly addresses the "song split
           by DJ talk" complaint.
        2. **Merge pass** — then collapse adjacent same-identity MATCHED
           segments (the classic "6 snippets of one song" case). Also merges
           adjacent non-MATCHED runs of the same kind within a tight gap.

        Both passes use duration-adaptive gap budgets; tiny SFX (<10s) keep a
        small tolerance so they don't get glued together, while long-form
        works (symphonies, extended mixes) tolerate 30s pauses. The ISRC
        veto in ``TrackMatch.merges_with`` prevents two distinct recordings
        from being accidentally merged.
        """
        if not segments:
            return []
        bridge_counter = [0]
        merged_counter = [0]
        ordered = sorted(
            segments,
            key=lambda segment: (segment.start_ms, segment.end_ms, segment.kind.value),
        )

        # --- Pass 1: bridge MATCHED → non-MATCHED → MATCHED of same identity ---
        bridged: list[DetectedSegment] = []
        index = 0
        while index < len(ordered):
            current = ordered[index]
            # Look ahead for a ``MATCHED(X) → (non-MATCHED) → MATCHED(X)`` pattern.
            if (
                current.kind == SegmentKind.MATCHED_TRACK
                and current.track is not None
                and index + 2 < len(ordered)
            ):
                bridge = ordered[index + 1]
                successor = ordered[index + 2]
                if (
                    bridge.kind in {SegmentKind.SPEECH_ONLY, SegmentKind.SILENCE_OR_FX, SegmentKind.MUSIC_UNRESOLVED}
                    and successor.kind == SegmentKind.MATCHED_TRACK
                    and successor.track is not None
                    and current.track.merges_with(successor.track)
                ):
                    gap_before = bridge.start_ms - current.end_ms
                    gap_after = successor.start_ms - bridge.end_ms
                    bridge_budget = self._bridge_gap_ms(current, successor, options=options)
                    bridge_duration = bridge.end_ms - bridge.start_ms
                    # Only bridge across SHORT intermediate segments. A 20s speech
                    # section is real speech (a host talking between tracks), not
                    # an announcement over continuous music; bridging it would
                    # wrongly glue two distinct occurrences of the same song.
                    if (
                        max(gap_before, 0) <= bridge_budget
                        and max(gap_after, 0) <= bridge_budget
                        and bridge_duration <= bridge_budget
                    ):
                        fused = self._merge_detected_segments(current, successor)
                        # Don't lose the bridged segment's time — just its body —
                        # so the outer segment covers the full span.
                        fused = fused.model_copy(update={"end_ms": max(fused.end_ms, successor.end_ms)})
                        fused = fused.model_copy(
                            update={
                                "explanation": list(
                                    dict.fromkeys(
                                        [
                                            *fused.explanation,
                                            f"Bridged across a {bridge.kind.value.replace('_', ' ')} gap of {max(0, gap_before)+max(0, gap_after)} ms.",
                                        ]
                                    )
                                )
                            }
                        )
                        bridge_counter[0] += 1
                        bridged.append(fused)
                        index += 3
                        continue
            bridged.append(current)
            index += 1

        # --- Pass 2: merge adjacent same-identity MATCHED + same-kind non-MATCHED ---
        merged: list[DetectedSegment] = []
        for segment in bridged:
            if merged and self._can_merge_segments(merged[-1], segment, options=options):
                merged[-1] = self._merge_detected_segments(merged[-1], segment)
                merged_counter[0] += 1
            else:
                merged.append(segment)

        # Finally clamp any residual overlaps.
        stitched: list[DetectedSegment] = []
        for index, segment in enumerate(merged):
            if index < len(merged) - 1:
                next_segment = merged[index + 1]
                if segment.end_ms > next_segment.start_ms:
                    segment = segment.model_copy(update={"end_ms": max(segment.start_ms, next_segment.start_ms)})
            if segment.end_ms > segment.start_ms:
                stitched.append(segment)
        # Stash counters for metric export (T4.2). Consumers look at them via
        # ``_consume_stitch_counters``.
        self._last_stitch_counters = {
            "segments_bridged_across_speech": bridge_counter[0],
            "segments_merged": merged_counter[0],
        }
        return stitched

    def _consume_stitch_counters(self) -> dict[str, int]:
        """Pop the per-stitch observability counters (T4.2)."""
        counters = getattr(self, "_last_stitch_counters", {}) or {}
        self._last_stitch_counters = {}
        return counters

    def _can_merge_segments(
        self,
        left: DetectedSegment,
        right: DetectedSegment,
        *,
        options: "JobOptions | None" = None,
    ) -> bool:
        gap_ms = max(0, right.start_ms - left.end_ms)
        if left.kind == SegmentKind.MATCHED_TRACK and right.kind == SegmentKind.MATCHED_TRACK:
            if not (left.track and right.track):
                return False
            if not left.track.merges_with(right.track):
                return False
            return gap_ms <= self._same_track_gap_ms(left, right, options=options)
        if left.kind != right.kind:
            return False
        # Non-MATCHED runs of the same kind merge only with the plain bridge-gap
        # allowance; they have no identity to compare.
        return gap_ms <= self._bridge_gap_ms(left, right, options=options) and (
            left.track is None and right.track is None
        )

    def _same_track_gap_ms(
        self,
        left: DetectedSegment,
        right: DetectedSegment,
        *,
        options: "JobOptions | None" = None,
    ) -> int:
        """Duration-adaptive gap for same-identity merging.

        Short SFX snippets stay crisp; normal 3-minute songs survive DJ intros;
        hour-long classical works absorb legitimate quiet movements.
        """
        longer = max(left.end_ms - left.start_ms, right.end_ms - right.start_ms)
        if longer < 10_000:
            return 2_000
        if longer < 60_000:
            return 5_000
        if longer < 600_000:
            base = (options or JobOptions()).merge_gap_same_track_ms
            return max(5_000, base)
        # long-form (>10 min): symphonies, extended mixes, full sets.
        return 30_000

    def _bridge_gap_ms(
        self,
        left: DetectedSegment,
        right: DetectedSegment,
        *,
        options: "JobOptions | None" = None,
    ) -> int:
        """Gap allowance for bridging a non-MATCHED segment between two
        MATCHED same-identity segments. Slightly stricter than same-track
        merging because a bridge has to survive TWO gap checks."""
        longer = max(left.end_ms - left.start_ms, right.end_ms - right.start_ms)
        base = (options or JobOptions()).merge_gap_bridge_ms
        if longer < 10_000:
            return min(2_000, base)
        if longer < 60_000:
            return min(5_000, base)
        if longer < 600_000:
            return base
        return max(base, 19_000)

    def _merge_detected_segments(self, left: DetectedSegment, right: DetectedSegment) -> DetectedSegment:
        metadata_hints = list(dict.fromkeys([*left.metadata_hints, *right.metadata_hints]))
        providers = sorted(set([*left.providers, *right.providers]), key=lambda provider: provider.value)
        alternates = list(left.alternates)
        seen = {alternate.normalized_key() for alternate in alternates}
        for alternate in right.alternates:
            key = alternate.normalized_key()
            if key in seen:
                continue
            seen.add(key)
            alternates.append(alternate)
        return left.model_copy(
            update={
                "end_ms": max(left.end_ms, right.end_ms),
                "confidence": max(left.confidence, right.confidence),
                "providers": providers,
                "evidence_count": left.evidence_count + right.evidence_count,
                "alternates": alternates,
                "probe_count": left.probe_count + right.probe_count,
                "provider_attempts": left.provider_attempts + right.provider_attempts,
                "metadata_hints": metadata_hints,
                "uncertainty": min(left.uncertainty or 1.0, right.uncertainty or 1.0),
                "explanation": list(dict.fromkeys([*left.explanation, *right.explanation])),
            }
        )

    def _select_windows(self, job: Job, item: SourceItem, normalized: Path, instrumental: Path | None, profile: SourceProfile) -> list[WindowPlan]:
        if profile.strategy == "long_mix":
            primary_path = instrumental if instrumental and profile.prefer_source_path == "instrumental" else normalized
            primary_label = "instrumental" if primary_path == instrumental and instrumental is not None else "mix"
            plans = metadata_windows(primary_path, item.metadata, duration_ms=profile.duration_ms, label=primary_label)
            try:
                plans.extend(clustered_long_mix_windows(primary_path, label=primary_label, max_windows=max(4, profile.max_windows // 2)))
            except MediaToolError:
                pass
            plans.extend(plan_windows_for_profile(primary_path, profile, primary_label))
            deduped: list[WindowPlan] = []
            seen: set[tuple[int, int, str]] = set()
            for plan in sorted(plans, key=lambda candidate: candidate.start_ms):
                key = (plan.start_ms, plan.end_ms, plan.label)
                if key in seen:
                    continue
                seen.add(key)
                deduped.append(plan)
            return deduped[: profile.max_windows]

        plans: list[WindowPlan] = []
        plans.extend(plan_windows_for_profile(normalized, profile, "mix"))
        if instrumental:
            secondary_profile = SourceProfile(
                duration_ms=profile.duration_ms,
                strategy="single_track" if profile.strategy == "single_track" else "multi_track",
                prefer_source_path=profile.prefer_source_path,
                request_budget=profile.request_budget,
                max_windows=max(2, profile.max_windows // 2),
                stop_after_consensus=profile.stop_after_consensus,
                use_source_separation=profile.use_source_separation,
            )
            if profile.strategy == "long_mix":
                plans.extend(metadata_windows(instrumental, item.metadata, duration_ms=profile.duration_ms, label="instrumental"))
                plans.extend(plan_windows_for_profile(instrumental, profile, "instrumental"))
            else:
                plans.extend(plan_windows_for_profile(instrumental, secondary_profile, "instrumental"))
        deduped: list[WindowPlan] = []
        seen: set[tuple[int, int, str]] = set()
        sort_key = (lambda plan: plan.start_ms) if profile.strategy == "long_mix" else (lambda plan: (-plan.score, plan.start_ms))
        for plan in sorted(plans, key=sort_key):
            key = (plan.start_ms, plan.end_ms, plan.label)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(plan)
        if profile.strategy == "long_mix":
            effective_limit = profile.max_windows
        else:
            effective_limit = min(job.options.max_windows, profile.max_windows)
        return deduped[:effective_limit]

    def _recognize_with_cache(
        self,
        job_id: str,
        item: SourceItem,
        provider: BaseProvider,
        excerpt_path: Path,
        start_ms: int,
        end_ms: int,
    ) -> list[TrackCandidate]:
        self._raise_if_canceled(job_id)
        started_at = time.monotonic()
        cache_key = fingerprint_cache_key(excerpt_path)
        cached = self.db.get_provider_cache(cache_key, provider.name)
        if cached:
            payload = json.loads(cached)
            hits = [TrackCandidate.model_validate(candidate) for candidate in payload]
            self.db.add_recognition_metric(
                RecognitionMetric(
                    id=str(uuid.uuid4()),
                    job_id=job_id,
                    source_item_id=item.id,
                    provider_name=provider.name,
                    cache_hit=True,
                    matched=bool(hits),
                    call_count=0,
                    elapsed_ms=int((time.monotonic() - started_at) * 1000),
                    payload={"start_ms": start_ms, "end_ms": end_ms, "cache_key": cache_key},
                    created_at=now_iso(),
                )
            )
            return hits
        try:
            self._raise_if_canceled(job_id)
            self._throttle_provider(provider.name)
            self._raise_if_canceled(job_id)
            provider_hits = provider.recognize(excerpt_path, start_ms, end_ms)
            self._raise_if_canceled(job_id)
        except ProviderError as exc:
            if self._is_canceled(job_id):
                raise RuntimeError("__CANCELLED__") from exc
            self.db.add_event(job_id, "warning", f"{provider.name} failed on {item.id}: {exc}")
            return []
        except Exception as exc:
            if self._is_canceled(job_id):
                raise RuntimeError("__CANCELLED__") from exc
            self.db.add_event(job_id, "warning", f"{provider.name} crashed on {item.id}: {exc}")
            return []
        self.db.set_provider_cache(cache_key, provider.name, json.dumps([candidate.model_dump(mode="json") for candidate in provider_hits]))
        self.db.add_recognition_metric(
            RecognitionMetric(
                id=str(uuid.uuid4()),
                job_id=job_id,
                source_item_id=item.id,
                provider_name=provider.name,
                cache_hit=False,
                matched=bool(provider_hits),
                call_count=1,
                elapsed_ms=int((time.monotonic() - started_at) * 1000),
                payload={"start_ms": start_ms, "end_ms": end_ms, "cache_key": cache_key},
                created_at=now_iso(),
            )
        )
        return provider_hits

    def _throttle_provider(self, provider_name: ProviderName) -> None:
        min_interval = max(0.0, self.settings.provider_min_interval_ms / 1000)
        if min_interval <= 0:
            return
        with self._provider_call_lock:
            now = time.monotonic()
            next_at = self._provider_next_call_at.get(provider_name, now)
            if next_at > now:
                time.sleep(next_at - now)
                now = time.monotonic()
            self._provider_next_call_at[provider_name] = now + min_interval

    def _should_stop_early(self, profile: SourceProfile, candidates: list[TrackCandidate]) -> bool:
        if profile.stop_after_consensus <= 0 or not candidates:
            return False
        counts = Counter(candidate.track.normalized_key() for candidate in candidates)
        top_key, top_count = counts.most_common(1)[0]
        if top_count >= profile.stop_after_consensus:
            return True
        top_confidence = max(
            candidate.confidence
            for candidate in candidates
            if candidate.track.normalized_key() == top_key
        )
        return top_confidence >= 0.92

    def _probes_have_strong_match(self, candidates: list[TrackCandidate]) -> bool:
        if not candidates:
            return False
        counts = Counter(candidate.track.normalized_key() for candidate in candidates)
        top_key, top_count = counts.most_common(1)[0]
        if top_count >= 2:
            return True
        top_confidence = max(
            candidate.confidence
            for candidate in candidates
            if candidate.track.normalized_key() == top_key
        )
        return top_confidence >= 0.90

    def _record_item_summary_metric(self, job_id: str, source_item_id: str, segments: list[DetectedSegment]) -> None:
        # Pop any transient counters the stitch/long-mix passes attached to
        # ``self``. They're per-item and must not leak into the next call, so
        # ``_consume_stitch_counters`` clears them as it returns.
        stitch_counters = self._consume_stitch_counters()
        segment_counters = getattr(self, "_last_segment_counters", {}) or {}
        self._last_segment_counters = {}
        # Tally acceptance-gate hits for observability.
        gate_tallies = Counter(
            (segment.acceptance_gate or "")
            for segment in segments
            if segment.kind == SegmentKind.MATCHED_TRACK
        )
        self.db.add_recognition_metric(
            RecognitionMetric(
                id=str(uuid.uuid4()),
                job_id=job_id,
                source_item_id=source_item_id,
                provider_name=None,
                call_count=0,
                matched_segments=sum(1 for segment in segments if segment.kind == SegmentKind.MATCHED_TRACK),
                unresolved_segments=sum(1 for segment in segments if segment.kind == SegmentKind.MUSIC_UNRESOLVED),
                segments_merged=int(stitch_counters.get("segments_merged", 0)),
                segments_bridged_across_speech=int(stitch_counters.get("segments_bridged_across_speech", 0)),
                repeat_group_reconfirmed=int(segment_counters.get("repeat_group_reconfirmed", 0)),
                repeat_group_rejected=int(segment_counters.get("repeat_group_rejected", 0)),
                gate_g1_hits=int(gate_tallies.get("G1", 0)),
                gate_g2_hits=int(gate_tallies.get("G2", 0)),
                gate_g3_hits=int(gate_tallies.get("G3", 0)),
                gate_g4_hits=int(gate_tallies.get("G4", 0)),
                gate_g5_hits=int(gate_tallies.get("G5", 0)),
                payload={"segment_count": len(segments)},
                created_at=now_iso(),
            )
        )

    def export_job(self, job_id: str, *, export_format: str = "json") -> tuple[str, str]:
        job = self.db.get_job(job_id)
        if not job:
            raise ValueError(f"Unknown job: {job_id}")
        items = self.db.get_source_items(job_id)
        segments = self.db.get_segments(job_id)
        export_format = export_format.lower()
        if export_format == "json":
            payload = {
                "job": job.model_dump(mode="json"),
                "items": [item.model_dump(mode="json") for item in items],
                "segments": [segment.model_dump(mode="json") for segment in segments],
                "events": [event.model_dump(mode="json") for event in self.db.list_events(job_id)],
            }
            return (f"music-fetch-{job_id[:8]}.json", json.dumps(payload, indent=2))
        if export_format == "csv":
            buffer = io.StringIO()
            writer = csv.writer(buffer)
            writer.writerow(
                [
                    "source_item_id",
                    "start_ms",
                    "end_ms",
                    "kind",
                    "confidence",
                    "uncertainty",
                    "title",
                    "artist",
                    "album",
                    "providers",
                    "repeat_group_id",
                    "metadata_hints",
                    "explanation",
                ]
            )
            for segment in segments:
                writer.writerow(
                    [
                        segment.source_item_id,
                        segment.start_ms,
                        segment.end_ms,
                        segment.kind.value,
                        f"{segment.confidence:.4f}",
                        "" if segment.uncertainty is None else f"{segment.uncertainty:.4f}",
                        segment.track.title if segment.track else "",
                        segment.track.artist if segment.track and segment.track.artist else "",
                        segment.track.album if segment.track and segment.track.album else "",
                        ", ".join(provider.value for provider in segment.providers),
                        segment.repeat_group_id or "",
                        " | ".join(segment.metadata_hints),
                        " | ".join(segment.explanation),
                    ]
                )
            return (f"music-fetch-{job_id[:8]}.csv", buffer.getvalue())
        if export_format == "chapters":
            lines: list[str] = []
            for segment in segments:
                stamp = self._chapter_timestamp(segment.start_ms)
                if segment.track:
                    title = segment.track.title
                    if segment.track.artist:
                        title = f"{segment.track.artist} - {title}"
                elif segment.kind == SegmentKind.MUSIC_UNRESOLVED:
                    title = "[Unresolved music]"
                elif segment.kind == SegmentKind.SPEECH_ONLY:
                    title = "[Speech only]"
                else:
                    title = "[Silence / FX]"
                lines.append(f"{stamp} {title}")
            return (f"music-fetch-{job_id[:8]}-chapters.txt", "\n".join(lines))
        raise ValueError(f"Unsupported export format: {export_format}")

    def correct_segment(
        self,
        job_id: str,
        *,
        source_item_id: str,
        start_ms: int,
        end_ms: int,
        title: str,
        artist: str | None = None,
        album: str | None = None,
    ) -> DetectedSegment:
        job = self.db.get_job(job_id)
        if not job:
            raise ValueError(f"Unknown job: {job_id}")
        grouped = self._segments_by_source_item(job_id)
        segments = grouped.get(source_item_id)
        if not segments:
            raise ValueError(f"Unknown source item: {source_item_id}")
        target_index = next(
            (index for index, segment in enumerate(segments) if segment.start_ms == start_ms and segment.end_ms == end_ms),
            None,
        )
        if target_index is None:
            raise ValueError("Segment not found")
        current = segments[target_index]
        manual_match = TrackMatch(
            title=title.strip(),
            artist=(artist or "").strip() or None,
            album=(album or "").strip() or None,
            external_links=build_search_links(title.strip(), (artist or "").strip() or None),
            raw={"source": "manual_correction", "job_id": job_id},
        )
        alternates = list(current.alternates)
        if current.track and current.track.normalized_key() != manual_match.normalized_key():
            alternates.insert(0, current.track)
        corrected = current.model_copy(
            update={
                "kind": SegmentKind.MATCHED_TRACK,
                "track": manual_match,
                "confidence": max(0.99, current.confidence),
                "alternates": alternates[:5],
                "metadata_hints": list(dict.fromkeys([*current.metadata_hints, f"manual:{manual_match.title}"])),
                "uncertainty": 0.0,
                "explanation": [
                    "Manually corrected by the user.",
                    *[line for line in current.explanation if line != "Manually corrected by the user."],
                ],
            }
        )
        segments[target_index] = corrected
        self.db.replace_segments(job_id, source_item_id, segments)
        self.db.add_event(job_id, "info", f"Manual correction saved for {source_item_id} {start_ms}-{end_ms}")
        return corrected

    def retry_unresolved_segments(
        self,
        job_id: str,
        *,
        source_item_id: str | None = None,
        options_override: JobOptions | None = None,
    ) -> dict[str, int]:
        job = self.db.get_job(job_id)
        if not job:
            raise ValueError(f"Unknown job: {job_id}")
        options = options_override or job.options
        providers = self.provider_registry.active_providers_for_order(options.provider_order)
        items = {item.id: item for item in self.db.get_source_items(job_id)}
        grouped = self._segments_by_source_item(job_id)
        retried_segments = 0
        matched_segments = 0
        remaining_unresolved = 0
        for item_id, segments in grouped.items():
            if source_item_id and item_id != source_item_id:
                continue
            item = items.get(item_id)
            if item is None:
                continue
            pending = [segment for segment in segments if segment.kind == SegmentKind.MUSIC_UNRESOLVED]
            if not pending:
                continue
            normalized, excerpt_source = self._ensure_retry_media(job, item, options)
            updated_segments = list(segments)
            for unresolved in pending:
                retried_segments += 1
                replacement = self._retry_segment(job, item, unresolved, excerpt_source, providers, options)
                for index, candidate in enumerate(updated_segments):
                    if (
                        candidate.source_item_id == unresolved.source_item_id
                        and candidate.start_ms == unresolved.start_ms
                        and candidate.end_ms == unresolved.end_ms
                    ):
                        updated_segments[index] = replacement
                        break
                if replacement.kind == SegmentKind.MATCHED_TRACK:
                    matched_segments += 1
                else:
                    remaining_unresolved += 1
            self.db.replace_segments(job_id, item_id, updated_segments)
            self._record_item_summary_metric(job_id, item_id, updated_segments)
        self.db.add_event(
            job_id,
            "info",
            f"Retried {retried_segments} unresolved segment(s); matched {matched_segments}.",
        )
        return {
            "retried_segments": retried_segments,
            "matched_segments": matched_segments,
            "remaining_unresolved_segments": remaining_unresolved,
        }

    def _segments_by_source_item(self, job_id: str) -> dict[str, list[DetectedSegment]]:
        grouped: dict[str, list[DetectedSegment]] = {}
        for segment in self.db.get_segments(job_id):
            grouped.setdefault(segment.source_item_id, []).append(segment)
        return grouped

    def _ensure_retry_media(self, job: Job, item: SourceItem, options: JobOptions) -> tuple[Path, Path]:
        if item.local_path:
            local_media = Path(item.local_path).expanduser()
        elif item.kind == SourceKind.LOCAL_FILE:
            local_media = Path(item.input_value).expanduser().resolve()
            if not local_media.exists():
                raise MediaToolError(f"Local file no longer exists: {item.input_value}")
        else:
            local_media = ensure_local_media(self.settings, item)
        item.local_path = str(local_media)
        normalized = Path(item.normalized_path).expanduser() if item.normalized_path and Path(item.normalized_path).exists() else None
        if normalized is None:
            normalized_dir = self.settings.cache_dir / "normalized" / job.id / item.id
            normalized = normalize_media(local_media, normalized_dir / "normalized.wav")
            item.normalized_path = str(normalized)
        excerpt_source = normalized
        if options.prefer_separation:
            instrumental = (
                Path(item.instrumental_path).expanduser()
                if item.instrumental_path and Path(item.instrumental_path).exists()
                else None
            )
            if instrumental is None:
                try:
                    instrumental = isolate_music(self.settings, normalized, normalized.parent / "stems")
                    item.instrumental_path = str(instrumental)
                except MediaToolError:
                    instrumental = None
            if instrumental is not None:
                excerpt_source = instrumental
        self.db.update_source_item(item)
        return normalized, excerpt_source

    def _retry_segment(
        self,
        job: Job,
        item: SourceItem,
        segment: DetectedSegment,
        excerpt_source: Path,
        providers: list[BaseProvider],
        options: JobOptions,
    ) -> DetectedSegment:
        drafts = SegmentDraft(
            start_ms=segment.start_ms,
            end_ms=segment.end_ms,
            kind=segment.kind,
            feature_vector=[],
            chroma_vector=[],
            music_ratio=1.0,
            speech_ratio=0.0,
            metadata_hints=list(segment.metadata_hints),
            repeat_group_id=segment.repeat_group_id,
        )
        excerpts_dir = excerpt_source.parent / "retry-clips"
        candidates: list[TrackCandidate] = []
        provider_attempts = 0
        probe_count = 0
        for retry_start, retry_end, reason in self._retry_windows(segment, options.max_probes_per_segment):
            probe_count += 1
            excerpt_path = build_excerpt_path(excerpts_dir, excerpt_source, retry_start, retry_end, f"retry-{reason}")
            if not excerpt_path.exists():
                create_excerpt(excerpt_source, retry_start, retry_end, excerpt_path)
            for provider in providers:
                state = provider.state()
                if not state.available:
                    continue
                provider_attempts += 1
                candidates.extend(
                    self._recognize_with_cache(job.id, item, provider, excerpt_path, retry_start, retry_end)
                )
        drafts.probe_count = probe_count
        drafts.provider_attempts = provider_attempts
        drafts.candidates = candidates
        best = self._pick_segment_candidate(drafts)
        if best is None and candidates:
            highest = max(candidates, key=lambda candidate: self._candidate_score(candidate, drafts))
            if self._candidate_score(highest, drafts) >= 0.60:
                best = highest
        if best:
            retried = self._candidate_to_detected(item.id, drafts, best, reused=False)
            retried = retried.model_copy(
                update={
                    "explanation": [
                        "Recovered by retrying an unresolved region.",
                        *retried.explanation,
                    ]
                }
            )
            return retried
        return segment.model_copy(
            update={
                "probe_count": segment.probe_count + probe_count,
                "provider_attempts": segment.provider_attempts + provider_attempts,
                "explanation": list(
                    dict.fromkeys(
                        [
                            *segment.explanation,
                            f"Retried with {probe_count} probe(s) and {provider_attempts} provider attempt(s), but no stronger match was found.",
                        ]
                    )
                ),
                "uncertainty": segment.uncertainty if segment.uncertainty is not None else 1.0,
            }
        )

    def _retry_windows(self, segment: DetectedSegment, max_probes: int) -> list[tuple[int, int, str]]:
        duration = max(1_000, segment.end_ms - segment.start_ms)
        probe_ms = min(18_000, duration)
        midpoint = segment.start_ms + duration // 2
        windows = [
            (segment.start_ms, min(segment.end_ms, segment.start_ms + probe_ms), "start"),
            (max(segment.start_ms, midpoint - probe_ms // 2), min(segment.end_ms, midpoint + probe_ms // 2), "mid"),
            (max(segment.start_ms, segment.end_ms - probe_ms), segment.end_ms, "end"),
        ]
        deduped: list[tuple[int, int, str]] = []
        seen: set[tuple[int, int]] = set()
        for start_ms, end_ms, reason in windows:
            key = (start_ms, end_ms)
            if key in seen or end_ms <= start_ms:
                continue
            seen.add(key)
            deduped.append((start_ms, end_ms, reason))
        return deduped[: max(1, max_probes)]

    def _chapter_timestamp(self, milliseconds: int) -> str:
        total_seconds = milliseconds // 1000
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        return f"{minutes:02d}:{seconds:02d}"

    def _collect_artifact_entries(self, job_id: str | None = None) -> list[ArtifactEntry]:
        jobs = [self.db.get_job(job_id)] if job_id else self.db.list_jobs(limit=500)
        pinned_jobs = self.db.list_pinned_job_ids()
        entries: list[ArtifactEntry] = []
        seen_paths: set[str] = set()
        for job in jobs:
            if job is None:
                continue
            items = self.db.get_source_items(job.id)
            for item in items:
                for category, label, path, temporary in self._item_artifact_specs(item):
                    normalized = str(path.expanduser())
                    if normalized in seen_paths:
                        continue
                    seen_paths.add(normalized)
                    entry = self._artifact_entry(
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
            for entry in self._orphan_recording_entries():
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
                entry = self._artifact_entry(category=category, label=label, path=path, temporary=temporary)
                if entry:
                    entries.append(entry)
        return entries

    def _item_artifact_specs(self, item: SourceItem) -> list[tuple[ArtifactCategory, str, Path, bool]]:
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
            if self._is_upload_path(input_path):
                specs.append((ArtifactCategory.UPLOAD, "Hochgeladene Datei", input_path, True))
            elif self._is_recording_path(input_path):
                specs.append((ArtifactCategory.RECORDING, "Temporäre Aufnahme", input_path, True))

        if item.local_path:
            local_path = Path(item.local_path).expanduser()
            if self._is_recording_path(local_path):
                specs.append((ArtifactCategory.RECORDING, "Temporäre Aufnahme", local_path, True))
            elif self._is_upload_path(local_path):
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

    def _orphan_recording_entries(self) -> list[ArtifactEntry]:
        temp_dir = Path(tempfile.gettempdir())
        entries: list[ArtifactEntry] = []
        for pattern, label in [("music-fetch-mic-*.m4a", "Temporäre Mikrofonaufnahme"), ("music-fetch-system-*.m4a", "Temporäre Systemaufnahme")]:
            for path in temp_dir.glob(pattern):
                entry = self._artifact_entry(
                    category=ArtifactCategory.RECORDING,
                    label=label,
                    path=path,
                    temporary=True,
                )
                if entry:
                    entries.append(entry)
        return entries

    def _artifact_entry(
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
            size_bytes=self._path_size(resolved) if exists else 0,
            exists=exists,
            temporary=temporary,
            job_id=job_id,
            source_item_id=source_item_id,
            pinned=pinned,
        )

    def _clear_item_artifact_references(self, job_id: str) -> None:
        for item in self.db.get_source_items(job_id):
            local_path = Path(item.local_path).expanduser() if item.local_path else None
            if local_path and (self._is_recording_path(local_path) or self._is_upload_path(local_path) or self._is_path_in_dir(local_path, self.settings.cache_dir)):
                item.local_path = None
            item.normalized_path = None
            item.instrumental_path = None
            self.db.update_source_item(item)

    def _delete_artifact_entries(self, entries: list[ArtifactEntry], *, skip_pinned: bool) -> None:
        paths = [
            Path(entry.path)
            for entry in entries
            if entry.temporary and entry.exists and not (skip_pinned and entry.pinned)
        ]
        for path in sorted({path for path in paths}, key=lambda candidate: len(candidate.parts), reverse=True):
            self._delete_path(path)

    def _delete_path(self, path: Path) -> None:
        if not path.exists():
            return
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        else:
            try:
                path.unlink()
            except FileNotFoundError:
                return

    def _prune_job_cache_dirs(self, job_id: str) -> None:
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

    def _path_size(self, path: Path) -> int:
        if not path.exists():
            return 0
        if path.is_file():
            return path.stat().st_size
        return sum(candidate.stat().st_size for candidate in path.rglob("*") if candidate.is_file())

    def _is_path_in_dir(self, path: Path, directory: Path) -> bool:
        try:
            path.resolve().relative_to(directory.resolve())
            return True
        except ValueError:
            return False

    def _is_recording_path(self, path: Path) -> bool:
        return path.name.startswith("music-fetch-mic-") or path.name.startswith("music-fetch-system-")

    def _is_recording_source(self, item: SourceItem) -> bool:
        for raw_path in [item.input_value, item.local_path]:
            if not raw_path:
                continue
            if self._is_recording_path(Path(raw_path).expanduser()):
                return True
        return False

    def _is_upload_path(self, path: Path) -> bool:
        return self._is_path_in_dir(path, self.settings.cache_dir / "uploads")

    def _storage_locations(self) -> dict[str, str]:
        return {
            "cache": str(self.settings.cache_dir),
            "data": str(self.settings.data_dir),
            "config": str(self.settings.config_dir),
            "database": str(self.settings.db_path),
            "temporary_recordings": str(Path(tempfile.gettempdir())),
        }
