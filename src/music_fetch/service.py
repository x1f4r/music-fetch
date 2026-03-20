from __future__ import annotations

import json
import shutil
import threading
import tempfile
import time
from collections import Counter
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path

from .config import Settings
from .db import Database
from .fusion import fuse_candidates
from .links import build_search_links
from .long_mix import SegmentDraft, analyze_long_mix
from .media import (
    build_excerpt_path,
    classify_source,
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
    ItemStatus,
    Job,
    JobCreate,
    JobStatus,
    LibraryEntry,
    ProviderConfig,
    ProviderName,
    ProviderState,
    SegmentKind,
    SourceItem,
    StorageSummary,
    TrackCandidate,
    TrackMatch,
    WindowPlan,
)
from .providers import ACRCloudProvider, AudDProvider, LocalCatalogProvider, VibraProvider
from .providers.base import BaseProvider, ProviderError
from .sources import SourceResolver


class JobManager:
    def __init__(self, settings: Settings, db: Database) -> None:
        self.settings = settings
        self.db = db
        self.source_resolver = SourceResolver(settings.cache_dir)
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

    def submit_payload(self, inputs: list[str]) -> Job:
        return self.submit(JobCreate(inputs=inputs))

    def wait(self, job_id: str, poll_interval: float = 0.5) -> Job:
        while True:
            job = self.db.get_job(job_id)
            if job and job.status in {JobStatus.SUCCEEDED, JobStatus.PARTIAL_FAILED, JobStatus.FAILED}:
                return job
            time.sleep(poll_interval)

    def provider_states(self) -> list[ProviderState]:
        saved = self.db.get_provider_configs()
        states: list[ProviderState] = []
        for provider in [
            LocalCatalogProvider(self.settings, self.db),
            VibraProvider(self.settings),
            AudDProvider((saved.get(ProviderName.AUDD) or ProviderConfig()).config.get("api_token")),
            ACRCloudProvider(
                (saved.get(ProviderName.ACRCLOUD) or ProviderConfig()).config.get("host"),
                (saved.get(ProviderName.ACRCLOUD) or ProviderConfig()).config.get("access_key"),
                (saved.get(ProviderName.ACRCLOUD) or ProviderConfig()).config.get("access_secret"),
            ),
        ]:
            state = provider.state()
            config = saved.get(state.name)
            if config:
                state = state.model_copy(update={"enabled": config.enabled, "config": {**state.config, **config.config}})
            states.append(state)
        order = {name: index for index, name in enumerate(self.settings.provider_order)}
        states.sort(key=lambda state: order.get(state.name, 999))
        return states

    def set_provider_config(self, name: ProviderName, config: ProviderConfig) -> ProviderState:
        self.db.set_provider_config(name, config)
        states = {state.name: state for state in self.provider_states()}
        return states[name]

    def import_catalog(self, paths: list[Path]) -> int:
        provider = LocalCatalogProvider(self.settings, self.db)
        return provider.import_paths(paths)

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
            summary = self.storage_summary(job.id)
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

    def storage_summary(self, job_id: str | None = None) -> StorageSummary:
        entries = self._collect_artifact_entries(job_id)
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
            locations=self._storage_locations(),
        )

    def set_job_pinned(self, job_id: str, pinned: bool) -> bool:
        if not self.db.get_job(job_id):
            raise ValueError(f"Unknown job: {job_id}")
        self.db.set_job_pinned(job_id, pinned)
        return pinned

    def cleanup_job_artifacts(self, job_id: str, *, force: bool = True) -> StorageSummary:
        job = self.db.get_job(job_id)
        if not job:
            raise ValueError(f"Unknown job: {job_id}")
        if self.db.is_job_pinned(job_id) and not force:
            return self.storage_summary(job_id)

        entries = self._collect_artifact_entries(job_id)
        self._delete_artifact_entries(entries, skip_pinned=not force)
        self._prune_job_cache_dirs(job_id)
        self._clear_item_artifact_references(job_id)
        self.db.add_event(job_id, "info", "Temporary artifacts removed")
        return self.storage_summary(job_id)

    def cleanup_temporary_artifacts(self) -> StorageSummary:
        for entry in self.list_library_entries(limit=500):
            if entry.pinned:
                continue
            self.cleanup_job_artifacts(entry.job_id, force=False)
        for orphan in self._orphan_recording_entries():
            self._delete_path(Path(orphan.path))
        return self.storage_summary()

    def _providers(self) -> list[BaseProvider]:
        saved = self.db.get_provider_configs()
        provider_chain: dict[ProviderName, BaseProvider] = {
            ProviderName.LOCAL_CATALOG: LocalCatalogProvider(self.settings, self.db),
            ProviderName.VIBRA: VibraProvider(self.settings),
            ProviderName.AUDD: AudDProvider((saved.get(ProviderName.AUDD) or ProviderConfig()).config.get("api_token")),
            ProviderName.ACRCLOUD: ACRCloudProvider(
                (saved.get(ProviderName.ACRCLOUD) or ProviderConfig()).config.get("host"),
                (saved.get(ProviderName.ACRCLOUD) or ProviderConfig()).config.get("access_key"),
                (saved.get(ProviderName.ACRCLOUD) or ProviderConfig()).config.get("access_secret"),
            ),
        }
        active: list[BaseProvider] = []
        for name in self.settings.provider_order:
            provider = provider_chain[name]
            config = saved.get(name)
            if config and not config.enabled:
                continue
            active.append(provider)
        return active

    def _run_job(self, job_id: str) -> None:
        job = self.db.get_job(job_id)
        if not job:
            return
        self.db.update_job(job_id, status=JobStatus.RUNNING)
        self.db.add_event(job_id, "info", "Resolving inputs")
        try:
            items = self.source_resolver.resolve_inputs(job_id, job.inputs)
            self.db.add_source_items(items)
            failures = 0
            worker_count = max(1, min(self.settings.max_workers, len(items)))
            with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="music-fetch-item") as item_executor:
                future_map = {item_executor.submit(self._process_item, job, item): item for item in items}
                for future in as_completed(future_map):
                    item = future_map[future]
                    try:
                        future.result()
                    except Exception as exc:
                        failures += 1
                        item.status = ItemStatus.FAILED
                        item.error = str(exc)
                        self.db.update_source_item(item)
                        self.db.add_event(job_id, "error", f"{item.input_value}: {exc}")
            if failures == 0:
                status = JobStatus.SUCCEEDED
            elif failures < len(items):
                status = JobStatus.PARTIAL_FAILED
            else:
                status = JobStatus.FAILED
            self.db.update_job(job_id, status=status)
            if status in {JobStatus.SUCCEEDED, JobStatus.PARTIAL_FAILED} and not self.settings.retain_artifacts and not self.db.is_job_pinned(job_id):
                try:
                    self.cleanup_job_artifacts(job_id, force=False)
                except Exception as exc:
                    self.db.add_event(job_id, "warning", f"Artifact cleanup failed: {exc}")
            self.db.add_event(job_id, "info", f"Job finished with status {status}")
        except Exception as exc:
            self.db.update_job(job_id, status=JobStatus.FAILED, error=str(exc))
            self.db.add_event(job_id, "error", f"Job failed: {exc}")

    def _process_item(self, job: Job, item: SourceItem) -> None:
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
            local_media = ensure_local_media(self.settings, item)
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
        item.normalized_path = str(normalized)
        if not item.metadata.duration_ms:
            item.metadata.duration_ms = probe_duration_ms(normalized)

        profile = classify_source(
            item.metadata.duration_ms or 0,
            has_playlist_context=item.metadata.playlist_id is not None,
            metadata=item.metadata,
        )
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
            item.instrumental_path = str(instrumental)
        self.db.update_source_item(item)

        if profile.strategy in {"long_mix", "multi_track"}:
            segments = self._process_long_mix_item(job, item, normalized, instrumental)
            self.db.replace_segments(job.id, item.id, segments)
            item.status = ItemStatus.SUCCEEDED
            self.db.update_source_item(item)
            return

        plans = self._select_windows(job, item, normalized, instrumental, profile)
        providers = self._providers()
        candidates: list[TrackCandidate] = []
        excerpts_dir = normalized_dir / "clips"
        remaining_budget = profile.request_budget
        for plan in plans:
            if remaining_budget <= 0:
                self.db.add_event(job.id, "info", f"Request budget exhausted for {item.id}")
                break
            excerpt_path = build_excerpt_path(excerpts_dir, Path(plan.source_path), plan.start_ms, plan.end_ms, plan.label)
            if not excerpt_path.exists():
                create_excerpt(Path(plan.source_path), plan.start_ms, plan.end_ms, excerpt_path)
            for provider in providers:
                if remaining_budget <= 0:
                    break
                state = provider.state()
                if not state.available:
                    continue
                provider_hits = self._recognize_with_cache(job.id, item, provider, excerpt_path, plan.start_ms, plan.end_ms)
                remaining_budget -= 1
                if provider_hits:
                    self.db.add_event(job.id, "info", f"{provider.name} matched {provider_hits[0].track.title}")
                candidates.extend(provider_hits)
            if self._should_stop_early(profile, candidates):
                self.db.add_event(job.id, "info", f"Early stop reached for {item.id}")
                break

        segments = fuse_candidates(item.id, candidates)
        self.db.replace_segments(job.id, item.id, segments)
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
            )
        ]

    def _process_long_mix_item(self, job: Job, item: SourceItem, normalized: Path, instrumental: Path | None = None) -> list[DetectedSegment]:
        providers = self._providers()
        analysis = analyze_long_mix(normalized, item.metadata, job.options)
        excerpts_dir = normalized.parent / "segment-clips"
        excerpt_source = instrumental or normalized
        remaining_budget = job.options.max_provider_calls
        if (item.metadata.duration_ms or 0) < 25 * 60_000:
            remaining_budget = min(96, remaining_budget)
        repeat_matches: dict[str, TrackCandidate] = {}
        segments: list[DetectedSegment] = []
        for index, draft in enumerate(analysis.segments, start=1):
            if index % 12 == 0:
                self.db.add_event(job.id, "info", f"Processed {index}/{len(analysis.segments)} segmented regions for {item.id}")
            if draft.kind in {SegmentKind.SILENCE_OR_FX, SegmentKind.SPEECH_ONLY}:
                segments.append(self._draft_to_detected(item.id, draft))
                continue
            if draft.repeat_group_id and draft.repeat_group_id in repeat_matches:
                segments.append(self._candidate_to_detected(item.id, draft, repeat_matches[draft.repeat_group_id], reused=True))
                continue

            candidates: list[TrackCandidate] = []
            provider_attempts = 0
            probe_count = 0
            for probe in draft.probe_windows[: job.options.max_probes_per_segment]:
                if remaining_budget <= 0:
                    break
                probe_count += 1
                excerpt_path = build_excerpt_path(excerpts_dir, excerpt_source, probe.start_ms, probe.end_ms, f"segment-{probe.reason}")
                if not excerpt_path.exists():
                    create_excerpt(excerpt_source, probe.start_ms, probe.end_ms, excerpt_path)
                for provider in providers:
                    if remaining_budget <= 0:
                        break
                    state = provider.state()
                    if not state.available:
                        continue
                    provider_hits = self._recognize_with_cache(job.id, item, provider, excerpt_path, probe.start_ms, probe.end_ms)
                    provider_attempts += 1
                    remaining_budget -= 1
                    if provider_hits:
                        self.db.add_event(job.id, "info", f"{provider.name} matched {provider_hits[0].track.title} for segment {draft.start_ms}-{draft.end_ms}")
                    candidates.extend(provider_hits)
            draft.probe_count = probe_count
            draft.provider_attempts = provider_attempts
            draft.candidates = candidates
            best = self._pick_segment_candidate(draft)
            if best:
                if draft.repeat_group_id:
                    repeat_matches[draft.repeat_group_id] = best
                segments.append(self._candidate_to_detected(item.id, draft, best, reused=False))
            else:
                unresolved_kind = SegmentKind.SPEECH_ONLY if draft.speech_ratio >= 0.70 and draft.music_ratio < 0.35 else SegmentKind.MUSIC_UNRESOLVED
                draft.kind = unresolved_kind
                segments.append(self._draft_to_detected(item.id, draft))
        return self._stitch_segment_timeline(segments)

    def _pick_segment_candidate(self, draft: SegmentDraft) -> TrackCandidate | None:
        if not draft.candidates:
            return None
        counts = Counter(candidate.track.normalized_key() for candidate in draft.candidates)
        top_key, top_count = counts.most_common(1)[0]
        ranked = sorted(
            [candidate for candidate in draft.candidates if candidate.track.normalized_key() == top_key],
            key=lambda candidate: candidate.confidence,
            reverse=True,
        )
        best = ranked[0]
        if top_count >= 2:
            return best
        if best.confidence >= 0.70 and draft.music_ratio >= 0.45 and draft.kind != SegmentKind.SPEECH_ONLY:
            return best
        return None

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
        )

    def _stitch_segment_timeline(self, segments: list[DetectedSegment]) -> list[DetectedSegment]:
        if not segments:
            return []
        ordered = sorted(segments, key=lambda segment: (segment.start_ms, segment.end_ms, segment.kind.value))
        merged: list[DetectedSegment] = []
        for segment in ordered:
            if merged and self._can_merge_segments(merged[-1], segment):
                merged[-1] = self._merge_detected_segments(merged[-1], segment)
            else:
                merged.append(segment)

        stitched: list[DetectedSegment] = []
        for index, segment in enumerate(merged):
            if index < len(merged) - 1:
                next_segment = merged[index + 1]
                if segment.end_ms > next_segment.start_ms:
                    segment = segment.model_copy(update={"end_ms": max(segment.start_ms, next_segment.start_ms)})
            if segment.end_ms > segment.start_ms:
                stitched.append(segment)
        return stitched

    def _can_merge_segments(self, left: DetectedSegment, right: DetectedSegment) -> bool:
        if left.kind != right.kind:
            return False
        gap_ms = right.start_ms - left.end_ms
        if gap_ms > 3_000:
            return False
        if left.kind == SegmentKind.MATCHED_TRACK and left.track and right.track:
            return left.track.normalized_key() == right.track.normalized_key()
        return left.track is None and right.track is None

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
            }
        )

    def _select_windows(self, job: Job, item: SourceItem, normalized: Path, instrumental: Path | None, profile: SourceProfile) -> list[WindowPlan]:
        if profile.strategy == "long_mix":
            primary_path = instrumental if instrumental and profile.prefer_source_path == "instrumental" else normalized
            primary_label = "instrumental" if primary_path == instrumental and instrumental is not None else "mix"
            plans = metadata_windows(primary_path, item.metadata, duration_ms=profile.duration_ms, label=primary_label)
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
        cache_key = fingerprint_cache_key(excerpt_path)
        cached = self.db.get_provider_cache(cache_key, provider.name)
        if cached:
            payload = json.loads(cached)
            return [TrackCandidate.model_validate(candidate) for candidate in payload]
        try:
            self._throttle_provider(provider.name)
            provider_hits = provider.recognize(excerpt_path, start_ms, end_ms)
        except ProviderError as exc:
            self.db.add_event(job_id, "warning", f"{provider.name} failed on {item.id}: {exc}")
            return []
        except Exception as exc:
            self.db.add_event(job_id, "warning", f"{provider.name} crashed on {item.id}: {exc}")
            return []
        self.db.set_provider_cache(cache_key, provider.name, json.dumps([candidate.model_dump(mode="json") for candidate in provider_hits]))
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
        top_count = counts.most_common(1)[0][1]
        return top_count >= profile.stop_after_consensus

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
