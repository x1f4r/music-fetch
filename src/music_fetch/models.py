from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class JobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    PARTIAL_FAILED = "partial_failed"
    FAILED = "failed"
    CANCELED = "canceled"


class ItemStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"


class ProviderName(StrEnum):
    VIBRA = "vibra"
    AUDD = "audd"
    ACRCLOUD = "acrcloud"
    LOCAL_CATALOG = "local_catalog"


class SourceKind(StrEnum):
    LOCAL_FILE = "local_file"
    DIRECT_HTTP = "direct_http"
    YT_DLP = "yt_dlp"


class AnalysisMode(StrEnum):
    AUTO = "auto"
    SINGLE_TRACK = "single_track"
    PLAYLIST_ENTRY = "playlist_entry"
    LONG_MIX = "long_mix"


class RecallProfile(StrEnum):
    MAX_RECALL = "max_recall"
    BALANCED = "balanced"
    FAST_FIRST = "fast_first"


class SegmentKind(StrEnum):
    MATCHED_TRACK = "matched_track"
    SPEECH_ONLY = "speech_only"
    MUSIC_UNRESOLVED = "music_unresolved"
    SILENCE_OR_FX = "silence_or_fx"


class ArtifactCategory(StrEnum):
    DOWNLOAD = "download"
    NORMALIZED = "normalized"
    STEM = "stem"
    EXCERPT = "excerpt"
    RECORDING = "recording"
    UPLOAD = "upload"
    MODEL = "model"
    SUPPORT = "support"


class JobOptions(BaseModel):
    prefer_separation: bool = True
    window_ms: int = 12_000
    hop_ms: int = 6_000
    max_windows: int = 24
    max_segments: int = 360
    max_probes_per_segment: int = 3
    max_provider_calls: int = 420
    min_provider_consensus: int = 1
    analysis_mode: AnalysisMode = AnalysisMode.AUTO
    recall_profile: RecallProfile = RecallProfile.MAX_RECALL
    enable_metadata_hints: bool = True
    enable_repeat_detection: bool = True
    provider_order: list[ProviderName] = Field(
        default_factory=lambda: [
            ProviderName.LOCAL_CATALOG,
            ProviderName.VIBRA,
            ProviderName.AUDD,
            ProviderName.ACRCLOUD,
        ]
    )
    # --- Reliability overhaul knobs --------------------------------------------
    # Default gap allowance when two *same-identity* matched segments are adjacent
    # (see duration-adaptive scaling in service._can_merge_segments). Floor/ceiling
    # for the duration-adaptive rule — the actual gap scales with the longer of
    # the two segments.
    merge_gap_same_track_ms: int = 12_000
    # Gap allowance for *bridging* a short non-MATCHED segment (speech/silence)
    # between two MATCHED segments of the same identity.
    merge_gap_bridge_ms: int = 8_000
    # When True, providers with no configured credentials (free only: VIBRA +
    # LOCAL_CATALOG) run with an effectively uncapped call budget; paid providers
    # honor ``max_provider_calls``. See service._effective_budget.
    budget_autoscale: bool = True
    # Skip paid providers for a segment as soon as a free provider has already
    # returned a strong match (score >= 0.80). Paid providers still run as a
    # fallback when free ones return nothing.
    prefer_free_providers: bool = True
    # After the initial parallel pass, the long-mix path reruns unresolved
    # segments through ``_retry_segment``. Disable to match pre-overhaul behavior.
    auto_retry_unresolved: bool = True
    # Parallel workers used per long-mix job when probing segments. ``0`` defers
    # to the default computed from ``settings.max_workers``.
    segment_workers: int = 0


class JobCreate(BaseModel):
    inputs: list[str]
    options: JobOptions = Field(default_factory=JobOptions)


class TrackMatch(BaseModel):
    title: str
    artist: str | None = None
    album: str | None = None
    isrc: str | None = None
    provider_ids: dict[str, str] = Field(default_factory=dict)
    external_links: dict[str, str] = Field(default_factory=dict)
    raw: dict[str, Any] = Field(default_factory=dict)

    def normalized_key(self) -> str:
        """Tiered identity used throughout the pipeline for merging/deduping.

        Prefers ISRC (tier A), then a provider-native id (tier B), then a
        fuzzy artist+title key (tier C). See ``music_fetch.identity`` for the
        exact logic and rationale.
        """
        from .identity import tiered_identity

        return tiered_identity(self.isrc, self.provider_ids, self.artist, self.title)[1]

    def identity_tier(self) -> str:
        """Return the tier label ("isrc" | "provider_id" | "fuzzy") for this track."""
        from .identity import tiered_identity

        return tiered_identity(self.isrc, self.provider_ids, self.artist, self.title)[0]

    def merges_with(self, other: "TrackMatch") -> bool:
        """Return True if ``self`` and ``other`` describe the same song.

        Uses ISRC veto (two distinct ISRCs are never merged even when fuzzy
        keys collide) and otherwise tiered-identity equality.
        """
        from .identity import merges_with

        return merges_with(
            self.isrc,
            self.provider_ids,
            self.artist,
            self.title,
            other.isrc,
            other.provider_ids,
            other.artist,
            other.title,
        )


class TrackCandidate(BaseModel):
    track: TrackMatch
    provider: ProviderName
    confidence: float = 0.0
    start_ms: int
    end_ms: int
    evidence: list[str] = Field(default_factory=list)
    raw: dict[str, Any] = Field(default_factory=dict)


class DetectedSegment(BaseModel):
    source_item_id: str
    start_ms: int
    end_ms: int
    kind: SegmentKind = SegmentKind.MATCHED_TRACK
    confidence: float
    providers: list[ProviderName]
    evidence_count: int
    track: TrackMatch | None = None
    alternates: list[TrackMatch] = Field(default_factory=list)
    repeat_group_id: str | None = None
    probe_count: int = 0
    provider_attempts: int = 0
    metadata_hints: list[str] = Field(default_factory=list)
    uncertainty: float | None = None
    explanation: list[str] = Field(default_factory=list)
    # Observability: the identity key that produced this segment and the
    # acceptance-gate label that promoted it from candidates → MATCHED_TRACK.
    # Populated lazily; older rows keep these as None.
    identity_key: str | None = None
    acceptance_gate: str | None = None


class SourceMetadata(BaseModel):
    title: str | None = None
    extractor: str | None = None
    webpage_url: str | None = None
    channel: str | None = None
    uploader: str | None = None
    duration_ms: int | None = None
    chapters: list[dict[str, Any]] = Field(default_factory=list)
    description: str | None = None
    playlist_id: str | None = None
    playlist_title: str | None = None
    entry_index: int | None = None
    extra: dict[str, Any] = Field(default_factory=dict)


class SourceItem(BaseModel):
    id: str
    job_id: str
    input_value: str
    kind: SourceKind
    status: ItemStatus = ItemStatus.QUEUED
    metadata: SourceMetadata = Field(default_factory=SourceMetadata)
    local_path: str | None = None
    download_url: str | None = None
    normalized_path: str | None = None
    instrumental_path: str | None = None
    error: str | None = None


class Job(BaseModel):
    id: str
    status: JobStatus
    created_at: str
    updated_at: str
    options: JobOptions
    inputs: list[str]
    error: str | None = None
    cancel_requested: bool = False


class ProviderConfig(BaseModel):
    enabled: bool = True
    config: dict[str, Any] = Field(default_factory=dict)


class ProviderState(BaseModel):
    name: ProviderName
    enabled: bool
    available: bool
    reason: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)


class JobEvent(BaseModel):
    id: int
    job_id: str
    level: str
    message: str
    created_at: str


class WindowPlan(BaseModel):
    start_ms: int
    end_ms: int
    score: float
    source_path: str
    label: str


class ArtifactEntry(BaseModel):
    id: str
    category: ArtifactCategory
    label: str
    path: str
    size_bytes: int
    exists: bool
    temporary: bool = True
    job_id: str | None = None
    source_item_id: str | None = None
    pinned: bool = False


class RecognitionMetric(BaseModel):
    id: str
    job_id: str
    source_item_id: str | None = None
    provider_name: ProviderName | None = None
    cache_hit: bool = False
    matched: bool = False
    call_count: int = 0
    matched_segments: int = 0
    unresolved_segments: int = 0
    elapsed_ms: int = 0
    # Reliability-overhaul observability. Stored in ``payload`` to avoid a
    # schema change on ``recognition_metrics``; extracted here for ergonomics.
    # See ``service._record_item_summary_metric`` for how these are populated.
    segments_merged: int = 0
    segments_bridged_across_speech: int = 0
    repeat_group_reconfirmed: int = 0
    repeat_group_rejected: int = 0
    gate_g1_hits: int = 0
    gate_g2_hits: int = 0
    gate_g3_hits: int = 0
    gate_g4_hits: int = 0
    gate_g5_hits: int = 0
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: str


class DiscoveryState(BaseModel):
    job_id: str
    input_value: str
    cursor: int = 0
    total: int | None = None
    completed: bool = False
    payload: dict[str, Any] = Field(default_factory=dict)
    updated_at: str


class ArtifactCategorySummary(BaseModel):
    category: ArtifactCategory
    count: int
    size_bytes: int


class StorageSummary(BaseModel):
    job_id: str | None = None
    auto_clean: bool
    total_size_bytes: int
    categories: list[ArtifactCategorySummary] = Field(default_factory=list)
    entries: list[ArtifactEntry] = Field(default_factory=list)
    locations: dict[str, str] = Field(default_factory=dict)


class LibraryEntry(BaseModel):
    job_id: str
    title: str
    input_value: str
    status: JobStatus
    created_at: str
    updated_at: str
    item_count: int
    segment_count: int
    matched_count: int
    pinned: bool = False
    artifact_size_bytes: int = 0


class EvaluationCase(BaseModel):
    id: str
    input_value: str
    expected_tracks: list[str] = Field(default_factory=list)
    max_runtime_ms: int | None = None
    notes: str | None = None


class EvaluationCaseResult(BaseModel):
    case_id: str
    job_id: str
    status: JobStatus
    runtime_ms: int
    provider_calls: int
    cache_hits: int
    matched_segments: int
    unresolved_segments: int
    precision: float
    recall: float
    expected_tracks: list[str] = Field(default_factory=list)
    actual_tracks: list[str] = Field(default_factory=list)


class EvaluationReport(BaseModel):
    manifest_path: str
    created_at: str
    case_results: list[EvaluationCaseResult] = Field(default_factory=list)
    summary: dict[str, float] = Field(default_factory=dict)
