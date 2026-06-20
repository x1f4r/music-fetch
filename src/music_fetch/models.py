from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator


PROVIDER_ATTEMPT_OUTCOMES = {
    "cache_hit_matched",
    "cache_hit_empty",
    "provider_call_matched",
    "provider_call_empty",
    "provider_error",
    "provider_exception",
    "budget_exhausted",
}
PROVIDER_DECISION_OUTCOMES = {
    "provider_unavailable",
    "prefer_free_skip",
    "budget_exhausted",
}


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
    window_ms: int = Field(default=12_000, gt=0)
    hop_ms: int = Field(default=6_000, gt=0)
    max_windows: int = Field(default=24, gt=0)
    max_segments: int = Field(default=360, gt=0)
    max_probes_per_segment: int = Field(default=3, gt=0)
    max_provider_calls: int = Field(default=420, ge=0)
    min_provider_consensus: int = Field(default=1, gt=0)
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
    merge_gap_same_track_ms: int = Field(default=12_000, ge=0)
    # Gap allowance for *bridging* a short non-MATCHED segment (speech/silence)
    # between two MATCHED segments of the same identity.
    merge_gap_bridge_ms: int = Field(default=8_000, ge=0)
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
    segment_workers: int = Field(default=0, ge=0, le=32)


class JobCreate(BaseModel):
    inputs: list[str] = Field(min_length=1)
    options: JobOptions = Field(default_factory=JobOptions)

    @field_validator("inputs")
    @classmethod
    def inputs_must_not_be_blank(cls, values: list[str]) -> list[str]:
        cleaned: list[str] = []
        for value in values:
            item = str(value).strip()
            if not item:
                raise ValueError("inputs must not be blank")
            cleaned.append(item)
        return cleaned


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

    @model_validator(mode="after")
    def validate_ledger_payload(self) -> "RecognitionMetric":
        metric_type = self.payload.get("metric_type")
        if metric_type is None:
            outcome = self.payload.get("outcome")
            if outcome is None:
                return self
            if outcome in PROVIDER_DECISION_OUTCOMES and (
                outcome != "budget_exhausted" or "cache_key" not in self.payload
            ):
                self.payload["metric_type"] = "provider_decision"
            elif outcome in PROVIDER_ATTEMPT_OUTCOMES:
                self.payload["metric_type"] = "provider_attempt"
            else:
                raise ValueError(f"Unknown recognition outcome without metric_type: {outcome}")
            metric_type = self.payload["metric_type"]
        if metric_type == "provider_attempt":
            self._validate_provider_attempt_payload()
            return self
        if metric_type == "provider_decision":
            self._validate_provider_decision_payload()
            return self
        if metric_type == "item_summary":
            self._validate_item_summary_payload()
            return self
        raise ValueError(f"Unknown recognition metric_type: {metric_type}")

    def _validate_provider_attempt_payload(self) -> None:
        required = {
            "ledger_version",
            "outcome",
            "start_ms",
            "end_ms",
            "probe_start_ms",
            "probe_end_ms",
            "cache_key",
            "cache_hit",
            "provider_call_attempted",
            "budget_consumed",
            "budget_exhausted",
        }
        self._require_payload_keys(required)
        outcome = str(self.payload["outcome"])
        if outcome not in PROVIDER_ATTEMPT_OUTCOMES:
            raise ValueError(f"Unknown provider_attempt outcome: {outcome}")
        self._require_bool("cache_hit")
        self._require_bool("provider_call_attempted")
        self._require_bool("budget_exhausted")
        self._require_int("budget_consumed")
        if outcome.startswith("cache_hit"):
            if not self.cache_hit or self.call_count != 0 or self.payload["provider_call_attempted"]:
                raise ValueError("cache-hit provider_attempt metrics must not count provider calls")
            if self.payload["budget_exhausted"] or self.payload["budget_consumed"] != 0:
                raise ValueError("cache-hit provider_attempt metrics must not consume budget")
            if outcome == "cache_hit_matched" and not self.matched:
                raise ValueError("cache_hit_matched metrics must be marked matched")
            if outcome == "cache_hit_empty" and self.matched:
                raise ValueError("cache_hit_empty metrics must not be marked matched")
        elif outcome in {"provider_call_matched", "provider_call_empty", "provider_error", "provider_exception"}:
            if (
                self.cache_hit
                or self.payload["cache_hit"]
                or self.payload["budget_exhausted"]
                or not self.payload["provider_call_attempted"]
                or self.call_count < 1
            ):
                raise ValueError("provider-call metrics must mark an attempted provider call")
            if "budget_remaining_before" in self.payload and self.payload["budget_consumed"] < 1:
                raise ValueError("budgeted provider-call metrics must consume budget")
            if outcome == "provider_call_matched" and not self.matched:
                raise ValueError("provider_call_matched metrics must be marked matched")
            if outcome in {"provider_call_empty", "provider_error", "provider_exception"} and self.matched:
                raise ValueError(f"{outcome} metrics must not be marked matched")
        elif outcome == "budget_exhausted":
            if (
                self.cache_hit
                or self.payload["cache_hit"]
                or self.payload["provider_call_attempted"]
                or self.payload["budget_consumed"] != 0
                or self.call_count != 0
                or not self.payload["budget_exhausted"]
            ):
                raise ValueError("budget_exhausted metrics must be zero-call budget skips")
            self._require_payload_keys({"skip_reason"})
        if outcome in {"provider_error", "provider_exception"}:
            self._require_payload_keys({"error_type", "error_message"})

    def _validate_provider_decision_payload(self) -> None:
        required = {
            "ledger_version",
            "outcome",
            "start_ms",
            "end_ms",
            "probe_start_ms",
            "probe_end_ms",
            "cache_hit",
            "provider_call_attempted",
            "budget_consumed",
            "budget_exhausted",
            "skip_reason",
        }
        self._require_payload_keys(required)
        outcome = str(self.payload["outcome"])
        if outcome not in PROVIDER_DECISION_OUTCOMES:
            raise ValueError(f"Unknown provider_decision outcome: {outcome}")
        self._require_bool("cache_hit")
        self._require_bool("provider_call_attempted")
        self._require_bool("budget_exhausted")
        self._require_int("budget_consumed")
        if self.cache_hit or self.payload["cache_hit"] or self.call_count != 0 or self.payload["provider_call_attempted"]:
            raise ValueError("provider_decision metrics must not count provider calls")
        if self.payload["budget_consumed"] != 0:
            raise ValueError("provider_decision metrics must not consume budget")
        if outcome == "budget_exhausted":
            if not self.payload["budget_exhausted"]:
                raise ValueError("budget_exhausted provider_decision metrics must mark budget_exhausted")
        elif self.payload["budget_exhausted"]:
            raise ValueError(f"{outcome} provider_decision metrics must not mark budget_exhausted")

    def _validate_item_summary_payload(self) -> None:
        self._require_payload_keys({"metric_type", "outcome", "segment_count"})
        if self.payload["outcome"] != "item_summary":
            raise ValueError("item_summary metrics must use outcome=item_summary")
        self._require_int("segment_count")
        if self.payload["segment_count"] < 0:
            raise ValueError("item_summary segment_count must be >= 0")
        if self.source_item_id is None:
            raise ValueError("item_summary metrics must be tied to a source item")
        if self.provider_name is not None or self.cache_hit or self.matched or self.call_count != 0:
            raise ValueError("item_summary metrics must not be provider-specific or count provider calls")

    def _require_payload_keys(self, keys: set[str]) -> None:
        missing = sorted(key for key in keys if key not in self.payload)
        if missing:
            raise ValueError(f"RecognitionMetric payload missing keys: {', '.join(missing)}")

    def _require_bool(self, key: str) -> None:
        if not isinstance(self.payload.get(key), bool):
            raise ValueError(f"RecognitionMetric payload key must be bool: {key}")

    def _require_int(self, key: str) -> None:
        value = self.payload.get(key)
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError(f"RecognitionMetric payload key must be int: {key}")


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
