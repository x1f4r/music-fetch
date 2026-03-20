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


class ItemStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


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
        artist = (self.artist or "").strip().lower()
        title = self.title.strip().lower()
        return f"{artist}::{title}"


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
