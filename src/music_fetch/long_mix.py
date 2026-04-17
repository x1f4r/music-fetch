from __future__ import annotations

import math
import re
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

from .media import description_starts, probe_duration_ms
from .models import JobOptions, RecallProfile, SegmentKind, SourceMetadata, TrackCandidate
from .utils import sha1_text


URL_RE = re.compile(r"https?://\S+")


@dataclass
class FeatureFrame:
    start_ms: int
    end_ms: int
    feature_vector: np.ndarray
    chroma_vector: np.ndarray
    music_score: float
    speech_score: float
    no_music_score: float
    label: SegmentKind


@dataclass
class ProbeWindow:
    start_ms: int
    end_ms: int
    reason: str


@dataclass
class SegmentDraft:
    start_ms: int
    end_ms: int
    kind: SegmentKind
    feature_vector: np.ndarray
    chroma_vector: np.ndarray
    music_ratio: float
    speech_ratio: float
    metadata_hints: list[str] = field(default_factory=list)
    probe_windows: list[ProbeWindow] = field(default_factory=list)
    repeat_group_id: str | None = None
    candidates: list[TrackCandidate] = field(default_factory=list)
    probe_count: int = 0
    provider_attempts: int = 0


@dataclass
class LongMixAnalysis:
    frames: list[FeatureFrame]
    segments: list[SegmentDraft]


@dataclass(frozen=True)
class SegmentAnalysisParameters:
    hop_seconds: float
    chunk_seconds: int
    context_seconds: float
    min_segment_ms: int
    max_segment_ms: int
    novelty_percentile: float


def analyze_long_mix(source_path: Path, metadata: SourceMetadata, options: JobOptions) -> LongMixAnalysis:
    duration_ms = probe_duration_ms(source_path)
    params = analysis_parameters(duration_ms, options.recall_profile)
    frames = extract_feature_frames(source_path, recall_profile=options.recall_profile, params=params, duration_ms=duration_ms)
    segments = segment_frames(frames, metadata, options, params=params)
    assign_repeat_groups(segments, enabled=options.enable_repeat_detection)
    return LongMixAnalysis(frames=frames, segments=segments)


def analysis_parameters(duration_ms: int, recall_profile: RecallProfile) -> SegmentAnalysisParameters:
    duration_seconds = duration_ms / 1000
    if duration_seconds <= 45:
        return SegmentAnalysisParameters(
            hop_seconds=0.5 if recall_profile is RecallProfile.MAX_RECALL else 0.75,
            chunk_seconds=90,
            context_seconds=4.0,
            min_segment_ms=4_000,
            max_segment_ms=45_000,
            novelty_percentile=68.0,
        )
    if duration_seconds <= 180:
        return SegmentAnalysisParameters(
            hop_seconds=0.75 if recall_profile is RecallProfile.MAX_RECALL else 1.0,
            chunk_seconds=180,
            context_seconds=5.0,
            min_segment_ms=6_000,
            max_segment_ms=60_000,
            novelty_percentile=74.0,
        )
    if duration_seconds <= 1_200:
        return SegmentAnalysisParameters(
            hop_seconds=1.0 if recall_profile is RecallProfile.MAX_RECALL else 1.5,
            chunk_seconds=300,
            context_seconds=6.0,
            min_segment_ms=8_000,
            max_segment_ms=3 * 60_000,
            novelty_percentile=78.0,
        )
    if recall_profile is RecallProfile.MAX_RECALL:
        hop_seconds = 1.0
        chunk_seconds = 600
    elif recall_profile is RecallProfile.BALANCED:
        hop_seconds = 2.0
        chunk_seconds = 900
    else:
        hop_seconds = 3.0
        chunk_seconds = 900
    return SegmentAnalysisParameters(
        hop_seconds=hop_seconds,
        chunk_seconds=chunk_seconds,
        context_seconds=8.0,
        min_segment_ms=20_000,
        max_segment_ms=6 * 60_000,
        novelty_percentile=82.0,
    )


def extract_feature_frames(
    source_path: Path,
    *,
    recall_profile: RecallProfile,
    params: SegmentAnalysisParameters | None = None,
    duration_ms: int | None = None,
) -> list[FeatureFrame]:
    try:
        import librosa
        import soundfile as sf
    except ImportError as exc:
        raise RuntimeError("Long-mix analysis requires librosa and soundfile") from exc

    if params is None:
        duration_ms = duration_ms if duration_ms is not None else probe_duration_ms(source_path)
        params = analysis_parameters(duration_ms, recall_profile)

    frames: list[FeatureFrame] = []
    seen_starts: set[int] = set()
    overlap_seconds = params.context_seconds
    with sf.SoundFile(str(source_path)) as handle:
        sr = handle.samplerate
        total_frames = len(handle)
        duration_ms = int(total_frames / sr * 1000)
        chunk_frames = max(sr, int(params.chunk_seconds * sr))
        overlap_frames = max(0, int(round(overlap_seconds * sr)))
        hop_length = max(1, int(sr * params.hop_seconds))
        offset = 0
        while offset < total_frames:
            handle.seek(offset)
            block = handle.read(chunk_frames + overlap_frames, dtype="float32", always_2d=False)
            if block.size == 0:
                break
            if block.ndim > 1:
                block = block.mean(axis=1)
            block = np.asarray(block, dtype=np.float32)
            block_frames = compute_frame_features(
                block,
                sr=sr,
                hop_length=hop_length,
                hop_seconds=params.hop_seconds,
                context_seconds=params.context_seconds,
            )
            for index, frame in enumerate(block_frames):
                start_ms = int(offset / sr * 1000) + int(index * params.hop_seconds * 1000)
                if start_ms in seen_starts or start_ms >= duration_ms:
                    continue
                seen_starts.add(start_ms)
                frames.append(
                    FeatureFrame(
                        start_ms=start_ms,
                        end_ms=min(duration_ms, start_ms + int(params.context_seconds * 1000)),
                        feature_vector=frame["feature_vector"],
                        chroma_vector=frame["chroma_vector"],
                        music_score=frame["music_score"],
                        speech_score=frame["speech_score"],
                        no_music_score=frame["no_music_score"],
                        label=frame["label"],
                    )
                )
            if offset + chunk_frames >= total_frames:
                break
            offset += chunk_frames
            if offset >= overlap_frames:
                offset -= overlap_frames
    return frames


def compute_frame_features(block: np.ndarray, *, sr: int, hop_length: int, hop_seconds: float, context_seconds: float) -> list[dict]:
    import librosa

    if block.size == 0:
        return []
    chroma = librosa.feature.chroma_cens(y=block, sr=sr, hop_length=hop_length).T
    mfcc = librosa.feature.mfcc(y=block, sr=sr, n_mfcc=13, hop_length=hop_length).T
    contrast = librosa.feature.spectral_contrast(y=block, sr=sr, hop_length=hop_length).T
    rms = librosa.feature.rms(y=block, hop_length=hop_length).T.squeeze(-1)
    flatness = librosa.feature.spectral_flatness(y=block, hop_length=hop_length).T.squeeze(-1)
    rolloff = librosa.feature.spectral_rolloff(y=block, sr=sr, hop_length=hop_length).T.squeeze(-1)
    zcr = librosa.feature.zero_crossing_rate(y=block, hop_length=hop_length).T.squeeze(-1)
    onset = librosa.onset.onset_strength(y=block, sr=sr, hop_length=hop_length)

    limit = min(len(chroma), len(mfcc), len(contrast), len(rms), len(flatness), len(rolloff), len(zcr), len(onset))
    if limit == 0:
        return []
    context_frames = max(1, int(round(context_seconds / max(hop_seconds, 0.25))))
    chroma = smooth_matrix(chroma[:limit], context=context_frames)
    mfcc = smooth_matrix(mfcc[:limit], context=context_frames)
    contrast = smooth_matrix(contrast[:limit], context=context_frames)
    rms = smooth_vector(rms[:limit], context=context_frames)
    flatness = smooth_vector(flatness[:limit], context=context_frames)
    rolloff = smooth_vector(rolloff[:limit], context=context_frames)
    zcr = smooth_vector(zcr[:limit], context=context_frames)
    onset = smooth_vector(onset[:limit], context=context_frames)

    chroma_strength = normalize(np.linalg.norm(chroma, axis=1))
    contrast_strength = normalize(np.mean(contrast, axis=1))
    rms_norm = normalize(rms)
    low_flatness = 1.0 - normalize(flatness)
    rolloff_norm = normalize(rolloff)
    zcr_norm = normalize(zcr)
    onset_norm = normalize(onset)
    voice_band_ratio = np.clip(rolloff_norm * 0.7 + zcr_norm * 0.3, 0.0, 1.0)

    rows: list[dict] = []
    for index in range(limit):
        music_score = float(
            np.clip(
                0.34 * chroma_strength[index]
                + 0.20 * onset_norm[index]
                + 0.16 * low_flatness[index]
                + 0.18 * contrast_strength[index]
                + 0.12 * rms_norm[index],
                0.0,
                1.0,
            )
        )
        speech_score = float(
            np.clip(
                0.34 * voice_band_ratio[index]
                + 0.22 * zcr_norm[index]
                + 0.18 * rms_norm[index]
                + 0.26 * (1.0 - chroma_strength[index]),
                0.0,
                1.0,
            )
        )
        no_music_score = float(np.clip((1.0 - rms_norm[index]) * 0.65 + (1.0 - low_flatness[index]) * 0.35, 0.0, 1.0))
        label = classify_label(music_score, speech_score, no_music_score, rms_norm[index], chroma_strength[index])
        rows.append(
            {
                "feature_vector": np.concatenate(
                    [
                        chroma[index],
                        mfcc[index],
                        contrast[index],
                        np.array([rms_norm[index], onset_norm[index], low_flatness[index], zcr_norm[index], voice_band_ratio[index]], dtype=np.float32),
                    ]
                ).astype(np.float32),
                "chroma_vector": chroma[index].astype(np.float32),
                "music_score": music_score,
                "speech_score": speech_score,
                "no_music_score": no_music_score,
                "label": label,
            }
        )
    return rows


def segment_frames(frames: list[FeatureFrame], metadata: SourceMetadata, options: JobOptions, *, params: SegmentAnalysisParameters) -> list[SegmentDraft]:
    if not frames:
        return []
    boundary_indices = {0, len(frames)}
    boundary_indices.update(metadata_boundary_indices(frames, metadata) if options.enable_metadata_hints else set())
    novelty = novelty_scores(frames)
    threshold = np.percentile(novelty, params.novelty_percentile) if len(novelty) > 4 else 0.0
    for index in local_maxima(novelty):
        if novelty[index] >= threshold:
            boundary_indices.add(index + 1)
    boundaries = sorted(boundary_indices)
    segments = []
    for start_index, end_index in zip(boundaries, boundaries[1:]):
        if end_index <= start_index:
            continue
        segments.append(build_segment(frames[start_index:end_index], metadata))
    segments = merge_short_segments(segments, min_length_ms=params.min_segment_ms)
    segments = split_long_segments(segments, max_length_ms=params.max_segment_ms)
    if len(segments) > options.max_segments:
        segments = prioritize_segments(segments, options.max_segments)
    for segment in segments:
        segment.probe_windows = choose_probe_windows(segment, options.max_probes_per_segment)
    return segments


def build_segment(frames: list[FeatureFrame], metadata: SourceMetadata) -> SegmentDraft:
    start_ms = frames[0].start_ms
    end_ms = frames[-1].end_ms
    labels = [frame.label for frame in frames]
    music_ratio = sum(1 for label in labels if label in {SegmentKind.MATCHED_TRACK, SegmentKind.MUSIC_UNRESOLVED}) / max(1, len(labels))
    speech_ratio = sum(1 for label in labels if label == SegmentKind.SPEECH_ONLY) / max(1, len(labels))
    if all(label == SegmentKind.SILENCE_OR_FX for label in labels):
        kind = SegmentKind.SILENCE_OR_FX
    elif speech_ratio >= 0.65 and music_ratio < 0.35:
        kind = SegmentKind.SPEECH_ONLY
    else:
        kind = SegmentKind.MUSIC_UNRESOLVED
    hints = metadata_hint_texts(metadata, start_ms, end_ms)
    return SegmentDraft(
        start_ms=start_ms,
        end_ms=end_ms,
        kind=kind,
        feature_vector=np.mean(np.stack([frame.feature_vector for frame in frames]), axis=0),
        chroma_vector=np.mean(np.stack([frame.chroma_vector for frame in frames]), axis=0),
        music_ratio=music_ratio,
        speech_ratio=speech_ratio,
        metadata_hints=hints,
    )


def merge_short_segments(segments: list[SegmentDraft], *, min_length_ms: int) -> list[SegmentDraft]:
    if not segments:
        return []
    merged: list[SegmentDraft] = [segments[0]]
    for segment in segments[1:]:
        previous = merged[-1]
        preserve_boundary = (
            previous.kind in {SegmentKind.SILENCE_OR_FX, SegmentKind.SPEECH_ONLY}
            or segment.kind in {SegmentKind.SILENCE_OR_FX, SegmentKind.SPEECH_ONLY}
        )
        if not preserve_boundary and (
            previous.end_ms - previous.start_ms < min_length_ms or segment.end_ms - segment.start_ms < min_length_ms
        ):
            merged[-1] = combine_segments(previous, segment)
        else:
            merged.append(segment)
    return merged


def split_long_segments(segments: list[SegmentDraft], *, max_length_ms: int) -> list[SegmentDraft]:
    split_segments: list[SegmentDraft] = []
    for segment in segments:
        duration_ms = segment.end_ms - segment.start_ms
        if duration_ms <= max_length_ms:
            split_segments.append(segment)
            continue
        parts = max(2, math.ceil(duration_ms / max_length_ms))
        part_duration = duration_ms // parts
        for index in range(parts):
            start_ms = segment.start_ms + index * part_duration
            end_ms = segment.end_ms if index == parts - 1 else min(segment.end_ms, start_ms + part_duration)
            split_segments.append(
                SegmentDraft(
                    start_ms=start_ms,
                    end_ms=end_ms,
                    kind=segment.kind,
                    feature_vector=segment.feature_vector.copy(),
                    chroma_vector=segment.chroma_vector.copy(),
                    music_ratio=segment.music_ratio,
                    speech_ratio=segment.speech_ratio,
                    metadata_hints=list(segment.metadata_hints),
                )
            )
    return split_segments


def prioritize_segments(segments: list[SegmentDraft], max_segments: int) -> list[SegmentDraft]:
    important = [segment for segment in segments if segment.kind == SegmentKind.MUSIC_UNRESOLVED]
    less_important = [segment for segment in segments if segment.kind != SegmentKind.MUSIC_UNRESOLVED]
    chosen = important[:max_segments]
    if len(chosen) < max_segments:
        chosen.extend(less_important[: max_segments - len(chosen)])
    chosen.sort(key=lambda segment: segment.start_ms)
    return chosen


def choose_probe_windows(segment: SegmentDraft, max_probes: int) -> list[ProbeWindow]:
    if segment.kind in {SegmentKind.SPEECH_ONLY, SegmentKind.SILENCE_OR_FX}:
        return []
    duration_ms = segment.end_ms - segment.start_ms
    if duration_ms <= 12_000:
        return [ProbeWindow(start_ms=segment.start_ms, end_ms=segment.end_ms, reason="segment")]
    probe_duration_ms = min(12_000, max(7_000, int(duration_ms * 0.45)))
    anchors = [0.18, 0.5, 0.82][:max_probes]
    probes: list[ProbeWindow] = []
    for anchor in anchors:
        center = segment.start_ms + int(duration_ms * anchor)
        half_probe = probe_duration_ms // 2
        start_ms = min(max(segment.start_ms, center - half_probe), max(segment.start_ms, segment.end_ms - probe_duration_ms))
        probes.append(
            ProbeWindow(
                start_ms=start_ms,
                end_ms=min(segment.end_ms, start_ms + probe_duration_ms),
                reason=f"anchor:{anchor:.2f}",
            )
        )
    deduped: list[ProbeWindow] = []
    seen: set[tuple[int, int]] = set()
    for probe in probes:
        key = (probe.start_ms, probe.end_ms)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(probe)
    return deduped


def assign_repeat_groups(segments: list[SegmentDraft], *, enabled: bool) -> None:
    """Group audio-similar segments under a single ``repeat_group_id`` so a
    later probe can reuse the first match (T2.1).

    Changes vs the previous implementation:

    - Similarity thresholds relaxed (``feature >= 0.88``, ``chroma >= 0.82``,
      ``duration_ratio >= 0.35``) so that real repeats of the same song
      across different mix positions actually get grouped.
    - Added a temporal gate: a repeat group keeps the *position* of each
      member so we can tell "same song, 40 min later" from "same song,
      adjacent window". Without this the group's representative was always
      the first occurrence and matches leaked forward indefinitely.
    """
    if not enabled:
        return
    # Track (group_id, rep_feature_vec, rep_chroma_vec, rep_duration_ms,
    #        list_of_member_midpoint_ms) so we can require a temporal
    # constraint on joining.
    representatives: list[tuple[str, np.ndarray, np.ndarray, int, list[int]]] = []
    musical_durations = [
        segment.end_ms - segment.start_ms
        for segment in segments
        if segment.kind in {SegmentKind.MUSIC_UNRESOLVED, SegmentKind.MATCHED_TRACK}
    ]
    median_segment_ms = int(sorted(musical_durations)[len(musical_durations) // 2]) if musical_durations else 30_000
    # Two members join the same group only if they're within this window OR
    # their audio similarity is overwhelming (>= 0.96). The 4x-median heuristic
    # means short-SFX clips stay separate even at high similarity.
    temporal_window_ms = max(120_000, 4 * median_segment_ms)
    for segment in segments:
        if segment.kind not in {SegmentKind.MUSIC_UNRESOLVED, SegmentKind.MATCHED_TRACK}:
            continue
        segment_duration = max(1, segment.end_ms - segment.start_ms)
        segment_midpoint = segment.start_ms + segment_duration // 2
        assigned = None
        for group_id, feature_vector, chroma_vector, duration_ms, midpoints in representatives:
            feature_similarity = cosine_similarity(segment.feature_vector, feature_vector)
            chroma_similarity = cosine_similarity(segment.chroma_vector, chroma_vector)
            duration_ratio = min(segment_duration, duration_ms) / max(segment_duration, duration_ms)
            if feature_similarity < 0.88 or chroma_similarity < 0.82 or duration_ratio < 0.35:
                continue
            # Temporal gate: skip unless there's a plausible nearby member, OR
            # the similarity is extreme (same-second-of-audio-twice case).
            nearest_distance = min(abs(segment_midpoint - existing) for existing in midpoints)
            if nearest_distance > temporal_window_ms and feature_similarity < 0.96:
                continue
            assigned = group_id
            midpoints.append(segment_midpoint)
            break
        if assigned is None:
            assigned = sha1_text(f"{segment.start_ms}:{segment.end_ms}:{uuid.uuid4().hex}")[:12]
            representatives.append(
                (assigned, segment.feature_vector, segment.chroma_vector, segment_duration, [segment_midpoint])
            )
        segment.repeat_group_id = assigned


def metadata_boundary_indices(frames: list[FeatureFrame], metadata: SourceMetadata) -> set[int]:
    starts = set(description_starts(metadata.description))
    for chapter in metadata.chapters:
        start = chapter.get("start_time")
        if start is not None:
            starts.add(int(float(start) * 1000))
    if not starts:
        return set()
    boundary_indices: set[int] = set()
    for start_ms in starts:
        index = min(range(len(frames)), key=lambda candidate: abs(frames[candidate].start_ms - start_ms))
        boundary_indices.add(index)
    return boundary_indices


def metadata_hint_texts(metadata: SourceMetadata, start_ms: int, end_ms: int) -> list[str]:
    hints: list[str] = []
    for chapter in metadata.chapters:
        chapter_start_ms = int(float(chapter.get("start_time", 0.0)) * 1000)
        chapter_end_ms = int(float(chapter.get("end_time", chapter_start_ms / 1000)) * 1000) if chapter.get("end_time") is not None else None
        if chapter_start_ms <= end_ms and (chapter_end_ms is None or chapter_end_ms >= start_ms):
            title = str(chapter.get("title") or "chapter")
            hints.append(f"chapter:{title}")
    for line in (metadata.description or "").splitlines():
        line = line.strip()
        if not line:
            continue
        if URL_RE.search(line):
            hints.append(f"url:{line}")
        elif any(token in line for token in [":", "-", "•"]) and len(line) <= 120:
            timestamp_hits = description_starts(line)
            if timestamp_hits and start_ms <= timestamp_hits[0] <= end_ms + 30_000:
                hints.append(f"tracklist:{line}")
    return hints[:6]


def combine_segments(left: SegmentDraft, right: SegmentDraft) -> SegmentDraft:
    duration_left = max(1, left.end_ms - left.start_ms)
    duration_right = max(1, right.end_ms - right.start_ms)
    total = duration_left + duration_right
    kind = left.kind if left.kind == right.kind else SegmentKind.MUSIC_UNRESOLVED
    return SegmentDraft(
        start_ms=min(left.start_ms, right.start_ms),
        end_ms=max(left.end_ms, right.end_ms),
        kind=kind,
        feature_vector=((left.feature_vector * duration_left) + (right.feature_vector * duration_right)) / total,
        chroma_vector=((left.chroma_vector * duration_left) + (right.chroma_vector * duration_right)) / total,
        music_ratio=((left.music_ratio * duration_left) + (right.music_ratio * duration_right)) / total,
        speech_ratio=((left.speech_ratio * duration_left) + (right.speech_ratio * duration_right)) / total,
        metadata_hints=list(dict.fromkeys(left.metadata_hints + right.metadata_hints)),
    )


def novelty_scores(frames: list[FeatureFrame]) -> np.ndarray:
    if len(frames) < 2:
        return np.zeros(len(frames), dtype=np.float32)
    matrix = np.stack([frame.feature_vector for frame in frames])
    if matrix.shape[0] < 2:
        return np.zeros(matrix.shape[0], dtype=np.float32)
    normalized = matrix - matrix.mean(axis=0, keepdims=True)
    normalized /= np.maximum(1e-6, normalized.std(axis=0, keepdims=True))
    deltas = np.linalg.norm(np.diff(normalized, axis=0), axis=1)
    return np.concatenate([np.array([0.0], dtype=np.float32), deltas.astype(np.float32)])


def local_maxima(values: np.ndarray) -> list[int]:
    maxima: list[int] = []
    for index in range(1, len(values) - 1):
        if values[index] >= values[index - 1] and values[index] >= values[index + 1]:
            maxima.append(index)
    return maxima


def classify_label(music_score: float, speech_score: float, no_music_score: float, rms: float, chroma_strength: float) -> SegmentKind:
    """Classify a feature frame into a coarse label.

    Thresholds widened (T1.4) so fewer real-music frames get mislabelled as
    ``SPEECH_ONLY`` / ``SILENCE_OR_FX`` and therefore skipped by the
    recognition probe loop. The SPEECH_ONLY gate in particular was
    aggressive on podcasts-with-BGM and quiet acoustic sections.
    """
    if rms < 0.06 or no_music_score >= 0.72:
        return SegmentKind.SILENCE_OR_FX
    # Widened: music_score 0.55 -> 0.48; chroma 0.30 -> 0.22; speech 0.62 -> 0.70.
    if music_score >= 0.48 and chroma_strength >= 0.22 and speech_score < 0.70:
        return SegmentKind.MATCHED_TRACK
    if speech_score >= 0.70 and music_score < 0.48:
        return SegmentKind.SPEECH_ONLY
    if music_score >= 0.32:
        return SegmentKind.MUSIC_UNRESOLVED
    return SegmentKind.SPEECH_ONLY


def smooth_matrix(values: np.ndarray, *, context: int) -> np.ndarray:
    if values.size == 0 or context <= 1:
        return values
    kernel = np.ones(context, dtype=np.float32) / context
    padded = np.pad(values, ((context // 2, context // 2), (0, 0)), mode="edge")
    return np.vstack([np.convolve(padded[:, column], kernel, mode="valid")[: values.shape[0]] for column in range(values.shape[1])]).T


def smooth_vector(values: np.ndarray, *, context: int) -> np.ndarray:
    if values.size == 0 or context <= 1:
        return values
    kernel = np.ones(context, dtype=np.float32) / context
    padded = np.pad(values, (context // 2, context // 2), mode="edge")
    return np.convolve(padded, kernel, mode="valid")[: values.shape[0]]


def normalize(values: np.ndarray) -> np.ndarray:
    if values.size == 0:
        return values.astype(np.float32)
    minimum = float(np.min(values))
    maximum = float(np.max(values))
    if math.isclose(minimum, maximum):
        return np.zeros_like(values, dtype=np.float32)
    return ((values - minimum) / (maximum - minimum)).astype(np.float32)


def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    if a.size == 0 or b.size == 0:
        return 0.0
    denom = float(np.linalg.norm(a) * np.linalg.norm(b))
    if denom <= 1e-6:
        return 0.0
    return float(np.dot(a, b) / denom)
