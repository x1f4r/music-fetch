from __future__ import annotations

from collections import defaultdict

from .models import DetectedSegment, ProviderName, SegmentKind, TrackCandidate, TrackMatch


def fuse_candidates(source_item_id: str, candidates: list[TrackCandidate], max_gap_ms: int = 8_000) -> list[DetectedSegment]:
    grouped: dict[str, list[TrackCandidate]] = defaultdict(list)
    for candidate in candidates:
        grouped[candidate.track.normalized_key()].append(candidate)

    segments: list[DetectedSegment] = []
    for key_candidates in grouped.values():
        key_candidates.sort(key=lambda item: item.start_ms)
        cluster: list[TrackCandidate] = []
        for candidate in key_candidates:
            if not cluster:
                cluster = [candidate]
                continue
            previous = cluster[-1]
            if candidate.start_ms <= previous.end_ms + max_gap_ms:
                cluster.append(candidate)
            else:
                segments.append(_build_segment(source_item_id, cluster))
                cluster = [candidate]
        if cluster:
            segments.append(_build_segment(source_item_id, cluster))

    segments.sort(key=lambda item: item.start_ms)
    return segments


def _build_segment(source_item_id: str, cluster: list[TrackCandidate]) -> DetectedSegment:
    cluster.sort(key=lambda item: item.confidence, reverse=True)
    primary = cluster[0]
    providers = sorted({item.provider for item in cluster}, key=lambda value: value.value)
    alternates = _alternate_tracks(cluster)
    confidence = min(1.0, sum(item.confidence for item in cluster) / max(1, len(cluster)) + 0.05 * (len(providers) - 1))
    return DetectedSegment(
        source_item_id=source_item_id,
        start_ms=min(item.start_ms for item in cluster),
        end_ms=max(item.end_ms for item in cluster),
        kind=SegmentKind.MATCHED_TRACK,
        confidence=confidence,
        providers=providers,
        evidence_count=sum(max(1, len(item.evidence)) for item in cluster),
        track=primary.track,
        alternates=alternates,
        probe_count=len(cluster),
        provider_attempts=len(cluster),
    )


def _alternate_tracks(cluster: list[TrackCandidate]) -> list[TrackMatch]:
    alternates: list[TrackMatch] = []
    seen = {cluster[0].track.normalized_key()}
    for candidate in cluster[1:]:
        key = candidate.track.normalized_key()
        if key in seen:
            continue
        seen.add(key)
        alternates.append(candidate.track)
    return alternates
