from music_fetch.fusion import fuse_candidates
from music_fetch.models import ProviderName, TrackCandidate, TrackMatch


def test_fuses_repeated_matches_into_segments() -> None:
    track = TrackMatch(title="Warriors", artist="Imagine Dragons")
    candidates = [
        TrackCandidate(track=track, provider=ProviderName.VIBRA, confidence=0.7, start_ms=0, end_ms=12000),
        TrackCandidate(track=track, provider=ProviderName.AUDD, confidence=0.9, start_ms=8000, end_ms=20000),
    ]
    segments = fuse_candidates("item-1", candidates)
    assert len(segments) == 1
    assert segments[0].start_ms == 0
    assert segments[0].end_ms == 20000
    assert segments[0].providers == [ProviderName.AUDD, ProviderName.VIBRA]


def test_fusion_merges_across_provider_metadata_drift() -> None:
    """Two providers return the same song with subtle title/artist drift.
    Pre-overhaul fusion keyed on ``artist::title`` lowercase; those hits
    stayed as two segments. With tiered identity (T1.1) they merge into one."""
    candidates = [
        TrackCandidate(
            track=TrackMatch(title="Slow Down", artist="CADMIUM, Chris Linton"),
            provider=ProviderName.VIBRA,
            confidence=0.72,
            start_ms=0,
            end_ms=12_000,
        ),
        TrackCandidate(
            track=TrackMatch(title="Slow Down (Remastered)", artist="Chris Linton & CADMIUM"),
            provider=ProviderName.AUDD,
            confidence=0.80,
            start_ms=6_000,
            end_ms=18_000,
        ),
    ]
    segments = fuse_candidates("item-1", candidates)
    assert len(segments) == 1
    assert segments[0].providers == [ProviderName.AUDD, ProviderName.VIBRA]


def test_fusion_splits_different_isrcs_sharing_fuzzy_key() -> None:
    """ISRC veto: same fuzzy key but distinct ISRCs must stay separate."""
    candidates = [
        TrackCandidate(
            track=TrackMatch(title="Clone", artist="Artist", isrc="USRC11111111"),
            provider=ProviderName.VIBRA,
            confidence=0.72,
            start_ms=0,
            end_ms=12_000,
        ),
        TrackCandidate(
            track=TrackMatch(title="Clone", artist="Artist", isrc="USRC22222222"),
            provider=ProviderName.AUDD,
            confidence=0.80,
            start_ms=1_000,
            end_ms=14_000,
        ),
    ]
    segments = fuse_candidates("item-1", candidates)
    assert len(segments) == 2
