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
