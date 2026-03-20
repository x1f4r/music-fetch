from pathlib import Path

from music_fetch.media import (
    FingerprintChunk,
    SourceProfile,
    classify_source,
    clustered_long_mix_windows,
    description_starts,
    metadata_windows,
    plan_windows_for_profile,
    yt_dlp_download_args,
)
from music_fetch.models import ItemStatus, SourceItem, SourceKind, SourceMetadata


def test_classify_source_prefers_long_mix_for_long_files() -> None:
    profile = classify_source(45 * 60_000, has_playlist_context=False, metadata=SourceMetadata())
    assert profile.strategy == "long_mix"
    assert profile.max_windows >= 12
    assert profile.request_budget >= profile.max_windows


def test_classify_source_keeps_long_mix_inside_playlists() -> None:
    profile = classify_source(45 * 60_000, has_playlist_context=True, metadata=SourceMetadata())
    assert profile.strategy == "long_mix"
    assert profile.max_windows >= 24


def test_classify_source_uses_segmented_multi_track_for_short_non_playlist_edits() -> None:
    profile = classify_source(110_000, has_playlist_context=False, metadata=SourceMetadata())
    assert profile.strategy == "multi_track"
    assert profile.request_budget >= 12


def test_classify_source_uses_segmented_multi_track_for_structural_hints() -> None:
    metadata = SourceMetadata(chapters=[{"start_time": 0}, {"start_time": 42}], description="Song A 00:00\nSong B 00:42")
    profile = classify_source(24_000, has_playlist_context=False, metadata=metadata)
    assert profile.strategy == "multi_track"


def test_classify_source_uses_segmented_multi_track_for_short_playlist_entries_too() -> None:
    profile = classify_source(45_000, has_playlist_context=True, metadata=SourceMetadata())
    assert profile.strategy == "multi_track"


def test_clustered_long_mix_windows_groups_adjacent_similar_chunks(monkeypatch) -> None:
    monkeypatch.setattr("music_fetch.media.probe_duration_ms", lambda source_path: 120_000)
    monkeypatch.setattr("music_fetch.media.score_window", lambda source_path, start_ms, end_ms: 1.0)
    monkeypatch.setattr(
        "music_fetch.media.chunk_fingerprints",
        lambda source_path, chunk_seconds: [
            FingerprintChunk(timestamp_ms=0, duration_ms=20_000, fingerprint=[100] * 80),
            FingerprintChunk(timestamp_ms=20_000, duration_ms=20_000, fingerprint=[102] * 80),
            FingerprintChunk(timestamp_ms=40_000, duration_ms=20_000, fingerprint=[400] * 80),
            FingerprintChunk(timestamp_ms=60_000, duration_ms=20_000, fingerprint=[401] * 80),
        ],
    )
    plans = clustered_long_mix_windows(Path("/tmp/fake.wav"), label="mix", max_windows=10)
    assert len(plans) == 2
    assert plans[0].start_ms < 20_000
    assert plans[1].start_ms > 45_000


def test_description_starts_parses_tracklist_lines() -> None:
    starts = description_starts("Intro 00:00\nBattle Theme 03:41\nEnding 1:02:03")
    assert starts == [0, 221_000, 3_723_000]


def test_plan_windows_for_long_mix_covers_full_duration(monkeypatch) -> None:
    monkeypatch.setattr("music_fetch.media.probe_duration_ms", lambda source_path: 8 * 60_000)
    monkeypatch.setattr("music_fetch.media.score_window", lambda source_path, start_ms, end_ms: 1.0)
    profile = SourceProfile(
        duration_ms=8 * 60_000,
        strategy="long_mix",
        prefer_source_path="instrumental",
        request_budget=20,
        max_windows=8,
        stop_after_consensus=0,
        use_source_separation=True,
    )
    plans = plan_windows_for_profile(Path("/tmp/fake.wav"), profile, "mix")
    assert len(plans) == 8
    assert plans[0].start_ms == 0
    assert plans[-1].start_ms >= (8 * 60_000) - 72_000


def test_metadata_windows_use_chapter_boundaries(monkeypatch) -> None:
    monkeypatch.setattr("music_fetch.media.score_window", lambda source_path, start_ms, end_ms: 1.0)
    metadata = type("Metadata", (), {"chapters": [{"start_time": 0}, {"start_time": 180}, {"start_time": 360}], "description": None})()
    plans = metadata_windows(Path("/tmp/fake.wav"), metadata, duration_ms=600_000, label="mix")
    assert len(plans) == 3
    assert [plan.start_ms for plan in plans] == [30_000, 210_000, 390_000]


def test_yt_dlp_download_args_prefer_playlist_context_before_direct_item() -> None:
    item = SourceItem(
        id="item-1",
        job_id="job-1",
        input_value="https://music.youtube.com/playlist?list=PL123",
        kind=SourceKind.YT_DLP,
        status=ItemStatus.QUEUED,
        metadata=SourceMetadata(
            playlist_id="PL123",
            entry_index=7,
            extra={"playlist_source_url": "https://music.youtube.com/playlist?list=PL123"},
        ),
        download_url="https://music.youtube.com/watch?v=abc123&list=PL123",
    )

    commands = yt_dlp_download_args(item, "/tmp/%(id)s.%(ext)s")
    assert commands[0][-3:] == ["--playlist-items", "7", "https://music.youtube.com/playlist?list=PL123"]
    assert commands[1][-2:] == ["--no-playlist", "https://music.youtube.com/watch?v=abc123&list=PL123"]
