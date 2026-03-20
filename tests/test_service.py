from __future__ import annotations

from pathlib import Path

from music_fetch.media import MediaToolError, SourceProfile
from music_fetch.long_mix import ProbeWindow, SegmentDraft
from music_fetch.models import (
    ItemStatus,
    DetectedSegment,
    Job,
    JobCreate,
    JobOptions,
    JobStatus,
    ProviderName,
    ProviderState,
    SegmentKind,
    SourceItem,
    SourceKind,
    SourceMetadata,
    TrackCandidate,
    TrackMatch,
    WindowPlan,
)
from music_fetch.service import JobManager

from conftest import write_test_tone


class FakeProvider:
    name = ProviderName.VIBRA

    def state(self) -> ProviderState:
        return ProviderState(name=self.name, enabled=True, available=True)

    def recognize(self, clip_path: Path, start_ms: int, end_ms: int):
        return [
            TrackCandidate(
                track=TrackMatch(title="ACIDO III (Super Slowed)", artist="UdieNhx"),
                provider=self.name,
                confidence=0.78,
                start_ms=start_ms,
                end_ms=end_ms,
                evidence=[clip_path.name],
            )
        ]


class CrashProvider:
    name = ProviderName.VIBRA

    def state(self) -> ProviderState:
        return ProviderState(name=self.name, enabled=True, available=True)

    def recognize(self, clip_path: Path, start_ms: int, end_ms: int):
        raise IndexError("list index out of range")


def test_job_manager_runs_local_file(monkeypatch, app_env, tmp_path: Path) -> None:
    settings, db, manager = app_env
    source = write_test_tone(tmp_path / "tone.wav")

    monkeypatch.setattr("music_fetch.service.normalize_media", lambda input_path, output_path: input_path)
    monkeypatch.setattr("music_fetch.service.isolate_music", lambda settings, normalized, output_dir: normalized)
    monkeypatch.setattr(
        JobManager,
        "_select_windows",
        lambda self, job, item, normalized, instrumental, profile: [
            WindowPlan(start_ms=0, end_ms=12000, score=1.0, source_path=str(normalized), label="mix")
        ],
    )
    monkeypatch.setattr(
        "music_fetch.service.create_excerpt",
        lambda source_path, start_ms, end_ms, output_path: (
            output_path.parent.mkdir(parents=True, exist_ok=True),
            output_path.write_bytes(source_path.read_bytes()),
            output_path,
        )[2],
    )
    monkeypatch.setattr("music_fetch.service.fingerprint_cache_key", lambda clip_path: "cache-key")
    monkeypatch.setattr(JobManager, "_providers", lambda self: [FakeProvider()])

    job = manager.submit(JobCreate(inputs=[str(source)]))
    final_job = manager.wait(job.id)
    segments = db.get_segments(job.id)
    assert final_job.status.value == "succeeded"
    assert len(segments) == 1
    assert segments[0].track.title == "ACIDO III (Super Slowed)"


def test_select_windows_keeps_long_mix_budget(monkeypatch, app_env) -> None:
    settings, db, manager = app_env
    plans = [
        WindowPlan(start_ms=index * 12_000, end_ms=(index + 1) * 12_000, score=1.0, source_path="/tmp/fake.wav", label="mix")
        for index in range(48)
    ]
    monkeypatch.setattr("music_fetch.service.plan_windows_for_profile", lambda source_path, profile, label: plans)
    job = Job(
        id="job-1",
        status=JobStatus.RUNNING,
        created_at="2026-03-20T00:00:00+00:00",
        updated_at="2026-03-20T00:00:00+00:00",
        options=JobOptions(),
        inputs=["/tmp/fake.wav"],
    )
    item = SourceItem(
        id="item-1",
        job_id=job.id,
        input_value="/tmp/fake.wav",
        kind=SourceKind.LOCAL_FILE,
        status=ItemStatus.QUEUED,
        metadata=SourceMetadata(duration_ms=45 * 60_000),
    )
    selected = manager._select_windows(
        job,
        item,
        Path("/tmp/fake.wav"),
        None,
        profile=SourceProfile(
            duration_ms=45 * 60_000,
            strategy="long_mix",
            prefer_source_path="instrumental",
            request_budget=60,
            max_windows=48,
            stop_after_consensus=0,
            use_source_separation=True,
        ),
    )
    assert len(selected) == 48


def test_long_mix_reuses_repeat_group_matches(monkeypatch, app_env, tmp_path: Path) -> None:
    settings, db, manager = app_env
    source = write_test_tone(tmp_path / "longmix.wav", seconds=80)
    item = SourceItem(
        id="item-long",
        job_id="job-long",
        input_value=str(source),
        kind=SourceKind.LOCAL_FILE,
        status=ItemStatus.QUEUED,
        metadata=SourceMetadata(title="Long Mix", duration_ms=80_000),
        local_path=str(source),
    )
    draft_a = SegmentDraft(
        start_ms=0,
        end_ms=20_000,
        kind=SegmentKind.MUSIC_UNRESOLVED,
        feature_vector=__import__("numpy").ones(4),
        chroma_vector=__import__("numpy").ones(4),
        music_ratio=1.0,
        speech_ratio=0.0,
        probe_windows=[ProbeWindow(start_ms=0, end_ms=12_000, reason="early")],
        repeat_group_id="repeat-a",
    )
    draft_b = SegmentDraft(
        start_ms=20_000,
        end_ms=40_000,
        kind=SegmentKind.SPEECH_ONLY,
        feature_vector=__import__("numpy").zeros(4),
        chroma_vector=__import__("numpy").zeros(4),
        music_ratio=0.0,
        speech_ratio=1.0,
    )
    draft_c = SegmentDraft(
        start_ms=40_000,
        end_ms=60_000,
        kind=SegmentKind.MUSIC_UNRESOLVED,
        feature_vector=__import__("numpy").ones(4),
        chroma_vector=__import__("numpy").ones(4),
        music_ratio=1.0,
        speech_ratio=0.0,
        probe_windows=[ProbeWindow(start_ms=40_000, end_ms=52_000, reason="early")],
        repeat_group_id="repeat-a",
    )

    monkeypatch.setattr(
        "music_fetch.service.analyze_long_mix",
        lambda normalized, metadata, options: type("Analysis", (), {"segments": [draft_a, draft_b, draft_c]})(),
    )
    monkeypatch.setattr("music_fetch.service.normalize_media", lambda input_path, output_path: input_path)
    monkeypatch.setattr("music_fetch.service.probe_duration_ms", lambda input_path: 80_000)
    monkeypatch.setattr("music_fetch.service.create_excerpt", lambda source_path, start_ms, end_ms, output_path: source_path)
    monkeypatch.setattr("music_fetch.service.fingerprint_cache_key", lambda clip_path: "cache-key")
    monkeypatch.setattr(JobManager, "_providers", lambda self: [FakeProvider()])

    db.create_job([str(source)], JobOptions())
    segments = manager._process_long_mix_item(
        Job(
            id="job-long",
            status=JobStatus.RUNNING,
            created_at="2026-03-20T00:00:00+00:00",
            updated_at="2026-03-20T00:00:00+00:00",
            options=JobOptions(),
            inputs=[str(source)],
        ),
        item,
        source,
        None,
    )
    assert len(segments) == 3
    assert segments[0].track is not None
    assert segments[1].kind == SegmentKind.SPEECH_ONLY
    assert segments[2].track is not None
    assert segments[2].track.title == segments[0].track.title


def test_recognize_with_cache_converts_unexpected_provider_crash_to_warning(app_env, tmp_path: Path) -> None:
    settings, db, manager = app_env
    clip = tmp_path / "clip.wav"
    clip.write_bytes(b"fake")
    item = SourceItem(
        id="item-1",
        job_id="job-1",
        input_value=str(clip),
        kind=SourceKind.LOCAL_FILE,
        status=ItemStatus.RUNNING,
        metadata=SourceMetadata(duration_ms=12_000),
        local_path=str(clip),
    )

    hits = manager._recognize_with_cache("job-1", item, CrashProvider(), clip, 0, 12_000)
    assert hits == []
    events = db.list_events("job-1")
    assert any("crashed on" in event.message for event in events)


def test_segmented_path_prefers_instrumental_excerpt_source(monkeypatch, app_env, tmp_path: Path) -> None:
    settings, db, manager = app_env
    source = write_test_tone(tmp_path / "mix.wav", seconds=40)
    instrumental = write_test_tone(tmp_path / "instrumental.wav", seconds=40)
    item = SourceItem(
        id="item-segmented",
        job_id="job-segmented",
        input_value=str(source),
        kind=SourceKind.LOCAL_FILE,
        status=ItemStatus.QUEUED,
        metadata=SourceMetadata(title="Segmented Mix", duration_ms=40_000),
        local_path=str(source),
        instrumental_path=str(instrumental),
    )
    draft = SegmentDraft(
        start_ms=0,
        end_ms=20_000,
        kind=SegmentKind.MUSIC_UNRESOLVED,
        feature_vector=__import__("numpy").ones(4),
        chroma_vector=__import__("numpy").ones(4),
        music_ratio=1.0,
        speech_ratio=0.0,
        probe_windows=[ProbeWindow(start_ms=0, end_ms=12_000, reason="early")],
        repeat_group_id=None,
    )
    captured_sources: list[Path] = []

    monkeypatch.setattr(
        "music_fetch.service.analyze_long_mix",
        lambda normalized, metadata, options: type("Analysis", (), {"segments": [draft]})(),
    )
    monkeypatch.setattr("music_fetch.service.fingerprint_cache_key", lambda clip_path: f"cache:{clip_path}")
    monkeypatch.setattr(JobManager, "_providers", lambda self: [FakeProvider()])

    def fake_create_excerpt(source_path: Path, start_ms: int, end_ms: int, output_path: Path) -> Path:
        captured_sources.append(source_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(source_path.read_bytes())
        return output_path

    monkeypatch.setattr("music_fetch.service.create_excerpt", fake_create_excerpt)

    segments = manager._process_long_mix_item(
        Job(
            id="job-segmented",
            status=JobStatus.RUNNING,
            created_at="2026-03-20T00:00:00+00:00",
            updated_at="2026-03-20T00:00:00+00:00",
            options=JobOptions(),
            inputs=[str(source)],
        ),
        item,
        source,
        instrumental,
    )
    assert captured_sources == [instrumental]


def test_metadata_only_playlist_entry_becomes_searchable_match(monkeypatch, app_env) -> None:
    settings, db, manager = app_env
    job = db.create_job(["https://open.spotify.com/playlist/example"], JobOptions())
    item = SourceItem(
        id="item-meta",
        job_id=job.id,
        input_value="https://open.spotify.com/playlist/example",
        kind=SourceKind.YT_DLP,
        status=ItemStatus.QUEUED,
        metadata=SourceMetadata(
            title="Song Title",
            playlist_id="playlist-1",
            playlist_title="My Playlist",
            duration_ms=185_000,
            extra={
                "metadata_only": True,
                "track_title": "Song Title",
                "track_artist": "Artist Name",
                "track_album": "Album Name",
            },
        ),
    )

    monkeypatch.setattr("music_fetch.service.ensure_local_media", lambda settings, item: (_ for _ in ()).throw(AssertionError("should not download")))

    manager._process_item(job, item)
    segments = db.get_segments(job.id)
    assert item.status == ItemStatus.SUCCEEDED
    assert len(segments) == 1
    assert segments[0].track is not None
    assert segments[0].track.title == "Song Title"
    assert segments[0].track.artist == "Artist Name"
    assert "spotify" in segments[0].track.external_links


def test_download_failure_can_fallback_to_metadata_only(monkeypatch, app_env) -> None:
    settings, db, manager = app_env
    job = db.create_job(["https://music.youtube.com/playlist?list=PL123"], JobOptions())
    item = SourceItem(
        id="item-fallback",
        job_id=job.id,
        input_value="https://music.youtube.com/playlist?list=PL123",
        kind=SourceKind.YT_DLP,
        status=ItemStatus.QUEUED,
        metadata=SourceMetadata(
            title="Fallback Song",
            playlist_id="PL123",
            duration_ms=200_000,
            extra={
                "metadata_only": True,
                "track_title": "Fallback Song",
                "track_artist": "Fallback Artist",
            },
        ),
        download_url="https://music.youtube.com/watch?v=abc123&list=PL123",
    )

    checks = iter([False, True])
    monkeypatch.setattr(JobManager, "_has_metadata_only_track", lambda self, item: next(checks))
    monkeypatch.setattr("music_fetch.service.ensure_local_media", lambda settings, item: (_ for _ in ()).throw(MediaToolError("download failed")))

    manager._process_item(job, item)
    segments = db.get_segments(job.id)
    assert len(segments) == 1
    assert segments[0].track is not None
    assert segments[0].track.title == "Fallback Song"
    assert segments[0].track is not None


def test_stitch_segment_timeline_merges_duplicate_track_segments_and_clamps_overlaps(app_env) -> None:
    settings, db, manager = app_env
    slow = TrackMatch(title="Slow Down", artist="Chris Linton & CADMIUM")
    high = TrackMatch(title="High", artist="Vanic")
    stitched = manager._stitch_segment_timeline(
        [
            DetectedSegment(
                source_item_id="item-1",
                start_ms=5_000,
                end_ms=53_000,
                kind=SegmentKind.MATCHED_TRACK,
                confidence=0.72,
                providers=[ProviderName.VIBRA],
                evidence_count=1,
                track=slow,
            ),
            DetectedSegment(
                source_item_id="item-1",
                start_ms=46_000,
                end_ms=74_000,
                kind=SegmentKind.MATCHED_TRACK,
                confidence=0.72,
                providers=[ProviderName.VIBRA],
                evidence_count=1,
                track=slow,
            ),
            DetectedSegment(
                source_item_id="item-1",
                start_ms=67_000,
                end_ms=98_000,
                kind=SegmentKind.MATCHED_TRACK,
                confidence=0.72,
                providers=[ProviderName.VIBRA],
                evidence_count=1,
                track=high,
            ),
        ]
    )
    assert len(stitched) == 2
    assert stitched[0].track.title == "Slow Down"
    assert stitched[0].start_ms == 5_000
    assert stitched[0].end_ms == 67_000
    assert stitched[1].track.title == "High"


def test_storage_cleanup_removes_generated_artifacts_but_keeps_job_history(app_env, tmp_path: Path) -> None:
    settings, db, manager = app_env
    job = db.create_job([str(tmp_path / "input.wav")], JobOptions())
    item = SourceItem(
        id="item-artifacts",
        job_id=job.id,
        input_value=str(tmp_path / "input.wav"),
        kind=SourceKind.LOCAL_FILE,
        status=ItemStatus.SUCCEEDED,
        metadata=SourceMetadata(title="Artifacts", duration_ms=12_000),
        local_path=str(tmp_path / "music-fetch-mic-demo.m4a"),
        normalized_path=str(settings.cache_dir / "normalized" / job.id / "item-artifacts" / "normalized.wav"),
        instrumental_path=str(settings.cache_dir / "normalized" / job.id / "item-artifacts" / "stems" / "normalized.instrumental.wav"),
    )
    db.add_source_items([item])

    recording = Path(item.local_path)
    recording.write_bytes(b"recording")
    normalized = Path(item.normalized_path)
    normalized.parent.mkdir(parents=True, exist_ok=True)
    normalized.write_bytes(b"normalized")
    stems_dir = normalized.parent / "stems"
    stems_dir.mkdir(parents=True, exist_ok=True)
    (stems_dir / "normalized.instrumental.wav").write_bytes(b"stem")
    clips_dir = normalized.parent / "clips"
    clips_dir.mkdir(parents=True, exist_ok=True)
    (clips_dir / "probe.wav").write_bytes(b"clip")

    before = manager.storage_summary(job.id)
    assert before.total_size_bytes > 0

    after = manager.cleanup_job_artifacts(job.id)
    assert after.total_size_bytes == 0
    assert db.get_job(job.id) is not None
    cleaned_item = db.get_source_items(job.id)[0]
    assert cleaned_item.normalized_path is None
    assert cleaned_item.instrumental_path is None
    assert cleaned_item.local_path is None


def test_cleanup_all_temporary_artifacts_skips_pinned_jobs(app_env, tmp_path: Path) -> None:
    settings, db, manager = app_env
    pinned = db.create_job([str(tmp_path / "a.wav")], JobOptions())
    unpinned = db.create_job([str(tmp_path / "b.wav")], JobOptions())
    db.set_job_pinned(pinned.id, True)

    for job in [pinned, unpinned]:
        normalized = settings.cache_dir / "normalized" / job.id / "item-1" / "normalized.wav"
        normalized.parent.mkdir(parents=True, exist_ok=True)
        normalized.write_bytes(job.id.encode())
        db.add_source_items(
            [
                SourceItem(
                    id=f"item-{job.id}",
                    job_id=job.id,
                    input_value=job.inputs[0],
                    kind=SourceKind.LOCAL_FILE,
                    status=ItemStatus.SUCCEEDED,
                    metadata=SourceMetadata(title=job.id, duration_ms=1_000),
                    normalized_path=str(normalized),
                )
            ]
        )

    summary = manager.cleanup_temporary_artifacts()
    assert any(entry.pinned for entry in summary.entries) or summary.total_size_bytes >= 0
    assert (settings.cache_dir / "normalized" / pinned.id).exists()
    assert not (settings.cache_dir / "normalized" / unpinned.id).exists()
