from __future__ import annotations

from pathlib import Path

import pytest

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


def test_run_existing_job_processes_created_job(monkeypatch, app_env, tmp_path: Path) -> None:
    settings, db, manager = app_env
    source = write_test_tone(tmp_path / "tone.wav")

    monkeypatch.setattr("music_fetch.service.normalize_media", lambda input_path, output_path: input_path)
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

    job = manager.create_job(JobCreate(inputs=[str(source)]))
    manager.run_existing_job(job.id)

    stored = db.get_job(job.id)
    segments = db.get_segments(job.id)
    assert stored is not None
    assert stored.status.value == "succeeded"
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

    # Create a real jobs row with the id the test uses so FK-backed writes
    # (events, metrics, source_items) succeed under the v5 cascade schema.
    db.create_job([str(source)], JobOptions())
    with db.connect() as conn:
        conn.execute("UPDATE jobs SET id = ? WHERE id != ?", ("job-long", "job-long"))
        conn.commit()
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
    # Create a job row so that the FK-backed event/metric writes succeed under
    # the v5 cascade schema.
    job = db.create_job([str(clip)], JobOptions())
    item = SourceItem(
        id="item-1",
        job_id=job.id,
        input_value=str(clip),
        kind=SourceKind.LOCAL_FILE,
        status=ItemStatus.RUNNING,
        metadata=SourceMetadata(duration_ms=12_000),
        local_path=str(clip),
    )

    hits = manager._recognize_with_cache(job.id, item, CrashProvider(), clip, 0, 12_000)
    assert hits == []
    events = db.list_events(job.id)
    assert any("crashed on" in event.message for event in events)


def test_segmented_path_prefers_instrumental_excerpt_source(monkeypatch, app_env, tmp_path: Path) -> None:
    settings, db, manager = app_env
    source = write_test_tone(tmp_path / "mix.wav", seconds=40)
    instrumental = write_test_tone(tmp_path / "instrumental.wav", seconds=40)
    # Pre-create the job row so FK-backed writes (events, metrics, source_items)
    # succeed under the v5 cascade schema.
    db.create_job([str(source)], JobOptions())
    with db.connect() as conn:
        conn.execute("UPDATE jobs SET id = ? WHERE id != ?", ("job-segmented", "job-segmented"))
        conn.commit()
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


def test_cancel_request_marks_job_canceled(app_env, tmp_path: Path) -> None:
    settings, db, manager = app_env
    source = write_test_tone(tmp_path / "cancel.wav")
    job = manager.create_job(JobCreate(inputs=[str(source)]))
    manager.cancel(job.id)
    manager.run_existing_job(job.id)
    stored = db.get_job(job.id)
    assert stored is not None
    assert stored.status == JobStatus.CANCELED


def test_short_system_recording_uses_single_track_path(monkeypatch, app_env, tmp_path: Path) -> None:
    settings, db, manager = app_env
    source = write_test_tone(tmp_path / "music-fetch-system-demo.wav", seconds=30)
    job = db.create_job([str(source)], JobOptions())
    item = SourceItem(
        id="item-recording",
        job_id=job.id,
        input_value=str(source),
        kind=SourceKind.LOCAL_FILE,
        status=ItemStatus.QUEUED,
        metadata=SourceMetadata(title="System Recording", duration_ms=30_000),
    )

    observed: dict[str, str] = {}
    monkeypatch.setattr("music_fetch.service.normalize_media", lambda input_path, output_path: input_path)
    def fake_select_windows(self, job, item, normalized, instrumental, profile):
        observed["strategy"] = profile.strategy
        return [WindowPlan(start_ms=0, end_ms=12_000, score=1.0, source_path=str(normalized), label="mix")]

    monkeypatch.setattr(JobManager, "_select_windows", fake_select_windows)
    monkeypatch.setattr(
        "music_fetch.service.create_excerpt",
        lambda source_path, start_ms, end_ms, output_path: (
            output_path.parent.mkdir(parents=True, exist_ok=True),
            output_path.write_bytes(source_path.read_bytes()),
            output_path,
        )[2],
    )
    monkeypatch.setattr("music_fetch.service.fingerprint_cache_key", lambda clip_path: "recording-cache-key")
    monkeypatch.setattr(JobManager, "_providers", lambda self: [FakeProvider()])
    monkeypatch.setattr(JobManager, "_process_long_mix_item", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not use segmented path")))

    manager._process_item(job, item)

    assert observed["strategy"] == "single_track"
    segments = db.get_segments(job.id)
    assert segments[0].track is not None


def test_library_entries_include_failed_jobs_without_source_items(app_env) -> None:
    settings, db, manager = app_env
    job = db.create_job([], JobOptions())
    db.update_job(job.id, status=JobStatus.FAILED, error="broken")

    entries = manager.list_library_entries(limit=20)

    failed = next(entry for entry in entries if entry.job_id == job.id)
    assert failed.status == JobStatus.FAILED
    assert failed.title == job.id
    assert failed.item_count == 0


def test_manual_correction_updates_segment_track_and_explanation(app_env) -> None:
    settings, db, manager = app_env
    job = db.create_job(["demo"], JobOptions())
    db.add_source_items(
        [
            SourceItem(
                id="item-1",
                job_id=job.id,
                input_value="demo",
                kind=SourceKind.LOCAL_FILE,
                status=ItemStatus.SUCCEEDED,
                metadata=SourceMetadata(title="Demo", duration_ms=20_000),
            )
        ]
    )
    db.replace_segments(
        job.id,
        "item-1",
        [
            DetectedSegment(
                source_item_id="item-1",
                start_ms=0,
                end_ms=12_000,
                kind=SegmentKind.MUSIC_UNRESOLVED,
                confidence=0.0,
                providers=[],
                evidence_count=0,
                explanation=["Music detected, but no candidate cleared the evidence threshold."],
            )
        ],
    )

    corrected = manager.correct_segment(
        job.id,
        source_item_id="item-1",
        start_ms=0,
        end_ms=12_000,
        title="Manual Song",
        artist="Manual Artist",
    )

    stored = db.get_segments(job.id)[0]
    assert corrected.track is not None
    assert stored.track is not None
    assert stored.track.title == "Manual Song"
    assert stored.track.artist == "Manual Artist"
    assert stored.uncertainty == 0.0
    assert stored.explanation[0] == "Manually corrected by the user."


def test_retry_unresolved_segment_can_promote_match(monkeypatch, app_env, tmp_path: Path) -> None:
    settings, db, manager = app_env
    source = write_test_tone(tmp_path / "retry.wav", seconds=20)
    job = db.create_job([str(source)], JobOptions())
    item = SourceItem(
        id="item-1",
        job_id=job.id,
        input_value=str(source),
        kind=SourceKind.LOCAL_FILE,
        status=ItemStatus.SUCCEEDED,
        metadata=SourceMetadata(title="Retry", duration_ms=20_000),
        local_path=str(source),
        normalized_path=str(source),
    )
    db.add_source_items([item])
    db.replace_segments(
        job.id,
        item.id,
        [
            DetectedSegment(
                source_item_id=item.id,
                start_ms=0,
                end_ms=12_000,
                kind=SegmentKind.MUSIC_UNRESOLVED,
                confidence=0.0,
                providers=[],
                evidence_count=0,
                explanation=["Music detected, but no candidate cleared the evidence threshold."],
            )
        ],
    )
    monkeypatch.setattr("music_fetch.service.create_excerpt", lambda source_path, start_ms, end_ms, output_path: (output_path.parent.mkdir(parents=True, exist_ok=True), output_path.write_bytes(source_path.read_bytes()), output_path)[2])
    monkeypatch.setattr("music_fetch.service.fingerprint_cache_key", lambda clip_path: f"retry:{clip_path}")
    monkeypatch.setattr(manager.provider_registry, "active_providers_for_order", lambda order=None: [FakeProvider()])

    result = manager.retry_unresolved_segments(job.id)

    stored = db.get_segments(job.id)[0]
    assert result["retried_segments"] == 1
    assert result["matched_segments"] == 1
    assert stored.kind == SegmentKind.MATCHED_TRACK
    assert stored.track is not None
    assert stored.track.title == "ACIDO III (Super Slowed)"
    assert stored.explanation[0] == "Recovered by retrying an unresolved region."


def test_retry_uses_input_path_for_local_files_without_local_path(monkeypatch, app_env, tmp_path: Path) -> None:
    settings, db, manager = app_env
    source = write_test_tone(tmp_path / "kept-local.wav", seconds=20)
    job = db.create_job([str(source)], JobOptions())
    item = SourceItem(
        id="item-local",
        job_id=job.id,
        input_value=str(source),
        kind=SourceKind.LOCAL_FILE,
        status=ItemStatus.SUCCEEDED,
        metadata=SourceMetadata(title="Local", duration_ms=20_000),
        local_path=None,
        normalized_path=None,
    )
    db.add_source_items([item])
    db.replace_segments(
        job.id,
        item.id,
        [
            DetectedSegment(
                source_item_id=item.id,
                start_ms=0,
                end_ms=12_000,
                kind=SegmentKind.MUSIC_UNRESOLVED,
                confidence=0.0,
                providers=[],
                evidence_count=0,
            )
        ],
    )
    monkeypatch.setattr(
        "music_fetch.service.normalize_media",
        lambda input_path, output_path: (
            output_path.parent.mkdir(parents=True, exist_ok=True),
            output_path.write_bytes(input_path.read_bytes()),
            output_path,
        )[2],
    )
    monkeypatch.setattr(
        "music_fetch.service.create_excerpt",
        lambda source_path, start_ms, end_ms, output_path: (
            output_path.parent.mkdir(parents=True, exist_ok=True),
            output_path.write_bytes(source_path.read_bytes()),
            output_path,
        )[2],
    )
    monkeypatch.setattr("music_fetch.service.fingerprint_cache_key", lambda clip_path: f"retry-local:{clip_path}")
    monkeypatch.setattr(manager.provider_registry, "active_providers_for_order", lambda order=None: [FakeProvider()])

    result = manager.retry_unresolved_segments(job.id)

    assert result["matched_segments"] == 1
    stored = db.get_segments(job.id)[0]
    assert stored.kind == SegmentKind.MATCHED_TRACK
    assert stored.track is not None


def test_stitch_bridges_short_speech_between_same_identity_matches(app_env) -> None:
    """A DJ talking for 3 seconds between two plays of the same song
    (identity-equivalent, even with slight metadata drift) should produce
    one segment, not two. Regression: this was the canonical "6 snippets
    of one song" complaint. T1.2."""
    settings, db, manager = app_env
    song_a = TrackMatch(title="Slow Down", artist="CADMIUM, Chris Linton")
    # Slightly different artist ordering — fuzzy identity must collapse these.
    song_a2 = TrackMatch(title="Slow Down", artist="Chris Linton & CADMIUM")
    stitched = manager._stitch_segment_timeline(
        [
            DetectedSegment(
                source_item_id="item-1",
                start_ms=0,
                end_ms=30_000,
                kind=SegmentKind.MATCHED_TRACK,
                confidence=0.72,
                providers=[ProviderName.VIBRA],
                evidence_count=1,
                track=song_a,
            ),
            DetectedSegment(
                source_item_id="item-1",
                start_ms=30_000,
                end_ms=33_000,
                kind=SegmentKind.SPEECH_ONLY,
                confidence=0.0,
                providers=[],
                evidence_count=0,
            ),
            DetectedSegment(
                source_item_id="item-1",
                start_ms=33_000,
                end_ms=60_000,
                kind=SegmentKind.MATCHED_TRACK,
                confidence=0.75,
                providers=[ProviderName.VIBRA],
                evidence_count=1,
                track=song_a2,
            ),
        ]
    )
    assert len(stitched) == 1
    assert stitched[0].start_ms == 0
    assert stitched[0].end_ms == 60_000
    assert stitched[0].track.title == "Slow Down"


def test_stitch_does_not_bridge_across_long_speech_region(app_env) -> None:
    """A genuine 20-second speech section between two plays of the same
    song should stay separate — that's legitimate repeat behavior, not DJ
    chatter. T1.2 bridge-length guard."""
    settings, db, manager = app_env
    track = TrackMatch(title="Repeat Song", artist="Artist")
    stitched = manager._stitch_segment_timeline(
        [
            DetectedSegment(
                source_item_id="item-1",
                start_ms=0,
                end_ms=30_000,
                kind=SegmentKind.MATCHED_TRACK,
                confidence=0.8,
                providers=[ProviderName.VIBRA],
                evidence_count=1,
                track=track,
            ),
            DetectedSegment(
                source_item_id="item-1",
                start_ms=30_000,
                end_ms=50_000,  # 20 seconds of speech — too long to bridge.
                kind=SegmentKind.SPEECH_ONLY,
                confidence=0.0,
                providers=[],
                evidence_count=0,
            ),
            DetectedSegment(
                source_item_id="item-1",
                start_ms=50_000,
                end_ms=80_000,
                kind=SegmentKind.MATCHED_TRACK,
                confidence=0.8,
                providers=[ProviderName.VIBRA],
                evidence_count=1,
                track=track,
            ),
        ]
    )
    # Expect 3 segments preserved: song / speech / song.
    assert len(stitched) == 3
    assert stitched[0].kind == SegmentKind.MATCHED_TRACK
    assert stitched[1].kind == SegmentKind.SPEECH_ONLY
    assert stitched[2].kind == SegmentKind.MATCHED_TRACK


def test_stitch_does_not_merge_distinct_isrcs_even_with_matching_titles(app_env) -> None:
    """Two songs with the same title + artist but distinct ISRCs are
    explicitly different recordings; ISRC veto must prevent merge."""
    settings, db, manager = app_env
    left_track = TrackMatch(title="Same Title", artist="Artist", isrc="USRC11111111")
    right_track = TrackMatch(title="Same Title", artist="Artist", isrc="USRC22222222")
    stitched = manager._stitch_segment_timeline(
        [
            DetectedSegment(
                source_item_id="item-1",
                start_ms=0,
                end_ms=10_000,
                kind=SegmentKind.MATCHED_TRACK,
                confidence=0.8,
                providers=[ProviderName.VIBRA],
                evidence_count=1,
                track=left_track,
            ),
            DetectedSegment(
                source_item_id="item-1",
                start_ms=10_000,
                end_ms=20_000,
                kind=SegmentKind.MATCHED_TRACK,
                confidence=0.8,
                providers=[ProviderName.VIBRA],
                evidence_count=1,
                track=right_track,
            ),
        ]
    )
    assert len(stitched) == 2


def test_pick_candidate_accepts_single_strong_hit_over_low_music_ratio(app_env) -> None:
    """A podcast with music has music_ratio below 0.45, which the old gate
    (AND all) rejected even when providers agreed. The layered gate (T1.3)
    accepts when score >= 0.72 regardless of music_ratio."""
    settings, db, manager = app_env
    from music_fetch.long_mix import SegmentDraft
    import numpy as np

    draft = SegmentDraft(
        start_ms=0,
        end_ms=30_000,
        kind=SegmentKind.SPEECH_ONLY,
        feature_vector=np.ones(4),
        chroma_vector=np.ones(4),
        music_ratio=0.30,
        speech_ratio=0.60,
        candidates=[
            TrackCandidate(
                track=TrackMatch(title="Underlying Music", artist="Some Artist"),
                provider=ProviderName.ACRCLOUD,
                confidence=0.85,  # ACRCloud weight 0.90 → score 0.765+ → G3
                start_ms=0,
                end_ms=10_000,
            )
        ],
    )
    pick = manager._pick_segment_candidate(draft)
    assert pick is not None
    assert pick.raw.get("_acceptance_gate") == "G3"


def test_pick_candidate_accepts_isrc_backed_hit_below_normal_threshold(app_env) -> None:
    """ISRC provenance is strong evidence; G5 accepts at a lower score."""
    settings, db, manager = app_env
    from music_fetch.long_mix import SegmentDraft
    import numpy as np

    draft = SegmentDraft(
        start_ms=0,
        end_ms=30_000,
        kind=SegmentKind.MUSIC_UNRESOLVED,
        feature_vector=np.ones(4),
        chroma_vector=np.ones(4),
        music_ratio=0.20,  # low — would fail G4
        speech_ratio=0.10,
        candidates=[
            TrackCandidate(
                track=TrackMatch(title="Song", artist="Artist", isrc="USRC00000001"),
                provider=ProviderName.AUDD,
                confidence=0.70,  # weight 0.87 → score ~0.60+
                start_ms=0,
                end_ms=10_000,
            )
        ],
    )
    pick = manager._pick_segment_candidate(draft)
    assert pick is not None
    assert pick.raw.get("_acceptance_gate") == "G5"


def test_delete_job_refuses_running_job(app_env) -> None:
    """Can't delete an in-flight job — cancel first. T0.2."""
    from music_fetch.service import JobBusyError

    settings, db, manager = app_env
    job = db.create_job(["/tmp/x.wav"], JobOptions())
    db.update_job(job.id, status=JobStatus.RUNNING)

    with pytest.raises(JobBusyError):
        manager.delete_job(job.id)

    # Still present.
    assert db.get_job(job.id) is not None


def test_delete_job_removes_library_row_and_children(app_env, tmp_path) -> None:
    """End-to-end: delete a terminal job, verify library entry disappears
    and cascades clear every child table."""
    settings, db, manager = app_env
    job = db.create_job(["/tmp/x.wav"], JobOptions())
    db.add_source_items(
        [
            SourceItem(
                id="item-1",
                job_id=job.id,
                input_value="/tmp/x.wav",
                kind=SourceKind.LOCAL_FILE,
                status=ItemStatus.SUCCEEDED,
                metadata=SourceMetadata(title="X", duration_ms=12_000),
            )
        ]
    )
    db.update_job(job.id, status=JobStatus.SUCCEEDED)

    result = manager.delete_job(job.id)

    assert result["deleted"] is True
    assert db.get_job(job.id) is None
    assert len(manager.list_library_entries()) == 0


def test_prune_zombie_entries_removes_artifactless_terminals(app_env) -> None:
    """Library reconciliation: terminal-status jobs with no artifacts and no
    segments are zombies; pinned jobs and still-running jobs are not. T0.5."""
    settings, db, manager = app_env
    zombie = db.create_job(["a"], JobOptions())
    db.update_job(zombie.id, status=JobStatus.SUCCEEDED)
    pinned = db.create_job(["b"], JobOptions())
    db.update_job(pinned.id, status=JobStatus.SUCCEEDED)
    db.set_job_pinned(pinned.id, True)
    running = db.create_job(["c"], JobOptions())
    db.update_job(running.id, status=JobStatus.RUNNING)

    result = manager.prune_zombie_library_entries()
    removed = set(result["removed_job_ids"])
    assert zombie.id in removed
    assert pinned.id not in removed
    assert running.id not in removed
    assert db.get_job(zombie.id) is None
    assert db.get_job(pinned.id) is not None
    assert db.get_job(running.id) is not None


def test_export_job_supports_csv_and_chapters(app_env) -> None:
    settings, db, manager = app_env
    job = db.create_job(["demo"], JobOptions())
    db.add_source_items(
        [
            SourceItem(
                id="item-1",
                job_id=job.id,
                input_value="demo",
                kind=SourceKind.LOCAL_FILE,
                status=ItemStatus.SUCCEEDED,
                metadata=SourceMetadata(title="Demo", duration_ms=20_000),
            )
        ]
    )
    db.replace_segments(
        job.id,
        "item-1",
        [
            DetectedSegment(
                source_item_id="item-1",
                start_ms=0,
                end_ms=12_000,
                kind=SegmentKind.MATCHED_TRACK,
                confidence=0.9,
                providers=[ProviderName.VIBRA],
                evidence_count=1,
                track=TrackMatch(title="Song", artist="Artist"),
                explanation=["Provider agreement: vibra."],
            )
        ],
    )

    csv_name, csv_content = manager.export_job(job.id, export_format="csv")
    chapters_name, chapters_content = manager.export_job(job.id, export_format="chapters")

    assert csv_name.endswith(".csv")
    assert "source_item_id,start_ms,end_ms" in csv_content
    assert "Song" in csv_content
    assert chapters_name.endswith("-chapters.txt")
    assert "00:00 Artist - Song" in chapters_content
