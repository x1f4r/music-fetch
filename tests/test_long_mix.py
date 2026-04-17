from __future__ import annotations

import math
import wave
from pathlib import Path

import numpy as np

from music_fetch.long_mix import (
    SegmentAnalysisParameters,
    SegmentDraft,
    analyze_long_mix,
    assign_repeat_groups,
    choose_probe_windows,
    classify_label,
    extract_feature_frames,
)
from music_fetch.models import JobOptions, SegmentKind, SourceMetadata


def write_mixed_fixture(path: Path) -> Path:
    sample_rate = 16_000
    segments = [
        ("tone_a", 440, 24),
        ("silence", 0, 12),
        ("tone_a", 440, 24),
        ("tone_b", 660, 24),
    ]
    amplitude = 10_000
    with wave.open(str(path), "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(sample_rate)
        samples = bytearray()
        index = 0
        for kind, frequency, seconds in segments:
            frame_count = seconds * sample_rate
            for _ in range(frame_count):
                if kind == "silence":
                    value = 0
                else:
                    value = int(amplitude * math.sin(2 * math.pi * frequency * index / sample_rate))
                samples += value.to_bytes(2, byteorder="little", signed=True)
                index += 1
        wav.writeframes(bytes(samples))
    return path


def write_three_song_fixture(path: Path) -> Path:
    sample_rate = 16_000
    segments = [
        (330, 10),
        (440, 10),
        (550, 10),
    ]
    amplitude = 11_000
    with wave.open(str(path), "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(sample_rate)
        samples = bytearray()
        index = 0
        for frequency, seconds in segments:
            frame_count = seconds * sample_rate
            for _ in range(frame_count):
                value = int(amplitude * math.sin(2 * math.pi * frequency * index / sample_rate))
                samples += value.to_bytes(2, byteorder="little", signed=True)
                index += 1
        wav.writeframes(bytes(samples))
    return path


def test_analyze_long_mix_detects_repeat_groups_and_silence(tmp_path: Path) -> None:
    fixture = write_mixed_fixture(tmp_path / "mix.wav")
    analysis = analyze_long_mix(
        fixture,
        SourceMetadata(description="Track A 00:00\nSilence 00:24\nTrack B 01:00"),
        JobOptions(max_segments=40, max_probes_per_segment=2),
    )
    assert analysis.segments
    assert any(segment.kind == SegmentKind.SILENCE_OR_FX for segment in analysis.segments)
    repeat_groups = [segment.repeat_group_id for segment in analysis.segments if segment.repeat_group_id]
    assert repeat_groups
    assert len(repeat_groups) > len(set(repeat_groups))


def test_choose_probe_windows_scales_down_for_short_transition_segments() -> None:
    segment = SegmentDraft(
        start_ms=92_000,
        end_ms=110_000,
        kind=SegmentKind.MUSIC_UNRESOLVED,
        feature_vector=np.ones(4),
        chroma_vector=np.ones(4),
        music_ratio=1.0,
        speech_ratio=0.0,
    )
    probes = choose_probe_windows(segment, 3)
    assert len(probes) == 3
    assert all((probe.end_ms - probe.start_ms) < 12_000 for probe in probes)
    assert probes[-1].end_ms == 110_000


def test_analyze_long_mix_segments_short_three_song_clip(tmp_path: Path) -> None:
    fixture = write_three_song_fixture(tmp_path / "three-song.wav")
    analysis = analyze_long_mix(
        fixture,
        SourceMetadata(),
        JobOptions(max_segments=20, max_probes_per_segment=2),
    )
    music_segments = [segment for segment in analysis.segments if segment.kind == SegmentKind.MUSIC_UNRESOLVED]
    assert len(music_segments) >= 3
    boundaries = [segment.end_ms for segment in music_segments[:-1]]
    assert any(8_000 <= boundary <= 12_500 for boundary in boundaries)
    assert any(18_000 <= boundary <= 22_500 for boundary in boundaries)


def test_classify_label_widened_thresholds_accept_borderline_music() -> None:
    """T1.4: music_score 0.50 and chroma 0.25 (previously borderline below
    the 0.55/0.30 threshold) must now classify as MATCHED_TRACK so the probe
    loop visits these frames. Fixes "songs missed entirely"."""
    label = classify_label(
        music_score=0.50,
        speech_score=0.40,
        no_music_score=0.20,
        rms=0.35,
        chroma_strength=0.25,
    )
    assert label == SegmentKind.MATCHED_TRACK


def test_classify_label_still_flags_pure_speech() -> None:
    label = classify_label(
        music_score=0.20,
        speech_score=0.80,
        no_music_score=0.20,
        rms=0.30,
        chroma_strength=0.10,
    )
    assert label == SegmentKind.SPEECH_ONLY


def test_assign_repeat_groups_temporal_gate_splits_distant_same_feature_segments() -> None:
    """T2.1: two segments with moderately-similar features far apart in the
    timeline should NOT share a repeat group. Only cosine-similarity >= 0.96
    overrides the temporal gate."""
    # Cosine similarity between ``base`` and ``drifted`` is ~0.93 — above the
    # 0.88 match floor, below the 0.96 extreme-similarity escape hatch.
    base = np.array([1.0, 1.0, 1.0, 1.0], dtype=np.float32)
    drifted = np.array([1.0, 1.0, 0.70, 0.30], dtype=np.float32)
    segments = [
        SegmentDraft(
            start_ms=0,
            end_ms=20_000,
            kind=SegmentKind.MUSIC_UNRESOLVED,
            feature_vector=base,
            chroma_vector=base,
            music_ratio=1.0,
            speech_ratio=0.0,
        ),
        SegmentDraft(
            start_ms=30 * 60_000,  # 30 minutes later — well outside temporal window.
            end_ms=30 * 60_000 + 20_000,
            kind=SegmentKind.MUSIC_UNRESOLVED,
            feature_vector=drifted,
            chroma_vector=drifted,
            music_ratio=1.0,
            speech_ratio=0.0,
        ),
    ]
    assign_repeat_groups(segments, enabled=True)
    groups = [segment.repeat_group_id for segment in segments]
    assert groups[0] != groups[1]


def test_assign_repeat_groups_groups_identical_features_even_far_apart() -> None:
    """The temporal gate has an escape hatch for near-perfect audio similarity
    — e.g. the exact same chorus sampled at minute 0 and minute 30 of a
    live-recorded mix. similarity >= 0.96 overrides the window."""
    base = np.array([1.0, 1.0, 1.0, 1.0], dtype=np.float32)
    segments = [
        SegmentDraft(
            start_ms=0,
            end_ms=20_000,
            kind=SegmentKind.MUSIC_UNRESOLVED,
            feature_vector=base,
            chroma_vector=base,
            music_ratio=1.0,
            speech_ratio=0.0,
        ),
        SegmentDraft(
            start_ms=30 * 60_000,
            end_ms=30 * 60_000 + 20_000,
            kind=SegmentKind.MUSIC_UNRESOLVED,
            feature_vector=base.copy(),
            chroma_vector=base.copy(),
            music_ratio=1.0,
            speech_ratio=0.0,
        ),
    ]
    assign_repeat_groups(segments, enabled=True)
    assert segments[0].repeat_group_id == segments[1].repeat_group_id


def test_extract_feature_frames_casts_overlap_frame_count_to_int(monkeypatch, tmp_path: Path) -> None:
    fixture = tmp_path / "dummy.wav"
    fixture.write_bytes(b"RIFF")

    class FakeSoundFile:
        samplerate = 16_000

        def __init__(self, path: str) -> None:
            self.path = path

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def __len__(self) -> int:
            return 16_000 * 10

        def seek(self, offset: int) -> None:
            return None

        def read(self, frames, dtype: str, always_2d: bool = False):
            assert isinstance(frames, int)
            return np.zeros(frames, dtype=np.float32)

    monkeypatch.setitem(__import__("sys").modules, "soundfile", type("FakeSFModule", (), {"SoundFile": FakeSoundFile}))
    monkeypatch.setitem(__import__("sys").modules, "librosa", object())
    monkeypatch.setattr(
        "music_fetch.long_mix.compute_frame_features",
        lambda *args, **kwargs: [
            {
                "feature_vector": np.ones(4),
                "chroma_vector": np.ones(4),
                "music_score": 0.9,
                "speech_score": 0.1,
                "no_music_score": 0.0,
                "label": SegmentKind.MUSIC_UNRESOLVED,
            }
        ],
    )

    params = SegmentAnalysisParameters(
        hop_seconds=1.0,
        chunk_seconds=5,
        context_seconds=8.0,
        min_segment_ms=5_000,
        max_segment_ms=60_000,
        novelty_percentile=80.0,
    )
    frames = extract_feature_frames(fixture, recall_profile=JobOptions().recall_profile, params=params, duration_ms=10_000)
    assert frames
