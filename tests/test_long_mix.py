from __future__ import annotations

import math
import wave
from pathlib import Path

import numpy as np

from music_fetch.long_mix import SegmentDraft, analyze_long_mix, choose_probe_windows
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
