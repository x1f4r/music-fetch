import pytest
from pydantic import ValidationError

from music_fetch.models import JobCreate, JobOptions


def test_job_create_rejects_empty_inputs() -> None:
    with pytest.raises(ValidationError):
        JobCreate(inputs=[])


def test_job_create_rejects_blank_inputs() -> None:
    with pytest.raises(ValidationError):
        JobCreate(inputs=["https://example.com/video", "  "])


def test_job_create_strips_input_whitespace() -> None:
    payload = JobCreate(inputs=["  https://example.com/video  "])

    assert payload.inputs == ["https://example.com/video"]


@pytest.mark.parametrize(
    "field,value",
    [
        ("window_ms", 0),
        ("hop_ms", 0),
        ("max_windows", 0),
        ("max_segments", 0),
        ("max_probes_per_segment", 0),
        ("min_provider_consensus", 0),
        ("max_provider_calls", -1),
        ("merge_gap_same_track_ms", -1),
        ("merge_gap_bridge_ms", -1),
        ("segment_workers", -1),
        ("segment_workers", 33),
    ],
)
def test_job_options_reject_pathological_values(field: str, value: int) -> None:
    with pytest.raises(ValidationError):
        JobOptions(**{field: value})


def test_job_options_preserve_meaningful_zero_values() -> None:
    options = JobOptions(max_provider_calls=0, segment_workers=0)

    assert options.max_provider_calls == 0
    assert options.segment_workers == 0
