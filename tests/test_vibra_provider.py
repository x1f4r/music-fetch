from __future__ import annotations

from pathlib import Path

import pytest

from music_fetch.config import Settings
from music_fetch.providers.base import ProviderError
from music_fetch.providers.vibra import VibraProvider


def test_vibra_invalid_json_raises_provider_error(monkeypatch, tmp_path: Path) -> None:
    clip = tmp_path / "clip.wav"
    clip.write_bytes(b"fake")

    class Result:
        returncode = 0
        stdout = "rate limited"
        stderr = ""

    monkeypatch.setattr("music_fetch.providers.vibra.run_command", lambda args: Result())

    provider = VibraProvider(Settings(base_dir=str(tmp_path)))
    with pytest.raises(ProviderError):
        provider.recognize(clip, 0, 12_000)


def test_vibra_handles_empty_section_metadata(monkeypatch, tmp_path: Path) -> None:
    clip = tmp_path / "clip.wav"
    clip.write_bytes(b"fake")

    class Result:
        returncode = 0
        stdout = '{"track":{"title":"Slow Down","subtitle":"Cadmium","sections":[{"type":"SONG","metadata":[]}]}}'
        stderr = ""

    monkeypatch.setattr("music_fetch.providers.vibra.run_command", lambda args: Result())

    provider = VibraProvider(Settings(base_dir=str(tmp_path)))
    candidates = provider.recognize(clip, 0, 12_000)
    assert len(candidates) == 1
    assert candidates[0].track.title == "Slow Down"
    assert candidates[0].track.album is None
