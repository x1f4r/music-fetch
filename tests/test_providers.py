from __future__ import annotations

import json
from pathlib import Path

from music_fetch.config import Settings
from music_fetch.providers.audd import AudDProvider
from music_fetch.providers.vibra import VibraProvider


class DummyResponse:
    def __init__(self, payload: dict) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self.payload


def test_vibra_provider_parses_fixture(monkeypatch, tmp_path: Path) -> None:
    fixture = Path("tests/fixtures/providers/vibra_result.json").read_text()

    def fake_run(args):
        class Result:
            returncode = 0
            stdout = fixture
            stderr = ""

        return Result()

    monkeypatch.setattr("music_fetch.providers.vibra.run_command", fake_run)
    provider = VibraProvider(Settings(base_dir=str(tmp_path), vibra_binary="vibra"))
    results = provider.recognize(tmp_path / "clip.wav", 0, 12000)
    assert results[0].track.title == "Bound 2"
    assert results[0].track.artist == "Kanye West"
    assert "spotify" in results[0].track.external_links
    assert "youtube_music" in results[0].track.external_links


def test_audd_provider_parses_fixture(monkeypatch, tmp_path: Path) -> None:
    payload = json.loads(Path("tests/fixtures/providers/audd_result.json").read_text())
    clip = tmp_path / "clip.wav"
    clip.write_bytes(b"wav")

    def fake_post(*args, **kwargs):
        return DummyResponse(payload)

    monkeypatch.setattr("music_fetch.providers.audd.httpx.post", fake_post)
    provider = AudDProvider("token")
    results = provider.recognize(clip, 0, 12000)
    assert results[0].track.title == "Warriors"
    assert results[0].track.isrc == "USUM71414163"
    assert "deezer" in results[0].track.external_links
