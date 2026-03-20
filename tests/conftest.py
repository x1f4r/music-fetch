from __future__ import annotations

import math
import wave
from pathlib import Path

import pytest

from music_fetch.config import Settings
from music_fetch.db import Database
from music_fetch.service import JobManager


def write_test_tone(path: Path, seconds: float = 2.0, sample_rate: int = 16000) -> Path:
    frames = int(seconds * sample_rate)
    amplitude = 12000
    with wave.open(str(path), "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(sample_rate)
        samples = bytearray()
        for index in range(frames):
            value = int(amplitude * math.sin(2 * math.pi * 440 * index / sample_rate))
            samples += value.to_bytes(2, byteorder="little", signed=True)
        wav.writeframes(bytes(samples))
    return path


@pytest.fixture
def app_env(tmp_path: Path):
    settings = Settings(base_dir=str(tmp_path), max_workers=1)
    db = Database(settings.db_path)
    manager = JobManager(settings, db)
    return settings, db, manager
