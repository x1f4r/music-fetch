from __future__ import annotations

import importlib.util
from dataclasses import dataclass

from .config import Settings
from .utils import which


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str


def run_doctor(settings: Settings) -> list[CheckResult]:
    results = [
        CheckResult("ffmpeg", which("ffmpeg") is not None, which("ffmpeg") or "Missing ffmpeg"),
        CheckResult("yt-dlp", which("yt-dlp") is not None, which("yt-dlp") or "Missing yt-dlp"),
        CheckResult("deno", which("deno") is not None, which("deno") or "Recommended for yt-dlp YouTube JS challenges"),
        CheckResult("vibra", which(settings.vibra_binary) is not None, which(settings.vibra_binary) or "Optional"),
        CheckResult("fpcalc", which(settings.fpcalc_binary) is not None, which(settings.fpcalc_binary) or "Optional"),
        CheckResult(
            "audio-separator",
            importlib.util.find_spec("audio_separator") is not None,
            "Python package available" if importlib.util.find_spec("audio_separator") else "Optional pip extra missing",
        ),
    ]
    return results
