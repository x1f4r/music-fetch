from __future__ import annotations

import importlib.util
from dataclasses import dataclass
from pathlib import Path

from .config import Settings
from .doctor import CheckResult, run_doctor
from .utils import command_env, run_command, which


@dataclass
class InstallRunResult:
    installed: list[str]
    skipped: list[str]
    failed: list[str]
    checks: list[CheckResult]


def install_dependencies(settings: Settings, *, include_optional: bool = False) -> InstallRunResult:
    installed: list[str] = []
    skipped: list[str] = []
    failed: list[str] = []

    brew = which("brew")
    if brew is None:
        raise RuntimeError("Homebrew is required to install dependencies automatically.")

    core_packages = [
        ("ffmpeg", "ffmpeg"),
        ("yt-dlp", "yt-dlp"),
        ("deno", "deno"),
        ("fpcalc", "chromaprint"),
    ]
    optional_packages = [
        ("vibra", None),
        ("audio-separator", None),
    ]

    for binary, formula in core_packages:
        if which(binary):
            skipped.append(binary)
            continue
        result = run_command([brew, "install", formula])
        if result.returncode == 0 and which(binary):
            installed.append(binary)
        else:
            failed.append(binary)

    if include_optional:
        if which(settings.vibra_binary):
            skipped.append("vibra")
        else:
            result = install_vibra()
            if result:
                installed.append("vibra")
            else:
                failed.append("vibra")

        if importlib.util.find_spec("audio_separator") is not None:
            skipped.append("audio-separator")
        else:
            result = run_command(["uv", "sync", "--extra", "separation"], cwd=project_root())
            if result.returncode == 0 and importlib.util.find_spec("audio_separator") is not None:
                installed.append("audio-separator")
            else:
                failed.append("audio-separator")

    return InstallRunResult(installed=installed, skipped=skipped, failed=failed, checks=run_doctor(settings))


def install_vibra() -> bool:
    if which("vibra"):
        return True
    script = """
set -euo pipefail
workdir="$(mktemp -d)"
git clone https://github.com/BayernMuller/vibra.git "$workdir/vibra"
cmake -S "$workdir/vibra" -B "$workdir/vibra/build"
cmake --build "$workdir/vibra/build" --parallel
sudo cmake --install "$workdir/vibra/build"
"""
    result = run_command(["/bin/zsh", "-lc", script], env=command_env())
    return result.returncode == 0 and which("vibra") is not None


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]
