from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

COMMON_BIN_DIRS = [
    "/opt/homebrew/bin",
    "/opt/homebrew/sbin",
    "/usr/local/bin",
    "/usr/local/sbin",
]


def sha1_text(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()


def now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


def json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def build_path(extra_dirs: list[str] | None = None) -> str:
    parts = []
    seen: set[str] = set()
    for part in (extra_dirs or []) + COMMON_BIN_DIRS + os.environ.get("PATH", "").split(os.pathsep):
        if not part:
            continue
        expanded = str(Path(part).expanduser())
        if expanded in seen:
            continue
        seen.add(expanded)
        parts.append(expanded)
    return os.pathsep.join(parts)


def which(binary: str) -> str | None:
    if "/" in binary:
        candidate = str(Path(binary).expanduser())
        return candidate if Path(candidate).exists() else None
    return shutil.which(binary, path=build_path())


def command_env(extra_dirs: list[str] | None = None, updates: dict[str, str] | None = None) -> dict[str, str]:
    merged_env = os.environ.copy()
    merged_env["PATH"] = build_path(extra_dirs)
    if updates:
        merged_env.update(updates)
    return merged_env


def run_command(args: list[str], *, cwd: Path | None = None, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    if args:
        resolved = which(args[0])
        if resolved:
            args = [resolved] + args[1:]
    merged_env = command_env(updates=env)
    return subprocess.run(args, cwd=cwd, env=merged_env, text=True, capture_output=True, check=False)


class TempDir:
    def __init__(self, prefix: str) -> None:
        self.path = Path(tempfile.mkdtemp(prefix=prefix))

    def cleanup(self) -> None:
        shutil.rmtree(self.path, ignore_errors=True)
