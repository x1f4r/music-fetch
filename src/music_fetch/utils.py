from __future__ import annotations

import hashlib
import json
import os
import shutil
import signal
import subprocess
import tempfile
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

COMMON_BIN_DIRS = [
    "/opt/homebrew/bin",
    "/opt/homebrew/sbin",
    "/usr/local/bin",
    "/usr/local/sbin",
]

_PROCESS_LOCK = threading.Lock()
_JOB_PROCESSES: dict[str, set[subprocess.Popen[str]]] = {}
_THREAD_CONTEXT = threading.local()


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


@contextmanager
def command_job_context(job_id: str | None) -> Iterator[None]:
    previous = getattr(_THREAD_CONTEXT, "job_id", None)
    _THREAD_CONTEXT.job_id = job_id
    try:
        yield
    finally:
        _THREAD_CONTEXT.job_id = previous


def cancel_job_processes(job_id: str) -> int:
    with _PROCESS_LOCK:
        processes = list(_JOB_PROCESSES.get(job_id, set()))
    killed = 0
    for process in processes:
        if process.poll() is not None:
            continue
        try:
            if os.name == "posix":
                os.killpg(process.pid, signal.SIGTERM)
            else:
                process.terminate()
            killed += 1
        except ProcessLookupError:
            continue
        except OSError:
            try:
                process.kill()
                killed += 1
            except OSError:
                continue
    return killed


def run_command(args: list[str], *, cwd: Path | None = None, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    if args:
        resolved = which(args[0])
        if resolved:
            args = [resolved] + args[1:]
    merged_env = command_env(updates=env)
    process = subprocess.Popen(
        args,
        cwd=cwd,
        env=merged_env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=(os.name == "posix"),
    )
    job_id = getattr(_THREAD_CONTEXT, "job_id", None)
    if job_id:
        with _PROCESS_LOCK:
            _JOB_PROCESSES.setdefault(job_id, set()).add(process)
    try:
        stdout, stderr = process.communicate()
        return subprocess.CompletedProcess(args, process.returncode, stdout, stderr)
    finally:
        if job_id:
            with _PROCESS_LOCK:
                processes = _JOB_PROCESSES.get(job_id)
                if processes is not None:
                    processes.discard(process)
                    if not processes:
                        _JOB_PROCESSES.pop(job_id, None)


class TempDir:
    def __init__(self, prefix: str) -> None:
        self.path = Path(tempfile.mkdtemp(prefix=prefix))

    def cleanup(self) -> None:
        shutil.rmtree(self.path, ignore_errors=True)
