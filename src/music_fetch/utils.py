from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import signal
import subprocess
import tempfile
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator


logger = logging.getLogger(__name__)


# Wall-clock cap for an individual subprocess invocation. Tunable via env so
# operators with very long downloads or large-file separations can raise it
# without code changes.
DEFAULT_COMMAND_TIMEOUT_SECONDS = float(os.environ.get("MUSIC_FETCH_COMMAND_TIMEOUT", "900"))

# How long to wait between SIGTERM and SIGKILL when force-canceling. Some
# subprocesses (notably torch-backed audio_separator and certain ffmpeg
# builds) ignore SIGTERM; without escalation the cancel "succeeds" while the
# child keeps spinning.
KILL_GRACE_SECONDS = float(os.environ.get("MUSIC_FETCH_KILL_GRACE", "3.0"))


class CommandTimeoutError(RuntimeError):
    """Raised when a subprocess exceeds its wall-clock budget."""

    def __init__(self, args: list[str], timeout: float) -> None:
        super().__init__(f"Command timed out after {timeout:.0f}s: {' '.join(args[:1])}")
        self.args = args
        self.timeout = timeout

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


def _send_signal(process: subprocess.Popen[str], sig: int) -> bool:
    """Send ``sig`` to ``process`` (the whole process group on POSIX).

    Returns True when the signal was delivered (or appeared to be), False
    when the process was already gone.
    """
    if process.poll() is not None:
        return False
    try:
        if os.name == "posix":
            os.killpg(process.pid, sig)
        else:
            # Windows has no SIGTERM/SIGKILL distinction; ``terminate`` and
            # ``kill`` both call TerminateProcess.
            if sig == signal.SIGKILL:
                process.kill()
            else:
                process.terminate()
        return True
    except ProcessLookupError:
        return False
    except OSError:
        return False


def cancel_job_processes(job_id: str, *, grace_seconds: float | None = None) -> int:
    """Kill every subprocess registered to ``job_id``.

    Sends SIGTERM first, waits up to ``grace_seconds`` for the children to
    exit voluntarily, then escalates to SIGKILL for any survivors. Returns
    the number of processes that received the initial SIGTERM.
    """
    grace = KILL_GRACE_SECONDS if grace_seconds is None else max(0.0, grace_seconds)
    with _PROCESS_LOCK:
        processes = list(_JOB_PROCESSES.get(job_id, set()))

    terminated: list[subprocess.Popen[str]] = []
    for process in processes:
        if _send_signal(process, signal.SIGTERM):
            terminated.append(process)

    if not terminated:
        return 0

    deadline = time.monotonic() + grace
    survivors: list[subprocess.Popen[str]] = []
    for process in terminated:
        remaining = max(0.0, deadline - time.monotonic())
        try:
            process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            survivors.append(process)

    for process in survivors:
        _send_signal(process, signal.SIGKILL)
        # Best-effort reap; don't block forever if a kill somehow doesn't
        # land (zombie process, kernel weirdness).
        try:
            process.wait(timeout=1.0)
        except subprocess.TimeoutExpired:
            logger.warning("job %s subprocess %s ignored SIGKILL", job_id, process.pid)

    return len(terminated)


def run_command(
    args: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    timeout: float | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run ``args`` to completion with a wall-clock budget.

    ``timeout`` defaults to ``DEFAULT_COMMAND_TIMEOUT_SECONDS``. When the
    budget is exceeded the subprocess (and its process group on POSIX) is
    terminated and a :class:`CommandTimeoutError` is raised. Without this,
    a hung yt-dlp / ffmpeg / audio_separator would pin a worker thread
    forever and — combined with the fixed pool size — starve the queue.
    """
    if args:
        resolved = which(args[0])
        if resolved:
            args = [resolved] + args[1:]
    merged_env = command_env(updates=env)
    effective_timeout = timeout if timeout is not None else DEFAULT_COMMAND_TIMEOUT_SECONDS
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
        try:
            stdout, stderr = process.communicate(timeout=effective_timeout)
        except subprocess.TimeoutExpired:
            _send_signal(process, signal.SIGTERM)
            try:
                stdout, stderr = process.communicate(timeout=KILL_GRACE_SECONDS)
            except subprocess.TimeoutExpired:
                _send_signal(process, signal.SIGKILL)
                # Best-effort drain so the pipes don't leak.
                try:
                    stdout, stderr = process.communicate(timeout=1.0)
                except subprocess.TimeoutExpired:
                    stdout, stderr = "", ""
            logger.warning("command timed out after %.0fs: %s", effective_timeout, args[:1])
            raise CommandTimeoutError(args, effective_timeout)
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
