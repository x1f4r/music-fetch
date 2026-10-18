"""What every full-track source hands to the refs stage."""

from __future__ import annotations

import atexit
import logging
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

log = logging.getLogger("mfidx.sources")
_RAM_DIR: Path | None = None


def ram_dir() -> Path:
    """A private directory in RAM (``/dev/shm``) for yt-dlp's working and temp files.

    yt-dlp writes HLS and DASH fragments next to its output before it streams them to stdout;
    running it here keeps those fragments off the disk. Removed at exit.
    """
    global _RAM_DIR
    if _RAM_DIR is None or not _RAM_DIR.exists():
        base = Path("/dev/shm")
        if not base.is_dir():
            raise RuntimeError("no /dev/shm: refusing to let yt-dlp write fragments to disk")
        _RAM_DIR = Path(tempfile.mkdtemp(prefix=f"mfidx-{os.getpid()}-", dir=base))
        atexit.register(shutil.rmtree, _RAM_DIR, ignore_errors=True)
    return _RAM_DIR


def ytdlp_ram_args() -> list[str]:
    """yt-dlp options that keep temporary files in ``ram_dir()``."""
    return ["--paths", f"temp:{ram_dir()}", "--no-cache-dir"]


@dataclass
class Pick:
    """One search result that may be the song's full recording."""

    ref_id: str  # "<source>:<id>", stored as track_melody.video_id
    title: str
    uploader: str
    duration: float | None
    score: float  # metadata plausibility, 0..1
    key: Any = None  # whatever the source needs to fetch it
