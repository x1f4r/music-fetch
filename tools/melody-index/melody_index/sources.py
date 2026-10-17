"""What every full-track source hands to the refs stage."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class Pick:
    """One search result that may be the song's full recording."""

    ref_id: str  # "<source>:<id>", stored as track_melody.video_id
    title: str
    uploader: str
    duration: float | None
    score: float  # metadata plausibility, 0..1
    key: Any = None  # whatever the source needs to fetch it
