from __future__ import annotations

from pathlib import Path

from ..models import ProviderName, ProviderState, TrackCandidate


class ProviderError(RuntimeError):
    pass


class BaseProvider:
    name: ProviderName

    def state(self) -> ProviderState:
        raise NotImplementedError

    def recognize(self, clip_path: Path, start_ms: int, end_ms: int) -> list[TrackCandidate]:
        raise NotImplementedError
