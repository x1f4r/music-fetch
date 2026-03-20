from __future__ import annotations

import json
from pathlib import Path

from ..config import Settings
from ..links import provider_search_links_from_shazam
from ..models import ProviderName, ProviderState, TrackCandidate, TrackMatch
from ..utils import run_command, which
from .base import BaseProvider, ProviderError


class VibraProvider(BaseProvider):
    name = ProviderName.VIBRA

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def state(self) -> ProviderState:
        available = which(self.settings.vibra_binary) is not None
        return ProviderState(
            name=self.name,
            enabled=True,
            available=available,
            reason=None if available else f"Missing binary: {self.settings.vibra_binary}",
        )

    def recognize(self, clip_path: Path, start_ms: int, end_ms: int) -> list[TrackCandidate]:
        result = run_command([self.settings.vibra_binary, "--recognize", "--file", str(clip_path)])
        if result.returncode != 0:
            raise ProviderError(result.stderr.strip() or "vibra failed")
        stdout = result.stdout.strip()
        if not stdout:
            raise ProviderError(result.stderr.strip() or "vibra returned no output")
        try:
            payload = json.loads(stdout)
        except json.JSONDecodeError as exc:
            detail = stdout[:200] if stdout else (result.stderr.strip() or str(exc))
            raise ProviderError(f"vibra returned invalid JSON: {detail}") from exc
        track = payload.get("track")
        if not track:
            return []
        external_links = provider_search_links_from_shazam(track)
        match = TrackMatch(
            title=track.get("title") or "Unknown track",
            artist=track.get("subtitle"),
            album=self._album_from_track(track),
            provider_ids={"shazam": str(track.get("key", ""))},
            external_links=external_links,
            raw=payload,
        )
        return [
            TrackCandidate(
                track=match,
                provider=self.name,
                confidence=0.72,
                start_ms=start_ms,
                end_ms=end_ms,
                evidence=[clip_path.name],
                raw=payload,
            )
        ]

    @staticmethod
    def _album_from_track(track: dict) -> str | None:
        for section in track.get("sections") or []:
            for metadata in section.get("metadata") or []:
                text = metadata.get("text")
                if text:
                    return str(text)
        return None
