from __future__ import annotations

from pathlib import Path

import httpx

from ..links import build_search_links
from ..models import ProviderName, ProviderState, TrackCandidate, TrackMatch
from .base import BaseProvider


class AudDProvider(BaseProvider):
    name = ProviderName.AUDD

    def __init__(self, token: str | None) -> None:
        self.token = token

    def state(self) -> ProviderState:
        return ProviderState(
            name=self.name,
            enabled=bool(self.token),
            available=bool(self.token),
            reason=None if self.token else "Set provider config with api_token",
            config={"configured": bool(self.token)},
        )

    def recognize(self, clip_path: Path, start_ms: int, end_ms: int) -> list[TrackCandidate]:
        if not self.token:
            return []
        with clip_path.open("rb") as handle:
            response = httpx.post(
                "https://api.audd.io/",
                data={"api_token": self.token, "return": "apple_music,spotify"},
                files={"file": (clip_path.name, handle, "audio/wav")},
                timeout=60.0,
            )
        response.raise_for_status()
        payload = response.json()
        result = payload.get("result")
        if not result:
            return []
        external_links = build_search_links(result["title"], result.get("artist"))
        external_links.update(
            {
                "audd": result.get("song_link", ""),
                "spotify": (result.get("spotify") or {}).get("external_urls", {}).get("spotify", external_links.get("spotify", "")),
                "apple_music": (result.get("apple_music") or {}).get("url", external_links.get("apple_music", "")),
            }
        )
        match = TrackMatch(
            title=result["title"],
            artist=result.get("artist"),
            album=result.get("album"),
            isrc=(result.get("spotify") or {}).get("external_ids", {}).get("isrc"),
            provider_ids={"audd": result.get("song_link", "")},
            external_links={key: value for key, value in external_links.items() if value},
            raw=payload,
        )
        return [
            TrackCandidate(
                track=match,
                provider=self.name,
                confidence=0.83,
                start_ms=start_ms,
                end_ms=end_ms,
                evidence=[clip_path.name],
                raw=payload,
            )
        ]
