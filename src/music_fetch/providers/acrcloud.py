from __future__ import annotations

import base64
import hashlib
import hmac
from pathlib import Path
from time import time

import httpx

from ..links import build_search_links
from ..models import ProviderName, ProviderState, TrackCandidate, TrackMatch
from .base import BaseProvider


class ACRCloudProvider(BaseProvider):
    name = ProviderName.ACRCLOUD

    def __init__(self, host: str | None, access_key: str | None, access_secret: str | None) -> None:
        self.host = host
        self.access_key = access_key
        self.access_secret = access_secret

    def state(self) -> ProviderState:
        configured = all([self.host, self.access_key, self.access_secret])
        return ProviderState(
            name=self.name,
            enabled=configured,
            available=configured,
            reason=None if configured else "Set host, access_key, access_secret",
            config={"configured": configured, "host": self.host or ""},
        )

    def recognize(self, clip_path: Path, start_ms: int, end_ms: int) -> list[TrackCandidate]:
        if not all([self.host, self.access_key, self.access_secret]):
            return []
        data_type = "audio"
        signature_version = "1"
        timestamp = str(int(time()))
        string_to_sign = "\n".join(["POST", "/v1/identify", self.access_key, data_type, signature_version, timestamp])
        signature = base64.b64encode(
            hmac.new(self.access_secret.encode(), string_to_sign.encode(), hashlib.sha1).digest()
        ).decode()
        with clip_path.open("rb") as handle:
            payload = {
                "access_key": self.access_key,
                "data_type": data_type,
                "signature_version": signature_version,
                "signature": signature,
                "timestamp": timestamp,
                "sample_bytes": str(clip_path.stat().st_size),
            }
            response = httpx.post(
                f"https://{self.host}/v1/identify",
                data=payload,
                files={"sample": (clip_path.name, handle, "audio/wav")},
                timeout=60.0,
            )
        response.raise_for_status()
        body = response.json()
        music = (body.get("metadata") or {}).get("music") or []
        if not music:
            return []
        top = music[0]
        artist_names = ", ".join(artist["name"] for artist in top.get("artists") or [])
        match = TrackMatch(
            title=top["title"],
            artist=artist_names,
            album=(top.get("album") or {}).get("name"),
            isrc=top.get("external_ids", {}).get("isrc"),
            provider_ids={"acrcloud": top.get("acrid", "")},
            external_links=build_search_links(top["title"], artist_names),
            raw=body,
        )
        score = float(body.get("metadata", {}).get("score", 80)) / 100.0
        return [
            TrackCandidate(
                track=match,
                provider=self.name,
                confidence=max(0.5, min(score, 0.95)),
                start_ms=start_ms,
                end_ms=end_ms,
                evidence=[clip_path.name],
                raw=body,
            )
        ]
