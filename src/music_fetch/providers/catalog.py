from __future__ import annotations

import json
from pathlib import Path

from ..config import Settings
from ..db import Database
from ..models import ProviderName, ProviderState, TrackCandidate, TrackMatch
from ..utils import run_command, sha1_text, which
from .base import BaseProvider, ProviderError


class LocalCatalogProvider(BaseProvider):
    name = ProviderName.LOCAL_CATALOG

    def __init__(self, settings: Settings, db: Database) -> None:
        self.settings = settings
        self.db = db

    def state(self) -> ProviderState:
        available = which(self.settings.fpcalc_binary) is not None
        catalog_tracks = len(self.db.list_catalog_tracks())
        return ProviderState(
            name=self.name,
            enabled=True,
            available=available and catalog_tracks > 0,
            reason=None if available else f"Missing binary: {self.settings.fpcalc_binary}",
            config={"track_count": catalog_tracks},
        )

    def fingerprint(self, path: Path) -> dict:
        result = run_command([self.settings.fpcalc_binary, "-raw", "-json", str(path)])
        if result.returncode != 0:
            raise ProviderError(result.stderr.strip() or "fpcalc failed")
        return json.loads(result.stdout)

    def import_paths(self, paths: list[Path]) -> int:
        count = 0
        for path in paths:
            if path.is_dir():
                for child in path.rglob("*"):
                    if child.is_file() and child.suffix.lower() in {".mp3", ".wav", ".m4a", ".flac", ".ogg", ".mp4", ".mkv"}:
                        count += self._import_file(child)
            elif path.is_file():
                count += self._import_file(path)
        return count

    def _import_file(self, path: Path) -> int:
        try:
            fingerprint = self.fingerprint(path)
        except ProviderError:
            return 0
        stem = path.stem
        artist = None
        title = stem
        if " - " in stem:
            artist, title = stem.split(" - ", 1)
        self.db.add_catalog_track(sha1_text(str(path.resolve())), str(path.resolve()), title, artist, None, fingerprint)
        return 1

    def recognize(self, clip_path: Path, start_ms: int, end_ms: int) -> list[TrackCandidate]:
        if which(self.settings.fpcalc_binary) is None:
            return []
        fingerprint = self.fingerprint(clip_path)
        target_fp = fingerprint.get("fingerprint") or []
        target_duration = float(fingerprint.get("duration") or 0.0)
        if not target_fp:
            return []
        best_score = 0.0
        best_track = None
        for row in self.db.list_catalog_tracks():
            candidate = json.loads(row["fingerprint_json"])
            score = fingerprint_similarity(target_fp, candidate.get("fingerprint") or [])
            duration = float(candidate.get("duration") or 0.0)
            if duration and target_duration:
                ratio = min(target_duration, duration) / max(target_duration, duration)
                score *= 0.7 + 0.3 * ratio
            if score > best_score:
                best_score = score
                best_track = row
        if not best_track or best_score < 0.20:
            return []
        match = TrackMatch(
            title=best_track["title"] or Path(best_track["path"]).stem,
            artist=best_track["artist"],
            album=best_track["album"],
            provider_ids={"catalog": best_track["id"]},
            external_links={"file": best_track["path"]},
        )
        return [
            TrackCandidate(
                track=match,
                provider=self.name,
                confidence=min(0.95, max(0.25, best_score)),
                start_ms=start_ms,
                end_ms=end_ms,
                evidence=[clip_path.name],
                raw={"catalog_path": best_track["path"], "similarity": best_score},
            )
        ]


def fingerprint_similarity(a: list[int], b: list[int]) -> float:
    if not a or not b:
        return 0.0
    limit = min(len(a), len(b), 120)
    matches = sum(1 for index in range(limit) if abs(a[index] - b[index]) < 10)
    return matches / limit
