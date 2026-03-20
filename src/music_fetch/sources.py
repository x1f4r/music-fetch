from __future__ import annotations

import mimetypes
import json
import uuid
from pathlib import Path
from urllib.parse import urlparse

import httpx

from .models import ItemStatus, SourceItem, SourceKind, SourceMetadata
from .utils import run_command, sha1_text


def yt_dlp_base_args() -> list[str]:
    return [
        "yt-dlp",
        "--no-progress",
        "--no-warnings",
        "--remote-components",
        "ejs:github",
        "--js-runtimes",
        "deno",
    ]


def yt_dlp_extract_info(url: str) -> dict:
    args = yt_dlp_base_args() + ["--dump-single-json", "--skip-download", url]
    result = run_command(args)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"yt-dlp failed for {url}")
    return json.loads(result.stdout)


def is_url(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"}


def is_direct_media_url(value: str) -> bool:
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"}:
        return False
    mime, _ = mimetypes.guess_type(parsed.path)
    return bool(mime and (mime.startswith("audio/") or mime.startswith("video/")))


class SourceResolver:
    def __init__(self, cache_dir: Path) -> None:
        self.cache_dir = cache_dir

    def resolve_inputs(self, job_id: str, inputs: list[str]) -> list[SourceItem]:
        items: list[SourceItem] = []
        for raw in inputs:
            if is_url(raw):
                if is_direct_media_url(raw):
                    items.append(self._direct_http_item(job_id, raw))
                else:
                    items.extend(self._yt_dlp_items(job_id, raw))
            else:
                items.append(self._local_file_item(job_id, raw))
        return items

    def _local_file_item(self, job_id: str, raw: str) -> SourceItem:
        path = Path(raw).expanduser().resolve()
        metadata = SourceMetadata(title=path.name, extra={"resolved_path": str(path)})
        return SourceItem(
            id=str(uuid.uuid4()),
            job_id=job_id,
            input_value=raw,
            kind=SourceKind.LOCAL_FILE,
            status=ItemStatus.QUEUED,
            metadata=metadata,
            local_path=str(path),
        )

    def _direct_http_item(self, job_id: str, raw: str) -> SourceItem:
        parsed = urlparse(raw)
        filename = Path(parsed.path).name or sha1_text(raw)
        metadata = SourceMetadata(title=filename, webpage_url=raw)
        return SourceItem(
            id=str(uuid.uuid4()),
            job_id=job_id,
            input_value=raw,
            kind=SourceKind.DIRECT_HTTP,
            status=ItemStatus.QUEUED,
            metadata=metadata,
            download_url=raw,
        )

    def _yt_dlp_items(self, job_id: str, raw: str) -> list[SourceItem]:
        info = yt_dlp_extract_info(raw)
        entries = _flatten_entries(info.get("entries") or [])
        if entries:
            items: list[SourceItem] = []
            playlist_title = info.get("title")
            playlist_id = info.get("id")
            for index, entry in enumerate(entries, start=1):
                if not entry:
                    continue
                items.append(self._from_yt_entry(job_id, raw, entry, playlist_id, playlist_title, index))
            return items
        return [self._from_yt_entry(job_id, raw, info, None, None, None)]

    def _from_yt_entry(
        self,
        job_id: str,
        raw: str,
        entry: dict,
        playlist_id: str | None,
        playlist_title: str | None,
        entry_index: int | None,
    ) -> SourceItem:
        duration = entry.get("duration")
        chapters = entry.get("chapters") or []
        download_url = _entry_download_url(entry, raw, playlist_id)
        metadata_only = bool(entry.get("title")) and not download_url
        track_artist = entry.get("artist") or entry.get("creator") or entry.get("uploader") or entry.get("channel")
        metadata = SourceMetadata(
            title=entry.get("track") or entry.get("title"),
            extractor=entry.get("extractor_key") or entry.get("extractor"),
            webpage_url=entry.get("webpage_url") or raw,
            channel=entry.get("channel"),
            uploader=entry.get("uploader"),
            duration_ms=int(duration * 1000) if duration else None,
            chapters=chapters,
            description=entry.get("description"),
            playlist_id=playlist_id,
            playlist_title=playlist_title,
            entry_index=entry_index,
            extra={
                "id": entry.get("id"),
                "url": entry.get("url"),
                "playlist_source_url": raw,
                "track_title": entry.get("track") or entry.get("title"),
                "track_artist": track_artist,
                "track_album": entry.get("album"),
                "metadata_only": metadata_only,
            },
        )
        return SourceItem(
            id=str(uuid.uuid4()),
            job_id=job_id,
            input_value=raw,
            kind=SourceKind.YT_DLP,
            status=ItemStatus.QUEUED,
            metadata=metadata,
            download_url=download_url,
        )


def _flatten_entries(entries: list[dict | None]) -> list[dict]:
    flattened: list[dict] = []
    for entry in entries:
        if not entry:
            continue
        nested = entry.get("entries")
        if nested:
            flattened.extend(_flatten_entries(nested))
        else:
            flattened.append(entry)
    return flattened


def _entry_download_url(entry: dict, raw: str, playlist_id: str | None) -> str | None:
    for candidate in [entry.get("webpage_url"), entry.get("original_url"), entry.get("url")]:
        if isinstance(candidate, str) and candidate.startswith(("http://", "https://")):
            return candidate

    entry_id = entry.get("id")
    extractor = str(entry.get("extractor_key") or entry.get("extractor") or "").lower()
    if entry_id and "youtube" in extractor:
        host = "music.youtube.com" if "music" in raw or "music" in extractor else "www.youtube.com"
        if playlist_id:
            return f"https://{host}/watch?v={entry_id}&list={playlist_id}"
        return f"https://{host}/watch?v={entry_id}"

    if entry_id and "vimeo" in extractor:
        return f"https://vimeo.com/{entry_id}"

    return None


def download_direct_http(url: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with httpx.stream("GET", url, follow_redirects=True, timeout=60.0) as response:
        response.raise_for_status()
        with dest.open("wb") as handle:
            for chunk in response.iter_bytes():
                handle.write(chunk)
    return dest
