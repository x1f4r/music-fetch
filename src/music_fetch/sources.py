from __future__ import annotations

import ipaddress
import logging
import mimetypes
import json
import socket
import uuid
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import httpx

from .models import ItemStatus, SourceItem, SourceKind, SourceMetadata
from .utils import run_command, sha1_text


logger = logging.getLogger(__name__)

# Cap on a direct-HTTP download response body. Without this, a hostile or
# misconfigured server can stream forever and fill the cache disk. Matches
# the default upload cap (4 GiB).
DIRECT_HTTP_MAX_BYTES = 4 * 1024 * 1024 * 1024
# Cap on the number of redirects we follow when probing/downloading. Each
# hop is host-validated, so this also bounds the SSRF check fan-out.
DIRECT_HTTP_MAX_REDIRECTS = 5


class UnsafeURLError(RuntimeError):
    """Raised when a direct-HTTP target resolves to a non-public address.

    Loopback (127.0.0.0/8, ::1), link-local (169.254.0.0/16, fe80::/10),
    multicast, broadcast, reserved, and private RFC1918 ranges are all
    rejected — these are the targets that turn a user-supplied URL into an
    SSRF vector against cloud metadata services or internal admin panels.
    """


def _assert_safe_external_host(host: str) -> None:
    """Resolve ``host`` and reject any address that's not globally routable.

    Done synchronously with the system resolver, which mirrors what httpx
    will do a moment later. There is still a TOCTOU window (a hostile DNS
    server could return a public IP here and a private IP to httpx), but
    blocking the obvious cases is materially safer than not checking.
    """
    cleaned = (host or "").strip().lower()
    if not cleaned:
        raise UnsafeURLError("URL has no host")
    # Strip an embedded port like "127.0.0.1:8080".
    if cleaned.startswith("["):
        # IPv6 literal: [::1]:8080
        end = cleaned.find("]")
        if end == -1:
            raise UnsafeURLError(f"Malformed IPv6 host: {host}")
        cleaned = cleaned[1:end]
    elif ":" in cleaned and cleaned.count(":") == 1:
        cleaned = cleaned.split(":", 1)[0]

    try:
        infos = socket.getaddrinfo(cleaned, None)
    except socket.gaierror as exc:
        raise UnsafeURLError(f"Cannot resolve host: {cleaned}") from exc

    for info in infos:
        sockaddr = info[4]
        try:
            ip = ipaddress.ip_address(sockaddr[0])
        except ValueError:
            raise UnsafeURLError(f"Host {cleaned} resolves to non-IP address {sockaddr[0]}")
        if (
            ip.is_loopback
            or ip.is_link_local
            or ip.is_private
            or ip.is_multicast
            or ip.is_reserved
            or ip.is_unspecified
        ):
            raise UnsafeURLError(f"Host {cleaned} resolves to non-public address {ip}")


def _assert_safe_external_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise UnsafeURLError(f"Unsupported scheme: {parsed.scheme!r}")
    _assert_safe_external_host(parsed.netloc or parsed.hostname or "")

KNOWN_EXTRACTOR_HOST_TOKENS = ("youtube.", "youtu.be", "instagram.", "tiktok.", "vimeo.", "soundcloud.")
KNOWN_SHORTENER_HOSTS = (
    "youtu.be",
    "t.co",
    "vm.tiktok.com",
    "vt.tiktok.com",
    "spotify.link",
    "spoti.fi",
    "deezer.page.link",
    "redd.it",
    "www.reddit.com",
    "l.instagram.com",
)
GENERIC_TRACKING_QUERY_KEYS = {
    "fbclid",
    "gclid",
    "igsh",
    "mc_cid",
    "mc_eid",
    "rdt",
    "si",
    "spm",
    "s",
    "share_id",
    "share_app_id",
    "utm_campaign",
    "utm_content",
    "utm_id",
    "utm_medium",
    "utm_name",
    "utm_source",
    "utm_term",
}


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


def normalize_source_url(value: str) -> str:
    parsed = urlparse(value.strip())
    if parsed.scheme not in {"http", "https"}:
        return value

    resolved = _resolve_known_short_url(value)
    parsed = urlparse(resolved)
    host = (parsed.hostname or "").lower()

    if host == "l.instagram.com":
        for key, item in parse_qsl(parsed.query, keep_blank_values=False):
            if key == "u" and item.startswith(("http://", "https://")):
                return normalize_source_url(item)

    normalizers = [
        _normalize_youtube_url,
        _normalize_instagram_url,
        _normalize_tiktok_url,
        _normalize_x_url,
        _normalize_spotify_url,
        _normalize_deezer_url,
        _normalize_tidal_url,
        _normalize_reddit_url,
        _normalize_pornhub_url,
        _normalize_vimeo_url,
        _normalize_soundcloud_url,
    ]
    for normalizer in normalizers:
        candidate = normalizer(parsed)
        if candidate is not None:
            return candidate
    return _rebuild_url(parsed, host=_canonical_host(host))


def _resolve_known_short_url(value: str) -> str:
    parsed = urlparse(value)
    host = (parsed.hostname or "").lower()
    if host not in KNOWN_SHORTENER_HOSTS:
        return value
    try:
        with httpx.Client(follow_redirects=True, timeout=8.0) as client:
            response = client.head(value)
            if response.status_code >= 400 or str(response.url) == value:
                response = client.get(value)
            return str(response.url)
    except Exception:
        return value


def _canonical_host(host: str) -> str:
    if host in {"youtube.com", "www.youtube.com", "m.youtube.com"}:
        return "www.youtube.com"
    if host == "music.youtube.com":
        return host
    if host in {"instagram.com", "www.instagram.com", "m.instagram.com"}:
        return "www.instagram.com"
    if host in {"tiktok.com", "www.tiktok.com", "m.tiktok.com"}:
        return "www.tiktok.com"
    if host in {"twitter.com", "www.twitter.com", "mobile.twitter.com", "x.com", "www.x.com", "mobile.x.com"}:
        return "x.com"
    if host in {"spotify.com", "www.spotify.com", "open.spotify.com"}:
        return "open.spotify.com"
    if host in {"deezer.com", "www.deezer.com"}:
        return "www.deezer.com"
    if host in {"tidal.com", "www.tidal.com", "listen.tidal.com"}:
        return "listen.tidal.com"
    if host in {"reddit.com", "www.reddit.com", "old.reddit.com", "new.reddit.com"}:
        return "www.reddit.com"
    if host in {"pornhub.com", "www.pornhub.com"}:
        return "www.pornhub.com"
    if host in {"vimeo.com", "www.vimeo.com"}:
        return "vimeo.com"
    if host in {"soundcloud.com", "www.soundcloud.com", "m.soundcloud.com"}:
        return "soundcloud.com"
    return host


def _rebuild_url(
    parsed,
    *,
    host: str | None = None,
    path: str | None = None,
    query_items: list[tuple[str, str]] | None = None,
) -> str:
    final_host = host or _canonical_host((parsed.hostname or "").lower())
    final_path = path if path is not None else (parsed.path or "")
    final_path = final_path or "/"
    if final_path != "/" and final_path.endswith("/"):
        final_path = final_path.rstrip("/")
    query = urlencode(query_items or [], doseq=True)
    return urlunparse(("https", final_host, final_path, "", query, ""))


def _filtered_query_items(parsed, allowed_keys: set[str] | None = None) -> list[tuple[str, str]]:
    items: list[tuple[str, str]] = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=False):
        if key.startswith("utm_") or key in GENERIC_TRACKING_QUERY_KEYS:
            continue
        if allowed_keys is not None and key not in allowed_keys:
            continue
        items.append((key, value))
    return items


def _normalize_youtube_url(parsed) -> str | None:
    host = (parsed.hostname or "").lower()
    path = parsed.path or ""
    if "youtube." not in host and "youtu.be" not in host:
        return None

    single_video_url = _single_video_extractor_url(urlunparse(parsed))
    if single_video_url:
        single_parsed = urlparse(single_video_url)
        keep = _filtered_query_items(single_parsed, {"v", "t", "start"})
        return _rebuild_url(single_parsed, host=_canonical_host(host), path=single_parsed.path, query_items=keep)

    if path == "/playlist":
        keep = _filtered_query_items(parsed, {"list", "index"})
        return _rebuild_url(parsed, host=_canonical_host(host), path=path, query_items=keep)
    return _rebuild_url(parsed, host=_canonical_host(host), path=path, query_items=_filtered_query_items(parsed))


def _normalize_instagram_url(parsed) -> str | None:
    host = (parsed.hostname or "").lower()
    path = parsed.path or ""
    if "instagram." not in host:
        return None
    parts = [segment for segment in path.split("/") if segment]
    if len(parts) >= 2 and parts[0] in {"p", "reel", "reels", "tv"}:
        path = "/" + "/".join(parts[:2])
        keep = _filtered_query_items(parsed, {"img_index"})
    else:
        keep = _filtered_query_items(parsed)
    return _rebuild_url(parsed, host=_canonical_host(host), path=path, query_items=keep)


def _normalize_tiktok_url(parsed) -> str | None:
    host = (parsed.hostname or "").lower()
    path = parsed.path or ""
    if "tiktok." not in host:
        return None
    parts = [segment for segment in path.split("/") if segment]
    if len(parts) >= 3 and parts[0].startswith("@") and parts[1] == "video":
        path = "/" + "/".join(parts[:3])
    return _rebuild_url(parsed, host=_canonical_host(host), path=path, query_items=[])


def _normalize_x_url(parsed) -> str | None:
    host = (parsed.hostname or "").lower()
    path = parsed.path or ""
    if host not in {"twitter.com", "www.twitter.com", "mobile.twitter.com", "x.com", "www.x.com", "mobile.x.com"}:
        return None
    parts = [segment for segment in path.split("/") if segment]
    if len(parts) >= 3 and parts[1] == "status":
        path = "/" + "/".join(parts[:3])
    return _rebuild_url(parsed, host=_canonical_host(host), path=path, query_items=[])


def _normalize_spotify_url(parsed) -> str | None:
    host = (parsed.hostname or "").lower()
    if "spotify." not in host:
        return None
    parts = [segment for segment in (parsed.path or "").split("/") if segment]
    if parts and parts[0].startswith("intl-"):
        parts = parts[1:]
    path = "/" + "/".join(parts)
    return _rebuild_url(parsed, host=_canonical_host(host), path=path, query_items=_filtered_query_items(parsed))


def _normalize_deezer_url(parsed) -> str | None:
    host = (parsed.hostname or "").lower()
    if "deezer." not in host:
        return None
    parts = [segment for segment in (parsed.path or "").split("/") if segment]
    if len(parts) >= 2 and len(parts[0]) == 2:
        parts = parts[1:]
    path = "/" + "/".join(parts)
    return _rebuild_url(parsed, host=_canonical_host(host), path=path, query_items=_filtered_query_items(parsed))


def _normalize_tidal_url(parsed) -> str | None:
    host = (parsed.hostname or "").lower()
    if "tidal." not in host:
        return None
    parts = [segment for segment in (parsed.path or "").split("/") if segment]
    if parts[:1] == ["browse"]:
        parts = parts[1:]
    path = "/" + "/".join(parts)
    return _rebuild_url(parsed, host=_canonical_host(host), path=path, query_items=_filtered_query_items(parsed))


def _normalize_reddit_url(parsed) -> str | None:
    host = (parsed.hostname or "").lower()
    if host not in {"reddit.com", "www.reddit.com", "old.reddit.com", "new.reddit.com", "redd.it"}:
        return None
    path = parsed.path or ""
    if host == "redd.it":
        return _rebuild_url(parsed, host=host, path=path, query_items=[])
    if path.startswith("/r/") and path.endswith("/"):
        path = path.rstrip("/")
    return _rebuild_url(parsed, host=_canonical_host(host), path=path, query_items=[])


def _normalize_pornhub_url(parsed) -> str | None:
    host = (parsed.hostname or "").lower()
    path = parsed.path or ""
    if "pornhub." not in host:
        return None
    allowed = {"viewkey"} if path == "/view_video.php" else None
    return _rebuild_url(parsed, host=_canonical_host(host), path=path, query_items=_filtered_query_items(parsed, allowed))


def _normalize_vimeo_url(parsed) -> str | None:
    host = (parsed.hostname or "").lower()
    if "vimeo." not in host:
        return None
    return _rebuild_url(parsed, host=_canonical_host(host), path=parsed.path or "", query_items=_filtered_query_items(parsed))


def _normalize_soundcloud_url(parsed) -> str | None:
    host = (parsed.hostname or "").lower()
    if "soundcloud." not in host:
        return None
    return _rebuild_url(parsed, host=_canonical_host(host), path=parsed.path or "", query_items=_filtered_query_items(parsed))


def _single_video_extractor_url(value: str) -> str | None:
    parsed = urlparse(value)
    host = (parsed.netloc or "").lower()
    path = parsed.path or ""

    if "youtu.be" in host:
        video_id = path.strip("/")
        if video_id:
            return urlunparse((parsed.scheme, parsed.netloc, f"/{video_id}", "", "", ""))
        return None

    if "youtube." not in host:
        return None

    if path == "/watch":
        params = dict(parse_qsl(parsed.query, keep_blank_values=False))
        video_id = params.get("v")
        if video_id:
            return urlunparse((parsed.scheme, parsed.netloc, path, "", urlencode({"v": video_id}), ""))
        return None

    for prefix in ("/shorts/", "/embed/", "/live/"):
        if path.startswith(prefix):
            video_id = path.removeprefix(prefix).split("/", 1)[0]
            if video_id:
                return urlunparse((parsed.scheme, parsed.netloc, f"/watch", "", urlencode({"v": video_id}), ""))
    return None


def yt_dlp_extract_args(url: str) -> list[str]:
    args = yt_dlp_base_args() + ["--dump-single-json", "--skip-download"]
    single_video_url = _single_video_extractor_url(url)
    if single_video_url:
        return args + ["--no-playlist", single_video_url]
    return args + [url]


def yt_dlp_extract_info(url: str) -> dict:
    args = yt_dlp_extract_args(url)
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


def probe_direct_media_url(value: str) -> bool:
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"}:
        return False
    host = (parsed.netloc or "").lower()
    if any(token in host for token in KNOWN_EXTRACTOR_HOST_TOKENS):
        return False
    try:
        _assert_safe_external_url(value)
    except UnsafeURLError:
        return False
    if is_direct_media_url(value):
        return True
    try:
        # Manual redirect handling so every hop is host-validated. With
        # ``follow_redirects=True`` a hostile server could 302 to
        # http://169.254.169.254/ (cloud metadata) or a private IP.
        current = value
        with httpx.Client(follow_redirects=False, timeout=5.0) as client:
            for _ in range(DIRECT_HTTP_MAX_REDIRECTS + 1):
                response = client.head(current)
                if response.is_redirect:
                    location = response.headers.get("location")
                    if not location:
                        return False
                    current = str(httpx.URL(current).join(location))
                    try:
                        _assert_safe_external_url(current)
                    except UnsafeURLError:
                        return False
                    continue
                content_type = (response.headers.get("content-type") or "").lower()
                return content_type.startswith(("audio/", "video/"))
    except Exception:
        return False
    return False


class SourceResolver:
    def __init__(self, cache_dir: Path) -> None:
        self.cache_dir = cache_dir

    def resolve_inputs(self, job_id: str, inputs: list[str]) -> list[SourceItem]:
        return list(self.iter_resolve_inputs(job_id, inputs))

    def iter_resolve_inputs(self, job_id: str, inputs: list[str]):
        items: list[SourceItem] = []
        for raw in inputs:
            if is_url(raw):
                normalized = normalize_source_url(raw)
                if is_direct_media_url(normalized) or probe_direct_media_url(normalized):
                    yield self._direct_http_item(job_id, raw, normalized)
                else:
                    yield from self._yt_dlp_items(job_id, raw, normalized)
            else:
                yield self._local_file_item(job_id, raw)

    def _local_file_item(self, job_id: str, raw: str) -> SourceItem:
        path = Path(raw).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"Input file does not exist: {path}")
        if not path.is_file():
            raise IsADirectoryError(f"Input path is not a file: {path}")
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

    def _direct_http_item(self, job_id: str, raw: str, normalized_url: str) -> SourceItem:
        parsed = urlparse(normalized_url)
        filename = Path(parsed.path).name or sha1_text(normalized_url)
        metadata = SourceMetadata(
            title=filename,
            webpage_url=normalized_url,
            extra={"original_input_url": raw, "normalized_input_url": normalized_url},
        )
        return SourceItem(
            id=str(uuid.uuid4()),
            job_id=job_id,
            input_value=raw,
            kind=SourceKind.DIRECT_HTTP,
            status=ItemStatus.QUEUED,
            metadata=metadata,
            download_url=normalized_url,
        )

    def _yt_dlp_items(self, job_id: str, raw: str, normalized_url: str) -> list[SourceItem]:
        info = yt_dlp_extract_info(normalized_url)
        entries = _flatten_entries(info.get("entries") or [])
        if entries:
            playlist_title = info.get("title")
            playlist_id = info.get("id")
            for index, entry in enumerate(entries, start=1):
                if not entry:
                    continue
                yield self._from_yt_entry(job_id, raw, normalized_url, entry, playlist_id, playlist_title, index)
            return
        yield self._from_yt_entry(job_id, raw, normalized_url, info, None, None, None)

    def _from_yt_entry(
        self,
        job_id: str,
        raw: str,
        normalized_url: str,
        entry: dict,
        playlist_id: str | None,
        playlist_title: str | None,
        entry_index: int | None,
    ) -> SourceItem:
        duration = entry.get("duration")
        chapters = entry.get("chapters") or []
        download_url = _entry_download_url(entry, normalized_url, playlist_id)
        metadata_only = bool(entry.get("title")) and not download_url
        track_artist = entry.get("artist") or entry.get("creator") or entry.get("uploader") or entry.get("channel")
        metadata = SourceMetadata(
            title=entry.get("track") or entry.get("title"),
            extractor=entry.get("extractor_key") or entry.get("extractor"),
            webpage_url=entry.get("webpage_url") or normalized_url,
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
                "playlist_source_url": normalized_url,
                "track_title": entry.get("track") or entry.get("title"),
                "track_artist": track_artist,
                "track_album": entry.get("album"),
                "metadata_only": metadata_only,
                "original_input_url": raw,
                "normalized_input_url": normalized_url,
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


def download_direct_http(url: str, dest: Path, *, max_bytes: int = DIRECT_HTTP_MAX_BYTES) -> Path:
    """Stream ``url`` to ``dest``, validating each redirect hop.

    Redirects are followed manually so each hop is host-checked against
    :func:`_assert_safe_external_url` — prevents an attacker from
    redirecting a user-supplied download into a private/loopback address.
    ``max_bytes`` caps response size; the partial file is removed on
    overflow so we don't leave a half-downloaded artifact on disk.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    _assert_safe_external_url(url)
    current = url
    with httpx.Client(follow_redirects=False, timeout=60.0) as client:
        for _ in range(DIRECT_HTTP_MAX_REDIRECTS + 1):
            with client.stream("GET", current) as response:
                if response.is_redirect:
                    location = response.headers.get("location")
                    if not location:
                        response.raise_for_status()
                        raise httpx.HTTPError("Redirect without Location header")
                    current = str(httpx.URL(current).join(location))
                    _assert_safe_external_url(current)
                    continue
                response.raise_for_status()
                written = 0
                try:
                    with dest.open("wb") as handle:
                        for chunk in response.iter_bytes():
                            written += len(chunk)
                            if max_bytes and written > max_bytes:
                                raise httpx.HTTPError(
                                    f"Download exceeds maximum size of {max_bytes} bytes"
                                )
                            handle.write(chunk)
                except Exception:
                    dest.unlink(missing_ok=True)
                    raise
                return dest
        raise httpx.HTTPError(f"Too many redirects (> {DIRECT_HTTP_MAX_REDIRECTS})")
