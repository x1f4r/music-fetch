from __future__ import annotations

import functools as _functools
import json
import math
import re
import wave
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from .config import Settings
from .models import SourceItem, SourceKind, SourceMetadata, WindowPlan
from .sources import download_direct_http, yt_dlp_base_args
from .utils import run_command, sha1_text


class MediaToolError(RuntimeError):
    pass


@dataclass
class FingerprintChunk:
    timestamp_ms: int
    duration_ms: int
    fingerprint: list[int]


@dataclass
class SourceProfile:
    duration_ms: int
    strategy: str
    prefer_source_path: str
    request_budget: int
    max_windows: int
    stop_after_consensus: int
    use_source_separation: bool


TIMESTAMP_RE = re.compile(r"(?<!\d)(?:(\d{1,2}):)?([0-5]?\d):([0-5]\d)(?!\d)")


def ensure_local_media(settings: Settings, item: SourceItem) -> Path:
    if item.kind is SourceKind.LOCAL_FILE and item.local_path:
        return Path(item.local_path)

    item_dir = settings.cache_dir / "sources" / item.id
    item_dir.mkdir(parents=True, exist_ok=True)
    if item.kind is SourceKind.DIRECT_HTTP and item.download_url:
        dest = item_dir / "source"
        return download_direct_http(item.download_url, dest)

    if item.kind is SourceKind.YT_DLP and item.download_url:
        outtmpl = str(item_dir / "%(id)s.%(ext)s")
        last_error: str | None = None
        for args in yt_dlp_download_args(item, outtmpl):
            result = run_command(args)
            if result.returncode == 0:
                matches = sorted(path for path in item_dir.glob("*") if path.is_file())
                path = matches[0] if matches else None
                if path is not None and path.exists():
                    return path
                last_error = f"yt-dlp did not download media for {item.download_url}"
                continue
            last_error = result.stderr.strip() or f"yt-dlp did not download media for {item.download_url}"
        raise MediaToolError(last_error or f"yt-dlp did not download media for {item.download_url}")

    raise MediaToolError(f"Unsupported source item kind: {item.kind}")


def yt_dlp_download_args(item: SourceItem, outtmpl: str) -> list[list[str]]:
    base = yt_dlp_base_args() + ["-f", "bestaudio/best", "-o", outtmpl]
    commands: list[list[str]] = []

    playlist_source_url = str(item.metadata.extra.get("playlist_source_url") or "").strip()
    if playlist_source_url and item.metadata.entry_index:
        commands.append(base + ["--playlist-items", str(item.metadata.entry_index), playlist_source_url])

    if item.download_url:
        commands.append(base + ["--no-playlist", item.download_url])

    deduped: list[list[str]] = []
    seen: set[tuple[str, ...]] = set()
    for command in commands:
        key = tuple(command)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(command)
    return deduped


def normalize_media(input_path: Path, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    args = [
        "ffmpeg",
        "-y",
        "-i",
        str(input_path),
        "-ac",
        "1",
        "-ar",
        "16000",
        "-c:a",
        "pcm_s16le",
        str(output_path),
    ]
    result = run_command(args)
    if result.returncode != 0:
        raise MediaToolError(result.stderr.strip() or "ffmpeg normalization failed")
    return output_path


def probe_duration_ms(input_path: Path) -> int:
    args = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "json",
        str(input_path),
    ]
    result = run_command(args)
    if result.returncode != 0:
        raise MediaToolError(result.stderr.strip() or "ffprobe failed")
    data = json.loads(result.stdout)
    duration = float(data["format"]["duration"])
    return int(duration * 1000)


def create_excerpt(source_path: Path, start_ms: int, end_ms: int, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    duration = max(0.1, (end_ms - start_ms) / 1000)
    args = [
        "ffmpeg",
        "-y",
        "-ss",
        f"{start_ms / 1000:.3f}",
        "-t",
        f"{duration:.3f}",
        "-i",
        str(source_path),
        "-ac",
        "1",
        "-ar",
        "16000",
        "-c:a",
        "pcm_s16le",
        str(output_path),
    ]
    result = run_command(args)
    if result.returncode != 0:
        raise MediaToolError(result.stderr.strip() or "ffmpeg excerpt creation failed")
    return output_path


def heuristic_music_stem(input_path: Path, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    args = [
        "ffmpeg",
        "-y",
        "-i",
        str(input_path),
        "-af",
        "highpass=f=120,lowpass=f=6000,acompressor=threshold=-18dB:ratio=2:attack=20:release=250",
        str(output_path),
    ]
    result = run_command(args)
    if result.returncode != 0:
        raise MediaToolError(result.stderr.strip() or "ffmpeg heuristic stem failed")
    return output_path


def isolate_music(settings: Settings, normalized_path: Path, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / f"{normalized_path.stem}.instrumental.wav"
    if target.exists():
        return target

    try:
        from audio_separator.separator import Separator  # type: ignore

        separator = Separator(
            log_level=30,
            output_dir=str(output_dir),
            output_format="wav",
            model_file_dir=str(settings.cache_dir / "models"),
            use_autocast=False,
        )
        separator.load_model(model_filename=settings.separation_model)
        outputs = separator.separate(str(normalized_path))
        for value in outputs:
            path = Path(value)
            if "instrumental" in path.name.lower() or "no_vocals" in path.name.lower():
                return path
    except Exception:
        pass

    return heuristic_music_stem(normalized_path, target)


def rank_windows(source_path: Path, window_ms: int, hop_ms: int, max_windows: int, label: str) -> list[WindowPlan]:
    duration_ms = probe_duration_ms(source_path)
    windows: list[WindowPlan] = []
    starts = list(range(0, max(duration_ms - window_ms, 0) + 1, hop_ms))
    if not starts:
        starts = [0]
    if starts[-1] + window_ms < duration_ms:
        starts.append(max(duration_ms - window_ms, 0))

    for start_ms in starts:
        end_ms = min(duration_ms, start_ms + window_ms)
        windows.append(WindowPlan(start_ms=start_ms, end_ms=end_ms, score=score_window(source_path, start_ms, end_ms), source_path=str(source_path), label=label))

    windows.sort(key=lambda item: (-item.score, item.start_ms))
    return windows[:max_windows]


def score_window(source_path: Path, start_ms: int, end_ms: int) -> float:
    with wave.open(str(source_path), "rb") as wav:
        frame_rate = wav.getframerate()
        sample_width = wav.getsampwidth()
        start_frame = int(start_ms / 1000 * frame_rate)
        frame_count = max(1, int((end_ms - start_ms) / 1000 * frame_rate))
        wav.setpos(min(start_frame, wav.getnframes()))
        raw = wav.readframes(frame_count)

    if sample_width != 2 or not raw:
        return 0.0
    samples = np.frombuffer(raw, dtype=np.int16).astype(np.float32)
    if samples.size == 0:
        return 0.0
    energy = float(np.sqrt(np.mean(np.square(samples))) / 32768.0)
    spectrum = np.abs(np.fft.rfft(samples[: min(samples.size, 32768)]))
    if spectrum.size == 0:
        return energy
    bins = np.linspace(0, 1, spectrum.size)
    mid_energy = float(spectrum[(bins > 0.02) & (bins < 0.45)].sum())
    high_energy = float(spectrum[(bins >= 0.45) & (bins < 0.9)].sum())
    return energy * 0.6 + mid_energy * 1e-6 + high_energy * 5e-7


def build_excerpt_path(base_dir: Path, source_path: Path, start_ms: int, end_ms: int, label: str) -> Path:
    token = sha1_text(f"{source_path}:{start_ms}:{end_ms}:{label}")
    return base_dir / f"{token}.wav"


def classify_source(duration_ms: int, *, has_playlist_context: bool, metadata: SourceMetadata | None = None) -> SourceProfile:
    has_structural_hints = False
    if metadata is not None:
        has_structural_hints = len(metadata.chapters) >= 2 or len(description_starts(metadata.description)) >= 2
    _ = has_playlist_context

    if duration_ms >= 25 * 60_000:
        estimated_windows = max(24, min(360, math.ceil(duration_ms / 90_000)))
        return SourceProfile(
            duration_ms=duration_ms,
            strategy="long_mix",
            prefer_source_path="instrumental",
            request_budget=min(420, estimated_windows + 48),
            max_windows=estimated_windows,
            stop_after_consensus=0,
            use_source_separation=duration_ms <= 90 * 60_000,
        )
    if has_structural_hints or duration_ms >= 15_000:
        estimated_windows = max(6, min(36, math.ceil(duration_ms / 12_000)))
        return SourceProfile(
            duration_ms=duration_ms,
            strategy="multi_track",
            prefer_source_path="instrumental",
            request_budget=min(96, estimated_windows * 3),
            max_windows=estimated_windows,
            stop_after_consensus=0,
            use_source_separation=True,
        )
    return SourceProfile(
        duration_ms=duration_ms,
        strategy="single_track",
        prefer_source_path="instrumental",
        request_budget=6,
        max_windows=6,
        stop_after_consensus=2,
        use_source_separation=True,
    )


def plan_windows_for_profile(source_path: Path, profile: SourceProfile, label: str) -> list[WindowPlan]:
    if profile.strategy == "long_mix":
        duration_ms = profile.duration_ms or probe_duration_ms(source_path)
        coverage_step_ms = max(60_000, math.ceil(duration_ms / max(1, profile.max_windows)))
        return uniform_windows(
            source_path,
            label=label,
            duration_ms=duration_ms,
            step_ms=coverage_step_ms,
            max_windows=profile.max_windows,
        )
    if profile.strategy == "single_track":
        return rank_windows(source_path, 12_000, 8_000, profile.max_windows, label)
    return rank_windows(source_path, 14_000, 10_000, profile.max_windows, label)


def metadata_windows(source_path: Path, metadata: SourceMetadata, *, duration_ms: int, label: str) -> list[WindowPlan]:
    starts = chapter_starts(metadata.chapters) or description_starts(metadata.description)
    if not starts:
        return []
    deduped_starts = sorted({start for start in starts if 0 <= start < duration_ms})
    plans: list[WindowPlan] = []
    for index, start_ms in enumerate(deduped_starts):
        next_start = deduped_starts[index + 1] if index + 1 < len(deduped_starts) else duration_ms
        anchor_start = start_ms + min(30_000, max(8_000, (next_start - start_ms) // 4))
        window_start = min(max(start_ms, anchor_start), max(0, duration_ms - 12_000))
        window_end = min(duration_ms, window_start + 12_000)
        plans.append(
            WindowPlan(
                start_ms=window_start,
                end_ms=window_end,
                score=score_window(source_path, window_start, window_end),
                source_path=str(source_path),
                label=label,
            )
        )
    return plans


def clustered_long_mix_windows(source_path: Path, *, label: str, max_windows: int) -> list[WindowPlan]:
    duration_ms = probe_duration_ms(source_path)
    chunks = chunk_fingerprints(source_path, chunk_seconds=20)
    if not chunks:
        return uniform_windows(source_path, label=label, duration_ms=duration_ms, step_ms=90_000, max_windows=max_windows)

    clusters: list[list[FingerprintChunk]] = []
    current: list[FingerprintChunk] = [chunks[0]]
    for chunk in chunks[1:]:
        previous = current[-1]
        similarity = raw_fingerprint_similarity(previous.fingerprint, chunk.fingerprint)
        if similarity >= 0.84:
            current.append(chunk)
        else:
            clusters.append(current)
            current = [chunk]
    if current:
        clusters.append(current)

    plans: list[WindowPlan] = []
    for cluster in clusters[:max_windows]:
        start_ms = cluster[0].timestamp_ms
        end_ms = cluster[-1].timestamp_ms + cluster[-1].duration_ms
        midpoint_ms = (start_ms + end_ms) // 2
        window_start = max(0, midpoint_ms - 6_000)
        window_end = min(duration_ms, window_start + 12_000)
        score = score_window(source_path, window_start, window_end)
        plans.append(
            WindowPlan(
                start_ms=window_start,
                end_ms=window_end,
                score=score,
                source_path=str(source_path),
                label=label,
            )
        )

    plans.sort(key=lambda item: item.start_ms)
    return plans[:max_windows]


def uniform_windows(source_path: Path, *, label: str, duration_ms: int, step_ms: int, max_windows: int) -> list[WindowPlan]:
    windows: list[WindowPlan] = []
    last_start = max(0, duration_ms - 12_000)
    starts = list(range(0, max(last_start, 0) + 1, max(1, step_ms)))
    if not starts:
        starts = [0]
    if starts[-1] < last_start:
        starts.append(last_start)
    for start_ms in starts:
        end_ms = min(duration_ms, start_ms + 12_000)
        windows.append(
            WindowPlan(
                start_ms=start_ms,
                end_ms=end_ms,
                score=score_window(source_path, start_ms, end_ms),
                source_path=str(source_path),
                label=label,
            )
        )
        if len(windows) >= max_windows:
            break
    return windows


def chapter_starts(chapters: list[dict]) -> list[int]:
    starts: list[int] = []
    for chapter in chapters:
        start_time = chapter.get("start_time")
        if start_time is None:
            continue
        starts.append(int(float(start_time) * 1000))
    return starts


def description_starts(description: str | None) -> list[int]:
    if not description:
        return []
    starts: list[int] = []
    for line in description.splitlines():
        line = line.strip()
        if not line:
            continue
        for match in TIMESTAMP_RE.finditer(line):
            hours = int(match.group(1) or 0)
            minutes = int(match.group(2))
            seconds = int(match.group(3))
            starts.append((hours * 3600 + minutes * 60 + seconds) * 1000)
            break
    return starts


def chunk_fingerprints(source_path: Path, *, chunk_seconds: int) -> list[FingerprintChunk]:
    result = run_command(["fpcalc", "-raw", "-json", "-chunk", str(chunk_seconds), "-overlap", str(source_path)])
    if result.returncode != 0:
        return []
    chunks: list[FingerprintChunk] = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        fingerprint = payload.get("fingerprint") or []
        if not fingerprint:
            continue
        chunks.append(
            FingerprintChunk(
                timestamp_ms=int(float(payload.get("timestamp", 0.0)) * 1000),
                duration_ms=int(float(payload.get("duration", 0.0)) * 1000),
                fingerprint=fingerprint,
            )
        )
    return chunks


def raw_fingerprint_similarity(a: list[int], b: list[int]) -> float:
    if not a or not b:
        return 0.0
    limit = min(len(a), len(b), 120)
    matches = sum(1 for index in range(limit) if abs(a[index] - b[index]) < 10)
    return matches / limit


def fingerprint_cache_key(clip_path: Path) -> str:
    """Return a stable key for ``clip_path`` suitable for the provider-result cache.

    Uses Chromaprint's ``fpcalc`` when available so two different excerpts
    that contain the same audio hash to the same key. Falls back to a content
    hash. Result is memoized on ``(resolved path, mtime, size)`` so repeated
    probes of the same excerpt don't re-fork ``fpcalc`` (T3.2).
    """
    try:
        stat = clip_path.stat()
    except FileNotFoundError:
        return sha1_text(str(clip_path))
    return _fingerprint_cache_lookup(str(clip_path.resolve()), stat.st_mtime_ns, stat.st_size)


@_functools.lru_cache(maxsize=2048)
def _fingerprint_cache_lookup(resolved_path: str, _mtime_ns: int, _size: int) -> str:
    path = Path(resolved_path)
    result = run_command(["fpcalc", "-raw", "-json", resolved_path])
    if result.returncode == 0:
        try:
            payload = json.loads(result.stdout)
            fingerprint = payload.get("fingerprint") or []
            duration = payload.get("duration") or 0.0
            if fingerprint:
                return sha1_text(json.dumps({"duration": duration, "fingerprint": fingerprint[:120]}, separators=(",", ":")))
        except json.JSONDecodeError:
            pass
    try:
        return sha1_text(path.read_bytes().hex())
    except FileNotFoundError:
        return sha1_text(resolved_path)


def clear_fingerprint_cache() -> None:
    """Reset the fingerprint-key LRU (useful for tests that rewrite excerpt files)."""
    _fingerprint_cache_lookup.cache_clear()
