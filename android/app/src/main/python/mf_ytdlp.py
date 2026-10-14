"""yt-dlp for the Android app, called from ChaquopyMediaBackend.kt through Chaquopy.

The entry points return strings and never raise. A failure comes back as the JSON
object ``{"error": kind, "message": text}``, where ``kind`` is one of ``unavailable``,
``unsupported``, ``network``, ``canceled`` or ``failed`` and ``text`` is a short
sentence that can be shown to the user.

YouTube is first asked through the ``visionos`` player client, which needs no JS
challenge solving. Solving them with quickjs-ng costs seconds per request on a phone
(about 16 s per probe on the emulator), so yt-dlp's default clients, which do,
are only used when that first attempt fails.

``opts_json`` is a JSON object with these optional keys:

- ``qjs_path``: the bundled quickjs-ng executable, used to solve YouTube's JS challenges
- ``tmp_dir``: private temporary directory (yt-dlp writes scripts for the JS runtime there)
- ``cache_dir``: yt-dlp's cache (player scripts, solved challenges)
- ``socket_timeout``: seconds, default 30
- ``extractor_args``: yt-dlp extractor arguments, for diagnostics (disables the fast path)

``probe(..., with_comments=True)`` also fetches up to 40 top comments (YouTube: pinned
first) so tracklists posted in comments can be used; they arrive in ``comments``.
"""

import json
import os
import re
import subprocess
import sys
import tempfile
import threading
import time

import yt_dlp
from yt_dlp.networking.exceptions import HTTPError, TransportError
from yt_dlp.utils import (
    DownloadCancelled,
    DownloadError,
    ExtractorError,
    GeoRestrictedError,
    UnsupportedError,
)
from yt_dlp.version import __version__ as yt_dlp_version

AUDIO_FORMAT = "bestaudio[ext=m4a]/bestaudio[acodec^=mp4a]/bestaudio/best"
OUTTMPL = "%(id).80s.%(ext)s"
MAX_MESSAGE_LENGTH = 200
FAST_YOUTUBE_CLIENTS = ["visionos"]
# The CLI's --write-comments --extractor-args "youtube:max_comments=40,all,0,0;comment_sort=top".
COMMENT_EXTRACTOR_ARGS = {"youtube": {"max_comments": ["40", "all", "0", "0"], "comment_sort": ["top"]}}

_YOUTUBE_URL = re.compile(
    r"^(?:https?://)?(?:[\w-]+\.)*(?:youtube\.com|youtu\.be|youtube-nocookie\.com)(?:[/:?#]|$)", re.IGNORECASE
)
# Failures no other player client can fix; skip the slow retry for these.
_PERMANENT = re.compile(
    r"private video|been removed|no longer available|terminated|copyright|does not exist|members-only"
    r"|premieres in|live event will begin|not made this video available in your country",
    re.IGNORECASE,
)

# Progress is forwarded at most this often unless it moved by PROGRESS_STEP.
PROGRESS_INTERVAL_S = 0.25
PROGRESS_STEP = 0.01

_environment_lock = threading.Lock()
_environment_tmp_dir = None


class Canceled(DownloadCancelled):
    """Raised from the progress hook once the session was canceled."""

    msg = "Canceled."


def probe(url, opts_json, with_comments=False):
    """Metadata for ``url`` as yt-dlp ``--dump-single-json --flat-playlist`` JSON.

    The selected format is the audio stream ``download_audio`` would fetch. With
    ``with_comments`` the top comments are included as ``comments``.
    """
    try:
        opts = _load_opts(opts_json)
        params = _base_params(opts)
        params.update(format=AUDIO_FORMAT, skip_download=True, extract_flat="in_playlist")
        fast = "extractor_args" not in params
        if with_comments:
            params["getcomments"] = True
            params["extractor_args"] = merge_extractor_args(params.get("extractor_args"), COMMENT_EXTRACTOR_ARGS)

        def extract(ydl):
            info = ydl.extract_info(url, download=False)
            return None if info is None else json.dumps(ydl.sanitize_info(info), ensure_ascii=False)

        result = _extract(url, params, extract, fast=fast)
        if result is None:
            return _error("unavailable", "No media was found at this link.")
        return result
    except BaseException as exc:  # noqa: BLE001 - every failure becomes an error object
        return _error_from_exception(exc)


def download_audio(url, dest_dir, opts_json, progress_cb=None, is_canceled_cb=None):
    """Download the best audio-only stream of ``url`` into ``dest_dir``.

    Prefers m4a (AAC), runs no post-processing and returns the absolute path of the
    file. ``progress_cb(fraction)`` receives 0..1; ``is_canceled_cb()`` is polled from
    the progress hook and stops the download. Partial files are left for the caller,
    which owns ``dest_dir``. For links with several items only the first is fetched.
    """
    progress_cb = _method(progress_cb, "report")
    is_canceled_cb = _method(is_canceled_cb, "isCanceled")
    try:
        if _is_canceled(is_canceled_cb):
            raise Canceled()
        opts = _load_opts(opts_json)
        os.makedirs(dest_dir, exist_ok=True)
        params = _base_params(opts)
        params.update(
            format=AUDIO_FORMAT,
            outtmpl={"default": OUTTMPL},
            paths={"home": dest_dir, "temp": dest_dir},
            noplaylist=True,
            playlist_items="1",
            progress_hooks=[_ProgressHook(progress_cb, is_canceled_cb)],
            postprocessors=[],
            fixup="never",
            overwrites=True,
            # A retry may pick another stream; never resume a different one's .part file.
            continuedl=False,
            writethumbnail=False,
            writesubtitles=False,
            writeinfojson=False,
        )
        info = _extract(url, params, lambda ydl: ydl.extract_info(url, download=True))
        if _is_canceled(is_canceled_cb):
            raise Canceled()
        path = _downloaded_path(info)
        if not path or not os.path.isfile(path):
            return _error("failed", "The download finished without an audio file.")
        return os.path.abspath(path)
    except BaseException as exc:  # noqa: BLE001
        if _is_canceled(is_canceled_cb):
            return _error("canceled", "Canceled.")
        return _error_from_exception(exc)


def runtime_info(opts_json):
    """Versions of the bundled tools as JSON, and whether the JS runtime executes."""
    try:
        opts = _load_opts(opts_json)
        import yt_dlp_ejs

        info = {
            "python": sys.version.split()[0],
            "yt_dlp": yt_dlp_version,
            "yt_dlp_ejs": getattr(yt_dlp_ejs, "version", None),
            "js_runtime": None,
            "js_ok": False,
        }
        qjs = opts.get("qjs_path")
        if qjs:
            from yt_dlp.utils._jsruntime import QuickJsRuntime

            runtime = QuickJsRuntime(path=qjs).info
            if runtime is not None:
                info["js_runtime"] = f"{runtime.name} {runtime.version}"
                info["js_ok"] = _run_js(qjs, "console.log(6 * 7)") == "42"
        return json.dumps(info)
    except BaseException as exc:  # noqa: BLE001
        return _error_from_exception(exc)


def _extract(url, params, work, fast=None):
    """Run ``work(ydl)``; for YouTube first with the fast client, then with the defaults.

    ``fast`` defaults to "no extractor arguments were given": explicit ones are diagnostics.
    """
    if fast is None:
        fast = "extractor_args" not in params
    if is_youtube(url) and fast:
        fast_args = merge_extractor_args(
            params.get("extractor_args"), {"youtube": {"player_client": FAST_YOUTUBE_CLIENTS}}
        )
        try:
            with yt_dlp.YoutubeDL(dict(params, extractor_args=fast_args)) as ydl:
                return work(ydl)
        except DownloadError as exc:
            kind, message = classify(exc)
            if kind not in ("unavailable", "failed") or _PERMANENT.search(message):
                raise
            print(f"yt-dlp: {'+'.join(FAST_YOUTUBE_CLIENTS)} client failed ({message}); retrying", file=sys.stderr)
    with yt_dlp.YoutubeDL(params) as ydl:
        return work(ydl)


def is_youtube(url):
    return bool(_YOUTUBE_URL.match(str(url or "").strip()))


def merge_extractor_args(base, extra):
    """``base`` updated with ``extra`` per extractor (``{"youtube": {"key": [values]}}``), as a copy."""
    merged = {name: dict(args) for name, args in (base or {}).items()}
    for name, args in extra.items():
        merged.setdefault(name, {}).update(args)
    return merged


class _ProgressHook:
    def __init__(self, progress_cb, is_canceled_cb):
        self._progress_cb = progress_cb
        self._is_canceled_cb = is_canceled_cb
        self._last_time = 0.0
        self._last_fraction = -1.0

    def __call__(self, status):
        if _is_canceled(self._is_canceled_cb):
            raise Canceled()
        if self._progress_cb is None:
            return
        state = status.get("status")
        if state == "finished":
            self._report(1.0, force=True)
        elif state == "downloading":
            fraction = _fraction(status)
            if fraction is not None:
                self._report(fraction)

    def _report(self, fraction, force=False):
        # A retry with another client starts over; keep the reported value monotonic.
        if fraction < self._last_fraction:
            return
        now = time.monotonic()
        if not force and (
            now - self._last_time < PROGRESS_INTERVAL_S and fraction - self._last_fraction < PROGRESS_STEP
        ):
            return
        self._last_time = now
        self._last_fraction = fraction
        self._progress_cb(float(fraction))


def _fraction(status):
    total = status.get("total_bytes") or status.get("total_bytes_estimate")
    done = status.get("downloaded_bytes")
    if total and done is not None:
        return min(max(done / total, 0.0), 1.0)
    index, count = status.get("fragment_index"), status.get("fragment_count")
    if index and count:
        return min(max(index / count, 0.0), 1.0)
    return None


def _method(callback, name):
    """The named method of a Java callback object, or ``callback`` itself if it is a plain callable.

    Calling the method by name keeps working when R8 merges lambda classes, which makes
    Chaquopy's call-a-functional-interface shortcut ambiguous.
    """
    if callback is None:
        return None
    return getattr(callback, name, callback)


def _is_canceled(is_canceled_cb):
    if is_canceled_cb is None:
        return False
    try:
        return bool(is_canceled_cb())
    except Exception:  # noqa: BLE001 - a broken callback must not hide the real result
        return False


def _load_opts(opts_json):
    if not opts_json:
        return {}
    opts = json.loads(opts_json)
    if not isinstance(opts, dict):
        raise ValueError("options must be a JSON object")
    _configure_environment(opts.get("tmp_dir"))
    return opts


def _configure_environment(tmp_dir):
    """Process-wide setup: private TMPDIR and certifi's CA bundle."""
    global _environment_tmp_dir
    with _environment_lock:
        if tmp_dir and tmp_dir != _environment_tmp_dir:
            os.makedirs(tmp_dir, exist_ok=True)
            os.environ["TMPDIR"] = tmp_dir
            tempfile.tempdir = tmp_dir
            _environment_tmp_dir = tmp_dir
        if "SSL_CERT_FILE" not in os.environ:
            try:
                import certifi

                os.environ["SSL_CERT_FILE"] = certifi.where()
            except ImportError:
                pass


def _base_params(opts):
    qjs = opts.get("qjs_path")
    params = {
        "quiet": True,
        "no_warnings": False,
        "noprogress": True,
        "color": "never",
        "logger": _Logger(),
        "socket_timeout": float(opts.get("socket_timeout") or 30),
        "retries": 3,
        "fragment_retries": 3,
        "extractor_retries": 2,
        "cachedir": opts.get("cache_dir") or False,
        # Without a runtime yt-dlp would look for deno, which is never present here.
        "js_runtimes": {"quickjs": {"path": qjs}} if qjs and os.access(qjs, os.X_OK) else {},
    }
    if isinstance(opts.get("extractor_args"), dict):
        params["extractor_args"] = opts["extractor_args"]
    return params


def _downloaded_path(info):
    if not info:
        return None
    if info.get("_type") in ("playlist", "multi_video"):
        for entry in info.get("entries") or []:
            path = _downloaded_path(entry)
            if path:
                return path
        return None
    for download in info.get("requested_downloads") or []:
        if download.get("filepath"):
            return download["filepath"]
    return info.get("filepath") or info.get("_filename")


def _run_js(qjs, source):
    fd, path = tempfile.mkstemp(suffix=".js")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(source)
        result = subprocess.run([qjs, path], capture_output=True, text=True, timeout=10, check=False)
        return result.stdout.strip() if result.returncode == 0 else None
    finally:
        os.unlink(path)


class _Logger:
    """Routes yt-dlp output to stderr (logcat tag python.stderr), minus the chatter."""

    def debug(self, msg):
        pass

    def info(self, msg):
        pass

    def warning(self, msg):
        print(f"yt-dlp: {msg}", file=sys.stderr)

    def error(self, msg):
        print(f"yt-dlp: {msg}", file=sys.stderr)


def _error(kind, message):
    return json.dumps({"error": kind, "message": message})


def _error_from_exception(exc):
    kind, message = classify(exc)
    return _error(kind, message)


def classify(exc):
    """Map an exception from yt-dlp to ``(kind, user-presentable message)``."""
    original = exc
    if isinstance(exc, DownloadError) and exc.exc_info and exc.exc_info[1] is not None:
        original = exc.exc_info[1]

    if isinstance(original, DownloadCancelled):
        return "canceled", "Canceled."
    if isinstance(original, UnsupportedError):
        return "unsupported", "This link isn't supported."
    if isinstance(original, GeoRestrictedError):
        return "unavailable", "This media isn't available in your country."
    if isinstance(original, ExtractorError):
        cause = original.cause or (original.exc_info[1] if original.exc_info else None)
        network = _network_kind(cause)
        if network:
            return network
        if original.expected:
            return "unavailable", clean_message(original.orig_msg) or "This media isn't available."
        return "failed", clean_message(original.orig_msg) or "The media could not be read."
    network = _network_kind(original)
    if network:
        return network
    if isinstance(original, (KeyboardInterrupt, SystemExit)):
        return "canceled", "Canceled."
    if isinstance(original, OSError):
        return "failed", "Could not write the download."
    message = clean_message(str(exc))
    return "failed", message or "The media could not be read."


def _network_kind(exc):
    if isinstance(exc, HTTPError):
        if exc.status in (401, 403, 404, 410, 451):
            return "unavailable", f"The site refused the request (HTTP {exc.status})."
        if exc.status == 429:
            return "network", "The site is rate limiting requests. Try again later."
        return "network", f"The site returned an error (HTTP {exc.status})."
    if isinstance(exc, TransportError):
        return "network", "Network error. Check the connection and try again."
    if isinstance(exc, (TimeoutError, ConnectionError)):
        return "network", "Network error. Check the connection and try again."
    return None


# Sentences that point at URLs or command-line flags are hints for CLI users.
_HINT = re.compile(r"(?:https?|ftp)://|(?:^|\s)--[a-z]|yt-dlp -U", re.IGNORECASE)
_PREFIX = re.compile(r"^(?:ERROR:\s*)?(?:\[[^\]]+\]\s*)?(?:[\w-]+:\s+)?")


def clean_message(text):
    """Shorten a yt-dlp message to its meaningful sentences, without URLs or CLI hints."""
    text = str(text or "").strip()
    if not text:
        return ""
    line = text.splitlines()[0]
    if line.startswith(("ERROR", "[")):
        line = _PREFIX.sub("", line, count=1)
    line = line.split("; please report this issue")[0]
    kept = [s for s in re.split(r"(?<=[.!?])\s+", line) if s.strip() and not _HINT.search(s)]
    message = " ".join(kept).strip()
    if message and message[-1] not in ".!?":
        message += "."
    if len(message) > MAX_MESSAGE_LENGTH:
        message = message[: MAX_MESSAGE_LENGTH - 1].rstrip() + "…"
    return message
