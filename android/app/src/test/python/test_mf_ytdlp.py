"""Tests for mf_ytdlp.py on the host Python (needs yt-dlp installed).

    python3 -m unittest discover -s android/app/src/test/python

Set MF_LIVE=1 (and optionally MF_QJS to a host quickjs-ng binary) to also probe and
download a short YouTube video.
"""

import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "main" / "python"))

import mf_ytdlp  # noqa: E402
from yt_dlp.networking import Response  # noqa: E402
from yt_dlp.networking.exceptions import HTTPError, TransportError  # noqa: E402
from yt_dlp.utils import DownloadError, ExtractorError, GeoRestrictedError, UnsupportedError  # noqa: E402


def wrapped(exc):
    """A DownloadError as YoutubeDL.report_error raises it while handling `exc`."""
    try:
        raise exc
    except Exception:
        return DownloadError(f"ERROR: {exc}", sys.exc_info())


def http_error(status):
    return HTTPError(Response(fp=io.BytesIO(), url="https://example.com/x", headers={}, status=status))


class CleanMessageTest(unittest.TestCase):
    def test_strips_prefixes(self):
        self.assertEqual(
            mf_ytdlp.clean_message("ERROR: [youtube] dQw4w9WgXcQ: Video unavailable"),
            "Video unavailable.",
        )

    def test_drops_cli_hints_and_urls(self):
        text = (
            "Sign in to confirm your age. This video may be inappropriate for some users. "
            "Use --cookies-from-browser or --cookies for the authentication. "
            "See https://github.com/yt-dlp/yt-dlp/wiki/FAQ for how to manually pass cookies."
        )
        self.assertEqual(
            mf_ytdlp.clean_message(text),
            "Sign in to confirm your age. This video may be inappropriate for some users.",
        )

    def test_drops_bug_report_suffix(self):
        text = "Unable to extract data; please report this issue on  https://github.com/yt-dlp/yt-dlp/issues"
        self.assertEqual(mf_ytdlp.clean_message(text), "Unable to extract data.")

    def test_first_line_only_and_length_limit(self):
        self.assertEqual(mf_ytdlp.clean_message("First line\nsecond line"), "First line.")
        long = mf_ytdlp.clean_message("word " * 100)
        self.assertLessEqual(len(long), mf_ytdlp.MAX_MESSAGE_LENGTH)
        self.assertTrue(long.endswith("…"))

    def test_empty(self):
        self.assertEqual(mf_ytdlp.clean_message(None), "")
        self.assertEqual(mf_ytdlp.clean_message("   "), "")
        self.assertEqual(mf_ytdlp.clean_message("See https://example.com"), "")


class ClassifyTest(unittest.TestCase):
    def test_expected_extractor_error_is_unavailable(self):
        exc = wrapped(ExtractorError("Private video. Sign in if you've been granted access", expected=True))
        self.assertEqual(mf_ytdlp.classify(exc), ("unavailable", "Private video. Sign in if you've been granted access."))

    def test_unexpected_extractor_error_is_failed(self):
        kind, message = mf_ytdlp.classify(wrapped(ExtractorError("Unable to extract player response")))
        self.assertEqual(kind, "failed")
        self.assertEqual(message, "Unable to extract player response.")

    def test_unsupported_and_geo(self):
        self.assertEqual(mf_ytdlp.classify(wrapped(UnsupportedError("https://example.com/x")))[0], "unsupported")
        self.assertEqual(mf_ytdlp.classify(wrapped(GeoRestrictedError("blocked")))[0], "unavailable")

    def test_network_causes(self):
        try:
            raise TransportError("connection reset")
        except TransportError as cause:
            exc = ExtractorError("Unable to download webpage", cause=cause)
        self.assertEqual(mf_ytdlp.classify(wrapped(exc))[0], "network")
        self.assertEqual(mf_ytdlp.classify(wrapped(TransportError("timed out")))[0], "network")
        self.assertEqual(mf_ytdlp.classify(TimeoutError())[0], "network")

    def test_http_status(self):
        self.assertEqual(mf_ytdlp.classify(wrapped(http_error(403)))[0], "unavailable")
        self.assertEqual(mf_ytdlp.classify(wrapped(http_error(429)))[0], "network")
        self.assertEqual(mf_ytdlp.classify(wrapped(http_error(503)))[0], "network")

    def test_canceled(self):
        self.assertEqual(mf_ytdlp.classify(mf_ytdlp.Canceled())[0], "canceled")
        self.assertEqual(mf_ytdlp.classify(wrapped(mf_ytdlp.Canceled()))[0], "canceled")

    def test_other_errors(self):
        self.assertEqual(mf_ytdlp.classify(PermissionError("denied")), ("failed", "Could not write the download."))
        self.assertEqual(mf_ytdlp.classify(ValueError("bad"))[0], "failed")


class ProgressTest(unittest.TestCase):
    def test_reports_throttled_fractions(self):
        reported = []
        hook = mf_ytdlp._ProgressHook(reported.append, None)
        hook({"status": "downloading", "downloaded_bytes": 0, "total_bytes": 1000})
        hook({"status": "downloading", "downloaded_bytes": 5, "total_bytes": 1000})  # < 1 %: skipped
        hook({"status": "downloading", "downloaded_bytes": 500, "total_bytes_estimate": 1000})
        hook({"status": "downloading", "fragment_index": 3, "fragment_count": 4})
        hook({"status": "downloading"})
        hook({"status": "finished"})
        self.assertEqual(reported, [0.0, 0.5, 0.75, 1.0])

    def test_never_goes_backwards(self):
        reported = []
        hook = mf_ytdlp._ProgressHook(reported.append, None)
        hook({"status": "downloading", "downloaded_bytes": 600, "total_bytes": 1000})
        hook({"status": "downloading", "downloaded_bytes": 100, "total_bytes": 1000})  # retry restarted
        hook({"status": "finished"})
        self.assertEqual(reported, [0.6, 1.0])

    def test_cancel_raises(self):
        hook = mf_ytdlp._ProgressHook(None, lambda: True)
        with self.assertRaises(mf_ytdlp.Canceled):
            hook({"status": "downloading"})

    def test_broken_cancel_callback_is_ignored(self):
        def broken():
            raise RuntimeError("boom")

        self.assertFalse(mf_ytdlp._is_canceled(broken))


class HelpersTest(unittest.TestCase):
    def test_downloaded_path(self):
        video = {"requested_downloads": [{"filepath": "/tmp/a.m4a"}]}
        self.assertEqual(mf_ytdlp._downloaded_path(video), "/tmp/a.m4a")
        self.assertEqual(mf_ytdlp._downloaded_path({"_filename": "/tmp/b.m4a"}), "/tmp/b.m4a")
        playlist = {"_type": "playlist", "entries": [None, {}, video]}
        self.assertEqual(mf_ytdlp._downloaded_path(playlist), "/tmp/a.m4a")
        self.assertIsNone(mf_ytdlp._downloaded_path(None))

    def test_js_runtime_only_when_executable(self):
        self.assertEqual(mf_ytdlp._base_params({"qjs_path": "/nonexistent/qjs"})["js_runtimes"], {})
        self.assertEqual(
            mf_ytdlp._base_params({"qjs_path": sys.executable})["js_runtimes"],
            {"quickjs": {"path": sys.executable}},
        )

    def test_entry_points_never_raise(self):
        self.assertEqual(json.loads(mf_ytdlp.probe("x", "[1]"))["error"], "failed")
        result = json.loads(mf_ytdlp.download_audio("x", tempfile.gettempdir(), "{}", None, lambda: True))
        self.assertEqual(result, {"error": "canceled", "message": "Canceled."})


class ProbeCommentsTest(unittest.TestCase):
    def setUp(self):
        self.real = mf_ytdlp.yt_dlp.YoutubeDL
        mf_ytdlp.yt_dlp.YoutubeDL = RecordingYoutubeDL
        RecordingYoutubeDL.params = []

    def tearDown(self):
        mf_ytdlp.yt_dlp.YoutubeDL = self.real

    def test_comments_requested_with_top_sort(self):
        info = json.loads(mf_ytdlp.probe("https://www.youtube.com/watch?v=x", "{}", True))
        self.assertEqual(info["comments"], [{"text": "0:00 A - B"}])
        params = RecordingYoutubeDL.params[0]
        self.assertTrue(params["getcomments"])
        youtube = params["extractor_args"]["youtube"]
        self.assertEqual(youtube["max_comments"], ["40", "all", "0", "0"])
        self.assertEqual(youtube["comment_sort"], ["top"])
        self.assertEqual(youtube["player_client"], mf_ytdlp.FAST_YOUTUBE_CLIENTS)

    def test_no_comments_by_default(self):
        mf_ytdlp.probe("https://soundcloud.com/a/b", "{}")
        params = RecordingYoutubeDL.params[0]
        self.assertNotIn("getcomments", params)
        self.assertNotIn("extractor_args", params)

    def test_merge_extractor_args(self):
        base = {"youtube": {"player_client": ["web"]}, "tiktok": {"x": ["1"]}}
        merged = mf_ytdlp.merge_extractor_args(base, {"youtube": {"comment_sort": ["top"]}})
        self.assertEqual(merged["youtube"], {"player_client": ["web"], "comment_sort": ["top"]})
        self.assertEqual(merged["tiktok"], {"x": ["1"]})
        self.assertEqual(base["youtube"], {"player_client": ["web"]})
        self.assertEqual(mf_ytdlp.merge_extractor_args(None, {"a": {"b": ["c"]}}), {"a": {"b": ["c"]}})


class RecordingYoutubeDL:
    """Records the params and returns one video with a comment."""

    params = []

    def __init__(self, params):
        RecordingYoutubeDL.params.append(params)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def extract_info(self, url, download):
        return {"id": "x", "title": "T", "comments": [{"text": "0:00 A - B"}]}

    def sanitize_info(self, info):
        return info


class FakeYoutubeDL:
    """Stands in for yt_dlp.YoutubeDL; `outcomes` are raised or returned in order."""

    outcomes = []
    calls = []

    def __init__(self, params):
        FakeYoutubeDL.calls.append(params.get("extractor_args"))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def extract_info(self, url, download):
        outcome = FakeYoutubeDL.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class YoutubeFallbackTest(unittest.TestCase):
    URL = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    FAST = {"youtube": {"player_client": mf_ytdlp.FAST_YOUTUBE_CLIENTS}}

    def setUp(self):
        self.real = mf_ytdlp.yt_dlp.YoutubeDL
        mf_ytdlp.yt_dlp.YoutubeDL = FakeYoutubeDL
        FakeYoutubeDL.calls = []

    def tearDown(self):
        mf_ytdlp.yt_dlp.YoutubeDL = self.real

    def run_extract(self, url, *outcomes, params=None):
        FakeYoutubeDL.outcomes = list(outcomes)
        return mf_ytdlp._extract(url, params or {}, lambda ydl: ydl.extract_info(url, download=False))

    def test_fast_client_first(self):
        self.assertEqual(self.run_extract(self.URL, {"id": "a"}), {"id": "a"})
        self.assertEqual(FakeYoutubeDL.calls, [self.FAST])

    def test_falls_back_to_default_clients(self):
        missing = wrapped(ExtractorError("Requested format is not available", expected=True))
        self.assertEqual(self.run_extract(self.URL, missing, {"id": "a"}), {"id": "a"})
        self.assertEqual(FakeYoutubeDL.calls, [self.FAST, None])

    def test_no_retry_for_permanent_network_or_cancel(self):
        for error in (
            wrapped(ExtractorError("Private video. Sign in if you've been granted access", expected=True)),
            wrapped(TransportError("timed out")),
            wrapped(mf_ytdlp.Canceled()),
        ):
            FakeYoutubeDL.calls = []
            with self.assertRaises(DownloadError):
                self.run_extract(self.URL, error)
            self.assertEqual(FakeYoutubeDL.calls, [self.FAST])

    def test_other_sites_and_explicit_args_skip_fast_path(self):
        self.run_extract("https://soundcloud.com/a/b", {"id": "a"})
        custom = {"youtube": {"player_client": ["web"]}}
        self.run_extract(self.URL, {"id": "b"}, params={"extractor_args": custom})
        self.assertEqual(FakeYoutubeDL.calls, [None, custom])

    def test_fast_client_keeps_other_arguments(self):
        comments = mf_ytdlp.COMMENT_EXTRACTOR_ARGS
        FakeYoutubeDL.outcomes = [{"id": "b"}]
        work = lambda ydl: ydl.extract_info(self.URL, download=False)  # noqa: E731
        mf_ytdlp._extract(self.URL, {"extractor_args": comments}, work, fast=True)
        expected = {"youtube": dict(comments["youtube"], player_client=mf_ytdlp.FAST_YOUTUBE_CLIENTS)}
        self.assertEqual(FakeYoutubeDL.calls, [expected])
        self.assertNotIn("player_client", comments["youtube"])

    def test_is_youtube(self):
        for url in ("https://youtu.be/x", "music.youtube.com/watch?v=x", "https://m.youtube.com/shorts/x",
                    "https://www.youtube-nocookie.com/embed/x"):
            self.assertTrue(mf_ytdlp.is_youtube(url), url)
        for url in ("https://youtube.com.example.org/x", "https://notyoutube.com/x", "", None):
            self.assertFalse(mf_ytdlp.is_youtube(url), url)


@unittest.skipUnless(os.environ.get("MF_LIVE") == "1", "set MF_LIVE=1 for network tests")
class LiveTest(unittest.TestCase):
    URL = "https://www.youtube.com/watch?v=jNQXAC9IVRw"  # "Me at the zoo", 19 s

    def setUp(self):
        self.saved_tmpdir = os.environ.get("TMPDIR")
        self.tmp = tempfile.TemporaryDirectory()
        self.opts = json.dumps(
            {"qjs_path": os.environ.get("MF_QJS"), "tmp_dir": os.path.join(self.tmp.name, "tmp"),
             "cache_dir": os.path.join(self.tmp.name, "cache")}
        )

    def tearDown(self):
        # mf_ytdlp points the process temp dir into self.tmp; undo that first.
        if self.saved_tmpdir is None:
            os.environ.pop("TMPDIR", None)
        else:
            os.environ["TMPDIR"] = self.saved_tmpdir
        tempfile.tempdir = None
        mf_ytdlp._environment_tmp_dir = None
        self.tmp.cleanup()

    def test_probe_and_download(self):
        info = json.loads(mf_ytdlp.probe(self.URL, self.opts))
        self.assertNotIn("error", info, info)
        self.assertEqual(info["id"], "jNQXAC9IVRw")
        self.assertGreater(info["duration"], 10)

        progress = []
        path = mf_ytdlp.download_audio(self.URL, os.path.join(self.tmp.name, "dl"), self.opts, progress.append, None)
        self.assertTrue(os.path.isfile(path), path)
        self.assertTrue(path.endswith(".m4a"), path)
        self.assertEqual(progress[-1], 1.0)

    def test_runtime_info(self):
        info = json.loads(mf_ytdlp.runtime_info(self.opts))
        self.assertIn("yt_dlp", info)
        if os.environ.get("MF_QJS"):
            self.assertTrue(info["js_ok"], info)


if __name__ == "__main__":
    unittest.main()
