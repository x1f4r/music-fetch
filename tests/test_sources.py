import pytest

from music_fetch.sources import (
    SourceResolver,
    UnsafeURLError,
    _entry_download_url,
    _flatten_entries,
    _single_video_extractor_url,
    is_direct_media_url,
    is_url,
    normalize_source_url,
    probe_direct_media_url,
    yt_dlp_extract_args,
    yt_dlp_extract_info,
)


def test_url_detection() -> None:
    assert is_url("https://youtube.com/watch?v=abc")
    assert not is_url("/tmp/file.mp4")


def test_direct_media_detection() -> None:
    assert is_direct_media_url("https://cdn.example.com/file.mp4")
    assert not is_direct_media_url("https://youtube.com/watch?v=abc")


def test_flatten_entries_flattens_nested_playlist_groups() -> None:
    entries = [
        {"entries": [{"id": "a"}, {"id": "b"}]},
        {"id": "c"},
        None,
    ]
    flattened = _flatten_entries(entries)
    assert [entry["id"] for entry in flattened] == ["a", "b", "c"]


def test_entry_download_url_reconstructs_youtube_music_watch_url() -> None:
    entry = {"id": "track123", "extractor_key": "YoutubeTab"}
    url = _entry_download_url(entry, "https://music.youtube.com/playlist?list=PL123", "PL123")
    assert url == "https://music.youtube.com/watch?v=track123&list=PL123"


def test_entry_download_url_rejects_private_extractor_media_url() -> None:
    entry = {"url": "http://169.254.169.254/latest/meta-data/"}

    with pytest.raises(UnsafeURLError):
        _entry_download_url(entry, "https://example.com/watch", None)


def test_probe_direct_media_url_uses_head_content_type(monkeypatch) -> None:
    class Response:
        headers = {"content-type": "audio/mpeg"}
        is_redirect = False

    class Client:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def head(self, value):
            return Response()

    monkeypatch.setattr("music_fetch.sources.httpx.Client", Client)
    # ``_assert_safe_external_url`` resolves DNS; bypass it so a synthetic
    # hostname like ``cdn.example.com`` (no A record) doesn't fail the
    # SSRF check. Coverage for that check lives in dedicated SSRF tests.
    monkeypatch.setattr("music_fetch.sources._assert_safe_external_url", lambda _url: None)
    assert probe_direct_media_url("https://cdn.example.com/stream")


def test_single_video_extractor_url_strips_youtube_radio_playlist_params() -> None:
    url = "https://www.youtube.com/watch?v=gBT60YL3lNw&list=RDgBT60YL3lNw&start_radio=1"
    assert _single_video_extractor_url(url) == "https://www.youtube.com/watch?v=gBT60YL3lNw"


def test_single_video_extractor_url_normalizes_shorts_to_watch_url() -> None:
    url = "https://www.youtube.com/shorts/abc123?feature=share"
    assert _single_video_extractor_url(url) == "https://www.youtube.com/watch?v=abc123"


def test_yt_dlp_extract_args_force_single_video_mode_for_watch_urls() -> None:
    url = "https://www.youtube.com/watch?v=gBT60YL3lNw&list=RDgBT60YL3lNw&start_radio=1"
    assert yt_dlp_extract_args(url)[-2:] == ["--no-playlist", "https://www.youtube.com/watch?v=gBT60YL3lNw"]


def test_yt_dlp_extract_args_keep_playlist_urls_expandable() -> None:
    url = "https://music.youtube.com/playlist?list=PL123"
    assert yt_dlp_extract_args(url)[-1] == url
    assert "--no-playlist" not in yt_dlp_extract_args(url)


def test_normalize_source_url_strips_tracking_from_instagram_reel() -> None:
    url = "https://www.instagram.com/reel/Cxyz123/?utm_source=ig_web_copy_link&igsh=MzRlODBiNWFlZA=="
    assert normalize_source_url(url) == "https://www.instagram.com/reel/Cxyz123"


def test_normalize_source_url_keeps_only_tiktok_video_path() -> None:
    url = "https://www.tiktok.com/@artist/video/1234567890?is_from_webapp=1&sender_device=pc"
    assert normalize_source_url(url) == "https://www.tiktok.com/@artist/video/1234567890"


def test_normalize_source_url_canonicalizes_x_status_links() -> None:
    url = "https://twitter.com/example/status/1234567890?s=20&t=abcdef"
    assert normalize_source_url(url) == "https://x.com/example/status/1234567890"


def test_normalize_source_url_strips_spotify_share_tracking() -> None:
    url = "https://open.spotify.com/track/abc123?si=deadbeef&utm_source=copy-link"
    assert normalize_source_url(url) == "https://open.spotify.com/track/abc123"


def test_normalize_source_url_keeps_only_pornhub_viewkey() -> None:
    url = "https://www.pornhub.com/view_video.php?viewkey=ph12345&foo=bar"
    assert normalize_source_url(url) == "https://www.pornhub.com/view_video.php?viewkey=ph12345"


def test_normalize_source_url_strips_reddit_share_tracking() -> None:
    url = "https://www.reddit.com/r/test/comments/abc123/example_post/?utm_source=share&utm_medium=ios_app&rdt=12345"
    assert normalize_source_url(url) == "https://www.reddit.com/r/test/comments/abc123/example_post"


def test_normalize_source_url_follows_known_shorteners(monkeypatch) -> None:
    class Response:
        def __init__(self, url: str, status_code: int = 200):
            self.url = url
            self.status_code = status_code

    class Client:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def head(self, value):
            return Response("https://open.spotify.com/track/abc123?si=deadbeef")

        def get(self, value):
            return Response("https://open.spotify.com/track/abc123?si=deadbeef")

    monkeypatch.setattr("music_fetch.sources.httpx.Client", Client)
    monkeypatch.setattr("music_fetch.sources._assert_safe_external_url", lambda _url: None)
    assert normalize_source_url("https://spotify.link/demo") == "https://open.spotify.com/track/abc123"


def test_normalize_source_url_follows_safe_shortener_redirect_location(monkeypatch) -> None:
    class Response:
        def __init__(self, url: str, *, location: str | None = None):
            self.url = url
            self.status_code = 302 if location else 200
            self.is_redirect = location is not None
            self.headers = {"location": location} if location else {}

    class Client:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def head(self, value):
            if value == "https://spotify.link/demo":
                return Response(value, location="https://open.spotify.com/track/abc123?si=deadbeef")
            return Response(value)

        def get(self, value):
            return Response(value)

    monkeypatch.setattr("music_fetch.sources.httpx.Client", Client)
    monkeypatch.setattr("music_fetch.sources._assert_safe_external_url", lambda _url: None)

    assert normalize_source_url("https://spotify.link/demo") == "https://open.spotify.com/track/abc123"


def test_normalize_source_url_blocks_shortener_redirect_to_private_host(monkeypatch) -> None:
    class Response:
        def __init__(self, url: str, location: str | None = None):
            self.url = url
            self.status_code = 302 if location else 200
            self.is_redirect = location is not None
            self.headers = {"location": location} if location else {}

    class Client:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def head(self, value):
            return Response(value, "http://169.254.169.254/latest/meta-data/")

        def get(self, value):
            return Response(value)

    def assert_safe(url: str) -> None:
        if "169.254.169.254" in url:
            raise UnsafeURLError("blocked private redirect")

    monkeypatch.setattr("music_fetch.sources.httpx.Client", Client)
    monkeypatch.setattr("music_fetch.sources._assert_safe_external_url", assert_safe)

    with pytest.raises(UnsafeURLError):
        normalize_source_url("https://spotify.link/demo")


def test_normalize_source_url_blocks_shortener_redirect_to_private_host_with_real_guard(monkeypatch) -> None:
    class Response:
        def __init__(self, url: str, location: str | None = None):
            self.url = url
            self.status_code = 302 if location else 200
            self.is_redirect = location is not None
            self.headers = {"location": location} if location else {}

    class Client:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def head(self, value):
            return Response(value, "http://169.254.169.254/latest/meta-data/")

        def get(self, value):
            return Response(value)

    monkeypatch.setattr("music_fetch.sources.httpx.Client", Client)

    with pytest.raises(UnsafeURLError):
        normalize_source_url("https://spotify.link/demo")


def test_source_resolver_rejects_loopback_before_yt_dlp(monkeypatch, tmp_path) -> None:
    called = False

    def extract_info(_url):
        nonlocal called
        called = True
        return {}

    monkeypatch.setattr("music_fetch.sources.yt_dlp_extract_info", extract_info)

    resolver = SourceResolver(tmp_path)
    with pytest.raises(UnsafeURLError):
        list(resolver.iter_resolve_inputs("job-1", ["http://127.0.0.1/admin"]))

    assert called is False


def test_source_resolver_rejects_private_download_url_from_extractor(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        "music_fetch.sources.yt_dlp_extract_info",
        lambda _url: {"id": "entry-1", "url": "http://169.254.169.254/latest/meta-data/"},
    )
    monkeypatch.setattr("music_fetch.sources.probe_direct_media_url", lambda _url: False)

    def real_guard_for_entry_urls(url: str) -> None:
        if "169.254.169.254" in url:
            raise UnsafeURLError("blocked private extractor URL")

    monkeypatch.setattr("music_fetch.sources._assert_safe_external_url", real_guard_for_entry_urls)

    resolver = SourceResolver(tmp_path)
    with pytest.raises(UnsafeURLError):
        list(resolver.iter_resolve_inputs("job-1", ["https://example.com/watch"]))


def test_source_resolver_rejects_probe_redirect_to_private_host_before_yt_dlp(monkeypatch, tmp_path) -> None:
    called = False

    class Response:
        is_redirect = True
        headers = {"location": "http://169.254.169.254/latest/meta-data/"}

    class Client:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def head(self, value):
            return Response()

    def assert_safe(url: str) -> None:
        if "169.254.169.254" in url:
            raise UnsafeURLError("blocked private redirect")

    def extract_info(_url):
        nonlocal called
        called = True
        return {}

    monkeypatch.setattr("music_fetch.sources.httpx.Client", Client)
    monkeypatch.setattr("music_fetch.sources._assert_safe_external_url", assert_safe)
    monkeypatch.setattr("music_fetch.sources.yt_dlp_extract_info", extract_info)

    resolver = SourceResolver(tmp_path)
    with pytest.raises(UnsafeURLError):
        list(resolver.iter_resolve_inputs("job-1", ["https://example.com/watch"]))

    assert called is False


def test_yt_dlp_extract_info_rejects_loopback_before_command(monkeypatch) -> None:
    def fail_run_command(_args):
        raise AssertionError("yt-dlp command should not run for unsafe URLs")

    monkeypatch.setattr("music_fetch.sources.run_command", fail_run_command)

    with pytest.raises(UnsafeURLError):
        yt_dlp_extract_info("http://127.0.0.1/admin")
