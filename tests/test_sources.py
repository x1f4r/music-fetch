from music_fetch.sources import _entry_download_url, _flatten_entries, is_direct_media_url, is_url


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
