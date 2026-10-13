use serde_json::json;

use super::*;

macro_rules! fixture {
    ($name:literal) => {
        include_str!(concat!("../../../tests/fixtures/ytdlp/", $name, ".json"))
    };
}

fn parse(value: serde_json::Value) -> MediaInfo {
    parse_probe(&value.to_string()).expect("parse")
}

#[test]
fn youtube_video() {
    let info = parse_probe(fixture!("youtube_video")).unwrap();
    assert_eq!(info.id.as_deref(), Some("dQw4w9WgXcQ"));
    assert_eq!(info.title.as_deref(), Some("Rick Astley - Never Gonna Give You Up (Official Video) (4K Remaster)"));
    assert_eq!(info.uploader.as_deref(), Some("Rick Astley"));
    assert_eq!(info.duration_ms, Some(213_000));
    assert_eq!(info.webpage_url.as_deref(), Some("https://www.youtube.com/watch?v=dQw4w9WgXcQ"));
    assert_eq!(info.extractor.as_deref(), Some("Youtube"));
    assert!(info.thumbnail.as_deref().is_some_and(|t| t.starts_with("https://i.ytimg.com/")));
    assert!(info.description.as_deref().is_some_and(|d| d.contains("Never gonna give you up")));
    assert!(!info.is_playlist && !info.is_live);
    assert!(info.entries.is_empty() && info.chapters.is_empty());
    assert_eq!((info.track, info.artist, info.album), (None, None, None));
    assert!(description_timestamps(info.description.as_deref().unwrap()).is_empty());
}

#[test]
fn youtube_chapters_and_description_tracklist() {
    let info = parse_probe(fixture!("youtube_chapters")).unwrap();
    assert_eq!(info.duration_ms, Some(2_700_000));
    assert_eq!(info.chapters.len(), 15);
    assert_eq!(
        info.chapters[0],
        Chapter { start_ms: 0, end_ms: Some(240_000), title: "The Blaze – Cycle of the Sun".into() }
    );
    assert_eq!(info.chapters[14].title, "Legend of the Endless Flame");
    assert!(info.chapters.windows(2).all(|w| w[0].start_ms < w[1].start_ms));

    let stamps = description_timestamps(info.description.as_deref().unwrap());
    assert_eq!(stamps.len(), 15);
    assert_eq!(stamps[0], (0, "The Blaze – Cycle of the Sun".to_string()));
    assert_eq!(stamps[1], (240_000, "Song of Beginning".to_string()));
    assert_eq!(stamps[14], ((42 * 60 + 30) * 1000, "Legend of the Endless Flame".to_string()));
    // The description tracklist agrees with the chapters.
    let starts: Vec<u64> = info.chapters.iter().map(|c| c.start_ms).collect();
    assert_eq!(stamps.iter().map(|s| s.0).collect::<Vec<_>>(), starts);
}

#[test]
fn youtube_flat_playlist() {
    let info = parse_probe(fixture!("youtube_playlist_flat")).unwrap();
    assert!(info.is_playlist);
    assert_eq!(info.extractor.as_deref(), Some("YoutubeTab"));
    assert_eq!(info.title.as_deref(), Some("Forever and More"));
    let urls: Vec<&str> = info.entries.iter().map(|e| e.url.as_str()).collect();
    assert_eq!(urls, ["https://www.youtube.com/watch?v=QJjrf9RkzGo", "https://www.youtube.com/watch?v=1hCm58jzquQ"]);
    assert_eq!(info.entries[0].index, 0);
    assert_eq!(info.entries[1].index, 1);
    assert_eq!(info.entries[0].id.as_deref(), Some("QJjrf9RkzGo"));
    assert_eq!(info.entries[0].title.as_deref(), Some("Rick Astley - Forever and More (Official Video)"));
    assert_eq!(info.entries[0].duration_ms, Some(227_000));
    assert_eq!(info.entries[1].duration_ms, Some(436_000));
}

#[test]
fn youtube_music_playlist_keeps_music_host() {
    let info = parse_probe(fixture!("youtube_music_playlist_flat")).unwrap();
    assert!(info.entries.iter().all(|e| e.url.starts_with("https://music.youtube.com/watch?v=")));

    // Entries reduced to id + ie_key are rebuilt on the YouTube Music host.
    let mut value: serde_json::Value = serde_json::from_str(fixture!("youtube_music_playlist_flat")).unwrap();
    for entry in value["entries"].as_array_mut().unwrap() {
        entry.as_object_mut().unwrap().remove("url");
    }
    let info = parse(value);
    let urls: Vec<&str> = info.entries.iter().map(|e| e.url.as_str()).collect();
    assert_eq!(
        urls,
        ["https://music.youtube.com/watch?v=QJjrf9RkzGo", "https://music.youtube.com/watch?v=1hCm58jzquQ"]
    );
}

#[test]
fn youtube_channel_playlists_tab() {
    let info = parse_probe(fixture!("youtube_channel_playlists_flat")).unwrap();
    assert!(info.is_playlist);
    assert_eq!(info.entries.len(), 5);
    assert!(info.entries.iter().all(|e| e.url.starts_with("https://www.youtube.com/playlist?list=PL")));
    assert_eq!(info.entries[2].title.as_deref(), Some("Forever and More"));
}

#[test]
fn soundcloud_track() {
    let info = parse_probe(fixture!("soundcloud_track")).unwrap();
    assert_eq!(info.extractor.as_deref(), Some("Soundcloud"));
    assert_eq!(info.track.as_deref(), Some("Flickermood"));
    assert_eq!(info.artist.as_deref(), Some("Forss"));
    assert_eq!(info.uploader.as_deref(), Some("Forss"));
    assert_eq!(info.duration_ms, Some(213_886));
    assert_eq!(info.webpage_url.as_deref(), Some("https://soundcloud.com/forss/flickermood"));
    assert!(info.thumbnail.is_some());
}

#[test]
fn soundcloud_set() {
    let info = parse_probe(fixture!("soundcloud_set_flat")).unwrap();
    assert!(info.is_playlist);
    assert_eq!(info.album.as_deref(), Some("Soulhack"));
    assert_eq!(info.duration_ms, Some(3_247_119));
    assert_eq!(info.entries.len(), 11);
    assert_eq!(info.entries[0].url, "https://soundcloud.com/forss/city-ports");
    assert_eq!(info.entries[0].id.as_deref(), Some("290"));
    // Flat SoundCloud entries carry no title.
    assert_eq!(info.entries[0].title, None);
}

#[test]
fn bandcamp_album() {
    let info = parse_probe(fixture!("bandcamp_album_flat")).unwrap();
    assert!(info.is_playlist);
    assert_eq!(info.entries.len(), 22);
    assert_eq!(info.entries[0].url, "https://blazo.bandcamp.com/track/intro");
    assert_eq!(info.entries[1].title.as_deref(), Some("Kero One - Keep It Alive (Blazo remix)"));
    assert_eq!(info.entries[21].index, 21);
}

#[test]
fn mixcloud_show() {
    let info = parse_probe(fixture!("mixcloud_show")).unwrap();
    assert_eq!(info.extractor.as_deref(), Some("Mixcloud"));
    assert_eq!(info.duration_ms, Some(3_723_000));
    assert_eq!(info.webpage_url.as_deref(), Some("https://www.mixcloud.com/dholbach/cryptkeeper/"));
    assert!(info.artist.as_deref().is_some_and(|a| a.contains("Porter Robinson")));
    assert!(!info.is_playlist);
}

#[test]
fn nested_entries_are_flattened_in_order() {
    let info = parse(json!({
        "_type": "playlist",
        "id": "UCxyz",
        "webpage_url": "https://www.youtube.com/@someone",
        "extractor_key": "YoutubeTab",
        "entries": [
            {"_type": "playlist", "title": "Videos", "entries": [
                {"_type": "url", "ie_key": "Youtube", "id": "aaaaaaaaaaa", "title": "A", "duration": 61.5},
                null,
                {"_type": "url", "ie_key": "Youtube", "id": "bbbbbbbbbbb", "title": "[Private video]"},
                {"_type": "url", "ie_key": "Youtube", "id": "ccccccccccc", "url": "https://www.youtube.com/watch?v=ccccccccccc"}
            ]},
            {"_type": "playlist", "title": "Shorts", "entries": [
                {"_type": "url", "ie_key": "Youtube", "id": "ddddddddddd", "url": "ddddddddddd", "title": "D"}
            ]},
            {"_type": "url", "ie_key": "YoutubeTab", "id": "PLabc", "title": "A playlist"},
            {"_type": "url", "ie_key": "Vimeo", "id": "76979871"},
            {"_type": "url", "ie_key": "Soundcloud", "id": "1"},
            {"_type": "url", "url": "file:///etc/passwd", "title": "local"},
            {"_type": "url", "url": "javascript:alert(1)"}
        ]
    }));
    let got: Vec<(u32, &str)> = info.entries.iter().map(|e| (e.index, e.url.as_str())).collect();
    assert_eq!(
        got,
        [
            (0, "https://www.youtube.com/watch?v=aaaaaaaaaaa"),
            (1, "https://www.youtube.com/watch?v=ccccccccccc"),
            (2, "https://www.youtube.com/watch?v=ddddddddddd"),
            (3, "https://www.youtube.com/playlist?list=PLabc"),
            (4, "https://vimeo.com/76979871"),
        ]
    );
    assert_eq!(info.entries[0].duration_ms, Some(61_500));
    assert_eq!(info.entries[2].title.as_deref(), Some("D"));
}

#[test]
fn deep_nesting_is_bounded() {
    let mut value = json!({"_type": "url", "ie_key": "Youtube", "id": "aaaaaaaaaaa"});
    for _ in 0..20 {
        value = json!({"_type": "playlist", "entries": [value]});
    }
    let info = parse(value);
    assert!(info.is_playlist);
    assert!(info.entries.is_empty());
}

#[test]
fn empty_playlist_is_still_a_playlist() {
    let info = parse(json!({"_type": "playlist", "id": "PLx", "title": "Empty", "entries": []}));
    assert!(info.is_playlist);
    assert!(info.entries.is_empty());
}

#[test]
fn lenient_values() {
    let info = parse(json!({
        "_type": "video",
        "id": 7_312_345_678_901u64,
        "title": "  Clip  ",
        "duration": "15.5",
        "uploader": "",
        "channel": "Chan",
        "creators": ["A", " ", "B"],
        "thumbnails": [{"url": "https://t/1.jpg"}, {"url": "https://t/2.jpg"}, {"id": "x"}],
        "chapters": [{"start_time": 30, "end_time": null, "title": "B"}, {"start_time": 0.25, "title": "A"}, {"title": "no start"}],
        "live_status": "is_live",
        "url": "https://cdn.example/stream.m4a?sig=1"
    }));
    assert_eq!(info.id.as_deref(), Some("7312345678901"));
    assert_eq!(info.title.as_deref(), Some("Clip"));
    assert_eq!(info.duration_ms, Some(15_500));
    assert_eq!(info.uploader.as_deref(), Some("Chan"));
    assert_eq!(info.artist.as_deref(), Some("A, B"));
    assert_eq!(info.thumbnail.as_deref(), Some("https://t/2.jpg"));
    assert_eq!(
        info.chapters.iter().map(|c| (c.start_ms, c.title.as_str())).collect::<Vec<_>>(),
        [(250, "A"), (30_000, "B")]
    );
    assert!(info.is_live);
    // A resolved video's top-level `url` is the stream, not the page.
    assert_eq!(info.webpage_url, None);

    let info = parse(json!({"_type": "url", "url": "https://www.tiktok.com/@a/video/1", "ie_key": "TikTok"}));
    assert_eq!(info.webpage_url.as_deref(), Some("https://www.tiktok.com/@a/video/1"));
    assert_eq!(info.extractor.as_deref(), Some("TikTok"));

    let info = parse(json!({"duration": -5, "title": 3}));
    assert_eq!(info.duration_ms, None);
    assert_eq!(info.title.as_deref(), Some("3"));
}

#[test]
fn rejects_non_objects() {
    assert!(matches!(parse_probe(""), Err(MfError::Platform { .. })));
    assert!(matches!(parse_probe("not json"), Err(MfError::Platform { .. })));
    assert!(matches!(parse_probe("[1, 2]"), Err(MfError::Platform { .. })));
    assert!(parse_probe("{}").is_ok());
}

fn first(line: &str) -> Option<(u64, String)> {
    description_timestamps(line).into_iter().next()
}

#[test]
fn timestamp_regex_semantics() {
    let s = |m: u64, sec: u64| (m * 60 + sec) * 1000;
    assert_eq!(first("12:34 Artist - Title"), Some((s(12, 34), "Artist - Title".into())));
    assert_eq!(first("1:02:03 Long one"), Some((3_723_000, "Long one".into())));
    assert_eq!(first("10:00:00 Late"), Some((36_000_000, "Late".into())));
    assert_eq!(first("1:2:33 x"), Some((3_753_000, "x".into())));
    assert_eq!(first("0:05 Intro"), Some((5_000, "Intro".into())));
    assert_eq!(first("Artist - Title (3:45)"), Some((s(3, 45), "Artist - Title".into())));
    assert_eq!(first("[00:00] Intro"), Some((0, "Intro".into())));
    assert_eq!(first("05. Song Name - 14:20"), Some((s(14, 20), "05. Song Name".into())));
    assert_eq!(first("00:00 - 03:30 Artist – Title"), Some((0, "Artist – Title".into())));
    assert_eq!(first("4:20pm meetup"), Some((s(4, 20), "pm meetup".into())));
    // Minutes above 59 need hours; preceding digits block a match.
    assert_eq!(first("75:30 too long"), None);
    assert_eq!(first("123:45 nope"), None);
    assert_eq!(first("12:345 nope"), None);
    assert_eq!(first("1:60 nope"), None);
    // The leftmost match wins, after a blocked start.
    assert_eq!(first("100:00:00 x").map(|m| m.0), Some(0));
    assert_eq!(first("no stamps here"), None);
    assert_eq!(first("0:00"), Some((0, String::new())));
    // Multibyte text around the stamp.
    assert_eq!(first("▶ 01:15 — Café Señorita"), Some((s(1, 15), "▶ Café Señorita".into())));
}

#[test]
fn one_timestamp_per_line_in_order() {
    let text = "Tracklist:\n\n00:00 First\nnot a stamp\n03:10 Second 03:50\n  1:02:03 - Third  \n";
    assert_eq!(
        description_timestamps(text),
        vec![(0, "First".into()), (190_000, "Second".into()), (3_723_000, "Third".into())]
    );
}
