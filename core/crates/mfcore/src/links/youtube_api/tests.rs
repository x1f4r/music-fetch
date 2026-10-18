use super::*;

fn page(player: &Value, initial: &Value) -> WatchData {
    WatchData { player: Some(player.clone()), initial: Some(initial.clone()), client: None }
}

#[test]
fn counts_and_clocks() {
    assert_eq!(parse_count("1.2K"), 1200);
    assert_eq!(parse_count("3M"), 3_000_000);
    assert_eq!(parse_count("12"), 12);
    assert_eq!(parse_count("1,234 likes"), 1234);
    assert_eq!(parse_count(""), 0);
    assert_eq!(clock_ms("1:02:03"), Some(3_723_000));
    assert_eq!(clock_ms("4:05"), Some(245_000));
    assert_eq!(clock_ms("x"), None);
}

#[test]
fn handles_videos_and_playlists_only() {
    assert!(handles("https://www.youtube.com/watch?v=dQw4w9WgXcQ"));
    assert!(handles("https://youtu.be/dQw4w9WgXcQ"));
    assert!(handles("https://www.youtube.com/playlist?list=PLSbDLF8wQ3oKcstd9ybCSv2lNm_8NTYkI"));
    assert!(!handles("https://www.youtube.com/@channel"));
    assert!(!handles("https://soundcloud.com/a/b"));
}

#[test]
fn video_fields_chapters_and_the_music_card() {
    let player = json!({
        "playabilityStatus": {"status": "OK"},
        "videoDetails": {"videoId": "abc", "title": "A mix", "author": "Mix Channel", "lengthSeconds": "600",
            "shortDescription": "0:00 Intro\n2:30 Song B", "isLive": false,
            "thumbnail": {"thumbnails": [{"url": "https://i.ytimg.com/s.jpg", "width": 120}, {"url": "https://i.ytimg.com/l.jpg", "width": 1280}]}}
    });
    let initial = json!({
        "playerOverlays": {"playerOverlayRenderer": {"decoratedPlayerBarRenderer": {"decoratedPlayerBarRenderer": {"playerBar": {
            "multiMarkersPlayerBarRenderer": {"markersMap": [{"key": "DESCRIPTION_CHAPTERS", "value": {"chapters": [
                {"chapterRenderer": {"title": {"simpleText": "Song B"}, "timeRangeStartMillis": 150000}},
                {"chapterRenderer": {"title": {"simpleText": "Intro"}, "timeRangeStartMillis": 0}}
            ]}}]}}}}}},
        "engagementPanels": [{"engagementPanelSectionListRenderer": {"content": {"structuredDescriptionContentRenderer": {"items": [
            {"horizontalCardListRenderer": {"header": {"richListHeaderRenderer": {"title": {"simpleText": "Music"}}},
                "cards": [{"videoAttributeViewModel": {"title": "Song B", "subtitle": "Artist B", "secondarySubtitle": {"content": "Album B"}}}]}}
        ]}}}}]
    });
    let info = video_info("abc", &page(&player, &initial)).unwrap();
    assert_eq!(info.title.as_deref(), Some("A mix"));
    assert_eq!(info.uploader.as_deref(), Some("Mix Channel"));
    assert_eq!(info.duration_ms, Some(600_000));
    assert_eq!(info.webpage_url.as_deref(), Some("https://www.youtube.com/watch?v=abc"));
    assert_eq!(info.extractor.as_deref(), Some("Youtube"));
    assert_eq!(info.thumbnail.as_deref(), Some("https://i.ytimg.com/l.jpg"));
    assert_eq!(info.description.as_deref(), Some("0:00 Intro\n2:30 Song B"));
    assert_eq!(
        info.chapters,
        vec![
            Chapter { start_ms: 0, end_ms: Some(150_000), title: "Intro".into() },
            Chapter { start_ms: 150_000, end_ms: Some(600_000), title: "Song B".into() },
        ]
    );
    assert_eq!(
        (info.track.as_deref(), info.artist.as_deref(), info.album.as_deref()),
        (Some("Song B"), Some("Artist B"), Some("Album B"))
    );
    assert!(!info.is_playlist && !info.is_live);
}

#[test]
fn chapters_from_the_engagement_panel() {
    let initial = json!({"engagementPanels": [{"engagementPanelSectionListRenderer": {
    "targetId": "engagement-panel-macro-markers-description-chapters",
    "content": {"macroMarkersListRenderer": {"contents": [
        {"macroMarkersListItemRenderer": {"title": {"simpleText": "One"}, "timeDescription": {"simpleText": "0:00"},
            "onTap": {"watchEndpoint": {"startTimeSeconds": 0}}}},
        {"macroMarkersListItemRenderer": {"title": {"simpleText": "Two"}, "timeDescription": {"simpleText": "3:32"}}}
    ]}}}}]});
    let chapters = chapters(&initial, None);
    assert_eq!(chapters.len(), 2);
    assert_eq!(chapters[1], Chapter { start_ms: 212_000, end_ms: None, title: "Two".into() });
    assert_eq!(chapters[0].end_ms, Some(212_000));
}

#[test]
fn art_tracks_and_unavailable_videos() {
    let player = json!({"playabilityStatus": {"status": "OK"}, "videoDetails": {"videoId": "t", "title": "Song",
        "author": "Artist - Topic", "lengthSeconds": "200",
        "shortDescription": "Provided to YouTube by Label\n\nSong · Artist\n\nThe Album\n\n℗ 2020 Label\n\nReleased on: 2020-01-01"}});
    let info = video_info("t", &page(&player, &json!({}))).unwrap();
    assert_eq!(
        (info.track.as_deref(), info.artist.as_deref(), info.album.as_deref()),
        (Some("Song"), Some("Artist"), Some("The Album"))
    );

    let gone = json!({"playabilityStatus": {"status": "ERROR", "reason": "Video unavailable"}});
    assert_eq!(
        video_info("x", &page(&gone, &json!({}))).unwrap_err(),
        MfError::MediaUnavailable { message: "Video unavailable.".into() }
    );
    let bot =
        json!({"playabilityStatus": {"status": "LOGIN_REQUIRED", "reason": "Sign in to confirm you’re not a bot"}});
    assert!(matches!(video_info("x", &page(&bot, &json!({}))), Err(MfError::MediaUnavailable { .. })));
    assert!(is_bot_check(&bot.to_string()));
}

#[test]
fn comment_threads_in_both_shapes() {
    let initial = json!({"contents": {"twoColumnWatchNextResults": {"results": {"results": {"contents": [
        {"videoPrimaryInfoRenderer": {}},
        {"itemSectionRenderer": {"sectionIdentifier": "comment-item-section", "contents": [
            {"continuationItemRenderer": {"continuationEndpoint": {"continuationCommand": {"token": "T1"}}}}]}}
    ]}}}}});
    assert_eq!(comments_token(&initial).as_deref(), Some("T1"));

    let page = json!({
        "onResponseReceivedEndpoints": [
            {"reloadContinuationItemsCommand": {"continuationItems": [{"commentsHeaderRenderer": {}}]}},
            {"reloadContinuationItemsCommand": {"continuationItems": [
                {"commentThreadRenderer": {"commentViewModel": {"commentViewModel": {"commentKey": "k1", "commentId": "c1"}}}},
                {"commentThreadRenderer": {"commentViewModel": {"commentViewModel": {"commentKey": "k2", "commentId": "c2", "pinnedText": "Pinned by @Mix"}}}},
                {"commentThreadRenderer": {"comment": {"commentRenderer": {"authorText": {"simpleText": "@old"},
                    "contentText": {"runs": [{"text": "legacy "}, {"text": "text"}]}, "voteCount": {"simpleText": "7"}}}}},
                {"continuationItemRenderer": {"button": {"buttonRenderer": {"command": {"continuationCommand": {"token": "T2"}}}}}}
            ]}}
        ],
        "frameworkUpdates": {"entityBatchUpdate": {"mutations": [
            {"payload": {"commentEntityPayload": {"key": "k1", "properties": {"commentId": "c1", "content": {"content": "great mix"}},
                "author": {"displayName": "@fan", "isCreator": false}, "toolbar": {"likeCountNotliked": "1.5K"}}}},
            {"payload": {"commentEntityPayload": {"key": "k2", "properties": {"commentId": "c2", "content": {"content": "0:00 A - B\n3:00 C - D"}},
                "author": {"displayName": "@Mix", "isCreator": true}, "toolbar": {"likeCountNotliked": "20"}}}}
        ]}}
    });
    let (comments, next) = comments_page(&page);
    assert_eq!(next.as_deref(), Some("T2"));
    assert_eq!(comments.len(), 3);
    assert_eq!(comments[0].text, "great mix");
    assert_eq!(comments[0].like_count, 1500);
    assert!(comments[1].is_pinned && comments[1].author_is_uploader);
    assert_eq!(comments[2].text, "legacy text");
    let sorted = sort_comments(comments);
    assert_eq!(sorted[0].text, "0:00 A - B\n3:00 C - D", "pinned first");
    assert_eq!(sorted[1].text, "great mix", "then by likes");
}

#[test]
fn playlist_pages_and_continuations() {
    let renderer = |id: &str, title: &str| {
        json!({"playlistVideoRenderer": {"videoId": id, "title": {"runs": [{"text": title}]},
        "shortBylineText": {"runs": [{"text": "Channel"}]}, "lengthSeconds": "61", "isPlayable": true}})
    };
    let initial = json!({
        "metadata": {"playlistMetadataRenderer": {"title": "My list", "description": "d"}},
        "sidebar": {"playlistSidebarRenderer": {"items": [{}, {"playlistSidebarSecondaryInfoRenderer": {"videoOwner": {"videoOwnerRenderer": {"title": {"runs": [{"text": "Owner"}]}}}}}]}},
        "contents": {"twoColumnBrowseResultsRenderer": {"tabs": [{"tabRenderer": {"content": {"sectionListRenderer": {"contents": [{"itemSectionRenderer": {"contents": [
            {"playlistVideoListRenderer": {"contents": [
                renderer("a", "First"),
                {"playlistVideoRenderer": {"videoId": "gone", "title": {"runs": [{"text": "[Deleted video]"}]}}},
                renderer("b", "Second"),
                {"continuationItemRenderer": {"continuationEndpoint": {"continuationCommand": {"token": "P2"}}}}
            ]}}
        ]}}]}}}}]}}
    });
    let page = playlist_page(&initial);
    assert_eq!(page.title.as_deref(), Some("My list"));
    assert_eq!(page.uploader.as_deref(), Some("Owner"));
    assert_eq!(page.entries.len(), 2);
    assert_eq!(page.entries[1].url, "https://www.youtube.com/watch?v=b");
    assert_eq!(page.entries[1].duration_ms, Some(61_000));
    assert_eq!(page.continuation.as_deref(), Some("P2"));

    let more = json!({"onResponseReceivedActions": [{"appendContinuationItemsAction": {"continuationItems": [renderer("c", "Third")]}}]});
    let (entries, next) = playlist_continuation(&more);
    assert_eq!(entries.len(), 1);
    assert_eq!(next, None);

    let missing = json!({"alerts": [{"alertRenderer": {"type": "ERROR", "text": {"runs": [{"text": "The playlist does not exist."}]}}}]});
    assert_eq!(playlist_alert(&missing).as_deref(), Some("The playlist does not exist."));
}

#[test]
fn the_client_comes_from_the_page() {
    let html = r#"<script>ytcfg.set({"INNERTUBE_CLIENT_VERSION":"2.20261015.01.00","VISITOR_DATA":"Cgt4eHh4eHh4eHh4"});</script>"#;
    let client = Client::from_page(html).unwrap();
    assert_eq!(client.version, "2.20261015.01.00");
    let context = client.context();
    assert_eq!(context["client"]["clientName"], "WEB");
    assert_eq!(context["client"]["hl"], "en");
    assert_eq!(context["client"]["visitorData"], "Cgt4eHh4eHh4eHh4");
    let fallback = Client::fallback();
    assert!(fallback.version.starts_with("2.20") && fallback.version.ends_with(".00.00"), "{}", fallback.version);
    assert_eq!(civil_date(0), (1970, 1, 1));
}

#[test]
fn chapters_from_the_description_like_yt_dlp() {
    let description = "Album - Tracklist\n\n00:00   Track 01  -  This World\n03:32   Track 02  -  Dune\n\
        06:47 Track 03 - BOUNCY\n#tags\n99:00 too late\nsee 1:00 in the middle";
    let chapters = description_chapters(description, 1_168_000);
    let starts: Vec<u64> = chapters.iter().map(|c| c.start_ms).collect();
    assert_eq!(starts, [0, 212_000, 407_000]);
    assert_eq!(chapters[0].title, "Track 01  -  This World");
    assert_eq!(chapters[2].end_ms, Some(1_168_000));
    let title_first = description_chapters("Intro 0:00\nSong A 2:10\n", 300_000);
    assert_eq!(
        title_first.iter().map(|c| (c.start_ms, c.title.as_str())).collect::<Vec<_>>(),
        [(0, "Intro"), (130_000, "Song A")]
    );
    assert!(description_chapters("no stamps here", 60_000).is_empty());
}

#[test]
fn lockup_playlist_entries() {
    let item = json!({"lockupViewModel": {"contentId": "IluRBvnYMoY", "contentType": "LOCKUP_CONTENT_TYPE_VIDEO",
        "contentImage": {"thumbnailViewModel": {"overlays": [{"thumbnailBottomOverlayViewModel": {"badges": [
            {"thumbnailBadgeViewModel": {"text": "4:44"}}]}}]}},
        "metadata": {"lockupMetadataViewModel": {"title": {"content": "Give Life Back to Music"},
            "metadata": {"contentMetadataViewModel": {"metadataRows": [{"metadataParts": [{"text": {"content": "Daft Punk"}}]},
                {"metadataParts": [{"text": {"content": "30M"}}]}]}}}}}});
    let (entries, next) = entries_of(&[
        item,
        json!({"lockupViewModel": {"contentId": "PL1", "contentType": "LOCKUP_CONTENT_TYPE_PLAYLIST"}}),
    ]);
    assert_eq!(next, None);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].id.as_deref(), Some("IluRBvnYMoY"));
    assert_eq!(entries[0].title.as_deref(), Some("Give Life Back to Music"));
    assert_eq!(entries[0].uploader.as_deref(), Some("Daft Punk"));
    assert_eq!(entries[0].duration_ms, Some(284_000));
}
