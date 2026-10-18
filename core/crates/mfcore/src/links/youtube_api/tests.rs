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

// ------------------------------------------------------------------ recorded answers
//
// Recorded from the Legion with `mf dev youtube-probe --record` (9 requests in
// total, 75 s apart, while the hum-refs crawl reported YouTube unpaused) and
// scrubbed with tools/fixtures/scrub_youtube.py: no session data, commenters
// renamed, their texts replaced except the uploader's pinned tracklist,
// continuation tokens and entity keys replaced by placeholders.

const WATCH_PINNED: &str = include_str!("../testdata/youtube_watch_pinned.html");
const NEXT_PINNED_1: &str = include_str!("../testdata/youtube_next_pinned_1.json");
const NEXT_PINNED_2: &str = include_str!("../testdata/youtube_next_pinned_2.json");
const WATCH_TRACKLIST: &str = include_str!("../testdata/youtube_watch_tracklist.html");
const PLAYLIST_ALBUM: &str = include_str!("../testdata/youtube_playlist_album.html");
const PLAYLIST_UPLOADS: &str = include_str!("../testdata/youtube_playlist_uploads.html");

#[test]
fn recorded_mix_with_the_tracklist_in_the_pinned_comment() {
    let page = WatchData::parse(WATCH_PINNED);
    let info = video_info("JL-YFJTxftU", &page).unwrap();
    assert!(info.title.as_deref().unwrap().starts_with("summer mix - dj snake, avicii"), "{:?}", info.title);
    assert_eq!(info.uploader.as_deref(), Some("Rammor"));
    assert_eq!(info.duration_ms, Some(4_026_000));
    assert!(info.description.as_deref().unwrap().contains("Tracklist in the pin"));
    assert!(info.chapters.is_empty(), "the description has no timestamps");
    assert!(info.thumbnail.as_deref().unwrap().starts_with("https://i.ytimg.com/"));
    assert_eq!(page.client.as_ref().unwrap().version, "2.20261016.01.00");

    let token = comments_token(page.initial.as_ref().unwrap()).expect("comments token");
    assert!(token.starts_with("CONTINUATION-"), "{token}");
    let (first, next) = comments_page(&serde_json::from_str(NEXT_PINNED_1).unwrap());
    assert_eq!(first.len(), 20);
    assert!(next.is_some());
    let (second, _) = comments_page(&serde_json::from_str(NEXT_PINNED_2).unwrap());
    assert!(second.len() >= 10, "{}", second.len());
    let all = sort_comments(first.into_iter().chain(second).collect());
    let pinned = &all[0];
    assert!(pinned.is_pinned && pinned.author_is_uploader, "{pinned:?}");
    assert_eq!(pinned.author.as_deref(), Some("@rammor"));
    assert!(pinned.text.starts_with("Hope you enjoy this summer mix"));
    assert_eq!(pinned.like_count, 731);
    // The tracklist in it is what the pipeline reads (timestamps or not, one line per song).
    assert!(pinned.text.lines().count() > 15, "{}", pinned.text);
    assert!(all[1..].iter().all(|c| !c.is_pinned && !c.author_is_uploader));
    assert!(all[1..].windows(2).all(|w| w[0].like_count >= w[1].like_count), "then by likes");
    assert_eq!(all.len(), MAX_COMMENTS);
}

#[test]
fn recorded_album_stream_with_chapters_in_the_description() {
    let info = video_info("VrJUZI-U2lg", &WatchData::parse(WATCH_TRACKLIST)).unwrap();
    assert_eq!(info.uploader.as_deref(), Some("K-POP undercover"));
    assert_eq!(info.duration_ms, Some(1_168_000));
    let starts: Vec<u64> = info.chapters.iter().map(|c| c.start_ms / 1000).collect();
    assert_eq!(starts, [0, 212, 407, 593, 784, 969]);
    assert_eq!(info.chapters[1].title, "Track 02  -  Dune");
    assert_eq!(info.chapters[5].end_ms, Some(1_168_000));
}

#[test]
fn recorded_playlists() {
    let album = playlist_page(WatchData::parse(PLAYLIST_ALBUM).initial.as_ref().unwrap());
    assert_eq!(album.title.as_deref(), Some("Random Access Memories"));
    assert_eq!(album.uploader.as_deref(), Some("Daft Punk"));
    assert_eq!(album.entries.len(), 13);
    let first = &album.entries[0];
    assert_eq!(first.id.as_deref(), Some("IluRBvnYMoY"));
    assert_eq!(first.title.as_deref(), Some("Daft Punk - Give Life Back to Music (Official Audio)"));
    assert_eq!(first.uploader.as_deref(), Some("Daft Punk"));
    assert_eq!(first.duration_ms, Some(284_000));
    assert_eq!(album.continuation, None);
    assert!(
        album.entries.iter().all(|e| e.duration_ms.is_some() && e.url.starts_with("https://www.youtube.com/watch?v="))
    );

    let uploads = playlist_page(WatchData::parse(PLAYLIST_UPLOADS).initial.as_ref().unwrap());
    assert_eq!(uploads.title.as_deref(), Some("Uploads from Rammor"));
    assert_eq!(uploads.uploader.as_deref(), Some("Rammor"));
    assert_eq!(uploads.entries.len(), 100);
    assert!(
        uploads.continuation.as_deref().is_some_and(|t| t.starts_with("CONTINUATION-")),
        "a continuation for the next 100"
    );
}

const BROWSE_UPLOADS_1: &str = include_str!("../testdata/youtube_browse_uploads_1.json");
const BROWSE_UPLOADS_2: &str = include_str!("../testdata/youtube_browse_uploads_2.json");

#[test]
fn recorded_browse_continuations() {
    let (second, next) = playlist_continuation(&serde_json::from_str(BROWSE_UPLOADS_1).unwrap());
    assert_eq!(second.len(), 100);
    assert!(next.is_some());
    let (third, _) = playlist_continuation(&serde_json::from_str(BROWSE_UPLOADS_2).unwrap());
    assert_eq!(third.len(), 100);
    assert!(second.iter().chain(&third).all(|e| e.id.is_some() && e.title.is_some()));
}

/// The whole probe against a local server standing in for www.youtube.com:
/// the requests it makes, their headers and bodies, and what it reports.
mod flow {
    use super::*;
    use crate::shazam::test_server::{Reply, TestServer};
    use crate::util::endpoints::{self, TestEndpoints};

    fn serve(replies: Vec<Reply>) -> TestServer {
        let server = TestServer::start(replies);
        endpoints::install(Some(
            TestEndpoints::new(vec![("https://www.youtube.com".into(), server.url())], None).unwrap(),
        ));
        server
    }

    fn html(body: &str) -> Reply {
        Reply::new(200, body).header("Content-Type", "text/html; charset=utf-8")
    }

    #[tokio::test]
    async fn a_video_with_its_comments() {
        let _guard = endpoints::TEST_LOCK.lock().await;
        let server = serve(vec![html(WATCH_PINNED), Reply::json(200, NEXT_PINNED_1), Reply::json(200, NEXT_PINNED_2)]);
        let http = crate::links::fetch::test_client();
        let info = probe(&http, "https://youtu.be/JL-YFJTxftU?si=x", true, &CancelToken::new()).await;
        endpoints::install(None);
        let info = info.unwrap();
        assert_eq!(info.id.as_deref(), Some("JL-YFJTxftU"));
        assert_eq!(info.comments.len(), MAX_COMMENTS);
        assert!(info.comments[0].is_pinned);

        let requests = server.requests();
        assert_eq!(requests.len(), 3, "the page and two comment pages");
        assert_eq!(requests[0].method, "GET");
        assert_eq!(requests[0].target, "/watch?v=JL-YFJTxftU&hl=en&persist_hl=1");
        assert_eq!(requests[0].header("cookie"), Some("SOCS=CAI; CONSENT=YES+1"));
        let token = comments_token(WatchData::parse(WATCH_PINNED).initial.as_ref().unwrap()).unwrap();
        let (_, second_token) = comments_page(&serde_json::from_str(NEXT_PINNED_1).unwrap());
        for (request, expected) in requests[1..].iter().zip([token, second_token.unwrap()]) {
            assert_eq!(request.method, "POST");
            assert_eq!(request.target, "/youtubei/v1/next?prettyPrint=false");
            assert_eq!(request.header("origin"), Some("https://www.youtube.com"));
            assert_eq!(request.header("cookie"), Some("SOCS=CAI; CONSENT=YES+1"));
            assert_eq!(request.header("x-youtube-client-version"), Some("2.20261016.01.00"));
            let body: Value = serde_json::from_slice(&request.body).unwrap();
            assert_eq!(body["continuation"], expected);
            assert_eq!(body["context"]["client"]["clientName"], "WEB");
            assert_eq!(body["context"]["client"]["hl"], "en");
        }
        assert!(requests[1].at - requests[0].at >= Duration::from_millis(900), "requests are spaced");
    }

    #[tokio::test]
    async fn a_playlist_with_continuations_up_to_the_cap() {
        let _guard = endpoints::TEST_LOCK.lock().await;
        let server =
            serve(vec![html(PLAYLIST_UPLOADS), Reply::json(200, BROWSE_UPLOADS_1), Reply::json(200, BROWSE_UPLOADS_2)]);
        let http = crate::links::fetch::test_client();
        let url = "https://www.youtube.com/playlist?list=UUeOuY27OfgZ5n3F581yV8uQ";
        let info = probe(&http, url, false, &CancelToken::new()).await;
        endpoints::install(None);
        let info = info.unwrap();
        assert!(info.is_playlist);
        assert_eq!(info.title.as_deref(), Some("Uploads from Rammor"));
        assert_eq!(info.extractor.as_deref(), Some("YoutubeTab"));
        assert_eq!(info.entries.len(), MAX_ENTRIES);
        assert!(info.entries.iter().enumerate().all(|(i, e)| e.index as usize == i));
        let requests = server.requests();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[1].target, "/youtubei/v1/browse?prettyPrint=false");
        let body: Value = serde_json::from_slice(&requests[1].body).unwrap();
        assert_eq!(
            Some(body["continuation"].as_str().unwrap()),
            playlist_page(WatchData::parse(PLAYLIST_UPLOADS).initial.as_ref().unwrap()).continuation.as_deref()
        );
    }

    #[tokio::test]
    async fn a_bot_check_stops_further_requests() {
        let _guard = endpoints::TEST_LOCK.lock().await;
        let wall = r#"<script>var ytInitialPlayerResponse = {"playabilityStatus":{"status":"LOGIN_REQUIRED","reason":"Sign in to confirm you’re not a bot"}};</script>"#;
        let server = serve(vec![html(wall)]);
        let http = crate::links::fetch::test_client();
        let first = probe(&http, "https://www.youtube.com/watch?v=dQw4w9WgXcQ", false, &CancelToken::new()).await;
        let second = probe(&http, "https://www.youtube.com/watch?v=dQw4w9WgXcQ", false, &CancelToken::new()).await;
        endpoints::install(None);
        assert!(matches!(first, Err(MfError::RateLimited { .. })), "{first:?}");
        assert!(matches!(second, Err(MfError::RateLimited { .. })), "{second:?}");
        assert_eq!(server.requests().len(), 1, "no request while paused");
        LIMITER.success();
    }
}

/// Against the real site: two requests, a minute apart. Run it only where
/// YouTube may be contacted (not from a machine whose address must stay clean):
/// `MF_YOUTUBE_LIVE=1 cargo test -p mfcore --lib youtube_api::live -- --ignored --nocapture`
#[tokio::test]
#[ignore = "live YouTube; needs MF_YOUTUBE_LIVE=1"]
async fn live() {
    if std::env::var("MF_YOUTUBE_LIVE").as_deref() != Ok("1") {
        eprintln!("skipped: set MF_YOUTUBE_LIVE=1 to contact YouTube");
        return;
    }
    let http = crate::links::fetch::test_client();
    let cancel = CancelToken::new();
    let video = probe(&http, "https://www.youtube.com/watch?v=VrJUZI-U2lg", false, &cancel).await.unwrap();
    assert_eq!(video.chapters.len(), 6, "{:?}", video.chapters);
    assert!(video.duration_ms.is_some_and(|d| d > 1_000_000));
    crate::util::rt::sleep(Duration::from_secs(60)).await;
    let list = "https://www.youtube.com/playlist?list=PLSbDLF8wQ3oKcstd9ybCSv2lNm_8NTYkI";
    let album = probe(&http, list, false, &cancel).await.unwrap();
    assert!(album.entries.len() >= 13, "{}", album.entries.len());
    assert!(album.entries.iter().all(|e| e.duration_ms.is_some()));
}
