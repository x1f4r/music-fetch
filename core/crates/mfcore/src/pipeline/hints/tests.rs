use super::*;
use crate::media::{Chapter, Comment, MediaInfo};

fn entry(label: &str) -> (String, String, bool) {
    parse_entry(label).unwrap_or_else(|| panic!("no entry in {label:?}"))
}

fn titles(entries: &[TrackHint]) -> Vec<(u64, &str, &str)> {
    entries.iter().map(|e| (e.start_ms / 1000, e.artist.as_str(), e.title.as_str())).collect()
}

#[test]
fn entry_formats() {
    assert_eq!(entry("Artist - Title"), ("Artist".into(), "Title".into(), false));
    assert_eq!(entry("Artist – Title (Remix)"), ("Artist".into(), "Title (Remix)".into(), false));
    assert_eq!(entry("Artist — Title"), ("Artist".into(), "Title".into(), false));
    assert_eq!(entry("1. Artist - Title [Label]"), ("Artist".into(), "Title".into(), false));
    assert_eq!(entry("01 | Artist - Title"), ("Artist".into(), "Title".into(), false));
    assert_eq!(entry("#12 Artist - Title"), ("Artist".into(), "Title".into(), false));
    assert_eq!(entry("07) Artist - Title [Extended Mix]"), ("Artist".into(), "Title [Extended Mix]".into(), false));
    assert_eq!(entry("▶ 🎵 Artist - Title •"), ("Artist".into(), "Title".into(), false));
    assert_eq!(entry("ID - ID"), ("ID".into(), "ID".into(), true));
    assert_eq!(entry("ID"), (String::new(), "ID".into(), true));
    assert_eq!(entry("Artist - ID"), ("Artist".into(), "ID".into(), false));
    assert_eq!(entry("Artist - Title w/ Other - Song"), ("Artist".into(), "Title".into(), false));
    assert_eq!(entry("A vs. B - Title"), ("A vs. B".into(), "Title".into(), false));
    assert_eq!(entry("A x B - Title (C Remix)"), ("A x B".into(), "Title (C Remix)".into(), false));
    assert_eq!(entry("Artist - Title - Extended Mix"), ("Artist".into(), "Title (Extended Mix)".into(), false));
    assert_eq!(entry("Jay-Z - Song"), ("Jay-Z".into(), "Song".into(), false));
    assert_eq!(entry("Only A Title"), (String::new(), "Only A Title".into(), false));
    assert_eq!(
        entry("Artist - Title (feat. Singer) [Armada]"),
        ("Artist".into(), "Title (feat. Singer)".into(), false)
    );
    assert!(parse_entry(" - | • ").is_none());
    assert_eq!(entry("Track 01 - This World"), (String::new(), "This World".into(), false));
    // "Title (Mix) - Artist" lists are turned around.
    assert_eq!(entry("Nerves (Viers Remix) - Rohar"), ("Rohar".into(), "Nerves (Viers Remix)".into(), false));
    assert_eq!(
        entry("RPM (Original Mix) - Charlotte De Witte"),
        ("Charlotte De Witte".into(), "RPM (Original Mix)".into(), false)
    );
}

#[test]
fn performer_from_media_titles() {
    assert_eq!(performer("Kiasmos live for Cercle at Citadelle de Sisteron, France").as_deref(), Some("Kiasmos"));
    assert_eq!(
        performer("ATEEZ(에이티즈) \"THE WORLD EP.2 : OUTLAW\" || FULL ALBUM - Tracklist").as_deref(),
        Some("ATEEZ")
    );
    assert_eq!(performer("Deep House Mix 2025 - Best Mega Hits"), None);
    assert_eq!(performer("Solomun | Boiler Room: Tulum").as_deref(), Some("Solomun"));
    let info = MediaInfo {
        title: Some("Kiasmos live for Cercle at Citadelle de Sisteron, France".into()),
        duration_ms: Some(1_200_000),
        description: Some("0:00 Grown\n4:16 Looped\n10:25 Laced\n14:12 ID".into()),
        ..MediaInfo::default()
    };
    let list = collect(&info, &[], None).tracklist.unwrap();
    assert_eq!(
        list.entries.iter().map(|e| e.artist.as_str()).collect::<Vec<_>>(),
        vec!["Kiasmos", "Kiasmos", "Kiasmos", ""]
    );
}

#[test]
fn timestamps_anywhere_in_the_line() {
    assert_eq!(split_timestamp("00:00 Artist - Title").map(|x| x.0), Some(0));
    assert_eq!(split_timestamp("[1:02:03] Artist - Title").map(|x| x.0), Some(3_723_000));
    assert_eq!(split_timestamp("1. Artist - Title [Label] 03:45").map(|x| x.0), Some(225_000));
    assert_eq!(split_timestamp("(12:30) Artist - Title").map(|x| x.0), Some(750_000));
    assert_eq!(split_timestamp("00:00 - 03:30 Artist – Title").map(|x| entry(&x.1)), Some(entry("Artist – Title")));
    assert_eq!(split_timestamp("75:30 nope"), None);
    assert_eq!(split_timestamp("12:345 nope"), None);
    assert_eq!(split_timestamp("no time"), None);
}

#[test]
fn description_tracklist() {
    let text = "Recorded live.\n\nTracklist:\n00:00 Kiasmos - Lit\n[05:12] Kiasmos – Blurred (Bonobo Remix)\n\
                10:40 Ólafur Arnalds - re:member\n1:02:03 ID - ID\nFollow us!";
    let list = parse_tracklist(text, Some(4_000_000)).unwrap();
    assert_eq!(
        titles(&list),
        vec![
            (0, "Kiasmos", "Lit"),
            (312, "Kiasmos", "Blurred (Bonobo Remix)"),
            (640, "Ólafur Arnalds", "re:member"),
            (3723, "ID", "ID"),
        ]
    );
    assert!(list[3].is_id);
    // Times past the end are dropped; too few left.
    assert!(parse_tracklist(text, Some(400_000)).is_none());
}

#[test]
fn numbered_list_with_lengths() {
    let text = "1. A - One 03:00\n2. B - Two 04:00\n3. C - Three 02:30\n4. D - Four 03:30";
    let list = parse_tracklist(text, Some(13 * 60_000)).unwrap();
    assert_eq!(titles(&list).iter().map(|t| t.0).collect::<Vec<_>>(), vec![0, 180, 420, 570]);
    // Without a duration the lengths cannot be told from start times.
    assert!(parse_tracklist(text, None).is_none());
}

#[test]
fn song_sections_are_not_a_tracklist() {
    let text = "0:00 Intro\n0:45 Verse 1\n1:30 Chorus\n2:15 Verse 2\n3:00 Bridge\n3:40 Outro";
    assert!(parse_tracklist(text, Some(240_000)).is_none());
    // An album of one artist: titles only, still songs.
    let album = "0:00 Opening\n3:40 Second Song\n7:10 Third Song\n10:00 Last One";
    assert_eq!(parse_tracklist(album, Some(800_000)).map(|l| l.len()), Some(4));
}

#[test]
fn non_increasing_lines_are_skipped() {
    let text = "00:00 A - One\n04:00 B - Two\n03:00 typo - here\n08:00 C - Three\n12:00 D - Four";
    let list = parse_tracklist(text, None).unwrap();
    assert_eq!(list.iter().map(|e| e.artist.as_str()).collect::<Vec<_>>(), vec!["A", "B", "C", "D"]);
}

fn info_with(description: &str, comments: Vec<Comment>, chapters: Vec<Chapter>) -> MediaInfo {
    MediaInfo {
        title: Some("Some DJ live at Some Festival".into()),
        duration_ms: Some(3_600_000),
        description: Some(description.into()),
        comments,
        chapters,
        ..MediaInfo::default()
    }
}

fn comment(text: &str, pinned: bool, uploader: bool) -> Comment {
    Comment { author: None, is_pinned: pinned, author_is_uploader: uploader, text: text.into(), like_count: 0 }
}

#[test]
fn best_source_wins() {
    let short = "00:00 A - One\n05:00 B - Two\n10:00 C - Three";
    let long = "00:00 A - One\n05:00 B - Two\n10:00 C - Three\n15:00 D - Four";
    let hints = collect(&info_with(short, vec![comment(long, false, false)], vec![]), &[], None);
    let list = hints.tracklist.unwrap();
    assert_eq!(list.source, HintSource::Comment);
    assert_eq!(list.entries.len(), 4);
    // Equal length: the more trusted source.
    let hints = collect(&info_with(short, vec![comment(short, true, false)], vec![]), &[], None);
    assert_eq!(hints.tracklist.unwrap().source, HintSource::Description);
    let hints =
        collect(&info_with("", vec![comment(short, false, false), comment(short, true, false)], vec![]), &[], None);
    assert_eq!(hints.tracklist.unwrap().source, HintSource::PinnedComment);
    let chapters = vec![
        Chapter { start_ms: 0, end_ms: None, title: "A - One".into() },
        Chapter { start_ms: 300_000, end_ms: None, title: "B - Two".into() },
        Chapter { start_ms: 600_000, end_ms: None, title: "C - Three".into() },
    ];
    let hints = collect(&info_with(short, vec![], chapters), &[], None);
    assert_eq!(hints.tracklist.unwrap().source, HintSource::Chapters);
    assert_eq!(hints.mix_words, MixWords::None);
}

#[test]
fn extractor_and_title_tracks() {
    let info = MediaInfo {
        title: Some("Daft Punk - One More Time (Official Video)".into()),
        uploader: Some("Daft Punk".into()),
        duration_ms: Some(320_000),
        ..MediaInfo::default()
    };
    let hints = collect(&info, &[], None);
    let t = hints.title_track.unwrap();
    assert_eq!((t.artist.as_str(), t.title.as_str()), ("Daft Punk", "One More Time"));
    assert!(hints.music_video);
    assert!(hints.extractor_track.is_none());

    let info = MediaInfo {
        title: Some("One More Time".into()),
        uploader: Some("Daft Punk - Topic".into()),
        track: Some("One More Time".into()),
        ..MediaInfo::default()
    };
    let hints = collect(&info, &[], None);
    assert_eq!(hints.extractor_track.unwrap().artist, "Daft Punk");
    assert_eq!(hints.title_track.unwrap().artist, "Daft Punk");
}

#[test]
fn media_titles() {
    let p = |t: &str, u: Option<&str>| parse_media_title(t, u);
    assert_eq!(
        p("Rick Astley - Never Gonna Give You Up (Official Video) (4K Remaster)", None),
        Some(("Rick Astley".into(), "Never Gonna Give You Up".into()))
    );
    assert_eq!(
        p("Michael Jackson - Thriller (Official Video - Shortened Version)", None),
        Some(("Michael Jackson".into(), "Thriller".into()))
    );
    assert_eq!(p("Queen – Bohemian Rhapsody [HD] | Lyrics", None), Some(("Queen".into(), "Bohemian Rhapsody".into())));
    assert_eq!(
        p("Adele - Someone Like You (Live at The BRIT Awards 2011)", None),
        Some(("Adele".into(), "Someone Like You (Live at The BRIT Awards 2011)".into()))
    );
    assert_eq!(p("Blinding Lights", Some("TheWeekndVEVO")), Some(("TheWeeknd".into(), "Blinding Lights".into())));
    assert_eq!(p("Blinding Lights", Some("The Weeknd - Topic")), Some(("The Weeknd".into(), "Blinding Lights".into())));
    assert_eq!(p("Blinding Lights", Some("Random Uploads")), None);
}

#[test]
fn mix_words_in_titles() {
    assert_eq!(mix_words("Kiasmos live for Cercle at Citadelle de Sisteron"), MixWords::Strong);
    assert_eq!(mix_words("Solomun | Boiler Room: Tulum"), MixWords::Strong);
    assert_eq!(mix_words("Deep House Mix 2024"), MixWords::Weak);
    assert_eq!(mix_words("Best Of 2023 Megamix"), MixWords::Strong);
    assert_eq!(mix_words("deadmau5 - Strobe (Club Edit)"), MixWords::None);
    assert_eq!(mix_words("Eric Prydz - Opus (Extended Mix)"), MixWords::None);
    assert_eq!(mix_words("Little Mix - Shout Out to My Ex"), MixWords::None);
    assert_eq!(mix_words("Adele - Set Fire to the Rain"), MixWords::None);
    assert_eq!(mix_words("Queen - Radio Ga Ga"), MixWords::None);
    assert_eq!(mix_words("Some Album (Full Album)"), MixWords::Strong);
}

#[test]
fn version_keys() {
    assert_eq!(version_key("Opus"), "");
    assert_eq!(version_key("Opus (Extended Mix)"), "");
    assert_eq!(version_key("Opus (Radio Edit)"), "");
    assert_eq!(version_key("Opus - Original Mix"), "");
    assert_eq!(version_key("Song (Remastered 2015)"), "");
    assert_eq!(version_key("Song (feat. Someone)"), "");
    assert_eq!(version_key("Opus (Four Tet Remix)"), "four tet remix");
    assert_eq!(version_key("Opus [Four Tet Extended Remix]"), "four tet remix");
    assert_eq!(version_key("Song - Live at Wembley"), "live at wembley");
    assert_eq!(version_key("Song (Sped Up)"), "sped up");
    assert_eq!(version_key("Song (VIP Mix)"), "vip");
    assert!(versions_compatible("live", "live at wembley stadium 1986"));
    assert!(!versions_compatible("tiesto remix", "afrojack remix"));
    assert!(!versions_compatible("", "tiesto remix"));
    assert!(versions_compatible("tiesto remix", "tiesto remix"));
}

#[test]
fn recordings_and_songs() {
    let t = |title: &str, artist: &str| Track { title: title.into(), artist: artist.into(), ..Track::default() };
    assert!(same_recording(&t("Opus (Extended Mix)", "Eric Prydz"), &t("Opus", "Eric Prydz")));
    assert!(!same_recording(&t("Opus (Four Tet Remix)", "Eric Prydz"), &t("Opus", "Eric Prydz")));
    assert!(same_song(&t("Opus (Four Tet Remix)", "Eric Prydz"), &t("Opus", "Eric Prydz")));
    assert!(same_song(&t("Strobe", "deadmau5"), &t("Strobe (Club Edit)", "Deadmau5")));
    assert!(!same_song(&t("Strobe", "deadmau5"), &t("Ghosts 'n' Stuff", "deadmau5")));
    let mut a = t("Same Title", "Artist");
    let mut b = t("Same Title", "Artist");
    a.isrc = Some("USRC11111111".into());
    b.isrc = Some("USRC22222222".into());
    assert!(!same_recording(&a, &b));
    assert_eq!(split_artists("A & B feat. C"), vec!["A", "B", "C"]);
    assert_eq!(split_artists("A vs. B x C"), vec!["A", "B", "C"]);
    assert!(artists_overlap("Armin van Buuren vs. Vini Vici", "Armin van Buuren"));
    assert!(!artists_overlap("Tiësto", "Armin van Buuren"));
}
