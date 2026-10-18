use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;

use super::*;
use crate::model::{
    CandidateSource, CandidateSourceKind, CandidateTier, Evidence, EvidenceSource, MelodyAlignment, Service,
    ServiceLink, Tier,
};

/// Unique directory under the system temp dir, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("mfcore-store-{}", crate::util::new_id()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn db(&self) -> PathBuf {
        self.0.join("library").join("musicfetch.sqlite3")
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn memory() -> Store {
    Store::open_in_memory().unwrap()
}

fn track(artist: &str, title: &str) -> Track {
    Track { title: title.into(), artist: artist.into(), ..Default::default() }
}

fn keyed(artist: &str, title: &str, key: &str) -> Track {
    Track { identity_key: key.into(), ..track(artist, title) }
}

fn segment(id: &str, item: u32, start_ms: u64, kind: SegmentKind, track: Option<Track>) -> Segment {
    Segment {
        id: id.into(),
        item_index: item,
        start_ms,
        end_ms: start_ms + 60_000,
        kind,
        confidence: if track.is_some() { 0.88 } else { 0.0 },
        tier: if track.is_some() { Tier::Likely } else { Tier::Unknown },
        track,
        evidence: vec![Evidence {
            source: EvidenceSource::Shazam,
            at_ms: start_ms,
            score: 0.88,
            detail: Some("offset 0:12".into()),
        }],
        alternates: vec![],
        source_url: None,
    }
}

fn candidate(artist: &str, title: &str, score: f32) -> Candidate {
    Candidate {
        track: track(artist, title),
        score,
        tier: CandidateTier::Possible,
        sources: vec![CandidateSource { kind: CandidateSourceKind::Melody, score, detail: None }],
        alignment: None,
    }
}

fn query() -> LibraryQuery {
    LibraryQuery { kinds: vec![], search: None, pinned_only: false, limit: 0, offset: 0 }
}

fn search(store: &Store, text: &str) -> Vec<String> {
    let q = LibraryQuery { search: Some(text.into()), ..query() };
    store.library(&q).unwrap().into_iter().map(|e| e.id).collect()
}

fn count(store: &Store, sql: &str) -> i64 {
    store.inner.lock().conn.query_row(sql, [], |row| row.get(0)).unwrap()
}

fn exec(store: &Store, sql: &str) {
    store.inner.lock().conn.execute_batch(sql).unwrap();
}

#[test]
fn store_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Store>();
}

#[test]
fn migration_from_empty_file() {
    let dir = TempDir::new();
    let store = Store::open(&dir.db()).unwrap();
    let guard = store.inner.lock();
    let conn = &guard.conn;
    assert_eq!(schema::user_version(conn).unwrap(), schema::CURRENT_VERSION);
    let tables: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(tables, ["cache", "candidates", "segments", "sessions"]);
    let mode: String = conn.pragma_query_value(None, "journal_mode", |row| row.get(0)).unwrap();
    assert_eq!(mode, "wal");
    let fk: i64 = conn.pragma_query_value(None, "foreign_keys", |row| row.get(0)).unwrap();
    assert_eq!(fk, 1);
    let sync: i64 = conn.pragma_query_value(None, "synchronous", |row| row.get(0)).unwrap();
    assert_eq!(sync, 1, "NORMAL");
}

#[test]
fn reopening_keeps_data_and_schema() {
    let dir = TempDir::new();
    {
        let store = Store::open(&dir.db()).unwrap();
        store.create_session("a", SessionKind::Link, "Mix", Some("https://example.com/mix"), 1).unwrap();
    }
    let store = Store::open(&dir.db()).unwrap();
    let entries = store.library(&query()).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].source_url.as_deref(), Some("https://example.com/mix"));
    assert_eq!(schema::user_version(&store.inner.lock().conn).unwrap(), schema::CURRENT_VERSION);
}

#[test]
fn newer_schema_is_rejected() {
    let dir = TempDir::new();
    std::fs::create_dir_all(dir.db().parent().unwrap()).unwrap();
    {
        let conn = Connection::open(dir.db()).unwrap();
        conn.pragma_update(None, "user_version", 99).unwrap();
    }
    match Store::open(&dir.db()) {
        Err(MfError::Storage { message }) => {
            assert!(message.contains("newer version"), "{message}")
        }
        Err(other) => panic!("unexpected error {other:?}"),
        Ok(_) => panic!("opened a newer schema"),
    }
}

#[test]
fn damaged_file_is_moved_aside() {
    let dir = TempDir::new();
    std::fs::create_dir_all(dir.db().parent().unwrap()).unwrap();
    std::fs::write(dir.db(), b"not an sqlite database ".repeat(200)).unwrap();
    let store = Store::open(&dir.db()).unwrap();
    assert!(store.library(&query()).unwrap().is_empty());
    let moved = std::fs::read_dir(dir.db().parent().unwrap())
        .unwrap()
        .filter_map(|e| e.ok())
        .any(|e| e.file_name().to_string_lossy().starts_with("musicfetch.sqlite3.corrupt-"));
    assert!(moved);
    assert!(dir.path().exists());
}

#[test]
fn create_update_and_detail_round_trip() {
    let store = memory();
    store.create_session("s1", SessionKind::Link, "Loading…", Some("https://youtu.be/x?si=abc"), 1_000).unwrap();

    let mut answer = track("Daft Punk", "One More Time");
    answer.artwork_url = Some("https://img/1.jpg".into());
    answer.ids.insert("deezer".into(), "3135556".into());
    answer.links =
        vec![ServiceLink { service: Service::Deezer, url: "https://deezer.com/track/1".into(), is_search: false }];
    let source = SourceInfo {
        url: Some("https://www.youtube.com/watch?v=x".into()),
        title: Some("Live set".into()),
        uploader: Some("Boiler Room".into()),
        duration_ms: Some(3_600_000),
        service: Some(Service::YouTube),
        site: Some("YouTube".into()),
        item_count: 1,
        hints: vec![track("Hint", "Song")],
        ..Default::default()
    };
    store
        .update_session(
            "s1",
            SessionUpdate {
                status: Some(SessionStatus::Completed),
                title: Some("Live set".into()),
                subtitle: Some("Boiler Room".into()),
                track: Some(answer.clone()),
                duration_ms: Some(3_600_000),
                source: Some(source.clone()),
                transcript: Some("one more time".into()),
                ..Default::default()
            },
        )
        .unwrap();
    let segments = vec![
        segment("b", 0, 120_000, SegmentKind::Unidentified, None),
        segment("a", 0, 0, SegmentKind::Track, Some(answer.clone())),
    ];
    for s in &segments {
        store.upsert_segment("s1", s).unwrap();
    }
    let mut hum = candidate("A", "One", 0.7);
    hum.alignment = Some(MelodyAlignment { frame_ms: 50, query: vec![1.0, f32::NAN], reference: vec![0.5, 2.0] });
    store.set_candidates("s1", &[hum, candidate("B", "Two", 0.2)]).unwrap();

    let detail = store.session_detail("s1").unwrap().unwrap();
    assert_eq!(detail.entry.id, "s1");
    assert_eq!(detail.entry.kind, SessionKind::Link);
    assert_eq!(detail.entry.status, SessionStatus::Completed);
    assert_eq!(detail.entry.created_at_ms, 1_000);
    assert_eq!(detail.entry.title, "Live set");
    assert_eq!(detail.entry.subtitle.as_deref(), Some("Boiler Room"));
    assert_eq!(detail.entry.track.as_ref(), Some(&answer));
    // Artwork falls back to the track's.
    assert_eq!(detail.entry.artwork_url.as_deref(), Some("https://img/1.jpg"));
    assert_eq!(detail.entry.track_count, 1);
    assert_eq!(detail.entry.duration_ms, Some(3_600_000));
    // The canonical URL replaces the input URL.
    assert_eq!(detail.entry.source_url.as_deref(), Some("https://www.youtube.com/watch?v=x"));
    assert!(!detail.entry.pinned);
    assert_eq!(detail.source.as_ref(), Some(&source));
    assert_eq!(detail.segments, vec![segments[1].clone(), segments[0].clone()]);
    assert_eq!(detail.candidates.len(), 2);
    assert_eq!(detail.candidates[0].track.title, "One");
    assert!(detail.candidates[0].alignment.as_ref().unwrap().query[1].is_nan());
    assert_eq!(detail.candidates[1].track.title, "Two");
    assert_eq!(detail.transcript.as_deref(), Some("one more time"));

    assert!(store.session_detail("missing").unwrap().is_none());
}

#[test]
fn update_is_partial() {
    let store = memory();
    store.create_session("s1", SessionKind::Listen, "Listening", None, 5).unwrap();
    store
        .update_session(
            "s1",
            SessionUpdate { subtitle: Some("Mic".into()), error: Some("x".into()), ..Default::default() },
        )
        .unwrap();
    store.update_session("s1", SessionUpdate { status: Some(SessionStatus::NoMatch), ..Default::default() }).unwrap();
    store.update_session("s1", SessionUpdate { kind: Some(SessionKind::Hum), ..Default::default() }).unwrap();
    let entry = store.session_detail("s1").unwrap().unwrap().entry;
    assert_eq!(entry.kind, SessionKind::Hum);
    assert_eq!(entry.status, SessionStatus::NoMatch);
    assert_eq!(entry.title, "Listening");
    assert_eq!(entry.subtitle.as_deref(), Some("Mic"));
    assert_eq!(entry.created_at_ms, 5);
}

#[test]
fn unknown_and_duplicate_sessions_are_rejected() {
    let store = memory();
    let not_found =
        |r: Result<()>| matches!(r, Err(MfError::InvalidInput { message }) if message == "Session not found.");
    assert!(not_found(store.update_session("nope", SessionUpdate::default())));
    assert!(not_found(store.upsert_segment("nope", &segment("a", 0, 0, SegmentKind::Speech, None))));
    assert!(not_found(store.set_candidates("nope", &[])));
    assert!(not_found(store.set_pinned("nope", true)));
    assert!(store.remove_segment("nope", "a").is_ok());
    assert!(matches!(store.export("nope", ExportFormat::Text), Err(MfError::InvalidInput { .. })));

    store.create_session("s1", SessionKind::Hum, "Hum", None, 0).unwrap();
    assert!(matches!(
        store.create_session("s1", SessionKind::Hum, "Again", None, 0),
        Err(MfError::InvalidInput { .. })
    ));
    assert_eq!(store.session_detail("s1").unwrap().unwrap().entry.title, "Hum");
}

#[test]
fn track_count_counts_distinct_identified_tracks() {
    let store = memory();
    store.create_session("s", SessionKind::Link, "Mix", None, 0).unwrap();
    let entry = |store: &Store| store.session_detail("s").unwrap().unwrap().entry;

    store.upsert_segment("s", &segment("1", 0, 0, SegmentKind::Track, Some(keyed("A", "One", "isrc::1")))).unwrap();
    store
        .upsert_segment("s", &segment("2", 0, 60_000, SegmentKind::Track, Some(keyed("A", "One (Edit)", "isrc::1"))))
        .unwrap();
    store.upsert_segment("s", &segment("3", 0, 120_000, SegmentKind::Track, Some(track("B", "Two")))).unwrap();
    store.upsert_segment("s", &segment("4", 0, 180_000, SegmentKind::Track, Some(track("b", "TWO")))).unwrap();
    store.upsert_segment("s", &segment("5", 0, 240_000, SegmentKind::Unidentified, None)).unwrap();
    store.upsert_segment("s", &segment("6", 0, 300_000, SegmentKind::Speech, None)).unwrap();
    assert_eq!(entry(&store).track_count, 2);

    store.upsert_segment("s", &segment("7", 1, 0, SegmentKind::Track, Some(keyed("C", "Three", "isrc::3")))).unwrap();
    assert_eq!(entry(&store).track_count, 3);

    store.remove_segment("s", "7").unwrap();
    assert_eq!(entry(&store).track_count, 2);
    store.remove_segment("s", "missing").unwrap();

    // Re-classifying a segment updates the count.
    store.upsert_segment("s", &segment("3", 0, 120_000, SegmentKind::Unidentified, None)).unwrap();
    store.remove_segment("s", "4").unwrap();
    assert_eq!(entry(&store).track_count, 1);
}

#[test]
fn segments_are_upserted_by_id_and_sorted() {
    let store = memory();
    store.create_session("s", SessionKind::Link, "Playlist", None, 0).unwrap();
    store.upsert_segment("s", &segment("c", 1, 0, SegmentKind::Track, Some(track("C", "c")))).unwrap();
    store.upsert_segment("s", &segment("b", 0, 90_000, SegmentKind::Track, Some(track("B", "b")))).unwrap();
    store.upsert_segment("s", &segment("a", 0, 30_000, SegmentKind::Track, Some(track("A", "a")))).unwrap();
    let mut moved = segment("a", 0, 120_000, SegmentKind::Track, Some(track("A2", "a2")));
    moved.confidence = 0.95;
    moved.tier = Tier::Confirmed;
    store.upsert_segment("s", &moved).unwrap();

    let detail = store.session_detail("s").unwrap().unwrap();
    let ids: Vec<&str> = detail.segments.iter().map(|s| s.id.as_str()).collect();
    assert_eq!(ids, ["b", "a", "c"]);
    assert_eq!(detail.segments[1], moved);
    assert_eq!(store.segment("s", "a").unwrap(), Some(moved));
    assert_eq!(store.segment("s", "zzz").unwrap(), None);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM segments"), 3);
}

#[test]
fn non_finite_scores_round_trip_as_zero() {
    let store = memory();
    store.create_session("s", SessionKind::File, "File", None, 0).unwrap();
    let mut s = segment("a", 0, 0, SegmentKind::Track, Some(track("A", "a")));
    s.confidence = f32::NAN;
    store.upsert_segment("s", &s).unwrap();
    let stored = store.segment("s", "a").unwrap().unwrap();
    assert_eq!(stored.confidence, 0.0);
}

#[test]
fn candidates_are_replaced() {
    let store = memory();
    store.create_session("s", SessionKind::Hum, "Hum", None, 0).unwrap();
    store.set_candidates("s", &[candidate("A", "One", 0.9), candidate("B", "Two", 0.5)]).unwrap();
    store.set_candidates("s", &[candidate("C", "Three", 0.6)]).unwrap();
    let detail = store.session_detail("s").unwrap().unwrap();
    assert_eq!(detail.candidates, vec![candidate("C", "Three", 0.6)]);
    store.set_candidates("s", &[]).unwrap();
    assert!(store.session_detail("s").unwrap().unwrap().candidates.is_empty());
}

#[test]
fn library_is_newest_first_and_paginated() {
    let store = memory();
    for i in 0..25 {
        store.create_session(&format!("s{i:02}"), SessionKind::Listen, &format!("Session {i}"), None, i).unwrap();
    }
    // Same timestamp: the later insert comes first.
    store.create_session("tie", SessionKind::Listen, "Tie", None, 24).unwrap();

    let page = |limit, offset| -> Vec<String> {
        store.library(&LibraryQuery { limit, offset, ..query() }).unwrap().into_iter().map(|e| e.id).collect()
    };
    assert_eq!(page(3, 0), ["tie", "s24", "s23"]);
    assert_eq!(page(3, 3), ["s22", "s21", "s20"]);
    assert_eq!(page(10, 20), ["s05", "s04", "s03", "s02", "s01", "s00"]);
    assert!(page(10, 100).is_empty());
    assert_eq!(page(0, 0).len(), 26);
}

#[test]
fn library_filters_kinds_and_pinned() {
    let store = memory();
    store.create_session("listen", SessionKind::Listen, "a", None, 1).unwrap();
    store.create_session("hum", SessionKind::Hum, "b", None, 2).unwrap();
    store.create_session("link", SessionKind::Link, "c", None, 3).unwrap();
    store.create_session("file", SessionKind::File, "d", None, 4).unwrap();
    store.set_pinned("hum", true).unwrap();
    store.set_pinned("file", true).unwrap();
    store.set_pinned("file", false).unwrap();

    let ids = |q: LibraryQuery| -> Vec<String> { store.library(&q).unwrap().into_iter().map(|e| e.id).collect() };
    assert_eq!(ids(LibraryQuery { kinds: vec![SessionKind::Link, SessionKind::File], ..query() }), ["file", "link"]);
    assert_eq!(ids(LibraryQuery { kinds: vec![SessionKind::Hum, SessionKind::Hum], ..query() }), ["hum"]);
    assert_eq!(ids(LibraryQuery { pinned_only: true, ..query() }), ["hum"]);
    assert_eq!(
        ids(LibraryQuery { kinds: vec![SessionKind::Listen], pinned_only: true, ..query() }),
        Vec::<String>::new()
    );
    assert!(store.library(&query()).unwrap().iter().any(|e| e.id == "hum" && e.pinned));
}

#[test]
fn library_search_covers_session_tracks_and_top_candidate() {
    let store = memory();
    store.create_session("mix", SessionKind::Link, "Boiler Room Berlin", None, 1).unwrap();
    store.upsert_segment("mix", &segment("1", 0, 0, SegmentKind::Track, Some(track("Beyoncé", "Halo")))).unwrap();
    store
        .upsert_segment("mix", &segment("2", 0, 60_000, SegmentKind::Track, Some(track("Daft Punk", "Aerodynamic"))))
        .unwrap();

    store.create_session("listen", SessionKind::Listen, "Heard at café", None, 2).unwrap();
    store
        .update_session(
            "listen",
            SessionUpdate {
                track: Some(track("Mötley Crüe", "Kickstart My Heart")),
                subtitle: Some("Kitchen radio".into()),
                ..Default::default()
            },
        )
        .unwrap();

    store.create_session("hum", SessionKind::Hum, "Hummed melody", None, 3).unwrap();
    store
        .set_candidates("hum", &[candidate("Queen", "Bohemian Rhapsody", 0.8), candidate("ABBA", "Waterloo", 0.1)])
        .unwrap();

    store.create_session("link", SessionKind::Link, "Playlist", None, 4).unwrap();
    store
        .update_session(
            "link",
            SessionUpdate {
                source: Some(SourceInfo { uploader: Some("Mixmag".into()), ..Default::default() }),
                ..Default::default()
            },
        )
        .unwrap();

    assert_eq!(search(&store, "boiler"), ["mix"]);
    assert_eq!(search(&store, "BEYONCE"), ["mix"]);
    assert_eq!(search(&store, "halo beyoncé"), ["mix"]);
    assert_eq!(search(&store, "punk aero"), ["mix"]);
    assert_eq!(search(&store, "motley"), ["listen"]);
    assert_eq!(search(&store, "Cafe"), ["listen"]);
    assert_eq!(search(&store, "kitchen"), ["listen"]);
    assert_eq!(search(&store, "bohemian"), ["hum"]);
    assert!(search(&store, "waterloo").is_empty(), "only the top candidate is searchable");
    assert_eq!(search(&store, "mixmag"), ["link"]);
    assert!(search(&store, "halo kickstart").is_empty(), "every word must match the same session");
    assert!(search(&store, "nothing like this").is_empty());
    assert_eq!(search(&store, " ").len(), 4, "a blank search lists everything");

    // Derived search text follows segment removals and title changes.
    store.remove_segment("mix", "1").unwrap();
    assert!(search(&store, "halo").is_empty());
    store.update_session("hum", SessionUpdate { title: Some("Shower song".into()), ..Default::default() }).unwrap();
    assert_eq!(search(&store, "shower"), ["hum"]);
    assert_eq!(search(&store, "bohemian"), ["hum"]);
    assert!(search(&store, "hummed").is_empty());

    // Search combines with filters and pagination.
    let q = LibraryQuery { search: Some("o".into()), kinds: vec![SessionKind::Link], limit: 1, offset: 0, ..query() };
    assert_eq!(store.library(&q).unwrap().len(), 1);
}

#[test]
fn delete_cascades_in_chunks() {
    let store = memory();
    for i in 0..1_201 {
        let id = format!("s{i}");
        store.create_session(&id, SessionKind::Link, "x", None, i).unwrap();
        if i % 100 == 0 && i < 1_200 {
            store.upsert_segment(&id, &segment("a", 0, 0, SegmentKind::Track, Some(track("A", "a")))).unwrap();
            store.set_candidates(&id, &[candidate("B", "b", 0.5)]).unwrap();
        }
    }
    let mut ids: Vec<String> = (0..1_200).map(|i| format!("s{i}")).collect();
    ids.push("unknown".into());
    store.delete_sessions(&ids).unwrap();
    assert_eq!(count(&store, "SELECT COUNT(*) FROM sessions"), 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM segments"), 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM candidates"), 0);
    store.delete_sessions(&[]).unwrap();

    store.upsert_segment("s1200", &segment("a", 0, 0, SegmentKind::Track, Some(track("A", "a")))).unwrap();
    store.delete_sessions(&["s1200".to_string()]).unwrap();
    assert_eq!(count(&store, "SELECT COUNT(*) FROM segments"), 0);
}

#[test]
fn recover_interrupted_marks_running_sessions() {
    let store = memory();
    store.create_session("a", SessionKind::Listen, "a", None, 1).unwrap();
    store.create_session("b", SessionKind::Link, "b", None, 2).unwrap();
    store.create_session("c", SessionKind::File, "c", None, 3).unwrap();
    store.update_session("c", SessionUpdate { status: Some(SessionStatus::Completed), ..Default::default() }).unwrap();

    assert_eq!(store.recover_interrupted().unwrap(), 2);
    for id in ["a", "b"] {
        let detail = store.session_detail(id).unwrap().unwrap();
        assert_eq!(detail.entry.status, SessionStatus::Failed);
    }
    let error: Option<String> =
        store.inner.lock().conn.query_row("SELECT error FROM sessions WHERE id = 'a'", [], |row| row.get(0)).unwrap();
    assert_eq!(error.as_deref(), Some(INTERRUPTED));
    assert_eq!(store.session_detail("c").unwrap().unwrap().entry.status, SessionStatus::Completed);
    assert_eq!(store.recover_interrupted().unwrap(), 0);
}

#[test]
fn cache_expires_lazily_and_by_purge() {
    let store = memory();
    let now = 1_000_000;
    store.cache_put_at("shazam", "k1", "v1", 1_000, now).unwrap();
    store.cache_put_at("deezer", "k1", "other", 1_000, now).unwrap();
    assert_eq!(store.cache_get_at("shazam", "k1", now + 999).unwrap().as_deref(), Some("v1"));
    assert_eq!(store.cache_get_at("deezer", "k1", now).unwrap().as_deref(), Some("other"));
    assert_eq!(store.cache_get_at("shazam", "missing", now).unwrap(), None);

    // Overwrite extends the expiry.
    store.cache_put_at("shazam", "k1", "v2", 5_000, now).unwrap();
    assert_eq!(store.cache_get_at("shazam", "k1", now + 2_000).unwrap().as_deref(), Some("v2"));

    // Lazy expiry removes the row on read.
    assert_eq!(store.cache_get_at("shazam", "k1", now + 5_000).unwrap(), None);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM cache WHERE namespace = 'shazam'"), 0);

    // Non-positive TTL removes.
    store.cache_put_at("deezer", "k1", "gone", 0, now).unwrap();
    assert_eq!(store.cache_get_at("deezer", "k1", now).unwrap(), None);

    // Purge removes expired rows without reading them.
    for i in 0..10 {
        store.cache_put_at("p", &format!("k{i}"), "v", if i < 7 { 10 } else { 1_000_000 }, now).unwrap();
    }
    assert_eq!(store.purge_expired_cache_at(now + 10).unwrap(), 7);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM cache"), 3);
}

#[test]
fn cache_put_purges_periodically() {
    let store = memory();
    let start = crate::util::now_ms();
    store.cache_put_at("p", "old", "v", 1, start).unwrap();
    store.cache_put_at("p", "fresh", "v", i64::MAX, start + 1).unwrap();
    assert_eq!(count(&store, "SELECT COUNT(*) FROM cache"), 2);
    store.cache_put_at("p", "later", "v", 1_000, start + CACHE_PURGE_INTERVAL_MS + 1).unwrap();
    assert_eq!(count(&store, "SELECT COUNT(*) FROM cache"), 2, "the expired row is purged");
    assert_eq!(store.cache_get("p", "fresh").unwrap().as_deref(), Some("v"));
}

#[test]
fn corrupted_rows_are_skipped_without_failing() {
    let store = memory();
    store.create_session("s", SessionKind::Link, "Mix", None, 2).unwrap();
    store.upsert_segment("s", &segment("good", 0, 0, SegmentKind::Track, Some(track("A", "a")))).unwrap();
    store.upsert_segment("s", &segment("bad", 0, 60_000, SegmentKind::Track, Some(track("B", "b")))).unwrap();
    store.set_candidates("s", &[candidate("A", "a", 0.5), candidate("B", "b", 0.4)]).unwrap();
    store.update_session("s", SessionUpdate { track: Some(track("T", "t")), ..Default::default() }).unwrap();
    store.update_session("s", SessionUpdate { source: Some(SourceInfo::default()), ..Default::default() }).unwrap();
    store.create_session("weird", SessionKind::Link, "Weird", None, 1).unwrap();
    exec(
        &store,
        "UPDATE segments SET json = '{\"id\": 12' WHERE id = 'bad';
         UPDATE candidates SET json = 'null' WHERE rank = 0;
         UPDATE sessions SET track_json = 'not json', source_json = '[]' WHERE id = 's';
         UPDATE sessions SET kind = 'hologram' WHERE id = 'weird';",
    );

    let detail = store.session_detail("s").unwrap().unwrap();
    assert_eq!(detail.entry.track, None);
    assert_eq!(detail.source, None);
    assert_eq!(detail.segments.len(), 1);
    assert_eq!(detail.segments[0].id, "good");
    assert_eq!(detail.candidates.len(), 1);
    assert_eq!(detail.candidates[0].track.title, "b");

    let ids: Vec<String> = store.library(&query()).unwrap().into_iter().map(|e| e.id).collect();
    assert_eq!(ids, ["s"]);
    assert!(store.session_detail("weird").unwrap().is_none());

    // Writes still work and refresh derived columns around the bad rows.
    store.update_session("s", SessionUpdate { title: Some("Renamed".into()), ..Default::default() }).unwrap();
    assert_eq!(search(&store, "renamed"), ["s"]);
    for format in [ExportFormat::Text, ExportFormat::Csv, ExportFormat::Json, ExportFormat::Cue] {
        store.export("s", format).unwrap();
    }
}

#[test]
fn concurrent_writers_on_distinct_sessions() {
    let store = Arc::new(memory());
    const THREADS: usize = 8;
    const SEGMENTS: usize = 60;
    let writers: Vec<_> = (0..THREADS)
        .map(|t| {
            let store = Arc::clone(&store);
            thread::spawn(move || {
                let id = format!("session-{t}");
                store.create_session(&id, SessionKind::Link, &format!("Mix {t}"), None, t as i64).unwrap();
                for i in 0..SEGMENTS {
                    let key = format!("isrc::{t}-{}", i % 20);
                    let s =
                        segment(&format!("{i}"), 0, i as u64 * 1_000, SegmentKind::Track, Some(keyed("A", "T", &key)));
                    store.upsert_segment(&id, &s).unwrap();
                    if i % 10 == 0 {
                        store.cache_put("t", &format!("{t}-{i}"), "v", 60_000).unwrap();
                    }
                }
                store
                    .update_session(&id, SessionUpdate { status: Some(SessionStatus::Completed), ..Default::default() })
                    .unwrap();
            })
        })
        .collect();
    let reader = {
        let store = Arc::clone(&store);
        thread::spawn(move || {
            for _ in 0..50 {
                for entry in store.library(&query()).unwrap() {
                    assert!(entry.track_count <= 20);
                    store.session_detail(&entry.id).unwrap();
                }
            }
        })
    };
    for w in writers {
        w.join().unwrap();
    }
    reader.join().unwrap();

    let entries = store.library(&query()).unwrap();
    assert_eq!(entries.len(), THREADS);
    for entry in entries {
        assert_eq!(entry.status, SessionStatus::Completed);
        assert_eq!(entry.track_count, 20);
        assert_eq!(store.session_detail(&entry.id).unwrap().unwrap().segments.len(), SEGMENTS);
    }
    assert_eq!(count(&store, "SELECT COUNT(*) FROM cache"), (THREADS * SEGMENTS / 10) as i64);
}

#[test]
fn two_connections_share_one_file() {
    let dir = TempDir::new();
    let a = Arc::new(Store::open(&dir.db()).unwrap());
    let b = Arc::new(Store::open(&dir.db()).unwrap());
    let handles: Vec<_> = [Arc::clone(&a), Arc::clone(&b)]
        .into_iter()
        .enumerate()
        .map(|(n, store)| {
            thread::spawn(move || {
                for i in 0..40 {
                    let id = format!("{n}-{i}");
                    store.create_session(&id, SessionKind::File, "f", None, i).unwrap();
                    store.upsert_segment(&id, &segment("x", 0, 0, SegmentKind::Track, Some(track("A", "a")))).unwrap();
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(a.library(&query()).unwrap().len(), 80);
    assert_eq!(b.library(&query()).unwrap().len(), 80);
}

#[test]
fn export_reads_from_the_store() {
    let store = memory();
    store.create_session("s", SessionKind::Link, "Mix", None, 0).unwrap();
    store.upsert_segment("s", &segment("b", 0, 3_600_000, SegmentKind::Unidentified, None)).unwrap();
    store.upsert_segment("s", &segment("a", 0, 75_000, SegmentKind::Track, Some(track("A", "One")))).unwrap();
    store.upsert_segment("s", &segment("c", 0, 3_700_000, SegmentKind::Silence, None)).unwrap();
    assert_eq!(store.export("s", ExportFormat::Text).unwrap(), "[01:15] A - One\n[1:00:00] ID - ID\n");
    let json = store.export("s", ExportFormat::Json).unwrap();
    let back: SessionDetail = serde_json::from_str(&json).unwrap();
    assert_eq!(back, store.session_detail("s").unwrap().unwrap());
}

#[test]
fn database_size_is_reported() {
    let dir = TempDir::new();
    let store = Store::open(&dir.db()).unwrap();
    assert!(store.database_bytes() > 0);
    assert!(memory().database_bytes() > 0);
}

#[test]
fn pinning_many_sessions_at_once() {
    let store = memory();
    for id in ["a", "b", "c"] {
        store.create_session(id, SessionKind::Listen, id, None, 1).unwrap();
    }
    let ids: Vec<String> = ["a", "c", "missing"].iter().map(|s| s.to_string()).collect();
    assert_eq!(store.set_pinned_many(&ids, true).unwrap(), 2, "unknown ids are ignored");
    let pinned = |store: &Store| {
        let mut ids: Vec<String> =
            store.library(&LibraryQuery { pinned_only: true, ..query() }).unwrap().into_iter().map(|e| e.id).collect();
        ids.sort();
        ids
    };
    assert_eq!(pinned(&store), vec!["a", "c"]);
    assert_eq!(store.set_pinned_many(&[], false).unwrap(), 0);
    // More ids than one statement binds.
    let many: Vec<String> = (0..1203).map(|i| format!("x{i}")).chain(["a".to_string()]).collect();
    assert_eq!(store.set_pinned_many(&many, false).unwrap(), 1);
    assert_eq!(pinned(&store), vec!["c"]);
}

#[test]
fn recovery_reports_the_interrupted_sessions() {
    let store = memory();
    store.create_session("r2", SessionKind::Hum, "", None, 2).unwrap();
    store.create_session("r1", SessionKind::Listen, "", None, 1).unwrap();
    store.create_session("done", SessionKind::Listen, "", None, 3).unwrap();
    store
        .update_session("done", SessionUpdate { status: Some(SessionStatus::Completed), ..Default::default() })
        .unwrap();
    assert_eq!(store.recover_interrupted_sessions().unwrap(), vec!["r1", "r2"]);
    assert!(store.recover_interrupted_sessions().unwrap().is_empty());
    let detail = store.session_detail("r1").unwrap().unwrap();
    assert_eq!(detail.entry.status, SessionStatus::Failed);
}

#[test]
fn kv_values_persist_and_survive_purges() {
    let dir = TempDir::new();
    let store = Store::open(&dir.db()).unwrap();
    assert_eq!(store.kv_get("shazam.breaker").unwrap(), None);
    store.kv_put("shazam.breaker", r#"{"until":1}"#).unwrap();
    store.kv_put("shazam.breaker", r#"{"until":2}"#).unwrap();
    store.cache_put_at("shazam", "sig", "x", 10, 0).unwrap();
    assert_eq!(store.purge_expired_cache_at(i64::MAX - 1).unwrap(), 1, "only the expiring entry goes");
    drop(store);
    let store = Store::open(&dir.db()).unwrap();
    assert_eq!(store.kv_get("shazam.breaker").unwrap().as_deref(), Some(r#"{"until":2}"#));
    assert!(store.kv_remove("shazam.breaker").unwrap());
    assert!(!store.kv_remove("shazam.breaker").unwrap());
    assert_eq!(store.kv_get("shazam.breaker").unwrap(), None);
}

#[test]
fn serialized_snapshot_round_trip() {
    let store = memory();
    store.create_session("s", SessionKind::Listen, "Snapshot", None, 1).unwrap();
    store.kv_put("k", "v").unwrap();
    let bytes = store.serialize().unwrap();
    assert!(bytes.starts_with(b"SQLite format 3\0"));
    let restored = Store::open_serialized(&bytes).unwrap();
    assert_eq!(restored.library(&query()).unwrap()[0].title, "Snapshot");
    assert_eq!(restored.kv_get("k").unwrap().as_deref(), Some("v"));
    restored.create_session("t", SessionKind::Listen, "", None, 2).unwrap();
    assert_eq!(restored.library(&query()).unwrap().len(), 2);
    assert!(Store::open_serialized(b"not a database").is_err());
}

#[test]
fn vfs_databases_use_a_rollback_journal() {
    let dir = TempDir::new();
    let path = dir.path().join("vfs.sqlite3");
    // Natively the platform VFS stands in for the browser's OPFS pool.
    let vfs = if cfg!(windows) { "win32" } else { "unix" };
    let store = Store::open_vfs(vfs, path.to_str().unwrap()).unwrap();
    let mode: String = store.inner.lock().conn.query_row("PRAGMA journal_mode", [], |r| r.get(0)).unwrap();
    assert_eq!(mode, "truncate");
    store.create_session("v", SessionKind::Listen, "On a VFS", None, 1).unwrap();
    drop(store);
    let store = Store::open_vfs(vfs, path.to_str().unwrap()).unwrap();
    assert_eq!(store.library(&query()).unwrap()[0].title, "On a VFS");
    drop(store);
    std::fs::write(&path, b"garbage, not a database ".repeat(400)).unwrap();
    let err = Store::open_vfs(vfs, path.to_str().unwrap()).err().expect("damaged");
    assert!(Store::is_damaged_error(&err), "{err:?}");
    assert!(Store::open_vfs("no-such-vfs", "x.sqlite3").is_err());
}
