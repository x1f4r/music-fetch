//! SQLite persistence: sessions, segments, candidates, caches.

use std::path::Path;

use parking_lot::Mutex;

use crate::error::Result;
use crate::model::{
    Candidate, ExportFormat, LibraryEntry, LibraryQuery, Segment, SessionDetail, SessionKind, SessionStatus, SourceInfo,
    Track,
};

pub struct Store {
    _conn: Mutex<rusqlite::Connection>,
}

/// Partial update; `None` fields are left unchanged.
#[derive(Debug, Clone, Default)]
pub struct SessionUpdate {
    pub status: Option<SessionStatus>,
    pub kind: Option<SessionKind>,
    pub title: Option<String>,
    pub subtitle: Option<String>,
    pub artwork_url: Option<String>,
    pub track: Option<Track>,
    pub duration_ms: Option<u64>,
    pub source: Option<SourceInfo>,
    pub transcript: Option<String>,
    pub error: Option<String>,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let _ = path;
        todo!("store")
    }

    pub fn create_session(&self, id: &str, kind: SessionKind, title: &str, source_url: Option<&str>, created_at_ms: i64) -> Result<()> {
        let _ = (id, kind, title, source_url, created_at_ms);
        todo!("store")
    }

    pub fn update_session(&self, id: &str, update: SessionUpdate) -> Result<()> {
        let _ = (id, update);
        todo!("store")
    }

    pub fn upsert_segment(&self, session_id: &str, segment: &Segment) -> Result<()> {
        let _ = (session_id, segment);
        todo!("store")
    }

    pub fn remove_segment(&self, session_id: &str, segment_id: &str) -> Result<()> {
        let _ = (session_id, segment_id);
        todo!("store")
    }

    pub fn set_candidates(&self, session_id: &str, candidates: &[Candidate]) -> Result<()> {
        let _ = (session_id, candidates);
        todo!("store")
    }

    pub fn library(&self, query: &LibraryQuery) -> Result<Vec<LibraryEntry>> {
        let _ = query;
        todo!("store")
    }

    pub fn session_detail(&self, id: &str) -> Result<Option<SessionDetail>> {
        let _ = id;
        todo!("store")
    }

    pub fn delete_sessions(&self, ids: &[String]) -> Result<()> {
        let _ = ids;
        todo!("store")
    }

    pub fn set_pinned(&self, id: &str, pinned: bool) -> Result<()> {
        let _ = (id, pinned);
        todo!("store")
    }

    /// Mark sessions left `Running` by a crash as failed (called at startup).
    pub fn recover_interrupted(&self) -> Result<u32> {
        todo!("store")
    }

    pub fn cache_get(&self, namespace: &str, key: &str) -> Result<Option<String>> {
        let _ = (namespace, key);
        todo!("store")
    }

    pub fn cache_put(&self, namespace: &str, key: &str, value: &str, ttl_ms: i64) -> Result<()> {
        let _ = (namespace, key, value, ttl_ms);
        todo!("store")
    }

    pub fn export(&self, id: &str, format: ExportFormat) -> Result<String> {
        let _ = (id, format);
        todo!("store")
    }
}
