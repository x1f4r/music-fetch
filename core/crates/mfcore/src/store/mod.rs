//! SQLite persistence: sessions, segments, candidates and a key-value cache.
//!
//! One connection behind a mutex serves all threads; every write is a short
//! `IMMEDIATE` transaction, so other processes (the CLI) can read alongside in
//! WAL mode. Model records are stored as JSON next to the columns used for
//! filtering and sorting. Rows whose JSON no longer decodes are skipped and
//! logged, never fatal.

mod codec;
mod export;
mod schema;
mod search;
#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use rusqlite::types::Value;
use rusqlite::{
    Connection, ErrorCode, OptionalExtension, Row, Transaction, TransactionBehavior, params,
    params_from_iter,
};

use self::codec::DbText;
use crate::error::{MfError, Result};
use crate::model::{
    Candidate, ExportFormat, LibraryEntry, LibraryQuery, Segment, SegmentKind, SessionDetail,
    SessionKind, SessionStatus, SourceInfo, Track,
};
use crate::util::now_ms;

pub use self::export::render as render_export;

/// Error stored on sessions that were `Running` when the app stopped.
pub const INTERRUPTED: &str = "Interrupted";

/// Expired cache rows are purged at most this often, besides lazy expiry on read.
const CACHE_PURGE_INTERVAL_MS: i64 = 10 * 60 * 1000;

/// Bound parameters per `DELETE … IN (…)` statement.
const DELETE_CHUNK: usize = 500;

/// Columns decoded by [`entry_from_row`], in order.
macro_rules! entry_columns {
    () => {
        "id, kind, status, created_at, title, subtitle, artwork_url, track_json, track_count, duration_ms, \
         source_url, pinned"
    };
}

/// Recomputes the columns derived from a session's segments and candidates:
/// distinct identified tracks and the text searched alongside the session.
const REFRESH_DERIVED: &str = "
UPDATE sessions SET
    track_count = (
        SELECT COUNT(DISTINCT identity_key) FROM segments WHERE session_id = ?1 AND kind = ?3
    ),
    search_tracks = trim(
        coalesce((SELECT group_concat(search_text, ' ') FROM segments WHERE session_id = ?1 AND search_text <> ''), '')
        || ' ' ||
        coalesce((SELECT search_text FROM candidates WHERE session_id = ?1 AND rank = 0), '')
    ),
    updated_at = ?2
WHERE id = ?1";

/// Partial update; `None` fields are left unchanged.
#[derive(Debug, Clone, Default)]
pub struct SessionUpdate {
    pub status: Option<SessionStatus>,
    pub kind: Option<SessionKind>,
    pub title: Option<String>,
    pub subtitle: Option<String>,
    pub artwork_url: Option<String>,
    /// The single answer (live match, confirmed candidate).
    pub track: Option<Track>,
    pub duration_ms: Option<u64>,
    /// What the input turned out to be; its `url`, when set, also replaces the
    /// session's `source_url` (the canonical link).
    pub source: Option<SourceInfo>,
    pub transcript: Option<String>,
    pub error: Option<String>,
}

impl SessionUpdate {
    fn touches_search(&self) -> bool {
        self.title.is_some()
            || self.subtitle.is_some()
            || self.track.is_some()
            || self.source.is_some()
    }
}

/// Internal failure, mapped to a short [`MfError`] at the API boundary.
#[derive(Debug)]
pub(crate) enum StoreError {
    Db(rusqlite::Error),
    Json(serde_json::Error),
    Io(std::io::Error),
    NotFound,
    Exists,
    NewerSchema { found: i64 },
}

impl From<rusqlite::Error> for StoreError {
    fn from(e: rusqlite::Error) -> Self {
        StoreError::Db(e)
    }
}

impl From<serde_json::Error> for StoreError {
    fn from(e: serde_json::Error) -> Self {
        StoreError::Json(e)
    }
}

impl From<std::io::Error> for StoreError {
    fn from(e: std::io::Error) -> Self {
        StoreError::Io(e)
    }
}

impl StoreError {
    fn into_mf(self, op: &'static str) -> MfError {
        let storage = |message: &str| MfError::Storage {
            message: message.to_string(),
        };
        match self {
            StoreError::NotFound => MfError::invalid("Session not found."),
            StoreError::Exists => MfError::invalid("A session with this id already exists."),
            StoreError::NewerSchema { found } => {
                tracing::error!(
                    op,
                    found,
                    supported = schema::CURRENT_VERSION,
                    "store: schema is newer than this build"
                );
                storage("The library was created by a newer version of Music Fetch.")
            }
            StoreError::Json(e) => {
                tracing::error!(op, error = %e, "store: could not encode record");
                MfError::internal("Could not save the session.")
            }
            StoreError::Io(e) => {
                tracing::error!(op, error = %e, "store: file system error");
                storage("The library folder is not accessible.")
            }
            StoreError::Db(e) => {
                tracing::error!(op, error = %e, "store: database error");
                storage(db_message(&e))
            }
        }
    }
}

fn db_message(e: &rusqlite::Error) -> &'static str {
    match e.sqlite_error_code() {
        Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked) => {
            "The library is busy. Try again in a moment."
        }
        Some(ErrorCode::DiskFull) => "There is not enough disk space to save the library.",
        Some(ErrorCode::ReadOnly | ErrorCode::PermissionDenied) => "The library is read-only.",
        Some(ErrorCode::CannotOpen) => "The library could not be opened.",
        Some(ErrorCode::DatabaseCorrupt | ErrorCode::NotADatabase) => {
            "The library database is damaged."
        }
        Some(ErrorCode::SystemIoFailure) => "The library could not be read or written.",
        _ => "The library could not be accessed.",
    }
}

type StoreResult<T> = std::result::Result<T, StoreError>;

/// The library database. Thread-safe; share it with an `Arc`.
pub struct Store {
    inner: Mutex<Inner>,
}

struct Inner {
    conn: Connection,
    /// Last purge of expired cache rows (Unix ms).
    last_purge_ms: i64,
}

impl Store {
    /// Opens (creating it and its folder if needed) the database at `path` and
    /// migrates it to the current schema.
    ///
    /// A file that is not a readable SQLite database is moved aside as
    /// `<name>.corrupt-<unix ms>` and replaced by an empty library.
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_file(path).map_err(|e| e.into_mf("open"))
    }

    /// An empty in-memory library, for tests and previews.
    pub fn open_in_memory() -> Result<Self> {
        Connection::open_in_memory()
            .map_err(StoreError::from)
            .and_then(|conn| Self::connect(conn, true))
            .map_err(|e| e.into_mf("open"))
    }

    fn open_file(path: &Path) -> StoreResult<Self> {
        if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::create_dir_all(dir)?;
        }
        match Self::connect(Connection::open(path)?, false) {
            Err(StoreError::Db(e)) if is_damaged(&e) => {
                tracing::error!(error = %e, "store: database is damaged; starting an empty library");
                quarantine(path)?;
                Self::connect(Connection::open(path)?, false)
            }
            result => result,
        }
    }

    fn connect(mut conn: Connection, in_memory: bool) -> StoreResult<Self> {
        schema::configure(&conn, in_memory)?;
        schema::migrate(&mut conn)?;
        let now = now_ms();
        if let Err(e) = purge_cache(&conn, now) {
            tracing::warn!(error = %e, "store: cache purge failed");
        }
        Ok(Self {
            inner: Mutex::new(Inner {
                conn,
                last_purge_ms: now,
            }),
        })
    }

    /// Runs `f` in a deferred (read) transaction for a consistent snapshot.
    fn read<T>(
        &self,
        op: &'static str,
        f: impl FnOnce(&Transaction<'_>) -> StoreResult<T>,
    ) -> Result<T> {
        let mut inner = self.inner.lock();
        run_tx(&mut inner.conn, TransactionBehavior::Deferred, f).map_err(|e| e.into_mf(op))
    }

    /// Runs `f` in an immediate (write) transaction; rolled back on error.
    fn write<T>(
        &self,
        op: &'static str,
        f: impl FnOnce(&Transaction<'_>) -> StoreResult<T>,
    ) -> Result<T> {
        let mut inner = self.inner.lock();
        run_tx(&mut inner.conn, TransactionBehavior::Immediate, f).map_err(|e| e.into_mf(op))
    }

    /// Creates a `Running` session.
    pub fn create_session(
        &self,
        id: &str,
        kind: SessionKind,
        title: &str,
        source_url: Option<&str>,
        created_at_ms: i64,
    ) -> Result<()> {
        let search_text = search::session_text(title, None, None, None);
        self.write("create_session", |tx| {
            if session_exists(tx, id)? {
                return Err(StoreError::Exists);
            }
            tx.execute(
                "INSERT INTO sessions (id, kind, status, created_at, updated_at, title, source_url, search_text)
                 VALUES (?1, ?2, ?3, ?4, ?4, ?5, ?6, ?7)",
                params![
                    id,
                    kind.as_text(),
                    SessionStatus::Running.as_text(),
                    created_at_ms,
                    title,
                    source_url,
                    search_text
                ],
            )?;
            Ok(())
        })
    }

    /// Applies the set fields of `update`.
    pub fn update_session(&self, id: &str, update: SessionUpdate) -> Result<()> {
        let now = now_ms();
        self.write("update_session", |tx| {
            let track_json = update.track.as_ref().map(codec::encode).transpose()?;
            let source_json = update.source.as_ref().map(codec::encode).transpose()?;
            let search_text = if update.touches_search() {
                Some(merged_search_text(tx, id, &update)?)
            } else {
                None
            };
            let changed = tx.execute(
                "UPDATE sessions SET
                    status = coalesce(?2, status),
                    kind = coalesce(?3, kind),
                    title = coalesce(?4, title),
                    subtitle = coalesce(?5, subtitle),
                    artwork_url = coalesce(?6, artwork_url),
                    track_json = coalesce(?7, track_json),
                    duration_ms = coalesce(?8, duration_ms),
                    source_json = coalesce(?9, source_json),
                    source_url = coalesce(?10, source_url),
                    transcript = coalesce(?11, transcript),
                    error = coalesce(?12, error),
                    search_text = coalesce(?13, search_text),
                    updated_at = ?14
                 WHERE id = ?1",
                params![
                    id,
                    update.status.map(DbText::as_text),
                    update.kind.map(DbText::as_text),
                    update.title,
                    update.subtitle,
                    update.artwork_url,
                    track_json,
                    update.duration_ms.map(codec::to_db_int),
                    source_json,
                    update.source.as_ref().and_then(|s| s.url.as_deref()),
                    update.transcript,
                    update.error,
                    search_text,
                    now
                ],
            )?;
            if changed == 0 {
                return Err(StoreError::NotFound);
            }
            Ok(())
        })
    }

    /// Inserts or replaces (by id) a segment and refreshes the session's track count.
    pub fn upsert_segment(&self, session_id: &str, segment: &Segment) -> Result<()> {
        let segment = codec::sanitize_segment(segment);
        let json = codec::encode(segment.as_ref())
            .map_err(|e| StoreError::from(e).into_mf("upsert_segment"))?;
        let track = segment
            .track
            .as_ref()
            .filter(|_| segment.kind == SegmentKind::Track);
        let identity =
            (segment.kind == SegmentKind::Track).then(|| search::identity_key(track, &segment.id));
        let search_text = track.map(search::track_text).unwrap_or_default();
        let now = now_ms();
        self.write("upsert_segment", |tx| {
            if !session_exists(tx, session_id)? {
                return Err(StoreError::NotFound);
            }
            tx.prepare_cached(
                "INSERT INTO segments
                    (session_id, id, item_index, start_ms, end_ms, kind, tier, confidence, identity_key, search_text, json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                 ON CONFLICT (session_id, id) DO UPDATE SET
                    item_index = excluded.item_index,
                    start_ms = excluded.start_ms,
                    end_ms = excluded.end_ms,
                    kind = excluded.kind,
                    tier = excluded.tier,
                    confidence = excluded.confidence,
                    identity_key = excluded.identity_key,
                    search_text = excluded.search_text,
                    json = excluded.json",
            )?
            .execute(params![
                session_id,
                segment.id,
                segment.item_index,
                codec::to_db_int(segment.start_ms),
                codec::to_db_int(segment.end_ms),
                segment.kind.as_text(),
                segment.tier.as_text(),
                f64::from(segment.confidence),
                identity,
                search_text,
                json
            ])?;
            refresh_derived(tx, session_id, now)
        })
    }

    /// Removes a segment (merged away by stitching). Unknown ids are ignored.
    pub fn remove_segment(&self, session_id: &str, segment_id: &str) -> Result<()> {
        let now = now_ms();
        self.write("remove_segment", |tx| {
            let removed = tx.execute(
                "DELETE FROM segments WHERE session_id = ?1 AND id = ?2",
                params![session_id, segment_id],
            )?;
            if removed > 0 {
                refresh_derived(tx, session_id, now)?;
            }
            Ok(())
        })
    }

    /// Replaces the session's candidates; their order is their rank.
    pub fn set_candidates(&self, session_id: &str, candidates: &[Candidate]) -> Result<()> {
        let rows = candidates
            .iter()
            .map(|c| {
                let c = codec::sanitize_candidate(c);
                Ok((
                    f64::from(c.score),
                    c.tier.as_text(),
                    search::track_text(&c.track),
                    codec::encode(c.as_ref())?,
                ))
            })
            .collect::<StoreResult<Vec<_>>>()
            .map_err(|e| e.into_mf("set_candidates"))?;
        let now = now_ms();
        self.write("set_candidates", |tx| {
            if !session_exists(tx, session_id)? {
                return Err(StoreError::NotFound);
            }
            tx.execute("DELETE FROM candidates WHERE session_id = ?1", [session_id])?;
            let mut insert = tx.prepare_cached(
                "INSERT INTO candidates (session_id, rank, score, tier, search_text, json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )?;
            for (rank, (score, tier, search_text, json)) in rows.iter().enumerate() {
                let rank = i64::try_from(rank).unwrap_or(i64::MAX);
                insert.execute(params![session_id, rank, score, tier, search_text, json])?;
            }
            refresh_derived(tx, session_id, now)
        })
    }

    /// Library entries, newest first.
    ///
    /// `search` is case- and accent-insensitive; every word must occur in the
    /// session's title, subtitle, track, source, identified tracks or top
    /// candidate. `limit = 0` means no limit.
    pub fn library(&self, query: &LibraryQuery) -> Result<Vec<LibraryEntry>> {
        let mut sql = String::from(concat!(
            "SELECT ",
            entry_columns!(),
            " FROM sessions WHERE 1 = 1"
        ));
        let mut args: Vec<Value> = Vec::new();
        let mut kinds: Vec<&'static str> = query.kinds.iter().map(|k| k.as_text()).collect();
        kinds.sort_unstable();
        kinds.dedup();
        if !kinds.is_empty() {
            sql.push_str(" AND kind IN (");
            sql.push_str(&vec!["?"; kinds.len()].join(", "));
            sql.push(')');
            args.extend(kinds.into_iter().map(|k| Value::Text(k.to_string())));
        }
        if query.pinned_only {
            sql.push_str(" AND pinned = 1");
        }
        for term in query
            .search
            .as_deref()
            .map(search::terms)
            .unwrap_or_default()
        {
            sql.push_str(" AND (instr(search_text, ?) > 0 OR instr(search_tracks, ?) > 0)");
            args.push(Value::Text(term.clone()));
            args.push(Value::Text(term));
        }
        sql.push_str(" ORDER BY created_at DESC, rowid DESC LIMIT ? OFFSET ?");
        args.push(Value::Integer(if query.limit == 0 {
            -1
        } else {
            i64::from(query.limit)
        }));
        args.push(Value::Integer(i64::from(query.offset)));

        self.read("library", |tx| {
            let mut stmt = tx.prepare_cached(&sql)?;
            let rows = stmt.query_map(params_from_iter(args.iter()), entry_from_row)?;
            let mut entries = Vec::new();
            for row in rows {
                entries.extend(row?);
            }
            Ok(entries)
        })
    }

    /// Everything stored for a session, or `None` if it does not exist.
    /// Segments are sorted by item and start; candidates by rank.
    pub fn session_detail(&self, id: &str) -> Result<Option<SessionDetail>> {
        self.read("session_detail", |tx| {
            let row = tx
                .prepare_cached(concat!(
                    "SELECT ",
                    entry_columns!(),
                    ", source_json, transcript FROM sessions WHERE id = ?1"
                ))?
                .query_row([id], |row| {
                    Ok((
                        entry_from_row(row)?,
                        row.get::<_, Option<String>>(12)?,
                        row.get::<_, Option<String>>(13)?,
                    ))
                })
                .optional()?;
            let Some((Some(entry), source_json, transcript)) = row else {
                return Ok(None);
            };
            let source = source_json
                .and_then(|json| decode_or_skip("source", id, &json, codec::decode_source));
            Ok(Some(SessionDetail {
                entry,
                source,
                segments: load_segments(tx, id)?,
                candidates: load_candidates(tx, id)?,
                transcript,
            }))
        })
    }

    /// One segment of a session, e.g. before applying a user correction.
    pub fn segment(&self, session_id: &str, segment_id: &str) -> Result<Option<Segment>> {
        self.read("segment", |tx| {
            let json = tx
                .prepare_cached("SELECT json FROM segments WHERE session_id = ?1 AND id = ?2")?
                .query_row(params![session_id, segment_id], |row| {
                    row.get::<_, String>(0)
                })
                .optional()?;
            Ok(json.and_then(|json| {
                decode_or_skip("segment", session_id, &json, codec::decode_segment)
            }))
        })
    }

    /// Deletes sessions with their segments and candidates in one transaction.
    /// Unknown ids are ignored.
    pub fn delete_sessions(&self, ids: &[String]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        self.write("delete_sessions", |tx| {
            let mut deleted = 0;
            for chunk in ids.chunks(DELETE_CHUNK) {
                let sql = format!(
                    "DELETE FROM sessions WHERE id IN ({})",
                    vec!["?"; chunk.len()].join(", ")
                );
                deleted += tx.execute(&sql, params_from_iter(chunk.iter()))?;
            }
            tracing::debug!(requested = ids.len(), deleted, "store: deleted sessions");
            Ok(())
        })
    }

    /// Pinned sessions keep their cached media and can be listed on their own.
    pub fn set_pinned(&self, id: &str, pinned: bool) -> Result<()> {
        let now = now_ms();
        self.write("set_pinned", |tx| {
            let changed = tx.execute(
                "UPDATE sessions SET pinned = ?2, updated_at = ?3 WHERE id = ?1",
                params![id, pinned, now],
            )?;
            if changed == 0 {
                return Err(StoreError::NotFound);
            }
            Ok(())
        })
    }

    /// Mark sessions left `Running` by a crash as failed (called at startup).
    /// Returns how many were marked.
    pub fn recover_interrupted(&self) -> Result<u32> {
        let now = now_ms();
        self.write("recover_interrupted", |tx| {
            let changed = tx.execute(
                "UPDATE sessions SET status = ?1, error = ?2, updated_at = ?3 WHERE status = ?4",
                params![
                    SessionStatus::Failed.as_text(),
                    INTERRUPTED,
                    now,
                    SessionStatus::Running.as_text()
                ],
            )?;
            Ok(u32::try_from(changed).unwrap_or(u32::MAX))
        })
    }

    /// The cached value, or `None` when missing or expired.
    pub fn cache_get(&self, namespace: &str, key: &str) -> Result<Option<String>> {
        self.cache_get_at(namespace, key, now_ms())
    }

    pub(crate) fn cache_get_at(
        &self,
        namespace: &str,
        key: &str,
        now: i64,
    ) -> Result<Option<String>> {
        let inner = self.inner.lock();
        let found = inner
            .conn
            .prepare_cached("SELECT value, expires_at FROM cache WHERE namespace = ?1 AND key = ?2")
            .and_then(|mut stmt| {
                stmt.query_row(params![namespace, key], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                })
                .optional()
            })
            .map_err(|e| StoreError::from(e).into_mf("cache_get"))?;
        match found {
            Some((value, expires_at)) if expires_at > now => Ok(Some(value)),
            Some(_) => {
                // Lazy expiry; a failure here only delays the cleanup.
                if let Err(e) = inner.conn.execute(
                    "DELETE FROM cache WHERE namespace = ?1 AND key = ?2 AND expires_at <= ?3",
                    params![namespace, key, now],
                ) {
                    tracing::warn!(error = %e, "store: could not drop expired cache entry");
                }
                Ok(None)
            }
            None => Ok(None),
        }
    }

    /// Stores `value` for `ttl_ms` milliseconds. A TTL of zero or less removes
    /// the entry instead.
    pub fn cache_put(&self, namespace: &str, key: &str, value: &str, ttl_ms: i64) -> Result<()> {
        self.cache_put_at(namespace, key, value, ttl_ms, now_ms())
    }

    pub(crate) fn cache_put_at(
        &self,
        namespace: &str,
        key: &str,
        value: &str,
        ttl_ms: i64,
        now: i64,
    ) -> Result<()> {
        let mut inner = self.inner.lock();
        let result = if ttl_ms <= 0 {
            inner.conn.execute(
                "DELETE FROM cache WHERE namespace = ?1 AND key = ?2",
                params![namespace, key],
            )
        } else {
            inner.conn.execute(
                "INSERT INTO cache (namespace, key, value, expires_at) VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT (namespace, key) DO UPDATE SET value = excluded.value, expires_at = excluded.expires_at",
                params![namespace, key, value, now.saturating_add(ttl_ms)],
            )
        };
        if now.saturating_sub(inner.last_purge_ms) >= CACHE_PURGE_INTERVAL_MS {
            inner.last_purge_ms = now;
            if let Err(e) = purge_cache(&inner.conn, now) {
                tracing::warn!(error = %e, "store: cache purge failed");
            }
        }
        result
            .map(drop)
            .map_err(|e| StoreError::from(e).into_mf("cache_put"))
    }

    /// Deletes expired cache entries now; returns how many were removed.
    pub fn purge_expired_cache(&self) -> Result<u32> {
        self.purge_expired_cache_at(now_ms())
    }

    pub(crate) fn purge_expired_cache_at(&self, now: i64) -> Result<u32> {
        let mut inner = self.inner.lock();
        inner.last_purge_ms = now;
        purge_cache(&inner.conn, now).map_err(|e| StoreError::from(e).into_mf("purge_cache"))
    }

    /// Size of the main database file in bytes (allocated pages, excluding the WAL).
    pub fn database_bytes(&self) -> u64 {
        let inner = self.inner.lock();
        inner
            .conn
            .query_row(
                "SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size()",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(codec::to_u64)
            .unwrap_or(0)
    }

    /// Renders a session; see [`render_export`] for the formats.
    pub fn export(&self, id: &str, format: ExportFormat) -> Result<String> {
        let detail = self
            .session_detail(id)?
            .ok_or_else(|| StoreError::NotFound.into_mf("export"))?;
        export::render(&detail, format)
    }
}

fn run_tx<T>(
    conn: &mut Connection,
    behavior: TransactionBehavior,
    f: impl FnOnce(&Transaction<'_>) -> StoreResult<T>,
) -> StoreResult<T> {
    let tx = conn.transaction_with_behavior(behavior)?;
    let value = f(&tx)?;
    tx.commit()?;
    Ok(value)
}

fn is_damaged(e: &rusqlite::Error) -> bool {
    matches!(
        e.sqlite_error_code(),
        Some(ErrorCode::NotADatabase | ErrorCode::DatabaseCorrupt)
    )
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

/// Moves a damaged database and its WAL files aside.
fn quarantine(path: &Path) -> std::io::Result<()> {
    let stamp = format!(".corrupt-{}", now_ms());
    for sidecar in ["", "-wal", "-shm"] {
        let from = with_suffix(path, sidecar);
        if from.exists() {
            std::fs::rename(&from, with_suffix(&from, &stamp))?;
        }
    }
    Ok(())
}

/// Deletes expired cache rows. Checks first, so a store opened only to read
/// (the CLI next to the running app) never waits for the write lock.
fn purge_cache(conn: &Connection, now: i64) -> rusqlite::Result<u32> {
    let expired = conn
        .prepare_cached("SELECT EXISTS (SELECT 1 FROM cache WHERE expires_at <= ?1)")?
        .query_row([now], |row| row.get::<_, bool>(0))?;
    if !expired {
        return Ok(0);
    }
    let removed = conn.execute("DELETE FROM cache WHERE expires_at <= ?1", [now])?;
    Ok(u32::try_from(removed).unwrap_or(u32::MAX))
}

fn session_exists(conn: &Connection, id: &str) -> StoreResult<bool> {
    Ok(conn
        .prepare_cached("SELECT 1 FROM sessions WHERE id = ?1")?
        .exists([id])?)
}

fn refresh_derived(tx: &Transaction<'_>, session_id: &str, now: i64) -> StoreResult<()> {
    tx.prepare_cached(REFRESH_DERIVED)?.execute(params![
        session_id,
        now,
        SegmentKind::Track.as_text()
    ])?;
    Ok(())
}

/// Search text for a session after `update`, merged with its stored fields.
fn merged_search_text(
    tx: &Transaction<'_>,
    id: &str,
    update: &SessionUpdate,
) -> StoreResult<String> {
    type Stored = (String, Option<String>, Option<String>, Option<String>);
    let (title, subtitle, track_json, source_json): Stored = tx
        .prepare_cached(
            "SELECT title, subtitle, track_json, source_json FROM sessions WHERE id = ?1",
        )?
        .query_row([id], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .optional()?
        .ok_or(StoreError::NotFound)?;
    let stored_track = match &update.track {
        Some(_) => None,
        None => track_json.and_then(|json| decode_or_skip("track", id, &json, codec::decode_track)),
    };
    let stored_source = match &update.source {
        Some(_) => None,
        None => {
            source_json.and_then(|json| decode_or_skip("source", id, &json, codec::decode_source))
        }
    };
    Ok(search::session_text(
        update.title.as_deref().unwrap_or(&title),
        update.subtitle.as_deref().or(subtitle.as_deref()),
        update.track.as_ref().or(stored_track.as_ref()),
        update.source.as_ref().or(stored_source.as_ref()),
    ))
}

/// Decodes a JSON column; an unreadable value is logged and treated as absent.
fn decode_or_skip<T>(
    what: &'static str,
    session_id: &str,
    json: &str,
    decode: fn(&str) -> serde_json::Result<T>,
) -> Option<T> {
    match decode(json) {
        Ok(value) => Some(value),
        Err(e) => {
            tracing::warn!(session_id, what, error = %e, "store: skipping unreadable row");
            None
        }
    }
}

/// Decodes [`entry_columns!`]; `None` for rows with an unknown kind or status.
fn entry_from_row(row: &Row<'_>) -> rusqlite::Result<Option<LibraryEntry>> {
    let id: String = row.get(0)?;
    let kind: String = row.get(1)?;
    let status: String = row.get(2)?;
    let (Some(kind), Some(status)) = (
        SessionKind::from_text(&kind),
        SessionStatus::from_text(&status),
    ) else {
        tracing::warn!(session_id = %id, kind, status, "store: skipping session with unknown kind or status");
        return Ok(None);
    };
    let track = row
        .get::<_, Option<String>>(7)?
        .and_then(|json| decode_or_skip("track", &id, &json, codec::decode_track));
    let artwork_url = row
        .get::<_, Option<String>>(6)?
        .or_else(|| track.as_ref().and_then(|t| t.artwork_url.clone()));
    Ok(Some(LibraryEntry {
        kind,
        status,
        created_at_ms: row.get(3)?,
        title: row.get(4)?,
        subtitle: row.get(5)?,
        artwork_url,
        track,
        track_count: codec::to_u32(row.get(8)?),
        duration_ms: row.get::<_, Option<i64>>(9)?.map(codec::to_u64),
        source_url: row.get(10)?,
        pinned: row.get::<_, i64>(11)? != 0,
        id,
    }))
}

fn load_segments(tx: &Transaction<'_>, session_id: &str) -> StoreResult<Vec<Segment>> {
    let mut stmt = tx.prepare_cached(
        "SELECT json FROM segments WHERE session_id = ?1 ORDER BY item_index, start_ms, end_ms, id",
    )?;
    let rows = stmt.query_map([session_id], |row| row.get::<_, String>(0))?;
    let mut segments = Vec::new();
    for json in rows {
        segments.extend(decode_or_skip(
            "segment",
            session_id,
            &json?,
            codec::decode_segment,
        ));
    }
    Ok(segments)
}

fn load_candidates(tx: &Transaction<'_>, session_id: &str) -> StoreResult<Vec<Candidate>> {
    let mut stmt =
        tx.prepare_cached("SELECT json FROM candidates WHERE session_id = ?1 ORDER BY rank")?;
    let rows = stmt.query_map([session_id], |row| row.get::<_, String>(0))?;
    let mut candidates = Vec::new();
    for json in rows {
        candidates.extend(decode_or_skip(
            "candidate",
            session_id,
            &json?,
            codec::decode_candidate,
        ));
    }
    Ok(candidates)
}
