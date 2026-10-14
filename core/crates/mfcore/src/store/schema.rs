//! Connection setup and forward-only schema migrations (`PRAGMA user_version`).

use std::time::Duration;

use rusqlite::{Connection, TransactionBehavior};

use super::StoreError;

const BUSY_TIMEOUT: Duration = Duration::from_millis(5000);

/// Migration `i` upgrades a database from `user_version = i` to `i + 1`.
/// Released entries are never edited; changes go into a new entry.
const MIGRATIONS: &[&str] = &[V1];

/// Schema version this build writes.
pub(crate) const CURRENT_VERSION: i64 = MIGRATIONS.len() as i64;

const V1: &str = "
CREATE TABLE sessions (
    id            TEXT PRIMARY KEY NOT NULL,
    kind          TEXT NOT NULL,
    status        TEXT NOT NULL,
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL,
    title         TEXT NOT NULL DEFAULT '',
    subtitle      TEXT,
    artwork_url   TEXT,
    track_json    TEXT,
    track_count   INTEGER NOT NULL DEFAULT 0,
    duration_ms   INTEGER,
    source_url    TEXT,
    source_json   TEXT,
    transcript    TEXT,
    error         TEXT,
    pinned        INTEGER NOT NULL DEFAULT 0,
    search_text   TEXT NOT NULL DEFAULT '',
    search_tracks TEXT NOT NULL DEFAULT ''
) STRICT;

CREATE INDEX sessions_by_created ON sessions (created_at DESC);
CREATE INDEX sessions_running ON sessions (status) WHERE status = 'running';

CREATE TABLE segments (
    session_id   TEXT NOT NULL REFERENCES sessions (id) ON DELETE CASCADE,
    id           TEXT NOT NULL,
    item_index   INTEGER NOT NULL,
    start_ms     INTEGER NOT NULL,
    end_ms       INTEGER NOT NULL,
    kind         TEXT NOT NULL,
    tier         TEXT NOT NULL,
    confidence   REAL NOT NULL,
    identity_key TEXT,
    search_text  TEXT NOT NULL DEFAULT '',
    json         TEXT NOT NULL,
    PRIMARY KEY (session_id, id)
) STRICT;

CREATE INDEX segments_by_position ON segments (session_id, item_index, start_ms);

CREATE TABLE candidates (
    session_id  TEXT NOT NULL REFERENCES sessions (id) ON DELETE CASCADE,
    rank        INTEGER NOT NULL,
    score       REAL NOT NULL,
    tier        TEXT NOT NULL,
    search_text TEXT NOT NULL DEFAULT '',
    json        TEXT NOT NULL,
    PRIMARY KEY (session_id, rank)
) STRICT;

CREATE TABLE cache (
    namespace  TEXT NOT NULL,
    key        TEXT NOT NULL,
    value      TEXT NOT NULL,
    expires_at INTEGER NOT NULL,
    PRIMARY KEY (namespace, key)
) STRICT;

CREATE INDEX cache_by_expiry ON cache (expires_at);
";

/// Applies connection pragmas. WAL is skipped for in-memory databases.
pub(crate) fn configure(conn: &Connection, in_memory: bool) -> Result<(), StoreError> {
    conn.busy_timeout(BUSY_TIMEOUT)?;
    if !in_memory {
        // WAL is persistent; only switch (which takes a lock) when needed.
        let mut mode: String = conn.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
        if !mode.eq_ignore_ascii_case("wal") {
            mode = conn.pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get(0))?;
        }
        if !mode.eq_ignore_ascii_case("wal") {
            tracing::warn!(mode, "store: WAL journal mode unavailable");
        }
    }
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.pragma_update(None, "foreign_keys", true)?;
    conn.pragma_update(None, "trusted_schema", false)?;
    Ok(())
}

pub(crate) fn user_version(conn: &Connection) -> Result<i64, StoreError> {
    Ok(conn.pragma_query_value(None, "user_version", |row| row.get(0))?)
}

/// Brings the schema to [`CURRENT_VERSION`] in one transaction. The version is
/// re-read under the write lock so concurrent openers migrate only once.
pub(crate) fn migrate(conn: &mut Connection) -> Result<(), StoreError> {
    if user_version(conn)? == CURRENT_VERSION {
        return Ok(());
    }
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let found = user_version(&tx)?;
    if found > CURRENT_VERSION {
        return Err(StoreError::NewerSchema { found });
    }
    let start = usize::try_from(found.max(0)).unwrap_or(0);
    for (index, sql) in MIGRATIONS.iter().enumerate().skip(start) {
        tx.execute_batch(sql)?;
        tracing::info!(version = index + 1, "store: migrated schema");
    }
    tx.pragma_update(None, "user_version", CURRENT_VERSION)?;
    tx.commit()?;
    Ok(())
}
