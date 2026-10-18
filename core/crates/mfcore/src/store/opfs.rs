//! The browser's persistent storage: SQLite databases in the Origin Private
//! File System through the "sahpool" VFS (one `FileSystemSyncAccessHandle` per
//! pool slot, synchronous reads and writes).
//!
//! Only a dedicated Worker can create sync access handles, and only one Worker
//! at a time can hold a pool: the engine's Worker installs it, a second install
//! fails until the first one pauses. Shut down cooperatively with
//! [`BrowserStorage::close`] (close the databases, then pause the pool) before
//! the host terminates the Worker; `terminate()` does not cancel a running
//! write in Chromium.
//!
//! Two databases share the pool: the library (`musicfetch.sqlite3`, the
//! unchanged store schema) and the work files (`files.sqlite3`, see
//! [`SqliteFiles`]).

use std::sync::Arc;

use sqlite_wasm_vfs::sahpool::{OpfsSAHPoolCfgBuilder, OpfsSAHPoolUtil, install};

use super::Store;
use crate::error::{MfError, Result};
use crate::model::LibraryQuery;
use crate::util::files::FileStore;
use crate::util::files_sqlite::SqliteFiles;
use crate::util::now_ms;

/// SQLite VFS name of the pool.
pub const VFS_NAME: &str = "opfs-sahpool";
/// The library database.
pub const LIBRARY_DB: &str = "musicfetch.sqlite3";
/// The work files database.
pub const FILES_DB: &str = "files.sqlite3";
/// Pool slots kept free: both databases and their journals, plus room for the
/// copies of damaged databases.
const MIN_CAPACITY: u32 = 8;

fn pool_error(action: &str, e: impl std::fmt::Display) -> MfError {
    MfError::Storage { message: format!("browser storage: cannot {action}: {e}") }
}

/// An installed OPFS pool.
pub struct OpfsPool {
    util: OpfsSAHPoolUtil,
    vfs: String,
}

impl OpfsPool {
    /// Installs (or attaches to) the pool stored in the OPFS directory
    /// `directory` as the VFS [`VFS_NAME`]. Fails outside a dedicated Worker and
    /// while another Worker holds the pool.
    pub async fn install(directory: &str) -> Result<Self> {
        Self::install_as(VFS_NAME, directory).await
    }

    /// Like [`install`](Self::install) under another VFS name. A Worker can hold
    /// several pools (in separate directories) under different names; a name
    /// that is already installed keeps its first directory.
    pub async fn install_as(vfs: &str, directory: &str) -> Result<Self> {
        let cfg =
            OpfsSAHPoolCfgBuilder::new().vfs_name(vfs).directory(directory).initial_capacity(MIN_CAPACITY).build();
        let util = install::<sqlite_wasm_rs::WasmOsCallback>(&cfg, false).await.map_err(|e| pool_error("open", e))?;
        if util.is_paused() {
            util.unpause_vfs().await.map_err(|e| pool_error("resume", e))?;
        }
        util.reserve_minimum_capacity(MIN_CAPACITY).await.map_err(|e| pool_error("reserve space", e))?;
        Ok(Self { util, vfs: vfs.to_string() })
    }

    /// The SQLite VFS name of this pool, for `Store::open_vfs`.
    pub fn vfs(&self) -> &str {
        &self.vfs
    }

    /// Database files in the pool.
    pub fn files(&self) -> Vec<String> {
        let mut files = self.util.list();
        files.sort();
        files
    }

    pub fn exists(&self, name: &str) -> bool {
        self.util.exists(name).unwrap_or(false)
    }

    /// A copy of the database `name` (closed or open; SQLite's own format).
    pub fn export(&self, name: &str) -> Result<Vec<u8>> {
        self.util.export_db(name).map_err(|e| pool_error("export", e))
    }

    /// Replaces the database `name` with `bytes` (it must not be open).
    pub fn import(&self, name: &str, bytes: &[u8]) -> Result<()> {
        self.util.import_db(name, bytes).map_err(|e| pool_error("import", e))
    }

    /// Deletes the database `name` (it must not be open).
    pub fn delete(&self, name: &str) -> Result<bool> {
        self.util.delete_db(name).map_err(|e| pool_error("delete", e))
    }

    /// Releases the pool's handles so another Worker can install it. Fails while
    /// a database is open.
    pub fn pause(&self) -> Result<()> {
        self.util.pause_vfs().map_err(|e| pool_error("pause", e))
    }

    /// Takes the handles back after [`pause`](Self::pause).
    pub async fn resume(&self) -> Result<()> {
        self.util.unpause_vfs().await.map_err(|e| pool_error("resume", e))
    }

    pub fn is_paused(&self) -> bool {
        self.util.is_paused()
    }

    /// Moves a damaged database aside as `<name>.corrupt-<unix ms>` (kept for
    /// diagnosis; the next open starts empty).
    fn quarantine(&self, name: &str) -> Result<()> {
        if let Ok(bytes) = self.util.export_db(name) {
            let aside = format!("{name}.corrupt-{}", now_ms());
            if let Err(e) = self.util.import_db_unchecked(&aside, &bytes) {
                tracing::warn!(error = %e, "browser storage: could not keep a copy of the damaged database");
            }
        }
        self.delete(name).map(drop)
    }
}

/// The engine's storage in the browser.
pub struct BrowserStorage {
    pub pool: OpfsPool,
    pub store: Arc<Store>,
    pub files: Arc<SqliteFiles>,
    /// Sessions a recycled or crashed Worker left running; marked failed on open.
    pub interrupted: Vec<String>,
}

impl BrowserStorage {
    /// Installs the pool, opens both databases (a damaged one is moved aside and
    /// replaced by an empty one, as natively) and marks sessions an earlier
    /// Worker left running as failed, deleting their work files.
    pub async fn open(directory: &str) -> Result<Self> {
        Self::open_in(OpfsPool::install(directory).await?).await
    }

    /// Like [`open`](Self::open) in an installed (possibly paused) pool.
    pub async fn open_in(pool: OpfsPool) -> Result<Self> {
        if pool.is_paused() {
            pool.resume().await?;
        }
        let vfs = pool.vfs().to_string();
        // Opening reads only the header; one query also reads the schema.
        let probe = LibraryQuery { kinds: Vec::new(), search: None, pinned_only: false, limit: 1, offset: 0 };
        let store = match Store::open_vfs(&vfs, LIBRARY_DB).and_then(|store| store.library(&probe).map(|_| store)) {
            Err(e) if Store::is_damaged_error(&e) => {
                tracing::error!("store: the library database is damaged; starting an empty library");
                pool.quarantine(LIBRARY_DB)?;
                Store::open_vfs(&vfs, LIBRARY_DB)?
            }
            other => other?,
        };
        let files = match SqliteFiles::open_vfs(&vfs, FILES_DB) {
            Err(e) if SqliteFiles::is_damaged_error(&e) => {
                tracing::error!(error = %e, "work files: the database is damaged; starting empty");
                pool.quarantine(FILES_DB)?;
                SqliteFiles::open_vfs(&vfs, FILES_DB)?
            }
            other => other?,
        };
        let interrupted = store.recover_interrupted_sessions()?;
        for id in &interrupted {
            if let Err(e) = files.remove_prefix(&format!("sessions/{id}/")) {
                tracing::warn!(session = %id, error = %e, "work files: could not delete");
            }
        }
        Ok(Self { pool, store: Arc::new(store), files: Arc::new(files), interrupted })
    }

    /// Closes both databases and pauses the pool, so the next Worker can take
    /// it over at once. Fails (and keeps everything open) while other owners
    /// still hold the store or the files.
    pub fn close(self) -> std::result::Result<OpfsPool, (Self, MfError)> {
        let Self { pool, store, files, interrupted } = self;
        let store = match Arc::try_unwrap(store) {
            Ok(store) => store,
            Err(store) => {
                let busy = MfError::internal("the library is still in use");
                return Err((Self { pool, store, files, interrupted }, busy));
            }
        };
        let files = match Arc::try_unwrap(files) {
            Ok(files) => files,
            Err(files) => {
                let busy = MfError::internal("the work files are still in use");
                return Err((Self { pool, store: Arc::new(store), files, interrupted }, busy));
            }
        };
        drop(store);
        drop(files);
        match pool.pause() {
            Ok(()) => Ok(pool),
            Err(e) => {
                // Nothing is open any more, so this is an OPFS failure; the pool
                // is released when the Worker ends anyway.
                tracing::warn!(error = %e, "browser storage: could not pause the pool");
                Ok(pool)
            }
        }
    }

    /// Bytes used by both databases (including free pages).
    pub fn bytes(&self) -> u64 {
        self.store.database_bytes() + self.files.database_bytes()
    }

    /// The work files as a [`FileStore`].
    pub fn file_store(&self) -> Arc<dyn FileStore> {
        self.files.clone()
    }
}
