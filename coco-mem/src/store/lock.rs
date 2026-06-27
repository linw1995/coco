use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use snafu::prelude::*;

use crate::StoreResult as Result;
use crate::error::{StoreError, StoreLockedSnafu, WriteStoreMetaSnafu};

const LOCK_FILE_NAME: &str = "store.lock";

pub(super) fn open_store_lock(store_dir: &Path) -> Result<Arc<File>> {
    static LOCKS: OnceLock<Mutex<HashMap<PathBuf, Weak<File>>>> = OnceLock::new();

    let key = store_lock_key(store_dir);
    let locks = LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut locks = locks.lock().expect("store lock registry poisoned");
    if let Some(lock_file) = locks.get(&key).and_then(Weak::upgrade) {
        return Ok(lock_file);
    }

    let lock_path = store_dir.join(LOCK_FILE_NAME);
    let lock_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .context(WriteStoreMetaSnafu {
            path: lock_path.clone(),
        })?;

    let result = unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        let lock_file = Arc::new(lock_file);
        locks.insert(key, Arc::downgrade(&lock_file));
        return Ok(lock_file);
    }

    let source = std::io::Error::last_os_error();
    if source.kind() == std::io::ErrorKind::WouldBlock {
        return StoreLockedSnafu {
            path: store_dir.to_owned(),
        }
        .fail();
    }

    Err(StoreError::WriteStoreMeta {
        path: lock_path,
        source,
    })
}

fn store_lock_key(store_dir: &Path) -> PathBuf {
    fs::canonicalize(store_dir).unwrap_or_else(|_| {
        if store_dir.is_absolute() {
            store_dir.to_owned()
        } else {
            std::env::current_dir()
                .map(|current_dir| current_dir.join(store_dir))
                .unwrap_or_else(|_| store_dir.to_owned())
        }
    })
}
