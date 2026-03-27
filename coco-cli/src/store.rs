use std::fs;
use std::io;
use std::path::Path;

use snafu::prelude::*;

use crate::{
    Error, Result,
    error::{CreateStoreDirectorySnafu, ParseStoreSnafu, SerializeStoreSnafu, WriteStoreSnafu},
};
use coco_mem::Store;

pub fn load_store(path: &Path) -> Result<Store> {
    match fs::read_to_string(path) {
        Ok(data) => serde_json::from_str(&data).context(ParseStoreSnafu {
            path: path.to_owned(),
        }),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(Store::new()),
        Err(source) => Err(Error::ReadStore {
            path: path.to_owned(),
            source,
        }),
    }
}

pub fn save_store(path: &Path, store: &Store) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).context(CreateStoreDirectorySnafu {
            path: path.to_owned(),
        })?;
    }

    let data = serde_json::to_string_pretty(store).context(SerializeStoreSnafu {
        path: path.to_owned(),
    })?;
    fs::write(path, data).context(WriteStoreSnafu {
        path: path.to_owned(),
    })?;

    Ok(())
}
