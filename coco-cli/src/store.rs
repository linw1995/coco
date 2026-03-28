use std::path::Path;

use snafu::prelude::*;

use crate::{Result, error::StoreSnafu};
use coco_mem::FsStore;

pub fn open_store(path: &Path) -> Result<FsStore> {
    FsStore::open(path).context(StoreSnafu)
}
