use std::path::Path;

use super::{current_schema_version, load_store_meta_bool};
use crate::StoreResult as Result;

use super::super::AsyncSqliteConnection;

pub const VERSION: i32 = 23;

pub const FS_MIGRATION_COMPLETE_META_KEY: &str = "fs_migration_complete";

pub async fn replaces_legacy_json_store(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<bool> {
    if current_schema_version(connection, path)
        .await?
        .is_some_and(|version| version >= VERSION)
    {
        return Ok(true);
    }
    load_store_meta_bool(connection, path, FS_MIGRATION_COMPLETE_META_KEY).await
}
