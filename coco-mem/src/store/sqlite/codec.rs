use std::path::Path;

use serde_json::Value;
use snafu::prelude::*;

use crate::error::{CorruptedStoreSnafu, ParseSqliteStoreValueSnafu, StoreError};
use crate::{SessionRole, StoreResult as Result};

pub fn parse_json_column(path: &Path, column: &str, value: &str) -> Result<Value> {
    serde_json::from_str(value).context(ParseSqliteStoreValueSnafu {
        path: path.to_owned(),
        column: column.to_owned(),
    })
}

pub fn parse_u64_column(path: &Path, column: &str, value: &str) -> Result<u64> {
    value.parse().map_err(|source| StoreError::CorruptedStore {
        path: path.to_owned(),
        message: format!("invalid SQLite {column}: {source}"),
    })
}

pub fn parse_session_role(role: &str, path: &Path) -> Result<SessionRole> {
    match role {
        "orchestrator" => Ok(SessionRole::Orchestrator),
        "runner" => Ok(SessionRole::Runner),
        _ => CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("invalid SQLite session role {role:?}"),
        }
        .fail(),
    }
}
