use std::path::Path;

use snafu::IntoError;

use super::SqliteTransactionError;
use crate::error::{QuerySqliteStoreSnafu, StoreError};

impl From<diesel::result::Error> for SqliteTransactionError {
    fn from(source: diesel::result::Error) -> Self {
        Self::Query(source)
    }
}

impl SqliteTransactionError {
    pub fn into_store_error(self, path: &Path) -> StoreError {
        match self {
            Self::Query(source) => QuerySqliteStoreSnafu {
                path: path.to_owned(),
            }
            .into_error(source),
            Self::Operation(error) => error,
        }
    }
}
