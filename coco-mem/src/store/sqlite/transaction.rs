use std::path::Path;

use diesel_async::{AsyncConnection, TransactionManager};
use snafu::{IntoError, ResultExt};

use super::{AsyncSqliteConnection, SqliteTransactionError};
use crate::StoreResult as Result;
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

pub async fn begin_deferred_transaction(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<()> {
    <AsyncSqliteConnection as AsyncConnection>::TransactionManager::begin_transaction(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
}

pub async fn commit_deferred_transaction(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<()> {
    <AsyncSqliteConnection as AsyncConnection>::TransactionManager::commit_transaction(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
}

pub async fn rollback_deferred_transaction(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<()> {
    <AsyncSqliteConnection as AsyncConnection>::TransactionManager::rollback_transaction(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
}
