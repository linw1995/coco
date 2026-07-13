use super::*;

pub enum SnapshotTransactionError {
    Query(diesel::result::Error),
    Operation(crate::Error),
    RollbackFalse,
}

impl From<diesel::result::Error> for SnapshotTransactionError {
    fn from(source: diesel::result::Error) -> Self {
        Self::Query(source)
    }
}

impl ConsoleGraphSnapshotStore {
    #[cfg(test)]
    pub fn run_write_transaction<T, F>(
        &self,
        connection: &mut SqliteConnection,
        operation: F,
    ) -> crate::Result<T>
    where
        F: FnOnce(&Self, &mut SqliteConnection) -> crate::Result<T>,
    {
        self.finish_transaction(connection.immediate_transaction(|connection| {
            operation(self, connection).map_err(SnapshotTransactionError::Operation)
        }))
    }

    pub fn run_read_transaction<T, F>(
        &self,
        connection: &mut SqliteConnection,
        operation: F,
    ) -> crate::Result<T>
    where
        F: FnOnce(&Self, &mut SqliteConnection) -> crate::Result<T>,
    {
        self.finish_transaction(connection.transaction(|connection| {
            operation(self, connection).map_err(SnapshotTransactionError::Operation)
        }))
    }

    pub fn run_bool_write_transaction<F>(
        &self,
        connection: &mut SqliteConnection,
        operation: F,
    ) -> crate::Result<bool>
    where
        F: FnOnce(&Self, &mut SqliteConnection) -> crate::Result<bool>,
    {
        match connection.immediate_transaction(|connection| match operation(self, connection) {
            Ok(true) => Ok(true),
            Ok(false) => Err(SnapshotTransactionError::RollbackFalse),
            Err(error) => Err(SnapshotTransactionError::Operation(error)),
        }) {
            Ok(true) => Ok(true),
            Err(SnapshotTransactionError::RollbackFalse) => Ok(false),
            result => self.finish_transaction(result),
        }
    }

    pub fn finish_transaction<T>(
        &self,
        result: std::result::Result<T, SnapshotTransactionError>,
    ) -> crate::Result<T> {
        result.map_err(|error| match error {
            SnapshotTransactionError::Query(source) => crate::Error::QueryGraphSnapshotStore {
                path: self.path.as_ref().clone(),
                source,
            },
            SnapshotTransactionError::Operation(error) => error,
            SnapshotTransactionError::RollbackFalse => crate::Error::ConsoleGraphRebuild {
                mode: "unknown",
                source_version: 0,
                message: "unexpected rollback sentinel".to_owned(),
            },
        })
    }
}
