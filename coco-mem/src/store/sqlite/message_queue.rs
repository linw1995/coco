use std::path::Path;

use async_trait::async_trait;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use snafu::ResultExt;

use super::{AsyncSqliteConnection, SqliteStore, SqliteTransactionError};
use crate::error::{ParseSqliteStoreValueSnafu, QuerySqliteStoreSnafu};
use crate::schema::message_queue_items;
use crate::store::MessageQueueStore;
use crate::{MessageQueueItem, StoreResult as Result};

diesel::table! {
    #[sql_name = "message_queue_items"]
    message_queue_items_with_rowid (queue, message_id) {
        rowid -> BigInt,
        queue -> Text,
        message_id -> Text,
        created_at -> Text,
        payload_json -> Text,
    }
}

#[derive(Queryable)]
struct MessageQueueItemRow {
    row_id: i64,
    created_at: String,
    item_json: String,
}

async fn load_queue_messages(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    queue: &str,
) -> Result<Vec<MessageQueueItem>> {
    let rows = message_queue_items_with_rowid::table
        .filter(message_queue_items_with_rowid::queue.eq(queue))
        .select((
            message_queue_items_with_rowid::rowid,
            message_queue_items_with_rowid::created_at,
            message_queue_items_with_rowid::payload_json,
        ))
        .order(message_queue_items_with_rowid::rowid)
        .load::<MessageQueueItemRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    message_queue_rows_into_sorted_items(path, rows)
}

async fn load_message_queue_names(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<Vec<String>> {
    message_queue_items::table
        .select(message_queue_items::queue)
        .distinct()
        .order(message_queue_items::queue)
        .load::<String>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
}

fn message_queue_item_row_into_item(
    path: &Path,
    row: MessageQueueItemRow,
) -> Result<MessageQueueItem> {
    let item = serde_json::from_str::<MessageQueueItem>(&row.item_json).context(
        ParseSqliteStoreValueSnafu {
            path: path.to_owned(),
            column: "message_queue_items.payload_json".to_owned(),
        },
    )?;
    validate_text_summary(
        path,
        "message_queue_items.created_at",
        &row.created_at,
        &item.created_at.to_string(),
    )?;
    Ok(item)
}

fn message_queue_rows_into_sorted_items(
    path: &Path,
    rows: Vec<MessageQueueItemRow>,
) -> Result<Vec<MessageQueueItem>> {
    let mut items = Vec::new();
    for row in rows {
        let row_id = row.row_id;
        let item = message_queue_item_row_into_item(path, row)?;
        items.push((row_id, item));
    }
    items.sort_by(|(left_row_id, left), (right_row_id, right)| {
        left.created_at
            .cmp(&right.created_at)
            .then_with(|| left_row_id.cmp(right_row_id))
    });
    Ok(items.into_iter().map(|(_, item)| item).collect())
}

pub async fn persist_message_queue_item(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    item: &MessageQueueItem,
) -> Result<()> {
    let item_json = serde_json::to_string(item).context(ParseSqliteStoreValueSnafu {
        path: path.to_owned(),
        column: "message_queue_items.payload_json".to_owned(),
    })?;
    diesel::insert_into(message_queue_items::table)
        .values((
            message_queue_items::queue.eq(&item.queue),
            message_queue_items::message_id.eq(&item.message_id),
            message_queue_items::created_at.eq(item.created_at.to_string()),
            message_queue_items::payload_json.eq(item_json),
        ))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(())
}

async fn dequeue_message_queue_item(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    queue: &str,
) -> Result<Option<MessageQueueItem>> {
    connection
        .immediate_transaction::<Option<MessageQueueItem>, SqliteTransactionError, _>(
            async |connection| {
                let rows = message_queue_items_with_rowid::table
                    .filter(message_queue_items_with_rowid::queue.eq(queue))
                    .select((
                        message_queue_items_with_rowid::rowid,
                        message_queue_items_with_rowid::created_at,
                        message_queue_items_with_rowid::payload_json,
                    ))
                    .order(message_queue_items_with_rowid::rowid)
                    .load::<MessageQueueItemRow>(connection)
                    .await
                    .context(QuerySqliteStoreSnafu {
                        path: path.to_owned(),
                    })
                    .map_err(SqliteTransactionError::Operation)?;
                let Some(item) = message_queue_rows_into_sorted_items(path, rows)
                    .map_err(SqliteTransactionError::Operation)?
                    .into_iter()
                    .next()
                else {
                    return Ok(None);
                };
                delete_message_queue_item(connection, path, &item)
                    .await
                    .map_err(SqliteTransactionError::Operation)?;
                Ok(Some(item))
            },
        )
        .await
        .map_err(|error| error.into_store_error(path))
}

async fn delete_message_queue_item(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    item: &MessageQueueItem,
) -> Result<()> {
    diesel::delete(
        message_queue_items::table
            .filter(message_queue_items::queue.eq(&item.queue))
            .filter(message_queue_items::message_id.eq(&item.message_id)),
    )
    .execute(connection)
    .await
    .context(QuerySqliteStoreSnafu {
        path: path.to_owned(),
    })?;
    Ok(())
}

fn validate_text_summary(
    path: &Path,
    column: &'static str,
    actual: &str,
    expected: &str,
) -> Result<()> {
    snafu::ensure!(
        actual == expected,
        crate::error::CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("{column} value {actual:?} does not match JSON value {expected:?}"),
        }
    );
    Ok(())
}

#[async_trait]
impl MessageQueueStore for SqliteStore {
    async fn enqueue_message(
        &self,
        queue: &str,
        payload: serde_json::Value,
    ) -> Result<MessageQueueItem> {
        self.ensure_writable()?;
        let item = MessageQueueItem::new(queue, payload, jiff::Timestamp::now());
        let mut connection = self.connect().await?;
        persist_message_queue_item(&mut connection, &self.database_path, &item).await?;
        Ok(item)
    }

    async fn dequeue_message(&self, queue: &str) -> Result<Option<MessageQueueItem>> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        dequeue_message_queue_item(&mut connection, &self.database_path, queue).await
    }

    async fn peek_message(&self, queue: &str) -> Result<Option<MessageQueueItem>> {
        let mut connection = self.connect().await?;
        Ok(
            load_queue_messages(&mut connection, &self.database_path, queue)
                .await?
                .into_iter()
                .next(),
        )
    }

    async fn list_queue_messages(&self, queue: &str) -> Result<Vec<MessageQueueItem>> {
        let mut connection = self.connect().await?;
        load_queue_messages(&mut connection, &self.database_path, queue).await
    }

    async fn list_message_queues(&self) -> Result<Vec<String>> {
        let mut connection = self.connect().await?;
        load_message_queue_names(&mut connection, &self.database_path).await
    }
}
