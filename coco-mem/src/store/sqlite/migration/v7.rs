use std::path::Path;

use diesel::prelude::*;
use diesel::sql_types::{Nullable, Text};
use diesel_async::RunQueryDsl;
use serde::de::DeserializeOwned;
use serde_json::Value;
use snafu::ResultExt;

use super::{current_schema_version, load_store_meta_bool, persist_store_meta_bool};
use crate::StoreResult as Result;
use crate::error::{CorruptedStoreSnafu, ParseSqliteStoreValueSnafu, QuerySqliteStoreSnafu};
use crate::schema::{node_metadata, node_tool_results, node_tool_uses, nodes};
use crate::{BackendMetadata, Kind, NodeMetadata, ToolResult, ToolUse};

use super::super::node::{
    NodeToolResultRow, NodeToolUseRow, expected_node_metadata_rows, expected_node_tool_result_rows,
    expected_node_tool_use_rows,
};
use super::super::{AsyncSqliteConnection, SqliteTransactionError};

pub const VERSION: i32 = 7;

pub const NODE_ITEM_ROWS_BACKFILL_META_KEY: &str = "node_items_backfilled";

#[derive(QueryableByName)]
struct NodeItemBackfillRow {
    #[diesel(sql_type = Text)]
    id: String,
    #[diesel(sql_type = Nullable<Text>)]
    metadata_json: Option<String>,
    #[diesel(sql_type = Text)]
    kind_json: String,
}

enum LegacyNodeMetadata {
    Missing,
    One(BackendMetadata),
    Many(NodeMetadata),
}

pub async fn backfill_node_item_rows(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<()> {
    if current_schema_version(connection, path).await? != Some(VERSION) {
        return Ok(());
    }
    if load_store_meta_bool(connection, path, NODE_ITEM_ROWS_BACKFILL_META_KEY).await? {
        return Ok(());
    }

    let total = nodes::table
        .count()
        .get_result::<i64>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    tracing::info!(
        path = %path.display(),
        total_nodes = total,
        "starting SQLite node item row backfill"
    );

    connection
        .immediate_transaction::<(), SqliteTransactionError, _>(async |connection| {
            diesel::delete(node_tool_uses::table)
                .execute(connection)
                .await
                .map_err(SqliteTransactionError::Query)?;
            diesel::delete(node_tool_results::table)
                .execute(connection)
                .await
                .map_err(SqliteTransactionError::Query)?;
            diesel::delete(node_metadata::table)
                .execute(connection)
                .await
                .map_err(SqliteTransactionError::Query)?;

            let rows =
                diesel::sql_query("SELECT id, metadata_json, kind_json FROM nodes ORDER BY id")
                    .load::<NodeItemBackfillRow>(connection)
                    .await
                    .map_err(SqliteTransactionError::Query)?;

            let mut processed = 0usize;
            for row in rows {
                backfill_node_item_row(connection, path, row)
                    .await
                    .map_err(SqliteTransactionError::Operation)?;
                processed += 1;
                if processed.is_multiple_of(1000) {
                    tracing::info!(
                        path = %path.display(),
                        processed_nodes = processed,
                        total_nodes = total,
                        "backfilled SQLite node item rows"
                    );
                }
            }

            persist_store_meta_bool(connection, path, NODE_ITEM_ROWS_BACKFILL_META_KEY, true)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            Ok(())
        })
        .await
        .map_err(|error| error.into_store_error(path))?;

    tracing::info!(
        path = %path.display(),
        total_nodes = total,
        "finished SQLite node item row backfill"
    );
    Ok(())
}

async fn backfill_node_item_row(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    row: NodeItemBackfillRow,
) -> Result<()> {
    let metadata = legacy_node_metadata(path, row.metadata_json.as_deref())?;
    let (kind_json, tool_use_rows, tool_result_rows, tool_item_count) =
        canonical_kind_json_and_tool_rows(path, &row.id, &row.kind_json)?;
    let metadata = metadata.into_node_metadata(tool_item_count);

    diesel::sql_query("UPDATE nodes SET kind_json = ? WHERE id = ?")
        .bind::<Text, _>(kind_json)
        .bind::<Text, _>(&row.id)
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    for metadata_row in expected_node_metadata_rows(&row.id, metadata.as_ref()) {
        diesel::insert_into(node_metadata::table)
            .values((
                node_metadata::node_id.eq(metadata_row.node_id),
                node_metadata::ordinal.eq(metadata_row.ordinal),
                node_metadata::execution_id.eq(metadata_row.execution_id),
                node_metadata::call_id.eq(metadata_row.call_id),
            ))
            .execute(connection)
            .await
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?;
    }

    for tool_use_row in tool_use_rows {
        diesel::insert_into(node_tool_uses::table)
            .values((
                node_tool_uses::node_id.eq(tool_use_row.node_id),
                node_tool_uses::ordinal.eq(tool_use_row.ordinal),
                node_tool_uses::tool_use_id.eq(tool_use_row.tool_use_id),
                node_tool_uses::name.eq(tool_use_row.name),
                node_tool_uses::input_json.eq(tool_use_row.input_json),
            ))
            .execute(connection)
            .await
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?;
    }

    for tool_result_row in tool_result_rows {
        diesel::insert_into(node_tool_results::table)
            .values((
                node_tool_results::node_id.eq(tool_result_row.node_id),
                node_tool_results::ordinal.eq(tool_result_row.ordinal),
                node_tool_results::tool_result_id.eq(tool_result_row.tool_result_id),
                node_tool_results::output.eq(tool_result_row.output),
            ))
            .execute(connection)
            .await
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?;
    }

    Ok(())
}

fn legacy_node_metadata(path: &Path, metadata_json: Option<&str>) -> Result<LegacyNodeMetadata> {
    let Some(metadata_json) = metadata_json else {
        return Ok(LegacyNodeMetadata::Missing);
    };
    let value =
        serde_json::from_str::<Value>(metadata_json).context(ParseSqliteStoreValueSnafu {
            path: path.to_owned(),
            column: "nodes.metadata_json".to_owned(),
        })?;
    match value {
        Value::Object(_) => serde_json::from_value(value)
            .map(LegacyNodeMetadata::One)
            .context(ParseSqliteStoreValueSnafu {
                path: path.to_owned(),
                column: "nodes.metadata_json".to_owned(),
            }),
        Value::Array(items) => items
            .into_iter()
            .map(|value| {
                serde_json::from_value(value).context(ParseSqliteStoreValueSnafu {
                    path: path.to_owned(),
                    column: "nodes.metadata_json".to_owned(),
                })
            })
            .collect::<Result<_>>()
            .map(LegacyNodeMetadata::Many),
        _ => CorruptedStoreSnafu {
            path: path.to_owned(),
            message: "SQLite nodes.metadata_json must be an object or array for migration"
                .to_owned(),
        }
        .fail(),
    }
}

impl LegacyNodeMetadata {
    fn into_node_metadata(self, tool_item_count: usize) -> Option<NodeMetadata> {
        match self {
            Self::Missing => None,
            Self::One(metadata) => Some(vec![metadata; tool_item_count.max(1)]),
            Self::Many(metadata) => Some(metadata),
        }
    }
}

fn canonical_kind_json_and_tool_rows(
    path: &Path,
    node_id: &str,
    kind_json: &str,
) -> Result<(String, Vec<NodeToolUseRow>, Vec<NodeToolResultRow>, usize)> {
    let value = serde_json::from_str::<Value>(kind_json).context(ParseSqliteStoreValueSnafu {
        path: path.to_owned(),
        column: "nodes.kind_json".to_owned(),
    })?;

    if let Some(payload) = value.get("ToolUse") {
        let tool_uses =
            legacy_one_or_many_items::<ToolUse>(path, "nodes.kind_json.ToolUse", payload.clone())?;
        let kind = Kind::tool_use_items(tool_uses);
        let kind_json = legacy_node_kind_residual_json(&kind, path)?;
        let rows = expected_node_tool_use_rows(node_id, &kind, path)?;
        let item_count = rows.len();
        return Ok((kind_json, rows, Vec::new(), item_count));
    }

    if let Some(payload) = value.get("ToolResult") {
        let tool_results = legacy_one_or_many_items::<ToolResult>(
            path,
            "nodes.kind_json.ToolResult",
            payload.clone(),
        )?;
        let kind = Kind::tool_result_items(tool_results);
        let kind_json = legacy_node_kind_residual_json(&kind, path)?;
        let rows = expected_node_tool_result_rows(node_id, &kind);
        let item_count = rows.len();
        return Ok((kind_json, Vec::new(), rows, item_count));
    }

    Ok((kind_json.to_owned(), Vec::new(), Vec::new(), 1))
}

fn legacy_one_or_many_items<T>(path: &Path, column: &str, value: Value) -> Result<Vec<T>>
where
    T: DeserializeOwned,
{
    match value {
        Value::Array(items) => items
            .into_iter()
            .map(|value| {
                serde_json::from_value(value).context(ParseSqliteStoreValueSnafu {
                    path: path.to_owned(),
                    column: column.to_owned(),
                })
            })
            .collect(),
        Value::Object(_) => serde_json::from_value(value)
            .map(|item| vec![item])
            .context(ParseSqliteStoreValueSnafu {
                path: path.to_owned(),
                column: column.to_owned(),
            }),
        _ => CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("SQLite {column} must be an object or array for migration"),
        }
        .fail(),
    }
}

fn legacy_node_kind_residual_json(kind: &Kind, path: &Path) -> Result<String> {
    let residual = match kind {
        Kind::ToolUse(_) => Kind::tool_use_items(Vec::new()),
        Kind::ToolResult(_) => Kind::tool_result_items(Vec::new()),
        _ => kind.clone(),
    };
    serde_json::to_string(&residual).context(ParseSqliteStoreValueSnafu {
        path: path.to_owned(),
        column: "nodes.kind_json".to_owned(),
    })
}
