use std::collections::{HashMap, HashSet};
use std::path::Path;

use async_trait::async_trait;
use diesel::prelude::*;
use diesel::result::OptionalExtension;
use diesel::sql_types::{Bool, Nullable, Text};
use diesel_async::RunQueryDsl;
use snafu::prelude::*;

use super::branch::maybe_load_branch_head;
use super::codec::{parse_json_column, parse_session_role, parse_u64_column};
use super::{AsyncSqliteConnection, SqliteGraphStore, SqliteStore, SqliteTransactionError};
use crate::error::{
    AmbiguousNodePrefixSnafu, BranchNotFoundSnafu, CorruptedStoreSnafu, DuplicateMergeParentSnafu,
    MergeParentMatchesParentSnafu, MultipleShadowParentsSnafu, NotFoundSnafu, ParentNotFoundSnafu,
    ParseSqliteStoreValueSnafu, QuerySqliteStoreSnafu, RefsNotConnectedSnafu, StoreError,
};
use crate::schema::{
    node_anchor_prompt_attachments, node_anchor_session_patch_tools, node_anchor_session_patches,
    node_anchor_session_tools, node_anchor_sessions, node_anchor_skill_invocations,
    node_anchor_skill_results, node_metadata, node_relations, node_tool_results, node_tool_uses,
    nodes,
};
use crate::store::NodeStore;
use crate::{
    Anchor, AnchorPayload, AnchorPayloadKind, BackendMetadata, Kind, MergeParent, NewNode, Node,
    NodeMetadata, PromptAnchor, PromptAttachment, PromptImageAttachment, Role, SessionAnchor,
    SessionAnchorPatch, SkillInvocationAnchor, SkillInvocationMode, SkillResultAnchor,
    SkillRuntimeContext, StoreResult as Result, Tool, ToolResult, ToolUse,
};

mod read;
mod row;
mod write;

pub use read::{
    load_ancestry_nodes, load_child_nodes, load_log_nodes, load_node_by_exact_id,
    load_node_by_prefix_or_branch, load_root_id, node_count, resolve_ref_id, validate_new_node,
};
pub use row::*;
#[cfg(test)]
pub use write::node_storage_kind;
pub use write::{
    expected_node_metadata_rows, expected_node_tool_result_rows, expected_node_tool_use_rows,
    persist_node, persist_node_without_transaction, upsert_node_without_transaction,
};

const NODE_KIND_ANCHOR_SESSION: &str = "anchor_session";
const NODE_KIND_ANCHOR_SESSION_PATCH: &str = "anchor_session_patch";
const NODE_KIND_ANCHOR_PROMPT: &str = "anchor_prompt";
const NODE_KIND_ANCHOR_SKILL_INVOCATION: &str = "anchor_skill_invocation";
const NODE_KIND_ANCHOR_SKILL_RESULT: &str = "anchor_skill_result";

#[async_trait]
impl NodeStore for SqliteGraphStore {
    fn root_id(&self) -> String {
        self.root_id.clone()
    }

    async fn append(&self, _node: NewNode) -> Result<String> {
        self.ensure_read_only()
    }

    async fn ancestry(&self, head_ref: &str) -> Result<Vec<Node>> {
        let head_ref = head_ref.to_owned();
        let path = self.database_path.clone();
        self.with_connection(move |connection| {
            Box::pin(async move { load_ancestry_nodes(connection, &path, &head_ref).await })
        })
        .await
    }

    async fn log(&self, base_ref: &str, head_ref: &str) -> Result<Vec<Node>> {
        let base_ref = base_ref.to_owned();
        let head_ref = head_ref.to_owned();
        let path = self.database_path.clone();
        self.with_connection(move |connection| {
            Box::pin(async move { load_log_nodes(connection, &path, &base_ref, &head_ref).await })
        })
        .await
    }

    async fn get_node(&self, id: &str) -> Result<Node> {
        self.get_node_by_prefix_or_branch(id).await
    }

    async fn list_children(&self, node_id: &str) -> Result<Vec<Node>> {
        let node_id = node_id.to_owned();
        let path = self.database_path.clone();
        self.with_connection(move |connection| {
            Box::pin(async move { load_child_nodes(connection, &path, &node_id).await })
        })
        .await
    }
}

#[async_trait]
impl NodeStore for SqliteStore {
    fn root_id(&self) -> String {
        self.root_id.clone()
    }

    async fn append(&self, node: NewNode) -> Result<String> {
        self.ensure_writable()?;
        let node = Node::new(
            node.parent,
            node.role,
            node.metadata,
            node.kind,
            jiff::Timestamp::now(),
        );
        let _write = self.database.inner.write.lock().await;
        let mut connection = self.connect().await?;
        validate_new_node(&mut connection, &self.database_path, &node).await?;
        persist_node(&mut connection, &self.database_path, &node).await?;
        Ok(node.id)
    }

    async fn ancestry(&self, head_ref: &str) -> Result<Vec<Node>> {
        let mut connection = self.connect().await?;
        load_ancestry_nodes(&mut connection, &self.database_path, head_ref).await
    }

    async fn log(&self, base_ref: &str, head_ref: &str) -> Result<Vec<Node>> {
        let mut connection = self.connect().await?;
        load_log_nodes(&mut connection, &self.database_path, base_ref, head_ref).await
    }

    async fn get_node(&self, id: &str) -> Result<Node> {
        let mut connection = self.connect().await?;
        load_node_by_prefix_or_branch(&mut connection, &self.database_path, id).await
    }

    async fn list_children(&self, node_id: &str) -> Result<Vec<Node>> {
        let mut connection = self.connect().await?;
        load_child_nodes(&mut connection, &self.database_path, node_id).await
    }
}
