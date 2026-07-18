use super::{
    MessageQueueItem, NodeAnchorPromptAttachmentRow, NodeAnchorSessionPatchRow,
    NodeAnchorSessionPatchToolRow, NodeAnchorSessionRow, NodeAnchorSessionToolRow,
    NodeAnchorSkillInvocationRow, NodeAnchorSkillResultRow, NodeMetadataRow, NodeToolResultRow,
    NodeToolUseRow, SqliteGraphStore, SqliteStore,
};
use crate::schema::{
    jobs, node_anchor_prompt_attachments, node_anchor_session_patch_tools,
    node_anchor_session_patches, node_anchor_session_tools, node_anchor_sessions,
    node_anchor_skill_invocations, node_anchor_skill_results, node_metadata, node_relations,
    node_tool_results, node_tool_uses, nodes, preset_version_tools, preset_versions, sessions,
    skill_version_scripts, skill_versions,
};
use crate::{
    Anchor, BackendMetadata, BranchStore, GRAPH_READ_BATCH_SIZE, GraphBranchRecord,
    GraphChildPageCursor, GraphMutationBranchChangeKind, GraphMutationRevisionBounds, Job,
    JobStatus, JobStore, Kind, MergeParent, MessageQueueStore, NewNode, NewNodeContent, Node,
    NodeStore, PauseReason, Preset, PresetStore, PromptAnchor, PromptAttachment,
    PromptImageAttachment, Role, SessionAnchor, SessionAnchorPatch, SessionRole, SessionState,
    SessionStore, SkillInvocationAnchor, SkillInvocationMode, SkillResultAnchor,
    SkillRuntimeContext, SkillScript, SkillStore, SkillUpdatePatch, SkillVersionSpec, StoreError,
    Tool, ToolResult, ToolUse,
};
use diesel::connection::InstrumentationEvent;
use diesel::prelude::*;
use diesel_async::{AsyncConnection, RunQueryDsl};
use std::collections::{BTreeSet, HashSet};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;
use tokio::sync::{Barrier, oneshot};

mod migration;

#[derive(diesel::Queryable, Debug, PartialEq, Eq)]
struct NodeRelationRow {
    child_node_id: String,
    parent_node_id: String,
    kind: String,
    ordinal: i32,
}

#[derive(diesel::Queryable, Debug, PartialEq, Eq)]
struct SessionSummaryRow {
    state: String,
    target_branch: Option<String>,
    base_head_id: Option<String>,
    pause_reason: Option<String>,
    merged_anchor_id: Option<String>,
}

#[derive(diesel::Queryable, Debug, PartialEq, Eq)]
struct JobSummaryRow {
    created_at: String,
    finished_at: Option<String>,
    branch: String,
    work_branch: String,
    base: String,
    status: String,
}

fn session_anchor_node(parent: &str) -> NewNode {
    NewNode {
        parent: parent.to_owned(),
        role: Role::System,
        metadata: None,
        kind: Kind::Anchor(Anchor::session(
            vec![],
            SessionAnchor {
                role: SessionRole::Orchestrator,
                provider_profile: None,
                provider: Some("openai".to_owned()),
                model: "gpt-5.4".to_owned(),
                tools: vec![],
                system_prompt: "system".to_owned(),
                prompt: "prompt".to_owned(),
                temperature: Some(0.1),
                max_tokens: Some(64),
                additional_params: None,
                enable_coco_shim: false,
                active_skill: None,
            },
        )),
    }
}

fn rich_session_anchor_node(parent: &str, merge_parents: Vec<MergeParent>) -> NewNode {
    NewNode {
        parent: parent.to_owned(),
        role: Role::System,
        metadata: None,
        kind: Kind::Anchor(Anchor::session(
            merge_parents,
            SessionAnchor {
                role: SessionRole::Runner,
                provider_profile: Some("runner-profile".to_owned()),
                provider: Some("openai".to_owned()),
                model: "gpt-5.4".to_owned(),
                tools: vec![
                    Tool {
                        name: "lookup".to_owned(),
                        description: "Look up a value".to_owned(),
                        input_schema: serde_json::json!({
                            "type": "object",
                            "properties": {"key": {"type": "string"}}
                        }),
                    },
                    Tool {
                        name: "finish".to_owned(),
                        description: "Finish the task".to_owned(),
                        input_schema: serde_json::json!({"type": "object"}),
                    },
                ],
                system_prompt: "system".to_owned(),
                prompt: "session prompt".to_owned(),
                temperature: Some(0.25),
                max_tokens: Some(u64::MAX),
                additional_params: Some(serde_json::json!({
                    "reasoning": {"effort": "high"}
                })),
                enable_coco_shim: true,
                active_skill: Some(SkillRuntimeContext {
                    name: "compact".to_owned(),
                    handoff: Some("Preserve the decisions".to_owned()),
                }),
            },
        )),
    }
}

fn rich_session_patch_anchor_node(parent: &str, merge_parents: Vec<MergeParent>) -> NewNode {
    NewNode {
        parent: parent.to_owned(),
        role: Role::System,
        metadata: None,
        kind: Kind::Anchor(Anchor::session_patch(
            merge_parents,
            SessionAnchorPatch {
                role: Some(SessionRole::Runner),
                provider_profile: Some(Some("runner-profile".to_owned())),
                provider: Some(Some("openai".to_owned())),
                model: Some("gpt-5.4".to_owned()),
                tools: Some(vec![Tool {
                    name: "lookup".to_owned(),
                    description: "Look up a value".to_owned(),
                    input_schema: serde_json::json!({"type": "object"}),
                }]),
                system_prompt: Some("patched system".to_owned()),
                temperature: Some(Some(0.5)),
                max_tokens: Some(Some(u64::MAX)),
                additional_params: Some(Some(serde_json::json!("priority"))),
                enable_coco_shim: Some(true),
            },
        )),
    }
}

fn rich_prompt_anchor_node(parent: &str, merge_parents: Vec<MergeParent>) -> NewNode {
    NewNode {
        parent: parent.to_owned(),
        role: Role::User,
        metadata: None,
        kind: Kind::Anchor(Anchor::prompt(
            merge_parents,
            PromptAnchor {
                prompt: "Inspect these images".to_owned(),
                attachments: vec![
                    PromptAttachment::Image(PromptImageAttachment {
                        id: "image-a".to_owned(),
                        width: Some(u32::MAX),
                        height: Some(1080),
                        file_size: Some(u64::MAX),
                        media_type: Some("image/png".to_owned()),
                    }),
                    PromptAttachment::Image(PromptImageAttachment {
                        id: "image-b".to_owned(),
                        width: None,
                        height: None,
                        file_size: None,
                        media_type: None,
                    }),
                ],
            },
        )),
    }
}

fn skill_invocation_anchor_node(parent: &str, merge_parents: Vec<MergeParent>) -> NewNode {
    NewNode {
        parent: parent.to_owned(),
        role: Role::System,
        metadata: None,
        kind: Kind::Anchor(Anchor::skill_invocation(
            merge_parents,
            SkillInvocationAnchor {
                skill_name: "compact".to_owned(),
                mode: SkillInvocationMode::Handoff {
                    prompt: "Compact this branch".to_owned(),
                },
            },
        )),
    }
}

fn skill_result_anchor_node(parent: &str, merge_parents: Vec<MergeParent>) -> NewNode {
    NewNode {
        parent: parent.to_owned(),
        role: Role::System,
        metadata: None,
        kind: Kind::Anchor(Anchor::skill_result(
            merge_parents,
            SkillResultAnchor {
                skill_name: "compact".to_owned(),
                output: "First line\nSecond line with \"quotes\"".to_owned(),
            },
        )),
    }
}

fn preset(model: &str) -> Preset {
    Preset {
        role: SessionRole::Orchestrator,
        provider_profile: "openai".to_owned(),
        model: model.to_owned(),
        tools: vec![],
        system_prompt: "system".to_owned(),
        prompt: "prompt".to_owned(),
        temperature: Some(0.1),
        max_tokens: Some(64),
        additional_params: None,
        enable_coco_shim: false,
    }
}

async fn node_relation_rows(store: &SqliteStore, child_node_id: &str) -> Vec<NodeRelationRow> {
    let mut connection = store.connect().await.unwrap();
    node_relations::table
        .filter(node_relations::child_node_id.eq(child_node_id))
        .select((
            node_relations::child_node_id,
            node_relations::parent_node_id,
            node_relations::kind,
            node_relations::ordinal,
        ))
        .order((
            node_relations::kind,
            node_relations::ordinal,
            node_relations::parent_node_id,
        ))
        .load::<NodeRelationRow>(&mut connection)
        .await
        .unwrap()
}

async fn node_metadata_rows(store: &SqliteStore, node_id: &str) -> Vec<NodeMetadataRow> {
    let mut connection = store.connect().await.unwrap();
    node_metadata::table
        .filter(node_metadata::node_id.eq(node_id))
        .select((
            node_metadata::node_id,
            node_metadata::ordinal,
            node_metadata::execution_id,
            node_metadata::call_id,
        ))
        .order(node_metadata::ordinal)
        .load::<NodeMetadataRow>(&mut connection)
        .await
        .unwrap()
}

async fn node_tool_use_rows(store: &SqliteStore, node_id: &str) -> Vec<NodeToolUseRow> {
    let mut connection = store.connect().await.unwrap();
    node_tool_uses::table
        .filter(node_tool_uses::node_id.eq(node_id))
        .select((
            node_tool_uses::node_id,
            node_tool_uses::ordinal,
            node_tool_uses::tool_use_id,
            node_tool_uses::name,
            node_tool_uses::input_json,
        ))
        .order(node_tool_uses::ordinal)
        .load::<NodeToolUseRow>(&mut connection)
        .await
        .unwrap()
}

async fn node_tool_result_rows(store: &SqliteStore, node_id: &str) -> Vec<NodeToolResultRow> {
    let mut connection = store.connect().await.unwrap();
    node_tool_results::table
        .filter(node_tool_results::node_id.eq(node_id))
        .select((
            node_tool_results::node_id,
            node_tool_results::ordinal,
            node_tool_results::tool_result_id,
            node_tool_results::output,
        ))
        .order(node_tool_results::ordinal)
        .load::<NodeToolResultRow>(&mut connection)
        .await
        .unwrap()
}

async fn node_anchor_session_row(store: &SqliteStore, node_id: &str) -> NodeAnchorSessionRow {
    let mut connection = store.connect().await.unwrap();
    node_anchor_sessions::table
        .filter(node_anchor_sessions::node_id.eq(node_id))
        .select((
            node_anchor_sessions::node_id,
            node_anchor_sessions::role,
            node_anchor_sessions::provider_profile,
            node_anchor_sessions::provider,
            node_anchor_sessions::model,
            node_anchor_sessions::system_prompt,
            node_anchor_sessions::prompt,
            node_anchor_sessions::temperature,
            node_anchor_sessions::max_tokens,
            node_anchor_sessions::additional_params_json,
            node_anchor_sessions::enable_coco_shim,
            node_anchor_sessions::active_skill_name,
            node_anchor_sessions::active_skill_handoff,
        ))
        .get_result::<NodeAnchorSessionRow>(&mut connection)
        .await
        .unwrap()
}

async fn node_anchor_session_tool_rows(
    store: &SqliteStore,
    node_id: &str,
) -> Vec<NodeAnchorSessionToolRow> {
    let mut connection = store.connect().await.unwrap();
    node_anchor_session_tools::table
        .filter(node_anchor_session_tools::node_id.eq(node_id))
        .select((
            node_anchor_session_tools::node_id,
            node_anchor_session_tools::ordinal,
            node_anchor_session_tools::name,
            node_anchor_session_tools::description,
            node_anchor_session_tools::input_schema_json,
        ))
        .order(node_anchor_session_tools::ordinal)
        .load::<NodeAnchorSessionToolRow>(&mut connection)
        .await
        .unwrap()
}

async fn node_anchor_session_patch_row(
    store: &SqliteStore,
    node_id: &str,
) -> NodeAnchorSessionPatchRow {
    let mut connection = store.connect().await.unwrap();
    node_anchor_session_patches::table
        .filter(node_anchor_session_patches::node_id.eq(node_id))
        .select((
            node_anchor_session_patches::node_id,
            node_anchor_session_patches::role,
            node_anchor_session_patches::provider_profile_present,
            node_anchor_session_patches::provider_profile,
            node_anchor_session_patches::provider_present,
            node_anchor_session_patches::provider,
            node_anchor_session_patches::model,
            node_anchor_session_patches::tools_present,
            node_anchor_session_patches::system_prompt,
            node_anchor_session_patches::temperature_present,
            node_anchor_session_patches::temperature,
            node_anchor_session_patches::max_tokens_present,
            node_anchor_session_patches::max_tokens,
            node_anchor_session_patches::additional_params_present,
            node_anchor_session_patches::additional_params_json,
            node_anchor_session_patches::enable_coco_shim,
        ))
        .get_result::<NodeAnchorSessionPatchRow>(&mut connection)
        .await
        .unwrap()
}

async fn node_anchor_session_patch_tool_rows(
    store: &SqliteStore,
    node_id: &str,
) -> Vec<NodeAnchorSessionPatchToolRow> {
    let mut connection = store.connect().await.unwrap();
    node_anchor_session_patch_tools::table
        .filter(node_anchor_session_patch_tools::node_id.eq(node_id))
        .select((
            node_anchor_session_patch_tools::node_id,
            node_anchor_session_patch_tools::ordinal,
            node_anchor_session_patch_tools::name,
            node_anchor_session_patch_tools::description,
            node_anchor_session_patch_tools::input_schema_json,
        ))
        .order(node_anchor_session_patch_tools::ordinal)
        .load::<NodeAnchorSessionPatchToolRow>(&mut connection)
        .await
        .unwrap()
}

async fn node_anchor_prompt_attachment_rows(
    store: &SqliteStore,
    node_id: &str,
) -> Vec<NodeAnchorPromptAttachmentRow> {
    let mut connection = store.connect().await.unwrap();
    node_anchor_prompt_attachments::table
        .filter(node_anchor_prompt_attachments::node_id.eq(node_id))
        .select((
            node_anchor_prompt_attachments::node_id,
            node_anchor_prompt_attachments::ordinal,
            node_anchor_prompt_attachments::kind,
            node_anchor_prompt_attachments::attachment_id,
            node_anchor_prompt_attachments::width,
            node_anchor_prompt_attachments::height,
            node_anchor_prompt_attachments::file_size,
            node_anchor_prompt_attachments::media_type,
        ))
        .order(node_anchor_prompt_attachments::ordinal)
        .load::<NodeAnchorPromptAttachmentRow>(&mut connection)
        .await
        .unwrap()
}

async fn node_anchor_skill_invocation_row(
    store: &SqliteStore,
    node_id: &str,
) -> NodeAnchorSkillInvocationRow {
    let mut connection = store.connect().await.unwrap();
    node_anchor_skill_invocations::table
        .filter(node_anchor_skill_invocations::node_id.eq(node_id))
        .select((
            node_anchor_skill_invocations::node_id,
            node_anchor_skill_invocations::skill_name,
            node_anchor_skill_invocations::mode,
            node_anchor_skill_invocations::prompt,
        ))
        .get_result::<NodeAnchorSkillInvocationRow>(&mut connection)
        .await
        .unwrap()
}

async fn node_anchor_skill_result_row(
    store: &SqliteStore,
    node_id: &str,
) -> NodeAnchorSkillResultRow {
    let mut connection = store.connect().await.unwrap();
    node_anchor_skill_results::table
        .filter(node_anchor_skill_results::node_id.eq(node_id))
        .select((
            node_anchor_skill_results::node_id,
            node_anchor_skill_results::skill_name,
            node_anchor_skill_results::output,
        ))
        .get_result::<NodeAnchorSkillResultRow>(&mut connection)
        .await
        .unwrap()
}

async fn node_kind(store: &SqliteStore, node_id: &str) -> String {
    let mut connection = store.connect().await.unwrap();
    nodes::table
        .filter(nodes::id.eq(node_id))
        .select(nodes::kind)
        .get_result::<String>(&mut connection)
        .await
        .unwrap()
}

async fn node_content(store: &SqliteStore, node_id: &str) -> Option<String> {
    let mut connection = store.connect().await.unwrap();
    nodes::table
        .filter(nodes::id.eq(node_id))
        .select(nodes::content)
        .get_result::<Option<String>>(&mut connection)
        .await
        .unwrap()
}

fn valid_job_row() -> super::JobRow {
    super::JobRow {
        job_id: "job-test".to_owned(),
        created_at: "2026-01-01T00:00:00Z".to_owned(),
        finished_at: None,
        branch: "main".to_owned(),
        work_branch: "main".to_owned(),
        base: "base".to_owned(),
        status: "queued".to_owned(),
    }
}

async fn session_summary(store: &SqliteStore, branch: &str) -> SessionSummaryRow {
    let mut connection = store.connect().await.unwrap();
    sessions::table
        .filter(sessions::branch_name.eq(branch))
        .select((
            sessions::state,
            sessions::target_branch,
            sessions::base_head_id,
            sessions::pause_reason,
            sessions::merged_anchor_id,
        ))
        .get_result::<SessionSummaryRow>(&mut connection)
        .await
        .unwrap()
}

async fn job_summary(store: &SqliteStore, job_id: &str) -> JobSummaryRow {
    let mut connection = store.connect().await.unwrap();
    jobs::table
        .filter(jobs::job_id.eq(job_id))
        .select((
            jobs::created_at,
            jobs::finished_at,
            jobs::branch,
            jobs::work_branch,
            jobs::base,
            jobs::status,
        ))
        .get_result::<JobSummaryRow>(&mut connection)
        .await
        .unwrap()
}

#[test]
fn job_row_rejects_empty_work_branch() {
    let mut row = valid_job_row();
    row.work_branch.clear();

    let error = super::job_row_into_job(std::path::Path::new("store.sqlite3"), row)
        .expect_err("empty work branch must fail");

    assert!(error.to_string().contains("empty work branch"));
}

#[test]
fn session_row_rejects_inconsistent_relational_state() {
    let row = super::SessionRow {
        branch_name: "main".to_owned(),
        state: "attached".to_owned(),
        target_branch: Some("base".to_owned()),
        base_head_id: None,
        pause_reason: None,
        merged_anchor_id: None,
    };

    let error = super::session_row_into_state(std::path::Path::new("store.sqlite3"), row)
        .expect_err("inconsistent session row must fail");

    assert!(error.to_string().contains("invalid SQLite session row"));
}

#[test]
fn job_row_rejects_invalid_timestamp() {
    let mut row = valid_job_row();
    row.created_at = "invalid".to_owned();

    let error = super::job_row_into_job(std::path::Path::new("store.sqlite3"), row)
        .expect_err("invalid timestamp must fail");

    assert!(error.to_string().contains("invalid SQLite job timestamp"));
}

#[test]
fn job_row_rejects_invalid_status() {
    let mut row = valid_job_row();
    row.status = "invalid".to_owned();

    let error = super::job_row_into_job(std::path::Path::new("store.sqlite3"), row)
        .expect_err("invalid status must fail");

    assert!(error.to_string().contains("invalid SQLite job status"));
}

#[tokio::test]
async fn open_temporary_removes_directory_after_last_store_drop() {
    let store = SqliteStore::open_temporary().await.unwrap();
    let path = store.store_path().to_owned();
    let clone = store.clone();

    assert!(path.exists());
    drop(store);
    assert!(path.exists());
    drop(clone);
    assert!(!path.exists());
}

#[tokio::test]
async fn cloned_sqlite_store_shares_database_instance() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");

    let store = SqliteStore::open(&path).await.unwrap();
    let cloned = store.clone();

    assert!(std::ptr::eq(
        store.database.shared_pool(),
        cloned.database.shared_pool()
    ));
}

#[tokio::test]
async fn reopened_sqlite_handles_share_database_instance() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");

    let store = SqliteStore::open(&path).await.unwrap();
    let read_only = SqliteStore::open_read_only(&path).await.unwrap();
    let graph = SqliteGraphStore::open_read_only(&path).await.unwrap();
    let lexical_read_only = SqliteStore::open_read_only(path.join(".")).await.unwrap();

    assert!(std::ptr::eq(
        store.database.shared_pool(),
        read_only.database.shared_pool()
    ));
    assert!(std::ptr::eq(
        store.database.shared_pool(),
        graph.database.shared_pool()
    ));
    assert!(std::ptr::eq(
        store.database.shared_pool(),
        lexical_read_only.database.shared_pool()
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_first_opens_share_one_initialized_database() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let barrier = Arc::new(Barrier::new(8));
    let handles = (0..8)
        .map(|_| {
            let barrier = barrier.clone();
            let path = path.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                SqliteStore::open(path).await.unwrap()
            })
        })
        .collect::<Vec<_>>();

    let mut stores = Vec::new();
    for handle in handles {
        stores.push(handle.await.unwrap());
    }

    let root_id = stores[0].root_id();
    assert!(stores.iter().all(|store| store.root_id() == root_id));
    assert!(stores.iter().all(|store| std::ptr::eq(
        stores[0].database.shared_pool(),
        store.database.shared_pool()
    )));
    let mut connection = stores[0].connect().await.unwrap();
    assert_eq!(
        nodes::table
            .count()
            .get_result::<i64>(&mut connection)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn graph_store_connection_contention_does_not_block_writer() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let graph = SqliteGraphStore::open_read_only(&path).await.unwrap();
    let graph_connection_database = graph.database.clone();
    let (graph_locked_tx, graph_locked_rx) = mpsc::channel();
    let (release_graph_tx, release_graph_rx) = oneshot::channel();
    let graph_lock = tokio::spawn(async move {
        let _connection = graph_connection_database.acquire().await.unwrap();
        graph_locked_tx.send(()).unwrap();
        release_graph_rx.await.unwrap();
    });
    graph_locked_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("graph connection lock should be held");

    let writer = store.clone();
    let root = store.root_id();
    let (write_tx, write_rx) = oneshot::channel();
    let write = tokio::spawn(async move {
        let node = writer
            .append(NewNode {
                parent: root,
                role: Role::User,
                metadata: None,
                kind: Kind::Text("write while graph rebuild holds its connection".to_owned()),
            })
            .await
            .unwrap();
        write_tx.send(node).unwrap();
    });

    let written = tokio::time::timeout(Duration::from_secs(1), write_rx)
        .await
        .expect("writer should not wait for graph connection release")
        .unwrap();
    release_graph_tx.send(()).unwrap();
    graph_lock.await.unwrap();
    write.await.unwrap();
    assert_eq!(store.get_node(&written).await.unwrap().id, written);
}

#[tokio::test(flavor = "multi_thread")]
async fn graph_read_batch_releases_its_connection_before_writer_runs() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let graph = SqliteGraphStore::open_read_only(&path).await.unwrap();
    assert!(std::ptr::eq(
        store.database.shared_pool(),
        graph.database.shared_pool()
    ));

    let mut held_connections = Vec::new();
    for _ in 0..3 {
        held_connections.push(graph.database.acquire().await.unwrap());
    }

    let root = store.root_id();
    tokio::time::timeout(
        Duration::from_secs(1),
        graph.graph_nodes_by_ids(std::slice::from_ref(&root)),
    )
    .await
    .expect("graph read should acquire the remaining pool connection")
    .unwrap();

    let written = tokio::time::timeout(
        Duration::from_secs(1),
        store.append(NewNode {
            parent: root,
            role: Role::User,
            metadata: None,
            kind: Kind::Text("write between graph read batches".to_owned()),
        }),
    )
    .await
    .expect("writer should reuse the connection released by the graph read batch")
    .unwrap();

    drop(held_connections);
    assert_eq!(store.get_node(&written).await.unwrap().id, written);
}

#[tokio::test]
async fn graph_read_api_loads_branches_nodes_and_children_in_bounded_calls() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let root = store.root_id();
    store.fork("main", &root).await.unwrap();
    let child = store
        .append(NewNode {
            parent: root.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("graph read batch".to_owned()),
        })
        .await
        .unwrap();
    store.set_branch_head("main", &root, &child).await.unwrap();
    let graph = SqliteGraphStore::open_read_only(&path).await.unwrap();

    assert_eq!(
        graph.graph_branches().await.unwrap(),
        vec![GraphBranchRecord {
            name: "main".to_owned(),
            head_id: child.clone(),
            state: SessionState::Active,
        }]
    );
    let nodes = graph
        .graph_nodes_by_ids(&[root.clone(), child.clone()])
        .await
        .unwrap();
    assert_eq!(
        nodes
            .into_iter()
            .map(|node| node.id)
            .collect::<HashSet<_>>(),
        HashSet::from([root.clone(), child.clone()])
    );
    assert_eq!(
        graph
            .graph_child_ids(std::slice::from_ref(&root))
            .await
            .unwrap()
            .get(&root),
        Some(&vec![child])
    );
}

#[tokio::test]
async fn graph_branch_pages_are_ordered_and_mark_an_exact_final_page_complete() {
    let store = SqliteStore::open_temporary().await.unwrap();
    let root = store.root_id();
    for index in 0..4 {
        let name = format!("branch-{index:02}");
        store.fork(&name, &root).await.unwrap();
    }
    let graph = SqliteGraphStore::open_read_only(store.store_path())
        .await
        .unwrap();
    let high_watermark = graph
        .graph_branch_name_high_watermark()
        .await
        .unwrap()
        .unwrap();
    let page_size = NonZeroUsize::new(2).unwrap();

    let first = graph
        .graph_branches_page(None, &high_watermark, page_size)
        .await
        .unwrap();
    assert!(!first.complete);
    assert_eq!(
        first
            .branches
            .iter()
            .map(|branch| branch.name.as_str())
            .collect::<Vec<_>>(),
        vec!["branch-00", "branch-01"]
    );
    assert_eq!(
        first
            .next_cursor
            .as_ref()
            .map(|cursor| cursor.name.as_str()),
        Some("branch-01")
    );

    let second = graph
        .graph_branches_page(first.next_cursor.as_ref(), &high_watermark, page_size)
        .await
        .unwrap();
    assert!(second.complete);
    assert!(second.next_cursor.is_none());
    assert_eq!(
        second
            .branches
            .iter()
            .map(|branch| branch.name.as_str())
            .collect::<Vec<_>>(),
        vec!["branch-02", "branch-03"]
    );

    let names = first
        .branches
        .into_iter()
        .chain(second.branches)
        .map(|branch| branch.name)
        .collect::<Vec<_>>();
    assert!(names.is_sorted());
    assert_eq!(names.iter().collect::<HashSet<_>>().len(), names.len());
}

#[tokio::test]
async fn graph_child_ids_page_is_stable_across_high_fan_out_and_relation_kinds() {
    let store = SqliteStore::open_temporary().await.unwrap();
    let root = store.root_id();
    let alternate_parent = store
        .append(NewNode {
            parent: root.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("alternate parent".to_owned()),
        })
        .await
        .unwrap();
    let mut expected_ids = vec![alternate_parent.clone()];
    for index in 0..=GRAPH_READ_BATCH_SIZE {
        expected_ids.push(
            store
                .append(NewNode {
                    parent: root.clone(),
                    role: Role::User,
                    metadata: None,
                    kind: Kind::Text(format!("fan-out child {index}")),
                })
                .await
                .unwrap(),
        );
    }
    let merge_child = store
        .append(rich_session_anchor_node(
            &alternate_parent,
            vec![MergeParent::merge(root.clone())],
        ))
        .await
        .unwrap();
    let shadow_child = store
        .append(rich_session_anchor_node(
            &alternate_parent,
            vec![MergeParent::shadow(root.clone())],
        ))
        .await
        .unwrap();
    expected_ids.extend([merge_child.clone(), shadow_child.clone()]);

    let early_id = expected_ids[0].clone();
    let late_id = expected_ids[1].clone();
    let duplicate_id = expected_ids[2].clone();
    let early_created_at = "2026-01-01T00:00:00Z";
    let shared_created_at = "2026-01-01T00:00:01Z";
    let late_created_at = "2026-01-01T00:00:02Z";
    let mut connection = store.connect().await.unwrap();
    diesel::update(nodes::table.filter(nodes::id.eq_any(&expected_ids)))
        .set(nodes::created_at.eq(shared_created_at))
        .execute(&mut connection)
        .await
        .unwrap();
    diesel::update(nodes::table.filter(nodes::id.eq(&early_id)))
        .set(nodes::created_at.eq(early_created_at))
        .execute(&mut connection)
        .await
        .unwrap();
    diesel::update(nodes::table.filter(nodes::id.eq(&late_id)))
        .set(nodes::created_at.eq(late_created_at))
        .execute(&mut connection)
        .await
        .unwrap();
    let duplicate_created_revision = node_relations::table
        .filter(node_relations::child_node_id.eq(&duplicate_id))
        .filter(node_relations::parent_node_id.eq(&root))
        .select(node_relations::created_revision)
        .first::<i64>(&mut connection)
        .await
        .unwrap();
    diesel::insert_into(node_relations::table)
        .values((
            node_relations::child_node_id.eq(&duplicate_id),
            node_relations::parent_node_id.eq(&root),
            node_relations::kind.eq("merge"),
            node_relations::ordinal.eq(0),
            node_relations::created_revision.eq(duplicate_created_revision),
        ))
        .execute(&mut connection)
        .await
        .unwrap();
    drop(connection);

    let expected = expected_ids;
    let graph = SqliteGraphStore::open_read_only(store.store_path())
        .await
        .unwrap();
    let page_size = NonZeroUsize::new(17).unwrap();
    let mut cursor = None;
    let mut actual = Vec::new();
    loop {
        let page = graph
            .graph_child_ids_page(&root, cursor.as_ref(), page_size)
            .await
            .unwrap();
        assert!(page.child_ids.len() <= page_size.get());
        if page.complete {
            assert!(page.next_cursor.is_none());
        } else {
            assert_eq!(page.child_ids.len(), page_size.get());
            assert_eq!(
                page.next_cursor.as_ref().map(|cursor| &cursor.node_id),
                page.child_ids.last()
            );
        }
        actual.extend(page.child_ids);
        cursor = page.next_cursor;
        if page.complete {
            break;
        }
    }

    assert!(actual.contains(&merge_child));
    assert!(actual.contains(&shadow_child));
    assert_eq!(actual.iter().collect::<HashSet<_>>().len(), actual.len());
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn graph_relation_revision_excludes_backdated_children_inserted_after_cutoff() {
    let store = SqliteStore::open_temporary().await.unwrap();
    let root_id = store.root_id();
    let initial_revision = SqliteGraphStore::open_read_only(store.store_path())
        .await
        .unwrap()
        .graph_relation_revision()
        .await
        .unwrap();
    assert_eq!(initial_revision, 1);
    let first_id = store
        .append(NewNode {
            parent: root_id.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("first child".to_owned()),
        })
        .await
        .unwrap();
    let mut connection = store.connect().await.unwrap();
    diesel::update(nodes::table.filter(nodes::id.eq(&first_id)))
        .set(nodes::created_at.eq("2026-01-01T00:00:02Z"))
        .execute(&mut connection)
        .await
        .unwrap();
    drop(connection);

    let graph = SqliteGraphStore::open_read_only(store.store_path())
        .await
        .unwrap();
    let frozen_revision = graph.graph_relation_revision().await.unwrap();
    assert_eq!(frozen_revision, initial_revision + 1);
    let frozen_high_watermark = graph
        .graph_child_high_watermark_at_revision(&root_id, frozen_revision)
        .await
        .unwrap()
        .unwrap();

    let backdated_id = store
        .append(NewNode {
            parent: root_id.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("backdated child".to_owned()),
        })
        .await
        .unwrap();
    let mut connection = store.connect().await.unwrap();
    diesel::update(nodes::table.filter(nodes::id.eq(&backdated_id)))
        .set(nodes::created_at.eq("2026-01-01T00:00:01Z"))
        .execute(&mut connection)
        .await
        .unwrap();
    drop(connection);

    let page_size = NonZeroUsize::new(2).unwrap();
    let frozen_page = graph
        .graph_child_ids_page_through_at_revision(
            &root_id,
            None,
            &frozen_high_watermark,
            frozen_revision,
            page_size,
        )
        .await
        .unwrap();
    assert_eq!(frozen_page.child_ids, vec![first_id.clone()]);

    let later_revision = graph.graph_relation_revision().await.unwrap();
    assert_eq!(later_revision, frozen_revision + 1);
    let later_page = graph
        .graph_child_ids_page_through_at_revision(
            &root_id,
            None,
            &frozen_high_watermark,
            later_revision,
            page_size,
        )
        .await
        .unwrap();
    assert_eq!(later_page.child_ids, vec![first_id]);
}

#[tokio::test]
async fn graph_revision_apis_reject_revisions_outside_the_serviceable_range() {
    let store = SqliteStore::open_temporary().await.unwrap();
    let graph = SqliteGraphStore::open_read_only(store.store_path())
        .await
        .unwrap();
    let root_id = store.root_id();
    store.fork("main", &root_id).await.unwrap();
    let bounds = graph.graph_mutation_revision_bounds().await.unwrap();
    assert_eq!(bounds.baseline_revision, 0);
    let future_revision = bounds.current_revision + 1;
    let page_size = NonZeroUsize::new(1).unwrap();
    let through = GraphChildPageCursor {
        relation_revision: i64::MAX,
        node_id: "f".repeat(64),
    };

    let errors = vec![
        graph
            .graph_mutation_events_page(future_revision, page_size)
            .await
            .unwrap_err(),
        graph
            .graph_mutation_branch_changes_page(future_revision, None, page_size)
            .await
            .unwrap_err(),
        graph
            .graph_mutation_dirty_parents_page(future_revision, None, page_size)
            .await
            .unwrap_err(),
        graph
            .graph_branches_at_revision_by_names(future_revision, &["main".to_owned()])
            .await
            .unwrap_err(),
        graph
            .graph_branch_name_high_watermark_at_revision(future_revision)
            .await
            .unwrap_err(),
        graph
            .graph_branches_at_revision_page(future_revision, None, None, page_size)
            .await
            .unwrap_err(),
        graph
            .graph_child_ids_page_at_revision(&root_id, None, future_revision, page_size)
            .await
            .unwrap_err(),
        graph
            .graph_child_high_watermark_at_revision(&root_id, future_revision)
            .await
            .unwrap_err(),
        graph
            .graph_child_ids_page_through_at_revision(
                &root_id,
                None,
                &through,
                future_revision,
                page_size,
            )
            .await
            .unwrap_err(),
    ];
    for error in errors {
        assert_graph_revision_error(error, future_revision, bounds);
    }

    let before_baseline = bounds.baseline_revision - 1;
    let branch_error = graph
        .graph_branches_at_revision_by_names(before_baseline, &["main".to_owned()])
        .await
        .unwrap_err();
    assert_graph_revision_error(branch_error, before_baseline, bounds);
    let child_error = graph
        .graph_child_ids_page_at_revision(&root_id, None, before_baseline, page_size)
        .await
        .unwrap_err();
    assert_graph_revision_error(child_error, before_baseline, bounds);
    let event_cursor_error = graph
        .graph_mutation_events_page(-1, page_size)
        .await
        .unwrap_err();
    assert_graph_revision_error(event_cursor_error, -1, bounds);
}

#[tokio::test]
async fn concurrent_commit_does_not_make_a_later_future_revision_serviceable() {
    let store = SqliteStore::open_temporary().await.unwrap();
    let graph = SqliteGraphStore::open_read_only(store.store_path())
        .await
        .unwrap();
    let root_id = store.root_id();
    let bounds = graph.graph_mutation_revision_bounds().await.unwrap();
    let requested_revision = bounds.current_revision + 2;

    let (append_result, read_result) = tokio::join!(
        store.append(NewNode {
            parent: root_id.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("concurrent child".to_owned()),
        }),
        graph.graph_child_high_watermark_at_revision(&root_id, requested_revision),
    );
    append_result.unwrap();
    let error = read_result.unwrap_err();
    let current_bounds = graph.graph_mutation_revision_bounds().await.unwrap();
    assert_eq!(current_bounds.current_revision, bounds.current_revision + 1);
    assert!(matches!(
        error,
        StoreError::GraphRevisionOutOfRange {
            requested,
            minimum,
            maximum,
        } if requested == requested_revision
            && minimum == bounds.baseline_revision
            && maximum <= current_bounds.current_revision
    ));
}

fn assert_graph_revision_error(
    error: StoreError,
    requested: i64,
    bounds: GraphMutationRevisionBounds,
) {
    assert!(matches!(
        error,
        StoreError::GraphRevisionOutOfRange {
            requested: actual_requested,
            minimum,
            maximum,
        } if actual_requested == requested
            && minimum == bounds.baseline_revision
            && maximum == bounds.current_revision
    ));
}

#[tokio::test]
async fn raw_node_append_commits_one_exact_graph_mutation_event() {
    let store = SqliteStore::open_temporary().await.unwrap();
    let graph = SqliteGraphStore::open_read_only(store.store_path())
        .await
        .unwrap();
    let root_id = store.root_id();
    let before = graph.graph_mutation_revision().await.unwrap();

    let child_id = store
        .append(NewNode {
            parent: root_id.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("journaled child".to_owned()),
        })
        .await
        .unwrap();

    let revision = graph.graph_mutation_revision().await.unwrap();
    assert_eq!(revision, before + 1);
    let events = graph
        .graph_mutation_events_page(before, NonZeroUsize::new(2).unwrap())
        .await
        .unwrap();
    assert_eq!(events.events, vec![super::GraphMutationEvent { revision }]);
    assert!(events.complete);
    assert_eq!(events.next_cursor, None);
    let dirty = graph
        .graph_mutation_dirty_parents_page(revision, None, NonZeroUsize::new(2).unwrap())
        .await
        .unwrap();
    assert_eq!(dirty.parent_ids, vec![root_id]);
    assert!(dirty.complete);
    let changes = graph
        .graph_mutation_branch_changes_page(revision, None, NonZeroUsize::new(2).unwrap())
        .await
        .unwrap();
    assert!(changes.changes.is_empty());
    assert!(changes.complete);

    let mut connection = store.connect().await.unwrap();
    assert_eq!(
        node_relations::table
            .filter(node_relations::child_node_id.eq(child_id))
            .select(node_relations::created_revision)
            .get_result::<i64>(&mut connection)
            .await
            .unwrap(),
        revision
    );
}

#[tokio::test]
async fn multi_node_branch_append_uses_one_revision_and_bounded_snapshot_pages() {
    let store = SqliteStore::open_temporary().await.unwrap();
    let root_id = store.root_id();
    store.fork("main", &root_id).await.unwrap();
    let graph = SqliteGraphStore::open_read_only(store.store_path())
        .await
        .unwrap();
    let before = graph.graph_mutation_revision().await.unwrap();

    let head_id = store
        .append_nodes_and_set_branch_head(
            "main",
            &root_id,
            &root_id,
            (0..3)
                .map(|index| NewNodeContent {
                    role: Role::User,
                    metadata: None,
                    kind: Kind::Text(format!("batch node {index}")),
                })
                .collect(),
        )
        .await
        .unwrap();
    let revision = graph.graph_mutation_revision().await.unwrap();
    assert_eq!(revision, before + 1);

    let ancestry = store.ancestry(&head_id).await.unwrap();
    let appended_nodes = ancestry
        .iter()
        .filter(|node| node.id != root_id)
        .collect::<Vec<_>>();
    assert_eq!(appended_nodes.len(), 3);
    let appended_ids = appended_nodes
        .iter()
        .map(|node| node.id.clone())
        .collect::<BTreeSet<_>>();
    let expected_dirty = appended_nodes
        .iter()
        .map(|node| node.parent.clone())
        .collect::<BTreeSet<_>>();

    let mut connection = store.connect().await.unwrap();
    let relation_rows = node_relations::table
        .filter(node_relations::created_revision.eq(revision))
        .select((
            node_relations::child_node_id,
            node_relations::created_revision,
        ))
        .load::<(String, i64)>(&mut connection)
        .await
        .unwrap();
    assert_eq!(relation_rows.len(), 3);
    assert_eq!(
        relation_rows
            .iter()
            .map(|(child_id, _)| child_id.clone())
            .collect::<BTreeSet<_>>(),
        appended_ids
    );
    assert!(
        relation_rows
            .iter()
            .all(|(_, created_revision)| *created_revision == revision)
    );
    drop(connection);

    let event_page = graph
        .graph_mutation_events_page(before, NonZeroUsize::new(1).unwrap())
        .await
        .unwrap();
    assert_eq!(event_page.events.len(), 1);
    assert_eq!(event_page.events[0].revision, revision);
    assert!(event_page.complete);

    let branch_changes = graph
        .graph_mutation_branch_changes_page(revision, None, NonZeroUsize::new(1).unwrap())
        .await
        .unwrap();
    assert_eq!(branch_changes.changes.len(), 1);
    assert_eq!(branch_changes.changes[0].name, "main");
    assert_eq!(
        branch_changes.changes[0].kind,
        GraphMutationBranchChangeKind::Upserted
    );
    assert_eq!(
        branch_changes.changes[0].head_id.as_deref(),
        Some(head_id.as_str())
    );
    assert_eq!(branch_changes.changes[0].state, Some(SessionState::Active));

    let mut dirty_cursor = None;
    let mut actual_dirty = BTreeSet::new();
    loop {
        let page = graph
            .graph_mutation_dirty_parents_page(
                revision,
                dirty_cursor.as_ref(),
                NonZeroUsize::new(1).unwrap(),
            )
            .await
            .unwrap();
        actual_dirty.extend(page.parent_ids);
        if page.complete {
            break;
        }
        dirty_cursor = page.next_cursor;
    }
    assert_eq!(actual_dirty, expected_dirty);

    let before_snapshot = graph
        .graph_branches_at_revision_by_names(before, &["main".to_owned(), "main".to_owned()])
        .await
        .unwrap();
    assert_eq!(before_snapshot.len(), 1);
    assert_eq!(before_snapshot[0].head_id, root_id);
    let high_watermark = graph
        .graph_branch_name_high_watermark_at_revision(revision)
        .await
        .unwrap();
    assert_eq!(high_watermark.as_deref(), Some("main"));
    let after_snapshot = graph
        .graph_branches_at_revision_page(
            revision,
            None,
            high_watermark.as_deref(),
            NonZeroUsize::new(1).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(after_snapshot.branches.len(), 1);
    assert_eq!(after_snapshot.branches[0].head_id, head_id);
    assert!(after_snapshot.complete);
}

#[tokio::test]
async fn branch_only_mutations_are_historical_and_failed_transactions_leave_no_event() {
    let store = SqliteStore::open_temporary().await.unwrap();
    let root_id = store.root_id();
    store.fork("reused", &root_id).await.unwrap();
    let graph = SqliteGraphStore::open_read_only(store.store_path())
        .await
        .unwrap();
    let active_revision = graph.graph_mutation_revision().await.unwrap();
    let paused_state = SessionState::Paused {
        target_branch: String::new(),
        reason: PauseReason::Closed,
    };
    store
        .set_session_state("reused", Some(&SessionState::Active), paused_state.clone())
        .await
        .unwrap();
    let paused_revision = graph.graph_mutation_revision().await.unwrap();
    assert_eq!(paused_revision, active_revision + 1);
    let state_change = graph
        .graph_mutation_branch_changes_page(paused_revision, None, NonZeroUsize::new(1).unwrap())
        .await
        .unwrap();
    assert_eq!(state_change.changes[0].state, Some(paused_state.clone()));
    assert_eq!(
        graph
            .graph_branches_at_revision_by_names(active_revision, &["reused".to_owned()])
            .await
            .unwrap()[0]
            .state,
        SessionState::Active
    );
    assert_eq!(
        graph
            .graph_branches_at_revision_by_names(paused_revision, &["reused".to_owned()])
            .await
            .unwrap()[0]
            .state,
        paused_state
    );
    let before_failure = paused_revision;

    store
        .set_branch_head("reused", "stale-head", &root_id)
        .await
        .unwrap_err();
    assert_eq!(
        graph.graph_mutation_revision().await.unwrap(),
        before_failure
    );
    assert!(
        graph
            .graph_mutation_events_page(before_failure, NonZeroUsize::new(1).unwrap())
            .await
            .unwrap()
            .events
            .is_empty()
    );

    store.delete_branch("reused").await.unwrap();
    let removed_revision = graph.graph_mutation_revision().await.unwrap();
    assert_eq!(removed_revision, before_failure + 1);
    let removed = graph
        .graph_mutation_branch_changes_page(removed_revision, None, NonZeroUsize::new(1).unwrap())
        .await
        .unwrap();
    assert_eq!(removed.changes.len(), 1);
    assert_eq!(
        removed.changes[0].kind,
        GraphMutationBranchChangeKind::Removed
    );
    assert_eq!(removed.changes[0].head_id, None);
    assert_eq!(removed.changes[0].state, None);
    assert!(
        graph
            .graph_branches_at_revision_by_names(removed_revision, &["reused".to_owned()])
            .await
            .unwrap()
            .is_empty()
    );

    store.fork("reused", &root_id).await.unwrap();
    let recreated_revision = graph.graph_mutation_revision().await.unwrap();
    assert_eq!(recreated_revision, removed_revision + 1);
    assert!(
        graph
            .graph_branches_at_revision_by_names(removed_revision, &["reused".to_owned()])
            .await
            .unwrap()
            .is_empty()
    );
    let recreated = graph
        .graph_branches_at_revision_by_names(recreated_revision, &["reused".to_owned()])
        .await
        .unwrap();
    assert_eq!(recreated.len(), 1);
    assert_eq!(recreated[0].head_id, root_id);
    assert_eq!(recreated[0].state, SessionState::Active);
}

#[tokio::test]
async fn historical_branch_pages_advance_across_removed_names() {
    let store = SqliteStore::open_temporary().await.unwrap();
    let root_id = store.root_id();
    store.fork("removed-first", &root_id).await.unwrap();
    store.delete_branch("removed-first").await.unwrap();
    store.fork("active", &root_id).await.unwrap();
    store.fork("removed-last", &root_id).await.unwrap();
    store.delete_branch("removed-last").await.unwrap();
    let graph = SqliteGraphStore::open_read_only(store.store_path())
        .await
        .unwrap();
    let revision = graph.graph_mutation_revision().await.unwrap();

    let mut cursor = None;
    let mut active_names = Vec::new();
    let mut empty_pages = 0;
    loop {
        let page = graph
            .graph_branches_at_revision_page(
                revision,
                cursor.as_ref(),
                None,
                NonZeroUsize::new(1).unwrap(),
            )
            .await
            .unwrap();
        if page.branches.is_empty() {
            empty_pages += 1;
        }
        active_names.extend(page.branches.into_iter().map(|branch| branch.name));
        if page.complete {
            break;
        }
        cursor = page.next_cursor;
    }

    assert_eq!(active_names, vec!["active"]);
    assert_eq!(empty_pages, 2);
}

#[tokio::test]
async fn historical_branch_pages_preserve_revision_keyset_order() {
    let store = SqliteStore::open_temporary().await.unwrap();
    let root_id = store.root_id();
    store.fork("z-first", &root_id).await.unwrap();
    store.fork("a-second", &root_id).await.unwrap();
    let graph = SqliteGraphStore::open_read_only(store.store_path())
        .await
        .unwrap();
    let revision = graph.graph_mutation_revision().await.unwrap();

    let page = graph
        .graph_branches_at_revision_page(revision, None, None, NonZeroUsize::new(2).unwrap())
        .await
        .unwrap();

    assert_eq!(
        page.branches
            .into_iter()
            .map(|branch| branch.name)
            .collect::<Vec<_>>(),
        vec!["z-first", "a-second"]
    );
    assert!(page.complete);
}

#[tokio::test]
async fn prompt_job_nodes_share_one_event_and_failed_prompt_job_rolls_back_it() {
    let store = SqliteStore::open_temporary().await.unwrap();
    let root_id = store.root_id();
    let session_id = store.append(session_anchor_node(&root_id)).await.unwrap();
    store.fork("main", &session_id).await.unwrap();
    let graph = SqliteGraphStore::open_read_only(store.store_path())
        .await
        .unwrap();
    let before = graph.graph_mutation_revision().await.unwrap();

    let job = store
        .submit_job_with_prompt_base(
            "main",
            PromptAnchor {
                prompt: "journaled prompt".to_owned(),
                attachments: vec![],
            },
            vec![],
            Some(SessionAnchorPatch::default()),
        )
        .await
        .unwrap();
    let revision = graph.graph_mutation_revision().await.unwrap();
    assert_eq!(revision, before + 1);
    let prompt_node = store.get_node(&job.base).await.unwrap();
    let patch_node = store.get_node(&prompt_node.parent).await.unwrap();
    let mut connection = store.connect().await.unwrap();
    let relation_revisions = node_relations::table
        .filter(node_relations::child_node_id.eq_any([&prompt_node.id, &patch_node.id]))
        .select(node_relations::created_revision)
        .load::<i64>(&mut connection)
        .await
        .unwrap();
    assert_eq!(relation_revisions, vec![revision, revision]);
    drop(connection);
    let event = graph
        .graph_mutation_events_page(before, NonZeroUsize::new(2).unwrap())
        .await
        .unwrap();
    assert_eq!(event.events, vec![super::GraphMutationEvent { revision }]);
    let dirty = graph
        .graph_mutation_dirty_parents_page(revision, None, NonZeroUsize::new(2).unwrap())
        .await
        .unwrap();
    assert_eq!(
        dirty.parent_ids.into_iter().collect::<BTreeSet<_>>(),
        BTreeSet::from([session_id.clone(), patch_node.id])
    );

    store
        .set_job_status(&job.job_id, JobStatus::Queued, JobStatus::Running)
        .await
        .unwrap();
    store
        .set_job_status(&job.job_id, JobStatus::Running, JobStatus::Finished)
        .await
        .unwrap();
    let before_failure = graph.graph_mutation_revision().await.unwrap();
    store
        .submit_job_with_prompt_base(
            "main",
            PromptAnchor {
                prompt: "invalid prompt".to_owned(),
                attachments: vec![],
            },
            vec![MergeParent::merge("missing-parent")],
            Some(SessionAnchorPatch::default()),
        )
        .await
        .unwrap_err();
    assert_eq!(
        graph.graph_mutation_revision().await.unwrap(),
        before_failure
    );
    assert!(
        graph
            .graph_mutation_events_page(before_failure, NonZeroUsize::new(1).unwrap())
            .await
            .unwrap()
            .events
            .is_empty()
    );
}

#[tokio::test]
async fn upsert_existing_node_preserves_immutable_relation_revision() {
    let store = SqliteStore::open_temporary().await.unwrap();
    let child_id = store
        .append(NewNode {
            parent: store.root_id(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("immutable child".to_owned()),
        })
        .await
        .unwrap();
    let node = store.get_node(&child_id).await.unwrap();
    let mut connection = store.connect().await.unwrap();
    let created_revision = node_relations::table
        .filter(node_relations::child_node_id.eq(&child_id))
        .select(node_relations::created_revision)
        .get_result::<i64>(&mut connection)
        .await
        .unwrap();

    super::node::upsert_node_without_transaction(
        &mut connection,
        &store.database_path,
        &node,
        created_revision,
    )
    .await
    .unwrap();

    assert_eq!(
        node_relations::table
            .filter(node_relations::child_node_id.eq(&child_id))
            .select(node_relations::created_revision)
            .get_result::<i64>(&mut connection)
            .await
            .unwrap(),
        created_revision
    );
    let mut conflicting = node.clone();
    conflicting.kind = Kind::Text("conflicting content".to_owned());
    let error = super::node::upsert_node_without_transaction(
        &mut connection,
        &store.database_path,
        &conflicting,
        created_revision,
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("conflicts with immutable stored data")
    );
    assert_eq!(store.get_node(&child_id).await.unwrap(), node);
}

#[tokio::test]
async fn graph_read_api_rejects_oversized_batches() {
    let store = SqliteStore::open_temporary().await.unwrap();
    let graph = SqliteGraphStore::open_read_only(store.store_path())
        .await
        .unwrap();
    let ids = (0..=GRAPH_READ_BATCH_SIZE)
        .map(|index| format!("node-{index}"))
        .collect::<Vec<_>>();

    let error = graph.graph_nodes_by_ids(&ids).await.unwrap_err();

    assert!(matches!(
        error,
        StoreError::GraphReadBatchTooLarge {
            actual,
            maximum: GRAPH_READ_BATCH_SIZE,
        } if actual == GRAPH_READ_BATCH_SIZE + 1
    ));

    let error = graph
        .graph_child_ids_page(
            &store.root_id(),
            None,
            NonZeroUsize::new(GRAPH_READ_BATCH_SIZE + 1).unwrap(),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        StoreError::GraphReadBatchTooLarge {
            actual,
            maximum: GRAPH_READ_BATCH_SIZE,
        } if actual == GRAPH_READ_BATCH_SIZE + 1
    ));

    let error = graph
        .graph_branches_page(
            None,
            "",
            NonZeroUsize::new(GRAPH_READ_BATCH_SIZE + 1).unwrap(),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        StoreError::GraphReadBatchTooLarge {
            actual,
            maximum: GRAPH_READ_BATCH_SIZE,
        } if actual == GRAPH_READ_BATCH_SIZE + 1
    ));
}

#[tokio::test]
async fn sqlite_store_handles_concurrent_writes_with_transactions() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let root_id = store.root_id();

    let handles = (0..8)
        .map(|index| {
            let store = store.clone();
            let root_id = root_id.clone();
            tokio::spawn(async move {
                store
                    .append(NewNode {
                        parent: root_id,
                        role: Role::User,
                        metadata: None,
                        kind: Kind::Text(format!("child-{index}")),
                    })
                    .await
                    .unwrap()
            })
        })
        .collect::<Vec<_>>();

    let mut node_ids = Vec::new();
    for handle in handles {
        node_ids.push(handle.await.unwrap());
    }
    node_ids.sort();

    let mut children = store
        .list_children(&store.root_id())
        .await
        .unwrap()
        .into_iter()
        .map(|node| node.id)
        .collect::<Vec<_>>();
    children.sort();

    assert_eq!(children, node_ids);
    let reopened = SqliteStore::open_read_only(&path).await.unwrap();
    assert_eq!(
        reopened
            .list_children(&reopened.root_id())
            .await
            .unwrap()
            .len(),
        8
    );
}

#[tokio::test]
async fn open_read_only_rejects_invalid_root_count() {
    let tempdir = tempfile::tempdir().unwrap();
    let missing_path = tempdir.path().join("missing-root");
    let missing_store = SqliteStore::open(&missing_path).await.unwrap();
    let mut connection = missing_store.connect().await.unwrap();
    diesel::update(nodes::table.filter(nodes::id.eq(missing_store.root_id())))
        .set(nodes::parent_id.eq("missing"))
        .execute(&mut connection)
        .await
        .unwrap();
    drop(connection);
    drop(missing_store);

    let error = SqliteStore::open_read_only(&missing_path)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("missing SQLite root node"));

    let multiple_path = tempdir.path().join("multiple-roots");
    let multiple_store = SqliteStore::open(&multiple_path).await.unwrap();
    let child_id = multiple_store
        .append(NewNode {
            parent: multiple_store.root_id(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("second root".to_owned()),
        })
        .await
        .unwrap();
    let mut connection = multiple_store.connect().await.unwrap();
    diesel::update(nodes::table.filter(nodes::id.eq(child_id)))
        .set(nodes::parent_id.eq(""))
        .execute(&mut connection)
        .await
        .unwrap();
    drop(connection);
    drop(multiple_store);

    let error = SqliteStore::open_read_only(&multiple_path)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("multiple SQLite root nodes"));
}

#[tokio::test]
async fn append_persists_node_relations() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let root_id = store.root_id();
    let primary_parent = store
        .append(NewNode {
            parent: root_id.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("primary parent".to_owned()),
        })
        .await
        .unwrap();
    let merge_parent = store
        .append(NewNode {
            parent: root_id.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("merge parent".to_owned()),
        })
        .await
        .unwrap();
    let shadow_parent = store
        .append(NewNode {
            parent: root_id,
            role: Role::User,
            metadata: None,
            kind: Kind::Text("shadow parent".to_owned()),
        })
        .await
        .unwrap();
    let child_kind = Kind::Anchor(Anchor::session(
        vec![
            MergeParent::merge(merge_parent.clone()),
            MergeParent::shadow(shadow_parent.clone()),
        ],
        SessionAnchor {
            role: SessionRole::Orchestrator,
            provider_profile: None,
            provider: Some("openai".to_owned()),
            model: "gpt-5.4".to_owned(),
            tools: vec![],
            system_prompt: "system".to_owned(),
            prompt: "prompt".to_owned(),
            temperature: Some(0.1),
            max_tokens: Some(64),
            additional_params: None,
            enable_coco_shim: false,
            active_skill: None,
        },
    ));
    let expected_node_kind = super::node_storage_kind(&child_kind).to_owned();
    let child = store
        .append(NewNode {
            parent: primary_parent.clone(),
            role: Role::System,
            metadata: None,
            kind: child_kind,
        })
        .await
        .unwrap();

    let relations = node_relation_rows(&store, &child).await;

    assert_eq!(node_kind(&store, &child).await, expected_node_kind);
    assert_eq!(relations.len(), 3);
    assert!(relations.contains(&NodeRelationRow {
        child_node_id: child.clone(),
        parent_node_id: primary_parent,
        kind: "primary".to_owned(),
        ordinal: 0,
    }));
    assert!(relations.contains(&NodeRelationRow {
        child_node_id: child.clone(),
        parent_node_id: merge_parent,
        kind: "merge".to_owned(),
        ordinal: 0,
    }));
    assert!(relations.contains(&NodeRelationRow {
        child_node_id: child,
        parent_node_id: shadow_parent,
        kind: "shadow".to_owned(),
        ordinal: 1,
    }));
}

#[tokio::test]
async fn append_persists_node_metadata_rows() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let root_id = store.root_id();
    let single_metadata = BackendMetadata {
        execution_id: Some("execution-single".to_owned()),
        call_id: Some("call-single".to_owned()),
    };
    let single = store
        .append(NewNode {
            parent: root_id.clone(),
            role: Role::User,
            metadata: Some(vec![single_metadata]),
            kind: Kind::Text("single metadata".to_owned()),
        })
        .await
        .unwrap();
    let many = store
        .append(NewNode {
            parent: root_id,
            role: Role::LLM,
            metadata: Some(vec![
                BackendMetadata {
                    execution_id: Some("execution-many".to_owned()),
                    call_id: Some("call-a".to_owned()),
                },
                BackendMetadata {
                    execution_id: Some("execution-many".to_owned()),
                    call_id: Some("call-b".to_owned()),
                },
            ]),
            kind: Kind::Text("many metadata".to_owned()),
        })
        .await
        .unwrap();

    assert_eq!(
        node_metadata_rows(&store, &single).await,
        vec![NodeMetadataRow {
            node_id: single,
            ordinal: 0,
            execution_id: Some("execution-single".to_owned()),
            call_id: Some("call-single".to_owned()),
        }]
    );
    assert_eq!(
        node_metadata_rows(&store, &many).await,
        vec![
            NodeMetadataRow {
                node_id: many.clone(),
                ordinal: 0,
                execution_id: Some("execution-many".to_owned()),
                call_id: Some("call-a".to_owned()),
            },
            NodeMetadataRow {
                node_id: many,
                ordinal: 1,
                execution_id: Some("execution-many".to_owned()),
                call_id: Some("call-b".to_owned()),
            },
        ]
    );
}

#[tokio::test]
async fn append_round_trips_present_empty_metadata() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let node_id = store
        .append(NewNode {
            parent: store.root_id(),
            role: Role::User,
            metadata: Some(Vec::new()),
            kind: Kind::Text("empty metadata".to_owned()),
        })
        .await
        .unwrap();

    assert_eq!(
        store.get_node(&node_id).await.unwrap().metadata,
        Some(Vec::new())
    );
    drop(store);
    let reopened = SqliteStore::open_read_only(&path).await.unwrap();
    assert_eq!(
        reopened.get_node(&node_id).await.unwrap().metadata,
        Some(Vec::new())
    );
}

#[tokio::test]
async fn append_persists_text_and_failure_content() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let root_id = store.root_id();
    let text_id = store
        .append(NewNode {
            parent: root_id.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("line one\n\"line two\"".to_owned()),
        })
        .await
        .unwrap();
    let failure_id = store
        .append(NewNode {
            parent: text_id.clone(),
            role: Role::System,
            metadata: None,
            kind: Kind::Failure(String::new()),
        })
        .await
        .unwrap();

    assert_eq!(
        node_content(&store, &root_id).await.as_deref(),
        Some("The Big Bang")
    );
    assert_eq!(
        node_content(&store, &text_id).await.as_deref(),
        Some("line one\n\"line two\"")
    );
    assert_eq!(node_content(&store, &failure_id).await.as_deref(), Some(""));
    assert_eq!(
        store.get_node(&text_id).await.unwrap().kind,
        Kind::Text("line one\n\"line two\"".to_owned())
    );
    assert_eq!(
        store.get_node(&failure_id).await.unwrap().kind,
        Kind::Failure(String::new())
    );
}

#[tokio::test]
async fn append_persists_node_tool_item_rows() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let root_id = store.root_id();
    let tool_use_node = store
        .append(NewNode {
            parent: root_id.clone(),
            role: Role::LLM,
            metadata: None,
            kind: Kind::tool_uses(vec![
                ToolUse {
                    id: "tool-call-a".to_owned(),
                    name: "exec_command".to_owned(),
                    input: serde_json::json!({"cmd": "pwd"}),
                },
                ToolUse {
                    id: "tool-call-b".to_owned(),
                    name: "exec_command".to_owned(),
                    input: serde_json::json!({"cmd": "ls"}),
                },
            ]),
        })
        .await
        .unwrap();
    let tool_result_node = store
        .append(NewNode {
            parent: tool_use_node.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::tool_results(vec![
                ToolResult {
                    id: "tool-call-a".to_owned(),
                    output: "left".to_owned(),
                },
                ToolResult {
                    id: "tool-call-b".to_owned(),
                    output: "right".to_owned(),
                },
            ]),
        })
        .await
        .unwrap();

    assert_eq!(
        node_tool_use_rows(&store, &tool_use_node).await,
        vec![
            NodeToolUseRow {
                node_id: tool_use_node.clone(),
                ordinal: 0,
                tool_use_id: "tool-call-a".to_owned(),
                name: "exec_command".to_owned(),
                input_json: r#"{"cmd":"pwd"}"#.to_owned(),
            },
            NodeToolUseRow {
                node_id: tool_use_node.clone(),
                ordinal: 1,
                tool_use_id: "tool-call-b".to_owned(),
                name: "exec_command".to_owned(),
                input_json: r#"{"cmd":"ls"}"#.to_owned(),
            },
        ]
    );
    assert_eq!(
        node_tool_result_rows(&store, &tool_result_node).await,
        vec![
            NodeToolResultRow {
                node_id: tool_result_node.clone(),
                ordinal: 0,
                tool_result_id: "tool-call-a".to_owned(),
                output: "left".to_owned(),
            },
            NodeToolResultRow {
                node_id: tool_result_node.clone(),
                ordinal: 1,
                tool_result_id: "tool-call-b".to_owned(),
                output: "right".to_owned(),
            },
        ]
    );
    assert_eq!(node_content(&store, &tool_use_node).await, None);
    assert_eq!(node_content(&store, &tool_result_node).await, None);
}

#[tokio::test]
async fn append_persists_node_anchor_payload_rows() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let root_id = store.root_id();
    let session = store
        .append(NewNode {
            parent: root_id.clone(),
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::session(
                vec![],
                SessionAnchor {
                    role: SessionRole::Runner,
                    provider_profile: Some("runner-profile".to_owned()),
                    provider: Some("openai".to_owned()),
                    model: "gpt-5.4".to_owned(),
                    tools: vec![],
                    system_prompt: "system".to_owned(),
                    prompt: "session prompt".to_owned(),
                    temperature: Some(0.1),
                    max_tokens: Some(64),
                    additional_params: None,
                    enable_coco_shim: false,
                    active_skill: None,
                },
            )),
        })
        .await
        .unwrap();
    let prompt = store
        .append(NewNode {
            parent: root_id,
            role: Role::User,
            metadata: None,
            kind: Kind::Anchor(Anchor::prompt(
                vec![],
                crate::PromptAnchor {
                    prompt: "detached prompt".to_owned(),
                    attachments: vec![],
                },
            )),
        })
        .await
        .unwrap();

    assert_eq!(
        node_anchor_session_row(&store, &session).await,
        NodeAnchorSessionRow {
            node_id: session.clone(),
            role: "runner".to_owned(),
            provider_profile: Some("runner-profile".to_owned()),
            provider: Some("openai".to_owned()),
            model: "gpt-5.4".to_owned(),
            system_prompt: "system".to_owned(),
            prompt: "session prompt".to_owned(),
            temperature: Some(0.1),
            max_tokens: Some("64".to_owned()),
            additional_params_json: None,
            enable_coco_shim: false,
            active_skill_name: None,
            active_skill_handoff: None,
        }
    );
    assert_eq!(node_content(&store, &session).await, None);
    assert_eq!(
        node_content(&store, &prompt).await.as_deref(),
        Some("detached prompt")
    );
}

#[tokio::test]
async fn reading_session_anchor_uses_relational_payload() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let anchor_id = store
        .append(rich_session_anchor_node(&store.root_id(), vec![]))
        .await
        .unwrap();
    let expected = store.get_node(&anchor_id).await.unwrap();

    assert_eq!(store.get_node(&anchor_id).await.unwrap(), expected);
}

#[tokio::test]
async fn reading_session_patch_anchor_preserves_explicit_clears() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let kind = Kind::Anchor(Anchor::session_patch(
        vec![],
        SessionAnchorPatch {
            provider_profile: Some(None),
            provider: Some(None),
            tools: Some(vec![]),
            temperature: Some(None),
            max_tokens: Some(None),
            additional_params: Some(None),
            ..SessionAnchorPatch::default()
        },
    ));
    let anchor_id = store
        .append(NewNode {
            parent: store.root_id(),
            role: Role::System,
            metadata: None,
            kind: kind.clone(),
        })
        .await
        .unwrap();

    assert_eq!(store.get_node(&anchor_id).await.unwrap().kind, kind);
    let row = node_anchor_session_patch_row(&store, &anchor_id).await;
    assert!(row.provider_profile_present);
    assert!(row.provider_profile.is_none());
    assert!(row.provider_present);
    assert!(row.provider.is_none());
    assert!(row.tools_present);
    assert!(row.temperature_present);
    assert!(row.temperature.is_none());
    assert!(row.max_tokens_present);
    assert!(row.max_tokens.is_none());
    assert!(row.additional_params_present);
    assert!(row.additional_params_json.is_none());
}

#[tokio::test]
async fn reading_prompt_anchor_uses_content_and_relational_attachments() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let anchor_id = store
        .append(rich_prompt_anchor_node(&store.root_id(), vec![]))
        .await
        .unwrap();
    let expected = store.get_node(&anchor_id).await.unwrap();

    assert_eq!(store.get_node(&anchor_id).await.unwrap(), expected);
}

#[tokio::test]
async fn reading_skill_invocation_anchor_uses_relational_payload() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let anchor_id = store
        .append(skill_invocation_anchor_node(&store.root_id(), vec![]))
        .await
        .unwrap();
    let expected = store.get_node(&anchor_id).await.unwrap();

    assert_eq!(store.get_node(&anchor_id).await.unwrap(), expected);
    assert_eq!(
        node_anchor_skill_invocation_row(&store, &anchor_id).await,
        NodeAnchorSkillInvocationRow {
            node_id: anchor_id,
            skill_name: "compact".to_owned(),
            mode: "handoff".to_owned(),
            prompt: Some("Compact this branch".to_owned()),
        }
    );
}

#[tokio::test]
async fn reading_skill_result_anchor_uses_relational_payload() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let anchor_id = store
        .append(skill_result_anchor_node(&store.root_id(), vec![]))
        .await
        .unwrap();
    let expected = store.get_node(&anchor_id).await.unwrap();

    assert_eq!(store.get_node(&anchor_id).await.unwrap(), expected);
    assert_eq!(
        node_anchor_skill_result_row(&store, &anchor_id).await,
        NodeAnchorSkillResultRow {
            node_id: anchor_id,
            skill_name: "compact".to_owned(),
            output: "First line\nSecond line with \"quotes\"".to_owned(),
        }
    );
}

#[tokio::test]
async fn reading_anchor_node_requires_matching_payload_row() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let anchor_id = store
        .append(session_anchor_node(&store.root_id()))
        .await
        .unwrap();
    let mut connection = store.connect().await.unwrap();
    diesel::delete(
        node_anchor_sessions::table.filter(node_anchor_sessions::node_id.eq(&anchor_id)),
    )
    .execute(&mut connection)
    .await
    .unwrap();
    drop(connection);

    let error = store.get_node(&anchor_id).await.unwrap_err();

    assert!(matches!(
        error,
        crate::StoreError::CorruptedStore { message, .. }
            if message.contains("missing SQLite node anchor session row")
    ));
}

#[tokio::test]
async fn reading_anchor_node_queries_only_selected_payload_tables() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let anchor_id = store
        .append(rich_session_anchor_node(&store.root_id(), vec![]))
        .await
        .unwrap();
    let mut connection = store.connect().await.unwrap();
    let queries = Arc::new(Mutex::new(Vec::new()));
    let captured_queries = Arc::clone(&queries);
    connection.set_instrumentation(move |event: InstrumentationEvent<'_>| {
        if let InstrumentationEvent::StartQuery { query, .. } = event {
            captured_queries.lock().unwrap().push(query.to_string());
        }
    });

    super::load_node_by_exact_id(&mut connection, &store.database_path, &anchor_id)
        .await
        .unwrap();

    let queries = queries.lock().unwrap();
    assert_eq!(queries.len(), 4, "unexpected queries: {queries:#?}");
    assert!(
        queries
            .iter()
            .any(|query| query.contains("node_anchor_sessions"))
    );
    assert!(
        queries
            .iter()
            .any(|query| query.contains("node_anchor_session_tools"))
    );
    assert!(queries.iter().any(|query| query.contains("node_relations")));
    for table in [
        "node_anchor_session_patches",
        "node_anchor_session_patch_tools",
        "node_anchor_prompt_attachments",
        "node_anchor_skill_invocations",
        "node_anchor_skill_results",
        "node_metadata",
        "node_tool_uses",
        "node_tool_results",
    ] {
        assert!(
            queries.iter().all(|query| !query.contains(table)),
            "unexpected query for {table}: {queries:#?}"
        );
    }
}

#[tokio::test]
async fn reading_nodes_validates_content_presence() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let root_id = store.root_id();
    let tool_use_id = store
        .append(NewNode {
            parent: root_id.clone(),
            role: Role::LLM,
            metadata: None,
            kind: Kind::tool_uses(Vec::new()),
        })
        .await
        .unwrap();
    let prompt_id = store
        .append(NewNode {
            parent: root_id.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Anchor(Anchor::prompt(
                vec![],
                PromptAnchor {
                    prompt: "prompt".to_owned(),
                    attachments: vec![],
                },
            )),
        })
        .await
        .unwrap();
    let mut connection = store.connect().await.unwrap();
    diesel::update(nodes::table.filter(nodes::id.eq(&root_id)))
        .set(nodes::content.eq(None::<String>))
        .execute(&mut connection)
        .await
        .unwrap();
    drop(connection);

    let error = store.get_node(&root_id).await.unwrap_err();
    assert!(error.to_string().contains("missing SQLite node content"));

    let mut connection = store.connect().await.unwrap();
    diesel::update(nodes::table.filter(nodes::id.eq(&prompt_id)))
        .set(nodes::content.eq(None::<String>))
        .execute(&mut connection)
        .await
        .unwrap();
    drop(connection);

    let error = store.get_node(&prompt_id).await.unwrap_err();
    assert!(error.to_string().contains("missing SQLite node content"));

    let mut connection = store.connect().await.unwrap();
    diesel::update(nodes::table.filter(nodes::id.eq(&tool_use_id)))
        .set(nodes::content.eq(Some("unexpected")))
        .execute(&mut connection)
        .await
        .unwrap();
    drop(connection);

    let error = store.get_node(&tool_use_id).await.unwrap_err();
    assert!(error.to_string().contains("unexpectedly has content"));
}

#[tokio::test]
async fn graph_store_reads_children_from_node_relations() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let writer = SqliteStore::open(&path).await.unwrap();
    let root_id = writer.root_id();
    let child_id = writer
        .append(NewNode {
            parent: root_id.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("graph child".to_owned()),
        })
        .await
        .unwrap();
    writer.fork("graph-child", &child_id).await.unwrap();
    drop(writer);

    let graph_store = SqliteGraphStore::open_read_only(&path).await.unwrap();

    assert_eq!(graph_store.root_id(), root_id);
    assert_eq!(
        graph_store.get_node(&child_id).await.unwrap().id,
        child_id.clone()
    );
    assert_eq!(
        graph_store.get_node(&child_id[..12]).await.unwrap().id,
        child_id.clone()
    );
    assert_eq!(
        graph_store.get_node("graph-child").await.unwrap().id,
        child_id.clone()
    );
    assert_eq!(
        graph_store.list_children(&root_id).await.unwrap()[0].id,
        child_id
    );
    assert_eq!(graph_store.ancestry(&child_id).await.unwrap().len(), 2);
    assert!(matches!(
        graph_store
            .append(NewNode {
                parent: root_id,
                role: Role::User,
                metadata: None,
                kind: Kind::Text("blocked".to_owned()),
            })
            .await
            .unwrap_err(),
        crate::StoreError::StoreReadOnly { .. }
    ));
}

#[tokio::test]
async fn open_read_only_rejects_writes() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let writable = SqliteStore::open(&path).await.unwrap();
    let root_id = writable.root_id();
    drop(writable);

    let store = SqliteStore::open_read_only(&path).await.unwrap();
    let err = store
        .append(NewNode {
            parent: root_id.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("child".to_owned()),
        })
        .await
        .unwrap_err();

    assert!(matches!(err, crate::StoreError::StoreReadOnly { .. }));
    let reopened = SqliteStore::open_read_only(&path).await.unwrap();
    assert!(reopened.list_children(&root_id).await.unwrap().is_empty());
}

#[tokio::test]
async fn graph_open_read_only_rejects_missing_schema_without_creating_database() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    std::fs::create_dir(&path).unwrap();

    let err = SqliteGraphStore::open_read_only(&path).await.unwrap_err();

    assert!(err.to_string().contains("SQLite"));
    assert!(!super::sqlite_database_path(&path).exists());
}

#[tokio::test]
async fn append_persists_node_across_reopen() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let root_id = store.root_id();
    let child_id = store
        .append(NewNode {
            parent: root_id.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("child".to_owned()),
        })
        .await
        .unwrap();
    assert_eq!(store.list_children(&root_id).await.unwrap()[0].id, child_id);

    let reopened = SqliteStore::open(&path).await.unwrap();
    let child = reopened.get_node(&child_id).await.unwrap();

    assert_eq!(child.parent, root_id);
    assert_eq!(
        reopened.list_children(&root_id).await.unwrap()[0].id,
        child_id
    );
}

#[tokio::test]
async fn reopened_store_supports_node_traversal() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let root_id = store.root_id();
    let first = store
        .append(NewNode {
            parent: root_id.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("first".to_owned()),
        })
        .await
        .unwrap();
    let second = store
        .append(NewNode {
            parent: first.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("second".to_owned()),
        })
        .await
        .unwrap();

    let reopened = SqliteStore::open(&path).await.unwrap();

    let ancestry = reopened
        .ancestry(&second)
        .await
        .unwrap()
        .into_iter()
        .map(|node| node.id)
        .collect::<Vec<_>>();
    assert_eq!(
        ancestry,
        vec![second.clone(), first.clone(), root_id.clone()]
    );
    let log = reopened
        .log(&root_id, &second)
        .await
        .unwrap()
        .into_iter()
        .map(|node| node.id)
        .collect::<Vec<_>>();
    assert_eq!(log, vec![second.clone(), first, root_id]);
    assert_eq!(reopened.get_node(&second[..12]).await.unwrap().id, second);
}

#[tokio::test]
async fn branch_operations_persist_across_reopen() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let root_id = store.root_id();
    let first = store
        .append(NewNode {
            parent: root_id.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("first".to_owned()),
        })
        .await
        .unwrap();
    let second = store
        .append(NewNode {
            parent: first.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("second".to_owned()),
        })
        .await
        .unwrap();

    assert_eq!(store.fork("main", &first).await.unwrap(), first);
    store
        .set_branch_head("main", &first, &second)
        .await
        .unwrap();
    assert_eq!(store.get_branch_head("main").await.unwrap(), second);

    let reopened = SqliteStore::open(&path).await.unwrap();
    assert_eq!(reopened.get_branch_head("main").await.unwrap(), second);

    reopened.delete_branch("main").await.unwrap();
    let reopened = SqliteStore::open(&path).await.unwrap();
    assert!(reopened.get_branch_head("main").await.is_err());
}

#[tokio::test]
async fn persist_session_nodes_rolls_back_node_when_branch_head_mismatch() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let root_id = store.root_id();
    store.fork("main", &root_id).await.unwrap();
    let node = Node::new(
        root_id.clone(),
        Role::User,
        None,
        Kind::Text("rolled back node".to_owned()),
        "1970-01-01T00:00:01Z".parse().unwrap(),
    );
    let node_id = node.id.clone();

    let mut connection = store.connect().await.unwrap();
    let err = super::persist_session_nodes_and_branch_head(
        &mut connection,
        &store.database_path,
        "main",
        "stale-head",
        &node_id,
        std::slice::from_ref(&node),
    )
    .await
    .unwrap_err();
    let count = nodes::table
        .filter(nodes::id.eq(node_id))
        .count()
        .get_result::<i64>(&mut connection)
        .await
        .unwrap();

    assert!(err.to_string().contains("did not match expected head"));
    assert_eq!(count, 0);
}

#[tokio::test]
async fn session_operations_persist_across_reopen() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let root_id = store.root_id();
    let session = store.append(session_anchor_node(&root_id)).await.unwrap();
    store.fork("main", &session).await.unwrap();
    let text = store
        .append(NewNode {
            parent: session.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("text".to_owned()),
        })
        .await
        .unwrap();
    store
        .set_branch_head("main", &session, &text)
        .await
        .unwrap();
    store
        .set_session_state(
            "main",
            Some(&SessionState::Active),
            SessionState::Paused {
                target_branch: String::new(),
                reason: PauseReason::Closed,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        session_summary(&store, "main").await,
        SessionSummaryRow {
            state: "paused".to_owned(),
            target_branch: Some(String::new()),
            base_head_id: None,
            pause_reason: Some("closed".to_owned()),
            merged_anchor_id: None,
        }
    );

    let rebased = store
        .rebase_session(
            "main",
            &SessionAnchorPatch {
                model: Some("gpt-5.5".to_owned()),
                ..SessionAnchorPatch::default()
            },
        )
        .await
        .unwrap();
    let handoff = store
        .handoff_session("main", &SessionAnchorPatch::default(), "next prompt")
        .await
        .unwrap();

    let reopened = SqliteStore::open(&path).await.unwrap();

    assert_eq!(reopened.get_branch_head("main").await.unwrap(), handoff);
    assert_eq!(
        reopened.get_session_state("main").await.unwrap(),
        SessionState::Paused {
            target_branch: String::new(),
            reason: PauseReason::Closed,
        }
    );
    assert!(reopened.get_node(&rebased).await.is_ok());
    assert!(reopened.get_node(&handoff).await.is_ok());
}

#[tokio::test]
async fn job_operations_persist_across_reopen() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let root_id = store.root_id();
    let session = store.append(session_anchor_node(&root_id)).await.unwrap();
    store.fork("main", &session).await.unwrap();

    let job = store
        .submit_job_with_id("job-test", "main", &session)
        .await
        .unwrap();
    assert_eq!(job.status, JobStatus::Queued);
    let job = store
        .set_job_status("job-test", JobStatus::Queued, JobStatus::Running)
        .await
        .unwrap();
    assert_eq!(job.status, JobStatus::Running);
    assert_eq!(
        job_summary(&store, "job-test").await,
        JobSummaryRow {
            created_at: job.created_at.to_string(),
            finished_at: None,
            branch: "main".to_owned(),
            work_branch: "main".to_owned(),
            base: session.clone(),
            status: "running".to_owned(),
        }
    );

    let reopened = SqliteStore::open(&path).await.unwrap();
    let job = reopened.get_job("job-test").await.unwrap();

    assert_eq!(job.status, JobStatus::Running);
    assert_eq!(job.branch, "main");
    assert_eq!(job.base, session);
}

#[tokio::test]
async fn message_queue_operations_persist_across_reopen() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let first = store
        .enqueue_message("runner", serde_json::json!({"index": 1}))
        .await
        .unwrap();
    let second = store
        .enqueue_message("runner", serde_json::json!({"index": 2}))
        .await
        .unwrap();

    let reopened = SqliteStore::open(&path).await.unwrap();
    let messages = reopened.list_queue_messages("runner").await.unwrap();
    assert_eq!(messages[0].message_id, first.message_id);
    assert_eq!(messages[1].message_id, second.message_id);
    assert_eq!(
        reopened
            .peek_message("runner")
            .await
            .unwrap()
            .unwrap()
            .payload["index"],
        1
    );

    let dequeued = reopened.dequeue_message("runner").await.unwrap().unwrap();
    assert_eq!(dequeued.message_id, first.message_id);
    let reopened = SqliteStore::open(&path).await.unwrap();
    let messages = reopened.list_queue_messages("runner").await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].message_id, second.message_id);
}

#[tokio::test]
async fn message_queue_preserves_insert_order_for_equal_timestamps() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let created_at = "2026-01-01T00:00:00Z".parse().unwrap();
    let first = MessageQueueItem {
        message_id: "z-first".to_owned(),
        queue: "runner".to_owned(),
        created_at,
        payload: serde_json::json!({"index": 1}),
    };
    let second = MessageQueueItem {
        message_id: "a-second".to_owned(),
        queue: "runner".to_owned(),
        created_at,
        payload: serde_json::json!({"index": 2}),
    };
    let mut connection = store.connect().await.unwrap();
    super::persist_message_queue_item(&mut connection, &store.database_path, &first)
        .await
        .unwrap();
    super::persist_message_queue_item(&mut connection, &store.database_path, &second)
        .await
        .unwrap();

    let reopened = SqliteStore::open(&path).await.unwrap();
    let messages = reopened.list_queue_messages("runner").await.unwrap();

    assert_eq!(messages[0].message_id, first.message_id);
    assert_eq!(messages[1].message_id, second.message_id);
}

#[tokio::test]
async fn message_queue_sorts_by_parsed_timestamp() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let first = MessageQueueItem {
        message_id: "first".to_owned(),
        queue: "runner".to_owned(),
        created_at: "2026-01-01T00:00:00Z".parse().unwrap(),
        payload: serde_json::json!({"index": 1}),
    };
    let second = MessageQueueItem {
        message_id: "second".to_owned(),
        queue: "runner".to_owned(),
        created_at: "2026-01-01T00:00:00.001Z".parse().unwrap(),
        payload: serde_json::json!({"index": 2}),
    };
    let mut connection = store.connect().await.unwrap();
    super::persist_message_queue_item(&mut connection, &store.database_path, &first)
        .await
        .unwrap();
    super::persist_message_queue_item(&mut connection, &store.database_path, &second)
        .await
        .unwrap();

    let reopened = SqliteStore::open(&path).await.unwrap();
    let messages = reopened.list_queue_messages("runner").await.unwrap();

    assert_eq!(messages[0].message_id, first.message_id);
    assert_eq!(messages[1].message_id, second.message_id);
}

#[tokio::test]
async fn preset_operations_persist_across_reopen() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();

    let first = store
        .set_preset("default", preset("gpt-5.4"))
        .await
        .unwrap();
    assert_eq!(first.current_version, 1);
    let second = store
        .set_preset("default", preset("gpt-5.5"))
        .await
        .unwrap();
    assert_eq!(second.current_version, 2);
    let rolled_back = store.rollback_preset("default", 1).await.unwrap();
    assert_eq!(rolled_back.current_version, 3);

    let reopened = SqliteStore::open(&path).await.unwrap();
    let record = reopened.get_preset_record("default").await.unwrap();

    assert_eq!(record.current_version, 3);
    assert_eq!(record.current_preset().unwrap().model, "gpt-5.4");
}

#[tokio::test]
async fn skill_operations_persist_across_reopen() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    assert!(
        store
            .get_skill(SessionRole::Orchestrator, "coco-orchestrator")
            .await
            .is_ok()
    );

    let created = store
        .add_skill(
            SessionRole::Runner,
            "custom-runner",
            SkillVersionSpec {
                description: "custom".to_owned(),
                body: "run".to_owned(),
                scripts: vec![],
                enable_coco_shim: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(created.current_version, 1);
    let updated = store
        .update_skill(
            SessionRole::Runner,
            "custom-runner",
            &SkillUpdatePatch {
                body: Some("run updated".to_owned()),
                ..SkillUpdatePatch::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.current_version, 2);
    let rolled_back = store
        .rollback_skill(SessionRole::Runner, "custom-runner", 1)
        .await
        .unwrap();
    assert_eq!(rolled_back.current_version, 3);

    let reopened = SqliteStore::open(&path).await.unwrap();
    let record = reopened
        .get_skill(SessionRole::Runner, "custom-runner")
        .await
        .unwrap();

    assert_eq!(record.current_version, 3);
    assert_eq!(record.current().unwrap().body, "run");
}

#[tokio::test]
async fn updating_builtin_skill_materializes_existing_history() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let original = store
        .get_skill(SessionRole::Orchestrator, "cronjob")
        .await
        .unwrap();

    let updated = store
        .update_skill(
            SessionRole::Orchestrator,
            "cronjob",
            &SkillUpdatePatch {
                body: Some("updated body".to_owned()),
                ..SkillUpdatePatch::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        updated.versions.keys().copied().collect::<Vec<_>>(),
        vec![1, 2]
    );
    drop(store);

    let reopened = SqliteStore::open(&path).await.unwrap();
    let record = reopened
        .get_skill(SessionRole::Orchestrator, "cronjob")
        .await
        .unwrap();
    assert_eq!(
        record.versions.keys().copied().collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(
        record.versions.get(&1).unwrap().id,
        original.versions.get(&1).unwrap().id
    );
    assert_eq!(record.current().unwrap().body, "updated body");

    let rolled_back = reopened
        .rollback_skill(SessionRole::Orchestrator, "cronjob", 1)
        .await
        .unwrap();
    assert_eq!(rolled_back.current_version, 3);
    assert_eq!(
        rolled_back.current().unwrap().body,
        original.current().unwrap().body
    );
    assert_eq!(
        rolled_back.current().unwrap().scripts,
        original.current().unwrap().scripts
    );
}
