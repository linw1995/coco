use std::collections::HashMap;
use std::fs;
use std::os::fd::AsRawFd;
use std::path::Path;

use super::fs::FsStore;
use super::memory::MemoryStore;
use super::state::StoreState;
use crate::{
    Anchor, BranchStore, JobStatus, JobStore, Kind, MergeParent, MessageQueueItem,
    MessageQueueStore, NewNode, Node, NodeStore, PauseReason, Preset, PresetStore, PromptAnchor,
    Role, SessionAnchor, SessionAnchorPatch, SessionRole, SessionState, SessionStore, SkillScript,
    SkillStore, SkillUpdatePatch, SkillVersionSpec, StoreError as Error,
};
use serde_json::json;

fn make_text_node(parent: &str, text: &str) -> NewNode {
    NewNode {
        parent: parent.to_owned(),
        role: Role::User,
        metadata: None,
        kind: Kind::Text(text.to_owned()),
    }
}

fn make_session_anchor_node(parent: &str) -> NewNode {
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
                additional_params: Some(json!({"reasoning_effort": "low"})),
                enable_coco_shim: false,
                active_skill: None,
            },
        )),
    }
}

fn make_preset(model: &str, role: SessionRole) -> Preset {
    Preset {
        role,
        provider_profile: "openai".to_owned(),
        model: model.to_owned(),
        tools: vec![],
        system_prompt: "preset system".to_owned(),
        prompt: "preset prompt".to_owned(),
        temperature: Some(0.2),
        max_tokens: Some(256),
        additional_params: Some(json!({"reasoning_effort": "medium"})),
        enable_coco_shim: true,
    }
}

fn current_preset(store: &impl PresetStore, name: &str) -> std::result::Result<Preset, Error> {
    let record = store.get_preset_record(name)?;
    record
        .current_preset()
        .ok_or_else(|| Error::PresetVersionNotFound {
            name: name.to_owned(),
            version: record.current_version,
        })
}

fn make_session_anchor_with_merge_parent(parent: &str, merge_parent: &str) -> NewNode {
    NewNode {
        parent: parent.to_owned(),
        role: Role::System,
        metadata: None,
        kind: Kind::Anchor(Anchor::session(
            vec![MergeParent::merge(merge_parent)],
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
                additional_params: Some(json!({"reasoning_effort": "low"})),
                enable_coco_shim: false,
                active_skill: None,
            },
        )),
    }
}

fn make_prompt_anchor_node(parent: &str, merge_parents: &[&str]) -> NewNode {
    NewNode {
        parent: parent.to_owned(),
        role: Role::System,
        metadata: None,
        kind: Kind::Anchor(Anchor::prompt(
            merge_parents
                .iter()
                .map(|id| MergeParent::merge(*id))
                .collect(),
            PromptAnchor {
                prompt: "merge prompt".to_owned(),
            },
        )),
    }
}

fn submit_prompt_job<S>(store: &S, branch: &str, prompt: &str) -> crate::Job
where
    S: BranchStore + JobStore + NodeStore,
{
    let parent = store.get_branch_head(branch).unwrap();
    let prompt_anchor_id = store
        .append(NewNode {
            parent,
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::prompt(
                vec![],
                PromptAnchor {
                    prompt: prompt.to_owned(),
                },
            )),
        })
        .unwrap();
    store.submit_job(branch, &prompt_anchor_id).unwrap()
}

fn read_jsonl_nodes(path: &Path) -> Vec<Node> {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn read_jsonl_values(path: &Path) -> Vec<serde_json::Value> {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn write_jsonl_values(path: &Path, values: &[serde_json::Value]) {
    let mut text = values
        .iter()
        .map(|value| serde_json::to_string(value).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    text.push('\n');
    fs::write(path, text).unwrap();
}

fn temp_store_path() -> (tempfile::TempDir, std::path::PathBuf) {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    (tempdir, path)
}

trait InspectableStore {
    fn snapshot_state(&self) -> StoreState;
}

trait TestStoreFactory {
    type Backend: PresetStore
        + BranchStore
        + InspectableStore
        + JobStore
        + MessageQueueStore
        + NodeStore
        + SessionStore
        + SkillStore;

    fn create() -> Self::Backend;
}

impl InspectableStore for MemoryStore {
    fn snapshot_state(&self) -> StoreState {
        self.snapshot_state()
    }
}

impl InspectableStore for FsStore {
    fn snapshot_state(&self) -> StoreState {
        self.snapshot_state()
    }
}

struct MemoryFactory;

impl TestStoreFactory for MemoryFactory {
    type Backend = MemoryStore;

    fn create() -> Self::Backend {
        MemoryStore::new()
    }
}

struct FsFactory;

impl TestStoreFactory for FsFactory {
    type Backend = FsStore;

    fn create() -> Self::Backend {
        let tempdir = tempfile::tempdir().expect("temporary directory should be created");
        let path = tempdir.path().join("store");
        let store = FsStore::open(&path).expect("file system store should open");
        std::mem::forget(tempdir);
        store
    }
}

fn assert_new_store_exposes_root_text_node<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let snapshot = store.snapshot_state();
    let root = snapshot.nodes.get(snapshot.root_id()).unwrap();

    let Kind::Text(text) = &root.kind else {
        panic!("expected text root node");
    };
    assert_eq!(text, "The Big Bang");
    assert!(root.is_root());
}

fn assert_append_inserts_node_and_updates_children_index<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let session_id = store.append(make_session_anchor_node(&root_id)).unwrap();

    let child_id = store.append(make_text_node(&session_id, "child")).unwrap();

    let snapshot = store.snapshot_state();
    let stored = snapshot.nodes.get(&child_id).unwrap();
    assert_eq!(stored.parent, session_id);
    assert!(
        snapshot
            .children
            .get(&stored.parent)
            .unwrap()
            .contains(&child_id)
    );
}

fn assert_append_rejects_missing_parent<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let err = store
        .append(make_text_node("missing", "child"))
        .unwrap_err();

    assert!(matches!(err, Error::ParentNotFound { id } if id == "missing"));
}

fn assert_append_prompt_anchor_indexes_merge_parents_as_children<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let session_id = store.append(make_session_anchor_node(&root_id)).unwrap();
    let merge_parent_id = store
        .append(make_text_node(&session_id, "merge-parent"))
        .unwrap();

    let anchor_id = store
        .append(make_prompt_anchor_node(&session_id, &[&merge_parent_id]))
        .unwrap();

    let snapshot = store.snapshot_state();
    assert!(
        snapshot
            .children
            .get(&session_id)
            .unwrap()
            .contains(&anchor_id)
    );
    assert!(
        snapshot
            .children
            .get(&merge_parent_id)
            .unwrap()
            .contains(&anchor_id)
    );
}

fn assert_append_session_anchor_indexes_merge_parents_as_children<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let merge_parent_id = store
        .append(make_text_node(&root_id, "merge-parent"))
        .unwrap();

    let anchor_id = store
        .append(make_session_anchor_with_merge_parent(
            &root_id,
            &merge_parent_id,
        ))
        .unwrap();

    let snapshot = store.snapshot_state();
    assert!(
        snapshot
            .children
            .get(&merge_parent_id)
            .unwrap()
            .contains(&anchor_id)
    );
}

fn assert_append_prompt_anchor_rejects_missing_merge_parent<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let session_id = store.append(make_session_anchor_node(&root_id)).unwrap();
    let err = store
        .append(make_prompt_anchor_node(&session_id, &["missing"]))
        .unwrap_err();

    assert!(matches!(err, Error::ParentNotFound { id } if id == "missing"));
}

fn assert_append_prompt_anchor_rejects_duplicate_merge_parents<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let session_id = store.append(make_session_anchor_node(&root_id)).unwrap();
    let merge_parent_id = store
        .append(make_text_node(&session_id, "merge-parent"))
        .unwrap();
    let err = store
        .append(make_prompt_anchor_node(
            &session_id,
            &[&merge_parent_id, &merge_parent_id],
        ))
        .unwrap_err();

    assert!(matches!(
        err,
        Error::DuplicateMergeParent { id } if id == merge_parent_id
    ));
}

fn assert_append_prompt_anchor_rejects_multiple_shadow_parents<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let session_id = store.append(make_session_anchor_node(&root_id)).unwrap();
    let left_shadow = store
        .append(make_text_node(&session_id, "left-shadow"))
        .unwrap();
    let right_shadow = store
        .append(make_text_node(&session_id, "right-shadow"))
        .unwrap();
    let err = store
        .append(NewNode {
            parent: session_id,
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::prompt(
                vec![
                    MergeParent::shadow(left_shadow.clone()),
                    MergeParent::shadow(right_shadow.clone()),
                ],
                PromptAnchor {
                    prompt: "shadow prompt".to_owned(),
                },
            )),
        })
        .unwrap_err();

    assert!(matches!(
        err,
        Error::MultipleShadowParents { ids }
            if ids == vec![left_shadow, right_shadow]
    ));
}

fn assert_append_prompt_anchor_rejects_merge_parent_matching_primary_parent<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let session_id = store.append(make_session_anchor_node(&root_id)).unwrap();
    let err = store
        .append(make_prompt_anchor_node(&session_id, &[&session_id]))
        .unwrap_err();

    assert!(matches!(
        err,
        Error::MergeParentMatchesParent { id } if id == session_id
    ));
}

fn assert_append_prompt_anchor_allows_merge_parent_from_other_session_root<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let left_root = store.append(make_session_anchor_node(&root_id)).unwrap();
    let right_root = store.append(make_session_anchor_node(&root_id)).unwrap();
    let left_leaf = store.append(make_text_node(&left_root, "left")).unwrap();
    let right_leaf = store.append(make_text_node(&right_root, "right")).unwrap();

    let merge_id = store
        .append(make_prompt_anchor_node(&left_leaf, &[&right_leaf]))
        .unwrap();

    let snapshot = store.snapshot_state();
    assert!(
        snapshot
            .children
            .get(&right_leaf)
            .is_some_and(|children| children.contains(&merge_id))
    );
}

fn assert_ancestry_returns_nodes_back_to_root<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let session_id = store.append(make_session_anchor_node(&root_id)).unwrap();
    let a_id = store.append(make_text_node(&session_id, "a")).unwrap();
    let b_id = store.append(make_text_node(&a_id, "b")).unwrap();

    let ancestry = store.ancestry(&b_id).unwrap();
    let ids: Vec<_> = ancestry.into_iter().map(|node| node.id).collect();

    assert_eq!(ids, vec![b_id, a_id, session_id, root_id]);
}

fn assert_list_children_returns_primary_and_merge_children_in_stable_order<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let session_id = store.append(make_session_anchor_node(&root_id)).unwrap();
    let left_id = store.append(make_text_node(&session_id, "left")).unwrap();
    let right_id = store
        .append(make_prompt_anchor_node(&left_id, &[&session_id]))
        .unwrap();

    let nodes = store.list_children(&session_id).unwrap();
    let ids = nodes.into_iter().map(|node| node.id).collect::<Vec<_>>();

    assert_eq!(ids, vec![left_id, right_id]);
}

fn assert_list_children_returns_empty_for_leaf_node<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let session_id = store.append(make_session_anchor_node(&root_id)).unwrap();
    let leaf_id = store.append(make_text_node(&session_id, "leaf")).unwrap();

    let children = store.list_children(&leaf_id).unwrap();

    assert!(children.is_empty());
}

fn assert_list_children_returns_not_found_for_missing_node<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let missing_id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    let err = store.list_children(missing_id).unwrap_err();

    assert!(matches!(err, Error::NotFound { id } if id == missing_id));
}

fn assert_log_returns_nodes_from_head_back_to_base_inclusive<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let session_id = store.append(make_session_anchor_node(&root_id)).unwrap();
    let a_id = store.append(make_text_node(&session_id, "a")).unwrap();
    let b_id = store.append(make_text_node(&a_id, "b")).unwrap();
    let c_id = store.append(make_text_node(&b_id, "c")).unwrap();

    let log = store.log(&a_id, &c_id).unwrap();
    let ids: Vec<_> = log.into_iter().map(|node| node.id).collect();

    assert_eq!(ids, vec![c_id, b_id, a_id]);
}

fn assert_log_returns_single_node_when_base_equals_head<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();

    let log = store.log(&root_id, &root_id).unwrap();
    let ids: Vec<_> = log.into_iter().map(|node| node.id).collect();

    assert_eq!(ids, vec![root_id]);
}

fn assert_log_returns_not_found_when_head_is_missing<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let missing_id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    let err = store.log(&root_id, missing_id).unwrap_err();

    assert!(matches!(err, Error::NotFound { id } if id == missing_id));
}

fn assert_log_ignores_prompt_anchor_parents<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let session_id = store.append(make_session_anchor_node(&root_id)).unwrap();
    let merge_parent_id = store
        .append(make_text_node(&session_id, "merge-parent"))
        .unwrap();
    let anchor_id = store
        .append(make_prompt_anchor_node(&session_id, &[&merge_parent_id]))
        .unwrap();

    let err = store.log(&merge_parent_id, &anchor_id).unwrap_err();

    assert!(matches!(
        err,
        Error::RefsNotConnected {
            base_ref,
            head_ref,
        } if base_ref == merge_parent_id && head_ref == anchor_id
    ));
}

fn assert_branch_creation_resolves_refs<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();

    let head_id = store.fork("main", &root_id).unwrap();

    assert_eq!(head_id, root_id);
    assert_eq!(store.get_branch_head("main").unwrap(), root_id);
}

fn assert_fork_rejects_duplicates<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    store.fork("base", &root_id).unwrap();
    store.fork("main", &root_id).unwrap();

    let err = store.fork("main", &root_id).unwrap_err();

    assert!(matches!(err, Error::BranchExists { name } if name == "main"));
}

fn assert_fork_initializes_session_state<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    store.fork("main", &root_id).unwrap();
    let state = store.get_session_state("main").unwrap();

    assert_eq!(state, SessionState::Active);
}

fn assert_set_branch_head_keeps_session_state_untouched<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let child_id = store.append(make_text_node(&root_id, "child")).unwrap();
    let next_id = store.append(make_text_node(&child_id, "next")).unwrap();
    store.fork("main", &child_id).unwrap();

    store.set_branch_head("main", &child_id, &next_id).unwrap();
    let state = store.get_session_state("main").unwrap();

    assert_eq!(store.get_branch_head("main").unwrap(), next_id);
    assert_eq!(state, SessionState::Active);
}

fn assert_delete_branch_removes_branch_and_session_state<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let branch_head = store.fork("main", &root_id).unwrap();

    store.delete_branch("main").unwrap();

    let err = store.get_branch_head("main").unwrap_err();
    assert!(matches!(err, Error::BranchNotFound { name } if name == "main"));
    let err = store.get_session_state("main").unwrap_err();
    assert!(matches!(err, Error::BranchNotFound { name } if name == "main"));
    assert_eq!(store.get_node(&branch_head).unwrap().id, branch_head);
}

fn assert_delete_branch_rejects_missing_branch<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();

    let err = store.delete_branch("missing").unwrap_err();

    assert!(matches!(err, Error::BranchNotFound { name } if name == "missing"));
}

fn assert_set_session_state_updates_value<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    store.fork("base", &root_id).unwrap();
    store.fork("main", &root_id).unwrap();

    let state = store
        .set_session_state(
            "main",
            Some(&SessionState::Active),
            SessionState::Attached {
                target_branch: "base".to_owned(),
                base_head_id: root_id.clone(),
            },
        )
        .unwrap();

    assert_eq!(
        state,
        SessionState::Attached {
            target_branch: "base".to_owned(),
            base_head_id: root_id,
        }
    );
}

fn assert_set_session_state_requires_matching_expected<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    store.fork("base", &root_id).unwrap();
    store.fork("main", &root_id).unwrap();
    store
        .set_session_state(
            "main",
            Some(&SessionState::Active),
            SessionState::Paused {
                target_branch: "base".to_owned(),
                reason: PauseReason::Closed,
            },
        )
        .unwrap();

    let err = store
        .set_session_state(
            "main",
            Some(&SessionState::Active),
            SessionState::Attached {
                target_branch: "base".to_owned(),
                base_head_id: root_id,
            },
        )
        .unwrap_err();

    assert!(matches!(
        err,
        Error::SessionStateMoved {
            name,
            expected,
            actual,
        } if name == "main" && expected == "Active" && actual.contains("Paused")
    ));
}

fn assert_set_branch_head_preserves_attached_state<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let base_anchor_id = store.append(make_session_anchor_node(&root_id)).unwrap();
    store.fork("base", &base_anchor_id).unwrap();
    store.fork("main", &root_id).unwrap();

    store
        .set_session_state(
            "main",
            Some(&SessionState::Active),
            SessionState::Attached {
                target_branch: "base".to_owned(),
                base_head_id: base_anchor_id,
            },
        )
        .unwrap();

    let feedback_id = store.append(make_text_node(&root_id, "feedback")).unwrap();
    store
        .set_branch_head("main", &root_id, &feedback_id)
        .unwrap();

    let state = store.get_session_state("main").unwrap();
    assert_eq!(store.get_branch_head("main").unwrap(), feedback_id);
    assert_eq!(
        state,
        SessionState::Attached {
            target_branch: "base".to_owned(),
            base_head_id: store.get_branch_head("base").unwrap(),
        }
    );
}

fn assert_set_session_state_accepts_merged_anchor_on_target_branch<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let base_anchor_id = store.append(make_session_anchor_node(&root_id)).unwrap();
    store.fork("base", &base_anchor_id).unwrap();
    store.fork("main", &root_id).unwrap();

    let state = store
        .set_session_state(
            "main",
            Some(&SessionState::Active),
            SessionState::Paused {
                target_branch: "base".to_owned(),
                reason: PauseReason::Merged {
                    merged_anchor_id: base_anchor_id.clone(),
                },
            },
        )
        .unwrap();

    assert_eq!(
        state,
        SessionState::Paused {
            target_branch: "base".to_owned(),
            reason: PauseReason::Merged {
                merged_anchor_id: base_anchor_id,
            },
        }
    );
}

fn assert_set_session_state_rejects_merged_anchor_outside_target_branch<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let base_anchor_id = store.append(make_session_anchor_node(&root_id)).unwrap();
    let other_anchor_id = store.append(make_session_anchor_node(&root_id)).unwrap();
    store.fork("base", &base_anchor_id).unwrap();
    store.fork("other", &other_anchor_id).unwrap();
    store.fork("main", &root_id).unwrap();

    let err = store
        .set_session_state(
            "main",
            Some(&SessionState::Active),
            SessionState::Paused {
                target_branch: "base".to_owned(),
                reason: PauseReason::Merged {
                    merged_anchor_id: other_anchor_id.clone(),
                },
            },
        )
        .unwrap_err();

    assert!(matches!(
        err,
        Error::RefsNotConnected { base_ref, head_ref }
            if base_ref == other_anchor_id && head_ref == "base"
    ));
}

fn assert_set_session_state_rejects_non_anchor_merged_node<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let base_text_id = store.append(make_text_node(&root_id, "base text")).unwrap();
    store.fork("base", &base_text_id).unwrap();
    store.fork("main", &root_id).unwrap();

    let err = store
        .set_session_state(
            "main",
            Some(&SessionState::Active),
            SessionState::Paused {
                target_branch: "base".to_owned(),
                reason: PauseReason::Merged {
                    merged_anchor_id: base_text_id.clone(),
                },
            },
        )
        .unwrap_err();

    assert!(matches!(err, Error::InvalidAnchor { id } if id == base_text_id));
}

fn assert_paused_merged_state_can_resume_as_attached<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let base_anchor_id = store.append(make_session_anchor_node(&root_id)).unwrap();
    store.fork("base", &base_anchor_id).unwrap();
    store.fork("main", &root_id).unwrap();

    store
        .set_session_state(
            "main",
            Some(&SessionState::Active),
            SessionState::Paused {
                target_branch: "base".to_owned(),
                reason: PauseReason::Merged {
                    merged_anchor_id: base_anchor_id.clone(),
                },
            },
        )
        .unwrap();

    let next_base_anchor_id = store
        .append(make_prompt_anchor_node(&base_anchor_id, &[]))
        .unwrap();
    store
        .set_branch_head("base", &base_anchor_id, &next_base_anchor_id)
        .unwrap();
    let state = store
        .set_session_state(
            "main",
            Some(&SessionState::Paused {
                target_branch: "base".to_owned(),
                reason: PauseReason::Merged {
                    merged_anchor_id: base_anchor_id,
                },
            }),
            SessionState::Attached {
                target_branch: "base".to_owned(),
                base_head_id: next_base_anchor_id.clone(),
            },
        )
        .unwrap();

    assert_eq!(
        state,
        SessionState::Attached {
            target_branch: "base".to_owned(),
            base_head_id: next_base_anchor_id,
        }
    );
}

fn assert_paused_closed_state_can_resume_as_attached<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let base_anchor_id = store.append(make_session_anchor_node(&root_id)).unwrap();
    store.fork("base", &base_anchor_id).unwrap();
    store.fork("main", &root_id).unwrap();

    store
        .set_session_state(
            "main",
            Some(&SessionState::Active),
            SessionState::Paused {
                target_branch: "base".to_owned(),
                reason: PauseReason::Closed,
            },
        )
        .unwrap();

    let next_base_anchor_id = store
        .append(make_prompt_anchor_node(&base_anchor_id, &[]))
        .unwrap();
    store
        .set_branch_head("base", &base_anchor_id, &next_base_anchor_id)
        .unwrap();
    let state = store
        .set_session_state(
            "main",
            Some(&SessionState::Paused {
                target_branch: "base".to_owned(),
                reason: PauseReason::Closed,
            }),
            SessionState::Attached {
                target_branch: "base".to_owned(),
                base_head_id: next_base_anchor_id.clone(),
            },
        )
        .unwrap();

    assert_eq!(
        state,
        SessionState::Attached {
            target_branch: "base".to_owned(),
            base_head_id: next_base_anchor_id,
        }
    );
}

fn assert_list_session_states_returns_branch_state_map<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    store.fork("base", &root_id).unwrap();
    store.fork("main", &root_id).unwrap();
    store
        .set_session_state(
            "main",
            Some(&SessionState::Active),
            SessionState::Attached {
                target_branch: "base".to_owned(),
                base_head_id: root_id.clone(),
            },
        )
        .unwrap();

    let states = store.list_session_states().unwrap();

    assert_eq!(states.get("base"), Some(&SessionState::Active));
    assert_eq!(
        states.get("main"),
        Some(&SessionState::Attached {
            target_branch: "base".to_owned(),
            base_head_id: root_id,
        })
    );
}

fn assert_preset_round_trip<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let preset_name = "coding";
    let config = make_preset("gpt-5.4", SessionRole::Orchestrator);

    let stored = store.set_preset(preset_name, config.clone()).unwrap();

    assert_eq!(stored.current_version, 1);
    assert_eq!(stored.current_preset(), Some(config.clone()));
    assert_eq!(stored.versions.keys().copied().collect::<Vec<_>>(), vec![1]);
    assert_eq!(current_preset(&store, preset_name).unwrap(), config);
    assert_eq!(
        store
            .get_preset_record(preset_name)
            .unwrap()
            .current_version,
        1
    );
    assert_eq!(
        store
            .list_preset_records()
            .unwrap()
            .get(preset_name)
            .unwrap()
            .current_preset(),
        Some(config)
    );
}

fn assert_preset_replaces_existing_value<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let preset_name = "coding";
    store
        .set_preset(
            preset_name,
            make_preset("gpt-5.4", SessionRole::Orchestrator),
        )
        .unwrap();
    let updated = make_preset("claude-sonnet-4-20250514", SessionRole::Runner);

    let stored = store.set_preset(preset_name, updated.clone()).unwrap();

    assert_eq!(stored.current_version, 2);
    assert_eq!(stored.current_preset(), Some(updated.clone()));
    assert_eq!(
        stored.versions.keys().copied().collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(
        stored.versions.get(&1).unwrap().to_preset(),
        make_preset("gpt-5.4", SessionRole::Orchestrator)
    );
    assert_eq!(stored.versions.get(&2).unwrap().to_preset(), updated);
    assert_eq!(current_preset(&store, preset_name).unwrap(), updated);
}

fn assert_rollback_preset_creates_new_current_version<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let preset_name = "coding";
    let original = make_preset("gpt-5.4", SessionRole::Orchestrator);
    let updated = make_preset("claude-sonnet-4-20250514", SessionRole::Runner);
    store.set_preset(preset_name, original.clone()).unwrap();
    store.set_preset(preset_name, updated.clone()).unwrap();

    let rolled_back = store.rollback_preset(preset_name, 1).unwrap();

    assert_eq!(rolled_back.current_version, 3);
    assert_eq!(
        rolled_back.versions.keys().copied().collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(rolled_back.current_preset(), Some(original.clone()));
    assert_eq!(rolled_back.versions.get(&2).unwrap().to_preset(), updated);
    assert_eq!(current_preset(&store, preset_name).unwrap(), original);
}

fn assert_delete_preset_removes_only_config<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let preset_name = "coding";
    store.fork("main", &root_id).unwrap();
    store
        .set_preset(
            preset_name,
            make_preset("gpt-5.4", SessionRole::Orchestrator),
        )
        .unwrap();

    store.delete_preset(preset_name).unwrap();

    assert!(store.list_preset_records().unwrap().is_empty());
    assert!(matches!(
        store.get_preset_record(preset_name),
        Err(Error::PresetNotFound { name }) if name == preset_name
    ));
    assert_eq!(store.get_branch_head("main").unwrap(), root_id);
}

fn assert_delete_branch_preserves_preset<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let preset_name = "coding";
    let config = make_preset("gpt-5.4", SessionRole::Orchestrator);
    store.fork("main", &root_id).unwrap();
    store.set_preset(preset_name, config.clone()).unwrap();

    store.delete_branch("main").unwrap();

    assert!(matches!(
        store.get_branch_head("main"),
        Err(Error::BranchNotFound { name }) if name == "main"
    ));
    assert_eq!(current_preset(&store, preset_name).unwrap(), config);
}

fn assert_get_preset_rejects_missing_config<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();

    let err = store.get_preset_record("missing-preset").unwrap_err();

    assert!(matches!(
        err,
        Error::PresetNotFound { name } if name == "missing-preset"
    ));
}

fn assert_delete_preset_rejects_missing_config<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();

    let err = store.delete_preset("missing-preset").unwrap_err();

    assert!(matches!(
        err,
        Error::PresetNotFound { name } if name == "missing-preset"
    ));
}

fn assert_get_node_supports_branch_name<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let branch_head = store.fork("draft", &root_id).unwrap();

    let node = store.get_node("draft").unwrap();

    assert_eq!(node.id, branch_head);
}

fn assert_get_node_supports_prefix_after_branch_delete<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    store.fork("draft", &root_id).unwrap();
    let draft_node = store
        .append(make_text_node(&root_id, "draft only"))
        .unwrap();
    store
        .set_branch_head("draft", &root_id, &draft_node)
        .unwrap();
    store.delete_branch("draft").unwrap();

    let prefix = &draft_node[..8];
    let node = store.get_node(prefix).unwrap();

    assert_eq!(node.id, draft_node);
}

fn assert_get_node_rejects_ambiguous_prefix<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let mut ids = vec![root_id.clone()];
    for index in 0..32 {
        ids.push(
            store
                .append(make_text_node(&root_id, &format!("node-{index}")))
                .unwrap(),
        );
    }

    let ambiguous = ids
        .into_iter()
        .fold(HashMap::<String, Vec<String>>::new(), |mut groups, id| {
            groups.entry(id[..1].to_owned()).or_default().push(id);
            groups
        })
        .into_iter()
        .find_map(|(prefix, matches)| (matches.len() > 1).then_some((prefix, matches)))
        .expect("expected at least one ambiguous one-character prefix");

    let err = store.get_node(&ambiguous.0).unwrap_err();

    assert!(matches!(
        err,
        Error::AmbiguousNodePrefix { prefix, matches }
            if prefix == ambiguous.0 && matches.len() == ambiguous.1.len()
    ));
}

fn assert_set_branch_head_requires_matching_expected_head<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let child_id = store.append(make_text_node(&root_id, "child")).unwrap();
    let next_id = store.append(make_text_node(&child_id, "next")).unwrap();
    store.fork("main", &child_id).unwrap();

    let err = store
        .set_branch_head("main", &root_id, &next_id)
        .unwrap_err();

    assert!(matches!(
        err,
        Error::BranchHeadMoved {
            name,
            expected,
            actual,
        } if name == "main" && expected == root_id && actual == child_id
    ));
}

fn assert_log_supports_branch_name_on_head_ref<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let child_id = store.append(make_text_node(&root_id, "child")).unwrap();
    store.fork("main", &child_id).unwrap();

    let log = store.log(&root_id, "main").unwrap();
    let ids: Vec<_> = log.into_iter().map(|node| node.id).collect();

    assert_eq!(ids, vec![child_id, root_id]);
}

fn assert_log_supports_branch_name_on_base_ref<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let child_id = store.append(make_text_node(&root_id, "child")).unwrap();
    let leaf_id = store.append(make_text_node(&child_id, "leaf")).unwrap();
    store.fork("base", &child_id).unwrap();

    let log = store.log("base", &leaf_id).unwrap();
    let ids: Vec<_> = log.into_iter().map(|node| node.id).collect();

    assert_eq!(ids, vec![leaf_id, child_id]);
}

fn assert_log_supports_branch_name_on_both_sides<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let child_id = store.append(make_text_node(&root_id, "child")).unwrap();
    let leaf_id = store.append(make_text_node(&child_id, "leaf")).unwrap();
    store.fork("base", &child_id).unwrap();
    store.fork("main", &leaf_id).unwrap();

    let log = store.log("base", "main").unwrap();
    let ids: Vec<_> = log.into_iter().map(|node| node.id).collect();

    assert_eq!(ids, vec![leaf_id, child_id]);
}

fn assert_log_returns_branch_not_found_when_branch_is_missing<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();

    let err = store.log(&root_id, "main").unwrap_err();

    assert!(matches!(err, Error::BranchNotFound { name } if name == "main"));
}

fn assert_rebase_session_rewrites_branch_chain_with_updated_config<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let session_id = store.append(make_session_anchor_node(&root_id)).unwrap();
    let child_id = store.append(make_text_node(&session_id, "child")).unwrap();
    let old_child = store.snapshot_state().nodes.get(&child_id).unwrap().clone();
    store.fork("main", &child_id).unwrap();

    let new_head = store
        .rebase_session(
            "main",
            &SessionAnchorPatch {
                provider: Some(Some("anthropic".to_owned())),
                model: Some("claude-sonnet-4-20250514".to_owned()),
                temperature: Some(None),
                ..SessionAnchorPatch::default()
            },
        )
        .unwrap();

    assert_ne!(new_head, child_id);
    let ancestry = store.ancestry("main").unwrap();
    assert_eq!(ancestry[0].id, new_head);
    assert!(matches!(&ancestry[0].kind, Kind::Text(text) if text == "child"));
    assert_ne!(ancestry[0].id, child_id);
    assert_eq!(ancestry[0].created_at, old_child.created_at);
    assert_ne!(ancestry[1].id, session_id);
    let Kind::Anchor(anchor) = &ancestry[1].kind else {
        panic!("expected session anchor");
    };
    let session = anchor.as_session().expect("expected session anchor");
    assert_eq!(session.provider.as_deref(), Some("anthropic"));
    assert_eq!(session.model, "claude-sonnet-4-20250514");
    assert_eq!(session.temperature, None);
    assert_eq!(
        ancestry[1].created_at,
        store
            .snapshot_state()
            .nodes
            .get(&session_id)
            .unwrap()
            .created_at
    );
    assert_eq!(ancestry[2].id, root_id);

    let snapshot = store.snapshot_state();
    let old_session = snapshot.nodes.get(&session_id).unwrap();
    let Kind::Anchor(old_anchor) = &old_session.kind else {
        panic!("expected original session anchor");
    };
    assert_eq!(
        old_anchor.as_session().unwrap().provider.as_deref(),
        Some("openai")
    );
}

fn assert_rebase_session_patch_system_prompt_rebuilds_branch_chain<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let session_id = store.append(make_session_anchor_node(&root_id)).unwrap();
    let child_id = store.append(make_text_node(&session_id, "child")).unwrap();
    store.fork("main", &child_id).unwrap();

    let new_head = store
        .rebase_session(
            "main",
            &SessionAnchorPatch {
                model: Some("claude-sonnet-4-20250514".to_owned()),
                system_prompt: Some("You are strict.".to_owned()),
                ..SessionAnchorPatch::default()
            },
        )
        .unwrap();

    let ancestry = store.ancestry("main").unwrap();
    assert_eq!(ancestry[0].id, new_head);
    assert_ne!(ancestry[1].id, session_id);
    let Kind::Anchor(anchor) = &ancestry[1].kind else {
        panic!("expected session anchor");
    };
    let session = anchor.as_session().expect("expected session anchor");
    assert_eq!(session.model, "claude-sonnet-4-20250514");
    assert_eq!(session.system_prompt, "You are strict.");

    let snapshot = store.snapshot_state();
    let old_session = snapshot.nodes.get(&session_id).unwrap();
    let Kind::Anchor(old_anchor) = &old_session.kind else {
        panic!("expected original session anchor");
    };
    assert_eq!(old_anchor.as_session().unwrap().system_prompt, "system");
}

fn assert_rebase_session_keeps_merge_parents_pointing_to_original_nodes<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let session_id = store.append(make_session_anchor_node(&root_id)).unwrap();
    let merge_source_id = store
        .append(make_text_node(&session_id, "merge-source"))
        .unwrap();
    let anchor_id = store
        .append(make_prompt_anchor_node(&merge_source_id, &[&session_id]))
        .unwrap();
    store.fork("main", &anchor_id).unwrap();

    store
        .rebase_session(
            "main",
            &SessionAnchorPatch {
                model: Some("claude-sonnet-4-20250514".to_owned()),
                ..SessionAnchorPatch::default()
            },
        )
        .unwrap();

    let ancestry = store.ancestry("main").unwrap();
    let rebased_prompt = &ancestry[0];
    let rebased_merge_source = &ancestry[1];
    let rebased_session = &ancestry[2];
    let Kind::Anchor(anchor) = &rebased_prompt.kind else {
        panic!("expected prompt anchor");
    };
    assert_eq!(rebased_prompt.parent, rebased_merge_source.id);
    assert_ne!(rebased_session.id, session_id);
    assert_eq!(anchor.merge_parent_node_ids(), [session_id.as_str()]);
}

fn assert_rebase_session_keeps_other_branches_on_old_chain<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let session_id = store.append(make_session_anchor_node(&root_id)).unwrap();
    let child_id = store.append(make_text_node(&session_id, "child")).unwrap();
    store.fork("main", &child_id).unwrap();
    store.fork("draft", &child_id).unwrap();

    let new_head = store
        .rebase_session(
            "main",
            &SessionAnchorPatch {
                provider: Some(Some("anthropic".to_owned())),
                ..SessionAnchorPatch::default()
            },
        )
        .unwrap();

    assert_eq!(store.get_branch_head("draft").unwrap(), child_id);
    assert_eq!(store.get_branch_head("main").unwrap(), new_head);
    let draft_ancestry = store.ancestry("draft").unwrap();
    let Kind::Anchor(anchor) = &draft_ancestry[1].kind else {
        panic!("expected session anchor");
    };
    assert_eq!(
        anchor.as_session().unwrap().provider.as_deref(),
        Some("openai")
    );
    assert_eq!(draft_ancestry[1].id, session_id);
}

fn assert_rebase_session_requires_visible_session_anchor<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    store.fork("main", &root_id).unwrap();

    let err = store
        .rebase_session("main", &SessionAnchorPatch::default())
        .unwrap_err();

    assert!(matches!(
        err,
        Error::MissingSessionAnchor { branch } if branch == "main"
    ));
}

fn assert_rebase_session_preserves_created_at_across_rewritten_chain<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let session_id = store.append(make_session_anchor_node(&root_id)).unwrap();
    let child_id = store.append(make_text_node(&session_id, "child")).unwrap();
    let snapshot = store.snapshot_state();
    let session_created_at = snapshot.nodes.get(&session_id).unwrap().created_at;
    let child_created_at = snapshot.nodes.get(&child_id).unwrap().created_at;
    store.fork("main", &child_id).unwrap();

    store
        .rebase_session(
            "main",
            &SessionAnchorPatch {
                provider: Some(Some("anthropic".to_owned())),
                ..SessionAnchorPatch::default()
            },
        )
        .unwrap();

    let ancestry = store.ancestry("main").unwrap();
    assert_eq!(ancestry[0].created_at, child_created_at);
    assert_eq!(ancestry[1].created_at, session_created_at);
    assert_eq!(ancestry[2].id, root_id);
}

fn assert_handoff_session_appends_session_anchor_after_current_head<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let session_id = store.append(make_session_anchor_node(&root_id)).unwrap();
    let child_id = store.append(make_text_node(&session_id, "child")).unwrap();
    store.fork("main", &child_id).unwrap();

    let new_head = store
        .handoff_session("main", &SessionAnchorPatch::default())
        .unwrap();

    assert_ne!(new_head, child_id);
    let ancestry = store.ancestry("main").unwrap();
    assert_eq!(ancestry[0].id, new_head);
    assert_eq!(ancestry[0].parent, child_id);
    let Kind::Anchor(anchor) = &ancestry[0].kind else {
        panic!("expected session anchor");
    };
    let session = anchor.as_session().expect("expected session anchor");
    assert_eq!(session.provider.as_deref(), Some("openai"));
    assert_eq!(session.model, "gpt-5.4");
    assert_eq!(session.system_prompt, "system");
    assert_eq!(ancestry[1].id, child_id);
    assert_eq!(ancestry[2].id, session_id);
    assert_eq!(ancestry[3].id, root_id);
}

fn assert_handoff_session_requires_visible_session_anchor<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    store.fork("main", &root_id).unwrap();

    let err = store
        .handoff_session("main", &SessionAnchorPatch::default())
        .unwrap_err();

    assert!(matches!(
        err,
        Error::MissingSessionAnchor { branch } if branch == "main"
    ));
}

fn assert_handoff_session_applies_session_patch<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let session_id = store.append(make_session_anchor_node(&root_id)).unwrap();
    store.fork("main", &session_id).unwrap();

    let new_head = store
        .handoff_session(
            "main",
            &SessionAnchorPatch {
                model: Some("claude-sonnet-4-20250514".to_owned()),
                system_prompt: Some("You are strict.".to_owned()),
                max_tokens: Some(Some(256)),
                ..SessionAnchorPatch::default()
            },
        )
        .unwrap();

    let node = store.get_node(&new_head).unwrap();
    let Kind::Anchor(anchor) = &node.kind else {
        panic!("expected session anchor");
    };
    let session = anchor.as_session().expect("expected session anchor");
    assert_eq!(session.provider.as_deref(), Some("openai"));
    assert_eq!(session.model, "claude-sonnet-4-20250514");
    assert_eq!(session.system_prompt, "You are strict.");
    assert_eq!(session.max_tokens, Some(256));
}

fn assert_job_round_trip<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    store.fork("main", &root_id).unwrap();
    let job = submit_prompt_job(&store, "main", "hello");

    assert_eq!(store.get_job(&job.job_id).unwrap(), job);
}

fn assert_create_job_generates_unique_ids<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    store.fork("main", &root_id).unwrap();
    store.fork("draft", &root_id).unwrap();
    let first = submit_prompt_job(&store, "main", "hello");
    let second = submit_prompt_job(&store, "draft", "world");

    assert!(!first.job_id.is_empty());
    assert!(!second.job_id.is_empty());
    assert_ne!(first.job_id, second.job_id);
}

fn assert_finished_job_round_trip<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    store.fork("main", &root_id).unwrap();
    let job = submit_prompt_job(&store, "main", "hello");
    let running = store
        .set_job_status(&job.job_id, JobStatus::Queued, JobStatus::Running)
        .unwrap();
    assert!(running.finished_at.is_none());
    let finished = store
        .set_job_status(&job.job_id, JobStatus::Running, JobStatus::Finished)
        .unwrap();

    assert!(finished.finished_at.is_some());
    assert_eq!(store.get_job(&job.job_id).unwrap(), finished);
}

fn assert_set_job_status_rejects_invalid_transition<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    store.fork("main", &root_id).unwrap();
    let job = submit_prompt_job(&store, "main", "hello");

    let err = store
        .set_job_status(&job.job_id, JobStatus::Queued, JobStatus::Finished)
        .unwrap_err();

    assert!(matches!(
        err,
        Error::PromptJobInvalidStatusTransition { job_id, current, next }
            if job_id == job.job_id && current == "Queued" && next == "Finished"
    ));
}

fn assert_submit_job_rejects_second_active_job_on_same_branch<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    store.fork("main", &root_id).unwrap();
    let first = submit_prompt_job(&store, "main", "hello");

    let second_parent = store.get_branch_head("main").unwrap();
    let second_anchor_id = store
        .append(NewNode {
            parent: second_parent,
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::prompt(
                vec![],
                PromptAnchor {
                    prompt: "world".to_owned(),
                },
            )),
        })
        .unwrap();
    let err = store.submit_job("main", &second_anchor_id).unwrap_err();

    assert!(matches!(
        err,
        Error::PromptJobActiveOnBranch { branch, job_id }
            if branch == "main" && job_id == first.job_id
    ));
}

fn assert_submit_job_allows_new_job_after_previous_job_finishes<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    store.fork("main", &root_id).unwrap();
    let first = submit_prompt_job(&store, "main", "hello");
    store
        .set_job_status(&first.job_id, JobStatus::Queued, JobStatus::Running)
        .unwrap();
    store
        .set_job_status(&first.job_id, JobStatus::Running, JobStatus::Finished)
        .unwrap();

    let second = submit_prompt_job(&store, "main", "world");

    assert_ne!(first.job_id, second.job_id);
    assert_eq!(second.status, JobStatus::Queued);
}

fn assert_message_queue_round_trip<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let first = store
        .enqueue_message("hooks", json!({"text": "first"}))
        .unwrap();
    let second = store
        .enqueue_message("hooks", json!({"text": "second"}))
        .unwrap();

    assert_eq!(
        store.list_queue_messages("hooks").unwrap(),
        vec![first.clone(), second.clone(),]
    );
    assert_eq!(store.peek_message("hooks").unwrap(), Some(first.clone()));
    assert_eq!(store.peek_message("missing").unwrap(), None);
    assert_eq!(store.dequeue_message("hooks").unwrap(), Some(first));
    assert_eq!(store.peek_message("hooks").unwrap(), Some(second.clone()));
    assert_eq!(store.dequeue_message("hooks").unwrap(), Some(second));
    assert_eq!(store.dequeue_message("hooks").unwrap(), None);
    assert_eq!(store.peek_message("hooks").unwrap(), None);
    assert!(store.list_queue_messages("hooks").unwrap().is_empty());
}

fn assert_message_queue_isolates_named_queues<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let hook = store
        .enqueue_message("hooks", json!({"text": "hook"}))
        .unwrap();
    let scheduler = store
        .enqueue_message("scheduler", json!({"text": "scheduler"}))
        .unwrap();

    assert_eq!(
        store.list_queue_messages("hooks").unwrap(),
        vec![hook.clone()]
    );
    assert_eq!(
        store.list_queue_messages("scheduler").unwrap(),
        vec![scheduler.clone()]
    );
    assert_eq!(store.dequeue_message("hooks").unwrap(), Some(hook));
    assert_eq!(store.dequeue_message("scheduler").unwrap(), Some(scheduler));
}

fn assert_message_queue_ids_are_content_derived<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let first = store
        .enqueue_message("hooks", json!({"text": "same"}))
        .unwrap();
    let second = store
        .enqueue_message("hooks", json!({"text": "same"}))
        .unwrap();

    assert_ne!(first.message_id, second.message_id);
    assert_eq!(first.message_id.len(), 64);
    assert!(
        first
            .message_id
            .chars()
            .all(|ch| ch.is_ascii_hexdigit() && !ch.is_ascii_uppercase())
    );

    let same = MessageQueueItem::new(first.queue.clone(), first.payload.clone(), first.created_at);
    let different_queue = MessageQueueItem::new("other", first.payload.clone(), first.created_at);
    let different_payload = MessageQueueItem::new(
        first.queue.clone(),
        json!({"text": "different"}),
        first.created_at,
    );

    assert_eq!(same.message_id, first.message_id);
    assert_ne!(different_queue.message_id, first.message_id);
    assert_ne!(different_payload.message_id, first.message_id);
}

fn assert_add_skill_starts_at_version_one<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let record = store
        .add_skill(
            SessionRole::Orchestrator,
            "custom-orchestrator",
            SkillVersionSpec {
                description: "first".to_owned(),
                body: "body".to_owned(),
                scripts: Vec::new(),
                enable_coco_shim: true,
            },
        )
        .unwrap();

    assert_eq!(record.current_version, 1);
    assert_eq!(record.versions.keys().copied().collect::<Vec<_>>(), vec![1]);
    assert_eq!(record.current().unwrap().description, "first");
}

fn assert_update_skill_creates_new_version<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let first_script = SkillScript {
        path: "scripts/inspect.py".to_owned(),
        content: "print('v1')".to_owned(),
    };
    let second_script = SkillScript {
        path: "scripts/inspect.py".to_owned(),
        content: "print('v2')".to_owned(),
    };
    store
        .add_skill(
            SessionRole::Runner,
            "custom-runner",
            SkillVersionSpec {
                description: "v1".to_owned(),
                body: "body-v1".to_owned(),
                scripts: vec![first_script.clone()],
                enable_coco_shim: false,
            },
        )
        .unwrap();

    let updated = store
        .update_skill(
            SessionRole::Runner,
            "custom-runner",
            &SkillUpdatePatch {
                description: Some("v2".to_owned()),
                body: None,
                scripts: Some(vec![second_script.clone()]),
                enable_coco_shim: Some(true),
            },
        )
        .unwrap();

    assert_eq!(updated.current_version, 2);
    assert_eq!(
        updated.versions.keys().copied().collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(updated.versions.get(&1).unwrap().description, "v1");
    assert_eq!(
        updated.versions.get(&1).unwrap().scripts,
        vec![first_script]
    );
    let current = updated.current().unwrap();
    assert_eq!(current.description, "v2");
    assert_eq!(current.body, "body-v1");
    assert_eq!(current.scripts, vec![second_script]);
    assert!(current.enable_coco_shim);
}

fn assert_rollback_skill_creates_new_current_version<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let first_script = SkillScript {
        path: "scripts/rollback.py".to_owned(),
        content: "print('v1')".to_owned(),
    };
    store
        .add_skill(
            SessionRole::Orchestrator,
            "custom-orchestrator",
            SkillVersionSpec {
                description: "v1".to_owned(),
                body: "body-v1".to_owned(),
                scripts: vec![first_script.clone()],
                enable_coco_shim: false,
            },
        )
        .unwrap();
    store
        .update_skill(
            SessionRole::Orchestrator,
            "custom-orchestrator",
            &SkillUpdatePatch {
                description: Some("v2".to_owned()),
                body: Some("body-v2".to_owned()),
                scripts: Some(vec![SkillScript {
                    path: "scripts/rollback.py".to_owned(),
                    content: "print('v2')".to_owned(),
                }]),
                enable_coco_shim: Some(true),
            },
        )
        .unwrap();

    let rolled_back = store
        .rollback_skill(SessionRole::Orchestrator, "custom-orchestrator", 1)
        .unwrap();

    assert_eq!(rolled_back.current_version, 3);
    assert_eq!(
        rolled_back.versions.keys().copied().collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    let current = rolled_back.current().unwrap();
    assert_eq!(current.description, "v1");
    assert_eq!(current.body, "body-v1");
    assert_eq!(current.scripts, vec![first_script]);
    assert!(!current.enable_coco_shim);
}

fn assert_new_store_seeds_default_skills<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();

    let orchestrator = store
        .get_skill(SessionRole::Orchestrator, "coco-orchestrator")
        .unwrap();
    let new_skill = store
        .get_skill(SessionRole::Orchestrator, "new-skill")
        .unwrap();
    let cronjob = store
        .get_skill(SessionRole::Orchestrator, "cronjob")
        .unwrap();
    let runner = store.get_skill(SessionRole::Runner, "coco-runner").unwrap();
    let telegram = store.get_skill(SessionRole::Runner, "telegram").unwrap();

    assert_eq!(orchestrator.current_version, 1);
    assert_eq!(new_skill.current_version, 1);
    assert_eq!(cronjob.current_version, 1);
    assert_eq!(telegram.current_version, 1);
    assert_eq!(runner.current_version, 1);
    assert!(orchestrator.current().unwrap().enable_coco_shim);
    assert!(new_skill.current().unwrap().enable_coco_shim);
    assert!(cronjob.current().unwrap().enable_coco_shim);
    assert!(telegram.current().unwrap().enable_coco_shim);
    assert!(runner.current().unwrap().enable_coco_shim);
    assert_eq!(cronjob.current().unwrap().scripts.len(), 3);
    assert_eq!(telegram.current().unwrap().scripts.len(), 2);
    assert!(
        cronjob
            .current()
            .unwrap()
            .scripts
            .iter()
            .any(|script| script.path == "scripts/cronjob_crontab.py")
    );
    assert!(
        orchestrator
            .current()
            .unwrap()
            .body
            .contains("fork from the node before the `SkillInvocation`")
    );
    assert!(
        orchestrator
            .current()
            .unwrap()
            .body
            .contains("--tool exec_command --tool write_stdin --tool search_skill")
    );
    assert!(
        orchestrator
            .current()
            .unwrap()
            .body
            .contains("--enable-coco-shim")
    );
    assert!(new_skill.current().unwrap().body.contains("coco skill add"));
    assert!(
        cronjob
            .current()
            .unwrap()
            .body
            .contains("uv run --script \"$COCO_SKILL_DIR/scripts/cronjob_add.py\"")
    );
    assert!(
        telegram
            .current()
            .unwrap()
            .body
            .contains("reply_to_message_id")
    );
}

macro_rules! define_common_store_tests {
    ($module:ident, $factory:ty) => {
        mod $module {
            use super::*;

            #[test]
            fn new_store_exposes_root_text_node() {
                assert_new_store_exposes_root_text_node::<$factory>();
            }

            #[test]
            fn append_inserts_node_and_updates_children_index() {
                assert_append_inserts_node_and_updates_children_index::<$factory>();
            }

            #[test]
            fn append_rejects_missing_parent() {
                assert_append_rejects_missing_parent::<$factory>();
            }

            #[test]
            fn append_prompt_anchor_indexes_merge_parents_as_children() {
                assert_append_prompt_anchor_indexes_merge_parents_as_children::<$factory>();
            }

            #[test]
            fn append_session_anchor_indexes_merge_parents_as_children() {
                assert_append_session_anchor_indexes_merge_parents_as_children::<$factory>();
            }

            #[test]
            fn append_prompt_anchor_rejects_missing_merge_parent() {
                assert_append_prompt_anchor_rejects_missing_merge_parent::<$factory>();
            }

            #[test]
            fn append_prompt_anchor_rejects_duplicate_merge_parents() {
                assert_append_prompt_anchor_rejects_duplicate_merge_parents::<$factory>();
            }

            #[test]
            fn append_prompt_anchor_rejects_multiple_shadow_parents() {
                assert_append_prompt_anchor_rejects_multiple_shadow_parents::<$factory>();
            }

            #[test]
            fn append_prompt_anchor_rejects_merge_parent_matching_primary_parent() {
                assert_append_prompt_anchor_rejects_merge_parent_matching_primary_parent::<$factory>();
            }

            #[test]
            fn append_prompt_anchor_allows_merge_parent_from_other_session_root() {
                assert_append_prompt_anchor_allows_merge_parent_from_other_session_root::<$factory>();
            }

            #[test]
            fn ancestry_returns_nodes_back_to_root() {
                assert_ancestry_returns_nodes_back_to_root::<$factory>();
            }

            #[test]
            fn list_children_returns_primary_and_merge_children_in_stable_order() {
                assert_list_children_returns_primary_and_merge_children_in_stable_order::<$factory>();
            }

            #[test]
            fn list_children_returns_empty_for_leaf_node() {
                assert_list_children_returns_empty_for_leaf_node::<$factory>();
            }

            #[test]
            fn list_children_returns_not_found_for_missing_node() {
                assert_list_children_returns_not_found_for_missing_node::<$factory>();
            }

            #[test]
            fn log_returns_nodes_from_head_back_to_base_inclusive() {
                assert_log_returns_nodes_from_head_back_to_base_inclusive::<$factory>();
            }

            #[test]
            fn log_returns_single_node_when_base_equals_head() {
                assert_log_returns_single_node_when_base_equals_head::<$factory>();
            }

            #[test]
            fn log_returns_not_found_when_head_is_missing() {
                assert_log_returns_not_found_when_head_is_missing::<$factory>();
            }

            #[test]
            fn log_ignores_prompt_anchor_parents() {
                assert_log_ignores_prompt_anchor_parents::<$factory>();
            }

            #[test]
            fn branch_creation_resolves_refs() {
                assert_branch_creation_resolves_refs::<$factory>();
            }

            #[test]
            fn fork_rejects_duplicates() {
                assert_fork_rejects_duplicates::<$factory>();
            }

            #[test]
            fn fork_initializes_session_state() {
                assert_fork_initializes_session_state::<$factory>();
            }

            #[test]
            fn set_branch_head_requires_matching_expected_head() {
                assert_set_branch_head_requires_matching_expected_head::<$factory>();
            }

            #[test]
            fn set_branch_head_keeps_session_state_untouched() {
                assert_set_branch_head_keeps_session_state_untouched::<$factory>();
            }

            #[test]
            fn delete_branch_removes_branch_and_session_state() {
                assert_delete_branch_removes_branch_and_session_state::<$factory>();
            }

            #[test]
            fn delete_branch_rejects_missing_branch() {
                assert_delete_branch_rejects_missing_branch::<$factory>();
            }

            #[test]
            fn set_session_state_updates_value() {
                assert_set_session_state_updates_value::<$factory>();
            }

            #[test]
            fn set_session_state_requires_matching_expected() {
                assert_set_session_state_requires_matching_expected::<$factory>();
            }

            #[test]
            fn set_branch_head_preserves_attached_state() {
                assert_set_branch_head_preserves_attached_state::<$factory>();
            }

            #[test]
            fn set_session_state_accepts_merged_anchor_on_target_branch() {
                assert_set_session_state_accepts_merged_anchor_on_target_branch::<$factory>();
            }

            #[test]
            fn set_session_state_rejects_merged_anchor_outside_target_branch() {
                assert_set_session_state_rejects_merged_anchor_outside_target_branch::<$factory>();
            }

            #[test]
            fn set_session_state_rejects_non_anchor_merged_node() {
                assert_set_session_state_rejects_non_anchor_merged_node::<$factory>();
            }

            #[test]
            fn paused_merged_state_can_resume_as_attached() {
                assert_paused_merged_state_can_resume_as_attached::<$factory>();
            }

            #[test]
            fn paused_closed_state_can_resume_as_attached() {
                assert_paused_closed_state_can_resume_as_attached::<$factory>();
            }

            #[test]
            fn list_session_states_returns_branch_state_map() {
                assert_list_session_states_returns_branch_state_map::<$factory>();
            }

            #[test]
            fn preset_round_trip() {
                assert_preset_round_trip::<$factory>();
            }

            #[test]
            fn preset_replaces_existing_value() {
                assert_preset_replaces_existing_value::<$factory>();
            }

            #[test]
            fn rollback_preset_creates_new_current_version() {
                assert_rollback_preset_creates_new_current_version::<$factory>();
            }

            #[test]
            fn delete_preset_removes_only_config() {
                assert_delete_preset_removes_only_config::<$factory>();
            }

            #[test]
            fn delete_branch_preserves_preset() {
                assert_delete_branch_preserves_preset::<$factory>();
            }

            #[test]
            fn get_preset_rejects_missing_config() {
                assert_get_preset_rejects_missing_config::<$factory>();
            }

            #[test]
            fn delete_preset_rejects_missing_config() {
                assert_delete_preset_rejects_missing_config::<$factory>();
            }

            #[test]
            fn get_node_supports_branch_name() {
                assert_get_node_supports_branch_name::<$factory>();
            }

            #[test]
            fn get_node_supports_prefix_after_branch_delete() {
                assert_get_node_supports_prefix_after_branch_delete::<$factory>();
            }

            #[test]
            fn get_node_rejects_ambiguous_prefix() {
                assert_get_node_rejects_ambiguous_prefix::<$factory>();
            }

            #[test]
            fn log_supports_branch_name_on_head_ref() {
                assert_log_supports_branch_name_on_head_ref::<$factory>();
            }

            #[test]
            fn log_supports_branch_name_on_base_ref() {
                assert_log_supports_branch_name_on_base_ref::<$factory>();
            }

            #[test]
            fn log_supports_branch_name_on_both_sides() {
                assert_log_supports_branch_name_on_both_sides::<$factory>();
            }

            #[test]
            fn log_returns_branch_not_found_when_branch_is_missing() {
                assert_log_returns_branch_not_found_when_branch_is_missing::<$factory>();
            }

            #[test]
            fn rebase_session_rewrites_branch_chain_with_updated_config() {
                assert_rebase_session_rewrites_branch_chain_with_updated_config::<$factory>();
            }

            #[test]
            fn rebase_session_patch_system_prompt_rebuilds_branch_chain() {
                assert_rebase_session_patch_system_prompt_rebuilds_branch_chain::<$factory>();
            }

            #[test]
            fn rebase_session_keeps_merge_parents_pointing_to_original_nodes() {
                assert_rebase_session_keeps_merge_parents_pointing_to_original_nodes::<$factory>();
            }

            #[test]
            fn rebase_session_keeps_other_branches_on_old_chain() {
                assert_rebase_session_keeps_other_branches_on_old_chain::<$factory>();
            }

            #[test]
            fn rebase_session_requires_visible_session_anchor() {
                assert_rebase_session_requires_visible_session_anchor::<$factory>();
            }

            #[test]
            fn rebase_session_preserves_created_at_across_rewritten_chain() {
                assert_rebase_session_preserves_created_at_across_rewritten_chain::<$factory>();
            }

            #[test]
            fn handoff_session_appends_session_anchor_after_current_head() {
                assert_handoff_session_appends_session_anchor_after_current_head::<$factory>();
            }

            #[test]
            fn handoff_session_requires_visible_session_anchor() {
                assert_handoff_session_requires_visible_session_anchor::<$factory>();
            }

            #[test]
            fn handoff_session_applies_session_patch() {
                assert_handoff_session_applies_session_patch::<$factory>();
            }

            #[test]
            fn job_round_trip() {
                assert_job_round_trip::<$factory>();
            }

            #[test]
            fn create_job_generates_unique_ids() {
                assert_create_job_generates_unique_ids::<$factory>();
            }

            #[test]
            fn add_skill_starts_at_version_one() {
                assert_add_skill_starts_at_version_one::<$factory>();
            }

            #[test]
            fn new_store_seeds_default_skills() {
                assert_new_store_seeds_default_skills::<$factory>();
            }

            #[test]
            fn update_skill_creates_new_version() {
                assert_update_skill_creates_new_version::<$factory>();
            }

            #[test]
            fn rollback_skill_creates_new_current_version() {
                assert_rollback_skill_creates_new_current_version::<$factory>();
            }

            #[test]
            fn finished_job_round_trip() {
                assert_finished_job_round_trip::<$factory>();
            }

            #[test]
            fn set_job_status_rejects_invalid_transition() {
                assert_set_job_status_rejects_invalid_transition::<$factory>();
            }

            #[test]
            fn submit_job_rejects_second_active_job_on_same_branch() {
                assert_submit_job_rejects_second_active_job_on_same_branch::<$factory>();
            }

            #[test]
            fn submit_job_allows_new_job_after_previous_job_finishes() {
                assert_submit_job_allows_new_job_after_previous_job_finishes::<$factory>();
            }

            #[test]
            fn message_queue_round_trip() {
                assert_message_queue_round_trip::<$factory>();
            }

            #[test]
            fn message_queue_isolates_named_queues() {
                assert_message_queue_isolates_named_queues::<$factory>();
            }

            #[test]
            fn message_queue_ids_are_content_derived() {
                assert_message_queue_ids_are_content_derived::<$factory>();
            }

        }
    };
}

define_common_store_tests!(memory_store, MemoryFactory);
define_common_store_tests!(fs_store, FsFactory);

#[test]
fn log_returns_parent_not_found_when_chain_is_broken() {
    let mut store = StoreState::new();
    let root_id = store.root_id().to_owned();
    let mut broken = store
        .plan_append_node(make_session_anchor_node(&root_id))
        .map(|_| {
            crate::Node::new(
                "missing".to_owned(),
                Role::User,
                None,
                Kind::Text("broken".to_owned()),
                "2026-03-25T09:10:11Z".parse().unwrap(),
            )
        })
        .unwrap();
    let broken_id = broken.id.clone();
    store.nodes.insert(broken.id.clone(), broken.clone());

    let err = store.log(&root_id, &broken_id).unwrap_err();

    assert!(matches!(err, Error::ParentNotFound { id } if id == "missing"));
    broken.parent = root_id;
}

#[test]
fn log_returns_not_found_when_branch_head_is_missing() {
    let mut store = StoreState::new();
    let root_id = store.root_id().to_owned();
    let missing_id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    store
        .branches
        .insert("main".to_owned(), missing_id.to_owned());

    let err = store.log(&root_id, "main").unwrap_err();

    assert!(matches!(err, Error::NotFound { id } if id == missing_id));
}

#[test]
fn open_creates_jsonl_store_directory_with_root_node() {
    let (_tempdir, path) = temp_store_path();

    let store = FsStore::open(&path).unwrap();

    assert!(path.join("meta.json").is_file());
    assert!(path.join("store.lock").is_file());
    assert!(path.join("nodes.jsonl").is_file());
    assert!(path.join("sessions.json").is_file());
    assert!(path.join("presets.json").is_file());
    assert!(path.join("skills.json").is_file());
    assert!(path.join("branches").is_dir());
    assert!(path.join("jobs.json").is_file());
    assert!(path.join("jobs.jsonl").is_file());
    assert!(path.join("preset-history").is_dir());
    assert!(path.join("skill-history").is_dir());
    assert!(path.join("skill-history/shared").is_dir());
    assert!(path.join("skill-history/orchestrator").is_dir());
    assert!(path.join("skill-history/runner").is_dir());
    assert!(
        path.join("skill-history/orchestrator/coco-orchestrator.jsonl")
            .is_file()
    );
    assert!(
        path.join("skill-history/runner/coco-runner.jsonl")
            .is_file()
    );
    assert!(path.join("skill-history/runner/telegram.jsonl").is_file());

    let nodes = fs::read_to_string(path.join("nodes.jsonl")).unwrap();
    assert!(nodes.lines().count() >= 1);
    assert_eq!(store.ancestry(&store.root_id()).unwrap().len(), 1);

    let meta: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(path.join("meta.json")).unwrap()).unwrap();
    assert_eq!(meta["version"], "2026-05-18");
}

#[test]
fn open_rejects_store_locked_by_another_owner() {
    let (_tempdir, path) = temp_store_path();
    fs::create_dir_all(&path).unwrap();
    let lock_path = path.join("store.lock");
    let lock_file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .unwrap();

    let result = unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    assert_eq!(result, 0);

    let err = FsStore::open(&path).unwrap_err();

    assert!(matches!(err, Error::StoreLocked { path: locked } if locked == path));
}

#[test]
fn open_read_only_allows_store_locked_by_another_owner() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let root_id = store.root_id();
    store.fork("main", &root_id).unwrap();
    drop(store);

    let lock_path = path.join("store.lock");
    let lock_file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock_path)
        .unwrap();
    let result = unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    assert_eq!(result, 0);

    let read_only = FsStore::open_read_only(&path).unwrap();

    assert_eq!(read_only.get_branch_head("main").unwrap(), root_id);
    let err = read_only
        .append(make_text_node(&root_id, "read-only write"))
        .unwrap_err();
    assert!(matches!(err, Error::StoreReadOnly { path: locked } if locked == path));
}

#[test]
fn open_read_only_does_not_create_missing_history_directories() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let root_id = store.root_id();
    drop(store);

    fs::write(
        path.join("skills.json"),
        r#"{"orchestrator":{},"runner":{}}"#,
    )
    .unwrap();
    fs::remove_dir_all(path.join("preset-history")).unwrap();
    fs::remove_dir_all(path.join("skill-history")).unwrap();

    let lock_path = path.join("store.lock");
    let lock_file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock_path)
        .unwrap();
    let result = unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    assert_eq!(result, 0);

    let read_only = FsStore::open_read_only(&path).unwrap();

    assert_eq!(read_only.root_id(), root_id);
    assert!(!path.join("preset-history").exists());
    assert!(!path.join("skill-history").exists());
}

#[test]
fn open_replays_presets() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let preset_name = "coding";
    let config = make_preset("gpt-5.4", SessionRole::Orchestrator);
    let updated = make_preset("claude-sonnet-4-20250514", SessionRole::Runner);
    store.set_preset(preset_name, config.clone()).unwrap();
    store.set_preset(preset_name, updated.clone()).unwrap();

    let reopened = FsStore::open(&path).unwrap();

    assert_eq!(current_preset(&reopened, preset_name).unwrap(), updated);
    let record = reopened.get_preset_record(preset_name).unwrap();
    assert_eq!(record.current_version, 2);
    assert_eq!(record.versions.get(&1).unwrap().to_preset(), config);
    assert_eq!(record.versions.get(&2).unwrap().to_preset(), updated);
}

#[test]
fn presets_json_only_stores_current_snapshots() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let preset_name = "coding";
    store
        .set_preset(
            preset_name,
            make_preset("gpt-5.4", SessionRole::Orchestrator),
        )
        .unwrap();
    store
        .set_preset(
            preset_name,
            make_preset("claude-sonnet-4-20250514", SessionRole::Runner),
        )
        .unwrap();

    let configs: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(path.join("presets.json")).unwrap()).unwrap();
    let coding = &configs[preset_name];

    assert_eq!(coding["current_version"], 2);
    assert!(coding.get("versions").is_none());
    assert_eq!(coding["model"], "claude-sonnet-4-20250514");
    assert_eq!(coding["role"], "runner");
}

#[test]
fn preset_history_is_appended_in_history_directory() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let preset_name = "coding";
    store
        .set_preset(
            preset_name,
            make_preset("gpt-5.4", SessionRole::Orchestrator),
        )
        .unwrap();
    store
        .set_preset(
            preset_name,
            make_preset("claude-sonnet-4-20250514", SessionRole::Runner),
        )
        .unwrap();
    store.rollback_preset(preset_name, 1).unwrap();

    let history = read_jsonl_values(&path.join("preset-history/coding.jsonl"));

    assert_eq!(history.len(), 3);
    assert_eq!(history[0]["name"], preset_name);
    assert_eq!(history[0]["version"], 1);
    assert_eq!(history[1]["version"], 2);
    assert_eq!(history[2]["version"], 3);
    assert_eq!(history[2]["model"], history[0]["model"]);
    assert_eq!(history[2]["role"], history[0]["role"]);
}

#[test]
fn open_rejects_missing_preset_metadata_file() {
    let (_tempdir, path) = temp_store_path();
    FsStore::open(&path).unwrap();
    fs::remove_file(path.join("presets.json")).unwrap();

    let err = FsStore::open(&path).unwrap_err();

    assert!(matches!(err, Error::CorruptedStore { .. }));
}

#[test]
fn open_does_not_restore_deleted_preset() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let preset_name = "coding";
    store
        .set_preset(
            preset_name,
            make_preset("gpt-5.4", SessionRole::Orchestrator),
        )
        .unwrap();

    store.delete_preset(preset_name).unwrap();
    let reopened = FsStore::open(&path).unwrap();

    assert!(matches!(
        reopened.get_preset_record(preset_name),
        Err(Error::PresetNotFound { name }) if name == preset_name
    ));
    assert!(!path.join("preset-history/coding.jsonl").exists());
}

#[test]
fn open_migrates_numeric_store_format_version_to_chronicle_version() {
    let (_tempdir, path) = temp_store_path();
    FsStore::open(&path).unwrap();
    let mut meta: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(path.join("meta.json")).unwrap()).unwrap();
    meta["version"] = json!(10);
    fs::write(
        path.join("meta.json"),
        serde_json::to_string_pretty(&meta).unwrap(),
    )
    .unwrap();
    fs::write(path.join("jobs.json"), "{}").unwrap();
    fs::remove_file(path.join("jobs.jsonl")).unwrap();
    let mut skills: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(path.join("skills.json")).unwrap()).unwrap();
    skills["orchestrator"]["coco-orchestrator"]
        .as_object_mut()
        .unwrap()
        .remove("id");
    fs::write(
        path.join("skills.json"),
        serde_json::to_string_pretty(&skills).unwrap(),
    )
    .unwrap();
    let history_path = path.join("skill-history/orchestrator/coco-orchestrator.jsonl");
    let mut history = read_jsonl_values(&history_path);
    history[0].as_object_mut().unwrap().remove("id");
    write_jsonl_values(&history_path, &history);

    FsStore::open(&path).unwrap();

    let migrated: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(path.join("meta.json")).unwrap()).unwrap();
    assert_eq!(migrated["version"], "2026-05-18");
    let migrated_skills: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(path.join("skills.json")).unwrap()).unwrap();
    let snapshot_id = migrated_skills["orchestrator"]["coco-orchestrator"]["id"]
        .as_str()
        .unwrap();
    let migrated_history = read_jsonl_values(&history_path);
    let history_id = migrated_history[0]["id"].as_str().unwrap();
    assert_eq!(snapshot_id.len(), 64);
    assert_eq!(history_id, snapshot_id);
}

#[test]
fn open_read_only_rejects_store_that_requires_format_migration() {
    let (_tempdir, path) = temp_store_path();
    FsStore::open(&path).unwrap();
    let mut meta: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(path.join("meta.json")).unwrap()).unwrap();
    meta["version"] = json!(10);
    fs::write(
        path.join("meta.json"),
        serde_json::to_string_pretty(&meta).unwrap(),
    )
    .unwrap();

    let err = FsStore::open_read_only(&path).unwrap_err();

    assert!(matches!(err, Error::CorruptedStore { .. }));
    let unchanged: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(path.join("meta.json")).unwrap()).unwrap();
    assert_eq!(unchanged["version"], 10);
}

#[test]
fn open_rejects_unsupported_store_format_version() {
    let (_tempdir, path) = temp_store_path();
    FsStore::open(&path).unwrap();
    let mut meta: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(path.join("meta.json")).unwrap()).unwrap();
    meta["version"] = json!("2020-01-01");
    fs::write(
        path.join("meta.json"),
        serde_json::to_string_pretty(&meta).unwrap(),
    )
    .unwrap();

    let err = FsStore::open(&path).unwrap_err();

    assert!(matches!(err, Error::CorruptedStore { .. }));
}

#[test]
fn open_creates_missing_message_queue_wal() {
    let (_tempdir, path) = temp_store_path();
    FsStore::open(&path).unwrap();
    fs::remove_file(path.join("queues.jsonl")).unwrap();

    let reopened = FsStore::open(&path).unwrap();

    assert!(reopened.list_queue_messages("hooks").unwrap().is_empty());
    assert!(path.join("queues.jsonl").is_file());
}

#[test]
fn queue_messages_survive_fs_store_reopen() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let item = store
        .enqueue_message("hooks", json!({"text": "persisted"}))
        .unwrap();

    drop(store);
    let reopened = FsStore::open(&path).unwrap();

    assert_eq!(reopened.list_queue_messages("hooks").unwrap(), vec![item]);
}

#[test]
fn queue_wal_records_dequeue_before_state_change() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let first = store
        .enqueue_message("hooks", json!({"text": "first"}))
        .unwrap();
    let second = store
        .enqueue_message("hooks", json!({"text": "second"}))
        .unwrap();

    assert_eq!(store.dequeue_message("hooks").unwrap(), Some(first.clone()));

    let entries = read_jsonl_values(&path.join("queues.jsonl"));
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0]["op"], "enqueued");
    assert_eq!(entries[1]["op"], "enqueued");
    assert_eq!(entries[2]["op"], "dequeued");
    assert_eq!(entries[2]["queue"], "hooks");
    assert_eq!(entries[2]["message_id"], first.message_id);

    drop(store);
    let reopened = FsStore::open(&path).unwrap();
    assert_eq!(reopened.list_queue_messages("hooks").unwrap(), vec![second]);
}

#[test]
fn queue_wal_compacts_when_queues_are_empty() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let item = store
        .enqueue_message("hooks", json!({"text": "transient"}))
        .unwrap();

    assert_eq!(store.dequeue_message("hooks").unwrap(), Some(item));

    assert!(store.list_queue_messages("hooks").unwrap().is_empty());
    assert!(read_jsonl_values(&path.join("queues.jsonl")).is_empty());
}

#[test]
fn open_replays_nodes_and_branch_updates_from_jsonl_logs() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let root_id = store.root_id();
    let session_id = store.append(make_session_anchor_node(&root_id)).unwrap();
    let child_id = store.append(make_text_node(&session_id, "child")).unwrap();
    store.fork("main", &child_id).unwrap();
    let next_id = store.append(make_text_node(&child_id, "next")).unwrap();
    store.set_branch_head("main", &child_id, &next_id).unwrap();

    let reopened = FsStore::open(&path).unwrap();

    assert_eq!(reopened.get_branch_head("main").unwrap(), next_id);
    assert_eq!(
        reopened.get_session_state("main").unwrap(),
        SessionState::Active
    );
    let ancestry = reopened.ancestry("main").unwrap();
    assert_eq!(ancestry[0].id, next_id);
    assert_eq!(ancestry[1].id, child_id);
    assert_eq!(ancestry[2].id, session_id);
    assert_eq!(ancestry[3].id, root_id);
}

#[test]
fn open_rejects_missing_session_metadata_file() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let root_id = store.root_id();
    store.fork("main", &root_id).unwrap();
    fs::remove_file(path.join("sessions.json")).unwrap();

    let err = FsStore::open(&path).unwrap_err();

    assert!(matches!(err, Error::CorruptedStore { .. }));
}

#[test]
fn open_defaults_missing_job_finished_at() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let root_id = store.root_id();
    store.fork("main", &root_id).unwrap();
    let job = submit_prompt_job(&store, "main", "hello");

    let history_path = path.join("jobs.jsonl");
    let mut history = read_jsonl_values(&history_path);
    history[0]["job"]
        .as_object_mut()
        .unwrap()
        .remove("finished_at");
    write_jsonl_values(&history_path, &history);

    let reopened = FsStore::open(&path).unwrap();
    let reopened_job = reopened.get_job(&job.job_id).unwrap();

    assert!(reopened_job.finished_at.is_none());
}

#[test]
fn fs_jobs_are_recorded_in_wal_after_snapshot() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let root_id = store.root_id();
    store.fork("main", &root_id).unwrap();
    store.fork("draft", &root_id).unwrap();
    let first = submit_prompt_job(&store, "main", "hello");
    let second = submit_prompt_job(&store, "draft", "world");

    let snapshot: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(path.join("jobs.json")).unwrap()).unwrap();
    assert_eq!(snapshot.as_object().unwrap().len(), 0);

    store
        .set_job_status(&first.job_id, JobStatus::Queued, JobStatus::Running)
        .unwrap();

    let history = read_jsonl_values(&path.join("jobs.jsonl"));
    assert_eq!(history.len(), 3);
    assert_eq!(history[0]["op"], "submitted");
    assert_eq!(history[0]["job"]["job_id"], first.job_id);
    assert_eq!(history[1]["op"], "submitted");
    assert_eq!(history[1]["job"]["job_id"], second.job_id);
    assert_eq!(history[2]["op"], "status_changed");
    assert_eq!(history[2]["job_id"], first.job_id);
    assert_eq!(history[2]["expected"], "queued");
    assert_eq!(history[2]["updated"]["status"], "running");

    let reopened = FsStore::open(&path).unwrap();
    assert_eq!(
        reopened.get_job(&first.job_id).unwrap().status,
        JobStatus::Running
    );
    assert_eq!(reopened.get_job(&second.job_id).unwrap(), second);
}

#[test]
fn open_migrates_legacy_jobs_json_to_snapshot_with_empty_wal() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let root_id = store.root_id();
    store.fork("main", &root_id).unwrap();
    let job = submit_prompt_job(&store, "main", "hello");

    let mut legacy_jobs = serde_json::Map::new();
    legacy_jobs.insert(job.job_id.clone(), serde_json::to_value(&job).unwrap());
    fs::write(
        path.join("jobs.json"),
        serde_json::to_vec_pretty(&serde_json::Value::Object(legacy_jobs)).unwrap(),
    )
    .unwrap();
    fs::remove_file(path.join("jobs.jsonl")).unwrap();
    let mut meta: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(path.join("meta.json")).unwrap()).unwrap();
    meta["version"] = json!("2026-05-17");
    fs::write(
        path.join("meta.json"),
        serde_json::to_string_pretty(&meta).unwrap(),
    )
    .unwrap();

    let reopened = FsStore::open(&path).unwrap();

    assert!(path.join("jobs.json").is_file());
    assert!(read_jsonl_values(&path.join("jobs.jsonl")).is_empty());
    assert_eq!(reopened.get_job(&job.job_id).unwrap(), job);
    let migrated_meta: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(path.join("meta.json")).unwrap()).unwrap();
    assert_eq!(migrated_meta["version"], "2026-05-18");
}

#[test]
fn open_compacts_job_wal_into_snapshot() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let root_id = store.root_id();
    for index in 0..64 {
        let branch = format!("branch-{index}");
        store.fork(&branch, &root_id).unwrap();
        submit_prompt_job(&store, &branch, "hello");
    }
    drop(store);

    let reopened = FsStore::open(&path).unwrap();

    let snapshot: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(path.join("jobs.json")).unwrap()).unwrap();
    assert_eq!(snapshot.as_object().unwrap().len(), 64);
    assert!(read_jsonl_values(&path.join("jobs.jsonl")).is_empty());
    assert_eq!(reopened.list_jobs().unwrap().len(), 64);
}

#[test]
fn open_recovers_interrupted_job_compaction() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let root_id = store.root_id();
    store.fork("main", &root_id).unwrap();
    let job = submit_prompt_job(&store, "main", "hello");
    let jobs = store.list_jobs().unwrap();
    fs::write(
        path.join("jobs.compaction.json"),
        serde_json::to_vec_pretty(&jobs).unwrap(),
    )
    .unwrap();
    fs::write(
        path.join("jobs.json"),
        serde_json::to_vec_pretty(&jobs).unwrap(),
    )
    .unwrap();
    drop(store);

    let reopened = FsStore::open(&path).unwrap();

    assert_eq!(reopened.get_job(&job.job_id).unwrap(), job);
    assert!(read_jsonl_values(&path.join("jobs.jsonl")).is_empty());
    assert!(!path.join("jobs.compaction.json").exists());
}

#[test]
fn open_rejects_job_snapshot_key_mismatch() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let root_id = store.root_id();
    store.fork("main", &root_id).unwrap();
    let job = submit_prompt_job(&store, "main", "hello");
    fs::write(path.join("jobs.jsonl"), "").unwrap();
    fs::write(
        path.join("jobs.json"),
        serde_json::to_vec_pretty(&json!({ "other-job": job })).unwrap(),
    )
    .unwrap();

    let err = FsStore::open(&path).unwrap_err();

    assert!(matches!(err, Error::CorruptedStore { .. }));
}

#[test]
fn open_rejects_job_snapshot_with_multiple_active_jobs_on_branch() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let root_id = store.root_id();
    store.fork("main", &root_id).unwrap();
    let first = submit_prompt_job(&store, "main", "hello");
    let mut second = first.clone();
    second.job_id = "manual-second-job".to_owned();
    fs::write(path.join("jobs.jsonl"), "").unwrap();
    fs::write(
        path.join("jobs.json"),
        serde_json::to_vec_pretty(&json!({
            first.job_id.clone(): first,
            second.job_id.clone(): second,
        }))
        .unwrap(),
    )
    .unwrap();

    let err = FsStore::open(&path).unwrap_err();

    assert!(matches!(err, Error::CorruptedStore { .. }));
}

#[test]
fn open_rejects_duplicate_job_wal_submission() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let root_id = store.root_id();
    store.fork("main", &root_id).unwrap();
    submit_prompt_job(&store, "main", "hello");
    let history_path = path.join("jobs.jsonl");
    let mut history = read_jsonl_values(&history_path);
    history.push(history[0].clone());
    write_jsonl_values(&history_path, &history);

    let err = FsStore::open(&path).unwrap_err();

    assert!(matches!(err, Error::CorruptedStore { .. }));
}

#[test]
fn open_rejects_job_wal_submission_when_branch_already_active() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let root_id = store.root_id();
    store.fork("main", &root_id).unwrap();
    let first = submit_prompt_job(&store, "main", "hello");
    let mut second = first.clone();
    second.job_id = "manual-second-job".to_owned();
    let history_path = path.join("jobs.jsonl");
    let mut history = read_jsonl_values(&history_path);
    history.push(json!({
        "op": "submitted",
        "job": second,
    }));
    write_jsonl_values(&history_path, &history);

    let err = FsStore::open(&path).unwrap_err();

    assert!(matches!(err, Error::CorruptedStore { .. }));
}

#[test]
fn open_rejects_job_wal_status_change_with_mismatched_expected_status() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let root_id = store.root_id();
    store.fork("main", &root_id).unwrap();
    let job = submit_prompt_job(&store, "main", "hello");
    store
        .set_job_status(&job.job_id, JobStatus::Queued, JobStatus::Running)
        .unwrap();
    let history_path = path.join("jobs.jsonl");
    let mut history = read_jsonl_values(&history_path);
    history[1]["expected"] = json!("running");
    write_jsonl_values(&history_path, &history);

    let err = FsStore::open(&path).unwrap_err();

    assert!(matches!(err, Error::CorruptedStore { .. }));
}

#[test]
fn open_rejects_job_wal_status_change_that_modifies_identity() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let root_id = store.root_id();
    store.fork("main", &root_id).unwrap();
    let job = submit_prompt_job(&store, "main", "hello");
    store
        .set_job_status(&job.job_id, JobStatus::Queued, JobStatus::Running)
        .unwrap();
    let history_path = path.join("jobs.jsonl");
    let mut history = read_jsonl_values(&history_path);
    history[1]["updated"]["branch"] = json!("other-branch");
    write_jsonl_values(&history_path, &history);

    let err = FsStore::open(&path).unwrap_err();

    assert!(matches!(err, Error::CorruptedStore { .. }));
}

#[test]
fn open_rejects_job_wal_invalid_status_transition() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let root_id = store.root_id();
    store.fork("main", &root_id).unwrap();
    let job = submit_prompt_job(&store, "main", "hello");
    store
        .set_job_status(&job.job_id, JobStatus::Queued, JobStatus::Running)
        .unwrap();
    let history_path = path.join("jobs.jsonl");
    let mut history = read_jsonl_values(&history_path);
    history[1]["updated"]["status"] = json!("finished");
    write_jsonl_values(&history_path, &history);

    let err = FsStore::open(&path).unwrap_err();

    assert!(matches!(err, Error::CorruptedStore { .. }));
}

#[test]
fn open_rejects_job_wal_unfinished_status_with_finished_at() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let root_id = store.root_id();
    store.fork("main", &root_id).unwrap();
    let job = submit_prompt_job(&store, "main", "hello");
    store
        .set_job_status(&job.job_id, JobStatus::Queued, JobStatus::Running)
        .unwrap();
    let history_path = path.join("jobs.jsonl");
    let mut history = read_jsonl_values(&history_path);
    history[1]["updated"]["finished_at"] = history[0]["job"]["created_at"].clone();
    write_jsonl_values(&history_path, &history);

    let err = FsStore::open(&path).unwrap_err();

    assert!(matches!(err, Error::CorruptedStore { .. }));
}

#[test]
fn open_replays_paused_merged_state_with_base_handoff_anchor() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let root_id = store.root_id();
    let base_anchor_id = store.append(make_session_anchor_node(&root_id)).unwrap();
    store.fork("base", &base_anchor_id).unwrap();
    store.fork("main", &root_id).unwrap();
    let merged_anchor_id = store
        .append(make_prompt_anchor_node(&base_anchor_id, &[]))
        .unwrap();
    store
        .set_branch_head("base", &base_anchor_id, &merged_anchor_id)
        .unwrap();
    store
        .set_session_state(
            "main",
            Some(&SessionState::Active),
            SessionState::Paused {
                target_branch: "base".to_owned(),
                reason: PauseReason::Merged {
                    merged_anchor_id: merged_anchor_id.clone(),
                },
            },
        )
        .unwrap();

    let reopened = FsStore::open(&path).unwrap();
    let state = reopened.get_session_state("main").unwrap();

    assert_eq!(
        state,
        SessionState::Paused {
            target_branch: "base".to_owned(),
            reason: PauseReason::Merged {
                merged_anchor_id: merged_anchor_id.clone(),
            },
        }
    );
    let ancestry = reopened.ancestry("base").unwrap();
    assert!(ancestry.iter().any(|node| node.id == merged_anchor_id));
}

#[test]
fn open_replays_paused_closed_state() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let root_id = store.root_id();
    let base_anchor_id = store.append(make_session_anchor_node(&root_id)).unwrap();
    store.fork("base", &base_anchor_id).unwrap();
    store.fork("main", &root_id).unwrap();
    store
        .set_session_state(
            "main",
            Some(&SessionState::Active),
            SessionState::Paused {
                target_branch: "base".to_owned(),
                reason: PauseReason::Closed,
            },
        )
        .unwrap();

    let reopened = FsStore::open(&path).unwrap();
    let state = reopened.get_session_state("main").unwrap();

    assert_eq!(
        state,
        SessionState::Paused {
            target_branch: "base".to_owned(),
            reason: PauseReason::Closed,
        }
    );
}

#[test]
fn open_does_not_restore_deleted_branch() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let root_id = store.root_id();
    let branch_head = store.fork("main", &root_id).unwrap();

    store.delete_branch("main").unwrap();

    let reopened = FsStore::open(&path).unwrap();
    let err = reopened.get_branch_head("main").unwrap_err();
    assert!(matches!(err, Error::BranchNotFound { name } if name == "main"));
    let err = reopened.get_session_state("main").unwrap_err();
    assert!(matches!(err, Error::BranchNotFound { name } if name == "main"));
    assert_eq!(reopened.get_node(&branch_head).unwrap().id, branch_head);
}

#[test]
fn open_replays_branch_view_after_head_rewind() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let root_id = store.root_id();
    let child_id = store.append(make_text_node(&root_id, "child")).unwrap();
    let next_id = store.append(make_text_node(&child_id, "next")).unwrap();
    store.fork("main", &next_id).unwrap();
    store.set_branch_head("main", &next_id, &child_id).unwrap();

    let reopened = FsStore::open(&path).unwrap();
    let branch_nodes = read_jsonl_nodes(&path.join("branches/main.jsonl"));
    let global_nodes = read_jsonl_nodes(&path.join("nodes.jsonl"));

    assert_eq!(reopened.get_branch_head("main").unwrap(), child_id);
    let ancestry = reopened.ancestry("main").unwrap();
    assert_eq!(ancestry[0].id, child_id);
    assert_eq!(ancestry[1].id, root_id);
    assert_eq!(
        branch_nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect::<Vec<_>>(),
        vec![root_id.as_str(), child_id.as_str()]
    );
    assert!(global_nodes.iter().any(|node| node.id == next_id));
}

#[test]
fn open_replays_encoded_branch_names() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let root_id = store.root_id();
    let child_id = store.append(make_text_node(&root_id, "child")).unwrap();
    let branch = "draft/review 你好";
    store.fork(branch, &child_id).unwrap();

    let reopened = FsStore::open(&path).unwrap();

    assert_eq!(reopened.get_branch_head(branch).unwrap(), child_id);
}

#[test]
fn open_rejects_corrupted_jsonl_logs() {
    let (_tempdir, path) = temp_store_path();
    let _store = FsStore::open(&path).unwrap();
    fs::write(path.join("nodes.jsonl"), "not-json\n").unwrap();

    let err = FsStore::open(&path).unwrap_err();

    assert!(matches!(err, Error::ParseStoreLog { line: 1, .. }));
}

#[test]
fn open_seeds_default_skills_when_skills_file_is_empty() {
    let (_tempdir, path) = temp_store_path();
    let _store = FsStore::open(&path).unwrap();
    fs::write(
        path.join("skills.json"),
        "{\n  \"orchestrator\": {},\n  \"runner\": {}\n}\n",
    )
    .unwrap();

    let reopened = FsStore::open(&path).unwrap();

    assert!(
        reopened
            .get_skill(SessionRole::Orchestrator, "coco-orchestrator")
            .is_ok()
    );
    assert!(
        reopened
            .get_skill(SessionRole::Orchestrator, "new-skill")
            .is_ok()
    );
    assert!(
        reopened
            .get_skill(SessionRole::Orchestrator, "cronjob")
            .is_ok()
    );
    assert!(
        reopened
            .get_skill(SessionRole::Runner, "coco-runner")
            .is_ok()
    );
    assert!(reopened.get_skill(SessionRole::Runner, "telegram").is_ok());
    assert!(
        path.join("skill-history/orchestrator/coco-orchestrator.jsonl")
            .is_file()
    );
    assert!(
        path.join("skill-history/orchestrator/new-skill.jsonl")
            .is_file()
    );
    assert!(
        path.join("skill-history/orchestrator/cronjob.jsonl")
            .is_file()
    );
    assert!(
        path.join("skill-history/runner/coco-runner.jsonl")
            .is_file()
    );
    assert!(path.join("skill-history/runner/telegram.jsonl").is_file());
}

#[test]
fn store_migration_adds_missing_builtin_skill() {
    let (_tempdir, path) = temp_store_path();
    FsStore::open(&path).unwrap();
    let mut skills: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(path.join("skills.json")).unwrap()).unwrap();
    skills["orchestrator"]
        .as_object_mut()
        .unwrap()
        .remove("new-skill");
    fs::write(
        path.join("skills.json"),
        serde_json::to_string_pretty(&skills).unwrap(),
    )
    .unwrap();
    fs::remove_file(path.join("skill-history/orchestrator/new-skill.jsonl")).unwrap();
    let mut meta: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(path.join("meta.json")).unwrap()).unwrap();
    meta["version"] = json!(10);
    fs::write(
        path.join("meta.json"),
        serde_json::to_string_pretty(&meta).unwrap(),
    )
    .unwrap();
    fs::write(path.join("jobs.json"), "{}").unwrap();
    fs::remove_file(path.join("jobs.jsonl")).unwrap();

    let reopened = FsStore::open(&path).unwrap();

    let skill = reopened
        .get_skill(SessionRole::Orchestrator, "new-skill")
        .unwrap();
    assert_eq!(skill.current_version, 1);
    assert!(
        path.join("skill-history/orchestrator/new-skill.jsonl")
            .is_file()
    );
}

#[test]
fn open_current_store_does_not_run_builtin_skill_migrations() {
    let (_tempdir, path) = temp_store_path();
    FsStore::open(&path).unwrap();
    let mut skills: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(path.join("skills.json")).unwrap()).unwrap();
    skills["orchestrator"]
        .as_object_mut()
        .unwrap()
        .remove("new-skill");
    fs::write(
        path.join("skills.json"),
        serde_json::to_string_pretty(&skills).unwrap(),
    )
    .unwrap();
    fs::remove_file(path.join("skill-history/orchestrator/new-skill.jsonl")).unwrap();

    let reopened = FsStore::open(&path).unwrap();

    assert!(matches!(
        reopened.get_skill(SessionRole::Orchestrator, "new-skill"),
        Err(Error::SkillNotFound { name, .. }) if name == "new-skill"
    ));
}

#[test]
fn open_rejects_skill_history_with_invalid_revision_id() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    drop(store);

    let mut skills: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(path.join("skills.json")).unwrap()).unwrap();
    skills["orchestrator"]["coco-orchestrator"]["description"] = json!("old builtin");
    skills["orchestrator"]["coco-orchestrator"]["body"] = json!("old body");
    fs::write(
        path.join("skills.json"),
        serde_json::to_string_pretty(&skills).unwrap(),
    )
    .unwrap();

    let history_path = path.join("skill-history/orchestrator/coco-orchestrator.jsonl");
    let mut history = read_jsonl_values(&history_path);
    history[0]["description"] = json!("old builtin");
    history[0]["body"] = json!("old body");
    write_jsonl_values(&history_path, &history);

    let err = FsStore::open(&path).unwrap_err();
    match err {
        Error::CorruptedStore { message, .. } => {
            assert!(message.contains("has invalid id"), "{message}");
        }
        other => panic!("expected corrupted store, got {other:?}"),
    }
}

#[test]
fn open_does_not_overwrite_user_modified_builtin_skill() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    store
        .update_skill(
            SessionRole::Orchestrator,
            "coco-orchestrator",
            &SkillUpdatePatch {
                description: Some("custom".to_owned()),
                body: Some("custom body".to_owned()),
                scripts: None,
                enable_coco_shim: None,
            },
        )
        .unwrap();
    drop(store);

    let reopened = FsStore::open(&path).unwrap();
    let skill = reopened
        .get_skill(SessionRole::Orchestrator, "coco-orchestrator")
        .unwrap();

    assert_eq!(skill.current_version, 2);
    assert_eq!(skill.current().unwrap().description, "custom");
    assert_eq!(skill.current().unwrap().body, "custom body");
}

#[test]
fn skills_json_only_stores_current_snapshots() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    store
        .update_skill(
            SessionRole::Orchestrator,
            "coco-orchestrator",
            &SkillUpdatePatch {
                description: Some("updated".to_owned()),
                body: None,
                scripts: None,
                enable_coco_shim: None,
            },
        )
        .unwrap();

    let skills: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(path.join("skills.json")).unwrap()).unwrap();
    let orchestrator = &skills["orchestrator"]["coco-orchestrator"];

    assert_eq!(orchestrator["current_version"], 2);
    assert!(orchestrator.get("versions").is_none());
    assert_eq!(orchestrator["description"], "updated");
}

#[test]
fn skill_history_is_appended_in_central_directory() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();

    store
        .update_skill(
            SessionRole::Orchestrator,
            "coco-orchestrator",
            &SkillUpdatePatch {
                description: Some("updated".to_owned()),
                body: Some("body-v2".to_owned()),
                scripts: Some(vec![SkillScript {
                    path: "scripts/history.py".to_owned(),
                    content: "print('history')".to_owned(),
                }]),
                enable_coco_shim: Some(false),
            },
        )
        .unwrap();
    store
        .rollback_skill(SessionRole::Orchestrator, "coco-orchestrator", 1)
        .unwrap();

    let history =
        read_jsonl_values(&path.join("skill-history/orchestrator/coco-orchestrator.jsonl"));

    assert_eq!(history.len(), 3);
    assert_eq!(history[0]["role"], "orchestrator");
    assert_eq!(history[0]["version"], 1);
    assert_eq!(history[1]["version"], 2);
    assert_eq!(history[2]["version"], 3);
    assert_eq!(history[2]["description"], history[0]["description"]);
    assert_eq!(history[2]["body"], history[0]["body"]);
    assert_eq!(history[1]["scripts"][0]["path"], "scripts/history.py");
    assert_eq!(history[2]["scripts"], history[0]["scripts"]);
}

#[test]
fn shared_skill_history_uses_shared_scope_directory() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    store
        .add_skill(
            SessionRole::Runner,
            "coco-orchestrator",
            SkillVersionSpec {
                description: "shared".to_owned(),
                body: "runner-shared".to_owned(),
                scripts: Vec::new(),
                enable_coco_shim: false,
            },
        )
        .unwrap();

    assert!(
        path.join("skill-history/shared/coco-orchestrator.jsonl")
            .is_file()
    );
    assert!(
        !path
            .join("skill-history/orchestrator/coco-orchestrator.jsonl")
            .exists()
    );
    assert!(
        !path
            .join("skill-history/runner/coco-orchestrator.jsonl")
            .exists()
    );

    let history = read_jsonl_values(&path.join("skill-history/shared/coco-orchestrator.jsonl"));
    assert_eq!(history.len(), 2);
    assert_eq!(history[0]["role"], "orchestrator");
    assert_eq!(history[1]["role"], "runner");
}

#[test]
fn skill_history_paths_do_not_collide_for_role_suffixed_names() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    store
        .add_skill(
            SessionRole::Runner,
            "foo",
            SkillVersionSpec {
                description: "runner only".to_owned(),
                body: "runner-only".to_owned(),
                scripts: Vec::new(),
                enable_coco_shim: false,
            },
        )
        .unwrap();
    store
        .add_skill(
            SessionRole::Orchestrator,
            "foo.runner",
            SkillVersionSpec {
                description: "shared orchestrator".to_owned(),
                body: "orchestrator".to_owned(),
                scripts: Vec::new(),
                enable_coco_shim: false,
            },
        )
        .unwrap();
    store
        .add_skill(
            SessionRole::Runner,
            "foo.runner",
            SkillVersionSpec {
                description: "shared runner".to_owned(),
                body: "runner".to_owned(),
                scripts: Vec::new(),
                enable_coco_shim: false,
            },
        )
        .unwrap();

    assert!(path.join("skill-history/runner/foo.jsonl").is_file());
    assert!(path.join("skill-history/shared/foo.runner.jsonl").is_file());

    let runner_only = read_jsonl_values(&path.join("skill-history/runner/foo.jsonl"));
    let shared = read_jsonl_values(&path.join("skill-history/shared/foo.runner.jsonl"));

    assert_eq!(runner_only.len(), 1);
    assert_eq!(runner_only[0]["name"], "foo");
    assert_eq!(runner_only[0]["role"], "runner");
    assert_eq!(shared.len(), 2);
    assert_eq!(shared[0]["name"], "foo.runner");
    assert_eq!(shared[1]["name"], "foo.runner");

    let reopened = FsStore::open(&path).unwrap();
    assert_eq!(
        reopened
            .get_skill(SessionRole::Runner, "foo")
            .unwrap()
            .current_version,
        1
    );
    assert_eq!(
        reopened
            .get_skill(SessionRole::Orchestrator, "foo.runner")
            .unwrap()
            .current_version,
        1
    );
    assert_eq!(
        reopened
            .get_skill(SessionRole::Runner, "foo.runner")
            .unwrap()
            .current_version,
        1
    );
}

#[test]
fn skill_update_does_not_advance_when_history_append_fails() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    store.fail_next_skill_history_append();

    let err = store
        .update_skill(
            SessionRole::Orchestrator,
            "coco-orchestrator",
            &SkillUpdatePatch {
                description: Some("should-not-persist".to_owned()),
                body: None,
                scripts: None,
                enable_coco_shim: None,
            },
        )
        .unwrap_err();

    assert!(matches!(err, Error::WriteStoreLog { .. }));
    assert_eq!(
        store
            .get_skill(SessionRole::Orchestrator, "coco-orchestrator")
            .unwrap()
            .current_version,
        1
    );

    let skills: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(path.join("skills.json")).unwrap()).unwrap();
    assert_eq!(
        skills["orchestrator"]["coco-orchestrator"]["current_version"],
        1
    );

    let reopened = FsStore::open(&path).unwrap();
    assert_eq!(
        reopened
            .get_skill(SessionRole::Orchestrator, "coco-orchestrator")
            .unwrap()
            .current_version,
        1
    );
}

#[test]
fn skill_add_recovers_from_history_when_snapshot_write_fails() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    store.fail_next_skill_snapshot_write();

    let created = store
        .add_skill(
            SessionRole::Runner,
            "wal-only",
            SkillVersionSpec {
                description: "checkpoint can lag".to_owned(),
                body: "history is authoritative".to_owned(),
                scripts: Vec::new(),
                enable_coco_shim: false,
            },
        )
        .unwrap();

    assert_eq!(created.current_version, 1);
    assert!(store.get_skill(SessionRole::Runner, "wal-only").is_ok());

    let skills: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(path.join("skills.json")).unwrap()).unwrap();
    assert!(skills["runner"].get("wal-only").is_none());

    let history = read_jsonl_values(&path.join("skill-history/runner/wal-only.jsonl"));
    assert_eq!(history.len(), 1);
    assert_eq!(history[0]["name"], "wal-only");
    assert_eq!(history[0]["role"], "runner");

    let reopened = FsStore::open(&path).unwrap();
    let reopened_skill = reopened.get_skill(SessionRole::Runner, "wal-only").unwrap();
    assert_eq!(reopened_skill.current_version, 1);
    assert_eq!(
        reopened_skill.current().unwrap().description,
        "checkpoint can lag"
    );

    let repaired_skills: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(path.join("skills.json")).unwrap()).unwrap();
    assert_eq!(repaired_skills["runner"]["wal-only"]["current_version"], 1);
}

#[test]
fn open_rejects_paused_merged_state_with_anchor_outside_target_branch() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let root_id = store.root_id();
    let base_anchor_id = store.append(make_session_anchor_node(&root_id)).unwrap();
    let other_anchor_id = store.append(make_session_anchor_node(&root_id)).unwrap();
    store.fork("base", &base_anchor_id).unwrap();
    store.fork("other", &other_anchor_id).unwrap();
    store.fork("main", &root_id).unwrap();
    store
        .set_session_state(
            "main",
            Some(&SessionState::Active),
            SessionState::Paused {
                target_branch: "base".to_owned(),
                reason: PauseReason::Merged {
                    merged_anchor_id: base_anchor_id.clone(),
                },
            },
        )
        .unwrap();

    let sessions_path = path.join("sessions.json");
    let mut sessions: std::collections::HashMap<String, SessionState> =
        serde_json::from_str(&fs::read_to_string(&sessions_path).unwrap()).unwrap();
    let state = sessions.get_mut("main").unwrap();
    *state = SessionState::Paused {
        target_branch: "base".to_owned(),
        reason: PauseReason::Merged {
            merged_anchor_id: other_anchor_id,
        },
    };
    fs::write(
        &sessions_path,
        serde_json::to_vec_pretty(&sessions).unwrap(),
    )
    .unwrap();

    let err = FsStore::open(&path).unwrap_err();

    assert!(matches!(err, Error::CorruptedStore { .. }));
}

#[test]
fn rebase_rewrites_branch_view_but_preserves_dangling_nodes() {
    let (_tempdir, path) = temp_store_path();
    let store = FsStore::open(&path).unwrap();
    let root_id = store.root_id();
    let session_id = store.append(make_session_anchor_node(&root_id)).unwrap();
    let child_id = store.append(make_text_node(&session_id, "child")).unwrap();
    store.fork("main", &child_id).unwrap();

    let new_head = store
        .rebase_session(
            "main",
            &SessionAnchorPatch {
                provider: Some(Some("anthropic".to_owned())),
                ..SessionAnchorPatch::default()
            },
        )
        .unwrap();

    let branch_nodes = read_jsonl_nodes(&path.join("branches/main.jsonl"));
    let global_nodes = read_jsonl_nodes(&path.join("nodes.jsonl"));
    let new_chain = store.ancestry("main").unwrap();
    let new_session_id = new_chain[1].id.clone();

    assert_eq!(
        branch_nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect::<Vec<_>>(),
        vec![root_id.as_str(), new_session_id.as_str(), new_head.as_str()]
    );
    assert!(global_nodes.iter().any(|node| node.id == session_id));
    assert!(global_nodes.iter().any(|node| node.id == child_id));
    assert!(global_nodes.iter().any(|node| node.id == new_session_id));
    assert!(global_nodes.iter().any(|node| node.id == new_head));
}
