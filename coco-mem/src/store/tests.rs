use std::collections::HashMap;
use std::fs;
use std::path::Path;

use super::fs::FsStore;
use super::memory::MemoryStore;
use super::state::StoreState;
use crate::Store as StoreTrait;
use crate::{
    Anchor, Kind, NewNode, Node, PauseReason, PromptAnchor, Role, SessionAnchor,
    SessionAnchorPatch, SessionState, StoreError as Error,
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
                provider: "openai".to_owned(),
                model: "gpt-5.4".to_owned(),
                tools: vec![],
                system_prompt: "system".to_owned(),
                prompt: "prompt".to_owned(),
                temperature: Some(0.1),
                max_tokens: Some(64),
                additional_params: Some(json!({"reasoning_effort": "low"})),
            },
        )),
    }
}

fn make_session_anchor_with_merge_parent(parent: &str, merge_parent: &str) -> NewNode {
    NewNode {
        parent: parent.to_owned(),
        role: Role::System,
        metadata: None,
        kind: Kind::Anchor(Anchor::session(
            vec![merge_parent.to_owned()],
            SessionAnchor {
                provider: "openai".to_owned(),
                model: "gpt-5.4".to_owned(),
                tools: vec![],
                system_prompt: "system".to_owned(),
                prompt: "prompt".to_owned(),
                temperature: Some(0.1),
                max_tokens: Some(64),
                additional_params: Some(json!({"reasoning_effort": "low"})),
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
            merge_parents.iter().map(|id| (*id).to_owned()).collect(),
            PromptAnchor {
                prompt: "merge prompt".to_owned(),
            },
        )),
    }
}

fn submit_prompt_job<S: StoreTrait>(store: &S, branch: &str, prompt: &str) -> crate::Job {
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

fn temp_store_path() -> (tempfile::TempDir, std::path::PathBuf) {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    (tempdir, path)
}

trait InspectableStore {
    fn snapshot_state(&self) -> StoreState;
}

trait TestStoreFactory {
    type Store: StoreTrait + InspectableStore;

    fn create() -> Self::Store;
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
    type Store = MemoryStore;

    fn create() -> Self::Store {
        MemoryStore::new()
    }
}

struct FsFactory;

impl TestStoreFactory for FsFactory {
    type Store = FsStore;

    fn create() -> Self::Store {
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
                provider: Some("anthropic".to_owned()),
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
    assert_eq!(session.provider, "anthropic");
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
    assert_eq!(old_anchor.as_session().unwrap().provider, "openai");
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
    assert_eq!(anchor.merge_parents(), [session_id.as_str()]);
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
                provider: Some("anthropic".to_owned()),
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
    assert_eq!(anchor.as_session().unwrap().provider, "openai");
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
                provider: Some("anthropic".to_owned()),
                ..SessionAnchorPatch::default()
            },
        )
        .unwrap();

    let ancestry = store.ancestry("main").unwrap();
    assert_eq!(ancestry[0].created_at, child_created_at);
    assert_eq!(ancestry[1].created_at, session_created_at);
    assert_eq!(ancestry[2].id, root_id);
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
            fn job_round_trip() {
                assert_job_round_trip::<$factory>();
            }

            #[test]
            fn create_job_generates_unique_ids() {
                assert_create_job_generates_unique_ids::<$factory>();
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
    assert!(path.join("nodes.jsonl").is_file());
    assert!(path.join("sessions.json").is_file());
    assert!(path.join("branches").is_dir());

    let nodes = fs::read_to_string(path.join("nodes.jsonl")).unwrap();
    assert!(nodes.lines().count() >= 1);
    assert_eq!(store.ancestry(&store.root_id()).unwrap().len(), 1);
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
                provider: Some("anthropic".to_owned()),
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
