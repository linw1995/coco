use std::collections::HashMap;

use super::sqlite::SqliteStore;
use crate::{
    Anchor, BranchStore, JobStatus, JobStore, Kind, MergeParent, MessageQueueItem,
    MessageQueueStore, NewNode, NodeStore, PauseReason, Preset, PresetStore, PromptAnchor, Role,
    SessionAnchor, SessionAnchorPatch, SessionRole, SessionState, SessionStore, SkillScript,
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
                attachments: vec![],
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
                    attachments: vec![],
                },
            )),
        })
        .unwrap();
    store.submit_job(branch, &prompt_anchor_id).unwrap()
}

trait TestStoreFactory {
    type Backend: PresetStore
        + BranchStore
        + JobStore
        + MessageQueueStore
        + NodeStore
        + SessionStore
        + SkillStore;

    fn create() -> Self::Backend;
}

struct SqliteFactory;

impl TestStoreFactory for SqliteFactory {
    type Backend = SqliteStore;

    fn create() -> Self::Backend {
        SqliteStore::open_temporary().expect("temporary SQLite store should open")
    }
}

fn assert_new_store_exposes_root_text_node<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root = store.get_node(&store.root_id()).unwrap();

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

    let stored = store.get_node(&child_id).unwrap();
    assert_eq!(stored.parent, session_id);
    let children = store.list_children(&stored.parent).unwrap();
    assert!(children.iter().any(|node| node.id == child_id));
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

    assert!(
        store
            .list_children(&session_id)
            .unwrap()
            .iter()
            .any(|node| node.id == anchor_id)
    );
    assert!(
        store
            .list_children(&merge_parent_id)
            .unwrap()
            .iter()
            .any(|node| node.id == anchor_id)
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

    assert!(
        store
            .list_children(&merge_parent_id)
            .unwrap()
            .iter()
            .any(|node| node.id == anchor_id)
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
                    attachments: vec![],
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

    assert!(
        store
            .list_children(&right_leaf)
            .unwrap()
            .iter()
            .any(|node| node.id == merge_id)
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
    let old_child = store.get_node(&child_id).unwrap();
    let old_session = store.get_node(&session_id).unwrap();
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
    assert_eq!(ancestry[1].created_at, old_session.created_at);
    assert_eq!(ancestry[2].id, root_id);

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
    let old_session = store.get_node(&session_id).unwrap();
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
    let session_created_at = store.get_node(&session_id).unwrap().created_at;
    let child_created_at = store.get_node(&child_id).unwrap().created_at;
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
        .handoff_session("main", &SessionAnchorPatch::default(), "handoff prompt")
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
    assert_eq!(session.prompt, "handoff prompt");
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
        .handoff_session("main", &SessionAnchorPatch::default(), "handoff prompt")
        .unwrap_err();

    assert!(matches!(
        err,
        Error::MissingSessionAnchor { branch } if branch == "main"
    ));
}

fn assert_handoff_session_rejects_empty_prompt<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    let session_id = store.append(make_session_anchor_node(&root_id)).unwrap();
    store.fork("main", &session_id).unwrap();

    let err = store
        .handoff_session("main", &SessionAnchorPatch::default(), "  ")
        .unwrap_err();

    assert!(matches!(err, Error::InvalidSessionHandoffPrompt));
    assert_eq!(store.get_branch_head("main").unwrap(), session_id);
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
            "handoff prompt",
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
    assert_eq!(session.prompt, "handoff prompt");
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

fn assert_job_work_branch_round_trip<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    store.fork("main", &root_id).unwrap();
    store.fork("recovery", &root_id).unwrap();
    let job = submit_prompt_job(&store, "main", "hello");

    assert_eq!(job.work_branch, "main");
    let updated = store
        .set_job_work_branch(&job.job_id, "main", "recovery")
        .unwrap();

    assert_eq!(updated.branch, "main");
    assert_eq!(updated.work_branch, "recovery");
    assert_eq!(store.get_job(&job.job_id).unwrap().work_branch, "recovery");
}

fn assert_set_job_work_branch_rejects_stale_expected_branch<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    store.fork("main", &root_id).unwrap();
    store.fork("recovery", &root_id).unwrap();
    let job = submit_prompt_job(&store, "main", "hello");

    let err = store
        .set_job_work_branch(&job.job_id, "stale", "recovery")
        .unwrap_err();

    assert!(matches!(
        err,
        Error::PromptJobMoved { job_id, expected, actual }
            if job_id == job.job_id && expected == "stale" && actual == "main"
    ));
}

fn assert_submit_job_rejects_active_work_branch<F>()
where
    F: TestStoreFactory,
{
    let store = F::create();
    let root_id = store.root_id();
    store.fork("main", &root_id).unwrap();
    store.fork("recovery", &root_id).unwrap();
    let first = submit_prompt_job(&store, "main", "hello");
    store
        .set_job_work_branch(&first.job_id, "main", "recovery")
        .unwrap();

    let err = store.submit_job("recovery", &root_id).unwrap_err();

    assert!(matches!(
        err,
        Error::PromptJobActiveOnBranch { branch, job_id }
            if branch == "recovery" && job_id == first.job_id
    ));
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
                    attachments: vec![],
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

async fn assert_message_queue_round_trip<F>()
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
        store.list_queue_messages("hooks").await.unwrap(),
        vec![first.clone(), second.clone(),]
    );
    assert_eq!(store.peek_message("hooks").unwrap(), Some(first.clone()));
    assert_eq!(store.peek_message("missing").unwrap(), None);
    assert_eq!(store.dequeue_message("hooks").unwrap(), Some(first));
    assert_eq!(store.peek_message("hooks").unwrap(), Some(second.clone()));
    assert_eq!(store.dequeue_message("hooks").unwrap(), Some(second));
    assert_eq!(store.dequeue_message("hooks").unwrap(), None);
    assert_eq!(store.peek_message("hooks").unwrap(), None);
    assert!(store.list_queue_messages("hooks").await.unwrap().is_empty());
    assert!(store.list_message_queues().await.unwrap().is_empty());
}

async fn assert_message_queue_isolates_named_queues<F>()
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
        store.list_queue_messages("hooks").await.unwrap(),
        vec![hook.clone()]
    );
    assert_eq!(
        store.list_queue_messages("scheduler").await.unwrap(),
        vec![scheduler.clone()]
    );
    assert_eq!(
        store.list_message_queues().await.unwrap(),
        vec!["hooks".to_owned(), "scheduler".to_owned()]
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
    let recovery = store
        .get_skill(SessionRole::Orchestrator, "recovery")
        .unwrap();
    let compact = store
        .get_skill(SessionRole::Orchestrator, "compact")
        .unwrap();
    let runner = store.get_skill(SessionRole::Runner, "coco-runner").unwrap();
    let telegram = store.get_skill(SessionRole::Runner, "telegram").unwrap();

    assert_eq!(orchestrator.current_version, 1);
    assert_eq!(new_skill.current_version, 1);
    assert_eq!(cronjob.current_version, 1);
    assert_eq!(recovery.current_version, 1);
    assert_eq!(compact.current_version, 1);
    assert_eq!(telegram.current_version, 1);
    assert_eq!(runner.current_version, 1);
    assert!(orchestrator.current().unwrap().enable_coco_shim);
    assert!(new_skill.current().unwrap().enable_coco_shim);
    assert!(cronjob.current().unwrap().enable_coco_shim);
    assert!(recovery.current().unwrap().enable_coco_shim);
    assert!(compact.current().unwrap().enable_coco_shim);
    assert!(telegram.current().unwrap().enable_coco_shim);
    assert!(runner.current().unwrap().enable_coco_shim);
    assert_eq!(cronjob.current().unwrap().scripts.len(), 3);
    assert_eq!(telegram.current().unwrap().scripts.len(), 3);
    assert!(
        cronjob
            .current()
            .unwrap()
            .scripts
            .iter()
            .any(|script| script.path == "scripts/cronjob_crontab.py")
    );
    assert!(
        telegram
            .current()
            .unwrap()
            .scripts
            .iter()
            .any(|script| script.path == "scripts/telegram_download.py")
    );
    assert!(
        orchestrator
            .current()
            .unwrap()
            .body
            .contains("fork from the node before the `SkillInvocation`")
    );
    assert!(
        orchestrator.current().unwrap().body.contains(
            "--tool exec_command --tool write_stdin --tool search_skill --tool load_image"
        )
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
        recovery
            .current()
            .unwrap()
            .body
            .contains("Use this orchestrator skill from the built-in `day` branch")
    );
    assert!(
        recovery
            .current()
            .unwrap()
            .body
            .contains("Do not create another recovery branch")
    );
    assert!(
        recovery
            .current()
            .unwrap()
            .body
            .contains("coco job worker --job <job-id>")
    );
    assert!(recovery.current().unwrap().body.contains("Do not fork a"));
    assert!(
        compact
            .current()
            .unwrap()
            .body
            .contains("coco session handoff --branch <branch>")
    );
    assert!(
        compact
            .current()
            .unwrap()
            .body
            .contains("Do not use `session rebase` for compaction")
    );
    assert!(
        compact
            .current()
            .unwrap()
            .body
            .contains("Self-compaction delegation")
    );
    assert!(
        compact
            .current()
            .unwrap()
            .body
            .contains("do not run `coco session handoff`")
    );
    assert!(
        compact
            .current()
            .unwrap()
            .body
            .contains("coco job --async --json --branch <worker-branch>")
    );
    assert!(
        compact
            .current()
            .unwrap()
            .body
            .contains("wait until the target branch has no active prompt jobs")
    );
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
    assert!(
        telegram
            .current()
            .unwrap()
            .body
            .contains("COCO_EXEC_WORKSPACE/telegram-downloads")
    );
    assert!(telegram.current().unwrap().body.contains("--photo"));
    assert!(telegram.current().unwrap().body.contains("--document"));
    assert!(!telegram.current().unwrap().body.contains("/tmp/telegram"));
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
            fn handoff_session_rejects_empty_prompt() {
                assert_handoff_session_rejects_empty_prompt::<$factory>();
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
            fn job_work_branch_round_trip() {
                assert_job_work_branch_round_trip::<$factory>();
            }

            #[test]
            fn set_job_work_branch_rejects_stale_expected_branch() {
                assert_set_job_work_branch_rejects_stale_expected_branch::<$factory>();
            }

            #[test]
            fn submit_job_rejects_active_work_branch() {
                assert_submit_job_rejects_active_work_branch::<$factory>();
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

            #[tokio::test]
            async fn message_queue_round_trip() {
                assert_message_queue_round_trip::<$factory>().await;
            }

            #[tokio::test]
            async fn message_queue_isolates_named_queues() {
                assert_message_queue_isolates_named_queues::<$factory>().await;
            }

            #[test]
            fn message_queue_ids_are_content_derived() {
                assert_message_queue_ids_are_content_derived::<$factory>();
            }

        }
    };
}

define_common_store_tests!(sqlite_store, SqliteFactory);
