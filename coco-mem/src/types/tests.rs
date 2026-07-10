use super::{
    Anchor, BackendMetadata, ExecutionMetadata, Kind, MergeParent, NewNode, Node, PauseReason,
    Preset, PromptAnchor, ProviderMetadata, Role, SessionAnchor, SessionAnchorPatch, SessionRole,
    SessionState, SkillInvocationAnchor, SkillInvocationMode, SkillResultAnchor, SkillScript,
    SkillVersion, SkillVersionSpec, Tool, ToolResult, ToolUse,
};
use jiff::Timestamp;
use serde_json::json;

fn fixed_timestamp() -> Timestamp {
    "2026-03-25T09:10:11Z".parse().unwrap()
}

fn make_text_node(parent: &str, text: &str, created_at: Timestamp) -> Node {
    Node::new(
        parent.to_owned(),
        Role::User,
        None,
        Kind::Text(text.to_owned()),
        created_at,
    )
}

fn make_failure_node(parent: &str, text: &str, created_at: Timestamp) -> Node {
    Node::new(
        parent.to_owned(),
        Role::System,
        None,
        Kind::Failure(text.to_owned()),
        created_at,
    )
}

fn make_session_anchor() -> SessionAnchor {
    SessionAnchor {
        role: SessionRole::Orchestrator,
        provider_profile: None,
        provider: Some("openai".to_owned()),
        model: "gpt-5.4".to_owned(),
        tools: vec![Tool {
            name: "search".to_owned(),
            description: "Search docs".to_owned(),
            input_schema: json!({"type": "object"}),
        }],
        system_prompt: "system".to_owned(),
        prompt: "prompt".to_owned(),
        temperature: Some(0.1),
        max_tokens: Some(128),
        additional_params: Some(json!({"service_tier": "default"})),
        enable_coco_shim: false,
        active_skill: None,
    }
}

fn make_prompt_anchor() -> PromptAnchor {
    PromptAnchor {
        prompt: "merge prompt".to_owned(),
        attachments: vec![],
    }
}

fn make_skill_invocation_anchor() -> SkillInvocationAnchor {
    SkillInvocationAnchor {
        skill_name: "find-skills".to_owned(),
        mode: SkillInvocationMode::Handoff {
            prompt: "find a skill for this task".to_owned(),
        },
    }
}

fn make_skill_result_anchor() -> SkillResultAnchor {
    SkillResultAnchor {
        skill_name: "find-skills".to_owned(),
        output: "child result".to_owned(),
    }
}

#[test]
fn node_id_is_stable_for_same_payload() {
    let left = make_text_node("parent", "hello", fixed_timestamp());
    let right = make_text_node("parent", "hello", fixed_timestamp());

    assert_eq!(left.id, right.id);
}

#[test]
fn node_id_changes_when_created_at_changes() {
    let left = make_text_node("parent", "hello", "2026-03-25T09:10:11Z".parse().unwrap());
    let right = make_text_node("parent", "hello", "2026-03-25T09:10:12Z".parse().unwrap());

    assert_ne!(left.id, right.id);
}

#[test]
fn node_id_changes_when_anchor_variant_changes() {
    let left = Node::new(
        "parent".to_owned(),
        Role::System,
        None,
        Kind::Anchor(Anchor::session(vec![], make_session_anchor())),
        fixed_timestamp(),
    );
    let right = Node::new(
        "parent".to_owned(),
        Role::System,
        None,
        Kind::Anchor(Anchor::prompt(
            vec![MergeParent::merge("merge-a")],
            make_prompt_anchor(),
        )),
        fixed_timestamp(),
    );

    assert_ne!(left.id, right.id);
}

#[test]
fn node_id_changes_when_prompt_anchor_merge_parents_change() {
    let left = Node::new(
        "parent".to_owned(),
        Role::System,
        None,
        Kind::Anchor(Anchor::prompt(
            vec![MergeParent::merge("merge-a")],
            make_prompt_anchor(),
        )),
        fixed_timestamp(),
    );
    let right = Node::new(
        "parent".to_owned(),
        Role::System,
        None,
        Kind::Anchor(Anchor::prompt(
            vec![MergeParent::merge("merge-b")],
            make_prompt_anchor(),
        )),
        fixed_timestamp(),
    );

    assert_ne!(left.id, right.id);
}

#[test]
fn node_id_changes_when_session_anchor_merge_parents_change() {
    let left = Node::new(
        "parent".to_owned(),
        Role::System,
        None,
        Kind::Anchor(Anchor::session(
            vec![MergeParent::merge("merge-a")],
            make_session_anchor(),
        )),
        fixed_timestamp(),
    );
    let right = Node::new(
        "parent".to_owned(),
        Role::System,
        None,
        Kind::Anchor(Anchor::session(
            vec![MergeParent::merge("merge-b")],
            make_session_anchor(),
        )),
        fixed_timestamp(),
    );

    assert_ne!(left.id, right.id);
}

#[test]
fn node_id_changes_when_skill_invocation_anchor_mode_changes() {
    let left = Node::new(
        "parent".to_owned(),
        Role::System,
        None,
        Kind::Anchor(Anchor::skill_invocation(
            vec![MergeParent::merge("merge-a")],
            make_skill_invocation_anchor(),
        )),
        fixed_timestamp(),
    );
    let right = Node::new(
        "parent".to_owned(),
        Role::System,
        None,
        Kind::Anchor(Anchor::skill_invocation(
            vec![MergeParent::merge("merge-a")],
            SkillInvocationAnchor {
                mode: SkillInvocationMode::InheritContext,
                ..make_skill_invocation_anchor()
            },
        )),
        fixed_timestamp(),
    );

    assert_ne!(left.id, right.id);
}

#[test]
fn node_id_changes_when_skill_result_anchor_output_changes() {
    let left = Node::new(
        "parent".to_owned(),
        Role::System,
        None,
        Kind::Anchor(Anchor::skill_result(
            vec![MergeParent::merge("merge-a")],
            make_skill_result_anchor(),
        )),
        fixed_timestamp(),
    );
    let right = Node::new(
        "parent".to_owned(),
        Role::System,
        None,
        Kind::Anchor(Anchor::skill_result(
            vec![MergeParent::merge("merge-a")],
            SkillResultAnchor {
                output: "different result".to_owned(),
                ..make_skill_result_anchor()
            },
        )),
        fixed_timestamp(),
    );

    assert_ne!(left.id, right.id);
}

#[test]
fn node_id_changes_when_skill_result_anchor_merge_parents_change() {
    let left = Node::new(
        "parent".to_owned(),
        Role::System,
        None,
        Kind::Anchor(Anchor::skill_result(
            vec![MergeParent::merge("merge-a")],
            make_skill_result_anchor(),
        )),
        fixed_timestamp(),
    );
    let right = Node::new(
        "parent".to_owned(),
        Role::System,
        None,
        Kind::Anchor(Anchor::skill_result(
            vec![MergeParent::merge("merge-b")],
            make_skill_result_anchor(),
        )),
        fixed_timestamp(),
    );

    assert_ne!(left.id, right.id);
}

#[test]
fn node_id_changes_when_execution_metadata_changes() {
    let left = Node::new(
        "parent".to_owned(),
        Role::User,
        BackendMetadata::builder()
            .execution(&ExecutionMetadata::new("execution-1".to_owned()))
            .build(),
        Kind::Text("hello".to_owned()),
        fixed_timestamp(),
    );
    let right = Node::new(
        "parent".to_owned(),
        Role::User,
        BackendMetadata::builder()
            .execution(&ExecutionMetadata::new("execution-2".to_owned()))
            .build(),
        Kind::Text("hello".to_owned()),
        fixed_timestamp(),
    );

    assert_ne!(left.id, right.id);
}

#[test]
fn node_id_changes_when_text_kind_changes_to_failure_kind() {
    let left = make_text_node("parent", "hello", fixed_timestamp());
    let right = make_failure_node("parent", "hello", fixed_timestamp());

    assert_ne!(left.id, right.id);
}

#[test]
fn node_id_is_lower_hex_sha256() {
    let node = make_text_node("parent", "hello", fixed_timestamp());

    assert_eq!(node.id.len(), 64);
    assert!(node.id.bytes().all(|byte| byte.is_ascii_hexdigit()));
    assert_eq!(node.id, node.id.to_ascii_lowercase());
}

#[test]
fn node_round_trip_preserves_id_and_created_at() {
    let node = Node::new(
        "parent".to_owned(),
        Role::LLM,
        BackendMetadata::builder()
            .provider(&ProviderMetadata::new(Some("call-1".to_owned())))
            .build(),
        Kind::tool_result(ToolResult {
            id: "call-1".to_owned(),
            output: "ok".to_owned(),
        }),
        fixed_timestamp(),
    );

    let encoded = serde_json::to_string(&node).unwrap();
    let decoded: Node = serde_json::from_str(&encoded).unwrap();

    assert_eq!(decoded.id, node.id);
    assert_eq!(decoded.parent, node.parent);
    assert_eq!(decoded.created_at, node.created_at);
}

#[test]
fn skill_version_id_is_stable_for_same_content() {
    let spec = SkillVersionSpec {
        description: "description".to_owned(),
        body: "body".to_owned(),
        scripts: vec![SkillScript {
            path: "scripts/run.py".to_owned(),
            content: "print('ok')".to_owned(),
        }],
        enable_coco_shim: true,
    };
    let left = SkillVersion::new(1, spec.clone());
    let right = SkillVersion::new(2, spec);

    assert_eq!(left.id, right.id);
    assert!(left.id_matches_content());
}

#[test]
fn skill_version_id_changes_when_content_changes() {
    let left = SkillVersion::new(
        1,
        SkillVersionSpec {
            description: "description".to_owned(),
            body: "body".to_owned(),
            scripts: Vec::new(),
            enable_coco_shim: true,
        },
    );
    let right = SkillVersion::new(
        1,
        SkillVersionSpec {
            description: "description".to_owned(),
            body: "different".to_owned(),
            scripts: Vec::new(),
            enable_coco_shim: true,
        },
    );

    assert_ne!(left.id, right.id);
}

#[test]
fn skill_version_id_is_lower_hex_sha256() {
    let version = SkillVersion::new(
        1,
        SkillVersionSpec {
            description: "description".to_owned(),
            body: "body".to_owned(),
            scripts: Vec::new(),
            enable_coco_shim: false,
        },
    );

    assert_eq!(version.id.len(), 64);
    assert!(version.id.bytes().all(|byte| byte.is_ascii_hexdigit()));
    assert_eq!(version.id, version.id.to_ascii_lowercase());
}

#[test]
fn tool_use_payload_requires_item_list() {
    let many: Kind = serde_json::from_value(serde_json::json!({
        "ToolUse": [
            {
                "id": "tool-call-1",
                "name": "exec_command",
                "input": { "cmd": "pwd" }
            },
            {
                "id": "tool-call-2",
                "name": "exec_command",
                "input": { "cmd": "ls" }
            }
        ]
    }))
    .unwrap();

    let one = serde_json::from_value::<Kind>(serde_json::json!({
        "ToolUse": {
            "id": "tool-call-1",
            "name": "exec_command",
            "input": { "cmd": "pwd" }
        }
    }));

    assert!(one.is_err());
    assert_eq!(many.as_tool_uses().unwrap().iter().count(), 2);
    assert_eq!(many.as_tool_uses().unwrap()[0].id, "tool-call-1");
}

#[test]
fn many_tool_node_metadata_round_trip_preserves_call_ids() {
    let node = Node::new(
        "parent".to_owned(),
        Role::LLM,
        Some(vec![
            BackendMetadata {
                execution_id: Some("execution-1".to_owned()),
                call_id: Some("call-1".to_owned()),
            },
            BackendMetadata {
                execution_id: Some("execution-1".to_owned()),
                call_id: Some("call-2".to_owned()),
            },
        ]),
        Kind::tool_uses(vec![
            ToolUse {
                id: "tool-call-1".to_owned(),
                name: "exec_command".to_owned(),
                input: json!({"cmd": "pwd"}),
            },
            ToolUse {
                id: "tool-call-2".to_owned(),
                name: "exec_command".to_owned(),
                input: json!({"cmd": "ls"}),
            },
        ]),
        fixed_timestamp(),
    );

    let encoded = serde_json::to_string(&node).unwrap();
    let decoded: Node = serde_json::from_str(&encoded).unwrap();
    let metadata = decoded.metadata.expect("expected node metadata");
    let items = metadata.as_slice();

    assert_eq!(items.len(), 2);
    assert_eq!(items[0].call_id.as_deref(), Some("call-1"));
    assert_eq!(items[1].call_id.as_deref(), Some("call-2"));
}

#[test]
fn failure_node_round_trip_preserves_message() {
    let node = make_failure_node("parent", "rate limited", fixed_timestamp());

    let encoded = serde_json::to_string(&node).unwrap();
    let decoded: Node = serde_json::from_str(&encoded).unwrap();

    assert_eq!(decoded.id, node.id);
    assert!(matches!(
        decoded.kind,
        Kind::Failure(message) if message == "rate limited"
    ));
}

#[test]
fn new_node_carries_only_unpersisted_fields() {
    let node = NewNode {
        parent: "parent".to_owned(),
        role: Role::System,
        metadata: BackendMetadata::builder()
            .execution(&ExecutionMetadata::new("execution-1".to_owned()))
            .build(),
        kind: Kind::Anchor(Anchor::prompt(
            vec![MergeParent::merge("merge-a"), MergeParent::merge("merge-b")],
            make_prompt_anchor(),
        )),
    };

    let encoded = serde_json::to_value(&node).unwrap();

    assert!(encoded.get("id").is_none());
    assert!(encoded.get("created_at").is_none());
}

#[test]
fn session_anchor_round_trip_preserves_prompt() {
    let node = Node::new(
        "parent".to_owned(),
        Role::System,
        None,
        Kind::Anchor(Anchor::session(
            vec![MergeParent::merge("merge-a"), MergeParent::merge("merge-b")],
            make_session_anchor(),
        )),
        fixed_timestamp(),
    );

    let encoded = serde_json::to_string(&node).unwrap();
    let decoded: Node = serde_json::from_str(&encoded).unwrap();
    let Kind::Anchor(anchor) = decoded.kind else {
        panic!("expected anchor node");
    };
    let session_anchor = anchor.as_session().expect("expected session anchor");

    assert_eq!(session_anchor.prompt, "prompt");
    assert_eq!(session_anchor.role, SessionRole::Orchestrator);
    assert_eq!(anchor.merge_parent_node_ids(), ["merge-a", "merge-b"]);
}

#[test]
fn prompt_anchor_round_trip_preserves_merge_parents() {
    let node = Node::new(
        "parent".to_owned(),
        Role::System,
        None,
        Kind::Anchor(Anchor::prompt(
            vec![MergeParent::merge("merge-a"), MergeParent::merge("merge-b")],
            make_prompt_anchor(),
        )),
        fixed_timestamp(),
    );

    let encoded = serde_json::to_string(&node).unwrap();
    let decoded: Node = serde_json::from_str(&encoded).unwrap();
    let Kind::Anchor(anchor) = decoded.kind else {
        panic!("expected anchor node");
    };
    let prompt_anchor = anchor.as_prompt().expect("expected prompt anchor");

    assert_eq!(anchor.merge_parent_node_ids(), ["merge-a", "merge-b"]);
    assert_eq!(prompt_anchor.prompt, "merge prompt");
}

#[test]
fn prompt_anchor_round_trip_preserves_shadow_merge_parent_kind() {
    let node = Node::new(
        "parent".to_owned(),
        Role::System,
        None,
        Kind::Anchor(Anchor::prompt(
            vec![MergeParent::shadow("shadow-a")],
            make_prompt_anchor(),
        )),
        fixed_timestamp(),
    );

    let encoded = serde_json::to_string(&node).unwrap();
    let decoded: Node = serde_json::from_str(&encoded).unwrap();
    let Kind::Anchor(anchor) = decoded.kind else {
        panic!("expected anchor node");
    };

    assert_eq!(
        anchor.merge_parents(),
        [MergeParent::shadow("shadow-a")].as_slice()
    );
}

#[test]
fn skill_result_anchor_round_trip_preserves_fields() {
    let node = Node::new(
        "parent".to_owned(),
        Role::System,
        None,
        Kind::Anchor(Anchor::skill_result(
            vec![MergeParent::merge("merge-a")],
            make_skill_result_anchor(),
        )),
        fixed_timestamp(),
    );

    let encoded = serde_json::to_string(&node).unwrap();
    let decoded: Node = serde_json::from_str(&encoded).unwrap();
    let Kind::Anchor(anchor) = decoded.kind else {
        panic!("expected anchor node");
    };
    let skill_result = anchor
        .as_skill_result()
        .expect("expected skill result anchor");

    assert_eq!(anchor.merge_parent_node_ids(), ["merge-a"]);
    assert_eq!(skill_result.skill_name, "find-skills");
    assert_eq!(skill_result.output, "child result");
}

#[test]
fn skill_invocation_anchor_round_trip_preserves_fields() {
    let node = Node::new(
        "parent".to_owned(),
        Role::System,
        None,
        Kind::Anchor(Anchor::skill_invocation(
            vec![MergeParent::merge("merge-a")],
            make_skill_invocation_anchor(),
        )),
        fixed_timestamp(),
    );

    let encoded = serde_json::to_string(&node).unwrap();
    let decoded: Node = serde_json::from_str(&encoded).unwrap();
    let Kind::Anchor(anchor) = decoded.kind else {
        panic!("expected anchor node");
    };
    let invocation = anchor
        .as_skill_invocation()
        .expect("expected skill invocation anchor");

    assert_eq!(anchor.merge_parent_node_ids(), ["merge-a"]);
    assert_eq!(invocation.skill_name, "find-skills");
    assert_eq!(
        invocation.mode,
        SkillInvocationMode::Handoff {
            prompt: "find a skill for this task".to_owned()
        }
    );
}

#[test]
fn node_metadata_round_trip_preserves_execution_id() {
    let node = Node::new(
        "parent".to_owned(),
        Role::User,
        BackendMetadata::builder()
            .execution(&ExecutionMetadata::new("execution-1".to_owned()))
            .provider(&ProviderMetadata::new(Some("call-1".to_owned())))
            .build(),
        Kind::Text("hello".to_owned()),
        fixed_timestamp(),
    );

    let encoded = serde_json::to_string(&node).unwrap();
    let decoded: Node = serde_json::from_str(&encoded).unwrap();
    let metadata = decoded
        .metadata
        .expect("expected node metadata")
        .first()
        .expect("expected single node metadata")
        .clone();

    assert_eq!(metadata.execution_id.as_deref(), Some("execution-1"));
    assert_eq!(metadata.call_id.as_deref(), Some("call-1"));
}

#[test]
fn backend_metadata_builder_returns_none_when_empty() {
    assert_eq!(BackendMetadata::builder().build(), None);
}

#[test]
fn session_anchor_patch_updates_selected_fields() {
    let updated = make_session_anchor().apply_patch(&SessionAnchorPatch {
        role: Some(SessionRole::Runner),
        provider_profile: Some(Some("anthropic-main".to_owned())),
        provider: Some(Some("anthropic".to_owned())),
        model: Some("claude-sonnet-4-20250514".to_owned()),
        tools: Some(vec![]),
        system_prompt: Some("new system".to_owned()),
        temperature: Some(None),
        max_tokens: Some(Some(256)),
        additional_params: Some(Some(json!({"service_tier": "priority"}))),
        enable_coco_shim: Some(true),
    });

    assert_eq!(updated.role, SessionRole::Runner);
    assert_eq!(updated.provider_profile.as_deref(), Some("anthropic-main"));
    assert_eq!(updated.provider.as_deref(), Some("anthropic"));
    assert_eq!(updated.model, "claude-sonnet-4-20250514");
    assert!(updated.tools.is_empty());
    assert_eq!(updated.system_prompt, "new system");
    assert_eq!(updated.prompt, "prompt");
    assert_eq!(updated.temperature, None);
    assert_eq!(updated.max_tokens, Some(256));
    assert!(updated.enable_coco_shim);
    assert_eq!(
        updated.additional_params,
        Some(json!({"service_tier": "priority"}))
    );
}

#[test]
fn preset_applies_session_and_role_settings() {
    let updated = Preset {
        role: SessionRole::Runner,
        provider_profile: "anthropic-main".to_owned(),
        model: "claude-sonnet-4-20250514".to_owned(),
        tools: vec![],
        system_prompt: "new system".to_owned(),
        prompt: "new prompt".to_owned(),
        temperature: Some(0.2),
        max_tokens: Some(256),
        additional_params: Some(json!({"service_tier": "priority"})),
        enable_coco_shim: true,
    }
    .apply_to_session_anchor(&make_session_anchor());

    assert_eq!(updated.role, SessionRole::Runner);
    assert_eq!(updated.provider_profile.as_deref(), Some("anthropic-main"));
    assert_eq!(updated.provider, None);
    assert_eq!(updated.model, "claude-sonnet-4-20250514");
    assert!(updated.tools.is_empty());
    assert_eq!(updated.system_prompt, "new system");
    assert_eq!(updated.prompt, "new prompt");
    assert_eq!(updated.temperature, Some(0.2));
    assert_eq!(updated.max_tokens, Some(256));
    assert_eq!(
        updated.additional_params,
        Some(json!({"service_tier": "priority"}))
    );
    assert!(updated.enable_coco_shim);
}

#[test]
fn session_state_round_trip_preserves_paused_reason() {
    let state = SessionState::Paused {
        target_branch: "base".to_owned(),
        reason: PauseReason::Merged {
            merged_anchor_id: "anchor-1".to_owned(),
        },
    };

    let encoded = serde_json::to_string(&state).unwrap();
    let decoded: SessionState = serde_json::from_str(&encoded).unwrap();

    assert_eq!(decoded, state);
}

#[test]
fn session_role_parse_accepts_known_values() {
    assert_eq!(
        SessionRole::parse("orchestrator"),
        Some(SessionRole::Orchestrator)
    );
    assert_eq!(SessionRole::parse("runner"), Some(SessionRole::Runner));
    assert_eq!(SessionRole::parse("unknown"), None);
}
