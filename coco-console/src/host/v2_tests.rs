use crate::graph::{
    GraphBranch, GraphEdge, GraphEdgeKind, GraphMode, GraphNode, GraphSnapshot,
    build_graph_snapshot, build_graph_snapshot_with_mode,
};
use crate::layout::{GRAPH_RANK_STEP, GRAPH_ROW_STEP, layout_graph};
use crate::render::render_snapshot_page;
use coco_mem::{
    Anchor, BranchStore, Kind, MergeParent, NewNode, NodeStore, PromptAnchor, Role, SessionAnchor,
    SessionRole, SessionState, SkillInvocationAnchor, SkillInvocationMode, SqliteStore, Tool,
};
use serde_json::json;

fn node(id: &str, created_at_ns: i128) -> GraphNode {
    GraphNode {
        id: id.to_owned(),
        short_id: id.to_owned(),
        kind: "text".to_owned(),
        role: "User".to_owned(),
        created_at: format!("time-{created_at_ns}"),
        created_at_ns,
        content: id.to_owned(),
        summary: id.to_owned(),
        labels: Vec::new(),
        provider_context_ids: Vec::new(),
    }
}

fn snapshot() -> GraphSnapshot {
    GraphSnapshot {
        version: 7,
        mode: GraphMode::All,
        root_id: "root".to_owned(),
        nodes: vec![node("a", 1), node("b", 2), node("c", 3)],
        edges: vec![
            GraphEdge {
                source: "a".to_owned(),
                target: "b".to_owned(),
                kind: GraphEdgeKind::Primary,
            },
            GraphEdge {
                source: "a".to_owned(),
                target: "c".to_owned(),
                kind: GraphEdgeKind::Primary,
            },
        ],
        branches: vec![
            GraphBranch {
                name: "draft".to_owned(),
                head_id: "c".to_owned(),
                visible_head_id: Some("c".to_owned()),
                state: SessionState::Active,
            },
            GraphBranch {
                name: "main".to_owned(),
                head_id: "b".to_owned(),
                visible_head_id: Some("b".to_owned()),
                state: SessionState::Active,
            },
        ],
        provider_contexts: Vec::new(),
    }
}

#[test]
fn layout_uses_fixed_compact_steps() {
    let layout = layout_graph(&snapshot());
    let a = layout
        .nodes
        .iter()
        .find(|node| node.node_id == "a")
        .unwrap();
    let b = layout
        .nodes
        .iter()
        .find(|node| node.node_id == "b")
        .unwrap();
    let c = layout
        .nodes
        .iter()
        .find(|node| node.node_id == "c")
        .unwrap();

    assert_eq!(b.point.x - a.point.x, GRAPH_RANK_STEP);
    assert_eq!((b.point.y - c.point.y).abs(), GRAPH_ROW_STEP);
}

#[test]
fn rendered_shell_has_node_targeted_timeline_and_unfiltered_branches() {
    let html = render_snapshot_page(&snapshot());

    assert!(html.contains("data-node-target=\"detail-a\""));
    assert!(!html.contains("data-graph-x"));
    assert!(!html.contains("data-lane-y"));
    assert!(html.find(">main<").unwrap() < html.find(">draft<").unwrap());
}

async fn test_store() -> SqliteStore {
    SqliteStore::open_temporary()
        .await
        .expect("temporary SQLite store should open")
}

fn session_anchor() -> SessionAnchor {
    SessionAnchor {
        role: SessionRole::Orchestrator,
        provider_profile: None,
        provider: Some("openai".to_owned()),
        model: "test-model".to_owned(),
        tools: Vec::<Tool>::new(),
        system_prompt: "system".to_owned(),
        prompt: "prompt".to_owned(),
        temperature: None,
        max_tokens: None,
        additional_params: None,
        enable_coco_shim: false,
        active_skill: None,
    }
}

async fn append_session(store: &SqliteStore) -> String {
    store
        .append(NewNode {
            parent: store.root_id(),
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn shared_branch_head_is_one_node_with_aggregated_labels_and_typed_edges() {
    let store = test_store().await;
    let session = append_session(&store).await;
    store.fork("main", &session).await.unwrap();
    store.fork("draft", &session).await.unwrap();
    let main_child = store
        .append(NewNode {
            parent: session.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("main".to_owned()),
        })
        .await
        .unwrap();
    let draft_child = store
        .append(NewNode {
            parent: session.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("draft".to_owned()),
        })
        .await
        .unwrap();
    let shadow_parent = store
        .append(NewNode {
            parent: store.root_id(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("shadow".to_owned()),
        })
        .await
        .unwrap();
    let merged = store
        .append(NewNode {
            parent: main_child.clone(),
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::prompt(
                vec![
                    MergeParent::merge(draft_child.clone()),
                    MergeParent::shadow(shadow_parent.clone()),
                ],
                PromptAnchor {
                    prompt: "merge".to_owned(),
                    attachments: Vec::new(),
                },
            )),
        })
        .await
        .unwrap();
    store
        .set_branch_head("main", &session, &merged)
        .await
        .unwrap();
    store
        .set_branch_head("draft", &session, &merged)
        .await
        .unwrap();

    let snapshot = build_graph_snapshot(&store, 10).await.unwrap();
    let merged_nodes = snapshot
        .nodes
        .iter()
        .filter(|node| node.id == merged)
        .collect::<Vec<_>>();

    assert_eq!(merged_nodes.len(), 1);
    assert_eq!(
        merged_nodes[0].labels,
        vec!["draft".to_owned(), "main".to_owned()]
    );
    assert!(snapshot.edges.contains(&GraphEdge {
        source: session.clone(),
        target: main_child.clone(),
        kind: GraphEdgeKind::Primary,
    }));
    assert!(snapshot.edges.contains(&GraphEdge {
        source: session,
        target: draft_child.clone(),
        kind: GraphEdgeKind::Primary,
    }));
    assert!(snapshot.edges.contains(&GraphEdge {
        source: draft_child,
        target: merged.clone(),
        kind: GraphEdgeKind::Merge,
    }));
    assert!(snapshot.edges.contains(&GraphEdge {
        source: shadow_parent,
        target: merged,
        kind: GraphEdgeKind::Shadow,
    }));

    let layout = layout_graph(&snapshot);
    assert_eq!(layout.nodes.len(), snapshot.nodes.len());
    assert_eq!(layout.edges.len(), snapshot.edges.len());
}

#[tokio::test]
async fn anchor_mode_reconnects_primary_and_merge_edges_through_hidden_nodes() {
    let store = test_store().await;
    let session = append_session(&store).await;
    store.fork("main", &session).await.unwrap();
    store.fork("draft", &session).await.unwrap();
    let main_anchor = store
        .append(NewNode {
            parent: session.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Anchor(Anchor::prompt(
                Vec::new(),
                PromptAnchor {
                    prompt: "main anchor".to_owned(),
                    attachments: Vec::new(),
                },
            )),
        })
        .await
        .unwrap();
    let main_hidden = store
        .append(NewNode {
            parent: main_anchor.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("main hidden".to_owned()),
        })
        .await
        .unwrap();
    let draft_anchor = store
        .append(NewNode {
            parent: session.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Anchor(Anchor::prompt(
                Vec::new(),
                PromptAnchor {
                    prompt: "draft anchor".to_owned(),
                    attachments: Vec::new(),
                },
            )),
        })
        .await
        .unwrap();
    let draft_hidden = store
        .append(NewNode {
            parent: draft_anchor.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("draft hidden".to_owned()),
        })
        .await
        .unwrap();
    let merge_anchor = store
        .append(NewNode {
            parent: main_hidden.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Anchor(Anchor::prompt(
                vec![MergeParent::merge(draft_hidden.clone())],
                PromptAnchor {
                    prompt: "merge anchor".to_owned(),
                    attachments: Vec::new(),
                },
            )),
        })
        .await
        .unwrap();
    store
        .set_branch_head("main", &session, &merge_anchor)
        .await
        .unwrap();
    store
        .set_branch_head("draft", &session, &draft_hidden)
        .await
        .unwrap();

    let snapshot = build_graph_snapshot_with_mode(&store, 11, GraphMode::Anchors)
        .await
        .unwrap();
    let ids = snapshot
        .nodes
        .iter()
        .map(|node| node.id.as_str())
        .collect::<std::collections::BTreeSet<_>>();

    assert!(!ids.contains(main_hidden.as_str()));
    assert!(!ids.contains(draft_hidden.as_str()));
    assert!(snapshot.edges.contains(&GraphEdge {
        source: main_anchor,
        target: merge_anchor.clone(),
        kind: GraphEdgeKind::Primary,
    }));
    assert!(snapshot.edges.contains(&GraphEdge {
        source: draft_anchor.clone(),
        target: merge_anchor,
        kind: GraphEdgeKind::Merge,
    }));
    assert!(snapshot.branches.iter().any(|branch| {
        branch.name == "draft" && branch.visible_head_id.as_deref() == Some(draft_anchor.as_str())
    }));
}

#[tokio::test]
async fn branch_rewind_and_delete_remove_unreachable_nodes_and_move_labels() {
    let store = test_store().await;
    let session = append_session(&store).await;
    store.fork("main", &session).await.unwrap();
    store.fork("draft", &session).await.unwrap();
    let main_child = store
        .append(NewNode {
            parent: session.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("main".to_owned()),
        })
        .await
        .unwrap();
    let draft_child = store
        .append(NewNode {
            parent: session.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("draft".to_owned()),
        })
        .await
        .unwrap();
    store
        .set_branch_head("main", &session, &main_child)
        .await
        .unwrap();
    store
        .set_branch_head("draft", &session, &draft_child)
        .await
        .unwrap();

    let before = build_graph_snapshot(&store, 1).await.unwrap();
    assert!(before.nodes.iter().any(|node| node.id == draft_child));

    store
        .set_branch_head("draft", &draft_child, &session)
        .await
        .unwrap();
    let rewound = build_graph_snapshot(&store, 2).await.unwrap();
    assert!(!rewound.nodes.iter().any(|node| node.id == draft_child));
    assert!(
        rewound
            .nodes
            .iter()
            .any(|node| { node.id == session && node.labels.iter().any(|label| label == "draft") })
    );

    store.delete_branch("draft").await.unwrap();
    let deleted = build_graph_snapshot(&store, 3).await.unwrap();
    assert!(deleted.branches.iter().all(|branch| branch.name != "draft"));
    assert!(
        deleted
            .nodes
            .iter()
            .all(|node| node.labels.iter().all(|label| label != "draft"))
    );
}

#[tokio::test]
async fn skill_invocation_subtree_is_included_without_unrelated_children() {
    let store = test_store().await;
    let session = append_session(&store).await;
    store.fork("main", &session).await.unwrap();
    let tool_use = store
        .append(NewNode {
            parent: session.clone(),
            role: Role::LLM,
            metadata: None,
            kind: Kind::tool_use(coco_mem::ToolUse {
                id: "tool-1".to_owned(),
                name: "skill".to_owned(),
                input: json!({}),
            }),
        })
        .await
        .unwrap();
    store
        .set_branch_head("main", &session, &tool_use)
        .await
        .unwrap();
    let ignored_child = store
        .append(NewNode {
            parent: tool_use.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("unrelated".to_owned()),
        })
        .await
        .unwrap();
    let invocation = store
        .append(NewNode {
            parent: tool_use.clone(),
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::skill_invocation(
                Vec::new(),
                SkillInvocationAnchor {
                    skill_name: "test-skill".to_owned(),
                    mode: SkillInvocationMode::InheritContext,
                },
            )),
        })
        .await
        .unwrap();
    let invocation_child = store
        .append(NewNode {
            parent: invocation.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("delegated context".to_owned()),
        })
        .await
        .unwrap();

    let snapshot = build_graph_snapshot(&store, 9).await.unwrap();
    let ids = snapshot
        .nodes
        .iter()
        .map(|node| node.id.as_str())
        .collect::<std::collections::BTreeSet<_>>();

    assert!(ids.contains(tool_use.as_str()));
    assert!(ids.contains(invocation.as_str()));
    assert!(ids.contains(invocation_child.as_str()));
    assert!(!ids.contains(ignored_child.as_str()));
    assert!(snapshot.edges.contains(&GraphEdge {
        source: tool_use,
        target: invocation.clone(),
        kind: GraphEdgeKind::Primary,
    }));
    assert!(snapshot.edges.contains(&GraphEdge {
        source: invocation,
        target: invocation_child,
        kind: GraphEdgeKind::Primary,
    }));
}
