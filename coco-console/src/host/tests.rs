use std::collections::BTreeSet;

use coco_mem::{
    Anchor, BranchStore, Kind, MemoryStore, MergeParent, MessageQueueStore, NewNode, NodeStore,
    PromptAnchor, Role, SessionAnchor, SessionAnchorPatch, SessionRole, SessionState,
    SkillInvocationAnchor, SkillInvocationMode, SkillResultAnchor, Tool, ToolResult, ToolUse,
};
use serde_json::json;

use crate::api::{GraphViewportItemKind, Point};
use crate::graph::{
    GraphBranch, GraphEdge, GraphEdgeKind, GraphMode, GraphNode, GraphSnapshot,
    build_graph_snapshot, build_graph_snapshot_with_mode, node_target_id,
};
use crate::host::api::{GraphViewportDiffRequest, GraphViewportKnownItems, GraphViewportRequest};
use crate::layout::{
    EDGE_TARGET_PORT_STEP, GRAPH_COLUMN_WIDTH, GRAPH_LANE_HEIGHT, GRAPH_LEFT_X, GRAPH_TOP_Y,
    GraphLayoutEdgeKind, layout_graph, layout_graph_viewport, layout_graph_viewport_diff,
    routed_elbow_points,
};
use crate::render::{
    render_fragment, render_index_page, render_node_detail_fragment,
    render_provider_context_fragment, render_snapshot_page,
};
use crate::{ConsolePublisher, ConsoleStore};

fn session_anchor() -> SessionAnchor {
    SessionAnchor {
        role: SessionRole::Orchestrator,
        provider_profile: None,
        provider: Some("openai".to_owned()),
        model: "gpt-4.1-mini".to_owned(),
        tools: Vec::<Tool>::new(),
        system_prompt: "You are helpful.".to_owned(),
        prompt: "Start".to_owned(),
        temperature: None,
        max_tokens: None,
        additional_params: None,
        enable_coco_shim: false,
        active_skill: None,
    }
}

fn provider_context_target(context_start: &str, branch: &str) -> String {
    format!(
        "{}-context-{}",
        node_target_id(context_start),
        branch
            .bytes()
            .flat_map(|byte| {
                const HEX: &[u8; 16] = b"0123456789abcdef";
                [
                    HEX[(byte >> 4) as usize] as char,
                    HEX[(byte & 0x0f) as usize] as char,
                ]
            })
            .collect::<String>()
    )
}

fn graph_node(id: &str, created_at_ns: i128) -> GraphNode {
    GraphNode {
        id: id.to_owned(),
        short_id: id.to_owned(),
        kind: "Text".to_owned(),
        role: "User".to_owned(),
        created_at: created_at_ns.to_string(),
        created_at_ns,
        content: String::new(),
        summary: String::new(),
        labels: Vec::new(),
        provider_context_ids: Vec::new(),
    }
}

fn two_node_snapshot(version: u64) -> GraphSnapshot {
    GraphSnapshot {
        version,
        mode: GraphMode::All,
        root_id: "root".to_owned(),
        nodes: vec![graph_node("base", 0), graph_node("merged", 1)],
        edges: vec![GraphEdge {
            source: "base".to_owned(),
            target: "merged".to_owned(),
            kind: GraphEdgeKind::Primary,
        }],
        branches: vec![GraphBranch {
            name: "main".to_owned(),
            head_id: "merged".to_owned(),
            visible_head_id: Some("merged".to_owned()),
            state: SessionState::Active,
        }],
        provider_contexts: Vec::new(),
    }
}

fn empty_snapshot(version: u64) -> GraphSnapshot {
    GraphSnapshot {
        version,
        mode: GraphMode::All,
        root_id: "root".to_owned(),
        nodes: Vec::new(),
        edges: Vec::new(),
        branches: Vec::new(),
        provider_contexts: Vec::new(),
    }
}

fn linear_snapshot(version: u64, node_ids: &[&str]) -> GraphSnapshot {
    let nodes = node_ids
        .iter()
        .enumerate()
        .map(|(index, node_id)| graph_node(node_id, index as i128))
        .collect::<Vec<_>>();
    let edges = node_ids
        .windows(2)
        .map(|window| GraphEdge {
            source: window[0].to_owned(),
            target: window[1].to_owned(),
            kind: GraphEdgeKind::Primary,
        })
        .collect::<Vec<_>>();
    let head_id = node_ids
        .last()
        .expect("linear snapshot should have at least one node")
        .to_string();

    GraphSnapshot {
        version,
        mode: GraphMode::All,
        root_id: "root".to_owned(),
        nodes,
        edges,
        branches: vec![GraphBranch {
            name: "main".to_owned(),
            head_id,
            visible_head_id: node_ids.last().map(|node_id| (*node_id).to_owned()),
            state: SessionState::Active,
        }],
        provider_contexts: Vec::new(),
    }
}

fn viewport_rendered_node_keys(
    snapshot: &GraphSnapshot,
    request: GraphViewportRequest,
) -> BTreeSet<String> {
    layout_graph_viewport(snapshot, request)
        .nodes
        .into_iter()
        .map(|node| node.key)
        .collect()
}

fn strict_viewport_node_keys(
    snapshot: &GraphSnapshot,
    request: GraphViewportRequest,
) -> BTreeSet<String> {
    viewport_rendered_node_keys(
        snapshot,
        GraphViewportRequest {
            overscan: 0,
            ..request
        },
    )
}

fn apply_diff_node_keys(
    mut rendered: BTreeSet<String>,
    snapshot: &GraphSnapshot,
    previous: GraphViewportRequest,
    current: GraphViewportRequest,
) -> BTreeSet<String> {
    let diff = layout_graph_viewport_diff(
        snapshot,
        GraphViewportDiffRequest {
            previous,
            current,
            known: None,
        },
    );
    for item in diff.removed {
        if item.kind == GraphViewportItemKind::Node {
            rendered.remove(&item.key);
        }
    }
    rendered.extend(diff.added.nodes.into_iter().map(|node| node.key));
    rendered.extend(diff.updated.nodes.into_iter().map(|node| node.key));
    rendered
}

#[test]
fn graph_snapshot_contains_primary_and_merge_edges() {
    let store = MemoryStore::new();
    let root = store.root_id();
    let left = store
        .append(NewNode {
            parent: root.clone(),
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
        })
        .unwrap();
    store.fork("main", &left).unwrap();
    let right = store
        .append(NewNode {
            parent: root,
            role: Role::User,
            metadata: None,
            kind: Kind::Text("feedback".to_owned()),
        })
        .unwrap();
    let merged = store
        .append(NewNode {
            parent: left.clone(),
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::session(
                vec![MergeParent::merge(right.clone())],
                session_anchor(),
            )),
        })
        .unwrap();
    store.set_branch_head("main", &left, &merged).unwrap();
    store.fork("draft", &left).unwrap();
    let draft = store
        .append(NewNode {
            parent: left.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("draft work".to_owned()),
        })
        .unwrap();
    store.set_branch_head("draft", &left, &draft).unwrap();

    let snapshot = build_graph_snapshot(&store, 7).unwrap();

    assert_eq!(snapshot.version, 7);
    assert_eq!(snapshot.nodes.len(), 4);
    assert!(snapshot.edges.contains(&GraphEdge {
        source: left.clone(),
        target: merged.clone(),
        kind: GraphEdgeKind::Primary,
    }));
    assert!(snapshot.edges.contains(&GraphEdge {
        source: left.clone(),
        target: draft,
        kind: GraphEdgeKind::Primary,
    }));
    assert!(snapshot.edges.contains(&GraphEdge {
        source: right,
        target: merged,
        kind: GraphEdgeKind::Merge,
    }));

    let layout = layout_graph(&snapshot);
    assert!(layout.lanes.iter().any(|lane| lane.label == "main"));
    assert!(
        layout
            .lanes
            .iter()
            .any(|lane| lane.label.starts_with("orphan "))
    );
    assert!(layout.primary_edges.iter().all(|edge| {
        let gap = edge.target.x - edge.source.x;
        edge.kind == GraphLayoutEdgeKind::PrimaryParent
            && gap >= GRAPH_COLUMN_WIDTH
            && gap % GRAPH_COLUMN_WIDTH == 0
    }));
    assert!(
        layout
            .fork_edges
            .iter()
            .any(|edge| edge.kind == GraphLayoutEdgeKind::Fork && edge.target.x > edge.source.x)
    );
    assert!(
        layout
            .merge_edges
            .iter()
            .all(|edge| edge.target.x > edge.source.x)
    );
    let merge_target = layout.merge_edges.first().unwrap().target;
    assert!(
        layout
            .primary_edges
            .iter()
            .any(|edge| edge.target == merge_target && edge.target_port_offset == 0.0)
    );
    assert!(layout.merge_edges.iter().any(
        |edge| edge.target == merge_target && edge.target_port_offset == EDGE_TARGET_PORT_STEP
    ));

    let html = render_snapshot_page(&snapshot);
    assert!(html.contains("class=\"graph-wrap virtual-graph\""));
    assert!(html.contains("class=\"follow-toggle\""));
    assert!(html.contains("Keep the graph pinned to the top-right edge"));
    assert!(html.contains("class=\"graph-lanes\""));
    assert!(html.contains("class=\"graph-edges\""));
    assert!(html.contains("class=\"graph-nodes\""));
    assert!(html.contains("id=\"selection-style\""));
    assert!(!html.contains("stroke: #facc15"));
    assert!(html.contains("class=\"time-scale\""));
    assert!(html.contains("Graph time navigator"));
    assert!(html.contains("class=\"time-scale-tick\""));
    assert!(html.contains("class=\"time-scale-cursor\""));
    assert!(html.contains("data-graph-x="));
    assert!(!html.contains("class=\"viewport-map\""));
    assert!(!html.contains("class=\"minimap-node\""));
    assert!(!html.contains("class=\"minimap-edge\""));
    assert!(!html.contains("/?node="));
}

#[test]
fn empty_snapshot_page_still_renders_virtual_graph_shell() {
    let html = render_snapshot_page(&empty_snapshot(30));

    assert!(html.contains("class=\"graph-wrap virtual-graph\""));
    assert!(html.contains("class=\"graph-lanes\""));
    assert!(html.contains("class=\"time-scale time-scale-empty\""));
    assert!(html.contains("No time data"));
    assert!(html.contains("Loading graph..."));
}

#[test]
fn time_scale_tick_positions_are_evenly_spaced() {
    let html = render_snapshot_page(&GraphSnapshot {
        version: 31,
        mode: GraphMode::All,
        root_id: "root".to_owned(),
        nodes: vec![
            graph_node("first", 0),
            graph_node("near", 10),
            graph_node("far", 100),
        ],
        edges: vec![
            GraphEdge {
                source: "first".to_owned(),
                target: "near".to_owned(),
                kind: GraphEdgeKind::Primary,
            },
            GraphEdge {
                source: "near".to_owned(),
                target: "far".to_owned(),
                kind: GraphEdgeKind::Primary,
            },
        ],
        branches: vec![GraphBranch {
            name: "main".to_owned(),
            head_id: "far".to_owned(),
            visible_head_id: Some("far".to_owned()),
            state: SessionState::Active,
        }],
        provider_contexts: Vec::new(),
    });

    assert!(html.contains("data-position=\"0.000000\""));
    assert!(html.contains("data-position=\"50.000000\""));
    assert!(html.contains("data-position=\"100.000000\""));
    assert!(!html.contains("data-position=\"10.000000\""));
}

#[test]
fn snapshot_page_defers_node_detail_content_until_requested() {
    let mut snapshot = two_node_snapshot(42);
    snapshot.nodes[0].content = "Deferred detail payload".to_owned();

    let html = render_snapshot_page(&snapshot);

    assert!(html.contains("class=\"node-detail-slot\""));
    assert!(html.contains("class=\"provider-context-slot\""));
    assert!(!html.contains("Deferred detail payload"));
    assert!(!html.contains("class=\"node-details node-detail\""));

    let detail = render_node_detail_fragment(&snapshot, Some("detail-base"));

    assert!(detail.contains("class=\"node-details node-detail\""));
    assert!(detail.contains("Deferred detail payload"));
    assert!(!detail.contains("Provider Context"));
}

#[test]
fn node_detail_fragment_renders_default_or_missing_selection() {
    let snapshot = empty_snapshot(0);

    let default_detail = render_node_detail_fragment(&snapshot, None);
    let missing_detail = render_node_detail_fragment(&snapshot, Some("detail-missing"));

    assert!(default_detail.contains("Select a node to inspect its content."));
    assert!(missing_detail.contains("The selected node is no longer available."));
    assert!(missing_detail.contains("detail-missing"));
}

#[test]
fn provider_context_fragment_renders_default_or_missing_selection() {
    let snapshot = empty_snapshot(0);

    let default_context = render_provider_context_fragment(&snapshot, None, None);
    let missing_context = render_provider_context_fragment(&snapshot, Some("detail-missing"), None);

    assert!(default_context.contains("Select a node to inspect its provider context."));
    assert!(missing_context.contains("The selected node is no longer available."));
    assert!(missing_context.contains("detail-missing"));
}

#[test]
fn graph_snapshot_contains_shadow_parent_edges() {
    let store = MemoryStore::new();
    let root = store.root_id();
    let session = store
        .append(NewNode {
            parent: root.clone(),
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
        })
        .unwrap();
    store.fork("main", &session).unwrap();
    let shadow_parent = store
        .append(NewNode {
            parent: root,
            role: Role::User,
            metadata: None,
            kind: Kind::Text("shadow".to_owned()),
        })
        .unwrap();
    let prompt = store
        .append(NewNode {
            parent: session.clone(),
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::prompt(
                vec![MergeParent::shadow(shadow_parent.clone())],
                coco_mem::PromptAnchor {
                    prompt: String::new(),
                    attachments: vec![],
                },
            )),
        })
        .unwrap();
    store.set_branch_head("main", &session, &prompt).unwrap();

    let snapshot = build_graph_snapshot(&store, 8).unwrap();

    assert!(snapshot.edges.contains(&GraphEdge {
        source: shadow_parent,
        target: prompt,
        kind: GraphEdgeKind::Shadow,
    }));
}

#[test]
fn graph_snapshot_anchor_mode_reconnects_edges_through_hidden_nodes() {
    let store = MemoryStore::new();
    let root = store.root_id();
    let session = store
        .append(NewNode {
            parent: root,
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
        })
        .unwrap();
    store.fork("main", &session).unwrap();
    store.fork("draft", &session).unwrap();

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
        .unwrap();
    let main_hidden = store
        .append(NewNode {
            parent: main_anchor.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("main hidden".to_owned()),
        })
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
        .unwrap();
    let draft_hidden = store
        .append(NewNode {
            parent: draft_anchor.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("draft hidden".to_owned()),
        })
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
        .unwrap();
    store
        .set_branch_head("main", &session, &merge_anchor)
        .unwrap();
    store
        .set_branch_head("draft", &session, &draft_hidden)
        .unwrap();

    let snapshot = build_graph_snapshot_with_mode(&store, 11, GraphMode::Anchors).unwrap();
    let node_ids = snapshot
        .nodes
        .iter()
        .map(|node| node.id.as_str())
        .collect::<Vec<_>>();

    assert_eq!(snapshot.mode, GraphMode::Anchors);
    assert!(snapshot.nodes.iter().all(|node| node.kind != "text"));
    assert!(!node_ids.contains(&main_hidden.as_str()));
    assert!(!node_ids.contains(&draft_hidden.as_str()));
    assert!(snapshot.edges.contains(&GraphEdge {
        source: main_anchor.clone(),
        target: merge_anchor.clone(),
        kind: GraphEdgeKind::Primary,
    }));
    assert!(snapshot.edges.contains(&GraphEdge {
        source: draft_anchor.clone(),
        target: merge_anchor.clone(),
        kind: GraphEdgeKind::Merge,
    }));
    assert!(snapshot.branches.iter().any(|branch| branch.name == "draft"
        && branch.head_id == draft_hidden
        && branch.visible_head_id.as_deref() == Some(draft_anchor.as_str())));
    assert!(
        snapshot
            .nodes
            .iter()
            .find(|node| node.id == merge_anchor)
            .expect("merge anchor should be visible")
            .labels
            .contains(&"main".to_owned())
    );
}

#[test]
fn graph_snapshot_includes_skill_invocation_subtree_after_tool_use() {
    let store = MemoryStore::new();
    let root = store.root_id();
    let session = store
        .append(NewNode {
            parent: root,
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
        })
        .unwrap();
    store.fork("main", &session).unwrap();
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
        .unwrap();
    store.set_branch_head("main", &session, &tool_use).unwrap();
    let ignored_child = store
        .append(NewNode {
            parent: tool_use.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("not a skill subtree".to_owned()),
        })
        .unwrap();
    let invocation = store
        .append(NewNode {
            parent: tool_use.clone(),
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::skill_invocation(
                Vec::new(),
                SkillInvocationAnchor {
                    skill_name: "fast-rust".to_owned(),
                    mode: SkillInvocationMode::InheritContext,
                },
            )),
        })
        .unwrap();
    let invocation_child = store
        .append(NewNode {
            parent: invocation.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("delegated context".to_owned()),
        })
        .unwrap();

    let snapshot = build_graph_snapshot(&store, 9).unwrap();
    let node_ids = snapshot
        .nodes
        .iter()
        .map(|node| node.id.as_str())
        .collect::<Vec<_>>();

    assert!(node_ids.contains(&tool_use.as_str()));
    assert!(node_ids.contains(&invocation.as_str()));
    assert!(node_ids.contains(&invocation_child.as_str()));
    assert!(!node_ids.contains(&ignored_child.as_str()));
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

#[test]
fn node_details_include_nodes_from_same_provider_context() {
    let store = MemoryStore::new();
    let root = store.root_id();
    let first_session = store
        .append(NewNode {
            parent: root,
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
        })
        .unwrap();
    store.fork("main", &first_session).unwrap();
    let previous_text = store
        .append(NewNode {
            parent: first_session.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("previous provider context".to_owned()),
        })
        .unwrap();
    let next_session = store
        .append(NewNode {
            parent: previous_text,
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
        })
        .unwrap();
    let hidden_text = store
        .append(NewNode {
            parent: next_session.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("hidden node inside current provider context".to_owned()),
        })
        .unwrap();
    let prompt = store
        .append(NewNode {
            parent: hidden_text.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Anchor(Anchor::prompt(
                Vec::new(),
                PromptAnchor {
                    prompt: "visible prompt in current provider context".to_owned(),
                    attachments: Vec::new(),
                },
            )),
        })
        .unwrap();
    store
        .set_branch_head("main", &first_session, &prompt)
        .unwrap();

    let snapshot = build_graph_snapshot_with_mode(&store, 32, GraphMode::Anchors).unwrap();
    let context = snapshot
        .provider_contexts
        .iter()
        .find(|context| context.id == provider_context_target(&next_session, "main"))
        .expect("provider context should exist");
    let context_ids = context
        .nodes
        .iter()
        .map(|node| node.id.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        context_ids,
        vec![prompt.as_str(), hidden_text.as_str(), next_session.as_str()]
    );
    assert!(
        context
            .nodes
            .iter()
            .any(|node| node.id == hidden_text && !node.visible)
    );
    assert!(!snapshot.nodes.iter().any(|node| node.id == hidden_text));

    let provider_context = render_provider_context_fragment(
        &snapshot,
        Some(&node_target_id(&next_session)),
        Some(&context.id),
    );

    assert!(provider_context.contains("Provider Context"));
    assert!(provider_context.contains("hidden node inside current provider context"));
    assert!(provider_context.contains("class=\"provider-context-node-link\""));
    assert!(provider_context.contains(&format!(
        "#{}?context={}",
        node_target_id(&hidden_text),
        context.id
    )));
    assert!(provider_context.contains("class=\"provider-context-node-graph-point\""));
    assert!(provider_context.contains("data-node-x="));
    assert!(provider_context.contains("data-node-y="));
    assert!(provider_context.contains("class=\"provider-context-node\""));
    assert!(provider_context.contains("class=\"provider-context-node visible selected\""));

    let hidden_detail = render_node_detail_fragment(&snapshot, Some(&node_target_id(&hidden_text)));
    let hidden_context = render_provider_context_fragment(
        &snapshot,
        Some(&node_target_id(&hidden_text)),
        Some(&context.id),
    );

    assert!(hidden_detail.contains("class=\"node-details node-detail\""));
    assert!(hidden_detail.contains(&hidden_text));
    assert!(hidden_detail.contains("Kind"));
    assert!(hidden_detail.contains("text"));
    assert!(hidden_detail.contains("hidden node inside current provider context"));
    assert!(hidden_context.contains("class=\"provider-context-node selected\""));
}

#[test]
fn provider_context_list_uses_one_head_to_context_start_path() {
    let store = MemoryStore::new();
    let root = store.root_id();
    let session = store
        .append(NewNode {
            parent: root,
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
        })
        .unwrap();
    store.fork("main", &session).unwrap();
    let shared_hidden = store
        .append(NewNode {
            parent: session.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("shared hidden context".to_owned()),
        })
        .unwrap();
    let shared_prompt = store
        .append(NewNode {
            parent: shared_hidden.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Anchor(Anchor::prompt(
                Vec::new(),
                PromptAnchor {
                    prompt: "shared prompt".to_owned(),
                    attachments: Vec::new(),
                },
            )),
        })
        .unwrap();
    let main_hidden = store
        .append(NewNode {
            parent: shared_prompt.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("main hidden context".to_owned()),
        })
        .unwrap();
    let main_prompt = store
        .append(NewNode {
            parent: main_hidden.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Anchor(Anchor::prompt(
                Vec::new(),
                PromptAnchor {
                    prompt: "main head prompt".to_owned(),
                    attachments: Vec::new(),
                },
            )),
        })
        .unwrap();
    store
        .set_branch_head("main", &session, &main_prompt)
        .unwrap();

    store.fork("review", &shared_prompt).unwrap();
    let review_hidden = store
        .append(NewNode {
            parent: shared_prompt.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("review hidden context".to_owned()),
        })
        .unwrap();
    let review_prompt = store
        .append(NewNode {
            parent: review_hidden.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Anchor(Anchor::prompt(
                Vec::new(),
                PromptAnchor {
                    prompt: "review head prompt".to_owned(),
                    attachments: Vec::new(),
                },
            )),
        })
        .unwrap();
    store
        .set_branch_head("review", &shared_prompt, &review_prompt)
        .unwrap();

    let snapshot = build_graph_snapshot_with_mode(&store, 33, GraphMode::Anchors).unwrap();
    let review_context = snapshot
        .provider_contexts
        .iter()
        .find(|context| context.id == provider_context_target(&session, "review"))
        .expect("review context should exist");
    let review_context_ids = review_context
        .nodes
        .iter()
        .map(|node| node.id.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        review_context_ids,
        vec![
            review_prompt.as_str(),
            review_hidden.as_str(),
            shared_prompt.as_str(),
            shared_hidden.as_str(),
            session.as_str()
        ]
    );

    let review_context_html = render_provider_context_fragment(
        &snapshot,
        Some(&node_target_id(&review_hidden)),
        Some(&review_context.id),
    );

    assert!(review_context_html.contains("review hidden context"));
    assert!(review_context_html.contains("shared hidden context"));
    assert!(!review_context_html.contains("main hidden context"));
    assert!(review_context_html.contains("class=\"provider-context-node selected\""));
    assert!(review_context_html.contains(&format!(
        "#{}?context={}",
        node_target_id(&shared_hidden),
        review_context.id
    )));

    let shared_hidden_context_from_review = render_provider_context_fragment(
        &snapshot,
        Some(&node_target_id(&shared_hidden)),
        Some(&review_context.id),
    );
    assert!(shared_hidden_context_from_review.contains("review hidden context"));
    assert!(!shared_hidden_context_from_review.contains("main hidden context"));
}

#[test]
fn provider_context_id_stays_stable_when_branch_head_moves() {
    let store = MemoryStore::new();
    let root = store.root_id();
    let session = store
        .append(NewNode {
            parent: root,
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
        })
        .unwrap();
    store.fork("main", &session).unwrap();
    let first_prompt = store
        .append(NewNode {
            parent: session.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Anchor(Anchor::prompt(
                Vec::new(),
                PromptAnchor {
                    prompt: "first prompt".to_owned(),
                    attachments: Vec::new(),
                },
            )),
        })
        .unwrap();
    store
        .set_branch_head("main", &session, &first_prompt)
        .unwrap();

    let first_snapshot = build_graph_snapshot_with_mode(&store, 35, GraphMode::Anchors).unwrap();
    let first_context = first_snapshot
        .provider_contexts
        .iter()
        .find(|context| context.nodes.iter().any(|node| node.id == first_prompt))
        .expect("initial provider context should exist");
    let first_context_id = first_context.id.clone();

    let next_prompt = store
        .append(NewNode {
            parent: first_prompt.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Anchor(Anchor::prompt(
                Vec::new(),
                PromptAnchor {
                    prompt: "next prompt".to_owned(),
                    attachments: Vec::new(),
                },
            )),
        })
        .unwrap();
    store
        .set_branch_head("main", &first_prompt, &next_prompt)
        .unwrap();

    let next_snapshot = build_graph_snapshot_with_mode(&store, 36, GraphMode::Anchors).unwrap();
    let next_context = next_snapshot
        .provider_contexts
        .iter()
        .find(|context| context.nodes.iter().any(|node| node.id == first_prompt))
        .expect("updated provider context should exist");

    assert_eq!(first_context_id, provider_context_target(&session, "main"));
    assert_eq!(next_context.id, first_context_id);
    assert!(next_context.nodes.iter().any(|node| node.id == next_prompt));
}

#[test]
fn provider_context_ids_preserve_unique_branch_names() {
    let store = MemoryStore::new();
    let root = store.root_id();
    let session = store
        .append(NewNode {
            parent: root,
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
        })
        .unwrap();

    store.fork("draft/review", &session).unwrap();
    let slash_hidden = store
        .append(NewNode {
            parent: session.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("slash branch context".to_owned()),
        })
        .unwrap();
    let slash_prompt = store
        .append(NewNode {
            parent: slash_hidden.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Anchor(Anchor::prompt(
                Vec::new(),
                PromptAnchor {
                    prompt: "slash branch prompt".to_owned(),
                    attachments: Vec::new(),
                },
            )),
        })
        .unwrap();
    store
        .set_branch_head("draft/review", &session, &slash_prompt)
        .unwrap();

    store.fork("draft-review", &session).unwrap();
    let dash_hidden = store
        .append(NewNode {
            parent: session.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("dash branch context".to_owned()),
        })
        .unwrap();
    let dash_prompt = store
        .append(NewNode {
            parent: dash_hidden.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Anchor(Anchor::prompt(
                Vec::new(),
                PromptAnchor {
                    prompt: "dash branch prompt".to_owned(),
                    attachments: Vec::new(),
                },
            )),
        })
        .unwrap();
    store
        .set_branch_head("draft-review", &session, &dash_prompt)
        .unwrap();

    let snapshot = build_graph_snapshot_with_mode(&store, 37, GraphMode::Anchors).unwrap();
    let slash_context_id = provider_context_target(&session, "draft/review");
    let dash_context_id = provider_context_target(&session, "draft-review");
    let slash_context = snapshot
        .provider_contexts
        .iter()
        .find(|context| context.id == slash_context_id)
        .expect("slash branch provider context should exist");
    let dash_context = snapshot
        .provider_contexts
        .iter()
        .find(|context| context.id == dash_context_id)
        .expect("dash branch provider context should exist");

    assert_ne!(slash_context.id, dash_context.id);
    assert!(
        slash_context
            .nodes
            .iter()
            .any(|node| node.id == slash_hidden)
    );
    assert!(
        !slash_context
            .nodes
            .iter()
            .any(|node| node.id == dash_hidden)
    );
    assert!(dash_context.nodes.iter().any(|node| node.id == dash_hidden));
    assert!(
        !dash_context
            .nodes
            .iter()
            .any(|node| node.id == slash_hidden)
    );
}

#[test]
fn all_mode_provider_contexts_cover_older_visible_segments() {
    let store = MemoryStore::new();
    let root = store.root_id();
    let first_session = store
        .append(NewNode {
            parent: root,
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
        })
        .unwrap();
    store.fork("main", &first_session).unwrap();
    let old_hidden = store
        .append(NewNode {
            parent: first_session.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("old hidden context".to_owned()),
        })
        .unwrap();
    let old_prompt = store
        .append(NewNode {
            parent: old_hidden.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Anchor(Anchor::prompt(
                Vec::new(),
                PromptAnchor {
                    prompt: "old prompt".to_owned(),
                    attachments: Vec::new(),
                },
            )),
        })
        .unwrap();
    let next_session = store
        .append(NewNode {
            parent: old_prompt.clone(),
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
        })
        .unwrap();
    let new_prompt = store
        .append(NewNode {
            parent: next_session.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Anchor(Anchor::prompt(
                Vec::new(),
                PromptAnchor {
                    prompt: "new prompt".to_owned(),
                    attachments: Vec::new(),
                },
            )),
        })
        .unwrap();
    store
        .set_branch_head("main", &first_session, &new_prompt)
        .unwrap();

    let snapshot = build_graph_snapshot_with_mode(&store, 34, GraphMode::All).unwrap();
    let old_context = snapshot
        .provider_contexts
        .iter()
        .find(|context| context.id == provider_context_target(&first_session, "main"))
        .expect("old provider context should be retained in all mode");
    let old_context_ids = old_context
        .nodes
        .iter()
        .map(|node| node.id.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        old_context_ids,
        vec![
            old_prompt.as_str(),
            old_hidden.as_str(),
            first_session.as_str()
        ]
    );

    let old_hidden_context = render_provider_context_fragment(
        &snapshot,
        Some(&node_target_id(&old_hidden)),
        Some(&old_context.id),
    );

    assert!(old_hidden_context.contains("old hidden context"));
    assert!(old_hidden_context.contains("old prompt"));
    assert!(!old_hidden_context.contains("new prompt"));
    assert!(old_hidden_context.contains("class=\"provider-context-node visible selected\""));
}

fn snapshot_content<'a>(snapshot: &'a GraphSnapshot, node_id: &str) -> &'a str {
    snapshot
        .nodes
        .iter()
        .find(|node| node.id == node_id)
        .map(|node| node.content.as_str())
        .expect("node should be visible")
}

#[test]
fn graph_snapshot_renders_content_for_visible_node_kinds() {
    let store = MemoryStore::new();
    let root = store.root_id();
    let mut empty_prompt_session_anchor = session_anchor();
    empty_prompt_session_anchor.prompt.clear();
    empty_prompt_session_anchor.system_prompt = "system fallback".to_owned();
    let empty_prompt_session = store
        .append(NewNode {
            parent: root,
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::session(Vec::new(), empty_prompt_session_anchor)),
        })
        .unwrap();
    store.fork("main", &empty_prompt_session).unwrap();

    let mut prompted_session_anchor = session_anchor();
    prompted_session_anchor.prompt = "session prompt".to_owned();
    let prompted_session = store
        .append(NewNode {
            parent: empty_prompt_session.clone(),
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::session(Vec::new(), prompted_session_anchor)),
        })
        .unwrap();

    let session_patch = SessionAnchorPatch {
        model: Some("gpt-5-mini".to_owned()),
        ..SessionAnchorPatch::default()
    };
    let patch = store
        .append(NewNode {
            parent: prompted_session.clone(),
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::session_patch(Vec::new(), session_patch.clone())),
        })
        .unwrap();
    let prompt = store
        .append(NewNode {
            parent: patch.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Anchor(Anchor::prompt(
                Vec::new(),
                PromptAnchor {
                    prompt: "prompt anchor".to_owned(),
                    attachments: Vec::new(),
                },
            )),
        })
        .unwrap();
    let invocation = store
        .append(NewNode {
            parent: prompt.clone(),
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::skill_invocation(
                Vec::new(),
                SkillInvocationAnchor {
                    skill_name: "rust-skill".to_owned(),
                    mode: SkillInvocationMode::InheritContext,
                },
            )),
        })
        .unwrap();
    let skill_result = store
        .append(NewNode {
            parent: invocation.clone(),
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::skill_result(
                Vec::new(),
                SkillResultAnchor {
                    skill_name: "rust-skill".to_owned(),
                    output: "skill output".to_owned(),
                },
            )),
        })
        .unwrap();
    let tool_use = store
        .append(NewNode {
            parent: skill_result.clone(),
            role: Role::LLM,
            metadata: None,
            kind: Kind::tool_use(ToolUse {
                id: "tool-1".to_owned(),
                name: "shell".to_owned(),
                input: json!({"cmd": "cargo test"}),
            }),
        })
        .unwrap();
    let empty_tool_use = store
        .append(NewNode {
            parent: tool_use.clone(),
            role: Role::LLM,
            metadata: None,
            kind: Kind::tool_use_items(Vec::new()),
        })
        .unwrap();
    let tool_result = store
        .append(NewNode {
            parent: empty_tool_use.clone(),
            role: Role::System,
            metadata: None,
            kind: Kind::tool_result(ToolResult {
                id: "tool-1".to_owned(),
                output: "tool output".to_owned(),
            }),
        })
        .unwrap();
    let empty_tool_result = store
        .append(NewNode {
            parent: tool_result.clone(),
            role: Role::System,
            metadata: None,
            kind: Kind::tool_result_items(Vec::new()),
        })
        .unwrap();
    let text = store
        .append(NewNode {
            parent: empty_tool_result.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("plain text".to_owned()),
        })
        .unwrap();
    let failure = store
        .append(NewNode {
            parent: text.clone(),
            role: Role::System,
            metadata: None,
            kind: Kind::Failure("failure message".to_owned()),
        })
        .unwrap();
    store
        .set_branch_head("main", &empty_prompt_session, &failure)
        .unwrap();

    let snapshot = build_graph_snapshot(&store, 10).unwrap();

    assert_eq!(
        snapshot_content(&snapshot, &empty_prompt_session),
        "system fallback"
    );
    assert_eq!(
        snapshot_content(&snapshot, &prompted_session),
        "session prompt"
    );
    assert_eq!(
        snapshot_content(&snapshot, &patch),
        serde_json::to_string(&session_patch).unwrap()
    );
    assert_eq!(snapshot_content(&snapshot, &prompt), "prompt anchor");
    assert_eq!(snapshot_content(&snapshot, &invocation), "rust-skill");
    assert_eq!(snapshot_content(&snapshot, &skill_result), "skill output");
    assert_eq!(
        snapshot_content(&snapshot, &tool_use),
        json!({"cmd": "cargo test"}).to_string()
    );
    assert_eq!(snapshot_content(&snapshot, &empty_tool_use), "");
    assert_eq!(snapshot_content(&snapshot, &tool_result), "tool output");
    assert_eq!(snapshot_content(&snapshot, &empty_tool_result), "");
    assert_eq!(snapshot_content(&snapshot, &text), "plain text");
    assert_eq!(snapshot_content(&snapshot, &failure), "failure message");
}

#[test]
fn layout_expands_empty_columns_from_event_order() {
    let snapshot = GraphSnapshot {
        version: 1,
        mode: GraphMode::All,
        root_id: "root".to_owned(),
        nodes: vec![
            graph_node("base", 0),
            graph_node("side-a", 1),
            graph_node("side-b", 2),
            graph_node("merged", 3),
        ],
        edges: vec![GraphEdge {
            source: "base".to_owned(),
            target: "merged".to_owned(),
            kind: GraphEdgeKind::Primary,
        }],
        branches: vec![GraphBranch {
            name: "main".to_owned(),
            head_id: "merged".to_owned(),
            visible_head_id: Some("merged".to_owned()),
            state: SessionState::Active,
        }],
        provider_contexts: Vec::new(),
    };

    let layout = layout_graph(&snapshot);
    let primary_edge = layout.primary_edges.first().unwrap();

    assert_eq!(
        primary_edge.target.x - primary_edge.source.x,
        3 * GRAPH_COLUMN_WIDTH
    );
}

#[test]
fn graph_viewport_response_includes_canvas_and_visible_nodes() {
    let snapshot = GraphSnapshot {
        version: 11,
        mode: GraphMode::All,
        root_id: "root".to_owned(),
        nodes: vec![
            graph_node("base", 0),
            graph_node("side-a", 1),
            graph_node("side-b", 2),
            graph_node("merged", 3),
        ],
        edges: vec![GraphEdge {
            source: "base".to_owned(),
            target: "merged".to_owned(),
            kind: GraphEdgeKind::Primary,
        }],
        branches: vec![GraphBranch {
            name: "main".to_owned(),
            head_id: "merged".to_owned(),
            visible_head_id: Some("merged".to_owned()),
            state: SessionState::Active,
        }],
        provider_contexts: Vec::new(),
    };

    let response = layout_graph_viewport(
        &snapshot,
        GraphViewportRequest {
            x: 0,
            y: 0,
            width: GRAPH_LEFT_X + 80,
            height: GRAPH_TOP_Y + 80,
            overscan: 0,
        },
    );

    assert_eq!(response.version, 11);
    assert!(response.canvas.width > response.viewport.width);
    assert!(response.canvas.height >= response.viewport.height);
    assert_eq!(response.viewport.overscan, 0);
    assert!(response.lanes.iter().any(|lane| lane.label == "main"));
    assert!(
        response
            .nodes
            .iter()
            .any(|node| node.key == "node:base:120:90")
    );
    assert!(!response.nodes.iter().any(|node| node.id == "merged"));
}

#[test]
fn graph_viewport_response_uses_stable_keys_for_patchable_items() {
    let snapshot = two_node_snapshot(12);

    let response = layout_graph_viewport(
        &snapshot,
        GraphViewportRequest {
            x: 0,
            y: 0,
            width: GRAPH_LEFT_X + GRAPH_COLUMN_WIDTH + 80,
            height: GRAPH_TOP_Y + 80,
            overscan: 0,
        },
    );

    assert!(response.lanes.iter().any(|lane| lane.key == "lane:main"));
    assert!(
        response
            .nodes
            .iter()
            .any(|node| node.key == "node:base:120:90")
    );
    assert!(
        response
            .nodes
            .iter()
            .any(|node| node.key == "node:merged:340:90")
    );
    assert!(response.edges.iter().any(|edge| {
        edge.key == "edge:primary_parent:base:120:90:merged:340:90"
            && edge.source_id == "base"
            && edge.target_id == "merged"
    }));
}

#[test]
fn rendered_branch_items_include_lane_metadata() {
    let snapshot = two_node_snapshot(12);
    let html = render_snapshot_page(&snapshot);

    assert!(html.contains("class=\"branch\""));
    assert!(html.contains("data-lane-key=\"lane:main\""));
    assert!(html.contains("data-lane-y=\"90\""));
}

#[test]
fn graph_viewport_uses_unique_keys_for_duplicate_node_occurrences() {
    let mut snapshot = two_node_snapshot(28);
    snapshot.branches.push(GraphBranch {
        name: "side".to_owned(),
        head_id: "merged".to_owned(),
        visible_head_id: Some("merged".to_owned()),
        state: SessionState::Active,
    });

    let response = layout_graph_viewport(
        &snapshot,
        GraphViewportRequest {
            x: 0,
            y: 0,
            width: GRAPH_LEFT_X + GRAPH_COLUMN_WIDTH + 80,
            height: GRAPH_TOP_Y + GRAPH_LANE_HEIGHT + 80,
            overscan: 0,
        },
    );
    let merged_keys = response
        .nodes
        .iter()
        .filter(|node| node.id == "merged")
        .map(|node| node.key.as_str())
        .collect::<Vec<_>>();

    assert_eq!(merged_keys.len(), 2);
    assert_ne!(merged_keys[0], merged_keys[1]);
    assert!(
        merged_keys
            .iter()
            .all(|key| key.starts_with("node:merged:"))
    );
}

#[test]
fn graph_viewport_uses_unique_keys_for_duplicate_edge_occurrences() {
    let mut snapshot = two_node_snapshot(31);
    snapshot.branches.push(GraphBranch {
        name: "side".to_owned(),
        head_id: "merged".to_owned(),
        visible_head_id: Some("merged".to_owned()),
        state: SessionState::Active,
    });

    let response = layout_graph_viewport(
        &snapshot,
        GraphViewportRequest {
            x: 0,
            y: 0,
            width: GRAPH_LEFT_X + GRAPH_COLUMN_WIDTH + 80,
            height: GRAPH_TOP_Y + GRAPH_LANE_HEIGHT + 80,
            overscan: 0,
        },
    );
    let edge_keys = response
        .edges
        .iter()
        .filter(|edge| edge.source_id == "base" && edge.target_id == "merged")
        .map(|edge| edge.key.as_str())
        .collect::<Vec<_>>();

    assert_eq!(edge_keys.len(), 2);
    assert_ne!(edge_keys[0], edge_keys[1]);
    assert!(
        edge_keys
            .iter()
            .all(|key| key.contains(":base:") && key.contains(":merged:"))
    );
}

#[test]
fn graph_viewport_overscan_includes_nodes_outside_strict_viewport() {
    let snapshot = two_node_snapshot(13);

    let without_overscan = layout_graph_viewport(
        &snapshot,
        GraphViewportRequest {
            x: 0,
            y: 0,
            width: GRAPH_LEFT_X + 80,
            height: GRAPH_TOP_Y + 80,
            overscan: 0,
        },
    );
    let with_overscan = layout_graph_viewport(
        &snapshot,
        GraphViewportRequest {
            x: 0,
            y: 0,
            width: GRAPH_LEFT_X + 80,
            height: GRAPH_TOP_Y + 80,
            overscan: GRAPH_COLUMN_WIDTH,
        },
    );

    assert!(
        !without_overscan
            .nodes
            .iter()
            .any(|node| node.key == "node:merged:340:90")
    );
    assert!(
        with_overscan
            .nodes
            .iter()
            .any(|node| node.key == "node:merged:340:90")
    );
}

#[test]
fn graph_viewport_overscan_preloads_nodes_and_edges() {
    let snapshot = two_node_snapshot(23);

    let response = layout_graph_viewport(
        &snapshot,
        GraphViewportRequest {
            x: 0,
            y: 0,
            width: GRAPH_LEFT_X + 80,
            height: GRAPH_TOP_Y + 80,
            overscan: GRAPH_COLUMN_WIDTH,
        },
    );

    assert!(
        response
            .nodes
            .iter()
            .any(|node| node.key == "node:merged:340:90"),
        "overscan should preload the neighboring endpoint node"
    );
    assert!(
        response
            .edges
            .iter()
            .any(|edge| edge.key == "edge:primary_parent:base:120:90:merged:340:90"),
        "edges can render from the expanded viewport even when an endpoint is outside the strict view"
    );
}

#[test]
fn graph_viewport_half_view_overscan_preloads_neighbor_nodes() {
    let snapshot = linear_snapshot(24, &["n0", "n1", "n2"]);
    let viewport_width = GRAPH_COLUMN_WIDTH * 2;

    let response = layout_graph_viewport(
        &snapshot,
        GraphViewportRequest {
            x: 0,
            y: 0,
            width: viewport_width,
            height: GRAPH_TOP_Y + 80,
            overscan: viewport_width / 2,
        },
    );

    assert!(
        response
            .nodes
            .iter()
            .any(|node| node.key == "node:n0:120:90")
    );
    assert!(
        response
            .nodes
            .iter()
            .any(|node| node.key == "node:n1:340:90")
    );
    assert!(
        response
            .nodes
            .iter()
            .any(|node| node.key == "node:n2:560:90"),
        "half-view overscan should keep the next node ready before it enters view"
    );
}

#[test]
fn graph_viewport_stepwise_diff_keeps_visible_nodes_rendered() {
    let snapshot = linear_snapshot(25, &["n0", "n1", "n2", "n3", "n4", "n5"]);
    let width = GRAPH_COLUMN_WIDTH * 2;
    let overscan = width / 2;
    let viewports = [0, GRAPH_COLUMN_WIDTH / 2, GRAPH_COLUMN_WIDTH]
        .into_iter()
        .map(|x| GraphViewportRequest {
            x,
            y: 0,
            width,
            height: GRAPH_TOP_Y + 80,
            overscan,
        })
        .collect::<Vec<_>>();
    let mut rendered = viewport_rendered_node_keys(&snapshot, viewports[0]);

    for window in viewports.windows(2) {
        rendered = apply_diff_node_keys(rendered, &snapshot, window[0], window[1]);
        let strict_nodes = strict_viewport_node_keys(&snapshot, window[1]);
        assert!(
            strict_nodes.is_subset(&rendered),
            "applying each diff should keep visible nodes in the rendered set"
        );
    }
}

#[test]
fn graph_viewport_skipped_intermediate_patch_can_leave_visible_nodes_unrendered() {
    let snapshot = linear_snapshot(26, &["n0", "n1", "n2", "n3", "n4", "n5"]);
    let width = GRAPH_COLUMN_WIDTH * 2;
    let overscan = width / 2;
    let initial = GraphViewportRequest {
        x: 0,
        y: 0,
        width,
        height: GRAPH_TOP_Y + 80,
        overscan,
    };
    let current = GraphViewportRequest {
        x: GRAPH_COLUMN_WIDTH * 2,
        y: 0,
        width,
        height: GRAPH_TOP_Y + 80,
        overscan,
    };
    let rendered = viewport_rendered_node_keys(&snapshot, initial);
    let strict_nodes = strict_viewport_node_keys(&snapshot, current);
    let missing = strict_nodes
        .difference(&rendered)
        .cloned()
        .collect::<Vec<_>>();

    assert!(
        !missing.is_empty(),
        "if intermediate patches are skipped while viewBox keeps moving, visible nodes can outrun the rendered payload"
    );
}

#[test]
fn graph_viewport_coalesced_patch_catches_up_visible_nodes() {
    let snapshot = linear_snapshot(27, &["n0", "n1", "n2", "n3", "n4", "n5"]);
    let width = GRAPH_COLUMN_WIDTH * 2;
    let overscan = width / 2;
    let initial = GraphViewportRequest {
        x: 0,
        y: 0,
        width,
        height: GRAPH_TOP_Y + 80,
        overscan,
    };
    let current = GraphViewportRequest {
        x: GRAPH_COLUMN_WIDTH * 2,
        y: 0,
        width,
        height: GRAPH_TOP_Y + 80,
        overscan,
    };
    let rendered = apply_diff_node_keys(
        viewport_rendered_node_keys(&snapshot, initial),
        &snapshot,
        initial,
        current,
    );
    let strict_nodes = strict_viewport_node_keys(&snapshot, current);

    assert!(
        strict_nodes.is_subset(&rendered),
        "a coalesced patch from the last rendered viewport to the latest target should catch up visible nodes"
    );
}

#[test]
fn graph_viewport_includes_edges_crossing_visible_bounds() {
    let snapshot = two_node_snapshot(14);

    let response = layout_graph_viewport(
        &snapshot,
        GraphViewportRequest {
            x: GRAPH_LEFT_X + 80,
            y: GRAPH_TOP_Y - 20,
            width: GRAPH_COLUMN_WIDTH - 160,
            height: 40,
            overscan: 0,
        },
    );

    assert!(response.nodes.is_empty());
    assert!(
        response
            .edges
            .iter()
            .any(|edge| edge.key == "edge:primary_parent:base:120:90:merged:340:90")
    );
}

#[test]
fn graph_viewport_diff_reports_added_and_removed_items() {
    let snapshot = two_node_snapshot(15);

    let diff = layout_graph_viewport_diff(
        &snapshot,
        GraphViewportDiffRequest {
            previous: GraphViewportRequest {
                x: 0,
                y: 0,
                width: GRAPH_LEFT_X + 80,
                height: GRAPH_TOP_Y + 80,
                overscan: 0,
            },
            current: GraphViewportRequest {
                x: GRAPH_LEFT_X + GRAPH_COLUMN_WIDTH - 80,
                y: 0,
                width: 180,
                height: GRAPH_TOP_Y + 80,
                overscan: 0,
            },
            known: None,
        },
    );

    assert_eq!(diff.version, 15);
    assert!(
        diff.added
            .nodes
            .iter()
            .any(|node| node.key == "node:merged:340:90")
    );
    assert!(
        diff.removed
            .iter()
            .any(|item| item.key == "node:base:120:90")
    );
    assert!(diff.updated.nodes.is_empty());
}

#[test]
fn graph_viewport_diff_keeps_edges_when_only_an_endpoint_leaves_viewport() {
    let snapshot = linear_snapshot(20, &["n0", "n1", "n2"]);
    let previous = GraphViewportRequest {
        x: 0,
        y: 0,
        width: GRAPH_LEFT_X + GRAPH_COLUMN_WIDTH + 80,
        height: GRAPH_TOP_Y + 80,
        overscan: 0,
    };
    let current = GraphViewportRequest {
        x: GRAPH_LEFT_X + 80,
        y: 0,
        width: GRAPH_COLUMN_WIDTH - 160,
        height: GRAPH_TOP_Y + 80,
        overscan: 0,
    };

    let diff = layout_graph_viewport_diff(
        &snapshot,
        GraphViewportDiffRequest {
            previous,
            current,
            known: None,
        },
    );

    assert!(
        diff.removed
            .iter()
            .any(|item| item.kind == GraphViewportItemKind::Node && item.key == "node:n0:120:90")
    );
    assert!(
        !diff.removed.iter().any(|item| {
            item.kind == GraphViewportItemKind::Edge
                && item.key == "edge:primary_parent:n0:120:90:n1:340:90"
        }),
        "an edge can remain visible while only one endpoint node is in the viewport payload"
    );
}

#[test]
fn graph_viewport_diff_sliding_window_replaces_incident_edges() {
    let snapshot = linear_snapshot(21, &["n0", "n1", "n2", "n3"]);
    let previous = GraphViewportRequest {
        x: 0,
        y: 0,
        width: GRAPH_LEFT_X + 80,
        height: GRAPH_TOP_Y + 80,
        overscan: 0,
    };
    let current = GraphViewportRequest {
        x: GRAPH_LEFT_X + GRAPH_COLUMN_WIDTH - 80,
        y: 0,
        width: GRAPH_COLUMN_WIDTH + 160,
        height: GRAPH_TOP_Y + 80,
        overscan: 0,
    };

    let diff = layout_graph_viewport_diff(
        &snapshot,
        GraphViewportDiffRequest {
            previous,
            current,
            known: None,
        },
    );

    assert!(
        diff.removed
            .iter()
            .any(|item| item.kind == GraphViewportItemKind::Node && item.key == "node:n0:120:90")
    );
    assert!(
        diff.added
            .nodes
            .iter()
            .any(|node| node.key == "node:n2:560:90")
    );
    assert!(
        diff.added
            .edges
            .iter()
            .any(|edge| edge.key == "edge:primary_parent:n1:340:90:n2:560:90")
    );
    assert!(
        !diff.removed.iter().any(|item| {
            item.kind == GraphViewportItemKind::Edge
                && item.key == "edge:primary_parent:n0:120:90:n1:340:90"
        }),
        "sliding the viewport should keep a crossing edge until it leaves the expanded render area"
    );
}

#[test]
fn graph_viewport_diff_same_viewport_is_empty_without_known_items() {
    let snapshot = two_node_snapshot(16);
    let viewport = GraphViewportRequest {
        x: 0,
        y: 0,
        width: GRAPH_LEFT_X + GRAPH_COLUMN_WIDTH + 80,
        height: GRAPH_TOP_Y + 80,
        overscan: 0,
    };

    let diff = layout_graph_viewport_diff(
        &snapshot,
        GraphViewportDiffRequest {
            previous: viewport,
            current: viewport,
            known: None,
        },
    );

    assert!(diff.added.nodes.is_empty());
    assert!(diff.added.edges.is_empty());
    assert!(diff.updated.nodes.is_empty());
    assert!(diff.updated.edges.is_empty());
    assert!(diff.removed.is_empty());
}

#[test]
fn graph_viewport_diff_known_items_reports_added_updated_and_removed() {
    let snapshot = two_node_snapshot(17);
    let viewport = GraphViewportRequest {
        x: 0,
        y: 0,
        width: GRAPH_LEFT_X + GRAPH_COLUMN_WIDTH + 80,
        height: GRAPH_TOP_Y + 80,
        overscan: 0,
    };

    let diff = layout_graph_viewport_diff(
        &snapshot,
        GraphViewportDiffRequest {
            previous: viewport,
            current: viewport,
            known: Some(GraphViewportKnownItems {
                lanes: vec!["lane:main".to_owned()],
                lane_fingerprints: Default::default(),
                nodes: vec!["node:base:120:90".to_owned(), "node:stale".to_owned()],
                node_fingerprints: Default::default(),
                edges: vec!["edge:primary_parent:base:stale".to_owned()],
                edge_fingerprints: Default::default(),
            }),
        },
    );

    assert!(
        diff.added
            .nodes
            .iter()
            .any(|node| node.key == "node:merged:340:90")
    );
    assert!(
        diff.added
            .edges
            .iter()
            .any(|edge| edge.key == "edge:primary_parent:base:120:90:merged:340:90")
    );
    assert!(
        diff.updated
            .lanes
            .iter()
            .any(|lane| lane.key == "lane:main")
    );
    assert!(
        diff.updated
            .nodes
            .iter()
            .any(|node| node.key == "node:base:120:90")
    );
    assert!(
        diff.removed
            .iter()
            .any(|item| { item.kind == GraphViewportItemKind::Node && item.key == "node:stale" })
    );
    assert!(diff.removed.iter().any(|item| {
        item.kind == GraphViewportItemKind::Edge && item.key == "edge:primary_parent:base:stale"
    }));
}

#[test]
fn graph_viewport_diff_known_empty_set_reports_current_items_as_added() {
    let snapshot = two_node_snapshot(29);
    let viewport = GraphViewportRequest {
        x: 0,
        y: 0,
        width: GRAPH_LEFT_X + GRAPH_COLUMN_WIDTH + 80,
        height: GRAPH_TOP_Y + 80,
        overscan: 0,
    };

    let diff = layout_graph_viewport_diff(
        &snapshot,
        GraphViewportDiffRequest {
            previous: viewport,
            current: viewport,
            known: Some(GraphViewportKnownItems::default()),
        },
    );

    assert!(
        diff.added
            .nodes
            .iter()
            .any(|node| node.key == "node:base:120:90")
    );
    assert!(
        diff.added
            .nodes
            .iter()
            .any(|node| node.key == "node:merged:340:90")
    );
    assert!(
        diff.added
            .edges
            .iter()
            .any(|edge| edge.key == "edge:primary_parent:base:120:90:merged:340:90")
    );
    assert!(diff.removed.is_empty());
}

#[test]
fn graph_viewport_diff_known_items_updates_edges_without_visible_endpoint_nodes() {
    let snapshot = linear_snapshot(22, &["n0", "n1", "n2"]);
    let viewport = GraphViewportRequest {
        x: 0,
        y: 0,
        width: GRAPH_LEFT_X + 80,
        height: GRAPH_TOP_Y + 80,
        overscan: 0,
    };

    let diff = layout_graph_viewport_diff(
        &snapshot,
        GraphViewportDiffRequest {
            previous: viewport,
            current: viewport,
            known: Some(GraphViewportKnownItems {
                lanes: Vec::new(),
                lane_fingerprints: Default::default(),
                nodes: vec!["node:n0:120:90".to_owned()],
                node_fingerprints: Default::default(),
                edges: vec!["edge:primary_parent:n0:120:90:n1:340:90".to_owned()],
                edge_fingerprints: Default::default(),
            }),
        },
    );

    assert!(
        diff.updated
            .nodes
            .iter()
            .any(|node| node.key == "node:n0:120:90"),
        "the visible endpoint should remain updated"
    );
    assert!(diff.added.edges.is_empty());
    assert!(
        diff.updated
            .edges
            .iter()
            .any(|edge| edge.key == "edge:primary_parent:n0:120:90:n1:340:90")
    );
    assert!(!diff.removed.iter().any(|item| {
        item.kind == GraphViewportItemKind::Edge
            && item.key == "edge:primary_parent:n0:120:90:n1:340:90"
    }));
}

#[test]
fn graph_viewport_diff_zooming_out_adds_newly_visible_items() {
    let snapshot = linear_snapshot(18, &["n0", "n1", "n2", "n3"]);
    let previous = GraphViewportRequest {
        x: 0,
        y: 0,
        width: GRAPH_LEFT_X + 80,
        height: GRAPH_TOP_Y + 80,
        overscan: 0,
    };
    let current = GraphViewportRequest {
        x: 0,
        y: 0,
        width: GRAPH_LEFT_X + 3 * GRAPH_COLUMN_WIDTH + 80,
        height: GRAPH_TOP_Y + 80,
        overscan: 0,
    };

    let diff = layout_graph_viewport_diff(
        &snapshot,
        GraphViewportDiffRequest {
            previous,
            current,
            known: None,
        },
    );

    assert!(
        diff.added
            .nodes
            .iter()
            .any(|node| node.key == "node:n2:560:90")
    );
    assert!(
        diff.added
            .nodes
            .iter()
            .any(|node| node.key == "node:n3:780:90")
    );
    assert!(
        diff.added
            .edges
            .iter()
            .any(|edge| edge.key == "edge:primary_parent:n1:340:90:n2:560:90")
    );
    assert!(
        !diff
            .removed
            .iter()
            .any(|item| item.kind == GraphViewportItemKind::Node)
    );
}

#[test]
fn graph_viewport_diff_zooming_in_removes_items_outside_new_viewport() {
    let snapshot = linear_snapshot(19, &["n0", "n1", "n2", "n3"]);
    let previous = GraphViewportRequest {
        x: 0,
        y: 0,
        width: GRAPH_LEFT_X + 3 * GRAPH_COLUMN_WIDTH + 80,
        height: GRAPH_TOP_Y + 80,
        overscan: 0,
    };
    let current = GraphViewportRequest {
        x: 0,
        y: 0,
        width: GRAPH_LEFT_X + 80,
        height: GRAPH_TOP_Y + 80,
        overscan: 0,
    };

    let diff = layout_graph_viewport_diff(
        &snapshot,
        GraphViewportDiffRequest {
            previous,
            current,
            known: None,
        },
    );

    assert!(
        diff.removed.iter().any(|item| {
            item.kind == GraphViewportItemKind::Node && item.key == "node:n2:560:90"
        })
    );
    assert!(
        diff.removed.iter().any(|item| {
            item.kind == GraphViewportItemKind::Node && item.key == "node:n3:780:90"
        })
    );
    assert!(diff.removed.iter().any(|item| {
        item.kind == GraphViewportItemKind::Edge
            && item.key == "edge:primary_parent:n1:340:90:n2:560:90"
    }));
    assert!(diff.added.nodes.is_empty());
}

#[test]
fn graph_viewport_request_normalizes_negative_or_empty_dimensions() {
    let request = GraphViewportRequest {
        x: -50,
        y: -10,
        width: 0,
        height: -1,
        overscan: -20,
    }
    .normalized();

    assert_eq!(request.x, 0);
    assert_eq!(request.y, 0);
    assert_eq!(request.width, 1);
    assert_eq!(request.height, 1);
    assert_eq!(request.overscan, 0);
}

#[test]
fn routed_edges_use_inter_lane_corridors() {
    let source = Point {
        x: GRAPH_LEFT_X,
        y: GRAPH_TOP_Y,
    };
    let target = Point {
        x: GRAPH_LEFT_X + GRAPH_COLUMN_WIDTH,
        y: GRAPH_TOP_Y + GRAPH_LANE_HEIGHT,
    };

    let first_route = routed_elbow_points(source, target, 0, 0.0);
    let second_route = routed_elbow_points(source, target, 1, 0.0);

    assert!(first_route.contains("190.0,160.0 258.0,160.0"));
    assert!(!first_route.contains("190.0,90.0 258.0,90.0"));
    assert!(!first_route.contains("190.0,230.0 258.0,230.0"));
    assert!(second_route.contains("190.0,148.0 258.0,148.0"));
}

#[test]
fn streamed_graph_markup_escapes_dynamic_values() {
    let mut node = graph_node("node-\"<&", 0);
    node.kind = "Text<&".to_owned();
    node.summary = "<script>alert(1)</script> & title".to_owned();
    node.content = "<img src=x onerror=alert(1)>".to_owned();
    node.labels = vec!["main<&".to_owned()];
    let snapshot = GraphSnapshot {
        version: 1,
        mode: GraphMode::All,
        root_id: "root".to_owned(),
        nodes: vec![node],
        edges: Vec::new(),
        branches: vec![GraphBranch {
            name: "main".to_owned(),
            head_id: "node-\"<&".to_owned(),
            visible_head_id: Some("node-\"<&".to_owned()),
            state: SessionState::Active,
        }],
        provider_contexts: Vec::new(),
    };

    let html = render_snapshot_page(&snapshot);

    assert!(html.contains("class=\"graph-lanes\""));
    assert!(html.contains("class=\"graph-edges\""));
    assert!(html.contains("class=\"graph-nodes\""));
    assert!(!html.contains("<script>alert(1)</script>"));
    assert!(!html.contains("<img src=x onerror=alert(1)>"));
}

#[test]
fn console_store_notifies_after_successful_writes() {
    let publisher = ConsolePublisher::new();
    let store = ConsoleStore::new(MemoryStore::new(), publisher.clone());
    let root = store.root_id();

    store
        .append(NewNode {
            parent: root,
            role: Role::User,
            metadata: None,
            kind: Kind::tool_use(coco_mem::ToolUse {
                id: "tool-1".to_owned(),
                name: "noop".to_owned(),
                input: json!({}),
            }),
        })
        .unwrap();

    assert_eq!(publisher.current_version(), 1);
}

#[test]
fn console_store_notifies_only_when_dequeue_removes_message() {
    let publisher = ConsolePublisher::new();
    let store = ConsoleStore::new(MemoryStore::new(), publisher.clone());

    assert_eq!(store.dequeue_message("system").unwrap(), None);
    assert_eq!(publisher.current_version(), 0);

    let item = store
        .enqueue_message("system", json!({"ok": true}))
        .unwrap();
    assert_eq!(publisher.current_version(), 1);

    assert_eq!(store.dequeue_message("system").unwrap(), Some(item));
    assert_eq!(publisher.current_version(), 2);

    assert_eq!(store.dequeue_message("system").unwrap(), None);
    assert_eq!(publisher.current_version(), 2);
}

#[test]
fn console_store_lists_message_queues() {
    let store = ConsoleStore::new(MemoryStore::new(), ConsolePublisher::new());

    store
        .enqueue_message("system", json!({"ok": true}))
        .unwrap();

    assert_eq!(store.list_message_queues().unwrap(), vec!["system"]);
}

#[test]
fn rendered_page_does_not_embed_javascript() {
    let snapshot = GraphSnapshot {
        version: 0,
        mode: GraphMode::All,
        root_id: "root".to_owned(),
        nodes: Vec::new(),
        edges: Vec::new(),
        branches: Vec::new(),
        provider_contexts: Vec::new(),
    };
    let html = render_snapshot_page(&snapshot);

    assert!(!html.contains("<script"));
    assert!(!html.contains("javascript"));
    assert!(!html.contains("http-equiv=\"refresh\""));
}

#[test]
fn fragment_renders_refresh_free_console_root() {
    let snapshot = GraphSnapshot {
        version: 0,
        mode: GraphMode::All,
        root_id: "root".to_owned(),
        nodes: Vec::new(),
        edges: Vec::new(),
        branches: Vec::new(),
        provider_contexts: Vec::new(),
    };
    let html = render_fragment(&snapshot);

    assert!(html.contains("id=\"console-root\""));
    assert!(html.contains("data-version=\"0\""));
    assert!(!html.contains("<!doctype"));
    assert!(!html.contains("<script"));
    assert!(!html.contains("javascript"));
    assert!(!html.contains("http-equiv=\"refresh\""));
}

#[test]
fn index_page_loads_wasm_client_without_document_refresh() {
    let snapshot = GraphSnapshot {
        version: 0,
        mode: GraphMode::All,
        root_id: "root".to_owned(),
        nodes: Vec::new(),
        edges: Vec::new(),
        branches: Vec::new(),
        provider_contexts: Vec::new(),
    };
    let html = render_index_page(&snapshot);

    assert!(html.contains("src=\"/pkg/coco_console.js\""));
    assert!(html.contains("id=\"console-root\""));
    assert!(!html.contains("<iframe"));
    assert!(!html.contains("/live"));
    assert!(!html.contains("javascript"));
    assert!(!html.contains("http-equiv=\"refresh\""));
}

#[test]
fn request_parser_extracts_path_without_query() {
    let header = b"GET /api/graph?x=1 HTTP/1.1\r\nhost: localhost\r\n\r\n";
    let header = String::from_utf8_lossy(header);
    let mut parts = header.lines().next().unwrap_or_default().split_whitespace();
    assert_eq!(parts.next(), Some("GET"));
    assert_eq!(
        parts.next().and_then(|target| target.split('?').next()),
        Some("/api/graph")
    );
}
