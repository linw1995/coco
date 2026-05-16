use coco_mem::{
    Anchor, BranchStore, Kind, MemoryStore, MergeParent, NewNode, NodeStore, Role, SessionAnchor,
    SessionRole, SessionState, Tool,
};
use serde_json::json;

use crate::graph::{GraphBranch, GraphEdge, GraphEdgeKind, GraphNode};
use crate::layout::{
    EDGE_TARGET_PORT_STEP, GRAPH_COLUMN_WIDTH, GRAPH_LANE_HEIGHT, GRAPH_LEFT_X, GRAPH_TOP_Y,
    GraphLayoutEdgeKind, Point, layout_graph, routed_elbow_points,
};
use crate::render::{render_fragment, render_index_page, render_snapshot_page};
use crate::{ConsolePublisher, ConsoleStore, GraphSnapshot, build_graph_snapshot};

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
    }
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
        kind: GraphEdgeKind::PrimaryParent,
    }));
    assert!(snapshot.edges.contains(&GraphEdge {
        source: left.clone(),
        target: draft,
        kind: GraphEdgeKind::PrimaryParent,
    }));
    assert!(snapshot.edges.contains(&GraphEdge {
        source: right,
        target: merged,
        kind: GraphEdgeKind::MergeParent,
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
    assert!(html.contains("href=\"#detail-"));
    assert!(html.contains("class=\"graph-control"));
    assert!(html.contains("data-zoom-action=\"in\""));
    assert!(html.contains("data-graph-x="));
    assert!(html.contains("data-graph-min-x="));
    assert!(html.contains("class=\"node-details node-detail-panel\""));
    assert!(html.contains("class=\"branch-section\""));
    assert!(html.contains("class=\"minimap\""));
    assert!(html.contains("preserveAspectRatio=\"xMidYMid meet\""));
    assert!(html.contains("class=\"minimap-viewport\""));
    assert!(html.contains("data-graph-width="));
    assert!(!html.contains("/?node="));
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
        kind: GraphEdgeKind::ShadowParent,
    }));
}

#[test]
fn layout_expands_empty_columns_from_event_order() {
    let snapshot = GraphSnapshot {
        version: 1,
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
            kind: GraphEdgeKind::PrimaryParent,
        }],
        branches: vec![GraphBranch {
            name: "main".to_owned(),
            head_id: "merged".to_owned(),
            state: SessionState::Active,
        }],
    };

    let layout = layout_graph(&snapshot);
    let primary_edge = layout.primary_edges.first().unwrap();

    assert_eq!(
        primary_edge.target.x - primary_edge.source.x,
        3 * GRAPH_COLUMN_WIDTH
    );
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
fn rendered_page_does_not_embed_javascript() {
    let snapshot = GraphSnapshot {
        version: 0,
        root_id: "root".to_owned(),
        nodes: Vec::new(),
        edges: Vec::new(),
        branches: Vec::new(),
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
        root_id: "root".to_owned(),
        nodes: Vec::new(),
        edges: Vec::new(),
        branches: Vec::new(),
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
        root_id: "root".to_owned(),
        nodes: Vec::new(),
        edges: Vec::new(),
        branches: Vec::new(),
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
