use std::net::SocketAddr;

use coco_mem::{
    Anchor, BranchStore, JobStore, Kind, MemoryStore, MergeParent, MessageQueueStore, NewNode,
    NodeStore, Preset, PresetStore, Role, SessionAnchor, SessionRole, SessionState, SkillStore,
    SkillVersionSpec, Tool,
};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::config::ConsoleConfig;
use crate::graph::{
    GraphBranch, GraphEdge, GraphEdgeKind, GraphEntityCollection, GraphEntityCounts,
    GraphEntityKind, GraphNode, node_target_id,
};
use crate::layout::{
    EDGE_TARGET_PORT_STEP, GRAPH_COLUMN_WIDTH, GRAPH_LANE_HEIGHT, GRAPH_LEFT_X, GRAPH_TOP_Y,
    GraphLayoutEdgeKind, Point, layout_graph, line_render_points, routed_elbow_points,
    routed_elbow_render_points,
};
use crate::render::{render_fragment, render_index_page, render_snapshot_page};
use crate::{
    ConsolePublisher, ConsoleStore, GraphSnapshot, build_graph_snapshot, start_console_server,
};

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

fn empty_snapshot() -> GraphSnapshot {
    GraphSnapshot {
        version: 0,
        root_id: "root".to_owned(),
        nodes: Vec::new(),
        edges: Vec::new(),
        branches: Vec::new(),
        entity_counts: GraphEntityCounts {
            nodes: 0,
            branches: 0,
            sessions: 0,
            presets: 0,
            skills: 0,
            jobs: 0,
            queues: 0,
        },
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
    assert!(html.contains("class=\"graph-control"));
    assert!(html.contains("data-zoom-action=\"in\""));
    assert!(html.contains("class=\"graph-items\""));
    assert!(html.contains("class=\"node-link graph-item\""));
    assert!(html.contains("data-graph-min-x="));
    assert!(html.contains("class=\"node-details node-detail-panel\""));
    assert!(html.contains("class=\"entity-nav\""));
    assert!(html.contains("id=\"branches\""));
    assert!(html.contains("id=\"sessions\""));
    assert!(html.contains("class=\"minimap\""));
    assert!(html.contains("preserveAspectRatio=\"xMidYMid meet\""));
    assert!(html.contains("class=\"minimap-viewport\""));
    assert!(!html.contains("class=\"minimap-node\""));
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
fn graph_snapshot_contains_store_entities() {
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
    store
        .set_preset(
            "default",
            Preset {
                role: SessionRole::Orchestrator,
                provider_profile: "default".to_owned(),
                model: "gpt-4.1-mini".to_owned(),
                tools: Vec::new(),
                system_prompt: "System prompt".to_owned(),
                prompt: "Prompt".to_owned(),
                temperature: None,
                max_tokens: None,
                additional_params: None,
                enable_coco_shim: false,
            },
        )
        .unwrap();
    store
        .add_skill(
            SessionRole::Runner,
            "demo",
            SkillVersionSpec {
                description: "Demo skill".to_owned(),
                body: "Run demo".to_owned(),
                scripts: Vec::new(),
                enable_coco_shim: true,
            },
        )
        .unwrap();
    store.submit_job("main", &session).unwrap();
    store
        .enqueue_message("hooks", json!({ "event": "created" }))
        .unwrap();

    let snapshot = build_graph_snapshot(&store, 9).unwrap();

    assert_eq!(snapshot.entity_counts.sessions, 1);
    assert_eq!(snapshot.entity_counts.presets, 1);
    assert!(snapshot.entity_counts.skills >= 1);
    assert_eq!(snapshot.entity_counts.jobs, 1);
    assert_eq!(snapshot.entity_counts.queues, 1);

    let skills = crate::graph::build_entity_collection(&store, GraphEntityKind::Skills).unwrap();
    match skills {
        GraphEntityCollection::Skills(skills) => {
            assert!(skills.iter().any(|skill| skill.name == "demo"));
        }
        _ => panic!("expected skills collection"),
    }
    let branches =
        crate::graph::build_entity_collection(&store, GraphEntityKind::Branches).unwrap();
    match branches {
        GraphEntityCollection::Branches(branches) => {
            assert_eq!(branches.len(), 1);
            assert_eq!(branches[0].name, "main");
            assert_eq!(branches[0].head_id, session);
        }
        _ => panic!("expected branches collection"),
    }
    let sessions =
        crate::graph::build_entity_collection(&store, GraphEntityKind::Sessions).unwrap();
    match sessions {
        GraphEntityCollection::Sessions(sessions) => {
            assert_eq!(sessions.len(), 1);
            assert_eq!(sessions[0].branch, "main");
            assert_eq!(sessions[0].state, "Active");
        }
        _ => panic!("expected sessions collection"),
    }
    let presets = crate::graph::build_entity_collection(&store, GraphEntityKind::Presets).unwrap();
    match presets {
        GraphEntityCollection::Presets(presets) => {
            assert_eq!(presets.len(), 1);
            assert_eq!(presets[0].name, "default");
            assert_eq!(presets[0].prompt, "Prompt");
            assert_eq!(presets[0].system_prompt, "System prompt");
        }
        _ => panic!("expected presets collection"),
    }
    let jobs = crate::graph::build_entity_collection(&store, GraphEntityKind::Jobs).unwrap();
    match jobs {
        GraphEntityCollection::Jobs(jobs) => {
            assert_eq!(jobs.len(), 1);
            assert_eq!(jobs[0].branch, "main");
            assert_eq!(jobs[0].status, "Queued");
        }
        _ => panic!("expected jobs collection"),
    }
    let queues = crate::graph::build_entity_collection(&store, GraphEntityKind::Queues).unwrap();
    match queues {
        GraphEntityCollection::Queues(queues) => {
            assert_eq!(queues.len(), 1);
            assert_eq!(queues[0].message_count, 1);
        }
        _ => panic!("expected queues collection"),
    }

    let html = render_snapshot_page(&snapshot);
    assert!(html.contains("href=\"#presets\""));
    assert!(html.contains("href=\"#skills\""));
    assert!(html.contains("href=\"#jobs\""));
    assert!(html.contains("href=\"#queues\""));
    assert!(html.contains("data-entity-kind=\"skills\""));
    assert!(!html.contains("Demo skill"));
    assert!(!html.contains("hooks"));
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
        entity_counts: GraphEntityCounts {
            nodes: 4,
            branches: 1,
            sessions: 1,
            presets: 0,
            skills: 0,
            jobs: 0,
            queues: 0,
        },
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
    let shifted_route = routed_elbow_render_points(source, target, 0, EDGE_TARGET_PORT_STEP);

    assert!(first_route.contains("190.0,160.0 258.0,160.0"));
    assert!(!first_route.contains("190.0,90.0 258.0,90.0"));
    assert!(!first_route.contains("190.0,230.0 258.0,230.0"));
    assert!(second_route.contains("190.0,148.0 258.0,148.0"));
    assert_eq!(
        shifted_route.last().unwrap().y,
        f64::from(target.y) + EDGE_TARGET_PORT_STEP
    );
}

#[test]
fn rendered_edge_points_include_target_port_offsets() {
    let source = Point {
        x: GRAPH_LEFT_X,
        y: GRAPH_TOP_Y,
    };
    let target = Point {
        x: GRAPH_LEFT_X + GRAPH_COLUMN_WIDTH,
        y: GRAPH_TOP_Y,
    };

    let base = line_render_points(source, target, 0.0);
    let shifted = line_render_points(source, target, EDGE_TARGET_PORT_STEP);

    assert!(shifted[1].y > base[1].y);
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
    let snapshot = empty_snapshot();
    let html = render_snapshot_page(&snapshot);

    assert!(!html.contains("<script"));
    assert!(!html.contains("javascript"));
    assert!(!html.contains("http-equiv=\"refresh\""));
}

#[test]
fn fragment_renders_refresh_free_console_root() {
    let snapshot = empty_snapshot();
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
    let snapshot = empty_snapshot();
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

#[test]
fn query_values_are_percent_decoded() {
    assert_eq!(
        super::server::parse_query_value("id=a%2Fb%20c&kind=skills", "id"),
        Some("a/b c".to_owned())
    );
    assert_eq!(
        super::server::parse_query_value("id=a+b", "id"),
        Some("a b".to_owned())
    );
}

#[test]
fn entity_kind_parser_accepts_known_sections() {
    assert_eq!(
        GraphEntityKind::parse("branches"),
        Some(GraphEntityKind::Branches)
    );
    assert_eq!(
        GraphEntityKind::parse("sessions"),
        Some(GraphEntityKind::Sessions)
    );
    assert_eq!(
        GraphEntityKind::parse("presets"),
        Some(GraphEntityKind::Presets)
    );
    assert_eq!(
        GraphEntityKind::parse("skills"),
        Some(GraphEntityKind::Skills)
    );
    assert_eq!(GraphEntityKind::parse("jobs"), Some(GraphEntityKind::Jobs));
    assert_eq!(
        GraphEntityKind::parse("queues"),
        Some(GraphEntityKind::Queues)
    );
    assert_eq!(GraphEntityKind::parse("nodes"), None);
}

#[tokio::test]
async fn server_serves_entity_and_node_details_on_demand() {
    let publisher = ConsolePublisher::new();
    let store = ConsoleStore::new(MemoryStore::new(), publisher.clone());
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
    let detail = store
        .append(NewNode {
            parent: session.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("load this node only".to_owned()),
        })
        .unwrap();
    store.set_branch_head("main", &session, &detail).unwrap();
    store
        .add_skill(
            SessionRole::Runner,
            "server-demo",
            SkillVersionSpec {
                description: "Server demo skill".to_owned(),
                body: "Run server demo".to_owned(),
                scripts: Vec::new(),
                enable_coco_shim: true,
            },
        )
        .unwrap();

    let handle = start_console_server(
        ConsoleConfig {
            addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        },
        store,
        publisher,
    )
    .unwrap();
    let addr = handle.addr();

    let index = http_get(addr, "/").await;
    assert_eq!(response_status(&index), 200);
    assert!(response_body(&index).contains("data-entity-kind=\"skills\""));
    assert!(response_body(&index).contains("class=\"graph-items\""));
    assert!(!response_body(&index).contains("Server demo skill"));
    assert!(response_body(&index).contains("class=\"node-link graph-item\""));

    let graph = http_get(addr, "/api/graph").await;
    assert_eq!(response_status(&graph), 200);
    let graph: serde_json::Value = serde_json::from_str(response_body(&graph)).unwrap();
    assert!(graph["entity_counts"]["skills"].as_u64().unwrap() >= 1);
    assert!(graph.get("skills").is_none());

    let node = http_get(addr, &format!("/api/node?id={detail}")).await;
    assert_eq!(response_status(&node), 200);
    let node: serde_json::Value = serde_json::from_str(response_body(&node)).unwrap();
    assert_eq!(node["id"], detail);
    assert_eq!(node["content"], "load this node only");

    let node_by_target = http_get(
        addr,
        &format!("/api/node?target={}", node_target_id(&detail)),
    )
    .await;
    assert_eq!(response_status(&node_by_target), 200);
    let node_by_target: serde_json::Value =
        serde_json::from_str(response_body(&node_by_target)).unwrap();
    assert_eq!(node_by_target["id"], detail);
    assert_eq!(node_by_target["content"], "load this node only");

    let graph_items = http_get(
        addr,
        "/api/graph-items?left=-100&top=-100&right=700&bottom=400",
    )
    .await;
    assert_eq!(response_status(&graph_items), 200);
    assert!(response_body(&graph_items).contains("class=\"node-link graph-item\""));
    assert!(response_body(&graph_items).contains("data-graph-min-x="));

    let empty_graph_items = http_get(
        addr,
        "/api/graph-items?left=10000&top=10000&right=10100&bottom=10100",
    )
    .await;
    assert_eq!(response_status(&empty_graph_items), 200);
    assert!(!response_body(&empty_graph_items).contains("class=\"node-link graph-item\""));
    let bad_graph_items = http_get(addr, "/api/graph-items?left=5&top=0&right=1&bottom=2").await;
    assert_eq!(response_status(&bad_graph_items), 400);

    let skills = http_get(addr, "/api/entities?kind=skills").await;
    assert_eq!(response_status(&skills), 200);
    let skills: serde_json::Value = serde_json::from_str(response_body(&skills)).unwrap();
    assert_eq!(skills["kind"], "skills");
    assert!(skills["items"].as_array().unwrap().iter().any(|skill| {
        skill["name"] == "server-demo" && skill["description"] == "Server demo skill"
    }));

    let bad_kind = http_get(addr, "/api/entities?kind=unknown").await;
    assert_eq!(response_status(&bad_kind), 400);
    let missing_node = http_get(addr, "/api/node").await;
    assert_eq!(response_status(&missing_node), 400);

    handle.shutdown().await.unwrap();
}

async fn http_get(addr: SocketAddr, target: &str) -> String {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let request = format!("GET {target} HTTP/1.1\r\nhost: localhost\r\n\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();

    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    String::from_utf8(response).unwrap()
}

fn response_status(response: &str) -> u16 {
    response
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse().ok())
        .unwrap()
}

fn response_body(response: &str) -> &str {
    response.split_once("\r\n\r\n").unwrap().1
}
