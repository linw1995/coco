use leptos::prelude::*;

use crate::api::{
    GraphBezierRoute, GraphViewportEdge, GraphViewportEdgeKind, GraphViewportNode,
    GraphViewportResponse,
};

pub struct GraphCanvasModel {
    canvas_width: i32,
    canvas_height: i32,
    viewport_x: i32,
    viewport_y: i32,
    viewport_width: i32,
    viewport_height: i32,
    render_edge_hit_targets: bool,
    nodes: Vec<RenderedNode>,
    edges: Vec<RenderedEdge>,
}

struct RenderedNode {
    id: String,
    key: String,
    target: String,
    node_id: String,
    x: i32,
    y: i32,
    class: String,
    title: String,
    label: String,
    kind: String,
}

struct RenderedEdge {
    id: String,
    hit_target_id: String,
    key: String,
    class: &'static str,
    marker: &'static str,
    path: String,
    kind: &'static str,
    source_id: String,
    target_id: String,
    source_x: i32,
    source_y: i32,
    control_1_x: i32,
    control_1_y: i32,
    control_2_x: i32,
    control_2_y: i32,
    target_x: i32,
    target_y: i32,
    hit_target_label: String,
}

impl GraphCanvasModel {
    pub fn new(render_edge_hit_targets: bool, response: &GraphViewportResponse) -> Self {
        Self {
            canvas_width: response.canvas.width,
            canvas_height: response.canvas.height,
            viewport_x: response.viewport.x,
            viewport_y: response.viewport.y,
            viewport_width: response.viewport.width,
            viewport_height: response.viewport.height,
            render_edge_hit_targets,
            nodes: response.nodes.iter().map(RenderedNode::new).collect(),
            edges: response.edges.iter().map(RenderedEdge::new).collect(),
        }
    }
}

impl RenderedNode {
    fn new(node: &GraphViewportNode) -> Self {
        Self {
            id: render_element_id(&node.key),
            key: node.key.clone(),
            target: node.node_target.clone(),
            node_id: node.id.clone(),
            x: node.x,
            y: node.y,
            class: node_group_class(node),
            title: node_title_text(node),
            label: node_label(node),
            kind: node.kind.clone(),
        }
    }
}

impl RenderedEdge {
    fn new(edge: &GraphViewportEdge) -> Self {
        let (class, marker) = edge_style(edge.kind);
        Self {
            id: render_element_id(&edge.key),
            hit_target_id: edge_hit_target_id(&edge.key),
            key: edge.key.clone(),
            class,
            marker,
            path: bezier_path(edge.route),
            kind: edge.kind.key_part(),
            source_id: edge.source_id.clone(),
            target_id: edge.target_id.clone(),
            source_x: edge.route.source.x,
            source_y: edge.route.source.y,
            control_1_x: edge.route.control_1.x,
            control_1_y: edge.route.control_1.y,
            control_2_x: edge.route.control_2.x,
            control_2_y: edge.route.control_2.y,
            target_x: edge.route.target.x,
            target_y: edge.route.target.y,
            hit_target_label: edge_hit_target_label(edge),
        }
    }
}

#[component]
pub fn GraphCanvas(graph: GraphCanvasModel) -> AnyView {
    let GraphCanvasModel {
        canvas_width,
        canvas_height,
        viewport_x,
        viewport_y,
        viewport_width,
        viewport_height,
        render_edge_hit_targets,
        nodes,
        edges,
    } = graph;
    let edges = edges
        .into_iter()
        .map(|edge| graph_edge_view(edge, render_edge_hit_targets))
        .collect_view();
    let nodes = nodes.into_iter().map(graph_node_view).collect_view();

    view! {
        <div
            class="graph-wrap virtual-graph"
            tabindex="0"
            data-viewport-x=viewport_x
            data-viewport-y=viewport_y
            data-canvas-width=canvas_width
            data-zoom="1"
        >
            <button
                class="follow-toggle"
                type="button"
                aria-pressed="false"
                title="Keep the graph pinned to the top-right edge"
            >
                "Follow"
            </button>
            <svg
                class="graph"
                role="img"
                aria-label="CoCo node graph"
                viewBox=format!("{viewport_x} {viewport_y} {viewport_width} {viewport_height}")
                width="100%"
                height="100%"
            >
                <GraphMarkers/>
                <rect
                    class="graph-bg"
                    x="0"
                    y="0"
                    width=canvas_width
                    height=canvas_height
                ></rect>
                <g class="graph-edges">{edges}</g>
                <g class="graph-nodes">{nodes}</g>
                <g class="graph-anchor-range" aria-live="polite"></g>
            </svg>
            <div class="graph-status" hidden></div>
        </div>
    }
    .into_any()
}

#[component]
fn GraphMarkers() -> AnyView {
    view! {
        <defs>
            <GraphMarker id="arrowhead" class="arrowhead"/>
            <GraphMarker id="merge-arrowhead" class="merge-arrowhead"/>
            <GraphMarker id="shadow-arrowhead" class="shadow-arrowhead"/>
        </defs>
    }
    .into_any()
}

#[component]
fn GraphMarker(id: &'static str, class: &'static str) -> AnyView {
    view! {
        <marker
            id=id
            markerWidth="10"
            markerHeight="8"
            refX="9"
            refY="4"
            orient="auto"
            markerUnits="strokeWidth"
        >
            <path class=class d="M 0 0 L 10 4 L 0 8 z"></path>
        </marker>
    }
    .into_any()
}

fn graph_edge_view(edge: RenderedEdge, render_hit_target: bool) -> AnyView {
    let hit_target = render_hit_target.then(|| {
        view! {
            <path
                id=edge.hit_target_id.clone()
                class="edge-hit-target"
                d=edge.path.clone()
                data-anchor-range="true"
                data-edge-kind=edge.kind
                data-source-id=edge.source_id.clone()
                data-target-id=edge.target_id.clone()
                tabindex="0"
                role="button"
                aria-pressed="false"
                aria-label=edge.hit_target_label
            ></path>
        }
    });
    view! {
        <path
            id=edge.id
            data-render-key=edge.key
            class=edge.class
            marker-end=edge.marker
            d=edge.path
            data-source-x=edge.source_x
            data-source-y=edge.source_y
            data-control-1-x=edge.control_1_x
            data-control-1-y=edge.control_1_y
            data-control-2-x=edge.control_2_x
            data-control-2-y=edge.control_2_y
            data-target-x=edge.target_x
            data-target-y=edge.target_y
            data-edge-kind=edge.kind
            data-source-id=edge.source_id
            data-target-id=edge.target_id
        ></path>
        {hit_target}
    }
    .into_any()
}

fn graph_node_view(node: RenderedNode) -> AnyView {
    view! {
        <a
            id=node.id
            data-render-key=node.key
            class="node-link"
            href=format!("#{}", node.target)
            data-node-target=node.target
            data-node-id=node.node_id
            data-base-node-x=node.x
            data-node-x=node.x
            data-node-y=node.y
        >
            <g class=node.class transform=format!("translate({} {})", node.x, node.y)>
                <title>{node.title}</title>
                <circle class="core" r="18"></circle>
                <text class="node-label" y="31">{node.label}</text>
                <text class="node-kind" y="44">{node.kind}</text>
            </g>
        </a>
    }
    .into_any()
}

pub fn edge_style(kind: GraphViewportEdgeKind) -> (&'static str, &'static str) {
    match kind {
        GraphViewportEdgeKind::Primary => ("edge primary-parent", "url(#arrowhead)"),
        GraphViewportEdgeKind::Merge => ("edge merge-parent", "url(#merge-arrowhead)"),
        GraphViewportEdgeKind::Shadow => ("edge shadow-parent", "url(#shadow-arrowhead)"),
    }
}

pub fn edge_kind_label(kind: GraphViewportEdgeKind) -> &'static str {
    match kind {
        GraphViewportEdgeKind::Primary => "Primary parent",
        GraphViewportEdgeKind::Merge => "Merge parent",
        GraphViewportEdgeKind::Shadow => "Shadow parent",
    }
}

pub fn edge_hit_target_label(edge: &GraphViewportEdge) -> String {
    let kind = edge_kind_label(edge.kind).to_lowercase();
    format!(
        "Expand {kind} relationship from {} to {}",
        edge.source_id, edge.target_id
    )
}

pub fn edge_hit_target_id(key: &str) -> String {
    format!("{}-hit", render_element_id(key))
}

pub fn node_group_class(node: &GraphViewportNode) -> String {
    let mut class = format!("node {}", css_token(&node.kind));
    if !node.labels.is_empty() {
        class.push_str(" active");
    }
    class
}

pub fn node_title_text(node: &GraphViewportNode) -> String {
    let labels = if node.labels.is_empty() {
        String::new()
    } else {
        format!(" [{}]", node.labels.join(", "))
    };
    format!(
        "{} · {}{}: {}",
        node.short_id, node.kind, labels, node.summary
    )
}

pub fn node_label(node: &GraphViewportNode) -> String {
    if node.labels.is_empty() {
        node.short_id.clone()
    } else {
        let mut labels = node
            .labels
            .iter()
            .take(2)
            .map(|label| truncate_label(label, 12))
            .collect::<Vec<_>>();
        if node.labels.len() > labels.len() {
            labels.push(format!("+{}", node.labels.len() - labels.len()));
        }
        format!("{} {}", node.short_id, labels.join(" · "))
    }
}

pub fn bezier_path(route: GraphBezierRoute) -> String {
    format!(
        "M {} {} C {} {}, {} {}, {} {}",
        route.source.x,
        route.source.y,
        route.control_1.x,
        route.control_1.y,
        route.control_2.x,
        route.control_2.y,
        route.target.x,
        route.target.y,
    )
}

pub fn render_element_id(key: &str) -> String {
    format!("graph-render-{}", percent_encode(key))
}

fn css_token(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect()
}

pub fn percent_encode(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                vec![byte as char]
            }
            _ => format!("%{byte:02X}").chars().collect(),
        })
        .collect()
}

pub fn truncate_label(label: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    let mut chars = label.chars();
    let prefix = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        let truncated = prefix
            .chars()
            .take(max_chars.saturating_sub(1))
            .collect::<String>();
        format!("{truncated}…")
    } else {
        prefix
    }
}
