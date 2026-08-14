#![cfg_attr(target_arch = "wasm32", allow(dead_code))]

use leptos::prelude::*;

use crate::api::{
    GraphBezierRoute, GraphErrorNode, GraphJob, GraphViewportEdge, GraphViewportEdgeKind,
    GraphViewportNode, GraphViewportResponse,
};

pub const GRAPH_FOCUS_TARGET_QUERY: &str = "graph_focus_target";
pub const GRAPH_FOCUS_X_QUERY: &str = "graph_focus_x";
pub const GRAPH_FOCUS_Y_QUERY: &str = "graph_focus_y";

pub struct GraphCanvasModel {
    canvas_width: i32,
    canvas_height: i32,
    viewport_x: i32,
    viewport_y: i32,
    viewport_width: i32,
    viewport_height: i32,
    render_edge_hit_targets: bool,
    index_links_are_local: bool,
    error_nodes: Vec<GraphErrorNode>,
    active_job_count: usize,
    jobs: Vec<GraphJob>,
    nodes: Vec<RenderedNode>,
    edges: Vec<RenderedEdge>,
}

struct RenderedNode {
    id: String,
    key: String,
    target: String,
    href: String,
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
    class: String,
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
    pub fn new(
        render_edge_hit_targets: bool,
        index_links_are_local: bool,
        response: &GraphViewportResponse,
    ) -> Self {
        Self {
            canvas_width: response.canvas.width,
            canvas_height: response.canvas.height,
            viewport_x: response.viewport.x,
            viewport_y: response.viewport.y,
            viewport_width: response.viewport.width,
            viewport_height: response.viewport.height,
            render_edge_hit_targets,
            index_links_are_local,
            error_nodes: response.error_nodes.clone(),
            active_job_count: response.active_job_count,
            jobs: response.jobs.clone(),
            nodes: response.nodes.iter().map(RenderedNode::new).collect(),
            edges: response.edges.iter().map(RenderedEdge::new).collect(),
        }
    }
}

impl RenderedNode {
    fn new(node: &GraphViewportNode) -> Self {
        let target = node.node_target.clone();
        Self {
            id: render_element_id(&node.key),
            key: node.key.clone(),
            href: node.href.clone().unwrap_or_else(|| format!("#{target}")),
            target,
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
        let (base_class, marker) = edge_style(edge.kind);
        Self {
            id: render_element_id(&edge.key),
            hit_target_id: edge_hit_target_id(&edge.key),
            key: edge.key.clone(),
            class: edge_class(edge, base_class),
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
        index_links_are_local,
        error_nodes,
        active_job_count,
        jobs,
        nodes,
        edges,
    } = graph;
    let edges = edges
        .into_iter()
        .map(|edge| graph_edge_view(edge, render_edge_hit_targets))
        .collect_view();
    let nodes = nodes.into_iter().map(graph_node_view).collect_view();
    let error_count = error_nodes.len();
    let error_nodes = error_nodes
        .into_iter()
        .map(|node| view! { <ErrorNodeItem node local=index_links_are_local/> })
        .collect_view();
    let job_count = jobs.len();
    let jobs = jobs
        .into_iter()
        .map(|job| view! { <JobItem job all_view=index_links_are_local/> })
        .collect_view();

    view! {
        <div class="graph-controls">
            <details class="graph-index-popover job-popover">
                <summary aria-label=format!("Jobs ({active_job_count} active)")>
                    <span>"Jobs"</span>
                    <span class="graph-index-count job-count">{active_job_count}</span>
                </summary>
                <div class="graph-index-menu job-menu" role="list" aria-label="Jobs">
                    <p class="graph-index-empty job-empty" hidden=job_count != 0>"No jobs"</p>
                    <div class="graph-index-list job-list">{jobs}</div>
                </div>
            </details>
            <details class="graph-index-popover error-node-popover">
                <summary aria-label=format!("Error Nodes ({error_count})")>
                    <span>"Error Nodes"</span>
                    <span class="graph-index-count error-node-count">{error_count}</span>
                </summary>
                <div class="graph-index-menu error-node-menu" role="list" aria-label="Error Nodes">
                    <p class="graph-index-empty error-node-empty" hidden=error_count != 0>"No error nodes"</p>
                    <div class="graph-index-list error-node-list">{error_nodes}</div>
                </div>
            </details>
            <button
                class="follow-toggle"
                type="button"
                aria-pressed="false"
                title="Keep the graph pinned to the top-right edge"
            >
                "Follow"
            </button>
        </div>
        <div
            class="graph-wrap virtual-graph"
            tabindex="0"
            data-viewport-x=viewport_x
            data-viewport-y=viewport_y
            data-canvas-width=canvas_width
            data-zoom="1"
        >
            <svg
                class="graph"
                role="img"
                aria-label="CoCo node graph"
                viewBox=format!("{viewport_x} {viewport_y} {viewport_width} {viewport_height}")
                preserveAspectRatio="xMaxYMin meet"
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
fn ErrorNodeItem(node: GraphErrorNode, local: bool) -> impl IntoView {
    let href = error_node_href(&node, local);
    let created_at = node.created_at;
    let datetime = created_at.clone();
    view! {
        <a
            class="graph-index-link error-node-link"
            href=href
            data-node-x=node.point.x
            data-node-y=node.point.y
            role="listitem"
        >
            <span class="graph-index-link-head error-node-link-head">
                <strong>{node.short_id}</strong>
                <time datetime=datetime>{created_at}</time>
            </span>
            <span class="graph-index-summary error-node-summary">{node.summary}</span>
        </a>
    }
}

#[component]
fn JobItem(job: GraphJob, all_view: bool) -> impl IntoView {
    let href = job_href(&job, all_view);
    let (_, point) = job_focus(&job, all_view);
    let head_href = job_destination_href(&job, all_view, JobDestination::Head);
    let (_, head_point) = job_destination_focus(&job, JobDestination::Head);
    let anchor_href = job_destination_href(&job, all_view, JobDestination::HeadAnchor);
    let (_, anchor_point) = job_destination_focus(&job, JobDestination::HeadAnchor);
    let head_label = format!("Jump job {} to head in All view", job.short_id);
    let anchor_label = format!("Jump job {} to head anchor in Anchors view", job.short_id);
    let created_at = job.created_at;
    let datetime = created_at.clone();
    let status_class = format!("job-status {}", job.status);
    let branch = if job.branch == job.work_branch {
        job.branch
    } else {
        format!("{} → {}", job.branch, job.work_branch)
    };
    view! {
        <div class="graph-index-item job-item" role="listitem">
            <a
                class="graph-index-link job-link"
                href=href
                data-node-x=point.x
                data-node-y=point.y
            >
                <span class="graph-index-link-head job-link-head">
                    <strong>{job.short_id}</strong>
                    <time datetime=datetime>{created_at}</time>
                </span>
                <span class="job-summary">
                    <span class=status_class>{job.status}</span>
                    <span class="job-branch">{branch}</span>
                    <span class="job-head">{format!("head {}", job.head_short_id)}</span>
                </span>
            </a>
            <nav class="job-destination-actions" aria-label="Jump destination">
                <a
                    class="job-destination-button job-destination-head"
                    href=head_href
                    data-node-x=head_point.x
                    data-node-y=head_point.y
                    aria-label=head_label
                >
                    "Head"
                </a>
                <a
                    class="job-destination-button job-destination-anchor"
                    href=anchor_href
                    data-node-x=anchor_point.x
                    data-node-y=anchor_point.y
                    aria-label=anchor_label
                >
                    "Head anchor"
                </a>
            </nav>
        </div>
    }
}

pub fn error_node_href(node: &GraphErrorNode, local: bool) -> String {
    graph_point_href(&node.node_target, node.point, local, "all")
}

pub fn job_href(job: &GraphJob, all_view: bool) -> String {
    let (target, _) = job_focus(job, all_view);
    format!("#{target}")
}

pub fn job_focus(job: &GraphJob, all_view: bool) -> (&str, crate::api::Point) {
    let destination = if all_view {
        JobDestination::Head
    } else {
        JobDestination::HeadAnchor
    };
    job_destination_focus(job, destination)
}

#[derive(Clone, Copy)]
pub enum JobDestination {
    Head,
    HeadAnchor,
}

pub fn job_destination_href(job: &GraphJob, all_view: bool, destination: JobDestination) -> String {
    let (target, point) = job_destination_focus(job, destination);
    let (local, mode) = match destination {
        JobDestination::Head => (all_view, "all"),
        JobDestination::HeadAnchor => (!all_view, "anchors"),
    };
    graph_point_href(target, point, local, mode)
}

pub fn job_destination_focus(
    job: &GraphJob,
    destination: JobDestination,
) -> (&str, crate::api::Point) {
    match destination {
        JobDestination::Head => (&job.head_target, job.point),
        JobDestination::HeadAnchor => (&job.head_anchor_target, job.head_anchor_point),
    }
}

pub fn graph_point_href(target: &str, point: crate::api::Point, local: bool, mode: &str) -> String {
    if local {
        return format!("#{target}");
    }
    format!(
        "/?mode={mode}&{GRAPH_FOCUS_TARGET_QUERY}={}&{GRAPH_FOCUS_X_QUERY}={}&{GRAPH_FOCUS_Y_QUERY}={}#{}",
        percent_encode(target),
        point.x,
        point.y,
        target,
    )
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
            href=node.href
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
    if let Some(origin) = node.origin.as_ref() {
        class.push_str(&format!(
            " origin origin-{}",
            origin_style_index(&origin.branch_instance_id)
        ));
    }
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
    let origin = node.origin.as_ref().map_or_else(
        || "unknown origin".to_owned(),
        |origin| {
            format!(
                "origin {} ({})",
                origin.branch_name,
                truncate_label(&origin.branch_instance_id, 16)
            )
        },
    );
    format!(
        "{} · {}{} · {}: {}",
        node.short_id, node.kind, labels, origin, node.summary
    )
}

fn edge_class(edge: &GraphViewportEdge, base: &str) -> String {
    match (edge.kind, edge.origin.as_ref()) {
        (GraphViewportEdgeKind::Primary, Some(origin)) => format!(
            "{base} origin origin-{}",
            origin_style_index(&origin.branch_instance_id)
        ),
        _ => base.to_owned(),
    }
}

pub fn origin_style_index(instance_id: &str) -> u8 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in instance_id.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash % 8) as u8
}

pub fn node_label(node: &GraphViewportNode) -> String {
    if node.labels.is_empty() {
        node.short_id.clone()
    } else {
        let labels = node
            .labels
            .iter()
            .map(|label| truncate_label(label, 12))
            .collect::<Vec<_>>();
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
