use std::collections::HashMap;

use leptos::prelude::*;

use crate::graph::{GraphNode, GraphSnapshot, css_token};
use crate::layout::{
    GraphLane, GraphLayout, GraphLayoutEdge, GraphLayoutEdgeKind, GraphNodeOccurrence, RenderPoint,
    layout_graph, line_points, line_render_points, routed_elbow_points, routed_elbow_render_points,
};

const GRAPH_CULL_EDGE_MARGIN: i32 = 180;

pub fn render_index_page(snapshot: &GraphSnapshot) -> String {
    render_snapshot_document(snapshot, true)
}

#[cfg(test)]
pub fn render_snapshot_page(snapshot: &GraphSnapshot) -> String {
    render_snapshot_document(snapshot, false)
}

pub fn render_fragment(snapshot: &GraphSnapshot) -> String {
    view! { <ConsoleRoot snapshot=snapshot /> }.to_html()
}

#[derive(Debug, Clone, Copy)]
pub struct GraphViewport {
    pub left: f64,
    pub top: f64,
    pub right: f64,
    pub bottom: f64,
}

pub fn render_graph_items(snapshot: &GraphSnapshot, viewport: GraphViewport) -> String {
    let layout = layout_graph(snapshot);
    view! { <GraphItems snapshot=snapshot layout=&layout viewport=Some(viewport) /> }.to_html()
}

fn render_snapshot_document(snapshot: &GraphSnapshot, include_client: bool) -> String {
    let rendered = view! { <ConsoleDocument snapshot=snapshot include_client=include_client /> };

    format!("<!doctype html>{}", rendered.to_html())
}

#[component]
fn ConsoleDocument<'a>(snapshot: &'a GraphSnapshot, include_client: bool) -> impl IntoView {
    let client_script = include_client
        .then(|| view! { <script type="module" src="/pkg/coco_console.js"></script> }.into_any())
        .into_iter()
        .collect::<Vec<_>>();

    view! {
        <html lang="en">
            <head>
                <meta charset="utf-8" />
                <meta name="viewport" content="width=device-width, initial-scale=1" />
                <title>"CoCo Console"</title>
                <link rel="stylesheet" href="/style.css" />
                {client_script}
            </head>
            <body>
                <ConsoleRoot snapshot=snapshot />
            </body>
        </html>
    }
}

#[component]
fn ConsoleRoot<'a>(snapshot: &'a GraphSnapshot) -> impl IntoView {
    let stats = format!(
        "{} nodes / {} edges / version {}",
        snapshot.nodes.len(),
        snapshot.edges.len(),
        snapshot.version
    );
    let version = snapshot.version.to_string();

    view! {
        <main id="console-root" class="shell" data-version=version>
            <header class="topbar">
                <section class="brand">
                    <h1>"CoCo Console"</h1>
                    <p>"Live node relationship graph from the daemon store."</p>
                </section>
                <p class="stats">{stats}</p>
            </header>
            <ConsoleContent snapshot=snapshot />
        </main>
    }
}

#[component]
fn ConsoleContent<'a>(snapshot: &'a GraphSnapshot) -> impl IntoView {
    let layout = (!snapshot.nodes.is_empty()).then(|| layout_graph(snapshot));
    view! {
        <section class="content">
            <EntityNav snapshot=snapshot />
            <main class="entity-workspace">
                <section id="nodes" class="entity-section graph-entity">
                    <EntitySectionHeader title="Nodes" count=snapshot.nodes.len() />
                    <GraphPanel layout=layout.as_ref() />
                </section>
                <LazyEntitySection id="branches" title="Branches" kind="branches" count=snapshot.entity_counts.branches />
                <LazyEntitySection id="sessions" title="Sessions" kind="sessions" count=snapshot.entity_counts.sessions />
                <LazyEntitySection id="presets" title="Presets" kind="presets" count=snapshot.entity_counts.presets />
                <LazyEntitySection id="skills" title="Skills" kind="skills" count=snapshot.entity_counts.skills />
                <LazyEntitySection id="jobs" title="Jobs" kind="jobs" count=snapshot.entity_counts.jobs />
                <LazyEntitySection id="queues" title="Queues" kind="queues" count=snapshot.entity_counts.queues />
            </main>
            <SidePanel snapshot=snapshot />
        </section>
    }
}

#[component]
fn EntityNav<'a>(snapshot: &'a GraphSnapshot) -> impl IntoView {
    view! {
        <nav class="entity-nav" aria-label="Store entities">
            <EntityNavLink href="#nodes" icon="N" label="Nodes" count=snapshot.nodes.len() />
            <EntityNavLink href="#branches" icon="B" label="Branches" count=snapshot.entity_counts.branches />
            <EntityNavLink href="#sessions" icon="S" label="Sessions" count=snapshot.entity_counts.sessions />
            <EntityNavLink href="#presets" icon="P" label="Presets" count=snapshot.entity_counts.presets />
            <EntityNavLink href="#skills" icon="K" label="Skills" count=snapshot.entity_counts.skills />
            <EntityNavLink href="#jobs" icon="J" label="Jobs" count=snapshot.entity_counts.jobs />
            <EntityNavLink href="#queues" icon="Q" label="Queues" count=snapshot.entity_counts.queues />
        </nav>
    }
}

#[component]
fn EntityNavLink(
    href: &'static str,
    icon: &'static str,
    label: &'static str,
    count: usize,
) -> impl IntoView {
    let count = count.to_string();

    view! {
        <a class="entity-nav-link" href=href aria-label=label title=label>
            <span class="entity-nav-icon">{icon}</span>
            <span class="entity-nav-count">{count}</span>
        </a>
    }
}

#[component]
fn EntitySectionHeader(title: &'static str, count: usize) -> impl IntoView {
    view! {
        <header class="entity-section-header">
            <h2>{title}</h2>
            <span>{count.to_string()}</span>
        </header>
    }
}

#[component]
fn LazyEntitySection(
    id: &'static str,
    title: &'static str,
    kind: &'static str,
    count: usize,
) -> impl IntoView {
    view! {
        <section id=id class="entity-section lazy-entity-section" data-entity-kind=kind>
            <EntitySectionHeader title=title count=count />
            <div class="entity-section-body" data-entity-kind=kind data-entity-state="idle">
                <p class="entity-placeholder">"Select this section to load records."</p>
            </div>
        </section>
    }
}

#[component]
fn GraphPanel<'a>(layout: Option<&'a GraphLayout>) -> AnyView {
    let Some(layout) = layout else {
        return view! {
            <div class="graph-shell">
                <div class="graph-wrap">
                    <div class="empty">"No sessions found."</div>
                </div>
            </div>
        }
        .into_any();
    };

    view! {
        <div class="graph-shell">
            <GraphToolbar />
            <div class="graph-wrap">
                <GraphSvg layout=layout />
            </div>
            <Minimap layout=layout />
        </div>
    }
    .into_any()
}

#[component]
fn GraphToolbar() -> impl IntoView {
    view! {
        <div class="graph-toolbar" aria-label="Graph controls">
            <button class="graph-control" type="button" data-zoom-action="out" aria-label="Zoom out">"-"</button>
            <button class="graph-control graph-scale" type="button" data-zoom-action="reset" aria-label="Reset zoom">"100%"</button>
            <button class="graph-control" type="button" data-zoom-action="in" aria-label="Zoom in">"+"</button>
        </div>
    }
}

#[component]
fn GraphSvg<'a>(layout: &'a GraphLayout) -> impl IntoView {
    let lanes = layout
        .lanes
        .iter()
        .map(|lane| view! { <GraphLaneLabel lane=lane /> })
        .collect::<Vec<_>>();
    let view_box = format!("0 0 {} {}", layout.width, layout.height);
    let width = layout.width.to_string();
    let height = layout.height.to_string();

    view! {
        <svg
            class="graph"
            role="img"
            aria-label="CoCo node graph"
            viewBox=view_box
            width=width.clone()
            height=height.clone()
            data-graph-width=width.clone()
            data-graph-height=height.clone()
            data-zoom="1"
        >
            <GraphMarkers />
            <rect class="graph-bg" width=width.clone() height=height.clone() />
            {lanes}
            <g class="graph-items" data-graph-items-key=""></g>
        </svg>
    }
}

#[component]
fn GraphItems<'a>(
    snapshot: &'a GraphSnapshot,
    layout: &'a GraphLayout,
    viewport: Option<GraphViewport>,
) -> impl IntoView {
    let nodes_by_id = snapshot
        .nodes
        .iter()
        .map(|node| (node.id.as_str(), node))
        .collect::<HashMap<_, _>>();
    let edges = graph_edges(layout)
        .filter(|edge| viewport.is_none_or(|viewport| edge_intersects_viewport(edge, viewport)))
        .map(|edge| view! { <GraphEdgeView edge=edge /> })
        .collect::<Vec<_>>();
    let nodes = layout
        .occurrences
        .iter()
        .filter(|occurrence| {
            viewport.is_none_or(|viewport| point_intersects_viewport(occurrence.point, viewport))
        })
        .filter_map(|occurrence| {
            let node = nodes_by_id.get(occurrence.node_id.as_str())?;
            Some(view! { <GraphNodeView node=*node occurrence=occurrence /> })
        })
        .collect::<Vec<_>>();

    view! { <g class="graph-items-fragment">{edges}{nodes}</g> }
}

#[component]
fn GraphMarkers() -> impl IntoView {
    view! {
        <defs>
            <marker id="arrowhead" markerWidth="10" markerHeight="8" refX="9" refY="4" orient="auto" markerUnits="strokeWidth">
                <path class="arrowhead" d="M 0 0 L 10 4 L 0 8 z" />
            </marker>
            <marker id="merge-arrowhead" markerWidth="10" markerHeight="8" refX="9" refY="4" orient="auto" markerUnits="strokeWidth">
                <path class="merge-arrowhead" d="M 0 0 L 10 4 L 0 8 z" />
            </marker>
            <marker id="fork-arrowhead" markerWidth="10" markerHeight="8" refX="9" refY="4" orient="auto" markerUnits="strokeWidth">
                <path class="fork-arrowhead" d="M 0 0 L 10 4 L 0 8 z" />
            </marker>
        </defs>
    }
}

#[component]
fn GraphLaneLabel<'a>(lane: &'a GraphLane) -> impl IntoView {
    let y = lane.y.to_string();
    let label = lane.label.clone();

    view! { <text class="lane-label" x="64" y=y>{label}</text> }
}

#[component]
fn GraphEdgeView<'a>(edge: &'a GraphLayoutEdge) -> AnyView {
    let marker = edge_marker(edge.kind);

    match edge.kind {
        GraphLayoutEdgeKind::PrimaryParent => {
            let (x1, y1, x2, y2) = line_points(edge.source, edge.target, edge.target_port_offset);
            let bounds = edge_bounds(line_render_points(
                edge.source,
                edge.target,
                edge.target_port_offset,
            ));
            view! {
                <line
                    class="edge primary-parent graph-item"
                    marker-end=marker
                    x1=x1
                    y1=y1
                    x2=x2
                    y2=y2
                    data-graph-min-x=bounds.min_x
                    data-graph-min-y=bounds.min_y
                    data-graph-max-x=bounds.max_x
                    data-graph-max-y=bounds.max_y
                />
            }
            .into_any()
        }
        GraphLayoutEdgeKind::Fork | GraphLayoutEdgeKind::MergeParent => {
            let points = routed_elbow_points(
                edge.source,
                edge.target,
                edge.route_slot,
                edge.target_port_offset,
            );
            let bounds = edge_bounds(routed_elbow_render_points(
                edge.source,
                edge.target,
                edge.route_slot,
                edge.target_port_offset,
            ));
            let class = match edge.kind {
                GraphLayoutEdgeKind::Fork => "edge fork graph-item",
                GraphLayoutEdgeKind::MergeParent => "edge merge-parent graph-item",
                GraphLayoutEdgeKind::PrimaryParent => unreachable!(),
            };
            view! {
                <polyline
                    class=class
                    marker-end=marker
                    points=points
                    data-graph-min-x=bounds.min_x
                    data-graph-min-y=bounds.min_y
                    data-graph-max-x=bounds.max_x
                    data-graph-max-y=bounds.max_y
                />
            }
            .into_any()
        }
    }
}

#[component]
fn GraphNodeView<'a>(node: &'a GraphNode, occurrence: &'a GraphNodeOccurrence) -> impl IntoView {
    let labels = if node.labels.is_empty() {
        String::new()
    } else {
        format!(" {}", node.labels.join(", "))
    };
    let class = if node.labels.is_empty() {
        format!("node {}", css_token(&node.kind))
    } else {
        format!("node {} active", css_token(&node.kind))
    };
    let transform = format!("translate({} {})", occurrence.point.x, occurrence.point.y);
    let title = format!("{}: {}", node.short_id, node.summary);
    let label = format!("{}{}", node.short_id, labels);
    let kind = node.kind.clone();
    let href = format!("#{}", occurrence.node_target);
    let node_id = node.id.clone();
    let node_target = occurrence.node_target.clone();
    let graph_x = occurrence.point.x.to_string();
    let graph_y = occurrence.point.y.to_string();

    view! {
        <a
            class="node-link graph-item"
            href=href
            data-node-target=node_target
            data-node-id=node_id
            data-graph-x=graph_x
            data-graph-y=graph_y
        >
            <g class=class transform=transform>
                <title>{title}</title>
                <circle class="core" r="26" />
                <text class="node-label" y="44">{label}</text>
                <text class="node-kind" y="58">{kind}</text>
            </g>
        </a>
    }
}

#[component]
fn Minimap<'a>(layout: &'a GraphLayout) -> impl IntoView {
    let view_box = format!("0 0 {} {}", layout.width, layout.height);
    let graph_width = layout.width.to_string();
    let graph_height = layout.height.to_string();

    view! {
        <svg
            class="minimap"
            role="img"
            aria-label="Graph minimap"
            viewBox=view_box
            preserveAspectRatio="xMidYMid meet"
            data-graph-width=graph_width
            data-graph-height=graph_height
        >
            <rect class="minimap-bg" x="0" y="0" width=layout.width.to_string() height=layout.height.to_string() />
            <rect class="minimap-viewport" x="0" y="0" width="0" height="0" rx="18" />
        </svg>
    }
}

#[component]
fn SidePanel<'a>(snapshot: &'a GraphSnapshot) -> impl IntoView {
    view! {
        <aside class="side">
            <NodeDetailsDefault />
            <NodeDetailPanel />
            <StoreSummary snapshot=snapshot />
        </aside>
    }
}

#[component]
fn NodeDetailsDefault() -> impl IntoView {
    view! {
        <section class="node-details node-details-default">
            <h2>"Node"</h2>
            <dl class="detail-list">
                <div>
                    <dt>"Selection"</dt>
                    <dd>"Select a node to inspect its content."</dd>
                </div>
            </dl>
        </section>
    }
}

#[component]
fn NodeDetailPanel() -> impl IntoView {
    view! {
        <section class="node-details node-detail-panel" hidden="true">
            <h2>"Node"</h2>
            <dl class="detail-list">
                <DetailField label="Id" field="id" />
                <DetailField label="Kind" field="kind" />
                <DetailField label="Role" field="role" />
                <DetailField label="Created" field="created_at" />
                <DetailField label="Labels" field="labels" />
                <DetailField label="Content" field="content" />
            </dl>
        </section>
    }
}

#[component]
fn DetailField(label: &'static str, field: &'static str) -> impl IntoView {
    view! {
        <div>
            <dt>{label}</dt>
            <dd data-node-field=field></dd>
        </div>
    }
}

#[component]
fn StoreSummary<'a>(snapshot: &'a GraphSnapshot) -> impl IntoView {
    let counts = &snapshot.entity_counts;
    view! {
        <section class="store-summary">
            <h2>"Store"</h2>
            <dl class="summary-grid">
                <SummaryMetric label="Nodes" value=counts.nodes />
                <SummaryMetric label="Branches" value=counts.branches />
                <SummaryMetric label="Presets" value=counts.presets />
                <SummaryMetric label="Skills" value=counts.skills />
                <SummaryMetric label="Jobs" value=counts.jobs />
                <SummaryMetric label="Queues" value=counts.queues />
            </dl>
        </section>
    }
}

#[component]
fn SummaryMetric(label: &'static str, value: usize) -> impl IntoView {
    view! {
        <div>
            <dt>{label}</dt>
            <dd>{value.to_string()}</dd>
        </div>
    }
}

#[derive(Debug, Clone)]
struct GraphItemBounds {
    min_x: String,
    min_y: String,
    max_x: String,
    max_y: String,
}

fn graph_edges(layout: &GraphLayout) -> impl Iterator<Item = &GraphLayoutEdge> {
    layout
        .primary_edges
        .iter()
        .chain(layout.fork_edges.iter())
        .chain(layout.merge_edges.iter())
}

fn edge_marker(kind: GraphLayoutEdgeKind) -> &'static str {
    match kind {
        GraphLayoutEdgeKind::PrimaryParent => "url(#arrowhead)",
        GraphLayoutEdgeKind::Fork => "url(#fork-arrowhead)",
        GraphLayoutEdgeKind::MergeParent => "url(#merge-arrowhead)",
    }
}

fn edge_bounds(points: impl IntoIterator<Item = RenderPoint>) -> GraphItemBounds {
    let bounds = edge_render_bounds(points);

    GraphItemBounds {
        min_x: format!("{:.1}", bounds.min_x),
        min_y: format!("{:.1}", bounds.min_y),
        max_x: format!("{:.1}", bounds.max_x),
        max_y: format!("{:.1}", bounds.max_y),
    }
}

#[derive(Debug, Clone, Copy)]
struct GraphRenderBounds {
    min_x: f64,
    min_y: f64,
    max_x: f64,
    max_y: f64,
}

fn edge_render_bounds(points: impl IntoIterator<Item = RenderPoint>) -> GraphRenderBounds {
    let mut points = points.into_iter();
    let Some(first) = points.next() else {
        return GraphRenderBounds {
            min_x: 0.0,
            min_y: 0.0,
            max_x: 0.0,
            max_y: 0.0,
        };
    };
    let (min_x, min_y, max_x, max_y) = points.fold(
        (first.x, first.y, first.x, first.y),
        |(min_x, min_y, max_x, max_y), point| {
            (
                min_x.min(point.x),
                min_y.min(point.y),
                max_x.max(point.x),
                max_y.max(point.y),
            )
        },
    );

    GraphRenderBounds {
        min_x: min_x - f64::from(GRAPH_CULL_EDGE_MARGIN),
        min_y: min_y - f64::from(GRAPH_CULL_EDGE_MARGIN),
        max_x: max_x + f64::from(GRAPH_CULL_EDGE_MARGIN),
        max_y: max_y + f64::from(GRAPH_CULL_EDGE_MARGIN),
    }
}

fn edge_intersects_viewport(edge: &GraphLayoutEdge, viewport: GraphViewport) -> bool {
    let bounds = match edge.kind {
        GraphLayoutEdgeKind::PrimaryParent => edge_render_bounds(line_render_points(
            edge.source,
            edge.target,
            edge.target_port_offset,
        )),
        GraphLayoutEdgeKind::Fork | GraphLayoutEdgeKind::MergeParent => {
            edge_render_bounds(routed_elbow_render_points(
                edge.source,
                edge.target,
                edge.route_slot,
                edge.target_port_offset,
            ))
        }
    };

    bounds.max_x >= viewport.left
        && bounds.min_x <= viewport.right
        && bounds.max_y >= viewport.top
        && bounds.min_y <= viewport.bottom
}

fn point_intersects_viewport(point: crate::layout::Point, viewport: GraphViewport) -> bool {
    let x = f64::from(point.x);
    let y = f64::from(point.y);

    x >= viewport.left && x <= viewport.right && y >= viewport.top && y <= viewport.bottom
}
