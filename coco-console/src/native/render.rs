use std::collections::HashMap;
use std::fmt::Write as _;

use askama::Template as _;
use coco_mem::{PauseReason, SessionState};
use leptos::{html::HtmlElement, prelude::*};

use crate::graph::{GraphNode, GraphSnapshot, css_token, node_target_id, shorten_id};
use crate::layout::{
    GraphLayout, GraphLayoutEdge, GraphLayoutEdgeKind, GraphNodeOccurrence, layout_graph,
    line_points, routed_elbow_points,
};

const GRAPH_MARKERS_SVG: &str = include_str!("templates/graph_markers.svg");

#[derive(askama::Template)]
#[template(path = "native/node.svg", escape = "html")]
struct NodeTemplate<'a> {
    href: &'a str,
    node_target: &'a str,
    node_id: &'a str,
    class: &'a str,
    x: i32,
    y: i32,
    title: &'a str,
    label: &'a str,
    kind: &'a str,
}

#[derive(askama::Template)]
#[template(path = "native/primary_edge.svg", escape = "html")]
struct PrimaryEdgeTemplate<'a> {
    class: &'a str,
    marker_end: Option<&'a str>,
    x1: &'a str,
    y1: &'a str,
    x2: &'a str,
    y2: &'a str,
}

#[derive(askama::Template)]
#[template(path = "native/routed_edge.svg", escape = "html")]
struct RoutedEdgeTemplate<'a> {
    class: &'a str,
    marker_end: Option<&'a str>,
    points: &'a str,
}

pub fn render_index_page(snapshot: &GraphSnapshot) -> String {
    render_snapshot_document(snapshot, true)
}

#[cfg(test)]
pub fn render_snapshot_page(snapshot: &GraphSnapshot) -> String {
    render_snapshot_document(snapshot, false)
}

pub fn render_fragment(snapshot: &GraphSnapshot) -> String {
    render_root(snapshot).to_html()
}

fn render_snapshot_document(snapshot: &GraphSnapshot, include_client: bool) -> String {
    let root = render_root(snapshot);
    let client_script = include_client
        .then(|| view! { <script type="module" src="/pkg/coco_console.js"></script> }.into_any())
        .into_iter()
        .collect::<Vec<_>>();
    let rendered: View<HtmlElement<_, _, _>> = view! {
        <html lang="en">
            <head>
                <meta charset="utf-8" />
                <meta name="viewport" content="width=device-width, initial-scale=1" />
                <title>"CoCo Console"</title>
                <link rel="stylesheet" href="/style.css" />
                {client_script}
            </head>
            <body>
                {root}
            </body>
        </html>
    };

    format!("<!doctype html>{}", rendered.to_html())
}

fn render_root(snapshot: &GraphSnapshot) -> AnyView {
    let stats = format!(
        "{} nodes / {} edges / version {}",
        snapshot.nodes.len(),
        snapshot.edges.len(),
        snapshot.version
    );
    let selection_style = render_selection_style(snapshot);
    let content = render_content(snapshot);
    let version = snapshot.version.to_string();

    view! {
        <main id="console-root" class="shell" data-version=version>
            <style id="selection-style">{selection_style}</style>
            <header class="topbar">
                <section class="brand">
                    <h1>"CoCo Console"</h1>
                    <p>"Live node relationship graph from the daemon store."</p>
                </section>
                <p class="stats">{stats}</p>
            </header>
            {content}
        </main>
    }
    .into_any()
}

fn render_content(snapshot: &GraphSnapshot) -> AnyView {
    if snapshot.nodes.is_empty() {
        return view! {
            <section class="content">
                <div class="graph-shell">
                    <div class="graph-wrap">
                        <div class="empty">"No sessions found."</div>
                    </div>
                </div>
                {render_side(snapshot)}
            </section>
        }
        .into_any();
    }

    let layout = layout_graph(snapshot);
    let graph_shell = render_graph_shell(snapshot, &layout);
    let side = render_side(snapshot);

    view! {
        <section class="content">
            <div class="graph-shell" inner_html=graph_shell></div>
            {side}
        </section>
    }
    .into_any()
}

fn render_graph_shell(snapshot: &GraphSnapshot, layout: &GraphLayout) -> String {
    let mut html = String::with_capacity(estimate_graph_shell_capacity(snapshot, layout));
    html.push_str("<div class=\"graph-wrap\">");
    render_graph_html(snapshot, layout, &mut html);
    html.push_str("</div>");
    render_minimap_html(layout, &mut html);
    html
}

fn render_graph_html(snapshot: &GraphSnapshot, layout: &GraphLayout, html: &mut String) {
    let nodes_by_id = snapshot
        .nodes
        .iter()
        .map(|node| (node.id.as_str(), node))
        .collect::<HashMap<_, _>>();

    write!(
        html,
        "<svg class=\"graph\" role=\"img\" aria-label=\"CoCo node graph\" viewBox=\"0 0 {} {}\" width=\"{}\" height=\"{}\">",
        layout.width, layout.height, layout.width, layout.height
    )
    .expect("writing to string should not fail");
    html.push_str(GRAPH_MARKERS_SVG);
    write!(
        html,
        "<rect class=\"graph-bg\" width=\"{}\" height=\"{}\"></rect>",
        layout.width, layout.height
    )
    .expect("writing to string should not fail");

    for lane in &layout.lanes {
        write!(
            html,
            "<text class=\"lane-label\" x=\"64\" y=\"{}\">",
            lane.y
        )
        .expect("writing to string should not fail");
        push_escaped_text(html, &lane.label);
        html.push_str("</text>");
    }
    for edge in layout
        .primary_edges
        .iter()
        .chain(layout.fork_edges.iter())
        .chain(layout.merge_edges.iter())
    {
        render_graph_edge_html(edge, html);
    }
    for occurrence in &layout.occurrences {
        let Some(node) = nodes_by_id.get(occurrence.node_id.as_str()) else {
            continue;
        };
        render_graph_node_html(node, occurrence, html);
    }

    html.push_str("</svg>");
}

fn render_graph_edge_html(edge: &GraphLayoutEdge, html: &mut String) {
    match edge.kind {
        GraphLayoutEdgeKind::PrimaryParent => render_primary_edge_path_html(
            edge,
            "edge primary-parent",
            Some("url(#arrowhead)"),
            html,
        ),
        GraphLayoutEdgeKind::Fork => {
            render_routed_edge_path_html(edge, "edge fork", Some("url(#fork-arrowhead)"), html)
        }
        GraphLayoutEdgeKind::MergeParent => render_routed_edge_path_html(
            edge,
            "edge merge-parent",
            Some("url(#merge-arrowhead)"),
            html,
        ),
    }
}

fn render_graph_node_html(node: &GraphNode, occurrence: &GraphNodeOccurrence, html: &mut String) {
    let class = if node.labels.is_empty() {
        format!("node {}", css_token(&node.kind))
    } else {
        format!("node {} active", css_token(&node.kind))
    };
    let title = format!("{}: {}", node.short_id, node.summary);
    let href = format!("#{}", occurrence.node_target);
    let mut label = node.short_id.clone();
    if !node.labels.is_empty() {
        label.push(' ');
        label.push_str(&node.labels.join(", "));
    }

    NodeTemplate {
        href: &href,
        node_target: &occurrence.node_target,
        node_id: &node.id,
        class: &class,
        x: occurrence.point.x,
        y: occurrence.point.y,
        title: &title,
        label: &label,
        kind: &node.kind,
    }
    .render_into(html)
    .expect("writing to string should not fail");
}

fn render_minimap_html(layout: &GraphLayout, html: &mut String) {
    write!(
        html,
        "<svg class=\"minimap\" role=\"img\" aria-label=\"Graph minimap\" viewBox=\"0 0 {} {}\" preserveAspectRatio=\"xMidYMid meet\" data-graph-width=\"{}\" data-graph-height=\"{}\">",
        layout.width, layout.height, layout.width, layout.height
    )
    .expect("writing to string should not fail");
    write!(
        html,
        "<rect class=\"minimap-bg\" x=\"0\" y=\"0\" width=\"{}\" height=\"{}\"></rect>",
        layout.width, layout.height
    )
    .expect("writing to string should not fail");
    for edge in layout
        .primary_edges
        .iter()
        .chain(layout.fork_edges.iter())
        .chain(layout.merge_edges.iter())
    {
        render_minimap_edge_html(edge, html);
    }
    for occurrence in &layout.occurrences {
        write!(
            html,
            "<circle class=\"minimap-node\" cx=\"{}\" cy=\"{}\" r=\"26\"></circle>",
            occurrence.point.x, occurrence.point.y
        )
        .expect("writing to string should not fail");
    }
    html.push_str(
        "<rect class=\"minimap-viewport\" x=\"0\" y=\"0\" width=\"0\" height=\"0\" rx=\"18\"></rect></svg>",
    );
}

fn render_minimap_edge_html(edge: &GraphLayoutEdge, html: &mut String) {
    match edge.kind {
        GraphLayoutEdgeKind::PrimaryParent => {
            render_primary_edge_path_html(edge, "minimap-edge primary-parent", None, html)
        }
        GraphLayoutEdgeKind::Fork => {
            render_routed_edge_path_html(edge, "minimap-edge fork", None, html)
        }
        GraphLayoutEdgeKind::MergeParent => {
            render_routed_edge_path_html(edge, "minimap-edge merge-parent", None, html)
        }
    }
}

fn render_primary_edge_path_html(
    edge: &GraphLayoutEdge,
    class: &str,
    marker_end: Option<&str>,
    html: &mut String,
) {
    let (x1, y1, x2, y2) = line_points(edge.source, edge.target, edge.target_port_offset);
    PrimaryEdgeTemplate {
        class,
        marker_end,
        x1: &x1,
        y1: &y1,
        x2: &x2,
        y2: &y2,
    }
    .render_into(html)
    .expect("writing to string should not fail");
}

fn render_routed_edge_path_html(
    edge: &GraphLayoutEdge,
    class: &str,
    marker_end: Option<&str>,
    html: &mut String,
) {
    let points = routed_elbow_points(
        edge.source,
        edge.target,
        edge.route_slot,
        edge.target_port_offset,
    );
    RoutedEdgeTemplate {
        class,
        marker_end,
        points: &points,
    }
    .render_into(html)
    .expect("writing to string should not fail");
}

fn estimate_graph_shell_capacity(snapshot: &GraphSnapshot, layout: &GraphLayout) -> usize {
    let edge_count =
        layout.primary_edges.len() + layout.fork_edges.len() + layout.merge_edges.len();
    2_048
        + layout.lanes.len() * 96
        + edge_count * 160
        + layout.occurrences.len() * 320
        + snapshot
            .nodes
            .iter()
            .map(|node| {
                node.id.len()
                    + node.short_id.len()
                    + node.kind.len()
                    + node.summary.len()
                    + node.labels.iter().map(String::len).sum::<usize>()
            })
            .sum::<usize>()
}

fn push_escaped_text(html: &mut String, value: &str) {
    for ch in value.chars() {
        match ch {
            '&' => html.push_str("&amp;"),
            '<' => html.push_str("&lt;"),
            '>' => html.push_str("&gt;"),
            _ => html.push(ch),
        }
    }
}

fn render_selection_style(snapshot: &GraphSnapshot) -> String {
    snapshot
        .nodes
        .iter()
        .map(|node| {
            let target = node_target_id(&node.id);
            format!(
                "body:has(#{target}:target) [data-node-target=\"{target}\"] .core {{ stroke: #facc15; stroke-width: 3.2; }}"
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_side(snapshot: &GraphSnapshot) -> AnyView {
    let default_details = view! {
        <section class="node-details node-details-default">
            <h2>"Node"</h2>
            <dl class="detail-list">
                <div>
                    <dt>"Selection"</dt>
                    <dd>"Select a node to inspect its content."</dd>
                </div>
            </dl>
        </section>
    };
    let detail_views = snapshot
        .nodes
        .iter()
        .map(render_node_details)
        .collect::<Vec<_>>();
    let branches = render_branches(snapshot);

    view! {
        <aside class="side">
            {default_details}
            {detail_views}
            {branches}
        </aside>
    }
    .into_any()
}

fn render_node_details(node: &GraphNode) -> AnyView {
    let labels = if node.labels.is_empty() {
        "None".to_owned()
    } else {
        node.labels.join(", ")
    };
    let id = node.id.clone();
    let kind = node.kind.clone();
    let role = node.role.clone();
    let created_at = node.created_at.clone();
    let content = node.content.clone();
    let target = node_target_id(&node.id);

    view! {
        <section id=target class="node-details node-detail">
            <h2>"Node"</h2>
            <dl class="detail-list">
                <div>
                    <dt>"Id"</dt>
                    <dd>{id}</dd>
                </div>
                <div>
                    <dt>"Kind"</dt>
                    <dd>{kind}</dd>
                </div>
                <div>
                    <dt>"Role"</dt>
                    <dd>{role}</dd>
                </div>
                <div>
                    <dt>"Created"</dt>
                    <dd>{created_at}</dd>
                </div>
                <div>
                    <dt>"Labels"</dt>
                    <dd>{labels}</dd>
                </div>
                <div>
                    <dt>"Content"</dt>
                    <dd>{content}</dd>
                </div>
            </dl>
        </section>
    }
    .into_any()
}

fn render_branches(snapshot: &GraphSnapshot) -> AnyView {
    let items = snapshot
        .branches
        .iter()
        .map(|branch| {
            let name = branch.name.clone();
            let head = format!("head {}", shorten_id(&branch.head_id));
            let state = format_session_state(&branch.state);
            view! {
                <li class="branch">
                    <strong>{name}</strong>
                    <span>{head}</span>
                    <span>{state}</span>
                </li>
            }
        })
        .collect::<Vec<_>>();

    view! { <section><h2>"Branches"</h2><ul class="branch-list">{items}</ul></section> }.into_any()
}

fn format_session_state(state: &SessionState) -> String {
    match state {
        SessionState::Active => "Active".to_owned(),
        SessionState::Attached {
            target_branch,
            base_head_id,
        } => format!(
            "Attached to {target_branch} from {}",
            shorten_id(base_head_id)
        ),
        SessionState::Paused {
            target_branch,
            reason,
        } => match reason {
            PauseReason::Merged { merged_anchor_id } => format!(
                "Paused on {target_branch}; merged at {}",
                shorten_id(merged_anchor_id)
            ),
            PauseReason::Closed => format!("Paused on {target_branch}; closed"),
        },
    }
}
