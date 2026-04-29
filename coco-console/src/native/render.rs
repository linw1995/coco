use std::collections::HashMap;

use coco_mem::{PauseReason, SessionState};
use leptos::{html::HtmlElement, prelude::*};

use crate::graph::{GraphNode, GraphSnapshot, css_token, node_target_id, shorten_id};
use crate::layout::{GraphLayoutEdgeKind, layout_graph, line_points, routed_elbow_points};

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
    let graph = render_graph(snapshot);
    let side = render_side(snapshot);

    view! {
        <section class="content">
            <div class="graph-wrap">{graph}</div>
            {side}
        </section>
    }
    .into_any()
}

fn render_graph(snapshot: &GraphSnapshot) -> AnyView {
    if snapshot.nodes.is_empty() {
        return view! { <div class="empty">"No sessions found."</div> }.into_any();
    }

    let layout = layout_graph(snapshot);
    let edge_views = layout
        .primary_edges
        .iter()
        .chain(layout.fork_edges.iter())
        .chain(layout.merge_edges.iter())
        .map(|edge| {
            let marker = match edge.kind {
                GraphLayoutEdgeKind::PrimaryParent => "url(#arrowhead)",
                GraphLayoutEdgeKind::Fork => "url(#fork-arrowhead)",
                GraphLayoutEdgeKind::MergeParent => "url(#merge-arrowhead)",
            };
            match edge.kind {
                GraphLayoutEdgeKind::PrimaryParent => {
                    let (x1, y1, x2, y2) =
                        line_points(edge.source, edge.target, edge.target_port_offset);
                    view! { <line class="edge primary-parent" marker-end=marker x1=x1 y1=y1 x2=x2 y2=y2 /> }
                        .into_any()
                }
                GraphLayoutEdgeKind::Fork => {
                    let points = routed_elbow_points(
                        edge.source,
                        edge.target,
                        edge.route_slot,
                        edge.target_port_offset,
                    );
                    view! { <polyline class="edge fork" marker-end=marker points=points /> }
                        .into_any()
                }
                GraphLayoutEdgeKind::MergeParent => {
                    let points = routed_elbow_points(
                        edge.source,
                        edge.target,
                        edge.route_slot,
                        edge.target_port_offset,
                    );
                    view! { <polyline class="edge merge-parent" marker-end=marker points=points /> }
                        .into_any()
                }
            }
        })
        .collect::<Vec<_>>();
    let lane_views = layout
        .lanes
        .iter()
        .map(|lane| {
            let y = lane.y.to_string();
            let label = lane.label.clone();
            view! { <text class="lane-label" x="64" y=y>{label}</text> }
        })
        .collect::<Vec<_>>();
    let nodes_by_id = snapshot
        .nodes
        .iter()
        .map(|node| (node.id.as_str(), node))
        .collect::<HashMap<_, _>>();
    let node_views = layout
        .occurrences
        .iter()
        .filter_map(|occurrence| {
            let node = nodes_by_id.get(occurrence.node_id.as_str())?;
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
            let node_target = occurrence.node_target.clone();
            let href = format!("#{node_target}");
            let node_id = node.id.clone();
            Some(view! {
                <a class="node-link" href=href data-node-target=node_target data-node-id=node_id>
                    <g class=class transform=transform>
                        <title>{title}</title>
                        <circle class="core" r="26" />
                        <text class="node-label" y="44">{label}</text>
                        <text class="node-kind" y="58">{kind}</text>
                    </g>
                </a>
            })
        })
        .collect::<Vec<_>>();
    let view_box = format!("0 0 {} {}", layout.width, layout.height);
    let width = layout.width.to_string();
    let height = layout.height.to_string();

    view! {
        <svg class="graph" role="img" aria-label="CoCo node graph" viewBox=view_box width=width.clone() height=height.clone()>
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
            <rect class="graph-bg" width=width.clone() height=height.clone() />
            {lane_views}
            {edge_views}
            {node_views}
        </svg>
    }
    .into_any()
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
