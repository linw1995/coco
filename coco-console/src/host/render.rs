use askama::Template;
use coco_mem::{PauseReason, SessionState};
use leptos::{html::HtmlElement, prelude::*};

use crate::graph::{GraphMode, GraphNode, GraphSnapshot, node_target_id, shorten_id};

#[derive(Template)]
#[template(path = "graph_shell.html")]
struct GraphShellTemplate;

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

pub fn render_node_detail_fragment(snapshot: &GraphSnapshot, target: Option<&str>) -> String {
    match target {
        Some(target) => snapshot
            .nodes
            .iter()
            .find(|node| node_target_id(&node.id) == target)
            .map(render_node_details)
            .unwrap_or_else(|| render_missing_node_details(target))
            .to_html(),
        None => render_default_node_details().to_html(),
    }
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
        "{} nodes / {} edges / {} / version {}",
        snapshot.nodes.len(),
        snapshot.edges.len(),
        snapshot.mode.label(),
        snapshot.version
    );
    let selection_style = render_selection_style(snapshot);
    let content = render_content(snapshot);
    let version = snapshot.version.to_string();
    let mode = snapshot.mode.as_query_value();
    let mode_switch = render_mode_switch(snapshot.mode);

    view! {
        <main id="console-root" class="shell" data-version=version data-graph-mode=mode>
            <style id="selection-style">{selection_style}</style>
            <header class="topbar">
                <section class="brand">
                    <h1>"CoCo Console"</h1>
                    <p>"Live node relationship graph from the daemon store."</p>
                </section>
                <section class="topbar-actions">
                    {mode_switch}
                    <p class="stats">{stats}</p>
                </section>
            </header>
            {content}
        </main>
    }
    .into_any()
}

fn render_mode_switch(mode: GraphMode) -> AnyView {
    let anchors_class = mode_switch_class(mode == GraphMode::Anchors);
    let all_class = mode_switch_class(mode == GraphMode::All);

    view! {
        <nav class="mode-switch" aria-label="Graph mode">
            <a class=anchors_class href="/?mode=anchors">"Anchors"</a>
            <a class=all_class href="/?mode=all">"All"</a>
        </nav>
    }
    .into_any()
}

fn mode_switch_class(active: bool) -> &'static str {
    if active {
        "mode-switch-item active"
    } else {
        "mode-switch-item"
    }
}

fn render_content(snapshot: &GraphSnapshot) -> AnyView {
    let graph_shell = render_graph_shell();
    let side = render_side(snapshot);

    view! {
        <section class="content">
            <div class="graph-shell" inner_html=graph_shell></div>
            {side}
        </section>
    }
    .into_any()
}

fn render_graph_shell() -> String {
    GraphShellTemplate
        .render()
        .expect("graph shell template should render")
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
    let default_details = render_default_node_details();
    let branches = render_branches(snapshot);

    view! {
        <aside class="side">
            <div class="node-detail-slot">{default_details}</div>
            {branches}
        </aside>
    }
    .into_any()
}

fn render_default_node_details() -> AnyView {
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

fn render_missing_node_details(target: &str) -> AnyView {
    let target = target.to_owned();

    view! {
        <section class="node-details node-details-default">
            <h2>"Node"</h2>
            <dl class="detail-list">
                <div>
                    <dt>"Selection"</dt>
                    <dd>"The selected node is no longer available."</dd>
                </div>
                <div>
                    <dt>"Target"</dt>
                    <dd>{target}</dd>
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

    view! { <section class="branch-section"><h2>"Branches"</h2><ul class="branch-list">{items}</ul></section> }.into_any()
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
