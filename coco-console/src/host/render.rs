use askama::Template;
use coco_mem::{PauseReason, SessionState};
use leptos::{html::HtmlElement, prelude::*};

use crate::api::Point;
use crate::graph::{
    GraphMode, GraphNode, GraphProviderContext, GraphProviderContextNode, GraphSnapshot,
    node_target_id, shorten_id,
};
use crate::layout::layout_graph;

#[derive(Template)]
#[template(path = "graph_shell.html")]
struct GraphShellTemplate;

pub fn render_loading_index_page(mode: GraphMode, version: u64) -> String {
    render_document(render_loading_root(mode, version), true)
}

pub(crate) fn render_materialized_index_page(shell: &MaterializedGraphShell) -> String {
    render_document(render_materialized_root(shell), true)
}

pub fn render_loading_fragment(mode: GraphMode, version: u64) -> String {
    render_loading_root(mode, version).to_html()
}

#[cfg(test)]
pub fn render_snapshot_page(snapshot: &GraphSnapshot) -> String {
    render_snapshot_document(snapshot, false)
}

pub fn render_fragment(snapshot: &GraphSnapshot) -> String {
    render_root(snapshot).to_html()
}

pub(crate) fn render_materialized_fragment(shell: &MaterializedGraphShell) -> String {
    render_materialized_root(shell).to_html()
}

pub fn render_node_detail_fragment(snapshot: &GraphSnapshot, target: Option<&str>) -> String {
    match target {
        Some(target) => focused_node(snapshot, target)
            .map(render_node_details)
            .unwrap_or_else(|| render_missing_node_details(target))
            .to_html(),
        None => render_default_node_details().to_html(),
    }
}

pub fn render_graph_node_detail_fragment(node: &GraphNode) -> String {
    render_node_details(FocusedNode::Graph(node)).to_html()
}

pub fn render_provider_context_fragment(
    snapshot: &GraphSnapshot,
    target: Option<&str>,
    context: Option<&str>,
) -> String {
    match target {
        Some(target) => match provider_context_for_target(snapshot, target, context) {
            Some(context) => {
                let items = provider_context_items(snapshot, context.context, context.selected_id);
                view! { <ProviderContextList items=items/> }.to_html()
            }
            None if graph_node_exists(snapshot, target) => {
                view! { <ProviderContextList items=Vec::new()/> }.to_html()
            }
            None => view! { <ProviderContextMissing target=target.to_owned()/> }.to_html(),
        },
        None => view! { <ProviderContextDefault/> }.to_html(),
    }
}

pub(crate) fn render_provider_context_items_fragment(items: Vec<ProviderContextItem>) -> String {
    view! { <ProviderContextList items=items/> }.to_html()
}

pub(crate) fn render_provider_context_missing_fragment(target: &str) -> String {
    view! { <ProviderContextMissing target=target.to_owned()/> }.to_html()
}

enum FocusedNode<'a> {
    Graph(&'a GraphNode),
    ProviderContext(&'a GraphProviderContextNode),
}

impl FocusedNode<'_> {
    fn id(&self) -> &str {
        match self {
            Self::Graph(node) => &node.id,
            Self::ProviderContext(node) => &node.id,
        }
    }

    fn kind(&self) -> &str {
        match self {
            Self::Graph(node) => &node.kind,
            Self::ProviderContext(node) => &node.kind,
        }
    }

    fn role(&self) -> &str {
        match self {
            Self::Graph(node) => &node.role,
            Self::ProviderContext(node) => &node.role,
        }
    }

    fn created_at(&self) -> &str {
        match self {
            Self::Graph(node) => &node.created_at,
            Self::ProviderContext(node) => &node.created_at,
        }
    }

    fn content(&self) -> &str {
        match self {
            Self::Graph(node) => &node.content,
            Self::ProviderContext(node) => &node.content,
        }
    }

    fn labels(&self) -> String {
        match self {
            Self::Graph(node) if !node.labels.is_empty() => node.labels.join(", "),
            _ => "None".to_owned(),
        }
    }
}

struct ProviderContextSelection<'a> {
    context: &'a GraphProviderContext,
    selected_id: &'a str,
}

#[derive(Clone)]
pub(crate) struct ProviderContextItem {
    pub context_target: String,
    pub node: GraphProviderContextNode,
    pub selected: bool,
    pub point: Option<Point>,
}

#[derive(Clone)]
pub(crate) struct MaterializedGraphShell {
    pub version: u64,
    pub mode: GraphMode,
    pub node_count: usize,
    pub edge_count: usize,
    pub branches: Vec<MaterializedGraphShellBranch>,
    pub time_ticks: Vec<MaterializedGraphShellTick>,
}

#[derive(Clone)]
pub(crate) struct MaterializedGraphShellBranch {
    pub name: String,
    pub head_short_id: String,
    pub state: SessionState,
}

#[derive(Clone)]
pub(crate) struct MaterializedGraphShellTick {
    pub time_ns: i128,
    pub label: String,
    pub node_target: String,
    pub point: Point,
}

#[cfg(test)]
fn render_snapshot_document(snapshot: &GraphSnapshot, include_client: bool) -> String {
    render_document(render_root(snapshot), include_client)
}

fn render_document(root: AnyView, include_client: bool) -> String {
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

fn render_loading_root(mode: GraphMode, version: u64) -> AnyView {
    let stats = format!("Loading graph / {} / version {}", mode.label(), version);
    let selection_style = render_selection_style();
    let graph_shell = render_graph_shell();
    let mode_value = mode.as_query_value();
    let mode_switch = render_mode_switch(mode);

    view! {
        <main id="console-root" class="shell" data-version=version.to_string() data-graph-mode=mode_value>
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
            <section class="content">
                <div class="graph-shell">
                    <div class="graph-surface" inner_html=graph_shell></div>
                    {render_empty_time_scale("Loading graph...")}
                </div>
                <ProviderContextPanel/>
                {render_loading_side()}
            </section>
        </main>
    }
    .into_any()
}

fn render_materialized_root(shell: &MaterializedGraphShell) -> AnyView {
    let stats = format!(
        "{} nodes / {} edges / {} / version {}",
        shell.node_count,
        shell.edge_count,
        shell.mode.label(),
        shell.version
    );
    let selection_style = render_selection_style();
    let content = render_materialized_content(shell);
    let version = shell.version.to_string();
    let mode = shell.mode.as_query_value();
    let mode_switch = render_mode_switch(shell.mode);

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

fn render_root(snapshot: &GraphSnapshot) -> AnyView {
    let stats = format!(
        "{} nodes / {} edges / {} / version {}",
        snapshot.nodes.len(),
        snapshot.edges.len(),
        snapshot.mode.label(),
        snapshot.version
    );
    let selection_style = render_selection_style();
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
    let time_scale = render_time_scale(snapshot);
    let side = render_side(snapshot);

    view! {
        <section class="content">
            <div class="graph-shell">
                <div class="graph-surface" inner_html=graph_shell></div>
                {time_scale}
            </div>
            <ProviderContextPanel/>
            {side}
        </section>
    }
    .into_any()
}

fn render_materialized_content(shell: &MaterializedGraphShell) -> AnyView {
    let graph_shell = render_graph_shell();
    let time_scale = render_materialized_time_scale(shell);
    let side = render_materialized_side(shell);

    view! {
        <section class="content">
            <div class="graph-shell">
                <div class="graph-surface" inner_html=graph_shell></div>
                {time_scale}
            </div>
            <ProviderContextPanel/>
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

fn render_time_scale(snapshot: &GraphSnapshot) -> AnyView {
    let ticks = time_scale_ticks(snapshot);
    render_time_scale_ticks(ticks)
}

fn render_materialized_time_scale(shell: &MaterializedGraphShell) -> AnyView {
    let mut ticks = shell
        .time_ticks
        .iter()
        .map(|tick| TimeScaleTick {
            time_ns: tick.time_ns,
            label: tick.label.clone(),
            node_target: tick.node_target.clone(),
            point: tick.point,
            position: 0.0,
        })
        .collect::<Vec<_>>();
    ticks.sort_by(|left, right| {
        left.time_ns
            .cmp(&right.time_ns)
            .then_with(|| left.node_target.cmp(&right.node_target))
    });
    let tick_count = ticks.len();
    for (index, tick) in ticks.iter_mut().enumerate() {
        tick.position = time_scale_position_for_index(index, tick_count);
    }
    render_time_scale_ticks(ticks)
}

fn render_time_scale_ticks(ticks: Vec<TimeScaleTick>) -> AnyView {
    let Some(first) = ticks.first() else {
        return render_empty_time_scale("No time data");
    };
    let cursor_label = first.label.clone();
    let tick_views = ticks
        .iter()
        .map(|tick| {
            let style = format!("left: {:.4}%;", tick.position);
            let position = format!("{:.6}", tick.position);
            let time_ns = tick.time_ns.to_string();
            let label = tick.label.clone();
            let title = tick.label.clone();
            view! {
                <span
                    class="time-scale-tick"
                    style=style
                    data-node-target=tick.node_target.clone()
                    data-node-x=tick.point.x.to_string()
                    data-node-y=tick.point.y.to_string()
                    data-position=position
                    data-time-ns=time_ns
                    data-time-label=label
                    title=title
                ></span>
            }
        })
        .collect::<Vec<_>>();
    let min_label = first.label.clone();
    let max_label = ticks
        .last()
        .map(|tick| tick.label.clone())
        .unwrap_or_else(|| first.label.clone());

    view! {
        <nav class="time-scale" aria-label="Graph time navigator" tabindex="0">
            <div class="time-scale-track">
                {tick_views}
                <div class="time-scale-cursor" style="left: 0%;" hidden=true>
                    <span class="time-scale-label">{cursor_label}</span>
                </div>
            </div>
            <div class="time-scale-extents">
                <span>{min_label}</span>
                <span>{max_label}</span>
            </div>
        </nav>
    }
    .into_any()
}

#[derive(Debug)]
struct TimeScaleTick {
    time_ns: i128,
    label: String,
    node_target: String,
    point: Point,
    position: f64,
}

fn time_scale_ticks(snapshot: &GraphSnapshot) -> Vec<TimeScaleTick> {
    let layout = layout_graph(snapshot);
    let points_by_node = layout
        .nodes
        .iter()
        .map(|node| (node.node_id.as_str(), node.point))
        .collect::<std::collections::BTreeMap<_, _>>();

    let mut ticks = snapshot
        .nodes
        .iter()
        .filter_map(|node| {
            let point = points_by_node.get(node.id.as_str())?;
            Some(TimeScaleTick {
                time_ns: node.created_at_ns,
                label: node.created_at.clone(),
                node_target: node_target_id(&node.id),
                point: *point,
                position: 0.0,
            })
        })
        .collect::<Vec<_>>();
    ticks.sort_by(|left, right| {
        left.time_ns
            .cmp(&right.time_ns)
            .then_with(|| left.node_target.cmp(&right.node_target))
    });
    let tick_count = ticks.len();
    for (index, tick) in ticks.iter_mut().enumerate() {
        tick.position = time_scale_position_for_index(index, tick_count);
    }
    ticks
}

fn time_scale_position_for_index(index: usize, tick_count: usize) -> f64 {
    if tick_count <= 1 {
        50.0
    } else {
        index as f64 / (tick_count - 1) as f64 * 100.0
    }
}

fn render_empty_time_scale(label: &'static str) -> AnyView {
    view! {
        <nav class="time-scale time-scale-empty" aria-label="Graph time navigator">
            <div class="time-scale-track">
                <div class="time-scale-cursor" style="left: 50%;">
                    <span class="time-scale-label">{label}</span>
                </div>
            </div>
            <div class="time-scale-extents">
                <span>"-"</span>
                <span>"-"</span>
            </div>
        </nav>
    }
    .into_any()
}

fn render_selection_style() -> String {
    String::new()
}

#[component]
fn ProviderContextPanel() -> impl IntoView {
    view! {
        <section class="provider-context-panel">
            <div class="provider-context-slot"><ProviderContextDefault/></div>
        </section>
    }
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

fn render_loading_side() -> AnyView {
    let default_details = render_default_node_details();

    view! {
        <aside class="side">
            <div class="node-detail-slot">{default_details}</div>
            <section class="branch-section"><h2>"Branches"</h2><ul class="branch-list"></ul></section>
        </aside>
    }
    .into_any()
}

fn render_materialized_side(shell: &MaterializedGraphShell) -> AnyView {
    let default_details = render_default_node_details();
    let branches = render_materialized_branches(shell);

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

fn focused_node<'a>(snapshot: &'a GraphSnapshot, target: &str) -> Option<FocusedNode<'a>> {
    snapshot
        .nodes
        .iter()
        .find(|node| node_target_id(&node.id) == target)
        .map(FocusedNode::Graph)
        .or_else(|| {
            snapshot
                .provider_contexts
                .iter()
                .flat_map(|context| context.nodes.iter())
                .find(|node| node_target_id(&node.id) == target)
                .map(FocusedNode::ProviderContext)
        })
}

fn graph_node_exists(snapshot: &GraphSnapshot, target: &str) -> bool {
    snapshot
        .nodes
        .iter()
        .any(|node| node_target_id(&node.id) == target)
}

fn provider_context_for_target<'a>(
    snapshot: &'a GraphSnapshot,
    target: &str,
    context: Option<&str>,
) -> Option<ProviderContextSelection<'a>> {
    if let Some(context) = context {
        return snapshot
            .provider_contexts
            .iter()
            .find(|provider_context| provider_context.id == context)
            .and_then(|provider_context| provider_context_selection(provider_context, target));
    }

    snapshot
        .provider_contexts
        .iter()
        .find_map(|context| provider_context_selection(context, target))
}

fn provider_context_selection<'a>(
    context: &'a GraphProviderContext,
    target: &str,
) -> Option<ProviderContextSelection<'a>> {
    let selected = context
        .nodes
        .iter()
        .find(|context_node| node_target_id(&context_node.id) == target)?;
    Some(ProviderContextSelection {
        context,
        selected_id: &selected.id,
    })
}

fn render_node_details(node: FocusedNode<'_>) -> AnyView {
    let labels = node.labels();
    let id = node.id().to_owned();
    let kind = node.kind().to_owned();
    let role = node.role().to_owned();
    let created_at = node.created_at().to_owned();
    let content = node.content().to_owned();
    let target = node_target_id(node.id());

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

#[component]
fn ProviderContextDefault() -> impl IntoView {
    view! {
        <section class="provider-context-section provider-context-default">
            <h2>"Provider Context"</h2>
            <p class="provider-context-empty">"Select a node to inspect its provider context."</p>
        </section>
    }
}

fn provider_context_items(
    snapshot: &GraphSnapshot,
    context: &GraphProviderContext,
    selected_id: &str,
) -> Vec<ProviderContextItem> {
    let graph_points = graph_points_by_node(snapshot);
    let context_target = context.id.clone();
    context
        .nodes
        .iter()
        .map(|node| ProviderContextItem {
            context_target: context_target.clone(),
            node: node.clone(),
            selected: node.id == selected_id,
            point: graph_points.get(&node.id).copied(),
        })
        .collect()
}

#[component]
fn ProviderContextList(items: Vec<ProviderContextItem>) -> AnyView {
    if items.is_empty() {
        view! {
            <section class="provider-context-section">
                <h2>"Provider Context"</h2>
                <p class="provider-context-empty">"No provider context nodes."</p>
            </section>
        }
        .into_any()
    } else {
        view! {
            <section class="provider-context-section">
                <h2>"Provider Context"</h2>
                <ol class="provider-context-list">
                    {items.into_iter().map(|item| view! { <ProviderContextRow item=item/> }).collect::<Vec<_>>()}
                </ol>
            </section>
        }
        .into_any()
    }
}

fn graph_points_by_node(snapshot: &GraphSnapshot) -> std::collections::BTreeMap<String, Point> {
    layout_graph(snapshot)
        .nodes
        .into_iter()
        .map(|node| (node.node_id, node.point))
        .collect()
}

#[component]
fn ProviderContextRow(item: ProviderContextItem) -> impl IntoView {
    let class = provider_context_node_class(item.node.visible, item.selected);
    let kind = item.node.kind;
    let role = item.node.role;
    let created_at = item.node.created_at;
    let summary = item.node.summary;
    let id = item.node.short_id;
    let node_target = node_target_id(&item.node.id);
    let target = format!("#{node_target}?context={}", item.context_target);
    let graph_point = item
        .point
        .map(|point| {
            view! {
                <span
                    class="provider-context-node-graph-point"
                    data-node-target=node_target.clone()
                    data-node-x=point.x.to_string()
                    data-node-y=point.y.to_string()
                ></span>
            }
            .into_any()
        })
        .into_iter()
        .collect::<Vec<_>>();

    view! {
        <li class=class>
            <a class="provider-context-node-link" href=target>
                {graph_point}
                <div class="provider-context-node-head">
                    <span>{id}</span>
                    <span>{kind}</span>
                    <span>{role}</span>
                </div>
                <time>{created_at}</time>
                <p>{summary}</p>
            </a>
        </li>
    }
}

fn provider_context_node_class(visible: bool, selected: bool) -> &'static str {
    match (visible, selected) {
        (true, true) => "provider-context-node visible selected",
        (true, false) => "provider-context-node visible",
        (false, true) => "provider-context-node selected",
        (false, false) => "provider-context-node",
    }
}

#[component]
fn ProviderContextMissing(target: String) -> impl IntoView {
    view! {
        <section class="provider-context-section provider-context-default">
            <h2>"Provider Context"</h2>
            <p class="provider-context-empty">"The selected node is no longer available."</p>
            <p class="provider-context-target">{target}</p>
        </section>
    }
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
    let mut branches = snapshot.branches.iter().collect::<Vec<_>>();
    branches.sort_by(|left, right| branch_order(&left.name).cmp(&branch_order(&right.name)));
    let items = branches
        .into_iter()
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

fn render_materialized_branches(shell: &MaterializedGraphShell) -> AnyView {
    let items = shell
        .branches
        .iter()
        .map(|branch| {
            let name = branch.name.clone();
            let head = format!("head {}", branch.head_short_id);
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

fn branch_order(branch: &str) -> (u8, &str) {
    (u8::from(branch != "main"), branch)
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
