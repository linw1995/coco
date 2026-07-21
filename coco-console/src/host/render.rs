use askama::Template;
use leptos::{html::HtmlElement, prelude::*};

use crate::api::Point;
use crate::host::web_graph_view::{NodeView, ViewMode, node_target_id};
use crate::panels::{
    NodeDetailPanel, ProviderContextPanel, render_default_node_detail,
    render_default_provider_context,
};

const HYDRATION_BOOTSTRAP: &str = "__RESOLVED_RESOURCES=[];\
__SERIALIZED_ERRORS=[];\
__PENDING_RESOURCES=[];\
__RESOURCE_RESOLVERS=[];\
__INCOMPLETE_CHUNKS=[];";

#[derive(Template)]
#[template(path = "graph_shell.html")]
struct GraphShellTemplate;

#[derive(Clone)]
pub struct ProviderContextItem {
    pub context_target: String,
    pub node: NodeView,
    pub selected: bool,
    pub point: Option<Point>,
}

pub fn render_index_page(mode: ViewMode, revision: u64) -> String {
    render_document(render_root(mode, revision))
}

pub fn render_node_detail_fragment(node: Option<&NodeView>, target: Option<&str>) -> String {
    match (node, target) {
        (Some(node), _) => render_node_details(node).to_html(),
        (None, Some(target)) => render_missing_node_details(target).to_html(),
        (None, None) => render_default_node_detail().to_html(),
    }
}

pub fn render_provider_context_default_fragment() -> String {
    render_default_provider_context().to_html()
}

pub fn render_provider_context_items_fragment(items: Vec<ProviderContextItem>) -> String {
    view! { <ProviderContextList items=items/> }.to_html()
}

pub fn render_provider_context_missing_fragment(target: &str) -> String {
    view! { <ProviderContextMissing target=target.to_owned()/> }.to_html()
}

fn render_document(root: AnyView) -> String {
    let options = LeptosOptions::builder().output_name("coco_console").build();
    let rendered: View<HtmlElement<_, _, _>> = view! {
        <html lang="en">
            <head>
                <meta charset="utf-8" />
                <meta name="viewport" content="width=device-width, initial-scale=1" />
                <title>"CoCo Console"</title>
                <link rel="stylesheet" href="/style.css" />
                <link rel="license" href="/third-party-notices.html" />
                <script>{HYDRATION_BOOTSTRAP}</script>
                <HydrationScripts options=options islands=true/>
            </head>
            <body>{root}</body>
        </html>
    };
    format!("<!doctype html>{}", rendered.to_html_branching())
}

fn render_root(mode: ViewMode, revision: u64) -> AnyView {
    let stats = format!("{} / revision {}", mode.label(), revision);
    let graph_mode = mode.as_query_value().to_owned();
    let graph_shell = GraphShellTemplate
        .render()
        .expect("graph shell template should render");

    view! {
        <main
            id="console-root"
            class="shell"
            data-version=revision.to_string()
            data-graph-mode=mode.as_query_value()
        >
            <header class="topbar">
                <section class="brand">
                    <h1>"CoCo Console"</h1>
                    <p>"Live node relationship graph from the daemon store."</p>
                </section>
                <section class="topbar-actions">
                    {render_mode_switch(mode)}
                    <p class="stats">{stats}</p>
                </section>
            </header>
            <section class="content">
                <div class="graph-shell">
                    <div class="graph-surface" inner_html=graph_shell></div>
                    {render_empty_time_scale()}
                </div>
                <section class="provider-context-panel">
                    <div class="provider-context-slot">
                        <ProviderContextPanel graph_mode=graph_mode.clone()/>
                    </div>
                </section>
                <aside class="side">
                    <div class="node-detail-slot"><NodeDetailPanel graph_mode=graph_mode/></div>
                </aside>
            </section>
        </main>
    }
    .into_any()
}

fn render_mode_switch(mode: ViewMode) -> AnyView {
    let anchors_class = mode_switch_class(mode == ViewMode::Anchors);
    let all_class = mode_switch_class(mode == ViewMode::All);
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

fn render_empty_time_scale() -> AnyView {
    view! {
        <nav class="time-scale time-scale-empty" aria-label="Graph time navigator">
            <div class="time-scale-track">
                <div class="time-scale-cursor" style="left: 50%;">
                    <span class="time-scale-label">"Live graph"</span>
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

fn render_node_details(node: &NodeView) -> AnyView {
    let target = node_target_id(&node.id);
    let id = node.id.clone();
    let kind = node.kind.clone();
    let role = node.role.clone();
    let created_at = node.created_at.clone();
    let content = node.content.clone();

    view! {
        <section id=target class="node-details node-detail">
            <h2>"Node"</h2>
            <dl class="detail-list">
                <div><dt>"Id"</dt><dd>{id}</dd></div>
                <div><dt>"Kind"</dt><dd>{kind}</dd></div>
                <div><dt>"Role"</dt><dd>{role}</dd></div>
                <div><dt>"Created"</dt><dd>{created_at}</dd></div>
                <div><dt>"Labels"</dt><dd>"None"</dd></div>
                <div><dt>"Content"</dt><dd>{content}</dd></div>
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
                <div><dt>"Target"</dt><dd>{target}</dd></div>
            </dl>
        </section>
    }
    .into_any()
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

#[component]
fn ProviderContextRow(item: ProviderContextItem) -> impl IntoView {
    let visible = item.point.is_some();
    let class = provider_context_node_class(visible, item.selected);
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
                    <span>{item.node.short_id}</span>
                    <span>{item.node.kind}</span>
                    <span>{item.node.role}</span>
                </div>
                <time>{item.node.created_at}</time>
                <p>{item.node.summary}</p>
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_contains_graph_bootstrap_contract() {
        let page = render_index_page(ViewMode::All, 7);

        assert!(page.contains("data-version=\"7\""));
        assert!(page.contains("data-graph-mode=\"all\""));
        assert!(page.contains("virtual-graph"));
        assert!(page.contains("/pkg/coco_console.js"));
        let bootstrap = page
            .find("__INCOMPLETE_CHUNKS=[]")
            .expect("hydration globals should be initialized");
        let graph_loader = page
            .find("mod.hydrate();")
            .expect("graph loader should be rendered");
        let island_loader = page
            .find("hydrateIslands(document.body, mod)")
            .expect("island loader should be rendered");
        assert!(bootstrap < graph_loader);
        assert!(graph_loader < island_loader);
        assert!(page.contains("<!--bo-"));
        assert!(page.contains("<!--bc-"));
        assert_eq!(page.matches("<leptos-island").count(), 2);
        assert!(page.contains("Select a node to inspect its content."));
        assert!(page.contains("Select a node to inspect its provider context."));
    }

    #[test]
    fn node_detail_escapes_content() {
        let node = NodeView {
            id: "node-1".to_owned(),
            short_id: "node-1".to_owned(),
            kind: "text".to_owned(),
            role: "User".to_owned(),
            created_at: "now".to_owned(),
            content: "<script>alert(1)</script>".to_owned(),
            summary: String::new(),
        };

        let fragment = render_node_detail_fragment(Some(&node), None);

        assert!(fragment.contains("&lt;script&gt;"));
        assert!(!fragment.contains("<script>"));
    }
}
