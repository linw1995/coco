use askama::Template;
use leptos::{html::HtmlElement, prelude::*};

use crate::host::web_graph_view::ViewMode;
use crate::panels::{NodeDetailPanel, ProviderContextPanel};

const HYDRATION_BOOTSTRAP: &str = "__RESOLVED_RESOURCES=[];\
__SERIALIZED_ERRORS=[];\
__PENDING_RESOURCES=[];\
__RESOURCE_RESOLVERS=[];\
__INCOMPLETE_CHUNKS=[];";

#[derive(Template)]
#[template(path = "graph_shell.html")]
struct GraphShellTemplate;

pub fn render_index_page(mode: ViewMode, revision: u64) -> String {
    render_document(render_root(mode, revision))
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
    format!("<!doctype html>{}", rendered.to_html())
}

fn render_root(mode: ViewMode, revision: u64) -> AnyView {
    let stats = format!("{} / revision {}", mode.label(), revision);
    let graph_mode = mode.as_query_value().to_owned();
    let graph_shell = GraphShellTemplate
        .render()
        .expect("graph shell template should render");
    let provider_context_panel = view! { <ProviderContextPanel graph_mode=graph_mode/> }.into_any();
    let node_detail_panel = view! { <NodeDetailPanel/> }.into_any();

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
                        {provider_context_panel}
                    </div>
                </section>
                <aside class="side">
                    <div class="node-detail-slot">{node_detail_panel}</div>
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
        assert!(!page.contains("<!--bo-"));
        assert!(!page.contains("<!--bc-"));
        assert_eq!(page.matches("<leptos-island").count(), 2);
        assert_eq!(
            page.matches("Select a node to inspect its content.")
                .count(),
            1
        );
        assert_eq!(
            page.matches("Select a node to inspect its provider context.")
                .count(),
            1
        );
    }
}
