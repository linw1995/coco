use leptos::{html::HtmlElement, prelude::*};

use crate::api::GraphViewportResponse;
use crate::graph_render::{GraphCanvas, GraphCanvasModel};
use crate::host::web_graph_view::ViewMode;
use crate::panels::{
    InitialProviderContext, NODE_DETAIL_PANEL_ID, NodeDetailPanel, ProviderContextPanel,
};

use super::CLIENT_ASSET_VERSION;

const HYDRATION_BOOTSTRAP: &str = "__RESOLVED_RESOURCES=[];\
__SERIALIZED_ERRORS=[];\
__PENDING_RESOURCES=[];\
__RESOURCE_RESOLVERS=[];\
__INCOMPLETE_CHUNKS=[];";

pub fn render_index_page(
    mode: ViewMode,
    viewport: &GraphViewportResponse,
    initial_provider_context: Option<InitialProviderContext>,
) -> String {
    render_document(render_root(mode, viewport, initial_provider_context))
}

fn render_document(root: AnyView) -> String {
    let options = LeptosOptions::builder()
        .output_name("coco_console")
        .site_pkg_dir(format!("pkg/{CLIENT_ASSET_VERSION}"))
        .build();
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

fn render_root(
    mode: ViewMode,
    viewport: &GraphViewportResponse,
    initial_provider_context: Option<InitialProviderContext>,
) -> AnyView {
    let revision = viewport.version;
    let stats = format!("{} / revision {}", mode.label(), revision);
    let graph_mode = mode.as_query_value().to_owned();
    let graph = GraphCanvasModel::new(mode == ViewMode::Anchors, viewport);
    let provider_context_panel = view! {
        <ProviderContextPanel graph_mode=graph_mode initial=initial_provider_context/>
    }
    .into_any();
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
                    <div class="graph-surface"><GraphCanvas graph/></div>
                    {render_empty_time_scale()}
                </div>
                <section class="provider-context-panel">
                    <div class="provider-context-slot">
                        {provider_context_panel}
                    </div>
                </section>
                <aside class="side">
                    <div id=NODE_DETAIL_PANEL_ID class="node-detail-slot">{node_detail_panel}</div>
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
    use crate::api::{
        GraphBezierRoute, GraphCanvas, GraphViewport, GraphViewportEdge, GraphViewportEdgeKind,
        GraphViewportNode, Point,
    };

    #[test]
    fn index_contains_graph_bootstrap_contract() {
        let viewport = GraphViewportResponse {
            version: 7,
            canvas: GraphCanvas {
                width: 2400,
                height: 900,
            },
            viewport: GraphViewport {
                x: 1120,
                y: 0,
                width: 1280,
                height: 720,
                overscan: 180,
            },
            nodes: vec![GraphViewportNode {
                key: "node:latest".to_owned(),
                id: "latest".to_owned(),
                node_target: "node-latest".to_owned(),
                short_id: "latest".to_owned(),
                kind: "text".to_owned(),
                summary: "Latest node".to_owned(),
                labels: vec!["main".to_owned()],
                x: 2300,
                y: 120,
            }],
            edges: vec![GraphViewportEdge {
                key: "edge:latest".to_owned(),
                kind: GraphViewportEdgeKind::Primary,
                source_id: "previous".to_owned(),
                target_id: "latest".to_owned(),
                route: GraphBezierRoute {
                    source: Point { x: 2200, y: 120 },
                    control_1: Point { x: 2230, y: 120 },
                    control_2: Point { x: 2250, y: 120 },
                    target: Point { x: 2280, y: 120 },
                },
            }],
        };
        let page = render_index_page(ViewMode::All, &viewport, None);

        assert!(page.contains("data-version=\"7\""));
        assert!(page.contains("data-graph-mode=\"all\""));
        assert!(page.contains("virtual-graph"));
        assert!(page.contains("data-viewport-x=\"1120\""));
        assert!(page.contains("data-canvas-width=\"2400\""));
        assert!(page.contains("viewBox=\"1120 0 1280 720\""));
        assert!(page.contains("preserveAspectRatio=\"xMaxYMin meet\""));
        assert!(page.contains("width=\"2400\" height=\"900\""));
        assert!(page.contains("data-node-id=\"latest\""));
        assert!(page.contains("data-render-key=\"edge:latest\""));
        assert!(!page.contains("Loading graph..."));
        assert!(page.contains(&format!("/pkg/{CLIENT_ASSET_VERSION}/coco_console.js")));
        assert!(page.contains(&format!("/pkg/{CLIENT_ASSET_VERSION}/coco_console_bg.wasm")));
        assert!(!page.contains("\"/pkg/coco_console.js\""));
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
        assert!(page.contains("graph-anchor-range"));
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
