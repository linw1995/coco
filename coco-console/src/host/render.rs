use askama::Template;
use leptos::{html::HtmlElement, prelude::*};

use crate::api::{GraphViewportEdge, GraphViewportNode, GraphViewportResponse};
use crate::graph_render::{
    bezier_path, edge_hit_target_id, edge_hit_target_label, edge_style, node_group_class,
    node_label, node_title_text, render_element_id,
};
use crate::host::web_graph_view::ViewMode;
use crate::panels::{NODE_DETAIL_PANEL_ID, NodeDetailPanel, ProviderContextPanel};

use super::CLIENT_ASSET_VERSION;

const HYDRATION_BOOTSTRAP: &str = "__RESOLVED_RESOURCES=[];\
__SERIALIZED_ERRORS=[];\
__PENDING_RESOURCES=[];\
__RESOURCE_RESOLVERS=[];\
__INCOMPLETE_CHUNKS=[];";

#[derive(Template)]
#[template(path = "graph_shell.html")]
struct GraphShellTemplate<'a> {
    graph: &'a RenderedGraph,
}

struct RenderedGraph {
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

pub fn render_index_page(mode: ViewMode, viewport: &GraphViewportResponse) -> String {
    render_document(render_root(mode, viewport))
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

fn render_root(mode: ViewMode, viewport: &GraphViewportResponse) -> AnyView {
    let revision = viewport.version;
    let stats = format!("{} / revision {}", mode.label(), revision);
    let graph_mode = mode.as_query_value().to_owned();
    let graph = RenderedGraph::new(mode, viewport);
    let graph_shell = GraphShellTemplate { graph: &graph }
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
                    <div id=NODE_DETAIL_PANEL_ID class="node-detail-slot">{node_detail_panel}</div>
                </aside>
            </section>
        </main>
    }
    .into_any()
}

impl RenderedGraph {
    fn new(mode: ViewMode, response: &GraphViewportResponse) -> Self {
        Self {
            canvas_width: response.canvas.width,
            canvas_height: response.canvas.height,
            viewport_x: response.viewport.x,
            viewport_y: response.viewport.y,
            viewport_width: response.viewport.width,
            viewport_height: response.viewport.height,
            render_edge_hit_targets: mode == ViewMode::Anchors,
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
        let kind = edge.kind.key_part();
        let id = render_element_id(&edge.key);
        Self {
            hit_target_id: edge_hit_target_id(&edge.key),
            id,
            key: edge.key.clone(),
            class,
            marker,
            path: bezier_path(edge.route),
            kind,
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
    use crate::api::{GraphBezierRoute, GraphCanvas, GraphViewport, GraphViewportEdgeKind, Point};

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
        let page = render_index_page(ViewMode::All, &viewport);

        assert!(page.contains("data-version=\"7\""));
        assert!(page.contains("data-graph-mode=\"all\""));
        assert!(page.contains("virtual-graph"));
        assert!(page.contains("data-viewport-x=\"1120\""));
        assert!(page.contains("data-canvas-width=\"2400\""));
        assert!(page.contains("viewBox=\"1120 0 1280 720\""));
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
