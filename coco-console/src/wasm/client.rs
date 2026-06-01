use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::rc::Rc;
use std::str::{FromStr, Split};

use serde::Deserialize;
use wasm_bindgen::{JsCast, JsValue, closure::Closure, prelude::wasm_bindgen};
use wasm_bindgen_futures::{JsFuture, spawn_local};
use web_sys::{
    AbortController, AbortSignal, Document, Element, MouseEvent, RequestInit, Response, WheelEvent,
    Window,
};

use super::refresh::{
    PendingViewportUpdate, VersionRefresh, ViewportFetch, next_viewport_fetch,
    pending_update_for_viewport_change, version_refresh_action, viewport_update_active,
};
use crate::api::{
    GraphCanvas, GraphViewport, GraphViewportDiffResponse, GraphViewportEdge,
    GraphViewportEdgeKind, GraphViewportItems, GraphViewportLane, GraphViewportNode,
    GraphViewportRemovedItem, GraphViewportResponse, Point,
};
use crate::viewport::{MIN_OVERSCAN, ViewportState, rounded_i32, same_viewport};

const ROOT_ID: &str = "console-root";
const SVG_NS: &str = "http://www.w3.org/2000/svg";
const VIEWPORT_KEY: &str = "coco-console:viewport";
const NODE_RADIUS: f64 = 26.0;
const EDGE_NODE_EXIT: f64 = 42.0;
const EDGE_TARGET_APPROACH: f64 = 48.0;
const EDGE_ROUTE_STEP: f64 = 12.0;
const MIN_ZOOM: f64 = 0.25;
const MAX_ZOOM: f64 = 4.0;
const VERSION_REFRESH_RETRY_MS: i32 = 50;

struct ViewportMapContentBounds {
    x: f64,
    y: f64,
    width: f64,
    height: f64,
}

struct GraphItemsRefreshInput {
    window: Window,
    viewport: ViewportState,
    query: String,
}

type GraphListenerInstaller = fn(Rc<RefCell<VirtualGraph>>) -> Result<(), JsValue>;

struct GraphRootElements {
    graph_wrap: Element,
    graph_svg: Element,
    graph_bg: Element,
}

struct GraphLayerElements {
    lane_group: Element,
    edge_group: Element,
    node_group: Element,
}

struct ViewportMapElements {
    viewport_map: Element,
    viewport_map_bg: Element,
    viewport_map_window: Element,
}

struct VirtualGraphElements {
    root: GraphRootElements,
    layers: GraphLayerElements,
    viewport_map: ViewportMapElements,
    status: Option<Element>,
}

struct ViewportPatchInput {
    window: Window,
    current: ViewportState,
    fetch: ViewportFetch,
}

#[wasm_bindgen(start)]
pub fn start() {
    spawn_local(async {
        if let Err(error) = run().await {
            web_sys::console::error_1(&error);
        }
    });
}

async fn run() -> Result<(), JsValue> {
    let graph = setup_graph()?;
    render_full_viewport(graph.clone()).await?;
    let has_selected_node = {
        let graph = graph.borrow();
        selected_node_target(&graph.window).is_some()
    };
    if has_selected_node {
        refresh_selected_node_detail_from_graph(graph.clone()).await?;
    }
    spawn_local(refresh_graph_items_on_version(graph.clone()));
    spawn_local(refresh_server_rendered_sections_on_version(graph));

    Ok(())
}

fn setup_graph() -> Result<Rc<RefCell<VirtualGraph>>, JsValue> {
    let (window, document) = browser_context()?;
    let graph = VirtualGraph::new(window.clone(), document.clone())?;
    let graph = Rc::new(RefCell::new(graph));

    install_graph_listeners(graph.clone())?;

    Ok(graph)
}

fn browser_context() -> Result<(Window, Document), JsValue> {
    let window = browser_window()?;
    let document = browser_document(&window)?;
    Ok((window, document))
}

impl From<GraphViewport> for ViewportState {
    fn from(value: GraphViewport) -> Self {
        Self {
            x: f64::from(value.x),
            y: f64::from(value.y),
            width: f64::from(value.width),
            height: f64::from(value.height),
            overscan: value.overscan,
        }
    }
}

struct RenderedKeys {
    lanes: BTreeMap<String, String>,
    nodes: BTreeMap<String, String>,
    edges: BTreeMap<String, String>,
}

impl RenderedKeys {
    fn new() -> Self {
        Self {
            lanes: BTreeMap::new(),
            nodes: BTreeMap::new(),
            edges: BTreeMap::new(),
        }
    }

    fn known_query(&self) -> String {
        self.lanes
            .iter()
            .flat_map(|(key, fingerprint)| {
                [
                    format!("known_lane={}", percent_encode(key)),
                    format!(
                        "known_lane_fingerprint={}:{}",
                        percent_encode(key),
                        fingerprint
                    ),
                ]
            })
            .chain(self.nodes.iter().flat_map(|(key, fingerprint)| {
                [
                    format!("known_node={}", percent_encode(key)),
                    format!(
                        "known_node_fingerprint={}:{}",
                        percent_encode(key),
                        fingerprint
                    ),
                ]
            }))
            .chain(self.edges.iter().flat_map(|(key, fingerprint)| {
                [
                    format!("known_edge={}", percent_encode(key)),
                    format!(
                        "known_edge_fingerprint={}:{}",
                        percent_encode(key),
                        fingerprint
                    ),
                ]
            }))
            .collect::<Vec<_>>()
            .join("&")
    }
}

struct VirtualGraph {
    window: Window,
    document: Document,
    graph_wrap: Element,
    graph_svg: Element,
    graph_bg: Element,
    lane_group: Element,
    edge_group: Element,
    node_group: Element,
    viewport_map: Element,
    viewport_map_bg: Element,
    viewport_map_window: Element,
    status: Option<Element>,
    viewport: ViewportState,
    zoom: f64,
    canvas: Option<GraphCanvas>,
    version: u64,
    shell_version: u64,
    rendered: RenderedKeys,
    rendered_viewport: ViewportState,
    patch_in_flight: bool,
    pending_viewport_update: PendingViewportUpdate,
    version_refresh_abort: Option<AbortController>,
}

impl VirtualGraph {
    fn new(window: Window, document: Document) -> Result<Self, JsValue> {
        let elements = VirtualGraphElements::query(&document)?;
        let zoom = initial_zoom(&elements.root.graph_wrap);
        let viewport = initial_viewport(&window, &elements.root.graph_wrap, zoom);
        let version = current_version(&document).unwrap_or_default();

        Ok(Self {
            window,
            document,
            graph_wrap: elements.root.graph_wrap,
            graph_svg: elements.root.graph_svg,
            graph_bg: elements.root.graph_bg,
            lane_group: elements.layers.lane_group,
            edge_group: elements.layers.edge_group,
            node_group: elements.layers.node_group,
            viewport_map: elements.viewport_map.viewport_map,
            viewport_map_bg: elements.viewport_map.viewport_map_bg,
            viewport_map_window: elements.viewport_map.viewport_map_window,
            status: elements.status,
            viewport,
            zoom,
            canvas: None,
            version,
            shell_version: version,
            rendered: RenderedKeys::new(),
            rendered_viewport: viewport,
            patch_in_flight: false,
            pending_viewport_update: PendingViewportUpdate::None,
            version_refresh_abort: None,
        })
    }

    fn resize_viewport(&mut self) {
        self.viewport.width = graph_client_width(&self.graph_wrap) / self.zoom;
        self.viewport.height = graph_client_height(&self.graph_wrap) / self.zoom;
        self.viewport.refresh_render_overscan();
        self.clamp_viewport();
    }

    fn clamp_viewport(&mut self) {
        if let Some(canvas) = self.canvas {
            self.viewport.x = self.viewport.x.clamp(
                0.0,
                (f64::from(canvas.width) - self.viewport.width).max(0.0),
            );
            self.viewport.y = self.viewport.y.clamp(
                0.0,
                (f64::from(canvas.height) - self.viewport.height).max(0.0),
            );
        } else {
            self.viewport.x = self.viewport.x.max(0.0);
            self.viewport.y = self.viewport.y.max(0.0);
        }
    }

    fn set_viewport(&mut self, viewport: GraphViewport) {
        self.viewport = viewport.into();
        self.zoom =
            (graph_client_width(&self.graph_wrap) / self.viewport.width).clamp(MIN_ZOOM, MAX_ZOOM);
        self.clamp_viewport();
        self.persist_viewport();
    }

    fn persist_viewport(&self) {
        let Some(storage) = session_storage(&self.window) else {
            return;
        };
        let value = format!(
            "{},{},{},{},{}",
            rounded_i32(self.viewport.x),
            rounded_i32(self.viewport.y),
            rounded_i32(self.viewport.width),
            rounded_i32(self.viewport.height),
            self.viewport.overscan
        );
        let _ = storage.set_item(VIEWPORT_KEY, &value);
    }

    fn apply_full(&mut self, response: GraphViewportResponse) -> Result<(), JsValue> {
        let GraphViewportResponse {
            version,
            canvas,
            viewport,
            lanes,
            nodes,
            edges,
        } = response;
        clear_children(&self.lane_group);
        clear_children(&self.edge_group);
        clear_children(&self.node_group);
        self.rendered = RenderedKeys::new();
        self.apply_response_viewport(version, canvas, viewport)?;
        self.upsert_graph_items(
            GraphViewportItems {
                lanes,
                nodes,
                edges,
            },
            false,
        )?;
        self.hide_status();
        Ok(())
    }

    fn apply_diff(&mut self, response: GraphViewportDiffResponse) -> Result<(), JsValue> {
        let GraphViewportDiffResponse {
            version,
            canvas,
            viewport,
            added,
            updated,
            removed,
            ..
        } = response;
        self.apply_response_viewport(version, canvas, viewport)?;
        self.remove_graph_items(removed);
        self.upsert_diff_items(added, updated)?;
        self.hide_status();
        Ok(())
    }

    fn apply_response_viewport(
        &mut self,
        version: u64,
        canvas: GraphCanvas,
        viewport: GraphViewport,
    ) -> Result<(), JsValue> {
        let desired_viewport = self.viewport;
        let response_viewport = ViewportState::from(viewport);
        self.version = version;
        self.canvas = Some(canvas);
        if same_viewport(desired_viewport, response_viewport) {
            self.set_viewport(viewport);
        }
        self.rendered_viewport = response_viewport;
        self.set_root_version();
        self.apply_canvas()
    }

    fn apply_canvas(&self) -> Result<(), JsValue> {
        apply_graph_viewport(&self.graph_svg, &self.graph_wrap, self.viewport, self.zoom)?;
        apply_canvas_dimensions(
            self.canvas,
            &self.graph_bg,
            &self.viewport_map,
            &self.viewport_map_bg,
        )?;
        apply_viewport_map_window(&self.viewport_map_window, self.viewport)
    }

    fn upsert_graph_items(
        &mut self,
        items: GraphViewportItems,
        nodes_are_new: bool,
    ) -> Result<(), JsValue> {
        let GraphViewportItems {
            lanes,
            nodes,
            edges,
        } = items;
        self.upsert_lanes(lanes)?;
        self.upsert_edges(edges)?;
        self.upsert_nodes(nodes, nodes_are_new)
    }

    fn upsert_lanes(&mut self, lanes: Vec<GraphViewportLane>) -> Result<(), JsValue> {
        for lane in lanes {
            self.upsert_lane(lane)?;
        }
        Ok(())
    }

    fn upsert_edges(&mut self, edges: Vec<GraphViewportEdge>) -> Result<(), JsValue> {
        for edge in edges {
            self.upsert_edge(edge)?;
        }
        Ok(())
    }

    fn upsert_nodes(
        &mut self,
        nodes: Vec<GraphViewportNode>,
        nodes_are_new: bool,
    ) -> Result<(), JsValue> {
        for node in nodes {
            self.upsert_node(node, nodes_are_new)?;
        }
        Ok(())
    }

    fn upsert_diff_items(
        &mut self,
        added: GraphViewportItems,
        updated: GraphViewportItems,
    ) -> Result<(), JsValue> {
        self.upsert_graph_items(added, true)?;
        self.upsert_graph_items(updated, false)
    }

    fn remove_graph_items(&mut self, items: Vec<GraphViewportRemovedItem>) {
        for item in items {
            self.remove_key(&item.key);
        }
    }

    fn upsert_lane(&mut self, lane: GraphViewportLane) -> Result<(), JsValue> {
        self.remove_key(&lane.key);
        let element = svg_element(&self.document, "text")?;
        set_attributes(
            &element,
            [
                ("id", render_element_id(&lane.key)),
                ("data-render-key", lane.key.clone()),
                ("class", "lane-label".to_owned()),
                ("x", "64".to_owned()),
                ("y", lane.y.to_string()),
            ],
        )?;
        element.set_text_content(Some(&lane.label));
        self.lane_group.append_child(&element)?;
        self.rendered
            .lanes
            .insert(lane.key.clone(), lane.fingerprint());
        Ok(())
    }

    fn upsert_edge(&mut self, edge: GraphViewportEdge) -> Result<(), JsValue> {
        self.remove_key(&edge.key);
        let element = self.edge_element(&edge)?;
        set_attributes(
            &element,
            [
                ("id", render_element_id(&edge.key)),
                ("data-render-key", edge.key.clone()),
            ],
        )?;
        self.edge_group.append_child(&element)?;
        self.rendered
            .edges
            .insert(edge.key.clone(), edge.fingerprint());
        Ok(())
    }

    fn edge_element(&self, edge: &GraphViewportEdge) -> Result<Element, JsValue> {
        match edge.kind {
            GraphViewportEdgeKind::PrimaryParent => self.primary_edge_element(edge),
            GraphViewportEdgeKind::Fork | GraphViewportEdgeKind::MergeParent => {
                self.routed_edge_element(edge)
            }
        }
    }

    fn primary_edge_element(&self, edge: &GraphViewportEdge) -> Result<Element, JsValue> {
        let element = svg_element(&self.document, "line")?;
        let (x1, y1, x2, y2) = line_points(edge.source, edge.target, edge.target_port_offset);
        set_attributes(
            &element,
            [
                ("class", "edge primary-parent".to_string()),
                ("marker-end", "url(#arrowhead)".to_string()),
                ("x1", x1),
                ("y1", y1),
                ("x2", x2),
                ("y2", y2),
            ],
        )?;
        Ok(element)
    }

    fn routed_edge_element(&self, edge: &GraphViewportEdge) -> Result<Element, JsValue> {
        let element = svg_element(&self.document, "polyline")?;
        let (class, marker) = routed_edge_style(edge.kind);
        set_attributes(
            &element,
            [
                ("class", class.to_string()),
                ("marker-end", marker.to_string()),
                (
                    "points",
                    routed_elbow_points(
                        edge.source,
                        edge.target,
                        edge.route_slot,
                        edge.target_port_offset,
                    ),
                ),
            ],
        )?;
        Ok(element)
    }

    fn upsert_node(&mut self, node: GraphViewportNode, is_new: bool) -> Result<(), JsValue> {
        self.remove_key(&node.key);
        let fingerprint = node.fingerprint();
        let link = self.node_link_element(&node, is_new)?;
        self.append_node(&node.key, &link, fingerprint)
    }

    fn node_link_element(
        &self,
        node: &GraphViewportNode,
        is_new: bool,
    ) -> Result<Element, JsValue> {
        let link = self.empty_node_link_element(node, is_new)?;
        self.append_node_group_to_link(&link, node)?;
        Ok(link)
    }

    fn empty_node_link_element(
        &self,
        node: &GraphViewportNode,
        is_new: bool,
    ) -> Result<Element, JsValue> {
        let link = svg_element(&self.document, "a")?;
        set_node_link_attributes(&link, node, is_new)?;
        Ok(link)
    }

    fn append_node_group_to_link(
        &self,
        link: &Element,
        node: &GraphViewportNode,
    ) -> Result<(), JsValue> {
        let group = self.node_group_element(node)?;
        link.append_child(&group)?;
        Ok(())
    }

    fn node_group_element(&self, node: &GraphViewportNode) -> Result<Element, JsValue> {
        let group = self.empty_node_group_element(node)?;
        self.append_node_group_content(&group, node)?;
        Ok(group)
    }

    fn empty_node_group_element(&self, node: &GraphViewportNode) -> Result<Element, JsValue> {
        let group = svg_element(&self.document, "g")?;
        set_node_group_attributes(&group, node)?;
        Ok(group)
    }

    fn append_node_group_content(
        &self,
        group: &Element,
        node: &GraphViewportNode,
    ) -> Result<(), JsValue> {
        self.append_node_identity_content(group, node)?;
        self.append_node_text_content(group, node)
    }

    fn append_node_identity_content(
        &self,
        group: &Element,
        node: &GraphViewportNode,
    ) -> Result<(), JsValue> {
        self.append_node_title(group, node)?;
        self.append_node_core(group)
    }

    fn append_node_text_content(
        &self,
        group: &Element,
        node: &GraphViewportNode,
    ) -> Result<(), JsValue> {
        self.append_node_label(group, node)?;
        self.append_node_kind(group, node)
    }

    fn append_node_title(&self, group: &Element, node: &GraphViewportNode) -> Result<(), JsValue> {
        let title = self.node_title_element(node)?;
        group.append_child(&title)?;
        Ok(())
    }

    fn node_title_element(&self, node: &GraphViewportNode) -> Result<Element, JsValue> {
        let title = svg_element(&self.document, "title")?;
        title.set_text_content(Some(&node_title_text(node)));
        Ok(title)
    }

    fn append_node_core(&self, group: &Element) -> Result<(), JsValue> {
        let core = self.node_core_element()?;
        group.append_child(&core)?;
        Ok(())
    }

    fn node_core_element(&self) -> Result<Element, JsValue> {
        let core = svg_element(&self.document, "circle")?;
        set_attributes(
            &core,
            [("class", "core".to_owned()), ("r", NODE_RADIUS.to_string())],
        )?;
        Ok(core)
    }

    fn append_node_label(&self, group: &Element, node: &GraphViewportNode) -> Result<(), JsValue> {
        let label = self.node_label_element(node)?;
        group.append_child(&label)?;
        Ok(())
    }

    fn node_label_element(&self, node: &GraphViewportNode) -> Result<Element, JsValue> {
        self.node_text_element("node-label", "44", node_label(node))
    }

    fn append_node_kind(&self, group: &Element, node: &GraphViewportNode) -> Result<(), JsValue> {
        let kind = self.node_kind_element(node)?;
        group.append_child(&kind)?;
        Ok(())
    }

    fn node_kind_element(&self, node: &GraphViewportNode) -> Result<Element, JsValue> {
        self.node_text_element("node-kind", "58", node.kind.clone())
    }

    fn node_text_element(&self, class: &str, y: &str, text: String) -> Result<Element, JsValue> {
        let element = svg_element(&self.document, "text")?;
        set_attributes(&element, [("class", class.to_owned()), ("y", y.to_owned())])?;
        element.set_text_content(Some(&text));
        Ok(element)
    }

    fn append_node(
        &mut self,
        key: &str,
        link: &Element,
        fingerprint: String,
    ) -> Result<(), JsValue> {
        self.node_group.append_child(link)?;
        self.rendered.nodes.insert(key.to_owned(), fingerprint);
        Ok(())
    }

    fn remove_key(&mut self, key: &str) {
        if let Some(element) = self.document.get_element_by_id(&render_element_id(key)) {
            element.remove();
        }
        self.rendered.lanes.remove(key);
        self.rendered.nodes.remove(key);
        self.rendered.edges.remove(key);
    }

    fn hide_status(&self) {
        if let Some(status) = &self.status {
            status.set_text_content(Some(""));
            let _ = status.set_attribute("hidden", "hidden");
        }
    }

    fn show_loading_status(&self) {
        if let Some(status) = &self.status {
            status.set_text_content(Some("Loading graph..."));
            let _ = status.remove_attribute("hidden");
        }
    }

    fn set_root_version(&self) {
        if let Some(root) = self.document.get_element_by_id(ROOT_ID) {
            let _ = root.set_attribute("data-version", &self.version.to_string());
        }
    }

    fn viewport_update_active(&self) -> bool {
        viewport_update_active(self.patch_in_flight, self.pending_viewport_update)
    }

    fn abort_version_refresh(&mut self) {
        if let Some(controller) = self.version_refresh_abort.take() {
            controller.abort();
        }
    }
}

impl VirtualGraphElements {
    fn query(document: &Document) -> Result<Self, JsValue> {
        Ok(Self {
            root: query_graph_root_elements(document)?,
            layers: query_graph_layer_elements(document)?,
            viewport_map: query_viewport_map_elements(document)?,
            status: query_optional(document, ".graph-status"),
        })
    }
}

fn query_optional(document: &Document, selector: &str) -> Option<Element> {
    document.query_selector(selector).ok().flatten()
}

fn query_graph_root_elements(document: &Document) -> Result<GraphRootElements, JsValue> {
    Ok(GraphRootElements {
        graph_wrap: query_required(document, ".graph-wrap")?,
        graph_svg: query_required(document, ".graph")?,
        graph_bg: query_required(document, ".graph-bg")?,
    })
}

fn query_graph_layer_elements(document: &Document) -> Result<GraphLayerElements, JsValue> {
    Ok(GraphLayerElements {
        lane_group: query_required(document, ".graph-lanes")?,
        edge_group: query_required(document, ".graph-edges")?,
        node_group: query_required(document, ".graph-nodes")?,
    })
}

fn query_viewport_map_elements(document: &Document) -> Result<ViewportMapElements, JsValue> {
    Ok(ViewportMapElements {
        viewport_map: query_required(document, ".viewport-map")?,
        viewport_map_bg: query_required(document, ".viewport-map-bg")?,
        viewport_map_window: query_required(document, ".viewport-map-window")?,
    })
}

fn initial_zoom(graph_wrap: &Element) -> f64 {
    graph_wrap
        .get_attribute("data-zoom")
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(1.0)
        .clamp(MIN_ZOOM, MAX_ZOOM)
}

fn initial_viewport(window: &Window, graph_wrap: &Element, zoom: f64) -> ViewportState {
    ViewportState::load(window).unwrap_or_else(|| viewport_from_element(graph_wrap, 0.0, 0.0, zoom))
}

impl ViewportState {
    fn load(window: &Window) -> Option<Self> {
        let value = stored_viewport_value(window)?;
        parse_stored_viewport(&value)
    }
}

fn stored_viewport_value(window: &Window) -> Option<String> {
    session_storage(window)?
        .get_item(VIEWPORT_KEY)
        .ok()
        .flatten()
}

fn parse_stored_viewport(value: &str) -> Option<ViewportState> {
    let mut parts = value.split(',');
    let (x, y) = parse_next_viewport_pair(&mut parts)?;
    let (width, height) = parse_next_viewport_pair(&mut parts)?;
    let overscan = parse_next_viewport_value(&mut parts)?;
    Some(ViewportState {
        x,
        y,
        width,
        height,
        overscan,
    })
}

fn parse_next_viewport_pair(parts: &mut Split<'_, char>) -> Option<(f64, f64)> {
    Some((
        parse_next_viewport_value(parts)?,
        parse_next_viewport_value(parts)?,
    ))
}

fn parse_next_viewport_value<T>(parts: &mut Split<'_, char>) -> Option<T>
where
    T: FromStr,
{
    parts.next()?.parse().ok()
}

async fn render_full_viewport(graph: Rc<RefCell<VirtualGraph>>) -> Result<(), JsValue> {
    let (window, query) = {
        let mut graph = graph.borrow_mut();
        graph.resize_viewport();
        (graph.window.clone(), graph.viewport.request_query())
    };
    let response =
        fetch_json::<GraphViewportResponse>(&window, &format!("/api/graph/viewport?{query}"))
            .await?;
    let patch_needed = {
        let mut graph = graph.borrow_mut();
        graph.apply_full(response)?;
        !same_viewport(graph.rendered_viewport, graph.viewport)
    };
    if patch_needed {
        request_viewport_patch(graph);
    }
    Ok(())
}

fn request_viewport_patch(graph: Rc<RefCell<VirtualGraph>>) {
    request_viewport_update(graph, PendingViewportUpdate::Patch);
}

fn request_viewport_update(graph: Rc<RefCell<VirtualGraph>>, update: PendingViewportUpdate) {
    let should_spawn = {
        let mut graph = graph.borrow_mut();
        graph.pending_viewport_update = graph.pending_viewport_update.merge(update);
        if graph.patch_in_flight {
            false
        } else {
            graph.patch_in_flight = true;
            true
        }
    };

    if should_spawn {
        spawn_local(drain_viewport_patches(graph));
    }
}

fn update_viewport<F>(graph: Rc<RefCell<VirtualGraph>>, update: F)
where
    F: FnOnce(&mut VirtualGraph),
{
    let pending_update = {
        let mut graph = graph.borrow_mut();
        graph.abort_version_refresh();
        let previous = graph.viewport;
        update(&mut graph);
        graph.clamp_viewport();
        let pending_update = pending_update_for_viewport_change(previous, graph.viewport);
        let _ = graph.apply_canvas();
        graph.persist_viewport();
        pending_update
    };
    request_viewport_update(graph, pending_update);
}

fn routed_edge_style(kind: GraphViewportEdgeKind) -> (&'static str, &'static str) {
    match kind {
        GraphViewportEdgeKind::Fork => ("edge fork", "url(#fork-arrowhead)"),
        GraphViewportEdgeKind::MergeParent => ("edge merge-parent", "url(#merge-arrowhead)"),
        GraphViewportEdgeKind::PrimaryParent => unreachable!("primary edges use line elements"),
    }
}

fn apply_graph_viewport(
    graph_svg: &Element,
    graph_wrap: &Element,
    viewport: ViewportState,
    zoom: f64,
) -> Result<(), JsValue> {
    set_attributes(
        graph_svg,
        [(
            "viewBox",
            format!(
                "{} {} {} {}",
                rounded_i32(viewport.x),
                rounded_i32(viewport.y),
                rounded_i32(viewport.width),
                rounded_i32(viewport.height)
            ),
        )],
    )?;
    set_attributes(
        graph_wrap,
        [
            ("data-viewport-x", rounded_i32(viewport.x).to_string()),
            ("data-viewport-y", rounded_i32(viewport.y).to_string()),
            ("data-zoom", format!("{zoom:.3}")),
        ],
    )
}

fn apply_canvas_dimensions(
    canvas: Option<GraphCanvas>,
    graph_bg: &Element,
    viewport_map: &Element,
    viewport_map_bg: &Element,
) -> Result<(), JsValue> {
    let Some(canvas) = canvas else {
        return Ok(());
    };
    apply_canvas_background(graph_bg, canvas)?;
    apply_viewport_map_canvas(viewport_map, viewport_map_bg, canvas)
}

fn apply_canvas_background(graph_bg: &Element, canvas: GraphCanvas) -> Result<(), JsValue> {
    set_attributes(
        graph_bg,
        [
            ("x", "0".to_string()),
            ("y", "0".to_string()),
            ("width", canvas.width.to_string()),
            ("height", canvas.height.to_string()),
        ],
    )
}

fn apply_viewport_map_canvas(
    viewport_map: &Element,
    viewport_map_bg: &Element,
    canvas: GraphCanvas,
) -> Result<(), JsValue> {
    set_attributes(
        viewport_map,
        [
            ("viewBox", format!("0 0 {} {}", canvas.width, canvas.height)),
            ("data-graph-width", canvas.width.to_string()),
            ("data-graph-height", canvas.height.to_string()),
        ],
    )?;
    set_attributes(
        viewport_map_bg,
        [
            ("width", canvas.width.to_string()),
            ("height", canvas.height.to_string()),
        ],
    )
}

fn apply_viewport_map_window(
    viewport_map_window: &Element,
    viewport: ViewportState,
) -> Result<(), JsValue> {
    set_attributes(
        viewport_map_window,
        [
            ("x", rounded_i32(viewport.x).to_string()),
            ("y", rounded_i32(viewport.y).to_string()),
            ("width", rounded_i32(viewport.width).to_string()),
            ("height", rounded_i32(viewport.height).to_string()),
        ],
    )
}

fn set_attributes<const N: usize>(
    element: &Element,
    attributes: [(&str, String); N],
) -> Result<(), JsValue> {
    for (name, value) in attributes {
        element.set_attribute(name, &value)?;
    }
    Ok(())
}

fn set_node_link_attributes(
    link: &Element,
    node: &GraphViewportNode,
    is_new: bool,
) -> Result<(), JsValue> {
    set_attributes(
        link,
        [
            ("id", render_element_id(&node.key)),
            ("data-render-key", node.key.clone()),
            ("class", node_link_class(is_new).to_owned()),
            ("href", format!("#{}", node.node_target)),
            ("data-node-target", node.node_target.clone()),
            ("data-node-id", node.id.clone()),
        ],
    )
}

fn node_link_class(is_new: bool) -> &'static str {
    if is_new {
        "node-link node-new"
    } else {
        "node-link"
    }
}

fn set_node_group_attributes(group: &Element, node: &GraphViewportNode) -> Result<(), JsValue> {
    set_attributes(
        group,
        [
            ("class", node_group_class(node)),
            ("transform", format!("translate({} {})", node.x, node.y)),
        ],
    )
}

fn node_group_class(node: &GraphViewportNode) -> String {
    let mut class = format!("node {}", css_token(&node.kind));
    if !node.labels.is_empty() {
        class.push_str(" active");
    }
    class
}

fn node_title_text(node: &GraphViewportNode) -> String {
    format!("{}: {}", node.short_id, node.summary)
}

async fn drain_viewport_patches(graph: Rc<RefCell<VirtualGraph>>) {
    while render_next_viewport_patch_or_stop(graph.clone()).await {}
}

async fn render_next_viewport_patch_or_stop(graph: Rc<RefCell<VirtualGraph>>) -> bool {
    match render_next_viewport_patch(graph.clone()).await {
        Ok(should_continue) => should_continue,
        Err(error) => {
            web_sys::console::error_1(&error);
            graph.borrow_mut().patch_in_flight = false;
            false
        }
    }
}

async fn render_next_viewport_patch(graph: Rc<RefCell<VirtualGraph>>) -> Result<bool, JsValue> {
    let input = next_viewport_patch_input(graph.clone());
    match input.fetch {
        ViewportFetch::None => finish_idle_viewport_patch(graph),
        ViewportFetch::Full => render_full_viewport_patch(graph, input).await,
        ViewportFetch::Patch => render_diff_viewport_patch(graph, input).await,
    }
}

fn next_viewport_patch_input(graph: Rc<RefCell<VirtualGraph>>) -> ViewportPatchInput {
    let mut graph = graph.borrow_mut();
    let update = graph.pending_viewport_update;
    graph.pending_viewport_update = PendingViewportUpdate::None;
    let rendered = graph.rendered_viewport;
    let current = graph.viewport;
    ViewportPatchInput {
        window: graph.window.clone(),
        current,
        fetch: next_viewport_fetch(rendered, current, update),
    }
}

fn finish_idle_viewport_patch(graph: Rc<RefCell<VirtualGraph>>) -> Result<bool, JsValue> {
    let mut graph = graph.borrow_mut();
    if graph.pending_viewport_update.is_pending() {
        return Ok(true);
    }
    graph.patch_in_flight = false;
    Ok(false)
}

async fn render_full_viewport_patch(
    graph: Rc<RefCell<VirtualGraph>>,
    input: ViewportPatchInput,
) -> Result<bool, JsValue> {
    graph.borrow().show_loading_status();
    let query = input.current.request_query();
    let response =
        fetch_json::<GraphViewportResponse>(&input.window, &format!("/api/graph/viewport?{query}"))
            .await?;
    let mut graph = graph.borrow_mut();
    graph.apply_full(response)?;
    Ok(finish_applied_viewport_patch(&mut graph))
}

async fn render_diff_viewport_patch(
    graph: Rc<RefCell<VirtualGraph>>,
    input: ViewportPatchInput,
) -> Result<bool, JsValue> {
    let query = viewport_patch_diff_query(&graph.borrow(), input.current);
    let response = fetch_json_form::<GraphViewportDiffResponse>(
        &input.window,
        "/api/graph/viewport/diff",
        &query,
    )
    .await?;
    let mut graph = graph.borrow_mut();
    graph.apply_diff(response)?;
    Ok(finish_applied_viewport_patch(&mut graph))
}

fn viewport_patch_diff_query(graph: &VirtualGraph, current: ViewportState) -> String {
    let known_query = graph.rendered.known_query();
    let mut query = format!("{}&known=1", current.request_query());
    append_known_query(&mut query, &known_query);
    query
}

fn finish_applied_viewport_patch(graph: &mut VirtualGraph) -> bool {
    let should_continue = graph.pending_viewport_update.is_pending()
        || !same_viewport(graph.rendered_viewport, graph.viewport);
    if !should_continue {
        graph.patch_in_flight = false;
    }
    should_continue
}

async fn refresh_graph_items_on_version(graph: Rc<RefCell<VirtualGraph>>) {
    refresh_on_version(graph, refresh_graph_items_once).await;
}

async fn refresh_graph_items_once(graph: Rc<RefCell<VirtualGraph>>) {
    let Some(input) = graph_items_refresh_input(graph.clone()) else {
        delay_graph_items_retry(graph).await;
        return;
    };
    let Some(controller) = begin_graph_items_refresh(graph.clone()) else {
        return;
    };
    let response = fetch_graph_items_diff(&input, &controller).await;
    clear_version_refresh(graph.clone());
    handle_graph_items_response(graph, input.viewport, response).await;
}

fn graph_items_refresh_input(graph: Rc<RefCell<VirtualGraph>>) -> Option<GraphItemsRefreshInput> {
    let graph = graph.borrow();
    (!graph.viewport_update_active()).then(|| GraphItemsRefreshInput {
        window: graph.window.clone(),
        viewport: graph.viewport,
        query: graph_items_refresh_query(
            graph.version,
            graph.viewport,
            graph.canvas,
            graph.rendered.known_query(),
        ),
    })
}

fn graph_items_refresh_query(
    version: u64,
    viewport: ViewportState,
    canvas: Option<GraphCanvas>,
    known_query: String,
) -> String {
    let mut query = format!("version={version}&{}&known=1", viewport.request_query());
    append_canvas_query(&mut query, canvas);
    append_known_query(&mut query, &known_query);
    query
}

fn append_canvas_query(query: &mut String, canvas: Option<GraphCanvas>) {
    if let Some(canvas) = canvas {
        query.push_str(&format!(
            "&canvas_width={}&canvas_height={}",
            canvas.width, canvas.height
        ));
    }
}

fn append_known_query(query: &mut String, known_query: &str) {
    if !known_query.is_empty() {
        query.push('&');
        query.push_str(known_query);
    }
}

fn begin_graph_items_refresh(graph: Rc<RefCell<VirtualGraph>>) -> Option<AbortController> {
    match begin_version_refresh(graph) {
        Ok(controller) => controller,
        Err(error) => {
            web_sys::console::error_1(&error);
            None
        }
    }
}

async fn fetch_graph_items_diff(
    input: &GraphItemsRefreshInput,
    controller: &AbortController,
) -> Result<GraphViewportDiffResponse, JsValue> {
    fetch_json_form_with_signal::<GraphViewportDiffResponse>(
        &input.window,
        "/api/graph/viewport/items/diff",
        &input.query,
        &controller.signal(),
    )
    .await
}

async fn handle_graph_items_response(
    graph: Rc<RefCell<VirtualGraph>>,
    viewport: ViewportState,
    response: Result<GraphViewportDiffResponse, JsValue>,
) {
    match response {
        Ok(response) => handle_graph_items_success(graph, viewport, response).await,
        Err(error) => handle_graph_items_error(graph, error),
    }
}

async fn handle_graph_items_success(
    graph: Rc<RefCell<VirtualGraph>>,
    viewport: ViewportState,
    response: GraphViewportDiffResponse,
) {
    match graph_items_refresh_action(graph.clone(), viewport) {
        VersionRefresh::Drop => {}
        VersionRefresh::Defer => delay_graph_items_retry(graph).await,
        VersionRefresh::Apply => apply_graph_items_diff(graph, response),
    }
}

fn graph_items_refresh_action(
    graph: Rc<RefCell<VirtualGraph>>,
    viewport: ViewportState,
) -> VersionRefresh {
    let graph = graph.borrow();
    version_refresh_action(
        viewport,
        graph.viewport,
        graph.patch_in_flight,
        graph.pending_viewport_update,
    )
}

fn apply_graph_items_diff(graph: Rc<RefCell<VirtualGraph>>, response: GraphViewportDiffResponse) {
    if let Err(error) = graph.borrow_mut().apply_diff(response) {
        web_sys::console::error_1(&error);
    }
}

fn handle_graph_items_error(graph: Rc<RefCell<VirtualGraph>>, error: JsValue) {
    if !graph.borrow().viewport_update_active() {
        web_sys::console::error_1(&error);
    }
}

fn begin_version_refresh(
    graph: Rc<RefCell<VirtualGraph>>,
) -> Result<Option<AbortController>, JsValue> {
    let mut graph = graph.borrow_mut();
    if graph.viewport_update_active() {
        return Ok(None);
    }
    let controller = AbortController::new()?;
    graph.version_refresh_abort = Some(controller.clone());
    Ok(Some(controller))
}

fn clear_version_refresh(graph: Rc<RefCell<VirtualGraph>>) {
    graph.borrow_mut().version_refresh_abort = None;
}

async fn delay_ms(window: &Window, delay_ms: i32) -> Result<(), JsValue> {
    let promise = js_sys::Promise::new(&mut |resolve, reject| {
        let callback = Closure::once_into_js(move || {
            let _ = resolve.call0(&JsValue::UNDEFINED);
        });
        if let Err(error) = window.set_timeout_with_callback_and_timeout_and_arguments_0(
            callback.unchecked_ref(),
            delay_ms,
        ) {
            let _ = reject.call1(&JsValue::UNDEFINED, &error);
        }
    });
    JsFuture::from(promise).await.map(|_| ())
}

async fn refresh_server_rendered_sections_on_version(graph: Rc<RefCell<VirtualGraph>>) {
    refresh_on_version(graph, refresh_server_rendered_sections_once).await;
}

async fn refresh_on_version<F, Fut>(graph: Rc<RefCell<VirtualGraph>>, refresh_once: F)
where
    F: Fn(Rc<RefCell<VirtualGraph>>) -> Fut,
    Fut: Future<Output = ()>,
{
    loop {
        refresh_once(graph.clone()).await;
    }
}

async fn refresh_server_rendered_sections_once(graph: Rc<RefCell<VirtualGraph>>) {
    let (window, document, url) = server_rendered_sections_request(&graph);
    let response = refresh_server_rendered_sections_from_url(&window, &document, &url).await;
    handle_server_rendered_sections_response(graph, &window, response).await;
}

fn server_rendered_sections_request(
    graph: &Rc<RefCell<VirtualGraph>>,
) -> (Window, Document, String) {
    let graph = graph.borrow();
    (
        graph.window.clone(),
        graph.document.clone(),
        format!("/fragment?version={}", graph.shell_version),
    )
}

async fn handle_server_rendered_sections_response(
    graph: Rc<RefCell<VirtualGraph>>,
    window: &Window,
    response: Result<Option<u64>, JsValue>,
) {
    match response {
        Ok(Some(version)) => graph.borrow_mut().shell_version = version,
        Ok(None) => {}
        Err(error) => {
            web_sys::console::error_1(&error);
            delay_window_retry(window).await;
        }
    }
}

async fn delay_graph_items_retry(graph: Rc<RefCell<VirtualGraph>>) {
    let window = graph.borrow().window.clone();
    delay_window_retry(&window).await;
}

async fn delay_window_retry(window: &Window) {
    if let Err(error) = delay_ms(window, VERSION_REFRESH_RETRY_MS).await {
        web_sys::console::error_1(&error);
    }
}

async fn refresh_server_rendered_sections_from_url(
    window: &Window,
    document: &Document,
    url: &str,
) -> Result<Option<u64>, JsValue> {
    let html = fetch_text(window, url).await?;
    let container = document.create_element("div")?;
    container.set_inner_html(&html);
    let version = fragment_version(&container);
    refresh_server_fragment_sections(document, &container)?;
    refresh_selected_node_detail_if_needed(window, document).await?;
    Ok(version)
}

fn fragment_version(container: &Element) -> Option<u64> {
    container
        .query_selector("#console-root")
        .ok()
        .flatten()?
        .get_attribute("data-version")?
        .parse()
        .ok()
}

fn refresh_server_fragment_sections(
    document: &Document,
    container: &Element,
) -> Result<(), JsValue> {
    refresh_text_content(document, container, ".stats")?;
    refresh_text_content(document, container, "#selection-style")?;
    refresh_inner_html(document, container, ".branch-section")
}

async fn refresh_selected_node_detail_if_needed(
    window: &Window,
    document: &Document,
) -> Result<(), JsValue> {
    if selected_node_target(window).is_some() {
        refresh_selected_node_detail(window, document).await?;
    }
    Ok(())
}

fn refresh_text_content(
    document: &Document,
    container: &Element,
    selector: &str,
) -> Result<(), JsValue> {
    let Some(source) = container.query_selector(selector)? else {
        return Ok(());
    };
    if let Some(target) = document.query_selector(selector)? {
        target.set_text_content(source.text_content().as_deref());
    }
    Ok(())
}

fn refresh_inner_html(
    document: &Document,
    container: &Element,
    selector: &str,
) -> Result<(), JsValue> {
    let Some(source) = container.query_selector(selector)? else {
        return Ok(());
    };
    if let Some(target) = document.query_selector(selector)? {
        target.set_inner_html(&source.inner_html());
    }
    Ok(())
}

fn install_graph_listeners(graph: Rc<RefCell<VirtualGraph>>) -> Result<(), JsValue> {
    let installers: [GraphListenerInstaller; 5] = [
        install_node_detail_listener,
        install_wheel_listener,
        install_resize_listener,
        install_viewport_map_listener,
        install_hashchange_node_detail_listener,
    ];
    for install in installers {
        install(graph.clone())?;
    }
    Ok(())
}

fn install_wheel_listener(graph: Rc<RefCell<VirtualGraph>>) -> Result<(), JsValue> {
    let graph_wrap = graph.borrow().graph_wrap.clone();
    let wheel_graph = graph.clone();
    let wheel_closure = Closure::<dyn FnMut(WheelEvent)>::new(move |event: WheelEvent| {
        event.prevent_default();
        update_viewport(wheel_graph.clone(), |graph| {
            if event.ctrl_key() || event.meta_key() {
                zoom_from_wheel(graph, &event);
            } else {
                pan_from_wheel(graph, &event);
            }
        });
    });
    graph_wrap.add_event_listener_with_callback("wheel", wheel_closure.as_ref().unchecked_ref())?;
    wheel_closure.forget();
    Ok(())
}

fn install_resize_listener(graph: Rc<RefCell<VirtualGraph>>) -> Result<(), JsValue> {
    let resize_graph = graph.clone();
    let resize_window = graph.borrow().window.clone();
    let resize_closure = Closure::<dyn FnMut()>::new(move || {
        update_viewport(resize_graph.clone(), |graph| {
            graph.resize_viewport();
        });
    });
    resize_window
        .add_event_listener_with_callback("resize", resize_closure.as_ref().unchecked_ref())?;
    resize_closure.forget();
    Ok(())
}

fn install_viewport_map_listener(graph: Rc<RefCell<VirtualGraph>>) -> Result<(), JsValue> {
    let viewport_map = graph.borrow().viewport_map.clone();
    let viewport_map_graph = graph.clone();
    let viewport_map_closure = Closure::<dyn FnMut(MouseEvent)>::new(move |event: MouseEvent| {
        event.prevent_default();
        update_viewport(viewport_map_graph.clone(), |graph| {
            center_viewport_from_map(graph, &event);
        });
    });
    viewport_map
        .add_event_listener_with_callback("click", viewport_map_closure.as_ref().unchecked_ref())?;
    viewport_map_closure.forget();
    Ok(())
}

fn install_hashchange_node_detail_listener(
    graph: Rc<RefCell<VirtualGraph>>,
) -> Result<(), JsValue> {
    let detail_graph = graph.clone();
    let detail_window = graph.borrow().window.clone();
    let detail_closure = Closure::<dyn FnMut()>::new(move || {
        let graph = detail_graph.clone();
        spawn_local(async move {
            if let Err(error) = refresh_selected_node_detail_from_graph(graph).await {
                web_sys::console::error_1(&error);
            }
        });
    });
    detail_window
        .add_event_listener_with_callback("hashchange", detail_closure.as_ref().unchecked_ref())?;
    detail_closure.forget();

    Ok(())
}

fn install_node_detail_listener(graph: Rc<RefCell<VirtualGraph>>) -> Result<(), JsValue> {
    let node_group = graph.borrow().node_group.clone();
    let detail_graph = graph.clone();
    let detail_closure = Closure::<dyn FnMut(MouseEvent)>::new(move |event: MouseEvent| {
        let Some(link) = node_link_from_event(&event) else {
            return;
        };
        let Some(target) = link.get_attribute("data-node-target") else {
            return;
        };

        event.prevent_default();
        event.stop_propagation();
        select_node_detail(detail_graph.clone(), target);
    });
    node_group
        .add_event_listener_with_callback("click", detail_closure.as_ref().unchecked_ref())?;
    detail_closure.forget();
    Ok(())
}

fn node_link_from_event(event: &MouseEvent) -> Option<Element> {
    let target = event.target()?.dyn_into::<Element>().ok()?;
    target.closest(".node-link").ok().flatten()
}

fn select_node_detail(graph: Rc<RefCell<VirtualGraph>>, target: String) {
    let (window, document) = {
        let graph = graph.borrow();
        (graph.window.clone(), graph.document.clone())
    };

    if selected_node_target(&window).as_deref() != Some(target.as_str())
        && let Err(error) = window.location().set_hash(&target)
    {
        web_sys::console::error_1(&error);
        return;
    }

    spawn_local(async move {
        if let Err(error) = refresh_selected_node_detail(&window, &document).await {
            web_sys::console::error_1(&error);
        }
    });
}

async fn refresh_selected_node_detail_from_graph(
    graph: Rc<RefCell<VirtualGraph>>,
) -> Result<(), JsValue> {
    let (window, document) = {
        let graph = graph.borrow();
        (graph.window.clone(), graph.document.clone())
    };
    refresh_selected_node_detail(&window, &document).await
}

async fn refresh_selected_node_detail(window: &Window, document: &Document) -> Result<(), JsValue> {
    let target = selected_node_target(window);
    render_loading_node_detail_if_current(window, document, target.as_deref())?;
    let url = node_detail_url(target.as_deref());
    let html = fetch_text(window, &url).await?;
    render_node_detail_if_current(window, document, target, &html)
}

fn node_detail_url(target: Option<&str>) -> String {
    target
        .map(|target| format!("/api/node-detail?target={}", percent_encode(target)))
        .unwrap_or_else(|| "/api/node-detail".to_owned())
}

fn render_node_detail_if_current(
    window: &Window,
    document: &Document,
    target: Option<String>,
    html: &str,
) -> Result<(), JsValue> {
    if selected_node_target(window) == target {
        let slot = query_required(document, ".node-detail-slot")?;
        slot.set_inner_html(html);
        mark_selected_node_detail(&slot)?;
    }
    Ok(())
}

fn render_loading_node_detail_if_current(
    window: &Window,
    document: &Document,
    target: Option<&str>,
) -> Result<(), JsValue> {
    let Some(target) = target else {
        return clear_selected_node_detail(document);
    };
    if selected_node_target(window).as_deref() != Some(target) {
        return Ok(());
    }
    let detail = loading_node_detail(document, target)?;
    replace_node_detail_slot(document, &detail)
}

fn replace_node_detail_slot(document: &Document, detail: &Element) -> Result<(), JsValue> {
    let slot = query_required(document, ".node-detail-slot")?;
    slot.set_inner_html("");
    slot.append_child(detail)?;
    Ok(())
}

fn clear_selected_node_detail(document: &Document) -> Result<(), JsValue> {
    let Some(detail) = document.query_selector(".node-detail.node-detail-selected")? else {
        return Ok(());
    };
    detail.class_list().remove_1("node-detail-selected")
}

fn loading_node_detail(document: &Document, target: &str) -> Result<Element, JsValue> {
    let section = node_detail_section(document, target)?;
    append_text_child(document, &section, "h2", "Node")?;
    let list = loading_detail_list(document)?;
    section.append_child(&list)?;
    Ok(section)
}

fn node_detail_section(document: &Document, target: &str) -> Result<Element, JsValue> {
    let section = document.create_element("section")?;
    set_attributes(
        &section,
        [
            ("id", target.to_owned()),
            (
                "class",
                "node-details node-detail node-detail-selected".to_owned(),
            ),
        ],
    )?;
    Ok(section)
}

fn loading_detail_list(document: &Document) -> Result<Element, JsValue> {
    let list = classed_element(document, "dl", "detail-list")?;
    let row = detail_row(document, "Selection", "Loading node detail...")?;
    list.append_child(&row)?;
    Ok(list)
}

fn detail_row(document: &Document, term: &str, detail: &str) -> Result<Element, JsValue> {
    let row = document.create_element("div")?;
    append_text_child(document, &row, "dt", term)?;
    append_text_child(document, &row, "dd", detail)?;
    Ok(row)
}

fn classed_element(document: &Document, tag: &str, class: &str) -> Result<Element, JsValue> {
    let element = document.create_element(tag)?;
    element.set_attribute("class", class)?;
    Ok(element)
}

fn append_text_child(
    document: &Document,
    parent: &Element,
    tag: &str,
    text: &str,
) -> Result<(), JsValue> {
    let child = text_element(document, tag, text)?;
    parent.append_child(&child)?;
    Ok(())
}

fn text_element(document: &Document, tag: &str, text: &str) -> Result<Element, JsValue> {
    let element = document.create_element(tag)?;
    element.set_text_content(Some(text));
    Ok(element)
}

fn mark_selected_node_detail(slot: &Element) -> Result<(), JsValue> {
    let Some(detail) = slot.query_selector(".node-detail")? else {
        return Ok(());
    };
    let class = detail.get_attribute("class").unwrap_or_default();
    if !class
        .split_ascii_whitespace()
        .any(|part| part == "node-detail-selected")
    {
        detail.set_attribute("class", &format!("{class} node-detail-selected"))?;
    }
    Ok(())
}

fn selected_node_target(window: &Window) -> Option<String> {
    let hash = window.location().hash().ok()?;
    let target = hash.strip_prefix('#')?;
    (!target.is_empty() && target.starts_with("detail-")).then(|| target.to_owned())
}

fn pan_from_wheel(graph: &mut VirtualGraph, event: &WheelEvent) {
    let delta_x = event.delta_x();
    let delta_y = event.delta_y();
    if event.shift_key() && delta_x.abs() < 0.1 {
        graph.viewport.x += delta_y / graph.zoom;
    } else {
        graph.viewport.x += delta_x / graph.zoom;
        graph.viewport.y += delta_y / graph.zoom;
    }
}

fn zoom_from_wheel(graph: &mut VirtualGraph, event: &WheelEvent) {
    let rect = graph.graph_wrap.get_bounding_client_rect();
    let local_x = f64::from(event.client_x()) - rect.left();
    let local_y = f64::from(event.client_y()) - rect.top();
    let anchor_x = graph.viewport.x + local_x / graph.zoom;
    let anchor_y = graph.viewport.y + local_y / graph.zoom;
    let next_zoom = (graph.zoom * (-event.delta_y() * 0.001).exp()).clamp(MIN_ZOOM, MAX_ZOOM);
    graph.zoom = next_zoom;
    graph.viewport.width = graph_client_width(&graph.graph_wrap) / graph.zoom;
    graph.viewport.height = graph_client_height(&graph.graph_wrap) / graph.zoom;
    graph.viewport.refresh_render_overscan();
    graph.viewport.x = anchor_x - local_x / graph.zoom;
    graph.viewport.y = anchor_y - local_y / graph.zoom;
}

fn center_viewport_from_map(graph: &mut VirtualGraph, event: &MouseEvent) {
    let Some(canvas) = graph.canvas else {
        return;
    };
    let rect = graph.viewport_map.get_bounding_client_rect();
    let canvas_width = f64::from(canvas.width);
    let canvas_height = f64::from(canvas.height);
    let Some(content) =
        viewport_map_content_bounds(rect.width(), rect.height(), canvas_width, canvas_height)
    else {
        return;
    };
    let local_x = f64::from(event.client_x()) - rect.left();
    let local_y = f64::from(event.client_y()) - rect.top();
    let ratio_x = ((local_x - content.x) / content.width).clamp(0.0, 1.0);
    let ratio_y = ((local_y - content.y) / content.height).clamp(0.0, 1.0);
    graph.viewport.x = ratio_x * canvas_width - graph.viewport.width / 2.0;
    graph.viewport.y = ratio_y * canvas_height - graph.viewport.height / 2.0;
    graph.clamp_viewport();
}

fn viewport_map_content_bounds(
    rect_width: f64,
    rect_height: f64,
    canvas_width: f64,
    canvas_height: f64,
) -> Option<ViewportMapContentBounds> {
    if !positive_dimensions([rect_width, rect_height, canvas_width, canvas_height]) {
        return None;
    }

    let canvas_ratio = canvas_width / canvas_height;
    let element_ratio = rect_width / rect_height;
    if element_ratio > canvas_ratio {
        let content_width = rect_height * canvas_ratio;
        Some(ViewportMapContentBounds {
            x: (rect_width - content_width) / 2.0,
            y: 0.0,
            width: content_width,
            height: rect_height,
        })
    } else {
        let content_height = rect_width / canvas_ratio;
        Some(ViewportMapContentBounds {
            x: 0.0,
            y: (rect_height - content_height) / 2.0,
            width: rect_width,
            height: content_height,
        })
    }
}

fn positive_dimensions(values: [f64; 4]) -> bool {
    values.into_iter().all(|value| value > 0.0)
}

fn browser_window() -> Result<Window, JsValue> {
    web_sys::window().ok_or_else(|| JsValue::from_str("window is unavailable"))
}

fn browser_document(window: &Window) -> Result<Document, JsValue> {
    window
        .document()
        .ok_or_else(|| JsValue::from_str("document is unavailable"))
}

async fn fetch_json<T>(window: &Window, url: &str) -> Result<T, JsValue>
where
    T: for<'de> Deserialize<'de>,
{
    let text = fetch_text(window, url).await?;
    serde_json::from_str(&text).map_err(|error| JsValue::from_str(&error.to_string()))
}

async fn fetch_json_form<T>(window: &Window, url: &str, body: &str) -> Result<T, JsValue>
where
    T: for<'de> Deserialize<'de>,
{
    let text = fetch_text_form(window, url, body, None).await?;
    serde_json::from_str(&text).map_err(|error| JsValue::from_str(&error.to_string()))
}

async fn fetch_json_form_with_signal<T>(
    window: &Window,
    url: &str,
    body: &str,
    signal: &AbortSignal,
) -> Result<T, JsValue>
where
    T: for<'de> Deserialize<'de>,
{
    let text = fetch_text_form(window, url, body, Some(signal)).await?;
    serde_json::from_str(&text).map_err(|error| JsValue::from_str(&error.to_string()))
}

async fn fetch_text(window: &Window, url: &str) -> Result<String, JsValue> {
    let response = JsFuture::from(window.fetch_with_str(url))
        .await?
        .dyn_into::<Response>()?;
    response_text(response).await
}

async fn fetch_text_form(
    window: &Window,
    url: &str,
    body: &str,
    signal: Option<&AbortSignal>,
) -> Result<String, JsValue> {
    let init = RequestInit::new();
    init.set_method("POST");
    init.set_body(&JsValue::from_str(body));
    if let Some(signal) = signal {
        init.set_signal(Some(signal));
    }
    let response = JsFuture::from(window.fetch_with_str_and_init(url, &init))
        .await?
        .dyn_into::<Response>()?;
    response_text(response).await
}

async fn response_text(response: Response) -> Result<String, JsValue> {
    if !response.ok() {
        return Err(JsValue::from_str(&format!(
            "request failed with status {}",
            response.status()
        )));
    }

    let text = JsFuture::from(response.text()?).await?;
    text.as_string()
        .ok_or_else(|| JsValue::from_str("response text is not a string"))
}

fn current_version(document: &Document) -> Option<u64> {
    document
        .get_element_by_id(ROOT_ID)?
        .get_attribute("data-version")?
        .parse()
        .ok()
}

fn query_required(document: &Document, selector: &str) -> Result<Element, JsValue> {
    document
        .query_selector(selector)?
        .ok_or_else(|| JsValue::from_str(&format!("{selector} is unavailable")))
}

fn viewport_from_element(element: &Element, x: f64, y: f64, zoom: f64) -> ViewportState {
    let mut viewport = ViewportState {
        x,
        y,
        width: graph_client_width(element) / zoom,
        height: graph_client_height(element) / zoom,
        overscan: MIN_OVERSCAN,
    };
    viewport.refresh_render_overscan();
    viewport
}

fn graph_client_width(element: &Element) -> f64 {
    element.get_bounding_client_rect().width().max(1.0)
}

fn graph_client_height(element: &Element) -> f64 {
    element.get_bounding_client_rect().height().max(1.0)
}

fn svg_element(document: &Document, tag: &str) -> Result<Element, JsValue> {
    document.create_element_ns(Some(SVG_NS), tag)
}

fn clear_children(element: &Element) {
    element.set_text_content(Some(""));
}

fn render_element_id(key: &str) -> String {
    format!("graph-render-{}", percent_encode(key))
}

fn node_label(node: &GraphViewportNode) -> String {
    if node.labels.is_empty() {
        node.short_id.clone()
    } else {
        format!("{} {}", node.short_id, node.labels.join(", "))
    }
}

fn line_points(
    source: Point,
    target: Point,
    target_port_offset: f64,
) -> (String, String, String, String) {
    let dx = f64::from(target.x - source.x);
    let target_y = f64::from(target.y) + target_port_offset;
    let dy = target_y - f64::from(source.y);
    let distance = (dx * dx + dy * dy).sqrt();
    if distance <= NODE_RADIUS * 2.0 {
        return (
            f64::from(source.x).to_string(),
            f64::from(source.y).to_string(),
            f64::from(target.x).to_string(),
            target_y.to_string(),
        );
    }

    let ux = dx / distance;
    let uy = dy / distance;
    let start_x = f64::from(source.x) + ux * (NODE_RADIUS + 2.0);
    let start_y = f64::from(source.y) + uy * (NODE_RADIUS + 2.0);
    let end_x = f64::from(target.x) - ux * (NODE_RADIUS + 8.0);
    let end_y = target_y - uy * (NODE_RADIUS + 8.0);

    (
        format!("{start_x:.1}"),
        format!("{start_y:.1}"),
        format!("{end_x:.1}"),
        format!("{end_y:.1}"),
    )
}

fn routed_elbow_points(
    source: Point,
    target: Point,
    route_slot: i32,
    target_port_offset: f64,
) -> String {
    let start_x = f64::from(source.x) + NODE_RADIUS + 2.0;
    let start_y = f64::from(source.y);
    let end_x = if target.x > source.x {
        f64::from(target.x) - NODE_RADIUS - 8.0
    } else {
        f64::from(target.x) + NODE_RADIUS + 8.0
    };
    let end_y = f64::from(target.y) + target_port_offset;
    let exit_x = (start_x + EDGE_NODE_EXIT).min(end_x - EDGE_TARGET_APPROACH);
    let approach_x = (end_x - EDGE_TARGET_APPROACH).max(exit_x + EDGE_TARGET_APPROACH);
    let corridor_y = edge_corridor_y(source.y, target.y, route_slot);

    format!(
        "{start_x:.1},{start_y:.1} {exit_x:.1},{start_y:.1} {exit_x:.1},{corridor_y:.1} {approach_x:.1},{corridor_y:.1} {approach_x:.1},{end_y:.1} {end_x:.1},{end_y:.1}"
    )
}

fn edge_corridor_y(source_y: i32, target_y: i32, route_slot: i32) -> f64 {
    let base_y = match target_y.cmp(&source_y) {
        std::cmp::Ordering::Less => source_y - 70,
        std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => source_y + 70,
    };
    let magnitude = (route_slot + 1) / 2;
    let direction = if route_slot % 2 == 0 { 1.0 } else { -1.0 };
    (f64::from(base_y) + f64::from(magnitude.min(4)) * EDGE_ROUTE_STEP * direction).max(16.0)
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

fn percent_encode(value: &str) -> String {
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

fn session_storage(window: &Window) -> Option<web_sys::Storage> {
    window.session_storage().ok().flatten()
}
