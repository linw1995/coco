use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::rc::Rc;
use std::str::{FromStr, Split};

use serde::Deserialize;
use wasm_bindgen::{JsCast, JsValue, closure::Closure};
use wasm_bindgen_futures::{JsFuture, spawn_local};
use web_sys::{
    AbortController, AbortSignal, Document, Element, EventSource, KeyboardEvent, MessageEvent,
    MouseEvent, RequestInit, Response, WheelEvent, Window,
};

use super::refresh::{
    PendingViewportUpdate, VersionRefresh, ViewportFetch, next_viewport_fetch,
    node_selection_needs_hash_change, pending_update_for_viewport_change, version_refresh_action,
    viewport_update_active,
};
use crate::api::{
    GraphBezierRoute, GraphCanvas, GraphViewport, GraphViewportDiffResponse, GraphViewportEdge,
    GraphViewportEdgeKind, GraphViewportItems, GraphViewportNode, GraphViewportRemovedItem,
    GraphViewportResponse, Point,
};
use crate::panels::PanelSelection;
use crate::viewport::{
    MIN_OVERSCAN, ViewportDrag, ViewportState, rounded_i32, same_viewport, short_canvas_auto_zoom,
};

const ROOT_ID: &str = "console-root";
const SVG_NS: &str = "http://www.w3.org/2000/svg";
const VIEWPORT_KEY: &str = "coco-console:viewport";
const AUTO_FOLLOW_KEY: &str = "coco-console:auto-follow";
const NODE_RADIUS: f64 = 18.0;
const MIN_ZOOM: f64 = 0.25;
const MAX_ZOOM: f64 = 4.0;
const MAX_SHORT_CANVAS_AUTO_ZOOM: f64 = 2.6;
const TIME_SCALE_FOCUS_SIGMA: f64 = 6.4;
const VERSION_REFRESH_RETRY_MS: i32 = 50;

struct GraphItemsRefreshInput {
    window: Window,
    viewport: ViewportState,
    query: String,
}

type GraphListenerInstaller = fn(Rc<RefCell<VirtualGraph>>) -> Result<(), JsValue>;

struct GraphRootElements {
    graph_wrap: Element,
    graph_bg: Element,
    follow_toggle: Element,
}

struct GraphLayerElements {
    edge_group: Element,
    node_group: Element,
}

struct TimeScaleElements {
    time_scale: Element,
    time_scale_track: Element,
    time_scale_cursor: Element,
    time_scale_label: Element,
}

struct VirtualGraphElements {
    root: GraphRootElements,
    layers: GraphLayerElements,
    time_scale: TimeScaleElements,
    status: Option<Element>,
}

struct ViewportPatchInput {
    window: Window,
    current: ViewportState,
    fetch: ViewportFetch,
}

#[cfg_attr(not(test), wasm_bindgen::prelude::wasm_bindgen(start))]
pub fn start() {
    leptos::mount::hydrate_islands();
    spawn_local(async {
        if let Err(error) = run().await {
            web_sys::console::error_1(&error);
        }
    });
}

async fn run() -> Result<(), JsValue> {
    let graph = setup_graph()?;
    render_full_viewport(graph.clone()).await?;
    install_graph_events_or_log(graph.clone());
    refresh_initial_selection(graph).await
}

fn install_graph_events_or_log(graph: Rc<RefCell<VirtualGraph>>) {
    if let Err(error) = install_graph_events(graph) {
        web_sys::console::error_1(&error);
    }
}

async fn refresh_initial_selection(graph: Rc<RefCell<VirtualGraph>>) -> Result<(), JsValue> {
    let has_selected_node = {
        let graph = graph.borrow();
        selected_node_target(&graph.window).is_some()
    };
    if has_selected_node {
        refresh_selected_node_detail_from_graph(graph.clone()).await?;
    }
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

fn install_graph_events(graph: Rc<RefCell<VirtualGraph>>) -> Result<(), JsValue> {
    let source = EventSource::new("/events")?;
    let version_graph = graph.clone();
    let version_callback =
        Closure::<dyn FnMut(MessageEvent)>::wrap(Box::new(move |event: MessageEvent| {
            let Some(data) = event.data().as_string() else {
                return;
            };
            handle_graph_version_event(version_graph.clone(), &data);
        }));
    source.add_event_listener_with_callback("graph", version_callback.as_ref().unchecked_ref())?;
    version_callback.forget();

    graph.borrow_mut().events = Some(source);
    Ok(())
}

fn handle_graph_version_event(graph: Rc<RefCell<VirtualGraph>>, data: &str) {
    match data.parse::<u64>() {
        Ok(version) => request_graph_items_refresh(graph, version),
        Err(error) => web_sys::console::error_1(&JsValue::from_str(&error.to_string())),
    }
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
        .with_render_overscan()
    }
}

struct RenderedKeys {
    nodes: BTreeMap<String, String>,
    edges: BTreeMap<String, String>,
}

impl RenderedKeys {
    fn new() -> Self {
        Self {
            nodes: BTreeMap::new(),
            edges: BTreeMap::new(),
        }
    }

    fn known_query(&self) -> String {
        self.nodes
            .iter()
            .flat_map(|(key, fingerprint)| {
                [
                    format!("known_node={}", percent_encode(key)),
                    format!(
                        "known_node_fingerprint={}:{}",
                        percent_encode(key),
                        fingerprint
                    ),
                ]
            })
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
    graph_mode: String,
    graph_wrap: Element,
    graph_bg: Element,
    follow_toggle: Element,
    edge_group: Element,
    node_group: Element,
    time_scale: Element,
    time_scale_track: Element,
    time_scale_cursor: Element,
    time_scale_label: Element,
    status: Option<Element>,
    viewport: ViewportState,
    zoom: f64,
    auto_follow: bool,
    canvas: Option<GraphCanvas>,
    auto_fit_short_canvas: bool,
    version: u64,
    rendered: RenderedKeys,
    rendered_viewport: ViewportState,
    patch_in_flight: bool,
    pending_viewport_update: PendingViewportUpdate,
    announced_version: u64,
    version_refresh_scheduled: bool,
    version_refresh_abort: Option<AbortController>,
    events: Option<EventSource>,
}

impl VirtualGraph {
    fn new(window: Window, document: Document) -> Result<Self, JsValue> {
        let elements = VirtualGraphElements::query(&document)?;
        let graph_mode = current_graph_mode(&document);
        let zoom = initial_zoom(&elements.root.graph_wrap);
        let stored_viewport = ViewportState::load(&window);
        let auto_fit_short_canvas = stored_viewport.is_none();
        let viewport = stored_viewport
            .unwrap_or_else(|| viewport_from_element(&elements.root.graph_wrap, 0.0, 0.0, zoom));
        let auto_follow = initial_auto_follow(&window);
        let version = current_version(&document).unwrap_or_default();

        let graph = Self {
            window,
            document,
            graph_mode,
            graph_wrap: elements.root.graph_wrap,
            graph_bg: elements.root.graph_bg,
            follow_toggle: elements.root.follow_toggle,
            edge_group: elements.layers.edge_group,
            node_group: elements.layers.node_group,
            time_scale: elements.time_scale.time_scale,
            time_scale_track: elements.time_scale.time_scale_track,
            time_scale_cursor: elements.time_scale.time_scale_cursor,
            time_scale_label: elements.time_scale.time_scale_label,
            status: elements.status,
            viewport,
            zoom,
            auto_follow,
            canvas: None,
            auto_fit_short_canvas,
            version,
            rendered: RenderedKeys::new(),
            rendered_viewport: viewport,
            patch_in_flight: false,
            pending_viewport_update: PendingViewportUpdate::None,
            announced_version: version,
            version_refresh_scheduled: false,
            version_refresh_abort: None,
            events: None,
        };
        graph.apply_follow_toggle_state()?;
        Ok(graph)
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

    fn follow_top_right(&mut self) {
        let Some(canvas) = self.canvas else {
            self.viewport.x = 0.0;
            self.viewport.y = 0.0;
            return;
        };
        self.viewport.x = (f64::from(canvas.width) - self.viewport.width).max(0.0);
        self.viewport.y = 0.0;
        self.clamp_viewport();
    }

    fn set_auto_follow(&mut self, enabled: bool) -> Result<(), JsValue> {
        self.auto_follow = enabled;
        self.persist_auto_follow();
        self.apply_follow_toggle_state()?;
        if self.auto_follow {
            self.follow_top_right();
        }
        Ok(())
    }

    fn persist_auto_follow(&self) {
        let Some(storage) = session_storage(&self.window) else {
            return;
        };
        let value = if self.auto_follow { "1" } else { "0" };
        let _ = storage.set_item(AUTO_FOLLOW_KEY, value);
    }

    fn apply_follow_toggle_state(&self) -> Result<(), JsValue> {
        let pressed = if self.auto_follow { "true" } else { "false" };
        let label = if self.auto_follow {
            "Following"
        } else {
            "Follow"
        };
        self.follow_toggle.set_attribute("aria-pressed", pressed)?;
        self.follow_toggle.set_text_content(Some(label));
        Ok(())
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
            nodes,
            edges,
        } = response;
        clear_children(&self.edge_group);
        clear_children(&self.node_group);
        self.rendered = RenderedKeys::new();
        self.apply_response_viewport(version, canvas, viewport)?;
        self.upsert_graph_items(GraphViewportItems { nodes, edges }, false)?;
        self.sync_svg_viewport();
        self.sync_selected_graph_node();
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
        self.sync_svg_viewport();
        self.sync_selected_graph_node();
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
        if self.auto_follow {
            self.follow_top_right();
            self.persist_viewport();
        } else if same_viewport(desired_viewport, response_viewport) {
            self.set_viewport(viewport);
        }
        self.fit_short_canvas_once();
        self.rendered_viewport = response_viewport;
        self.set_root_version();
        self.apply_canvas()
    }

    fn fit_short_canvas_once(&mut self) {
        if !self.auto_fit_short_canvas {
            return;
        }
        self.auto_fit_short_canvas = false;
        let Some(canvas) = self.canvas else {
            return;
        };
        let client_width = graph_client_width(&self.graph_wrap);
        let client_height = graph_client_height(&self.graph_wrap);
        let next_zoom = short_canvas_auto_zoom(
            client_width,
            client_height,
            f64::from(canvas.width),
            f64::from(canvas.height),
            self.zoom,
        )
        .clamp(MIN_ZOOM, MAX_SHORT_CANVAS_AUTO_ZOOM.min(MAX_ZOOM));
        if next_zoom <= self.zoom {
            return;
        }
        let center_x =
            viewport_center_for_short_canvas(self.viewport.x, self.viewport.width, canvas.width);
        let center_y =
            viewport_center_for_short_canvas(self.viewport.y, self.viewport.height, canvas.height);
        self.zoom = next_zoom;
        self.viewport.width = client_width / self.zoom;
        self.viewport.height = client_height / self.zoom;
        self.viewport.refresh_render_overscan();
        self.viewport.x = center_x - self.viewport.width / 2.0;
        self.viewport.y = center_y - self.viewport.height / 2.0;
        self.clamp_viewport();
        self.persist_viewport();
    }

    fn apply_canvas(&self) -> Result<(), JsValue> {
        apply_graph_viewport_metadata(&self.graph_wrap, self.viewport, self.zoom)?;
        apply_canvas_dimensions(self.canvas, &self.graph_bg)?;
        apply_time_scale_cursor(
            &self.time_scale,
            &self.time_scale_cursor,
            &self.time_scale_label,
            &self.window,
        )?;
        self.sync_svg_viewport();
        Ok(())
    }

    fn sync_selected_graph_node(&self) {
        if let Err(error) = sync_selected_graph_node(&self.window, &self.document) {
            web_sys::console::error_1(&error);
        }
    }

    fn upsert_graph_items(
        &mut self,
        items: GraphViewportItems,
        nodes_are_new: bool,
    ) -> Result<(), JsValue> {
        let GraphViewportItems { nodes, edges } = items;
        self.upsert_edges(edges)?;
        self.upsert_nodes(nodes, nodes_are_new)
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

    fn upsert_edge(&mut self, edge: GraphViewportEdge) -> Result<(), JsValue> {
        let element = self
            .document
            .get_element_by_id(&render_element_id(&edge.key))
            .map_or_else(|| self.edge_element(&edge), Ok)?;
        let (class, marker) = edge_style(edge.kind);
        set_attributes(
            &element,
            [
                ("id", render_element_id(&edge.key)),
                ("data-render-key", edge.key.clone()),
                ("class", class.to_owned()),
                ("marker-end", marker.to_owned()),
                ("d", bezier_path(edge.route)),
                ("data-source-x", edge.route.source.x.to_string()),
                ("data-source-y", edge.route.source.y.to_string()),
                ("data-control-1-x", edge.route.control_1.x.to_string()),
                ("data-control-1-y", edge.route.control_1.y.to_string()),
                ("data-control-2-x", edge.route.control_2.x.to_string()),
                ("data-control-2-y", edge.route.control_2.y.to_string()),
                ("data-target-x", edge.route.target.x.to_string()),
                ("data-target-y", edge.route.target.y.to_string()),
                ("data-edge-kind", edge.kind.key_part().to_owned()),
            ],
        )?;
        if element.parent_element().is_none() {
            self.edge_group.append_child(&element)?;
        }
        self.rendered
            .edges
            .insert(edge.key.clone(), edge.fingerprint());
        Ok(())
    }

    fn edge_element(&self, edge: &GraphViewportEdge) -> Result<Element, JsValue> {
        let element = svg_element(&self.document, "path")?;
        let (class, marker) = edge_style(edge.kind);
        set_attributes(
            &element,
            [
                ("class", class.to_string()),
                ("marker-end", marker.to_string()),
                ("d", bezier_path(edge.route)),
            ],
        )?;
        Ok(element)
    }

    fn upsert_node(&mut self, node: GraphViewportNode, is_new: bool) -> Result<(), JsValue> {
        let fingerprint = node.fingerprint();
        if let Some(link) = self
            .document
            .get_element_by_id(&render_element_id(&node.key))
        {
            set_node_link_attributes(&link, &node, false)?;
            clear_children(&link);
            self.append_node_group_to_link(&link, &node)?;
            self.rendered.nodes.insert(node.key, fingerprint);
            return Ok(());
        }
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
        self.node_text_element("node-label", "31", node_label(node))
    }

    fn append_node_kind(&self, group: &Element, node: &GraphViewportNode) -> Result<(), JsValue> {
        let kind = self.node_kind_element(node)?;
        group.append_child(&kind)?;
        Ok(())
    }

    fn node_kind_element(&self, node: &GraphViewportNode) -> Result<Element, JsValue> {
        self.node_text_element("node-kind", "44", node.kind.clone())
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
        self.show_status("Loading graph...");
    }

    fn show_status(&self, message: &str) {
        if let Some(status) = &self.status {
            status.set_text_content(Some(message));
            let _ = status.remove_attribute("hidden");
        }
    }

    fn set_root_version(&self) {
        if let Some(root) = self.document.get_element_by_id(ROOT_ID) {
            let _ = root.set_attribute("data-version", &self.version.to_string());
        }
        if let Ok(Some(stats)) = self.document.query_selector(".stats") {
            let mode = if self.graph_mode == "all" {
                "All"
            } else {
                "Anchors"
            };
            stats.set_text_content(Some(&format!("{mode} / revision {}", self.version)));
        }
    }

    #[rustfmt::skip]
    fn sync_svg_viewport(&self) { if let Err(error) = sync_svg_viewport(&self.document, self.viewport) { web_sys::console::error_1(&error); } }

    fn viewport_update_active(&self) -> bool {
        viewport_update_active(self.patch_in_flight, self.pending_viewport_update)
    }

    fn abort_version_refresh(&mut self) {
        if let Some(controller) = self.version_refresh_abort.take() {
            controller.abort();
        }
    }

    fn graph_query(&self, query: String) -> String {
        append_graph_mode_query(query, &self.graph_mode)
    }
}

fn current_graph_mode(document: &Document) -> String {
    document
        .get_element_by_id(ROOT_ID)
        .and_then(|root| root.get_attribute("data-graph-mode"))
        .filter(|mode| mode == "all" || mode == "anchors")
        .unwrap_or_else(|| "anchors".to_owned())
}

fn append_graph_mode_query(mut query: String, mode: &str) -> String {
    if !query.is_empty() {
        query.push('&');
    }
    query.push_str("mode=");
    query.push_str(mode);
    query
}

impl VirtualGraphElements {
    fn query(document: &Document) -> Result<Self, JsValue> {
        Ok(Self {
            root: query_graph_root_elements(document)?,
            layers: query_graph_layer_elements(document)?,
            time_scale: query_time_scale_elements(document)?,
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
        graph_bg: query_required(document, ".graph-bg")?,
        follow_toggle: query_required(document, ".follow-toggle")?,
    })
}

fn query_graph_layer_elements(document: &Document) -> Result<GraphLayerElements, JsValue> {
    Ok(GraphLayerElements {
        edge_group: query_required(document, ".graph-edges")?,
        node_group: query_required(document, ".graph-nodes")?,
    })
}

fn query_time_scale_elements(document: &Document) -> Result<TimeScaleElements, JsValue> {
    Ok(TimeScaleElements {
        time_scale: query_required(document, ".time-scale")?,
        time_scale_track: query_required(document, ".time-scale-track")?,
        time_scale_cursor: query_required(document, ".time-scale-cursor")?,
        time_scale_label: query_required(document, ".time-scale-label")?,
    })
}

fn initial_zoom(graph_wrap: &Element) -> f64 {
    graph_wrap
        .get_attribute("data-zoom")
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(1.0)
        .clamp(MIN_ZOOM, MAX_ZOOM)
}

fn initial_auto_follow(window: &Window) -> bool {
    stored_auto_follow_value(window).is_some_and(|value| value == "1" || value == "true")
}

fn stored_auto_follow_value(window: &Window) -> Option<String> {
    session_storage(window)?
        .get_item(AUTO_FOLLOW_KEY)
        .ok()
        .flatten()
}

impl ViewportState {
    fn load(window: &Window) -> Option<Self> {
        let value = stored_viewport_value(window)?;
        parse_stored_viewport(&value).map(ViewportState::with_render_overscan)
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
        (
            graph.window.clone(),
            graph.graph_query(graph.viewport.request_query()),
        )
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
        if graph.auto_follow {
            graph.follow_top_right();
        }
        let pending_update = pending_update_for_viewport_change(previous, graph.viewport);
        let _ = graph.apply_canvas();
        graph.persist_viewport();
        pending_update
    };
    request_viewport_update(graph, pending_update);
}

fn update_auto_follow(graph: Rc<RefCell<VirtualGraph>>, enabled: bool) {
    let pending_update = {
        let mut graph = graph.borrow_mut();
        graph.abort_version_refresh();
        let previous = graph.viewport;
        if let Err(error) = graph.set_auto_follow(enabled) {
            web_sys::console::error_1(&error);
            return;
        }
        let pending_update = pending_update_for_viewport_change(previous, graph.viewport);
        let _ = graph.apply_canvas();
        graph.persist_viewport();
        pending_update
    };
    request_viewport_update(graph, pending_update);
}

fn edge_style(kind: GraphViewportEdgeKind) -> (&'static str, &'static str) {
    match kind {
        GraphViewportEdgeKind::Primary => ("edge primary-parent", "url(#arrowhead)"),
        GraphViewportEdgeKind::Merge => ("edge merge-parent", "url(#merge-arrowhead)"),
        GraphViewportEdgeKind::Shadow => ("edge shadow-parent", "url(#shadow-arrowhead)"),
    }
}

fn apply_graph_viewport_metadata(
    graph_wrap: &Element,
    viewport: ViewportState,
    zoom: f64,
) -> Result<(), JsValue> {
    set_attributes(
        graph_wrap,
        [
            ("data-viewport-x", rounded_i32(viewport.x).to_string()),
            ("data-viewport-y", rounded_i32(viewport.y).to_string()),
            ("data-zoom", format!("{zoom:.3}")),
        ],
    )
}

fn apply_canvas_dimensions(canvas: Option<GraphCanvas>, graph_bg: &Element) -> Result<(), JsValue> {
    let Some(canvas) = canvas else {
        return Ok(());
    };
    apply_canvas_background(graph_bg, canvas)
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

fn apply_time_scale_cursor(
    time_scale: &Element,
    time_scale_cursor: &Element,
    time_scale_label: &Element,
    window: &Window,
) -> Result<(), JsValue> {
    let ticks = time_scale_ticks(time_scale)?;
    let selected = selected_node_target(window);
    let Some(tick) = selected
        .as_deref()
        .and_then(|target| ticks.iter().find(|tick| tick.node_target == target))
    else {
        time_scale_cursor.set_attribute("hidden", "hidden")?;
        apply_time_scale_tick_focus(&ticks, -100.0)?;
        return Ok(());
    };
    time_scale_cursor.remove_attribute("hidden")?;
    let label_shift = time_scale_label_shift(tick.position);
    set_attributes(
        time_scale_cursor,
        [
            ("style", format!("left: {:.4}%;", tick.position)),
            ("data-time-label", tick.label.clone()),
        ],
    )?;
    time_scale_label.set_text_content(Some(&tick.label));
    time_scale_label.set_attribute("style", &format!("--label-shift: {label_shift};"))?;
    apply_time_scale_tick_focus(&ticks, tick.position)
}

#[derive(Clone)]
struct TimeScaleTick {
    element: Element,
    position: f64,
    node_target: String,
    point: Point,
    label: String,
}

fn time_scale_ticks(time_scale: &Element) -> Result<Vec<TimeScaleTick>, JsValue> {
    let nodes = time_scale.query_selector_all(".time-scale-tick")?;
    let mut ticks = Vec::with_capacity(nodes.length() as usize);
    for index in 0..nodes.length() {
        let Some(node) = nodes.item(index) else {
            continue;
        };
        let Ok(element) = node.dyn_into::<Element>() else {
            continue;
        };
        let Some(position) = numeric_attribute(&element, "data-position") else {
            continue;
        };
        let Some(node_target) = element.get_attribute("data-node-target") else {
            continue;
        };
        let Some(x) = numeric_attribute(&element, "data-node-x") else {
            continue;
        };
        let Some(y) = numeric_attribute(&element, "data-node-y") else {
            continue;
        };
        let label = element
            .get_attribute("data-time-label")
            .unwrap_or_else(|| "-".to_owned());
        ticks.push(TimeScaleTick {
            element,
            position,
            node_target,
            point: Point {
                x: rounded_i32(x),
                y: rounded_i32(y),
            },
            label,
        });
    }
    Ok(ticks)
}

fn numeric_attribute(element: &Element, name: &str) -> Option<f64> {
    element.get_attribute(name)?.parse::<f64>().ok()
}

fn apply_time_scale_tick_focus(
    ticks: &[TimeScaleTick],
    cursor_position: f64,
) -> Result<(), JsValue> {
    for tick in ticks {
        let distance = (tick.position - cursor_position).abs();
        let focus = time_scale_wave_focus(distance);
        let opacity = 0.38 + focus * 0.62;
        tick.element.set_attribute(
            "style",
            &format!(
                "left: {:.4}%; --tick-focus: {:.3}; --tick-opacity: {:.3};",
                tick.position, focus, opacity
            ),
        )?;
    }
    Ok(())
}

fn time_scale_wave_focus(distance: f64) -> f64 {
    let focus =
        (-(distance * distance) / (2.0 * TIME_SCALE_FOCUS_SIGMA * TIME_SCALE_FOCUS_SIGMA)).exp();
    if focus < 0.02 { 0.0 } else { focus }
}

fn time_scale_label_shift(position: f64) -> &'static str {
    if position < 8.0 {
        "0%"
    } else if position > 92.0 {
        "-100%"
    } else {
        "-50%"
    }
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
            ("data-node-x", node.x.to_string()),
            ("data-node-y", node.y.to_string()),
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
    let labels = if node.labels.is_empty() {
        String::new()
    } else {
        format!(" [{}]", node.labels.join(", "))
    };
    format!(
        "{} · {}{}: {}",
        node.short_id, node.kind, labels, node.summary
    )
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
    let query = graph.borrow().graph_query(input.current.request_query());
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
    let mut query = format!("{}&known=1", graph.graph_query(current.request_query()));
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

fn request_graph_items_refresh(graph: Rc<RefCell<VirtualGraph>>, announced_version: u64) {
    {
        let mut graph = graph.borrow_mut();
        graph.announced_version = graph.announced_version.max(announced_version);
    }
    schedule_graph_items_refresh(graph);
}

fn schedule_graph_items_refresh(graph: Rc<RefCell<VirtualGraph>>) {
    let should_schedule = {
        let mut graph = graph.borrow_mut();
        if graph.version_refresh_scheduled || graph.announced_version <= graph.version {
            false
        } else {
            graph.version_refresh_scheduled = true;
            true
        }
    };
    if should_schedule {
        spawn_local(run_scheduled_graph_items_refresh(graph));
    }
}

async fn run_scheduled_graph_items_refresh(graph: Rc<RefCell<VirtualGraph>>) {
    refresh_graph_items_once(graph.clone()).await;
    let retry = {
        let graph = graph.borrow();
        graph.announced_version > graph.version
    };
    if retry {
        delay_graph_items_retry(graph.clone()).await;
    }
    graph.borrow_mut().version_refresh_scheduled = false;
    schedule_graph_items_refresh(graph);
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
            &graph.graph_mode,
        ),
    })
}

fn graph_items_refresh_query(
    version: u64,
    viewport: ViewportState,
    canvas: Option<GraphCanvas>,
    known_query: String,
    graph_mode: &str,
) -> String {
    let mut query = format!(
        "version={version}&{}&known=1",
        append_graph_mode_query(viewport.request_query(), graph_mode)
    );
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
    let patch_needed = {
        let mut graph = graph.borrow_mut();
        if let Err(error) = graph.apply_diff(response) {
            web_sys::console::error_1(&error);
            return;
        }
        !same_viewport(graph.rendered_viewport, graph.viewport)
    };
    if patch_needed {
        request_viewport_patch(graph);
    }
}

fn handle_graph_items_error(graph: Rc<RefCell<VirtualGraph>>, error: JsValue) {
    if is_abort_error(&error) {
        return;
    }
    if !graph.borrow().viewport_update_active() {
        web_sys::console::error_1(&error);
    }
}

fn is_abort_error(error: &JsValue) -> bool {
    js_sys::Reflect::get(error, &JsValue::from_str("name"))
        .ok()
        .and_then(|name| name.as_string())
        .is_some_and(|name| name == "AbortError")
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

async fn delay_graph_items_retry(graph: Rc<RefCell<VirtualGraph>>) {
    let window = graph.borrow().window.clone();
    delay_window_retry(&window).await;
}

async fn delay_window_retry(window: &Window) {
    if let Err(error) = delay_ms(window, VERSION_REFRESH_RETRY_MS).await {
        web_sys::console::error_1(&error);
    }
}

fn sync_svg_viewport(document: &Document, viewport: ViewportState) -> Result<(), JsValue> {
    if let Some(graph_svg) = document.query_selector(".graph")? {
        set_attributes(
            &graph_svg,
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
    }
    Ok(())
}

fn graph_item_point(element: &Element, x_attr: &str, y_attr: &str) -> Option<(i32, i32)> {
    Some((
        graph_item_i32(element, x_attr)?,
        graph_item_i32(element, y_attr)?,
    ))
}

fn graph_item_i32(element: &Element, attr: &str) -> Option<i32> {
    element.get_attribute(attr)?.parse().ok()
}

fn install_graph_listeners(graph: Rc<RefCell<VirtualGraph>>) -> Result<(), JsValue> {
    let installers: [GraphListenerInstaller; 8] = [
        install_node_detail_listener,
        install_follow_toggle_listener,
        install_mouse_pan_listener,
        install_wheel_listener,
        install_resize_listener,
        install_time_scale_listener,
        install_time_scale_keyboard_listener,
        install_hashchange_node_detail_listener,
    ];
    for install in installers {
        install(graph.clone())?;
    }
    Ok(())
}

fn install_mouse_pan_listener(graph: Rc<RefCell<VirtualGraph>>) -> Result<(), JsValue> {
    let graph_wrap = graph.borrow().graph_wrap.clone();
    let window = graph.borrow().window.clone();
    let drag = Rc::new(RefCell::new(None::<ViewportDrag>));
    let suppress_click = Rc::new(Cell::new(false));

    let down_graph = graph.clone();
    let down_drag = drag.clone();
    let down_suppress_click = suppress_click.clone();
    let down_closure = Closure::<dyn FnMut(MouseEvent)>::new(move |event: MouseEvent| {
        if event.button() != 0 {
            return;
        }
        down_suppress_click.set(false);
        if mouse_pan_starts_on_control(&event) {
            return;
        }
        event.prevent_default();
        let graph = down_graph.borrow();
        down_drag.replace(Some(ViewportDrag::new(
            graph.viewport,
            graph.zoom,
            f64::from(event.client_x()),
            f64::from(event.client_y()),
        )));
    });
    graph_wrap
        .add_event_listener_with_callback("mousedown", down_closure.as_ref().unchecked_ref())?;
    down_closure.forget();

    let move_graph = graph.clone();
    let move_drag = drag.clone();
    let move_suppress_click = suppress_click.clone();
    let move_closure = Closure::<dyn FnMut(MouseEvent)>::new(move |event: MouseEvent| {
        if event.buttons() & 1 == 0 {
            if let Some(drag) = move_drag.borrow_mut().take() {
                move_suppress_click.set(drag.did_pan());
            }
            return;
        }
        let Some((x, y)) = move_drag.borrow_mut().as_mut().and_then(|drag| {
            drag.viewport_origin_at(f64::from(event.client_x()), f64::from(event.client_y()))
        }) else {
            return;
        };
        event.prevent_default();
        update_viewport(move_graph.clone(), |graph| {
            if graph.auto_follow
                && let Err(error) = graph.set_auto_follow(false)
            {
                web_sys::console::error_1(&error);
            }
            graph.viewport.x = x;
            graph.viewport.y = y;
        });
    });
    window.add_event_listener_with_callback("mousemove", move_closure.as_ref().unchecked_ref())?;
    move_closure.forget();

    let up_drag = drag;
    let up_suppress_click = suppress_click.clone();
    let up_closure = Closure::<dyn FnMut(MouseEvent)>::new(move |event: MouseEvent| {
        if event.button() == 0
            && let Some(drag) = up_drag.borrow_mut().take()
        {
            up_suppress_click.set(drag.did_pan());
        }
    });
    window.add_event_listener_with_callback("mouseup", up_closure.as_ref().unchecked_ref())?;
    up_closure.forget();

    let click_graph_wrap = graph_wrap;
    let click_closure = Closure::<dyn FnMut(MouseEvent)>::new(move |event: MouseEvent| {
        if suppress_click.replace(false) && mouse_event_targets_element(&event, &click_graph_wrap) {
            event.prevent_default();
            event.stop_propagation();
        }
    });
    window.add_event_listener_with_callback_and_bool(
        "click",
        click_closure.as_ref().unchecked_ref(),
        true,
    )?;
    click_closure.forget();
    Ok(())
}

fn mouse_pan_starts_on_control(event: &MouseEvent) -> bool {
    event
        .target()
        .and_then(|target| target.dyn_into::<Element>().ok())
        .and_then(|target| {
            target
                .closest("button, input, select, textarea, [contenteditable=\"true\"]")
                .ok()
                .flatten()
        })
        .is_some()
}

fn mouse_event_targets_element(event: &MouseEvent, element: &Element) -> bool {
    event
        .target()
        .and_then(|target| target.dyn_into::<web_sys::Node>().ok())
        .is_some_and(|target| element.contains(Some(&target)))
}

fn install_follow_toggle_listener(graph: Rc<RefCell<VirtualGraph>>) -> Result<(), JsValue> {
    let follow_toggle = graph.borrow().follow_toggle.clone();
    let follow_graph = graph.clone();
    let follow_closure = Closure::<dyn FnMut(MouseEvent)>::new(move |event: MouseEvent| {
        event.prevent_default();
        let enabled = !follow_graph.borrow().auto_follow;
        update_auto_follow(follow_graph.clone(), enabled);
    });
    follow_toggle
        .add_event_listener_with_callback("click", follow_closure.as_ref().unchecked_ref())?;
    follow_closure.forget();
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

fn install_time_scale_listener(graph: Rc<RefCell<VirtualGraph>>) -> Result<(), JsValue> {
    let time_scale_track = graph.borrow().time_scale_track.clone();
    let time_scale_graph = graph.clone();
    let time_scale_closure = Closure::<dyn FnMut(MouseEvent)>::new(move |event: MouseEvent| {
        event.prevent_default();
        update_viewport(time_scale_graph.clone(), |graph| {
            center_viewport_from_time_scale(graph, &event);
        });
    });
    time_scale_track
        .add_event_listener_with_callback("click", time_scale_closure.as_ref().unchecked_ref())?;
    time_scale_closure.forget();
    Ok(())
}

fn install_time_scale_keyboard_listener(graph: Rc<RefCell<VirtualGraph>>) -> Result<(), JsValue> {
    let time_scale = graph.borrow().time_scale.clone();
    let keyboard_graph = graph.clone();
    let keyboard_closure = Closure::<dyn FnMut(KeyboardEvent)>::new(move |event: KeyboardEvent| {
        let direction = match event.key().as_str() {
            "ArrowLeft" => -1,
            "ArrowRight" => 1,
            _ => return,
        };
        event.prevent_default();
        update_viewport(keyboard_graph.clone(), |graph| {
            center_viewport_from_time_scale_key(graph, direction);
        });
    });
    time_scale
        .add_event_listener_with_callback("keydown", keyboard_closure.as_ref().unchecked_ref())?;
    keyboard_closure.forget();
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
    let window = {
        let graph = graph.borrow();
        graph.window.clone()
    };

    match set_selected_node_hash(&window, &target) {
        Ok(true) => return,
        Ok(false) => {}
        Err(error) => {
            web_sys::console::error_1(&error);
            return;
        }
    }

    spawn_local(async move {
        if let Err(error) = refresh_selected_node_detail_from_graph(graph).await {
            web_sys::console::error_1(&error);
        }
    });
}

fn set_selected_node_hash(window: &Window, target: &str) -> Result<bool, JsValue> {
    if !node_selection_needs_hash_change(
        selected_node_target(window).as_deref(),
        selected_provider_context_target(window).as_deref(),
        target,
    ) {
        return Ok(false);
    }
    window.location().set_hash(target)?;
    Ok(true)
}

async fn refresh_selected_node_detail_from_graph(
    graph: Rc<RefCell<VirtualGraph>>,
) -> Result<(), JsValue> {
    let (window, document) = {
        let graph = graph.borrow();
        (graph.window.clone(), graph.document.clone())
    };
    refresh_selected_node_detail(&window, &document).await?;
    focus_selected_node_in_graph(graph);
    Ok(())
}

struct SelectedNodeDetailRequest {
    target: Option<String>,
    context: Option<String>,
    detail_url: String,
    provider_context_url: String,
}

async fn refresh_selected_node_detail(window: &Window, document: &Document) -> Result<(), JsValue> {
    let request = selected_node_detail_request(window, document)?;
    let detail_html = fetch_text(window, &request.detail_url).await?;
    let provider_context_html = fetch_text(window, &request.provider_context_url).await?;
    render_node_detail_if_current(window, document, request.target.clone(), &detail_html)?;
    render_provider_context_if_current(
        window,
        document,
        request.target,
        request.context,
        &provider_context_html,
    )
}

fn selected_node_detail_request(
    window: &Window,
    document: &Document,
) -> Result<SelectedNodeDetailRequest, JsValue> {
    let target = selected_node_target(window);
    let context = selected_provider_context_target(window);
    render_loading_node_detail_if_current(window, document, target.as_deref())?;
    render_loading_provider_context_if_current(
        window,
        document,
        target.as_deref(),
        context.as_deref(),
    )?;
    let graph_mode = current_graph_mode(document);
    let detail_url = node_detail_url(target.as_deref(), &graph_mode);
    let provider_context_url =
        provider_context_url(target.as_deref(), context.as_deref(), &graph_mode);
    Ok(SelectedNodeDetailRequest {
        target,
        context,
        detail_url,
        provider_context_url,
    })
}

fn node_detail_url(target: Option<&str>, graph_mode: &str) -> String {
    target
        .map(|target| {
            format!(
                "/api/node-detail?target={}&mode={}",
                percent_encode(target),
                graph_mode
            )
        })
        .unwrap_or_else(|| format!("/api/node-detail?mode={graph_mode}"))
}

fn provider_context_url(target: Option<&str>, context: Option<&str>, graph_mode: &str) -> String {
    let mut query = format!("mode={graph_mode}");
    if let Some(target) = target {
        query.push_str("&target=");
        query.push_str(&percent_encode(target));
    }
    if let Some(context) = context {
        query.push_str("&context=");
        query.push_str(&percent_encode(context));
    }
    format!("/api/provider-context?{query}")
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

fn render_provider_context_if_current(
    window: &Window,
    document: &Document,
    target: Option<String>,
    context: Option<String>,
    html: &str,
) -> Result<(), JsValue> {
    if selected_node_target(window) == target && selected_provider_context_target(window) == context
    {
        let slot = query_required(document, ".provider-context-slot")?;
        slot.set_inner_html(html);
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

fn render_loading_provider_context_if_current(
    window: &Window,
    document: &Document,
    target: Option<&str>,
    context: Option<&str>,
) -> Result<(), JsValue> {
    if selected_node_target(window).as_deref() != target
        || selected_provider_context_target(window).as_deref() != context
    {
        return Ok(());
    }
    let provider_context = loading_provider_context(document, target)?;
    replace_provider_context_slot(document, &provider_context)
}

fn replace_node_detail_slot(document: &Document, detail: &Element) -> Result<(), JsValue> {
    let slot = query_required(document, ".node-detail-slot")?;
    slot.set_inner_html("");
    slot.append_child(detail)?;
    Ok(())
}

fn replace_provider_context_slot(
    document: &Document,
    provider_context: &Element,
) -> Result<(), JsValue> {
    let slot = query_required(document, ".provider-context-slot")?;
    slot.set_inner_html("");
    slot.append_child(provider_context)?;
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

fn loading_provider_context(document: &Document, target: Option<&str>) -> Result<Element, JsValue> {
    let section = classed_element(document, "section", "provider-context-section")?;
    append_text_child(document, &section, "h2", "Provider Context")?;
    let message = if target.is_some() {
        "Loading provider context..."
    } else {
        "Select a node to inspect its provider context."
    };
    let paragraph = classed_element(document, "p", "provider-context-empty")?;
    paragraph.set_text_content(Some(message));
    section.append_child(&paragraph)?;
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

fn focus_selected_node_in_graph(graph: Rc<RefCell<VirtualGraph>>) {
    let point = {
        let graph = graph.borrow();
        graph.sync_selected_graph_node();
        match selected_graph_focus_point(&graph.window, &graph.document) {
            Ok(point) => point,
            Err(error) => {
                web_sys::console::error_1(&error);
                None
            }
        }
    };
    if let Some(point) = point {
        update_viewport(graph, |graph| {
            if graph.auto_follow
                && let Err(error) = graph.set_auto_follow(false)
            {
                web_sys::console::error_1(&error);
            }
            center_viewport_on_graph_point(graph, point);
        });
    }
}

fn sync_selected_graph_node(window: &Window, document: &Document) -> Result<(), JsValue> {
    let selected = selected_node_target(window);
    let nodes = document.query_selector_all(".node-link[data-node-target]")?;
    for index in 0..nodes.length() {
        let node = nodes
            .item(index)
            .expect("query selector index should exist")
            .unchecked_into::<Element>();
        let is_selected = node.get_attribute("data-node-target").as_deref() == selected.as_deref();
        node.class_list()
            .toggle_with_force("node-link-selected", is_selected)?;
    }
    Ok(())
}

fn selected_graph_focus_point(
    window: &Window,
    document: &Document,
) -> Result<Option<Point>, JsValue> {
    let Some(target) = selected_node_target(window) else {
        return Ok(None);
    };
    if let Some(point) = graph_focus_point_from_provider_context(document, &target)? {
        return Ok(Some(point));
    }
    graph_focus_point_from_rendered_node(document, &target)
}

fn graph_focus_point_from_provider_context(
    document: &Document,
    target: &str,
) -> Result<Option<Point>, JsValue> {
    let points = document.query_selector_all(
        ".provider-context-node-graph-point[data-node-target][data-node-x][data-node-y]",
    )?;
    for index in 0..points.length() {
        let point = points
            .item(index)
            .expect("query selector index should exist")
            .unchecked_into::<Element>();
        if point.get_attribute("data-node-target").as_deref() == Some(target)
            && let Some((x, y)) = graph_item_point(&point, "data-node-x", "data-node-y")
        {
            return Ok(Some(Point { x, y }));
        }
    }
    Ok(None)
}

fn graph_focus_point_from_rendered_node(
    document: &Document,
    target: &str,
) -> Result<Option<Point>, JsValue> {
    let nodes =
        document.query_selector_all(".node-link[data-node-target][data-node-x][data-node-y]")?;
    for index in 0..nodes.length() {
        let node = nodes
            .item(index)
            .expect("query selector index should exist")
            .unchecked_into::<Element>();
        if node.get_attribute("data-node-target").as_deref() == Some(target)
            && let Some((x, y)) = graph_item_point(&node, "data-node-x", "data-node-y")
        {
            return Ok(Some(Point { x, y }));
        }
    }
    Ok(None)
}

fn selected_node_target(window: &Window) -> Option<String> {
    selected_panel_selection(window).target
}

fn selected_provider_context_target(window: &Window) -> Option<String> {
    selected_panel_selection(window).context
}

fn selected_panel_selection(window: &Window) -> PanelSelection {
    PanelSelection::from_hash(&window.location().hash().unwrap_or_default())
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

fn center_viewport_from_time_scale(graph: &mut VirtualGraph, event: &MouseEvent) {
    let Ok(ticks) = time_scale_ticks(&graph.time_scale) else {
        return;
    };
    if ticks.is_empty() {
        return;
    }
    let rect = graph.time_scale_track.get_bounding_client_rect();
    if rect.width() <= 0.0 {
        return;
    }
    let local_x = f64::from(event.client_x()) - rect.left();
    let position = (local_x / rect.width() * 100.0).clamp(0.0, 100.0);
    if let Some(tick) = ticks.into_iter().min_by(|left, right| {
        (left.position - position)
            .abs()
            .total_cmp(&(right.position - position).abs())
    }) {
        select_time_scale_tick(graph, &tick);
    }
}

fn center_viewport_from_time_scale_key(graph: &mut VirtualGraph, direction: i32) {
    let Ok(ticks) = time_scale_ticks(&graph.time_scale) else {
        return;
    };
    if ticks.is_empty() {
        return;
    }
    let selected_position = selected_node_target(&graph.window).and_then(|target| {
        ticks
            .iter()
            .find(|tick| tick.node_target == target)
            .map(|tick| tick.position)
    });
    let Some(tick) = adjacent_time_scale_tick(&ticks, selected_position, direction) else {
        return;
    };
    let tick = tick.clone();
    select_time_scale_tick(graph, &tick);
}

fn select_time_scale_tick(graph: &mut VirtualGraph, tick: &TimeScaleTick) {
    if let Err(error) = graph.set_auto_follow(false) {
        web_sys::console::error_1(&error);
    }
    center_viewport_on_graph_point(graph, tick.point);
    if let Err(error) = graph.window.location().set_hash(&tick.node_target) {
        web_sys::console::error_1(&error);
    }
    if let Err(error) = apply_time_scale_cursor(
        &graph.time_scale,
        &graph.time_scale_cursor,
        &graph.time_scale_label,
        &graph.window,
    ) {
        web_sys::console::error_1(&error);
    }
}

fn center_viewport_on_graph_point(graph: &mut VirtualGraph, point: Point) {
    graph.viewport.x = f64::from(point.x) - graph.viewport.width / 2.0;
    graph.viewport.y = f64::from(point.y) - graph.viewport.height / 2.0;
    graph.clamp_viewport();
}

fn adjacent_time_scale_tick(
    ticks: &[TimeScaleTick],
    position: Option<f64>,
    direction: i32,
) -> Option<&TimeScaleTick> {
    let mut by_position = ticks.iter().collect::<Vec<_>>();
    by_position.sort_by(|left, right| left.position.total_cmp(&right.position));
    let Some(position) = position else {
        return if direction < 0 {
            by_position.last().copied()
        } else {
            by_position.first().copied()
        };
    };
    if direction < 0 {
        by_position
            .iter()
            .rev()
            .copied()
            .find(|tick| tick.position < position - f64::EPSILON)
            .or_else(|| by_position.first().copied())
    } else {
        by_position
            .iter()
            .copied()
            .find(|tick| tick.position > position + f64::EPSILON)
            .or_else(|| by_position.last().copied())
    }
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

fn viewport_center_for_short_canvas(
    viewport_start: f64,
    viewport_size: f64,
    canvas_size: i32,
) -> f64 {
    let canvas_size = f64::from(canvas_size);
    if viewport_size >= canvas_size {
        canvas_size / 2.0
    } else {
        (viewport_start + viewport_size / 2.0).clamp(0.0, canvas_size)
    }
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
        let mut labels = node
            .labels
            .iter()
            .take(2)
            .map(|label| truncate_label(label, 12))
            .collect::<Vec<_>>();
        if node.labels.len() > labels.len() {
            labels.push(format!("+{}", node.labels.len() - labels.len()));
        }
        format!("{} {}", node.short_id, labels.join(" · "))
    }
}

fn truncate_label(label: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    let mut chars = label.chars();
    let prefix = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        let truncated = prefix
            .chars()
            .take(max_chars.saturating_sub(1))
            .collect::<String>();
        format!("{truncated}…")
    } else {
        prefix
    }
}

fn bezier_path(route: GraphBezierRoute) -> String {
    format!(
        "M {} {} C {} {}, {} {}, {} {}",
        route.source.x,
        route.source.y,
        route.control_1.x,
        route.control_1.y,
        route.control_2.x,
        route.control_2.y,
        route.target.x,
        route.target.y,
    )
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

#[cfg(all(test, target_arch = "wasm32"))]
mod tests {
    use super::*;

    use wasm_bindgen::UnwrapThrowExt;
    use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};
    use web_sys::MouseEventInit;

    wasm_bindgen_test_configure!(run_in_browser);

    struct GraphFixture {
        graph: Rc<RefCell<VirtualGraph>>,
        root: Element,
    }

    impl GraphFixture {
        fn new() -> Self {
            let window = web_sys::window().expect_throw("window should be available");
            if let Some(storage) = session_storage(&window) {
                let _ = storage.remove_item(AUTO_FOLLOW_KEY);
                let _ = storage.remove_item(VIEWPORT_KEY);
            }
            let _ = window.location().set_hash("");
            let document = window
                .document()
                .expect_throw("document should be available");
            let root = document
                .create_element("div")
                .expect_throw("test root should be created");
            root.set_inner_html(
                r#"
                <main id="console-root" data-version="0" data-graph-mode="anchors">
                  <div class="graph-wrap" data-zoom="1">
                    <button class="follow-toggle" type="button" aria-pressed="false">Follow</button>
                    <svg class="graph">
                      <rect class="graph-bg"></rect>
                      <g class="graph-edges"></g>
                      <g class="graph-nodes"></g>
                    </svg>
                  </div>
                  <nav class="time-scale" tabindex="0">
                    <div class="time-scale-track">
                      <span class="time-scale-tick"
                            data-position="0"
                            data-node-target="detail-aaaaaaaa"
                            data-node-x="120"
                            data-node-y="90"
                            data-time-label="start"></span>
                      <span class="time-scale-tick"
                            data-position="50"
                            data-node-target="detail-bbbbbbbb"
                            data-node-x="232"
                            data-node-y="162"
                            data-time-label="middle"></span>
                      <span class="time-scale-tick"
                            data-position="100"
                            data-node-target="detail-cccccccc"
                            data-node-x="344"
                            data-node-y="90"
                            data-time-label="end"></span>
                      <div class="time-scale-cursor" hidden>
                        <span class="time-scale-label"></span>
                      </div>
                    </div>
                  </nav>
                  <div class="graph-status" hidden></div>
                </main>
                "#,
            );
            document
                .body()
                .expect_throw("document body should be available")
                .append_child(&root)
                .expect_throw("test root should be mounted");
            let graph =
                VirtualGraph::new(window, document).expect_throw("test graph should be created");

            Self {
                graph: Rc::new(RefCell::new(graph)),
                root,
            }
        }
    }

    impl Drop for GraphFixture {
        fn drop(&mut self) {
            let window = self.graph.borrow().window.clone();
            if let Some(storage) = session_storage(&window) {
                let _ = storage.remove_item(AUTO_FOLLOW_KEY);
                let _ = storage.remove_item(VIEWPORT_KEY);
            }
            let _ = window.location().set_hash("");
            self.root.remove();
        }
    }

    #[wasm_bindgen_test]
    fn graph_items_node_label_limits_heads_and_character_count() {
        let mut node = graph_node("aaaaaaaa", 56, 56);
        node.labels = vec![
            "abcdefghijklmnop".to_owned(),
            "short".to_owned(),
            "third".to_owned(),
        ];

        assert_eq!(node_label(&node), "aaaaaaaa abcdefghijk… · short · +1");
        assert_eq!(truncate_label("abcdefghijkl", 12), "abcdefghijkl");
        assert_eq!(truncate_label("abcdefghijklm", 12), "abcdefghijk…");
        assert_eq!(truncate_label("anything", 0), "");
    }

    #[wasm_bindgen_test]
    fn graph_items_render_loading_provider_context() {
        let fixture = GraphFixture::new();
        let document = fixture.graph.borrow().document.clone();
        let loading = loading_provider_context(&document, Some("detail-aaaaaaaa"))
            .expect_throw("loading provider context should render");
        assert_eq!(
            loading
                .query_selector(".provider-context-empty")
                .expect_throw("loading message query should succeed")
                .expect_throw("loading message should exist")
                .text_content()
                .as_deref(),
            Some("Loading provider context...")
        );
        let empty = loading_provider_context(&document, None)
            .expect_throw("empty provider context should render");
        assert_eq!(
            empty
                .query_selector(".provider-context-empty")
                .expect_throw("empty message query should succeed")
                .expect_throw("empty message should exist")
                .text_content()
                .as_deref(),
            Some("Select a node to inspect its provider context.")
        );
    }

    #[wasm_bindgen_test]
    fn graph_items_node_selection_uses_one_refresh_trigger() {
        let fixture = GraphFixture::new();
        let window = fixture.graph.borrow().window.clone();

        assert!(
            set_selected_node_hash(&window, "detail-aaaaaaaa")
                .expect_throw("new selection hash should be set")
        );
        assert!(
            !set_selected_node_hash(&window, "detail-aaaaaaaa")
                .expect_throw("unchanged selection should be detected")
        );
        window
            .location()
            .set_hash("detail-aaaaaaaa?context=detail-context")
            .expect_throw("provider context hash should be set");
        assert!(
            set_selected_node_hash(&window, "detail-aaaaaaaa")
                .expect_throw("provider context selection should be cleared")
        );
    }

    #[wasm_bindgen_test]
    fn graph_items_mouse_pan_updates_viewport_and_suppresses_drag_click() {
        let fixture = GraphFixture::new();
        let (graph_wrap, follow_toggle, time_scale, window) = {
            let mut graph = fixture.graph.borrow_mut();
            graph.viewport = ViewportState {
                x: 100.0,
                y: 80.0,
                width: 200.0,
                height: 100.0,
                overscan: MIN_OVERSCAN,
            };
            graph.rendered_viewport = graph.viewport;
            graph.zoom = 2.0;
            graph.canvas = Some(GraphCanvas {
                width: 640,
                height: 360,
            });
            graph.auto_follow = true;
            graph.patch_in_flight = true;
            (
                graph.graph_wrap.clone(),
                graph.follow_toggle.clone(),
                graph.time_scale.clone(),
                graph.window.clone(),
            )
        };
        install_mouse_pan_listener(fixture.graph.clone())
            .expect_throw("mouse pan listener should install");

        let control_down = mouse_event("mousedown", 0, 1, 40, 30);
        follow_toggle
            .dispatch_event(&control_down)
            .expect_throw("control mouse down should dispatch");
        assert!(!control_down.default_prevented());
        let control_move = mouse_event("mousemove", 0, 1, 60, 50);
        window
            .dispatch_event(&control_move)
            .expect_throw("control mouse move should dispatch");
        let control_up = mouse_event("mouseup", 0, 0, 60, 50);
        window
            .dispatch_event(&control_up)
            .expect_throw("control mouse up should dispatch");
        let control_click = mouse_event("click", 0, 0, 60, 50);
        assert!(
            follow_toggle
                .dispatch_event(&control_click)
                .expect_throw("control click should dispatch")
        );
        assert_eq!(fixture.graph.borrow().viewport.x, 100.0);

        let right_down = mouse_event("mousedown", 2, 2, 40, 30);
        graph_wrap
            .dispatch_event(&right_down)
            .expect_throw("right mouse down should dispatch");
        assert!(!right_down.default_prevented());

        let move_without_drag = mouse_event("mousemove", 0, 1, 60, 50);
        window
            .dispatch_event(&move_without_drag)
            .expect_throw("orphan mouse move should dispatch");
        assert_eq!(fixture.graph.borrow().viewport.x, 100.0);

        let left_down = mouse_event("mousedown", 0, 1, 40, 30);
        graph_wrap
            .dispatch_event(&left_down)
            .expect_throw("left mouse down should dispatch");
        assert!(left_down.default_prevented());

        let below_threshold = mouse_event("mousemove", 0, 1, 42, 32);
        window
            .dispatch_event(&below_threshold)
            .expect_throw("small mouse move should dispatch");
        assert!(!below_threshold.default_prevented());
        assert_eq!(fixture.graph.borrow().viewport.x, 100.0);

        let drag_move = mouse_event("mousemove", 0, 1, 60, 50);
        window
            .dispatch_event(&drag_move)
            .expect_throw("drag mouse move should dispatch");
        assert!(drag_move.default_prevented());
        {
            let graph = fixture.graph.borrow();
            assert!(!graph.auto_follow);
            assert_eq!(graph.viewport.x, 90.0);
            assert_eq!(graph.viewport.y, 70.0);
            assert_eq!(graph.pending_viewport_update, PendingViewportUpdate::Patch);
        }

        let return_to_origin = mouse_event("mousemove", 0, 1, 40, 30);
        window
            .dispatch_event(&return_to_origin)
            .expect_throw("return mouse move should dispatch");
        assert!(return_to_origin.default_prevented());
        assert_eq!(fixture.graph.borrow().viewport.x, 100.0);
        assert_eq!(fixture.graph.borrow().viewport.y, 80.0);

        let left_up = mouse_event("mouseup", 0, 0, 40, 30);
        window
            .dispatch_event(&left_up)
            .expect_throw("left mouse up should dispatch");

        let suppressed_click = mouse_event("click", 0, 0, 60, 50);
        assert!(
            !graph_wrap
                .dispatch_event(&suppressed_click)
                .expect_throw("suppressed click should dispatch")
        );
        assert!(suppressed_click.default_prevented());

        let next_click = mouse_event("click", 0, 0, 60, 50);
        assert!(
            graph_wrap
                .dispatch_event(&next_click)
                .expect_throw("next click should dispatch")
        );

        let next_down = mouse_event("mousedown", 0, 1, 60, 50);
        graph_wrap
            .dispatch_event(&next_down)
            .expect_throw("next mouse down should dispatch");
        let lost_button_move = mouse_event("mousemove", 0, 0, 70, 60);
        window
            .dispatch_event(&lost_button_move)
            .expect_throw("lost-button mouse move should dispatch");
        let unsuppressed_click = mouse_event("click", 0, 0, 70, 60);
        assert!(
            graph_wrap
                .dispatch_event(&unsuppressed_click)
                .expect_throw("unsuppressed click should dispatch")
        );

        let outside_down = mouse_event("mousedown", 0, 1, 60, 50);
        graph_wrap
            .dispatch_event(&outside_down)
            .expect_throw("outside-release mouse down should dispatch");
        let outside_drag = mouse_event("mousemove", 0, 1, 80, 70);
        window
            .dispatch_event(&outside_drag)
            .expect_throw("outside-release drag should dispatch");
        let outside_up = mouse_event("mouseup", 0, 0, 80, 70);
        time_scale
            .dispatch_event(&outside_up)
            .expect_throw("outside mouse up should dispatch");
        let outside_click = mouse_event("click", 0, 0, 80, 70);
        assert!(
            time_scale
                .dispatch_event(&outside_click)
                .expect_throw("outside click should dispatch")
        );
        let graph_click_after_outside = mouse_event("click", 0, 0, 80, 70);
        assert!(
            graph_wrap
                .dispatch_event(&graph_click_after_outside)
                .expect_throw("graph click after outside release should dispatch")
        );

        let missed_click_down = mouse_event("mousedown", 0, 1, 80, 70);
        graph_wrap
            .dispatch_event(&missed_click_down)
            .expect_throw("missed-click mouse down should dispatch");
        let missed_click_drag = mouse_event("mousemove", 0, 1, 100, 90);
        window
            .dispatch_event(&missed_click_drag)
            .expect_throw("missed-click drag should dispatch");
        let missed_click_up = mouse_event("mouseup", 0, 0, 100, 90);
        time_scale
            .dispatch_event(&missed_click_up)
            .expect_throw("missed-click mouse up should dispatch");

        let next_control_down = mouse_event("mousedown", 0, 1, 100, 90);
        follow_toggle
            .dispatch_event(&next_control_down)
            .expect_throw("next control mouse down should dispatch");
        let next_control_up = mouse_event("mouseup", 0, 0, 100, 90);
        follow_toggle
            .dispatch_event(&next_control_up)
            .expect_throw("next control mouse up should dispatch");
        let next_control_click = mouse_event("click", 0, 0, 100, 90);
        assert!(
            follow_toggle
                .dispatch_event(&next_control_click)
                .expect_throw("next control click should dispatch")
        );
    }

    #[wasm_bindgen_test]
    fn graph_items_viewport_updates_reuse_stable_node_and_edge_elements() {
        let fixture = GraphFixture::new();
        let initial_route = route((74, 56), (104, 56), (120, 128), (150, 128));
        let updated_route = route((298, 162), (326, 162), (350, 90), (374, 90));
        {
            let mut graph = fixture.graph.borrow_mut();
            graph
                .apply_full(GraphViewportResponse {
                    version: 1,
                    canvas: GraphCanvas {
                        width: 640,
                        height: 360,
                    },
                    viewport: viewport(),
                    nodes: vec![
                        graph_node("aaaaaaaa", 56, 56),
                        graph_node("bbbbbbbb", 168, 128),
                    ],
                    edges: vec![graph_edge(
                        GraphViewportEdgeKind::Primary,
                        "aaaaaaaa",
                        "bbbbbbbb",
                        initial_route,
                    )],
                })
                .expect_throw("full viewport should render");
            graph
                .window
                .location()
                .set_hash("detail-aaaaaaaa")
                .expect_throw("selection hash should be set");
            graph.sync_selected_graph_node();
        }

        let document = fixture.graph.borrow().document.clone();
        let node_key = "node:aaaaaaaa";
        let edge_key = "edge:primary_parent:aaaaaaaa:bbbbbbbb";
        let node_before = document
            .get_element_by_id(&render_element_id(node_key))
            .expect_throw("node should be rendered");
        let edge_before = document
            .get_element_by_id(&render_element_id(edge_key))
            .expect_throw("edge should be rendered");
        assert!(node_before.class_list().contains("node-link-selected"));

        {
            let mut graph = fixture.graph.borrow_mut();
            graph
                .apply_diff(GraphViewportDiffResponse {
                    version: 2,
                    canvas: GraphCanvas {
                        width: 640,
                        height: 360,
                    },
                    previous_viewport: viewport(),
                    viewport: viewport(),
                    added: GraphViewportItems::default(),
                    updated: GraphViewportItems {
                        nodes: vec![graph_node("aaaaaaaa", 280, 162)],
                        edges: vec![graph_edge(
                            GraphViewportEdgeKind::Primary,
                            "aaaaaaaa",
                            "bbbbbbbb",
                            updated_route,
                        )],
                    },
                    removed: Vec::new(),
                })
                .expect_throw("viewport diff should render");
        }

        let node_after = document
            .get_element_by_id(&render_element_id(node_key))
            .expect_throw("updated node should remain rendered");
        let edge_after = document
            .get_element_by_id(&render_element_id(edge_key))
            .expect_throw("updated edge should remain rendered");
        assert!(node_before.is_same_node(Some(&node_after)));
        assert!(edge_before.is_same_node(Some(&edge_after)));
        assert!(node_after.class_list().contains("node-link-selected"));
        assert!(!node_after.class_list().contains("node-new"));
        assert_eq!(
            node_after
                .query_selector("g")
                .expect_throw("node group query should succeed")
                .expect_throw("node group should exist")
                .get_attribute("transform")
                .as_deref(),
            Some("translate(280 162)")
        );
        assert_eq!(
            edge_after.get_attribute("d").as_deref(),
            Some(bezier_path(updated_route).as_str())
        );
    }

    #[wasm_bindgen_test]
    fn graph_items_time_scale_targets_nodes_and_navigates_by_tick() {
        let fixture = GraphFixture::new();
        let ticks = {
            let graph = fixture.graph.borrow();
            time_scale_ticks(&graph.time_scale).expect_throw("ticks should parse")
        };
        assert_eq!(ticks.len(), 3);
        assert_eq!(ticks[1].node_target, "detail-bbbbbbbb");
        assert_eq!(ticks[1].point, Point { x: 232, y: 162 });
        assert_eq!(
            adjacent_time_scale_tick(&ticks, Some(50.0), -1)
                .expect_throw("previous tick should exist")
                .node_target,
            "detail-aaaaaaaa"
        );
        assert_eq!(
            adjacent_time_scale_tick(&ticks, Some(50.0), 1)
                .expect_throw("next tick should exist")
                .node_target,
            "detail-cccccccc"
        );

        {
            let mut graph = fixture.graph.borrow_mut();
            graph.canvas = Some(GraphCanvas {
                width: 640,
                height: 360,
            });
            graph.viewport = ViewportState {
                x: 0.0,
                y: 0.0,
                width: 200.0,
                height: 100.0,
                overscan: MIN_OVERSCAN,
            };
            graph
                .set_auto_follow(true)
                .expect_throw("follow should enable");
            select_time_scale_tick(&mut graph, &ticks[1]);

            assert!(!graph.auto_follow);
            assert_eq!(
                selected_node_target(&graph.window).as_deref(),
                Some("detail-bbbbbbbb")
            );
            assert_eq!(rounded_i32(graph.viewport.x), 132);
            assert_eq!(rounded_i32(graph.viewport.y), 112);
            assert!(graph.time_scale_cursor.get_attribute("hidden").is_none());
            assert_eq!(
                graph
                    .time_scale_cursor
                    .get_attribute("data-time-label")
                    .as_deref(),
                Some("middle")
            );
        }
    }

    fn graph_node(id: &str, x: i32, y: i32) -> GraphViewportNode {
        GraphViewportNode {
            key: format!("node:{id}"),
            id: id.to_owned(),
            node_target: format!("detail-{id}"),
            short_id: id.to_owned(),
            kind: "prompt".to_owned(),
            summary: format!("summary for {id}"),
            labels: Vec::new(),
            x,
            y,
        }
    }

    fn graph_edge(
        kind: GraphViewportEdgeKind,
        source_id: &str,
        target_id: &str,
        route: GraphBezierRoute,
    ) -> GraphViewportEdge {
        GraphViewportEdge {
            key: format!("edge:{}:{source_id}:{target_id}", kind.key_part()),
            kind,
            source_id: source_id.to_owned(),
            target_id: target_id.to_owned(),
            route,
        }
    }

    fn route(
        source: (i32, i32),
        control_1: (i32, i32),
        control_2: (i32, i32),
        target: (i32, i32),
    ) -> GraphBezierRoute {
        GraphBezierRoute {
            source: Point {
                x: source.0,
                y: source.1,
            },
            control_1: Point {
                x: control_1.0,
                y: control_1.1,
            },
            control_2: Point {
                x: control_2.0,
                y: control_2.1,
            },
            target: Point {
                x: target.0,
                y: target.1,
            },
        }
    }

    fn viewport() -> GraphViewport {
        GraphViewport {
            x: 0,
            y: 0,
            width: 400,
            height: 240,
            overscan: MIN_OVERSCAN,
        }
    }

    fn mouse_event(
        event_type: &str,
        button: i16,
        buttons: u16,
        client_x: i32,
        client_y: i32,
    ) -> MouseEvent {
        let init = MouseEventInit::new();
        init.set_bubbles(true);
        init.set_cancelable(true);
        init.set_button(button);
        init.set_buttons(buttons);
        init.set_client_x(client_x);
        init.set_client_y(client_y);
        MouseEvent::new_with_mouse_event_init_dict(event_type, &init)
            .expect_throw("mouse event should be created")
    }
}
