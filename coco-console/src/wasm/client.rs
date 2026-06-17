use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
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
    pending_update_for_viewport_change, version_refresh_action, viewport_update_active,
};
use crate::api::{
    GraphCanvas, GraphViewport, GraphViewportDiffResponse, GraphViewportEdge,
    GraphViewportEdgeKind, GraphViewportItems, GraphViewportLane, GraphViewportNode,
    GraphViewportRemovedItem, GraphViewportResponse, Point,
};
use crate::viewport::{
    MIN_OVERSCAN, ViewportState, rounded_i32, same_viewport, short_canvas_auto_zoom,
};

const ROOT_ID: &str = "console-root";
const SVG_NS: &str = "http://www.w3.org/2000/svg";
const VIEWPORT_KEY: &str = "coco-console:viewport";
const AUTO_FOLLOW_KEY: &str = "coco-console:auto-follow";
const NODE_RADIUS: f64 = 26.0;
const EDGE_NODE_EXIT: f64 = 42.0;
const EDGE_TARGET_APPROACH: f64 = 48.0;
const EDGE_ROUTE_STEP: f64 = 12.0;
const GRAPH_LANE_HEIGHT: i32 = 140;
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

#[derive(Deserialize)]
struct ConsoleGraphRebuildStatus {
    mode: String,
    state: String,
    processed: usize,
    total: usize,
    message: String,
}

#[derive(Debug, PartialEq, Eq)]
enum GraphProgressAction {
    Active(String),
    Failed(String),
    Ready,
    Ignore,
}

#[derive(Clone, Copy)]
struct LaneDisplay {
    y: i32,
    visible: bool,
}

type GraphListenerInstaller = fn(Rc<RefCell<VirtualGraph>>) -> Result<(), JsValue>;

struct GraphRootElements {
    graph_wrap: Element,
    graph_bg: Element,
    follow_toggle: Element,
}

struct GraphLayerElements {
    lane_group: Element,
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
    if let Err(error) = install_graph_progress_events(graph.clone()) {
        web_sys::console::error_1(&error);
    }

    Ok(graph)
}

fn browser_context() -> Result<(Window, Document), JsValue> {
    let window = browser_window()?;
    let document = browser_document(&window)?;
    Ok((window, document))
}

fn install_graph_progress_events(graph: Rc<RefCell<VirtualGraph>>) -> Result<(), JsValue> {
    let source = EventSource::new("/events")?;
    let callback_graph = graph.clone();
    let callback =
        Closure::<dyn FnMut(MessageEvent)>::wrap(Box::new(move |event: MessageEvent| {
            let Some(data) = event.data().as_string() else {
                return;
            };
            handle_graph_progress_event(callback_graph.clone(), &data);
        }));
    source.add_event_listener_with_callback("graph-progress", callback.as_ref().unchecked_ref())?;
    callback.forget();
    graph.borrow_mut().progress_events = Some(source);
    Ok(())
}

fn handle_graph_progress_event(graph: Rc<RefCell<VirtualGraph>>, data: &str) {
    match serde_json::from_str::<Vec<ConsoleGraphRebuildStatus>>(data) {
        Ok(statuses) => {
            let should_resume = graph.borrow_mut().apply_progress_statuses(&statuses);
            if should_resume {
                request_viewport_update(graph, PendingViewportUpdate::None);
            }
        }
        Err(error) => web_sys::console::error_1(&JsValue::from_str(&error.to_string())),
    }
}

fn progress_status_message(status: &ConsoleGraphRebuildStatus) -> String {
    if status.total == 0 {
        return status.message.clone();
    }
    format!(
        "{} ({}/{})",
        status.message,
        status.processed.min(status.total),
        status.total
    )
}

fn graph_progress_action(
    mode: &str,
    statuses: &[ConsoleGraphRebuildStatus],
) -> GraphProgressAction {
    statuses
        .iter()
        .find(|status| status.mode == mode)
        .map_or(GraphProgressAction::Ignore, progress_status_action)
}

fn progress_status_action(status: &ConsoleGraphRebuildStatus) -> GraphProgressAction {
    match status.state.as_str() {
        "scheduled" | "building" => GraphProgressAction::Active(progress_status_message(status)),
        "failed" => GraphProgressAction::Failed(status.message.clone()),
        "ready" => GraphProgressAction::Ready,
        _ => GraphProgressAction::Ignore,
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
    graph_mode: String,
    graph_wrap: Element,
    graph_bg: Element,
    follow_toggle: Element,
    lane_group: Element,
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
    shell_version: u64,
    rendered: RenderedKeys,
    rendered_viewport: ViewportState,
    patch_in_flight: bool,
    pending_viewport_update: PendingViewportUpdate,
    graph_rebuild_active: bool,
    version_refresh_abort: Option<AbortController>,
    progress_events: Option<EventSource>,
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
            lane_group: elements.layers.lane_group,
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
            shell_version: version,
            rendered: RenderedKeys::new(),
            rendered_viewport: viewport,
            patch_in_flight: false,
            pending_viewport_update: PendingViewportUpdate::None,
            graph_rebuild_active: false,
            version_refresh_abort: None,
            progress_events: None,
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
        self.sync_branch_visibility();
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
        self.sync_branch_visibility();
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
            self.viewport,
        )?;
        self.sync_branch_visibility();
        Ok(())
    }

    fn sync_selected_graph_node(&self) {
        if let Err(error) = sync_selected_graph_node(&self.window, &self.document) {
            web_sys::console::error_1(&error);
        }
    }

    fn refresh_time_scale_elements(&mut self) -> Result<(), JsValue> {
        let elements = query_time_scale_elements(&self.document)?;
        self.time_scale = elements.time_scale;
        self.time_scale_track = elements.time_scale_track;
        self.time_scale_cursor = elements.time_scale_cursor;
        self.time_scale_label = elements.time_scale_label;
        self.apply_canvas()
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
                ("data-lane-y", lane.y.to_string()),
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
                ("data-source-x", edge.source.x.to_string()),
                ("data-source-y", edge.source.y.to_string()),
                ("data-target-x", edge.target.x.to_string()),
                ("data-target-y", edge.target.y.to_string()),
                ("data-edge-kind", edge_kind_data(edge.kind).to_owned()),
                ("data-route-slot", edge.route_slot.to_string()),
                (
                    "data-target-port-offset",
                    edge.target_port_offset.to_string(),
                ),
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
        self.show_status("Loading graph...");
    }

    fn show_status(&self, message: &str) {
        if let Some(status) = &self.status {
            status.set_text_content(Some(message));
            let _ = status.remove_attribute("hidden");
        }
    }

    fn apply_progress_statuses(&mut self, statuses: &[ConsoleGraphRebuildStatus]) -> bool {
        self.apply_progress_action(graph_progress_action(&self.graph_mode, statuses))
    }

    fn apply_progress_action(&mut self, action: GraphProgressAction) -> bool {
        match action {
            GraphProgressAction::Active(message) => self.apply_active_progress(&message),
            GraphProgressAction::Failed(message) => self.apply_terminal_progress(Some(&message)),
            GraphProgressAction::Ready => self.apply_terminal_progress(None),
            GraphProgressAction::Ignore => false,
        }
    }

    fn apply_active_progress(&mut self, message: &str) -> bool {
        self.graph_rebuild_active = true;
        self.abort_version_refresh();
        self.show_status(message);
        false
    }

    fn apply_terminal_progress(&mut self, message: Option<&str>) -> bool {
        let was_active = self.graph_rebuild_active;
        self.graph_rebuild_active = false;
        self.apply_terminal_progress_status(message);
        was_active && self.deferred_viewport_update_needed()
    }

    fn apply_terminal_progress_status(&self, message: Option<&str>) {
        match message {
            Some(message) => self.show_status(message),
            None => self.hide_status_when_idle(),
        }
    }

    fn hide_status_when_idle(&self) {
        if !self.viewport_update_active() {
            self.hide_status();
        }
    }

    fn set_root_version(&self) {
        if let Some(root) = self.document.get_element_by_id(ROOT_ID) {
            let _ = root.set_attribute("data-version", &self.version.to_string());
        }
    }

    #[rustfmt::skip]
    fn apply_shell_version(&mut self, version: u64) { self.shell_version = version; self.sync_branch_visibility(); }

    #[rustfmt::skip]
    fn sync_branch_visibility(&self) { if let Err(error) = sync_branch_visibility(&self.document, self.viewport) { web_sys::console::error_1(&error); } }

    fn viewport_update_active(&self) -> bool {
        viewport_update_active(self.patch_in_flight, self.pending_viewport_update)
    }

    fn deferred_viewport_update_needed(&self) -> bool {
        self.pending_viewport_update.is_pending()
            || !same_viewport(self.rendered_viewport, self.viewport)
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
        lane_group: query_required(document, ".graph-lanes")?,
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
        if graph.graph_rebuild_active || graph.patch_in_flight {
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

fn routed_edge_style(kind: GraphViewportEdgeKind) -> (&'static str, &'static str) {
    match kind {
        GraphViewportEdgeKind::Fork => ("edge fork", "url(#fork-arrowhead)"),
        GraphViewportEdgeKind::MergeParent => ("edge merge-parent", "url(#merge-arrowhead)"),
        GraphViewportEdgeKind::PrimaryParent => unreachable!("primary edges use line elements"),
    }
}

fn edge_kind_data(kind: GraphViewportEdgeKind) -> &'static str {
    match kind {
        GraphViewportEdgeKind::PrimaryParent => "primary_parent",
        GraphViewportEdgeKind::Fork => "fork",
        GraphViewportEdgeKind::MergeParent => "merge_parent",
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
    viewport: ViewportState,
) -> Result<(), JsValue> {
    let ticks = time_scale_ticks(time_scale)?;
    if ticks.is_empty() {
        return Ok(());
    }
    let graph_x = viewport.x + viewport.width / 2.0;
    let cursor = time_scale_cursor_for_graph_x(&ticks, graph_x);
    let label_shift = time_scale_label_shift(cursor.position);
    set_attributes(
        time_scale_cursor,
        [
            ("style", format!("left: {:.4}%;", cursor.position)),
            ("data-time-label", cursor.label.clone()),
        ],
    )?;
    time_scale_label.set_text_content(Some(&cursor.label));
    time_scale_label.set_attribute("style", &format!("--label-shift: {label_shift};"))?;
    apply_time_scale_tick_focus(&ticks, cursor.position)
}

#[derive(Clone)]
struct TimeScaleTick {
    element: Element,
    position: f64,
    graph_x: f64,
    label: String,
}

struct TimeScaleCursor {
    position: f64,
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
        let Some(graph_x) = numeric_attribute(&element, "data-graph-x") else {
            continue;
        };
        let label = element
            .get_attribute("data-time-label")
            .unwrap_or_else(|| "-".to_owned());
        ticks.push(TimeScaleTick {
            element,
            position,
            graph_x,
            label,
        });
    }
    Ok(ticks)
}

fn numeric_attribute(element: &Element, name: &str) -> Option<f64> {
    element.get_attribute(name)?.parse::<f64>().ok()
}

fn time_scale_cursor_for_graph_x(ticks: &[TimeScaleTick], graph_x: f64) -> TimeScaleCursor {
    let mut by_graph_x = ticks.to_vec();
    by_graph_x.sort_by(|left, right| left.graph_x.total_cmp(&right.graph_x));
    let position = interpolate_timeline_position(&by_graph_x, graph_x);
    let label = nearest_tick_label(&by_graph_x, graph_x);
    TimeScaleCursor { position, label }
}

fn interpolate_timeline_position(ticks: &[TimeScaleTick], graph_x: f64) -> f64 {
    let Some(first) = ticks.first() else {
        return 0.0;
    };
    if graph_x <= first.graph_x {
        return first.position;
    }
    for window in ticks.windows(2) {
        let left = &window[0];
        let right = &window[1];
        if graph_x <= right.graph_x {
            let span = right.graph_x - left.graph_x;
            if span.abs() < f64::EPSILON {
                return right.position;
            }
            let ratio = ((graph_x - left.graph_x) / span).clamp(0.0, 1.0);
            return left.position + (right.position - left.position) * ratio;
        }
    }
    ticks
        .last()
        .map(|tick| tick.position)
        .unwrap_or(first.position)
}

fn nearest_tick_label(ticks: &[TimeScaleTick], graph_x: f64) -> String {
    ticks
        .iter()
        .min_by(|left, right| {
            (left.graph_x - graph_x)
                .abs()
                .total_cmp(&(right.graph_x - graph_x).abs())
        })
        .map(|tick| tick.label.clone())
        .unwrap_or_else(|| "-".to_owned())
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
    if graph.borrow().graph_rebuild_active {
        graph.borrow_mut().patch_in_flight = false;
        return Ok(false);
    }
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
    (!graph.graph_rebuild_active && !graph.viewport_update_active()).then(|| {
        GraphItemsRefreshInput {
            window: graph.window.clone(),
            viewport: graph.viewport,
            query: graph_items_refresh_query(
                graph.version,
                graph.viewport,
                graph.canvas,
                graph.rendered.known_query(),
                &graph.graph_mode,
            ),
        }
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
    if graph.graph_rebuild_active || graph.viewport_update_active() {
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
    if graph.borrow().graph_rebuild_active {
        delay_graph_items_retry(graph).await;
        return;
    }
    let (window, document, url) = server_rendered_sections_request(&graph);
    let response = refresh_server_rendered_sections_from_url(&window, &document, &url)
        .await
        .and_then(|version| {
            graph.borrow_mut().refresh_time_scale_elements()?;
            Ok(version)
        });
    handle_server_rendered_sections_response(graph, &window, response).await;
}

fn server_rendered_sections_request(
    graph: &Rc<RefCell<VirtualGraph>>,
) -> (Window, Document, String) {
    let graph = graph.borrow();
    (
        graph.window.clone(),
        graph.document.clone(),
        format!(
            "/fragment?version={}&mode={}",
            graph.shell_version, graph.graph_mode
        ),
    )
}

async fn handle_server_rendered_sections_response(
    graph: Rc<RefCell<VirtualGraph>>,
    window: &Window,
    response: Result<Option<u64>, JsValue>,
) {
    match response {
        Ok(Some(version)) => graph.borrow_mut().apply_shell_version(version),
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
    refresh_inner_html(document, container, ".branch-section")?;
    refresh_time_scale_fragment(document, container)
}

fn refresh_time_scale_fragment(document: &Document, container: &Element) -> Result<(), JsValue> {
    refresh_attributes(
        document,
        container,
        ".time-scale",
        &["class", "tabindex", "aria-label"],
    )?;
    refresh_inner_html(document, container, ".time-scale-track")?;
    refresh_inner_html(document, container, ".time-scale-extents")
}

fn refresh_attributes(
    document: &Document,
    container: &Element,
    selector: &str,
    names: &[&str],
) -> Result<(), JsValue> {
    let Some(source) = container.query_selector(selector)? else {
        return Ok(());
    };
    if let Some(target) = document.query_selector(selector)? {
        for name in names {
            match source.get_attribute(name) {
                Some(value) => target.set_attribute(name, &value)?,
                None => target.remove_attribute(name)?,
            }
        }
    }
    Ok(())
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

fn sync_branch_visibility(document: &Document, viewport: ViewportState) -> Result<(), JsValue> {
    let lane_ys = graph_lane_ys(document)?;
    let visible_lanes = stabilized_visible_graph_item_lanes(document, viewport, &lane_ys)?;
    let lane_display = compact_lane_display_map(&lane_ys, &visible_lanes);
    sync_canvas_lane_visibility(document, viewport, &lane_display)?;
    let branches = document.query_selector_all(".branch[data-lane-y]")?;
    for index in 0..branches.length() {
        let branch = branches
            .item(index)
            .expect("query selector index should exist")
            .unchecked_into::<Element>();
        sync_branch_visibility_element(&branch, &lane_display)?;
    }
    Ok(())
}

fn stabilized_visible_graph_item_lanes(
    document: &Document,
    viewport: ViewportState,
    lane_ys: &BTreeSet<i32>,
) -> Result<BTreeSet<i32>, JsValue> {
    let identity_display = BTreeMap::new();
    let mut visible_lanes = visible_graph_item_lanes(document, viewport, &identity_display)?;
    for _ in 0..=lane_ys.len() {
        let lane_display = compact_lane_display_map(lane_ys, &visible_lanes);
        let display_viewport = ViewportState {
            y: collapsed_viewport_y(viewport.y, &lane_display),
            ..viewport
        };
        let next_visible_lanes =
            visible_graph_item_lanes(document, display_viewport, &lane_display)?;
        if next_visible_lanes == visible_lanes {
            return Ok(visible_lanes);
        }
        visible_lanes = next_visible_lanes;
    }
    Ok(visible_lanes)
}

fn graph_lane_ys(document: &Document) -> Result<BTreeSet<i32>, JsValue> {
    let mut lane_ys = BTreeSet::new();
    collect_lane_y_attributes(document, ".branch[data-lane-y]", &mut lane_ys)?;
    collect_lane_y_attributes(document, ".lane-label[data-lane-y]", &mut lane_ys)?;
    collect_item_lane_y_attributes(
        document,
        ".node-link[data-node-y]",
        "data-node-y",
        &mut lane_ys,
    )?;
    collect_item_lane_y_attributes(
        document,
        ".edge[data-source-y]",
        "data-source-y",
        &mut lane_ys,
    )?;
    collect_item_lane_y_attributes(
        document,
        ".edge[data-target-y]",
        "data-target-y",
        &mut lane_ys,
    )?;
    Ok(lane_ys)
}

fn collect_lane_y_attributes(
    document: &Document,
    selector: &str,
    lane_ys: &mut BTreeSet<i32>,
) -> Result<(), JsValue> {
    collect_item_lane_y_attributes(document, selector, "data-lane-y", lane_ys)
}

fn collect_item_lane_y_attributes(
    document: &Document,
    selector: &str,
    attribute: &str,
    lane_ys: &mut BTreeSet<i32>,
) -> Result<(), JsValue> {
    let items = document.query_selector_all(selector)?;
    for index in 0..items.length() {
        let item = items
            .item(index)
            .expect("query selector index should exist")
            .unchecked_into::<Element>();
        if let Some(y) = graph_item_i32(&item, attribute) {
            lane_ys.insert(y);
        }
    }
    Ok(())
}

fn compact_lane_display_map(
    lane_ys: &BTreeSet<i32>,
    visible_lanes: &BTreeSet<i32>,
) -> BTreeMap<i32, LaneDisplay> {
    let mut hidden_before = 0;
    lane_ys
        .iter()
        .map(|lane_y| {
            let visible = visible_lanes.contains(lane_y);
            let display = LaneDisplay {
                y: *lane_y - hidden_before * GRAPH_LANE_HEIGHT,
                visible,
            };
            if !visible {
                hidden_before += 1;
            }
            (*lane_y, display)
        })
        .collect()
}

fn sync_canvas_lane_visibility(
    document: &Document,
    viewport: ViewportState,
    lane_display: &BTreeMap<i32, LaneDisplay>,
) -> Result<(), JsValue> {
    sync_canvas_viewport(document, viewport, lane_display)?;
    sync_canvas_lane_labels(document, lane_display)?;
    sync_canvas_nodes(document, lane_display)?;
    sync_canvas_edges(document, viewport, lane_display)
}

fn sync_canvas_viewport(
    document: &Document,
    viewport: ViewportState,
    lane_display: &BTreeMap<i32, LaneDisplay>,
) -> Result<(), JsValue> {
    let Some(graph_svg) = document.query_selector(".graph")? else {
        return Ok(());
    };
    let display_y = collapsed_viewport_y(viewport.y, lane_display);
    set_attributes(
        &graph_svg,
        [(
            "viewBox",
            format!(
                "{} {} {} {}",
                rounded_i32(viewport.x),
                rounded_i32(display_y),
                rounded_i32(viewport.width),
                rounded_i32(viewport.height)
            ),
        )],
    )
}

fn sync_canvas_lane_labels(
    document: &Document,
    lane_display: &BTreeMap<i32, LaneDisplay>,
) -> Result<(), JsValue> {
    let labels = document.query_selector_all(".lane-label[data-lane-y]")?;
    for index in 0..labels.length() {
        let label = labels
            .item(index)
            .expect("query selector index should exist")
            .unchecked_into::<Element>();
        let Some(y) = graph_item_i32(&label, "data-lane-y") else {
            continue;
        };
        let display = lane_display_for_y(lane_display, y);
        set_attributes(
            &label,
            [
                ("y", display.y.to_string()),
                ("data-display-y", display.y.to_string()),
            ],
        )?;
        label
            .class_list()
            .toggle_with_force("lane-viewport-hidden", !display.visible)?;
    }
    Ok(())
}

fn sync_canvas_nodes(
    document: &Document,
    lane_display: &BTreeMap<i32, LaneDisplay>,
) -> Result<(), JsValue> {
    let nodes = document.query_selector_all(".node-link[data-node-x][data-node-y]")?;
    for index in 0..nodes.length() {
        let node = nodes
            .item(index)
            .expect("query selector index should exist")
            .unchecked_into::<Element>();
        let Some((x, y)) = graph_item_point(&node, "data-node-x", "data-node-y") else {
            continue;
        };
        let display = lane_display_for_y(lane_display, y);
        if let Some(group) = node.query_selector("g")? {
            set_attributes(
                &group,
                [("transform", format!("translate({x} {})", display.y))],
            )?;
        }
        apply_node_display_visibility(&node, display.visible)?;
        node.set_attribute("data-display-y", &display.y.to_string())?;
    }
    Ok(())
}

fn sync_canvas_edges(
    document: &Document,
    viewport: ViewportState,
    lane_display: &BTreeMap<i32, LaneDisplay>,
) -> Result<(), JsValue> {
    let display_viewport = ViewportState {
        y: collapsed_viewport_y(viewport.y, lane_display),
        ..viewport
    };
    let edges = document
        .query_selector_all(".edge[data-source-x][data-source-y][data-target-x][data-target-y]")?;
    for index in 0..edges.length() {
        let edge = edges
            .item(index)
            .expect("query selector index should exist")
            .unchecked_into::<Element>();
        let Some((source_x, source_y)) = graph_item_point(&edge, "data-source-x", "data-source-y")
        else {
            continue;
        };
        let Some((target_x, target_y)) = graph_item_point(&edge, "data-target-x", "data-target-y")
        else {
            continue;
        };
        let source = lane_display_for_y(lane_display, source_y);
        let target = lane_display_for_y(lane_display, target_y);
        let visible = graph_edge_visible_in_viewport(
            &edge,
            display_viewport,
            source_x,
            source.y,
            target_x,
            target.y,
        );
        set_edge_display_geometry(&edge, source_x, source.y, target_x, target.y)?;
        edge.class_list()
            .toggle_with_force("edge-viewport-hidden", !visible)?;
        edge.set_attribute("data-display-source-y", &source.y.to_string())?;
        edge.set_attribute("data-display-target-y", &target.y.to_string())?;
    }
    Ok(())
}

fn set_edge_display_geometry(
    edge: &Element,
    source_x: i32,
    source_y: i32,
    target_x: i32,
    target_y: i32,
) -> Result<(), JsValue> {
    match edge.get_attribute("data-edge-kind").as_deref() {
        Some("primary_parent") => set_primary_edge_display_geometry(
            edge,
            Point {
                x: source_x,
                y: source_y,
            },
            Point {
                x: target_x,
                y: target_y,
            },
        ),
        Some("fork") | Some("merge_parent") => set_routed_edge_display_geometry(
            edge,
            Point {
                x: source_x,
                y: source_y,
            },
            Point {
                x: target_x,
                y: target_y,
            },
        ),
        _ => Ok(()),
    }
}

fn set_primary_edge_display_geometry(
    edge: &Element,
    source: Point,
    target: Point,
) -> Result<(), JsValue> {
    let target_port_offset = graph_item_f64(edge, "data-target-port-offset").unwrap_or_default();
    let (x1, y1, x2, y2) = line_points(source, target, target_port_offset);
    set_attributes(edge, [("x1", x1), ("y1", y1), ("x2", x2), ("y2", y2)])
}

fn set_routed_edge_display_geometry(
    edge: &Element,
    source: Point,
    target: Point,
) -> Result<(), JsValue> {
    let route_slot = graph_item_i32(edge, "data-route-slot").unwrap_or_default();
    let target_port_offset = graph_item_f64(edge, "data-target-port-offset").unwrap_or_default();
    set_attributes(
        edge,
        [(
            "points",
            routed_elbow_points(source, target, route_slot, target_port_offset),
        )],
    )
}

fn apply_node_display_visibility(node: &Element, visible: bool) -> Result<(), JsValue> {
    node.class_list()
        .toggle_with_force("node-viewport-hidden", !visible)?;
    if visible {
        node.remove_attribute("aria-hidden")?;
        node.remove_attribute("tabindex")?;
        node.remove_attribute("focusable")
    } else {
        node.set_attribute("aria-hidden", "true")?;
        node.set_attribute("tabindex", "-1")?;
        node.set_attribute("focusable", "false")
    }
}

fn visible_graph_item_lanes(
    document: &Document,
    viewport: ViewportState,
    lane_display: &BTreeMap<i32, LaneDisplay>,
) -> Result<BTreeSet<i32>, JsValue> {
    let mut lanes = BTreeSet::new();
    collect_visible_node_lanes(document, viewport, lane_display, &mut lanes)?;
    collect_visible_edge_lanes(document, viewport, lane_display, &mut lanes)?;
    Ok(lanes)
}

fn collect_visible_node_lanes(
    document: &Document,
    viewport: ViewportState,
    lane_display: &BTreeMap<i32, LaneDisplay>,
    lanes: &mut BTreeSet<i32>,
) -> Result<(), JsValue> {
    let nodes = document.query_selector_all(".node-link[data-node-x][data-node-y]")?;
    for index in 0..nodes.length() {
        let node = nodes
            .item(index)
            .expect("query selector index should exist")
            .unchecked_into::<Element>();
        let Some((x, y)) = graph_item_point(&node, "data-node-x", "data-node-y") else {
            continue;
        };
        let display = lane_display_for_y(lane_display, y);
        if graph_node_visible_in_viewport(viewport, x, display.y) {
            lanes.insert(y);
        }
    }
    Ok(())
}

fn collect_visible_edge_lanes(
    document: &Document,
    viewport: ViewportState,
    lane_display: &BTreeMap<i32, LaneDisplay>,
    lanes: &mut BTreeSet<i32>,
) -> Result<(), JsValue> {
    let edges = document
        .query_selector_all(".edge[data-source-x][data-source-y][data-target-x][data-target-y]")?;
    for index in 0..edges.length() {
        let edge = edges
            .item(index)
            .expect("query selector index should exist")
            .unchecked_into::<Element>();
        let Some((source_x, source_y)) = graph_item_point(&edge, "data-source-x", "data-source-y")
        else {
            continue;
        };
        let Some((target_x, target_y)) = graph_item_point(&edge, "data-target-x", "data-target-y")
        else {
            continue;
        };
        let source = lane_display_for_y(lane_display, source_y);
        let target = lane_display_for_y(lane_display, target_y);
        if graph_edge_visible_in_viewport(&edge, viewport, source_x, source.y, target_x, target.y) {
            lanes.insert(source_y);
            lanes.insert(target_y);
        }
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

fn graph_item_f64(element: &Element, attr: &str) -> Option<f64> {
    element.get_attribute(attr)?.parse().ok()
}

fn lane_display_for_y(lane_display: &BTreeMap<i32, LaneDisplay>, y: i32) -> LaneDisplay {
    lane_display
        .get(&y)
        .copied()
        .unwrap_or(LaneDisplay { y, visible: true })
}

fn collapsed_viewport_y(viewport_y: f64, lane_display: &BTreeMap<i32, LaneDisplay>) -> f64 {
    let collapsed_offset = lane_display
        .iter()
        .filter(|(_, display)| !display.visible)
        .map(|(lane_y, _)| collapsed_lane_offset(viewport_y, *lane_y))
        .sum::<f64>();
    viewport_y - collapsed_offset
}

fn collapsed_lane_offset(viewport_y: f64, lane_y: i32) -> f64 {
    let lane_height = f64::from(GRAPH_LANE_HEIGHT);
    let lane_top = f64::from(lane_y) - lane_height / 2.0;
    (viewport_y - lane_top).clamp(0.0, lane_height)
}

fn graph_edge_bounds(
    edge: &Element,
    source_x: i32,
    source_y: i32,
    target_x: i32,
    target_y: i32,
) -> (f64, f64, f64, f64) {
    let source = Point {
        x: source_x,
        y: source_y,
    };
    let target = Point {
        x: target_x,
        y: target_y,
    };
    let target_port_offset = graph_item_f64(edge, "data-target-port-offset").unwrap_or_default();
    let points = match edge.get_attribute("data-edge-kind").as_deref() {
        Some("fork") | Some("merge_parent") => {
            let route_slot = graph_item_i32(edge, "data-route-slot").unwrap_or_default();
            routed_elbow_point_values(source, target, route_slot, target_port_offset)
        }
        _ => {
            let (start_x, start_y, end_x, end_y) =
                line_point_values(source, target, target_port_offset);
            vec![(start_x, start_y), (end_x, end_y)]
        }
    };
    point_bounds(&points)
}

fn point_bounds(points: &[(f64, f64)]) -> (f64, f64, f64, f64) {
    let mut left = f64::INFINITY;
    let mut top = f64::INFINITY;
    let mut right = f64::NEG_INFINITY;
    let mut bottom = f64::NEG_INFINITY;
    for (x, y) in points {
        left = left.min(*x);
        top = top.min(*y);
        right = right.max(*x);
        bottom = bottom.max(*y);
    }
    (left, top, right, bottom)
}

fn graph_node_visible_in_viewport(viewport: ViewportState, x: i32, y: i32) -> bool {
    let padding = NODE_RADIUS.ceil();
    crate::viewport::bounds_visible_in_viewport(
        viewport,
        f64::from(x) - padding,
        f64::from(y) - padding,
        f64::from(x) + padding,
        f64::from(y) + padding,
    )
}

fn graph_edge_visible_in_viewport(
    edge: &Element,
    viewport: ViewportState,
    source_x: i32,
    source_y: i32,
    target_x: i32,
    target_y: i32,
) -> bool {
    let padding = (NODE_RADIUS + EDGE_TARGET_APPROACH).ceil();
    let (left, top, right, bottom) =
        graph_edge_bounds(edge, source_x, source_y, target_x, target_y);
    crate::viewport::bounds_visible_in_viewport(
        viewport,
        left - padding,
        top - padding,
        right + padding,
        bottom + padding,
    )
}

fn sync_branch_visibility_element(
    branch: &Element,
    lane_display: &BTreeMap<i32, LaneDisplay>,
) -> Result<(), JsValue> {
    let Some(lane_y) = branch_lane_y(branch) else {
        return Ok(());
    };
    apply_branch_visibility(branch, lane_display_for_y(lane_display, lane_y).visible)
}

#[rustfmt::skip]
fn branch_lane_y(branch: &Element) -> Option<i32> { branch.get_attribute("data-lane-y")?.parse().ok() }

#[rustfmt::skip]
fn apply_branch_visibility(branch: &Element, visible: bool) -> Result<(), JsValue> { branch.class_list().toggle_with_force("branch-viewport-hidden", !visible)?; set_branch_aria_hidden(branch, !visible) }

#[rustfmt::skip]
fn set_branch_aria_hidden(branch: &Element, hidden: bool) -> Result<(), JsValue> { if hidden { branch.set_attribute("aria-hidden", "true") } else { branch.remove_attribute("aria-hidden") } }

fn install_graph_listeners(graph: Rc<RefCell<VirtualGraph>>) -> Result<(), JsValue> {
    let installers: [GraphListenerInstaller; 7] = [
        install_node_detail_listener,
        install_follow_toggle_listener,
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

    if (selected_node_target(&window).as_deref() != Some(target.as_str())
        || selected_provider_context_target(&window).is_some())
        && let Err(error) = window.location().set_hash(&target)
    {
        web_sys::console::error_1(&error);
        return;
    }

    spawn_local(async move {
        if let Err(error) = refresh_selected_node_detail_from_graph(graph).await {
            web_sys::console::error_1(&error);
        }
    });
}

async fn refresh_selected_node_detail_from_graph(
    graph: Rc<RefCell<VirtualGraph>>,
) -> Result<(), JsValue> {
    let (window, document, graph_rebuild_active) = {
        let graph = graph.borrow();
        (
            graph.window.clone(),
            graph.document.clone(),
            graph.graph_rebuild_active,
        )
    };
    if graph_rebuild_active {
        let _ = selected_node_detail_request(&window, &document)?;
        focus_selected_node_in_graph(graph);
        return Ok(());
    }
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
    let hash = selected_hash(window)?;
    let target = hash
        .split_once('?')
        .map(|(target, _)| target)
        .unwrap_or(&hash);
    (!target.is_empty() && target.starts_with("detail-")).then(|| target.to_owned())
}

fn selected_provider_context_target(window: &Window) -> Option<String> {
    let hash = selected_hash(window)?;
    let (_, query) = hash.split_once('?')?;
    query.split('&').find_map(|part| {
        let (name, value) = part.split_once('=')?;
        (name == "context" && value.starts_with("detail-")).then(|| value.to_owned())
    })
}

fn selected_hash(window: &Window) -> Option<String> {
    let hash = window.location().hash().ok()?;
    hash.strip_prefix('#')
        .filter(|target| !target.is_empty())
        .map(str::to_owned)
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
    let graph_x = graph_x_for_time_scale_position(&ticks, position);
    graph.viewport.x = graph_x - graph.viewport.width / 2.0;
    graph.clamp_viewport();
}

fn center_viewport_from_time_scale_key(graph: &mut VirtualGraph, direction: i32) {
    let Ok(ticks) = time_scale_ticks(&graph.time_scale) else {
        return;
    };
    if ticks.is_empty() {
        return;
    }
    let graph_x = graph.viewport.x + graph.viewport.width / 2.0;
    let cursor = time_scale_cursor_for_graph_x(&ticks, graph_x);
    let Some(tick) = adjacent_time_scale_tick(&ticks, cursor.position, direction) else {
        return;
    };
    graph.viewport.x = tick.graph_x - graph.viewport.width / 2.0;
    graph.clamp_viewport();
}

fn center_viewport_on_graph_point(graph: &mut VirtualGraph, point: Point) {
    graph.viewport.x = f64::from(point.x) - graph.viewport.width / 2.0;
    graph.viewport.y = f64::from(point.y) - graph.viewport.height / 2.0;
    graph.clamp_viewport();
}

fn adjacent_time_scale_tick(
    ticks: &[TimeScaleTick],
    position: f64,
    direction: i32,
) -> Option<&TimeScaleTick> {
    let mut by_position = ticks.iter().collect::<Vec<_>>();
    by_position.sort_by(|left, right| left.position.total_cmp(&right.position));
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

fn graph_x_for_time_scale_position(ticks: &[TimeScaleTick], position: f64) -> f64 {
    let mut by_position = ticks.to_vec();
    by_position.sort_by(|left, right| {
        left.position
            .total_cmp(&right.position)
            .then_with(|| left.graph_x.total_cmp(&right.graph_x))
    });
    let Some(first) = by_position.first() else {
        return 0.0;
    };
    if position <= first.position {
        return first.graph_x;
    }
    for window in by_position.windows(2) {
        let left = &window[0];
        let right = &window[1];
        if position <= right.position {
            let span = right.position - left.position;
            if span.abs() < f64::EPSILON {
                return right.graph_x;
            }
            let ratio = ((position - left.position) / span).clamp(0.0, 1.0);
            return left.graph_x + (right.graph_x - left.graph_x) * ratio;
        }
    }
    by_position
        .last()
        .map(|tick| tick.graph_x)
        .unwrap_or(first.graph_x)
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
        format!("{} {}", node.short_id, node.labels.join(", "))
    }
}

fn line_points(
    source: Point,
    target: Point,
    target_port_offset: f64,
) -> (String, String, String, String) {
    let (start_x, start_y, end_x, end_y) = line_point_values(source, target, target_port_offset);
    (
        format!("{start_x:.1}"),
        format!("{start_y:.1}"),
        format!("{end_x:.1}"),
        format!("{end_y:.1}"),
    )
}

fn line_point_values(
    source: Point,
    target: Point,
    target_port_offset: f64,
) -> (f64, f64, f64, f64) {
    let dx = f64::from(target.x - source.x);
    let target_y = f64::from(target.y) + target_port_offset;
    let dy = target_y - f64::from(source.y);
    let distance = (dx * dx + dy * dy).sqrt();
    if distance <= NODE_RADIUS * 2.0 {
        return (
            f64::from(source.x),
            f64::from(source.y),
            f64::from(target.x),
            target_y,
        );
    }

    let ux = dx / distance;
    let uy = dy / distance;
    let start_x = f64::from(source.x) + ux * (NODE_RADIUS + 2.0);
    let start_y = f64::from(source.y) + uy * (NODE_RADIUS + 2.0);
    let end_x = f64::from(target.x) - ux * (NODE_RADIUS + 8.0);
    let end_y = target_y - uy * (NODE_RADIUS + 8.0);

    (start_x, start_y, end_x, end_y)
}

fn routed_elbow_points(
    source: Point,
    target: Point,
    route_slot: i32,
    target_port_offset: f64,
) -> String {
    routed_elbow_point_values(source, target, route_slot, target_port_offset)
        .into_iter()
        .map(|(x, y)| format!("{x:.1},{y:.1}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn routed_elbow_point_values(
    source: Point,
    target: Point,
    route_slot: i32,
    target_port_offset: f64,
) -> Vec<(f64, f64)> {
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

    vec![
        (start_x, start_y),
        (exit_x, start_y),
        (exit_x, corridor_y),
        (approach_x, corridor_y),
        (approach_x, end_y),
        (end_x, end_y),
    ]
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

#[cfg(all(test, target_arch = "wasm32"))]
mod tests {
    use super::*;
    use std::cell::Cell;
    use wasm_bindgen::UnwrapThrowExt;
    use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

    wasm_bindgen_test_configure!(run_in_browser);

    struct ConsoleErrorGuard {
        console: JsValue,
        original: JsValue,
        _closure: Closure<dyn FnMut(JsValue)>,
    }

    impl Drop for ConsoleErrorGuard {
        fn drop(&mut self) {
            let _ =
                js_sys::Reflect::set(&self.console, &JsValue::from_str("error"), &self.original);
        }
    }

    struct GraphFixture {
        graph: Rc<RefCell<VirtualGraph>>,
        root: Element,
    }

    impl Drop for GraphFixture {
        fn drop(&mut self) {
            if let Some(window) = web_sys::window()
                && let Some(storage) = session_storage(&window)
            {
                let _ = storage.remove_item(AUTO_FOLLOW_KEY);
                let _ = storage.remove_item(VIEWPORT_KEY);
            }
            self.root.remove();
        }
    }

    #[wasm_bindgen_test]
    fn graph_progress_action_formats_active_status() {
        let statuses = [progress_status(
            "primary",
            "building",
            9,
            12,
            "Building graph entries",
        )];

        let action = graph_progress_action("primary", &statuses);

        assert_eq!(
            action,
            GraphProgressAction::Active("Building graph entries (9/12)".to_owned())
        );
    }

    #[wasm_bindgen_test]
    fn graph_progress_action_clamps_processed_count() {
        let statuses = [progress_status(
            "primary",
            "scheduled",
            15,
            12,
            "Queued graph build",
        )];

        let action = graph_progress_action("primary", &statuses);

        assert_eq!(
            action,
            GraphProgressAction::Active("Queued graph build (12/12)".to_owned())
        );
    }

    #[wasm_bindgen_test]
    fn graph_progress_action_maps_terminal_statuses() {
        let failed = [progress_status(
            "primary",
            "failed",
            0,
            0,
            "Graph build failed",
        )];
        let ready = [progress_status("primary", "ready", 0, 0, "Graph ready")];

        assert_eq!(
            graph_progress_action("primary", &failed),
            GraphProgressAction::Failed("Graph build failed".to_owned())
        );
        assert_eq!(
            graph_progress_action("primary", &ready),
            GraphProgressAction::Ready
        );
    }

    #[wasm_bindgen_test]
    fn graph_progress_action_ignores_unmatched_or_unknown_statuses() {
        let statuses = [
            progress_status("other", "building", 1, 4, "Building other graph"),
            progress_status("primary", "mystery", 0, 0, "Unknown status"),
        ];

        assert_eq!(
            graph_progress_action("primary", &statuses),
            GraphProgressAction::Ignore
        );
        assert_eq!(
            graph_progress_action("missing", &statuses),
            GraphProgressAction::Ignore
        );
    }

    #[wasm_bindgen_test]
    fn graph_items_abort_error_does_not_log_to_console() {
        let fixture = GraphFixture::new();
        let (console_error_calls, _guard) = install_console_error_counter();
        let error = abort_error();

        handle_graph_items_error(fixture.graph.clone(), error);

        assert_eq!(console_error_calls.get(), 0);
    }

    #[wasm_bindgen_test]
    fn graph_items_non_abort_error_logs_to_console_when_idle() {
        let fixture = GraphFixture::new();
        let (console_error_calls, _guard) = install_console_error_counter();

        handle_graph_items_error(fixture.graph.clone(), JsValue::from_str("network failed"));

        assert_eq!(console_error_calls.get(), 1);
    }

    #[wasm_bindgen_test]
    fn graph_items_loading_provider_context_renders_selection_state() {
        let fixture = GraphFixture::new();
        let document = fixture.graph.borrow().document.clone();

        let empty = loading_provider_context(&document, None)
            .expect_throw("empty provider context loading state should render");
        assert_eq!(
            empty.text_content().as_deref(),
            Some("Provider ContextSelect a node to inspect its provider context.")
        );

        let loading = loading_provider_context(&document, Some("detail-node"))
            .expect_throw("selected provider context loading state should render");
        assert_eq!(
            loading.text_content().as_deref(),
            Some("Provider ContextLoading provider context...")
        );
    }

    #[wasm_bindgen_test]
    fn graph_items_selected_node_detail_request_uses_context_hash() {
        let fixture = GraphFixture::new();
        let (window, document) = {
            let graph = fixture.graph.borrow();
            (graph.window.clone(), graph.document.clone())
        };
        let detail_slot = classed_element(&document, "div", "node-detail-slot")
            .expect_throw("node detail slot should be created");
        let provider_context_slot = classed_element(&document, "div", "provider-context-slot")
            .expect_throw("provider context slot should be created");
        fixture
            .root
            .append_child(&detail_slot)
            .expect_throw("node detail slot should be mounted");
        fixture
            .root
            .append_child(&provider_context_slot)
            .expect_throw("provider context slot should be mounted");

        window
            .location()
            .set_hash("detail-node?context=detail-head")
            .expect_throw("hash should be set");

        let request = selected_node_detail_request(&window, &document)
            .expect_throw("selected node detail request should be created");
        window
            .location()
            .set_hash("")
            .expect_throw("hash should be cleared");

        assert_eq!(request.target.as_deref(), Some("detail-node"));
        assert_eq!(request.context.as_deref(), Some("detail-head"));
        assert_eq!(
            request.detail_url,
            "/api/node-detail?target=detail-node&mode=anchors"
        );
        assert_eq!(
            request.provider_context_url,
            "/api/provider-context?mode=anchors&target=detail-node&context=detail-head"
        );
        assert_eq!(
            detail_slot.text_content().as_deref(),
            Some("NodeSelectionLoading node detail...")
        );
        assert_eq!(
            provider_context_slot.text_content().as_deref(),
            Some("Provider ContextLoading provider context...")
        );
        assert_eq!(selected_node_target(&window), None);
    }

    #[wasm_bindgen_test]
    fn graph_items_server_fragment_refreshes_time_scale_without_replacing_track() {
        let fixture = GraphFixture::new();
        let document = fixture.graph.borrow().document.clone();
        let original_track = fixture.graph.borrow().time_scale_track.clone();
        let container = document
            .create_element("div")
            .expect_throw("fragment container should be created");
        container.set_inner_html(
            r#"
            <main id="console-root" data-version="1">
              <nav class="time-scale" aria-label="Graph time navigator" tabindex="0">
                <div class="time-scale-track">
                  <span class="time-scale-tick" data-position="0" data-graph-x="10" data-time-label="new-start"></span>
                  <span class="time-scale-tick" data-position="50" data-graph-x="20" data-time-label="new-middle"></span>
                  <span class="time-scale-tick" data-position="100" data-graph-x="30" data-time-label="new-end"></span>
                  <div class="time-scale-cursor"><span class="time-scale-label"></span></div>
                </div>
                <div class="time-scale-extents">
                  <span>new-start</span>
                  <span>new-end</span>
                </div>
              </nav>
            </main>
            "#,
        );

        refresh_time_scale_fragment(&document, &container)
            .expect_throw("time scale fragment should refresh");
        fixture
            .graph
            .borrow_mut()
            .refresh_time_scale_elements()
            .expect_throw("time scale elements should refresh");

        let graph = fixture.graph.borrow();
        assert!(original_track.is_same_node(Some(&graph.time_scale_track)));
        assert_eq!(
            graph
                .time_scale
                .get_attribute("tabindex")
                .expect_throw("time scale should remain focusable"),
            "0",
        );
        assert_eq!(
            graph
                .time_scale_track
                .query_selector_all(".time-scale-tick")
                .expect_throw("time scale ticks should be queryable")
                .length(),
            3,
        );
        assert_eq!(
            graph
                .time_scale
                .query_selector_all(".time-scale-extents span")
                .expect_throw("time scale extents should be queryable")
                .item(1)
                .expect_throw("time scale max extent should exist")
                .text_content()
                .expect_throw("time scale max extent should have text"),
            "new-end",
        );
    }

    #[wasm_bindgen_test]
    fn graph_items_auto_follow_pins_to_top_right() {
        let fixture = GraphFixture::new();
        {
            let mut graph = fixture.graph.borrow_mut();
            graph.viewport = ViewportState {
                x: 25.0,
                y: 40.0,
                width: 320.0,
                height: 180.0,
                overscan: MIN_OVERSCAN,
            };
            graph.canvas = Some(GraphCanvas {
                width: 1000,
                height: 600,
            });
        }

        update_auto_follow(fixture.graph.clone(), true);

        let graph = fixture.graph.borrow();
        assert!(graph.auto_follow);
        assert_eq!(rounded_i32(graph.viewport.x), 680);
        assert_eq!(rounded_i32(graph.viewport.y), 0);
        assert_eq!(
            graph.follow_toggle.get_attribute("aria-pressed").as_deref(),
            Some("true")
        );
        assert_eq!(
            graph.follow_toggle.text_content().as_deref(),
            Some("Following")
        );
        assert_eq!(
            session_storage(&graph.window)
                .and_then(|storage| storage.get_item(AUTO_FOLLOW_KEY).ok().flatten())
                .as_deref(),
            Some("1")
        );
    }

    #[wasm_bindgen_test]
    fn graph_items_auto_follow_loads_stored_state() {
        let window = web_sys::window().expect_throw("window should be available");
        session_storage(&window)
            .expect_throw("session storage should be available")
            .set_item(AUTO_FOLLOW_KEY, "1")
            .expect_throw("auto follow state should be stored");

        let fixture = GraphFixture::new();
        let graph = fixture.graph.borrow();

        assert!(graph.auto_follow);
        assert_eq!(
            graph.follow_toggle.get_attribute("aria-pressed").as_deref(),
            Some("true")
        );
        assert_eq!(
            graph.follow_toggle.text_content().as_deref(),
            Some("Following")
        );
    }

    #[wasm_bindgen_test]
    fn graph_items_normalizes_stored_viewport_overscan() {
        let window = web_sys::window().expect_throw("window should be available");
        session_storage(&window)
            .expect_throw("session storage should be available")
            .set_item(VIEWPORT_KEY, "10,20,1000,600,200")
            .expect_throw("stored viewport should be written");

        let fixture = GraphFixture::new();
        let graph = fixture.graph.borrow();

        assert_eq!(rounded_i32(graph.viewport.x), 10);
        assert_eq!(rounded_i32(graph.viewport.y), 20);
        assert_eq!(rounded_i32(graph.viewport.width), 1000);
        assert_eq!(rounded_i32(graph.viewport.height), 600);
        assert_eq!(graph.viewport.overscan, graph.viewport.render_overscan());
        assert_eq!(graph.viewport.overscan, 1800);
    }

    #[wasm_bindgen_test]
    fn graph_items_canvas_lane_visibility_reflows_when_branch_enters_and_exits_viewport() {
        let fixture = GraphFixture::new();
        {
            let mut graph = fixture.graph.borrow_mut();
            graph.viewport = viewport(1780, 0, 1000, 600).into();
            graph
                .apply_full(lane_visibility_response(viewport(1780, 0, 1000, 600)))
                .expect_throw("graph payload should render");
        }

        assert_branch_hidden(&fixture.root, "B", true);
        assert_branch_hidden(&fixture.root, "day", true);
        assert_node_display(&fixture.root, "node:b2", 230, true);
        assert_node_display(&fixture.root, "node:c3", 230, false);
        assert_edge_display(&fixture.root, "edge:merge:a5:c3", 90, 230, false);

        {
            let mut graph = fixture.graph.borrow_mut();
            graph.viewport = viewport(1780, 300, 1000, 300).into();
            graph.sync_branch_visibility();
        }

        assert_graph_viewbox(&fixture.root, "1780 160 1000 300");
        assert_node_display(&fixture.root, "node:c3", 230, false);

        {
            let mut graph = fixture.graph.borrow_mut();
            graph.viewport = viewport(1780, 229, 1000, 300).into();
            graph.sync_branch_visibility();
        }

        assert_graph_viewbox(&fixture.root, "1780 160 1000 300");

        {
            let mut graph = fixture.graph.borrow_mut();
            graph.viewport = viewport(1780, 231, 1000, 300).into();
            graph.sync_branch_visibility();
        }

        assert_graph_viewbox(&fixture.root, "1780 160 1000 300");

        {
            let mut graph = fixture.graph.borrow_mut();
            graph.viewport = viewport(1100, 0, 1000, 600).into();
            graph.sync_branch_visibility();
        }

        assert_branch_hidden(&fixture.root, "B", false);
        assert_branch_hidden(&fixture.root, "day", true);
        assert_node_display(&fixture.root, "node:b2", 230, false);
        assert_node_display(&fixture.root, "node:c3", 370, false);
        assert_edge_display(&fixture.root, "edge:merge:a5:c3", 90, 370, false);
    }

    #[wasm_bindgen_test]
    fn graph_items_canvas_lane_visibility_expands_lanes_shifted_into_viewport() {
        let fixture = GraphFixture::new();
        {
            let mut graph = fixture.graph.borrow_mut();
            graph.viewport = viewport(1780, 0, 1000, 300).into();
            graph
                .apply_full(lane_shift_response(viewport(1780, 0, 1000, 300)))
                .expect_throw("graph payload should render");
        }

        assert_branch_hidden(&fixture.root, "A", true);
        assert_branch_hidden(&fixture.root, "B", true);
        assert_branch_hidden(&fixture.root, "C", false);
        assert_branch_hidden(&fixture.root, "day", true);
        assert_node_display(&fixture.root, "node:c3", 90, false);
    }

    #[wasm_bindgen_test]
    fn graph_items_canvas_lane_visibility_keeps_shifted_lanes_in_viewbox_after_scroll() {
        let fixture = GraphFixture::new();
        {
            let mut graph = fixture.graph.borrow_mut();
            graph.viewport = viewport(1780, 300, 1000, 300).into();
            graph
                .apply_full(lane_shift_response(viewport(1780, 300, 1000, 300)))
                .expect_throw("graph payload should render");
        }

        assert_graph_viewbox(&fixture.root, "1780 20 1000 300");
        assert_branch_hidden(&fixture.root, "A", true);
        assert_branch_hidden(&fixture.root, "B", true);
        assert_branch_hidden(&fixture.root, "C", false);
        assert_branch_hidden(&fixture.root, "day", true);
        assert_node_display(&fixture.root, "node:c3", 90, false);
    }

    impl GraphFixture {
        fn new() -> Self {
            let window = web_sys::window().expect_throw("window should be available");
            let document = window
                .document()
                .expect_throw("document should be available");
            let root = document
                .create_element("div")
                .expect_throw("test root should be created");
            root.set_inner_html(
                r#"
                <main id="console-root" data-version="0">
                  <div class="graph-wrap" data-zoom="1">
                    <button class="follow-toggle" type="button" aria-pressed="false">Follow</button>
                    <svg class="graph">
                      <rect class="graph-bg"></rect>
                      <g class="graph-lanes"></g>
                      <g class="graph-edges"></g>
                      <g class="graph-nodes"></g>
                    </svg>
                  </div>
                  <nav class="time-scale">
                    <div class="time-scale-track">
                      <span class="time-scale-tick" data-position="0" data-graph-x="120" data-time-label="start"></span>
                      <span class="time-scale-tick" data-position="100" data-graph-x="340" data-time-label="end"></span>
                      <div class="time-scale-cursor"><span class="time-scale-label"></span></div>
                    </div>
                    <div class="time-scale-extents">
                      <span>start</span>
                      <span>end</span>
                    </div>
                  </nav>
                  <div class="graph-status" hidden></div>
                  <aside class="side">
                    <ul class="branch-list">
                      <li class="branch" data-lane-key="lane:A" data-lane-y="90"><strong>A</strong></li>
                      <li class="branch" data-lane-key="lane:B" data-lane-y="230"><strong>B</strong></li>
                      <li class="branch" data-lane-key="lane:C" data-lane-y="370"><strong>C</strong></li>
                      <li class="branch" data-lane-key="lane:day" data-lane-y="510"><strong>day</strong></li>
                    </ul>
                  </aside>
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

    fn lane_shift_response(viewport: GraphViewport) -> GraphViewportResponse {
        GraphViewportResponse {
            version: 1,
            canvas: GraphCanvas {
                width: 4200,
                height: 720,
            },
            viewport,
            lanes: vec![lane("A", 90), lane("B", 230), lane("C", 370)],
            nodes: vec![node("node:c3", "c3", 2100, 370)],
            edges: Vec::new(),
        }
    }

    fn lane_visibility_response(viewport: GraphViewport) -> GraphViewportResponse {
        GraphViewportResponse {
            version: 1,
            canvas: GraphCanvas {
                width: 4200,
                height: 720,
            },
            viewport,
            lanes: vec![
                lane("A", 90),
                lane("B", 230),
                lane("C", 370),
                lane("day", 510),
            ],
            nodes: vec![
                node("node:a5", "a5", 1000, 90),
                node("node:a7", "a7", 1440, 90),
                node("node:b1", "b1", 1220, 230),
                node("node:b2", "b2", 1440, 230),
                node("node:c2", "c2", 1440, 370),
                node("node:c3", "c3", 2100, 370),
                node("node:day", "day", 120, 510),
            ],
            edges: vec![
                primary_edge("edge:primary:b1:b2", "b1", "b2", 1220, 230, 1440, 230),
                primary_edge("edge:primary:c2:c3", "c2", "c3", 1440, 370, 2100, 370),
                GraphViewportEdge {
                    key: "edge:merge:a5:c3".to_owned(),
                    kind: GraphViewportEdgeKind::MergeParent,
                    source_id: "a5".to_owned(),
                    target_id: "c3".to_owned(),
                    source: Point { x: 1000, y: 90 },
                    target: Point { x: 2100, y: 370 },
                    route_slot: 0,
                    target_port_offset: 0.0,
                },
            ],
        }
    }

    fn lane(label: &str, y: i32) -> GraphViewportLane {
        GraphViewportLane {
            key: format!("lane:{label}"),
            label: label.to_owned(),
            y,
        }
    }

    fn node(key: &str, id: &str, x: i32, y: i32) -> GraphViewportNode {
        GraphViewportNode {
            key: key.to_owned(),
            id: id.to_owned(),
            node_target: id.to_owned(),
            short_id: id.to_owned(),
            kind: "prompt".to_owned(),
            summary: id.to_owned(),
            labels: Vec::new(),
            x,
            y,
        }
    }

    fn primary_edge(
        key: &str,
        source_id: &str,
        target_id: &str,
        source_x: i32,
        source_y: i32,
        target_x: i32,
        target_y: i32,
    ) -> GraphViewportEdge {
        GraphViewportEdge {
            key: key.to_owned(),
            kind: GraphViewportEdgeKind::PrimaryParent,
            source_id: source_id.to_owned(),
            target_id: target_id.to_owned(),
            source: Point {
                x: source_x,
                y: source_y,
            },
            target: Point {
                x: target_x,
                y: target_y,
            },
            route_slot: 0,
            target_port_offset: 0.0,
        }
    }

    fn viewport(x: i32, y: i32, width: i32, height: i32) -> GraphViewport {
        GraphViewport {
            x,
            y,
            width,
            height,
            overscan: MIN_OVERSCAN,
        }
    }

    fn assert_branch_hidden(root: &Element, name: &str, hidden: bool) {
        let branch = root
            .query_selector(&format!(".branch[data-lane-key=\"lane:{name}\"]"))
            .expect_throw("branch query should succeed")
            .expect_throw("branch should exist");
        assert_eq!(
            branch.class_list().contains("branch-viewport-hidden"),
            hidden
        );
    }

    fn assert_node_display(root: &Element, key: &str, display_y: i32, hidden: bool) {
        let node = root
            .query_selector(&format!("[data-render-key=\"{key}\"]"))
            .expect_throw("node query should succeed")
            .expect_throw("node should exist");
        assert_eq!(
            node.get_attribute("data-display-y"),
            Some(display_y.to_string())
        );
        assert_eq!(node.class_list().contains("node-viewport-hidden"), hidden);
        assert!(
            node.query_selector("g")
                .expect_throw("node group query should succeed")
                .expect_throw("node group should exist")
                .get_attribute("transform")
                .expect_throw("node group should have a transform")
                .ends_with(&format!(" {display_y})"))
        );
        if hidden {
            assert_eq!(node.get_attribute("aria-hidden").as_deref(), Some("true"));
            assert_eq!(node.get_attribute("tabindex").as_deref(), Some("-1"));
            assert_eq!(node.get_attribute("focusable").as_deref(), Some("false"));
        } else {
            assert!(node.get_attribute("aria-hidden").is_none());
            assert!(node.get_attribute("tabindex").is_none());
            assert!(node.get_attribute("focusable").is_none());
        }
    }

    fn assert_edge_display(root: &Element, key: &str, source_y: i32, target_y: i32, hidden: bool) {
        let edge = root
            .query_selector(&format!("[data-render-key=\"{key}\"]"))
            .expect_throw("edge query should succeed")
            .expect_throw("edge should exist");
        assert_eq!(
            edge.get_attribute("data-display-source-y"),
            Some(source_y.to_string())
        );
        assert_eq!(
            edge.get_attribute("data-display-target-y"),
            Some(target_y.to_string())
        );
        assert_eq!(edge.class_list().contains("edge-viewport-hidden"), hidden);
    }

    fn assert_graph_viewbox(root: &Element, viewbox: &str) {
        let graph = root
            .query_selector(".graph")
            .expect_throw("graph query should succeed")
            .expect_throw("graph should exist");
        assert_eq!(graph.get_attribute("viewBox").as_deref(), Some(viewbox));
    }

    fn abort_error() -> JsValue {
        let error = js_sys::Error::new("aborted");
        js_sys::Reflect::set(
            error.as_ref(),
            &JsValue::from_str("name"),
            &JsValue::from_str("AbortError"),
        )
        .expect_throw("abort error name should be set");
        error.into()
    }

    fn progress_status(
        mode: &str,
        state: &str,
        processed: usize,
        total: usize,
        message: &str,
    ) -> ConsoleGraphRebuildStatus {
        ConsoleGraphRebuildStatus {
            mode: mode.to_owned(),
            state: state.to_owned(),
            processed,
            total,
            message: message.to_owned(),
        }
    }

    fn install_console_error_counter() -> (Rc<Cell<u32>>, ConsoleErrorGuard) {
        let console = js_sys::Reflect::get(&js_sys::global(), &JsValue::from_str("console"))
            .expect_throw("console should be available");
        let original = js_sys::Reflect::get(&console, &JsValue::from_str("error"))
            .expect_throw("console.error should be available");
        let calls = Rc::new(Cell::new(0));
        let calls_for_closure = calls.clone();
        let closure = Closure::<dyn FnMut(JsValue)>::new(move |_| {
            calls_for_closure.set(calls_for_closure.get() + 1);
        });

        js_sys::Reflect::set(&console, &JsValue::from_str("error"), closure.as_ref())
            .expect_throw("console.error should be replaceable");

        (
            calls,
            ConsoleErrorGuard {
                console,
                original,
                _closure: closure,
            },
        )
    }
}
