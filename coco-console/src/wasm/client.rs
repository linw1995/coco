use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;

use serde::Deserialize;
use wasm_bindgen::{JsCast, JsValue, closure::Closure, prelude::wasm_bindgen};
use wasm_bindgen_futures::{JsFuture, spawn_local};
use web_sys::{Document, Element, MouseEvent, RequestInit, Response, WheelEvent, Window};

use crate::api::{
    GraphCanvas, GraphViewport, GraphViewportDiffResponse, GraphViewportEdge,
    GraphViewportEdgeKind, GraphViewportLane, GraphViewportNode, GraphViewportResponse, Point,
};
use crate::viewport::{
    MIN_OVERSCAN, ViewportState, needs_full_viewport_fetch, needs_full_viewport_jump_fetch,
    rounded_i32, same_viewport,
};

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
    spawn_local(refresh_on_graph_version(graph));

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
    lanes: BTreeSet<String>,
    nodes: BTreeSet<String>,
    edges: BTreeSet<String>,
}

impl RenderedKeys {
    fn new() -> Self {
        Self {
            lanes: BTreeSet::new(),
            nodes: BTreeSet::new(),
            edges: BTreeSet::new(),
        }
    }

    fn known_query(&self) -> String {
        self.lanes
            .iter()
            .map(|key| format!("known_lane={}", percent_encode(key)))
            .chain(
                self.nodes
                    .iter()
                    .map(|key| format!("known_node={}", percent_encode(key))),
            )
            .chain(
                self.edges
                    .iter()
                    .map(|key| format!("known_edge={}", percent_encode(key))),
            )
            .collect::<Vec<_>>()
            .join("&")
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PendingViewportUpdate {
    None,
    Patch,
    FullFetch,
}

impl PendingViewportUpdate {
    fn merge(self, update: Self) -> Self {
        match (self, update) {
            (Self::FullFetch, _) | (_, Self::FullFetch) => Self::FullFetch,
            (Self::Patch, _) | (_, Self::Patch) => Self::Patch,
            (Self::None, Self::None) => Self::None,
        }
    }

    fn needs_full_fetch(self) -> bool {
        self == Self::FullFetch
    }

    fn is_pending(self) -> bool {
        self != Self::None
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
    rendered: RenderedKeys,
    rendered_viewport: ViewportState,
    patch_in_flight: bool,
    pending_viewport_update: PendingViewportUpdate,
}

impl VirtualGraph {
    fn new(window: Window, document: Document) -> Result<Self, JsValue> {
        let graph_wrap = query_required(&document, ".graph-wrap")?;
        let graph_svg = query_required(&document, ".graph")?;
        let graph_bg = query_required(&document, ".graph-bg")?;
        let lane_group = query_required(&document, ".graph-lanes")?;
        let edge_group = query_required(&document, ".graph-edges")?;
        let node_group = query_required(&document, ".graph-nodes")?;
        let viewport_map = query_required(&document, ".viewport-map")?;
        let viewport_map_bg = query_required(&document, ".viewport-map-bg")?;
        let viewport_map_window = query_required(&document, ".viewport-map-window")?;
        let status = document.query_selector(".graph-status")?;
        let zoom = graph_wrap
            .get_attribute("data-zoom")
            .and_then(|value| value.parse::<f64>().ok())
            .unwrap_or(1.0)
            .clamp(MIN_ZOOM, MAX_ZOOM);
        let viewport = ViewportState::load(&window)
            .unwrap_or_else(|| viewport_from_element(&graph_wrap, 0.0, 0.0, zoom));
        let version = current_version(&document).unwrap_or_default();

        Ok(Self {
            window,
            document,
            graph_wrap,
            graph_svg,
            graph_bg,
            lane_group,
            edge_group,
            node_group,
            viewport_map,
            viewport_map_bg,
            viewport_map_window,
            status,
            viewport,
            zoom,
            canvas: None,
            version,
            rendered: RenderedKeys::new(),
            rendered_viewport: viewport,
            patch_in_flight: false,
            pending_viewport_update: PendingViewportUpdate::None,
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
        let desired_viewport = self.viewport;
        let response_viewport = ViewportState::from(response.viewport);
        clear_children(&self.lane_group);
        clear_children(&self.edge_group);
        clear_children(&self.node_group);
        self.rendered = RenderedKeys::new();
        self.version = response.version;
        self.canvas = Some(response.canvas);
        if same_viewport(desired_viewport, response_viewport) {
            self.set_viewport(response.viewport);
        }
        self.rendered_viewport = response_viewport;
        self.set_root_version();
        self.apply_canvas()?;
        for lane in response.lanes {
            self.upsert_lane(lane)?;
        }
        for edge in response.edges {
            self.upsert_edge(edge)?;
        }
        for node in response.nodes {
            self.upsert_node(node, false)?;
        }
        self.hide_status();
        Ok(())
    }

    fn apply_diff(&mut self, response: GraphViewportDiffResponse) -> Result<(), JsValue> {
        let desired_viewport = self.viewport;
        let response_viewport = ViewportState::from(response.viewport);
        self.version = response.version;
        self.canvas = Some(response.canvas);
        self.rendered_viewport = response_viewport;
        if same_viewport(desired_viewport, response_viewport) {
            self.set_viewport(response.viewport);
        }
        self.set_root_version();
        self.apply_canvas()?;
        for item in response.removed {
            self.remove_key(&item.key);
        }
        for lane in response.added.lanes {
            self.upsert_lane(lane)?;
        }
        for edge in response.added.edges {
            self.upsert_edge(edge)?;
        }
        for node in response.added.nodes {
            self.upsert_node(node, true)?;
        }
        for lane in response.updated.lanes {
            self.upsert_lane(lane)?;
        }
        for edge in response.updated.edges {
            self.upsert_edge(edge)?;
        }
        for node in response.updated.nodes {
            self.upsert_node(node, false)?;
        }
        self.hide_status();
        Ok(())
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

    fn upsert_lane(&mut self, lane: GraphViewportLane) -> Result<(), JsValue> {
        self.remove_key(&lane.key);
        let element = svg_element(&self.document, "text")?;
        element.set_attribute("id", &render_element_id(&lane.key))?;
        element.set_attribute("data-render-key", &lane.key)?;
        element.set_attribute("class", "lane-label")?;
        element.set_attribute("x", "64")?;
        element.set_attribute("y", &lane.y.to_string())?;
        element.set_text_content(Some(&lane.label));
        self.lane_group.append_child(&element)?;
        self.rendered.lanes.insert(lane.key);
        Ok(())
    }

    fn upsert_edge(&mut self, edge: GraphViewportEdge) -> Result<(), JsValue> {
        self.remove_key(&edge.key);
        let element = match edge.kind {
            GraphViewportEdgeKind::PrimaryParent => self.primary_edge_element(&edge)?,
            GraphViewportEdgeKind::Fork | GraphViewportEdgeKind::MergeParent => {
                self.routed_edge_element(&edge)?
            }
        };
        element.set_attribute("id", &render_element_id(&edge.key))?;
        element.set_attribute("data-render-key", &edge.key)?;
        self.edge_group.append_child(&element)?;
        self.rendered.edges.insert(edge.key);
        Ok(())
    }

    fn primary_edge_element(&self, edge: &GraphViewportEdge) -> Result<Element, JsValue> {
        let element = svg_element(&self.document, "line")?;
        let (x1, y1, x2, y2) = line_points(edge.source, edge.target, edge.target_port_offset);
        element.set_attribute("class", "edge primary-parent")?;
        element.set_attribute("marker-end", "url(#arrowhead)")?;
        element.set_attribute("x1", &x1)?;
        element.set_attribute("y1", &y1)?;
        element.set_attribute("x2", &x2)?;
        element.set_attribute("y2", &y2)?;
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
        let link = svg_element(&self.document, "a")?;
        let class = if is_new {
            "node-link node-new"
        } else {
            "node-link"
        };
        link.set_attribute("id", &render_element_id(&node.key))?;
        link.set_attribute("data-render-key", &node.key)?;
        link.set_attribute("class", class)?;
        link.set_attribute("href", &format!("#{}", node.node_target))?;
        link.set_attribute("data-node-target", &node.node_target)?;
        link.set_attribute("data-node-id", &node.id)?;

        let group = svg_element(&self.document, "g")?;
        let mut group_class = format!("node {}", css_token(&node.kind));
        if !node.labels.is_empty() {
            group_class.push_str(" active");
        }
        group.set_attribute("class", &group_class)?;
        group.set_attribute("transform", &format!("translate({} {})", node.x, node.y))?;

        let title = svg_element(&self.document, "title")?;
        title.set_text_content(Some(&format!("{}: {}", node.short_id, node.summary)));
        let core = svg_element(&self.document, "circle")?;
        core.set_attribute("class", "core")?;
        core.set_attribute("r", "26")?;
        let label = svg_element(&self.document, "text")?;
        label.set_attribute("class", "node-label")?;
        label.set_attribute("y", "44")?;
        label.set_text_content(Some(&node_label(&node)));
        let kind = svg_element(&self.document, "text")?;
        kind.set_attribute("class", "node-kind")?;
        kind.set_attribute("y", "58")?;
        kind.set_text_content(Some(&node.kind));

        group.append_child(&title)?;
        group.append_child(&core)?;
        group.append_child(&label)?;
        group.append_child(&kind)?;
        link.append_child(&group)?;
        self.node_group.append_child(&link)?;
        self.rendered.nodes.insert(node.key);
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
        self.patch_in_flight || self.pending_viewport_update.is_pending()
    }
}

impl ViewportState {
    fn load(window: &Window) -> Option<Self> {
        let storage = session_storage(window)?;
        let value = storage.get_item(VIEWPORT_KEY).ok().flatten()?;
        let mut parts = value.split(',');
        Some(Self {
            x: parts.next()?.parse::<f64>().ok()?,
            y: parts.next()?.parse::<f64>().ok()?,
            width: parts.next()?.parse::<f64>().ok()?,
            height: parts.next()?.parse::<f64>().ok()?,
            overscan: parts.next()?.parse::<i32>().ok()?,
        })
    }
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
    let (document, refresh_shell, patch_needed) = {
        let mut graph = graph.borrow_mut();
        let refresh_shell = response.version != graph.version;
        let document = graph.document.clone();
        graph.apply_full(response)?;
        let patch_needed = !same_viewport(graph.rendered_viewport, graph.viewport);
        (document, refresh_shell, patch_needed)
    };
    if refresh_shell {
        refresh_server_rendered_sections(&window, &document).await?;
    }
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
        let previous = graph.viewport;
        update(&mut graph);
        graph.clamp_viewport();
        let pending_update = if needs_full_viewport_jump_fetch(previous, graph.viewport) {
            PendingViewportUpdate::FullFetch
        } else {
            PendingViewportUpdate::Patch
        };
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

async fn drain_viewport_patches(graph: Rc<RefCell<VirtualGraph>>) {
    loop {
        match render_next_viewport_patch(graph.clone()).await {
            Ok(should_continue) if should_continue => {}
            Ok(_) => break,
            Err(error) => {
                web_sys::console::error_1(&error);
                graph.borrow_mut().patch_in_flight = false;
                break;
            }
        }
    }
}

async fn render_next_viewport_patch(graph: Rc<RefCell<VirtualGraph>>) -> Result<bool, JsValue> {
    let (window, rendered, current, needs_full_fetch) = {
        let mut graph = graph.borrow_mut();
        let update = graph.pending_viewport_update;
        graph.pending_viewport_update = PendingViewportUpdate::None;
        let rendered = graph.rendered_viewport;
        let current = graph.viewport;
        (
            graph.window.clone(),
            rendered,
            current,
            update.needs_full_fetch() || needs_full_viewport_fetch(rendered, current),
        )
    };
    if !needs_full_fetch && same_viewport(rendered, current) {
        let mut graph = graph.borrow_mut();
        if graph.pending_viewport_update.is_pending() {
            return Ok(true);
        }
        graph.patch_in_flight = false;
        return Ok(false);
    }

    if needs_full_fetch {
        let query = current.request_query();
        graph.borrow().show_loading_status();
        let response =
            fetch_json::<GraphViewportResponse>(&window, &format!("/api/graph/viewport?{query}"))
                .await?;
        let (document, refresh_shell, should_continue) = {
            let mut graph = graph.borrow_mut();
            let refresh_shell = response.version != graph.version;
            let document = graph.document.clone();
            graph.apply_full(response)?;
            let should_continue = graph.pending_viewport_update.is_pending()
                || !same_viewport(graph.rendered_viewport, graph.viewport);
            if !should_continue {
                graph.patch_in_flight = false;
            }
            (document, refresh_shell, should_continue)
        };
        if refresh_shell {
            refresh_server_rendered_sections(&window, &document).await?;
        }
        return Ok(should_continue);
    }

    let known_query = graph.borrow().rendered.known_query();
    let mut query = format!("{}&known=1", current.request_query());
    if !known_query.is_empty() {
        query.push('&');
        query.push_str(&known_query);
    }
    let response =
        fetch_json_form::<GraphViewportDiffResponse>(&window, "/api/graph/viewport/diff", &query)
            .await?;
    let (document, refresh_shell, should_continue) = {
        let mut graph = graph.borrow_mut();
        let refresh_shell = response.version != graph.version;
        let document = graph.document.clone();
        graph.apply_diff(response)?;
        let should_continue = graph.pending_viewport_update.is_pending()
            || !same_viewport(graph.rendered_viewport, graph.viewport);
        if !should_continue {
            graph.patch_in_flight = false;
        }
        (document, refresh_shell, should_continue)
    };
    if refresh_shell {
        refresh_server_rendered_sections(&window, &document).await?;
    }
    Ok(should_continue)
}

async fn refresh_on_graph_version(graph: Rc<RefCell<VirtualGraph>>) {
    loop {
        let (window, version, viewport, known_query, viewport_update_active) = {
            let graph = graph.borrow();
            (
                graph.window.clone(),
                graph.version,
                graph.viewport,
                graph.rendered.known_query(),
                graph.viewport_update_active(),
            )
        };
        if viewport_update_active {
            if let Err(error) = delay_ms(&window, VERSION_REFRESH_RETRY_MS).await {
                web_sys::console::error_1(&error);
            }
            continue;
        }
        let mut query = format!("version={version}&{}&known=1", viewport.request_query());
        if !known_query.is_empty() {
            query.push('&');
            query.push_str(&known_query);
        }
        match fetch_json_form::<GraphViewportDiffResponse>(
            &window,
            "/api/graph/viewport/diff",
            &query,
        )
        .await
        {
            Ok(response) => {
                let refresh = {
                    let mut graph = graph.borrow_mut();
                    if !same_viewport(graph.viewport, viewport) {
                        continue;
                    }
                    if graph.viewport_update_active() {
                        None
                    } else {
                        let refresh_shell = response.version != graph.version;
                        let window = graph.window.clone();
                        let document = graph.document.clone();
                        if let Err(error) = graph.apply_diff(response) {
                            web_sys::console::error_1(&error);
                        }
                        Some((window, document, refresh_shell))
                    }
                };
                let Some((window, document, refresh_shell)) = refresh else {
                    let window = graph.borrow().window.clone();
                    if let Err(error) = delay_ms(&window, VERSION_REFRESH_RETRY_MS).await {
                        web_sys::console::error_1(&error);
                    }
                    continue;
                };
                if refresh_shell
                    && let Err(error) = refresh_server_rendered_sections(&window, &document).await
                {
                    web_sys::console::error_1(&error);
                }
            }
            Err(error) => web_sys::console::error_1(&error),
        }
    }
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

async fn refresh_server_rendered_sections(
    window: &Window,
    document: &Document,
) -> Result<(), JsValue> {
    let html = fetch_text(window, "/fragment").await?;
    let container = document.create_element("div")?;
    container.set_inner_html(&html);
    refresh_server_fragment_sections(document, &container)?;
    refresh_selected_node_detail_if_needed(window, document).await
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
        query_required(document, ".node-detail-slot")?.set_inner_html(html);
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
    let text = fetch_text_form(window, url, body).await?;
    serde_json::from_str(&text).map_err(|error| JsValue::from_str(&error.to_string()))
}

async fn fetch_text(window: &Window, url: &str) -> Result<String, JsValue> {
    let response = JsFuture::from(window.fetch_with_str(url))
        .await?
        .dyn_into::<Response>()?;
    response_text(response).await
}

async fn fetch_text_form(window: &Window, url: &str, body: &str) -> Result<String, JsValue> {
    let init = RequestInit::new();
    init.set_method("POST");
    init.set_body(&JsValue::from_str(body));
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
