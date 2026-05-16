use std::collections::HashSet;

use serde::Deserialize;
use wasm_bindgen::{JsCast, JsValue, closure::Closure, prelude::wasm_bindgen};
use wasm_bindgen_futures::{JsFuture, spawn_local};
use web_sys::{Document, Element, MouseEvent, Response, Window};

const ROOT_ID: &str = "console-root";
const SCROLL_KEY: &str = "coco-console:scroll";
const ZOOM_KEY: &str = "coco-console:zoom";
const MIN_ZOOM: f64 = 0.35;
const MAX_ZOOM: f64 = 1.25;
const ZOOM_STEP: f64 = 1.2;
const CULL_PADDING: f64 = 520.0;

#[derive(Debug, Deserialize)]
struct ClientGraphSnapshot {
    nodes: Vec<ClientGraphNode>,
}

#[derive(Debug, Deserialize)]
struct ClientGraphNode {
    id: String,
    kind: String,
    role: String,
    created_at: String,
    content: String,
    labels: Vec<String>,
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
    let window = browser_window()?;
    let document = browser_document(&window)?;

    install_scroll_persistence(&window, document.clone())?;
    install_resize_listener(&window, document.clone())?;
    install_hash_listener(&window, document.clone())?;
    install_graph_interactions(&window, &document)?;
    if let Some(state) = ScrollState::load(&window) {
        state.restore(&document);
    }
    update_node_detail_from_hash(&window, &document)?;
    update_graph_viewport_state(&document)?;

    loop {
        let version = current_version(&document).unwrap_or_default();
        let known_nodes = collect_node_ids(&document);
        let scroll = ScrollState::read(&document);
        scroll.save(&window);

        let fragment = fetch_text(&window, &format!("/fragment?version={version}")).await?;
        replace_root(&document, &fragment)?;
        install_graph_interactions(&window, &document)?;
        mark_new_nodes(&document, &known_nodes)?;
        scroll.restore(&document);
        update_node_detail_from_hash(&window, &document)?;
        update_graph_viewport_state(&document)?;
        scroll.save(&window);
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

async fn fetch_text(window: &Window, url: &str) -> Result<String, JsValue> {
    let response = JsFuture::from(window.fetch_with_str(url))
        .await?
        .dyn_into::<Response>()?;
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

fn replace_root(document: &Document, html: &str) -> Result<(), JsValue> {
    let root = document
        .get_element_by_id(ROOT_ID)
        .ok_or_else(|| JsValue::from_str("console root is unavailable"))?;
    root.set_outer_html(html);
    Ok(())
}

fn current_version(document: &Document) -> Option<u64> {
    document
        .get_element_by_id(ROOT_ID)?
        .get_attribute("data-version")?
        .parse()
        .ok()
}

fn collect_node_ids(document: &Document) -> HashSet<String> {
    let Ok(nodes) = document.query_selector_all("[data-node-id]") else {
        return HashSet::new();
    };

    (0..nodes.length())
        .filter_map(|index| nodes.item(index))
        .filter_map(|node| node.dyn_into::<Element>().ok())
        .filter_map(|element| element.get_attribute("data-node-id"))
        .collect()
}

fn mark_new_nodes(document: &Document, known_nodes: &HashSet<String>) -> Result<(), JsValue> {
    let nodes = document.query_selector_all("[data-node-id]")?;
    for index in 0..nodes.length() {
        let Some(node) = nodes.item(index) else {
            continue;
        };
        let Ok(element) = node.dyn_into::<Element>() else {
            continue;
        };
        let Some(node_id) = element.get_attribute("data-node-id") else {
            continue;
        };
        if !known_nodes.contains(&node_id) {
            element.class_list().add_1("node-new")?;
        }
    }
    Ok(())
}

fn install_scroll_persistence(window: &Window, document: Document) -> Result<(), JsValue> {
    let listener_target = window.clone();
    let storage_window = window.clone();
    let closure = Closure::<dyn FnMut()>::new(move || {
        ScrollState::read(&document).save(&storage_window);
    });
    listener_target
        .add_event_listener_with_callback("beforeunload", closure.as_ref().unchecked_ref())?;
    closure.forget();
    Ok(())
}

fn install_resize_listener(window: &Window, document: Document) -> Result<(), JsValue> {
    let closure = Closure::<dyn FnMut()>::new(move || {
        let _ = update_graph_viewport_state(&document);
    });
    window.add_event_listener_with_callback("resize", closure.as_ref().unchecked_ref())?;
    closure.forget();
    Ok(())
}

fn install_hash_listener(window: &Window, document: Document) -> Result<(), JsValue> {
    let window_for_listener = window.clone();
    let closure = Closure::<dyn FnMut()>::new(move || {
        let _ = update_node_detail_from_hash(&window_for_listener, &document);
    });
    window.add_event_listener_with_callback("hashchange", closure.as_ref().unchecked_ref())?;
    closure.forget();
    Ok(())
}

fn install_graph_interactions(window: &Window, document: &Document) -> Result<(), JsValue> {
    let zoom = load_zoom(window).unwrap_or(1.0);
    set_graph_zoom(window, document, zoom, false)?;
    install_zoom_controls(window, document)?;
    install_minimap(document)?;
    update_graph_visibility(document)
}

fn install_minimap(document: &Document) -> Result<(), JsValue> {
    update_minimap_viewport(document)?;

    let scroll_document = document.clone();
    if let Some(graph) = scroll_element(document, ".graph-wrap") {
        let closure = Closure::<dyn FnMut()>::new(move || {
            let _ = update_graph_viewport_state(&scroll_document);
        });
        graph.add_event_listener_with_callback("scroll", closure.as_ref().unchecked_ref())?;
        closure.forget();
    }

    let click_document = document.clone();
    if let Some(minimap) = scroll_element(document, ".minimap") {
        let prevent_selection = Closure::<dyn FnMut(MouseEvent)>::new(move |event: MouseEvent| {
            event.prevent_default();
        });
        minimap.add_event_listener_with_callback(
            "mousedown",
            prevent_selection.as_ref().unchecked_ref(),
        )?;
        prevent_selection.forget();

        let closure = Closure::<dyn FnMut(MouseEvent)>::new(move |event: MouseEvent| {
            event.prevent_default();
            let _ = scroll_graph_from_minimap(&click_document, &event);
        });
        minimap.add_event_listener_with_callback("click", closure.as_ref().unchecked_ref())?;
        closure.forget();
    }

    Ok(())
}

fn install_zoom_controls(window: &Window, document: &Document) -> Result<(), JsValue> {
    let controls = document.query_selector_all("[data-zoom-action]")?;
    for index in 0..controls.length() {
        let Some(control) = controls.item(index) else {
            continue;
        };
        let Ok(control) = control.dyn_into::<Element>() else {
            continue;
        };
        let Some(action) = control.get_attribute("data-zoom-action") else {
            continue;
        };

        let control_window = window.clone();
        let control_document = document.clone();
        let closure = Closure::<dyn FnMut(MouseEvent)>::new(move |event: MouseEvent| {
            event.prevent_default();
            let current = graph_zoom(&control_document);
            let next = match action.as_str() {
                "in" => current * ZOOM_STEP,
                "out" => current / ZOOM_STEP,
                "reset" => 1.0,
                _ => current,
            };
            let _ = set_graph_zoom(&control_window, &control_document, next, true);
        });
        control.add_event_listener_with_callback("click", closure.as_ref().unchecked_ref())?;
        closure.forget();
    }
    update_zoom_controls(document)
}

fn set_graph_zoom(
    window: &Window,
    document: &Document,
    zoom: f64,
    keep_center: bool,
) -> Result<(), JsValue> {
    let Some(graph) = scroll_element(document, ".graph") else {
        return Ok(());
    };
    let Some(graph_width) = data_f64(&graph, "graph-width") else {
        return Ok(());
    };
    let Some(graph_height) = data_f64(&graph, "graph-height") else {
        return Ok(());
    };

    let old_zoom = graph_zoom(document);
    let center = keep_center
        .then(|| scroll_element(document, ".graph-wrap"))
        .flatten()
        .map(|wrap| {
            (
                (f64::from(wrap.scroll_left()) + f64::from(wrap.client_width()) / 2.0) / old_zoom,
                (f64::from(wrap.scroll_top()) + f64::from(wrap.client_height()) / 2.0) / old_zoom,
            )
        });
    let zoom = zoom.clamp(MIN_ZOOM, MAX_ZOOM);

    graph.set_attribute("data-zoom", &format_zoom(zoom))?;
    graph.set_attribute(
        "style",
        &format!(
            "width: {:.0}px; height: {:.0}px;",
            graph_width * zoom,
            graph_height * zoom
        ),
    )?;
    save_zoom(window, zoom);

    if let (Some((center_x, center_y)), Some(wrap)) =
        (center, scroll_element(document, ".graph-wrap"))
    {
        wrap.set_scroll_left(clamp_scroll(
            center_x * zoom - f64::from(wrap.client_width()) / 2.0,
            wrap.scroll_width(),
            wrap.client_width(),
        ));
        wrap.set_scroll_top(clamp_scroll(
            center_y * zoom - f64::from(wrap.client_height()) / 2.0,
            wrap.scroll_height(),
            wrap.client_height(),
        ));
    }

    update_zoom_controls(document)?;
    update_graph_viewport_state(document)
}

fn update_zoom_controls(document: &Document) -> Result<(), JsValue> {
    let zoom = graph_zoom(document);
    if let Some(scale) = scroll_element(document, ".graph-scale") {
        scale.set_text_content(Some(&format!("{:.0}%", zoom * 100.0)));
    }

    let controls = document.query_selector_all("[data-zoom-action]")?;
    for index in 0..controls.length() {
        let Some(control) = controls.item(index) else {
            continue;
        };
        let Ok(control) = control.dyn_into::<Element>() else {
            continue;
        };
        match control.get_attribute("data-zoom-action").as_deref() {
            Some("in") if zoom >= MAX_ZOOM => control.set_attribute("disabled", "true")?,
            Some("out") if zoom <= MIN_ZOOM => control.set_attribute("disabled", "true")?,
            Some("in") | Some("out") => control.remove_attribute("disabled")?,
            _ => {}
        }
    }

    Ok(())
}

fn graph_zoom(document: &Document) -> f64 {
    scroll_element(document, ".graph")
        .and_then(|graph| data_f64(&graph, "zoom"))
        .unwrap_or(1.0)
        .clamp(MIN_ZOOM, MAX_ZOOM)
}

fn format_zoom(zoom: f64) -> String {
    format!("{zoom:.2}")
}

fn save_zoom(window: &Window, zoom: f64) {
    let Some(storage) = session_storage(window) else {
        return;
    };
    let _ = storage.set_item(ZOOM_KEY, &format_zoom(zoom));
}

fn load_zoom(window: &Window) -> Option<f64> {
    let storage = session_storage(window)?;
    storage
        .get_item(ZOOM_KEY)
        .ok()
        .flatten()?
        .parse::<f64>()
        .ok()
}

fn update_graph_viewport_state(document: &Document) -> Result<(), JsValue> {
    update_minimap_viewport(document)?;
    update_graph_visibility(document)
}

fn update_minimap_viewport(document: &Document) -> Result<(), JsValue> {
    let Some(graph) = scroll_element(document, ".graph-wrap") else {
        return Ok(());
    };
    let Some(viewport) = scroll_element(document, ".minimap-viewport") else {
        return Ok(());
    };

    let zoom = graph_zoom(document);
    viewport.set_attribute(
        "x",
        &format!("{:.1}", f64::from(graph.scroll_left()) / zoom),
    )?;
    viewport.set_attribute("y", &format!("{:.1}", f64::from(graph.scroll_top()) / zoom))?;
    viewport.set_attribute(
        "width",
        &format!("{:.1}", f64::from(graph.client_width()) / zoom),
    )?;
    viewport.set_attribute(
        "height",
        &format!("{:.1}", f64::from(graph.client_height()) / zoom),
    )?;
    Ok(())
}

fn scroll_graph_from_minimap(document: &Document, event: &MouseEvent) -> Result<(), JsValue> {
    let Some(graph) = scroll_element(document, ".graph-wrap") else {
        return Ok(());
    };
    let Some(minimap) = scroll_element(document, ".minimap") else {
        return Ok(());
    };
    let Some(graph_width) = data_f64(&minimap, "graph-width") else {
        return Ok(());
    };
    let Some(graph_height) = data_f64(&minimap, "graph-height") else {
        return Ok(());
    };

    let rect = minimap.get_bounding_client_rect();
    let Some(content_rect) =
        minimap_content_rect(rect.width(), rect.height(), graph_width, graph_height)
    else {
        return Ok(());
    };
    let local_x = f64::from(event.client_x()) - rect.left() - content_rect.left;
    let local_y = f64::from(event.client_y()) - rect.top() - content_rect.top;
    let ratio_x = (local_x / content_rect.width).clamp(0.0, 1.0);
    let ratio_y = (local_y / content_rect.height).clamp(0.0, 1.0);
    let zoom = graph_zoom(document);
    let target_left = ratio_x * graph_width * zoom - f64::from(graph.client_width()) / 2.0;
    let target_top = ratio_y * graph_height * zoom - f64::from(graph.client_height()) / 2.0;

    graph.set_scroll_left(clamp_scroll(
        target_left,
        graph.scroll_width(),
        graph.client_width(),
    ));
    graph.set_scroll_top(clamp_scroll(
        target_top,
        graph.scroll_height(),
        graph.client_height(),
    ));
    update_minimap_viewport(document)
}

fn update_graph_visibility(document: &Document) -> Result<(), JsValue> {
    let Some(graph) = scroll_element(document, ".graph-wrap") else {
        return Ok(());
    };
    let zoom = graph_zoom(document);
    let left = f64::from(graph.scroll_left()) / zoom - CULL_PADDING;
    let top = f64::from(graph.scroll_top()) / zoom - CULL_PADDING;
    let right = left + f64::from(graph.client_width()) / zoom + CULL_PADDING * 2.0;
    let bottom = top + f64::from(graph.client_height()) / zoom + CULL_PADDING * 2.0;

    let items = document.query_selector_all(".graph-item")?;
    for index in 0..items.length() {
        let Some(item) = items.item(index) else {
            continue;
        };
        let Ok(item) = item.dyn_into::<Element>() else {
            continue;
        };
        let visible = graph_item_intersects(&item, left, top, right, bottom);
        if visible {
            item.class_list().remove_1("is-culled")?;
        } else {
            item.class_list().add_1("is-culled")?;
        }
    }

    Ok(())
}

fn graph_item_intersects(item: &Element, left: f64, top: f64, right: f64, bottom: f64) -> bool {
    if let (Some(x), Some(y)) = (data_f64(item, "graph-x"), data_f64(item, "graph-y")) {
        return x >= left && x <= right && y >= top && y <= bottom;
    }

    let Some(min_x) = data_f64(item, "graph-min-x") else {
        return true;
    };
    let Some(min_y) = data_f64(item, "graph-min-y") else {
        return true;
    };
    let Some(max_x) = data_f64(item, "graph-max-x") else {
        return true;
    };
    let Some(max_y) = data_f64(item, "graph-max-y") else {
        return true;
    };

    max_x >= left && min_x <= right && max_y >= top && min_y <= bottom
}

struct MinimapContentRect {
    left: f64,
    top: f64,
    width: f64,
    height: f64,
}

fn minimap_content_rect(
    minimap_width: f64,
    minimap_height: f64,
    graph_width: f64,
    graph_height: f64,
) -> Option<MinimapContentRect> {
    if minimap_width <= 0.0 || minimap_height <= 0.0 || graph_width <= 0.0 || graph_height <= 0.0 {
        return None;
    }

    let scale = (minimap_width / graph_width).min(minimap_height / graph_height);
    let width = graph_width * scale;
    let height = graph_height * scale;

    Some(MinimapContentRect {
        left: (minimap_width - width) / 2.0,
        top: (minimap_height - height) / 2.0,
        width,
        height,
    })
}

fn data_f64(element: &Element, key: &str) -> Option<f64> {
    element
        .get_attribute(&format!("data-{key}"))?
        .parse::<f64>()
        .ok()
}

fn clamp_scroll(value: f64, content_size: i32, viewport_size: i32) -> i32 {
    let max_scroll = (content_size - viewport_size).max(0);
    value.round().clamp(0.0, f64::from(max_scroll)) as i32
}

fn update_node_detail_from_hash(window: &Window, document: &Document) -> Result<(), JsValue> {
    let selected = selected_node_id(window, document)?;
    update_node_selection(document, selected.as_deref())?;

    let Some(node_id) = selected else {
        hide_node_detail(document)?;
        return Ok(());
    };

    show_node_detail_loading(document, &node_id)?;
    let detail_window = window.clone();
    let detail_document = document.clone();
    spawn_local(async move {
        if let Err(error) = load_node_detail(&detail_window, &detail_document, node_id).await {
            web_sys::console::error_1(&error);
        }
    });

    Ok(())
}

fn selected_node_id(window: &Window, document: &Document) -> Result<Option<String>, JsValue> {
    let hash = window.location().hash().unwrap_or_default();
    let Some(target) = hash.strip_prefix('#').filter(|target| !target.is_empty()) else {
        return Ok(None);
    };
    let Some(link) = document.query_selector(&format!("[data-node-target=\"{target}\"]"))? else {
        return Ok(None);
    };

    Ok(link.get_attribute("data-node-id"))
}

fn update_node_selection(
    document: &Document,
    selected_node_id: Option<&str>,
) -> Result<(), JsValue> {
    let links = document.query_selector_all(".node-link")?;
    for index in 0..links.length() {
        let Some(link) = links.item(index) else {
            continue;
        };
        let Ok(link) = link.dyn_into::<Element>() else {
            continue;
        };
        if link.get_attribute("data-node-id").as_deref() == selected_node_id {
            link.class_list().add_1("node-selected")?;
        } else {
            link.class_list().remove_1("node-selected")?;
        }
    }
    Ok(())
}

fn hide_node_detail(document: &Document) -> Result<(), JsValue> {
    if let Some(side) = scroll_element(document, ".side") {
        side.class_list().remove_1("has-selection")?;
    }
    if let Some(panel) = scroll_element(document, ".node-detail-panel") {
        panel.set_attribute("hidden", "true")?;
    }
    Ok(())
}

fn show_node_detail_loading(document: &Document, node_id: &str) -> Result<(), JsValue> {
    let Some(panel) = scroll_element(document, ".node-detail-panel") else {
        return Ok(());
    };
    if let Some(side) = scroll_element(document, ".side") {
        side.class_list().add_1("has-selection")?;
    }
    panel.remove_attribute("hidden")?;
    set_node_detail_field(&panel, "id", node_id)?;
    set_node_detail_field(&panel, "kind", "")?;
    set_node_detail_field(&panel, "role", "")?;
    set_node_detail_field(&panel, "created_at", "")?;
    set_node_detail_field(&panel, "labels", "")?;
    set_node_detail_field(&panel, "content", "Loading...")?;
    Ok(())
}

async fn load_node_detail(
    window: &Window,
    document: &Document,
    node_id: String,
) -> Result<(), JsValue> {
    let response = fetch_text(window, "/api/graph").await?;
    let snapshot = serde_json::from_str::<ClientGraphSnapshot>(&response)
        .map_err(|error| JsValue::from_str(&format!("failed to parse graph snapshot: {error}")))?;
    let Some(node) = snapshot.nodes.into_iter().find(|node| node.id == node_id) else {
        return Ok(());
    };
    if selected_node_id(window, document)?.as_deref() != Some(node.id.as_str()) {
        return Ok(());
    }
    render_node_detail(document, &node)
}

fn render_node_detail(document: &Document, node: &ClientGraphNode) -> Result<(), JsValue> {
    let Some(panel) = scroll_element(document, ".node-detail-panel") else {
        return Ok(());
    };
    let labels = if node.labels.is_empty() {
        "None".to_owned()
    } else {
        node.labels.join(", ")
    };

    set_node_detail_field(&panel, "id", &node.id)?;
    set_node_detail_field(&panel, "kind", &node.kind)?;
    set_node_detail_field(&panel, "role", &node.role)?;
    set_node_detail_field(&panel, "created_at", &node.created_at)?;
    set_node_detail_field(&panel, "labels", &labels)?;
    set_node_detail_field(&panel, "content", &node.content)
}

fn set_node_detail_field(panel: &Element, field: &str, value: &str) -> Result<(), JsValue> {
    if let Some(element) = panel.query_selector(&format!("[data-node-field=\"{field}\"]"))? {
        element.set_text_content(Some(value));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, Default)]
struct ScrollState {
    graph_left: i32,
    graph_top: i32,
    side_top: i32,
    detail_top: i32,
}

impl ScrollState {
    fn read(document: &Document) -> Self {
        let graph = scroll_element(document, ".graph-wrap");
        let side = scroll_element(document, ".branch-section");
        let detail = scroll_element(document, ".node-detail-panel");

        Self {
            graph_left: graph.as_ref().map_or(0, Element::scroll_left),
            graph_top: graph.as_ref().map_or(0, Element::scroll_top),
            side_top: side.as_ref().map_or(0, Element::scroll_top),
            detail_top: detail.as_ref().map_or(0, Element::scroll_top),
        }
    }

    fn restore(self, document: &Document) {
        if let Some(graph) = scroll_element(document, ".graph-wrap") {
            graph.set_scroll_left(self.graph_left);
            graph.set_scroll_top(self.graph_top);
        }
        if let Some(side) = scroll_element(document, ".branch-section") {
            side.set_scroll_top(self.side_top);
        }
        if let Some(detail) = scroll_element(document, ".node-detail-panel") {
            detail.set_scroll_top(self.detail_top);
        }
    }

    fn save(self, window: &Window) {
        let Some(storage) = session_storage(window) else {
            return;
        };
        let value = format!(
            "{},{},{},{}",
            self.graph_left, self.graph_top, self.side_top, self.detail_top
        );
        let _ = storage.set_item(SCROLL_KEY, &value);
    }

    fn load(window: &Window) -> Option<Self> {
        let storage = session_storage(window)?;
        let value = storage.get_item(SCROLL_KEY).ok().flatten()?;
        let mut parts = value.split(',');
        Some(Self {
            graph_left: parts.next()?.parse().ok()?,
            graph_top: parts.next()?.parse().ok()?,
            side_top: parts.next()?.parse().ok()?,
            detail_top: parts.next()?.parse().ok()?,
        })
    }
}

fn scroll_element(document: &Document, selector: &str) -> Option<Element> {
    document.query_selector(selector).ok().flatten()
}

fn session_storage(window: &Window) -> Option<web_sys::Storage> {
    window.session_storage().ok().flatten()
}
