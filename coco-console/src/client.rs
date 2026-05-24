use std::collections::HashSet;

use serde::Deserialize;
use serde_json::Value;
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
const DEFAULT_GRAPH_VIEWPORT_WIDTH: f64 = 1280.0;
const DEFAULT_GRAPH_VIEWPORT_HEIGHT: f64 = 720.0;

#[derive(Debug, Deserialize)]
struct ClientGraphNode {
    id: String,
    kind: String,
    role: String,
    created_at: String,
    content: String,
    labels: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", content = "items", rename_all = "snake_case")]
enum ClientEntityCollection {
    Branches(Vec<ClientBranch>),
    Sessions(Vec<ClientSession>),
    Presets(Vec<ClientPreset>),
    Skills(Vec<ClientSkill>),
    Jobs(Vec<ClientJob>),
    Queues(Vec<ClientQueue>),
}

#[derive(Debug, Deserialize)]
struct ClientBranch {
    name: String,
    head_id: String,
    state: Value,
}

#[derive(Debug, Deserialize)]
struct ClientSession {
    branch: String,
    head_id: String,
    state: String,
    target_branch: Option<String>,
    base_head_id: Option<String>,
    pause_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ClientPreset {
    name: String,
    current_version: u64,
    version_count: usize,
    role: String,
    provider_profile: String,
    model: String,
    tool_count: usize,
    prompt: String,
    system_prompt: String,
}

#[derive(Debug, Deserialize)]
struct ClientSkill {
    role: String,
    name: String,
    current_version: u64,
    version_count: usize,
    revision_id: String,
    description: String,
    script_count: usize,
    enable_coco_shim: bool,
}

#[derive(Debug, Deserialize)]
struct ClientJob {
    job_id: String,
    created_at: String,
    finished_at: Option<String>,
    branch: String,
    base: String,
    status: String,
}

#[derive(Debug, Deserialize)]
struct ClientQueue {
    name: String,
    message_count: usize,
    messages: Vec<ClientQueueMessage>,
}

#[derive(Debug, Deserialize)]
struct ClientQueueMessage {
    message_id: String,
    created_at: String,
    payload: String,
}

#[derive(Clone)]
struct SelectedNode {
    target: String,
    id: Option<String>,
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
    install_entity_navigation(&window, &document)?;
    if let Some(state) = ScrollState::load(&window) {
        state.restore(&document);
    }
    update_hash_state(&window, &document)?;
    update_graph_viewport_state(&document)?;

    loop {
        let version = current_version(&document).unwrap_or_default();
        let known_nodes = collect_node_ids(&document);
        let scroll = ScrollState::read(&document);
        scroll.save(&window);

        let fragment = fetch_text(&window, &format!("/fragment?version={version}")).await?;
        replace_root(&document, &fragment)?;
        install_graph_interactions(&window, &document)?;
        install_entity_navigation(&window, &document)?;
        mark_new_nodes(&document, &known_nodes)?;
        scroll.restore(&document);
        update_hash_state(&window, &document)?;
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
        let _ = update_hash_state(&window_for_listener, &document);
    });
    window.add_event_listener_with_callback("hashchange", closure.as_ref().unchecked_ref())?;
    closure.forget();
    Ok(())
}

fn install_entity_navigation(window: &Window, document: &Document) -> Result<(), JsValue> {
    let links = document.query_selector_all(".entity-nav-link")?;
    for index in 0..links.length() {
        let Some(link) = links.item(index) else {
            continue;
        };
        let Ok(link) = link.dyn_into::<Element>() else {
            continue;
        };
        let Some(href) = link.get_attribute("href") else {
            continue;
        };
        let Some(section_id) = href.strip_prefix('#') else {
            continue;
        };
        let Some(kind) = entity_kind_for_section(document, section_id) else {
            continue;
        };

        let nav_window = window.clone();
        let nav_document = document.clone();
        let closure = Closure::<dyn FnMut(MouseEvent)>::new(move |_| {
            let load_window = nav_window.clone();
            let load_document = nav_document.clone();
            let load_kind = kind.clone();
            spawn_local(async move {
                if let Err(error) =
                    load_entity_section(&load_window, &load_document, &load_kind).await
                {
                    web_sys::console::error_1(&error);
                }
            });
        });
        link.add_event_listener_with_callback("click", closure.as_ref().unchecked_ref())?;
        closure.forget();
    }
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
    let Some(items) = scroll_element(document, ".graph-items") else {
        return Ok(());
    };
    let zoom = graph_zoom(document);
    let viewport_width = if graph.client_width() > 0 {
        f64::from(graph.client_width())
    } else {
        DEFAULT_GRAPH_VIEWPORT_WIDTH
    };
    let viewport_height = if graph.client_height() > 0 {
        f64::from(graph.client_height())
    } else {
        DEFAULT_GRAPH_VIEWPORT_HEIGHT
    };
    let left = f64::from(graph.scroll_left()) / zoom - CULL_PADDING;
    let top = f64::from(graph.scroll_top()) / zoom - CULL_PADDING;
    let right = left + viewport_width / zoom + CULL_PADDING * 2.0;
    let bottom = top + viewport_height / zoom + CULL_PADDING * 2.0;
    let key = format!("{left:.0}:{top:.0}:{right:.0}:{bottom:.0}");

    if items.get_attribute("data-graph-items-key").as_deref() == Some(key.as_str())
        || items.get_attribute("data-graph-items-pending").as_deref() == Some(key.as_str())
    {
        return Ok(());
    }

    let Some(window) = web_sys::window() else {
        return Ok(());
    };
    items.set_attribute("data-graph-items-pending", &key)?;
    let load_document = document.clone();
    spawn_local(async move {
        if let Err(error) =
            load_graph_items(&window, &load_document, left, top, right, bottom, key).await
        {
            web_sys::console::error_1(&error);
        }
    });
    Ok(())
}

async fn load_graph_items(
    window: &Window,
    document: &Document,
    left: f64,
    top: f64,
    right: f64,
    bottom: f64,
    key: String,
) -> Result<(), JsValue> {
    let url =
        format!("/api/graph-items?left={left:.0}&top={top:.0}&right={right:.0}&bottom={bottom:.0}");
    let fragment = match fetch_text(window, &url).await {
        Ok(fragment) => fragment,
        Err(error) => {
            let _ = clear_graph_items_pending(document, &key);
            return Err(error);
        }
    };
    let Some(items) = scroll_element(document, ".graph-items") else {
        return Ok(());
    };
    if items.get_attribute("data-graph-items-pending").as_deref() != Some(key.as_str()) {
        return Ok(());
    }

    items.set_inner_html(&fragment);
    items.set_attribute("data-graph-items-key", &key)?;
    items.remove_attribute("data-graph-items-pending")?;
    update_node_detail_from_hash(window, document)
}

fn clear_graph_items_pending(document: &Document, key: &str) -> Result<(), JsValue> {
    let Some(items) = scroll_element(document, ".graph-items") else {
        return Ok(());
    };
    if items.get_attribute("data-graph-items-pending").as_deref() == Some(key) {
        items.remove_attribute("data-graph-items-pending")?;
    }
    Ok(())
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

fn update_hash_state(window: &Window, document: &Document) -> Result<(), JsValue> {
    update_node_detail_from_hash(window, document)?;
    load_entity_from_hash(window, document)
}

fn load_entity_from_hash(window: &Window, document: &Document) -> Result<(), JsValue> {
    let hash = window.location().hash().unwrap_or_default();
    let Some(section_id) = hash.strip_prefix('#').filter(|value| !value.is_empty()) else {
        return Ok(());
    };
    let Some(kind) = entity_kind_for_section(document, section_id) else {
        return Ok(());
    };

    let load_window = window.clone();
    let load_document = document.clone();
    spawn_local(async move {
        if let Err(error) = load_entity_section(&load_window, &load_document, &kind).await {
            web_sys::console::error_1(&error);
        }
    });
    Ok(())
}

fn entity_kind_for_section(document: &Document, section_id: &str) -> Option<String> {
    document
        .get_element_by_id(section_id)?
        .get_attribute("data-entity-kind")
}

async fn load_entity_section(
    window: &Window,
    document: &Document,
    kind: &str,
) -> Result<(), JsValue> {
    let Some(body) = entity_section_body(document, kind)? else {
        return Ok(());
    };
    match body.get_attribute("data-entity-state").as_deref() {
        Some("loaded") | Some("loading") => return Ok(()),
        _ => {}
    }

    set_entity_status(&body, "loading", "Loading...")?;
    let result = async {
        let response = fetch_text(window, &format!("/api/entities?kind={kind}")).await?;
        serde_json::from_str::<ClientEntityCollection>(&response)
            .map_err(|error| JsValue::from_str(&format!("failed to parse entity records: {error}")))
    }
    .await;

    match result {
        Ok(collection) => render_entity_collection(document, &body, collection),
        Err(error) => {
            set_entity_status(&body, "error", "Failed to load records.")?;
            Err(error)
        }
    }
}

fn entity_section_body(document: &Document, kind: &str) -> Result<Option<Element>, JsValue> {
    document.query_selector(&format!(
        ".entity-section-body[data-entity-kind=\"{kind}\"]"
    ))
}

fn set_entity_status(body: &Element, state: &str, message: &str) -> Result<(), JsValue> {
    body.set_attribute("data-entity-state", state)?;
    body.set_text_content(None);
    let Some(document) = body.owner_document() else {
        return Ok(());
    };
    append_text_element(&document, body, "p", "entity-placeholder", message)?;
    Ok(())
}

fn render_entity_collection(
    document: &Document,
    body: &Element,
    collection: ClientEntityCollection,
) -> Result<(), JsValue> {
    body.set_text_content(None);
    body.set_attribute("data-entity-state", "loaded")?;

    match collection {
        ClientEntityCollection::Branches(items) => {
            render_entity_list(document, body, items, render_branch_item)
        }
        ClientEntityCollection::Sessions(items) => {
            render_entity_list(document, body, items, render_session_item)
        }
        ClientEntityCollection::Presets(items) => {
            render_entity_list(document, body, items, render_preset_item)
        }
        ClientEntityCollection::Skills(items) => {
            render_entity_list(document, body, items, render_skill_item)
        }
        ClientEntityCollection::Jobs(items) => {
            render_entity_list(document, body, items, render_job_item)
        }
        ClientEntityCollection::Queues(items) => {
            render_entity_list(document, body, items, render_queue_item)
        }
    }
}

fn render_entity_list<T>(
    document: &Document,
    body: &Element,
    items: Vec<T>,
    render_item: fn(&Document, &Element, T) -> Result<(), JsValue>,
) -> Result<(), JsValue> {
    if items.is_empty() {
        append_text_element(document, body, "p", "entity-placeholder", "No records.")?;
        return Ok(());
    }

    let list = document.create_element("ul")?;
    list.set_class_name("entity-list");
    for item in items {
        render_item(document, &list, item)?;
    }
    body.append_child(&list)?;
    Ok(())
}

fn render_branch_item(
    document: &Document,
    list: &Element,
    branch: ClientBranch,
) -> Result<(), JsValue> {
    let item = document.create_element("li")?;
    item.set_class_name("branch");
    append_text_element(document, &item, "strong", "", &branch.name)?;
    append_text_element(
        document,
        &item,
        "span",
        "",
        &format!("head {}", shorten_id(&branch.head_id)),
    )?;
    append_text_element(
        document,
        &item,
        "span",
        "",
        &format_state_value(&branch.state),
    )?;
    list.append_child(&item)?;
    Ok(())
}

fn render_session_item(
    document: &Document,
    list: &Element,
    session: ClientSession,
) -> Result<(), JsValue> {
    let item = document.create_element("li")?;
    item.set_class_name("entity-card");
    append_text_element(document, &item, "strong", "", &session.branch)?;
    append_text_element(document, &item, "span", "", &session.state)?;
    append_text_element(
        document,
        &item,
        "span",
        "",
        &format!("head {}", shorten_id(&session.head_id)),
    )?;
    if let Some(target) = session.target_branch {
        append_text_element(document, &item, "span", "", &format!("target {target}"))?;
    }
    if let Some(base) = session.base_head_id {
        append_text_element(
            document,
            &item,
            "span",
            "",
            &format!("base {}", shorten_id(&base)),
        )?;
    }
    if let Some(reason) = session.pause_reason {
        append_text_element(document, &item, "span", "", &reason)?;
    }
    list.append_child(&item)?;
    Ok(())
}

fn render_preset_item(
    document: &Document,
    list: &Element,
    preset: ClientPreset,
) -> Result<(), JsValue> {
    let item = document.create_element("li")?;
    item.set_class_name("entity-card");
    append_text_element(document, &item, "strong", "", &preset.name)?;
    append_text_element(
        document,
        &item,
        "span",
        "",
        &format!(
            "v{} / {} versions",
            preset.current_version, preset.version_count
        ),
    )?;
    append_text_element(document, &item, "span", "", &preset.role)?;
    append_text_element(document, &item, "span", "", &preset.provider_profile)?;
    append_text_element(document, &item, "span", "", &preset.model)?;
    append_text_element(
        document,
        &item,
        "span",
        "",
        &format!("{} tools", preset.tool_count),
    )?;
    append_text_element(document, &item, "p", "", &preset.prompt)?;
    append_text_element(document, &item, "p", "", &preset.system_prompt)?;
    list.append_child(&item)?;
    Ok(())
}

fn render_skill_item(
    document: &Document,
    list: &Element,
    skill: ClientSkill,
) -> Result<(), JsValue> {
    let item = document.create_element("li")?;
    item.set_class_name("entity-card");
    append_text_element(document, &item, "strong", "", &skill.name)?;
    append_text_element(document, &item, "span", "", &skill.role)?;
    append_text_element(
        document,
        &item,
        "span",
        "",
        &format!(
            "v{} / {} versions",
            skill.current_version, skill.version_count
        ),
    )?;
    append_text_element(
        document,
        &item,
        "span",
        "",
        &format!("revision {}", shorten_id(&skill.revision_id)),
    )?;
    append_text_element(
        document,
        &item,
        "span",
        "",
        &format!("{} scripts", skill.script_count),
    )?;
    append_text_element(
        document,
        &item,
        "span",
        "",
        if skill.enable_coco_shim {
            "shim enabled"
        } else {
            "shim disabled"
        },
    )?;
    append_text_element(document, &item, "p", "", &skill.description)?;
    list.append_child(&item)?;
    Ok(())
}

fn render_job_item(document: &Document, list: &Element, job: ClientJob) -> Result<(), JsValue> {
    let item = document.create_element("li")?;
    item.set_class_name("entity-card");
    append_text_element(document, &item, "strong", "", &shorten_id(&job.job_id))?;
    append_text_element(document, &item, "span", "", &job.status)?;
    append_text_element(document, &item, "span", "", &job.branch)?;
    append_text_element(
        document,
        &item,
        "span",
        "",
        &format!("base {}", shorten_id(&job.base)),
    )?;
    append_text_element(document, &item, "span", "", &job.created_at)?;
    if let Some(finished_at) = job.finished_at {
        append_text_element(
            document,
            &item,
            "span",
            "",
            &format!("finished {finished_at}"),
        )?;
    }
    list.append_child(&item)?;
    Ok(())
}

fn render_queue_item(
    document: &Document,
    list: &Element,
    queue: ClientQueue,
) -> Result<(), JsValue> {
    let item = document.create_element("li")?;
    item.set_class_name("entity-card");
    append_text_element(document, &item, "strong", "", &queue.name)?;
    append_text_element(
        document,
        &item,
        "span",
        "",
        &format!("{} messages", queue.message_count),
    )?;

    let messages = document.create_element("ul")?;
    messages.set_class_name("queue-message-list");
    for message in queue.messages {
        let message_item = document.create_element("li")?;
        append_text_element(
            document,
            &message_item,
            "strong",
            "",
            &shorten_id(&message.message_id),
        )?;
        append_text_element(document, &message_item, "span", "", &message.created_at)?;
        append_text_element(document, &message_item, "pre", "", &message.payload)?;
        messages.append_child(&message_item)?;
    }
    item.append_child(&messages)?;
    list.append_child(&item)?;
    Ok(())
}

fn append_text_element(
    document: &Document,
    parent: &Element,
    tag: &str,
    class_name: &str,
    text: &str,
) -> Result<Element, JsValue> {
    let child = document.create_element(tag)?;
    if !class_name.is_empty() {
        child.set_class_name(class_name);
    }
    child.set_text_content(Some(text));
    parent.append_child(&child)?;
    Ok(child)
}

fn format_state_value(value: &Value) -> String {
    match value {
        Value::String(raw) => raw.clone(),
        Value::Object(fields) if fields.len() == 1 => fields
            .keys()
            .next()
            .cloned()
            .unwrap_or_else(|| value.to_string()),
        _ => value.to_string(),
    }
}

fn update_node_detail_from_hash(window: &Window, document: &Document) -> Result<(), JsValue> {
    let selected = selected_node(window, document)?;
    update_node_selection(
        document,
        selected.as_ref().and_then(|node| node.id.as_deref()),
    )?;

    let Some(selected) = selected else {
        hide_node_detail(document)?;
        return Ok(());
    };

    if node_detail_is_current(document, &selected)? {
        return Ok(());
    }

    show_node_detail_loading(
        document,
        selected.id.as_deref().unwrap_or(selected.target.as_str()),
        &selected,
    )?;
    let detail_window = window.clone();
    let detail_document = document.clone();
    let pending = selected.clone();
    spawn_local(async move {
        if let Err(error) = load_node_detail(&detail_window, &detail_document, selected).await {
            if let Err(render_error) =
                show_node_detail_error(&detail_window, &detail_document, &pending)
            {
                web_sys::console::error_1(&render_error);
            }
            web_sys::console::error_1(&error);
        }
    });

    Ok(())
}

fn selected_node(window: &Window, document: &Document) -> Result<Option<SelectedNode>, JsValue> {
    let hash = window.location().hash().unwrap_or_default();
    let Some(target) = hash
        .strip_prefix('#')
        .filter(|target| target.starts_with("detail-"))
    else {
        return Ok(None);
    };
    Ok(Some(SelectedNode {
        target: target.to_owned(),
        id: rendered_node_id_for_target(document, target)?,
    }))
}

fn rendered_node_id_for_target(
    document: &Document,
    target: &str,
) -> Result<Option<String>, JsValue> {
    let links = document.query_selector_all(".node-link")?;
    for index in 0..links.length() {
        let Some(link) = links.item(index) else {
            continue;
        };
        let Ok(link) = link.dyn_into::<Element>() else {
            continue;
        };
        if link.get_attribute("data-node-target").as_deref() == Some(target) {
            return Ok(link.get_attribute("data-node-id"));
        }
    }

    Ok(None)
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
        panel.remove_attribute("data-node-detail-target")?;
        panel.remove_attribute("data-node-detail-id")?;
    }
    Ok(())
}

fn node_detail_is_current(document: &Document, selected: &SelectedNode) -> Result<bool, JsValue> {
    let Some(panel) = scroll_element(document, ".node-detail-panel") else {
        return Ok(false);
    };
    if panel.has_attribute("hidden") {
        return Ok(false);
    }

    let target_matches =
        panel.get_attribute("data-node-detail-target").as_deref() == Some(selected.target.as_str());
    let id_matches = selected
        .id
        .as_deref()
        .is_some_and(|id| panel.get_attribute("data-node-detail-id").as_deref() == Some(id));
    Ok(target_matches || id_matches)
}

fn show_node_detail_loading(
    document: &Document,
    node_id: &str,
    selected: &SelectedNode,
) -> Result<(), JsValue> {
    let Some(panel) = scroll_element(document, ".node-detail-panel") else {
        return Ok(());
    };
    if let Some(side) = scroll_element(document, ".side") {
        side.class_list().add_1("has-selection")?;
    }
    panel.remove_attribute("hidden")?;
    panel.set_attribute("data-node-detail-target", &selected.target)?;
    if let Some(id) = selected.id.as_deref() {
        panel.set_attribute("data-node-detail-id", id)?;
    } else {
        panel.remove_attribute("data-node-detail-id")?;
    }
    set_node_detail_field(&panel, "id", node_id)?;
    set_node_detail_field(&panel, "kind", "")?;
    set_node_detail_field(&panel, "role", "")?;
    set_node_detail_field(&panel, "created_at", "")?;
    set_node_detail_field(&panel, "labels", "")?;
    set_node_detail_field(&panel, "content", "Loading...")?;
    Ok(())
}

fn show_node_detail_error(
    window: &Window,
    document: &Document,
    selected: &SelectedNode,
) -> Result<(), JsValue> {
    if selected_node(window, document)?
        .as_ref()
        .map(|current| current.target.as_str())
        != Some(selected.target.as_str())
    {
        return Ok(());
    }
    let Some(panel) = scroll_element(document, ".node-detail-panel") else {
        return Ok(());
    };
    if panel.get_attribute("data-node-detail-target").as_deref() != Some(selected.target.as_str()) {
        return Ok(());
    }

    set_node_detail_field(&panel, "kind", "")?;
    set_node_detail_field(&panel, "role", "")?;
    set_node_detail_field(&panel, "created_at", "")?;
    set_node_detail_field(&panel, "labels", "")?;
    set_node_detail_field(&panel, "content", "Unable to load node detail.")?;
    Ok(())
}

async fn load_node_detail(
    window: &Window,
    document: &Document,
    selected: SelectedNode,
) -> Result<(), JsValue> {
    let query = match selected.id.as_deref() {
        Some(id) => format!("id={}", query_encode(id)),
        None => format!("target={}", query_encode(&selected.target)),
    };
    let response = fetch_text(window, &format!("/api/node?{query}")).await?;
    let node = serde_json::from_str::<ClientGraphNode>(&response)
        .map_err(|error| JsValue::from_str(&format!("failed to parse node detail: {error}")))?;
    let Some(current) = selected_node(window, document)? else {
        return Ok(());
    };
    if current.id.as_deref() != Some(node.id.as_str()) && current.target != node_target_id(&node.id)
    {
        return Ok(());
    }
    render_node_detail(document, &node)
}

fn shorten_id(id: &str) -> String {
    id.chars().take(8).collect()
}

fn query_encode(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(byte as char);
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

fn node_target_id(node_id: &str) -> String {
    format!("detail-{}", css_token(node_id))
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

fn render_node_detail(document: &Document, node: &ClientGraphNode) -> Result<(), JsValue> {
    let Some(panel) = scroll_element(document, ".node-detail-panel") else {
        return Ok(());
    };
    let labels = if node.labels.is_empty() {
        "None".to_owned()
    } else {
        node.labels.join(", ")
    };

    panel.set_attribute("data-node-detail-target", &node_target_id(&node.id))?;
    panel.set_attribute("data-node-detail-id", &node.id)?;
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
        let side = scroll_element(document, ".entity-workspace");
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
        if let Some(side) = scroll_element(document, ".entity-workspace") {
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
