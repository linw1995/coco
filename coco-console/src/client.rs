use std::collections::HashSet;

use wasm_bindgen::{JsCast, JsValue, closure::Closure, prelude::wasm_bindgen};
use wasm_bindgen_futures::{JsFuture, spawn_local};
use web_sys::{Document, Element, MouseEvent, Response, Window};

const ROOT_ID: &str = "console-root";
const SCROLL_KEY: &str = "coco-console:scroll";

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
    if let Some(state) = ScrollState::load(&window) {
        state.restore(&document);
    }
    install_minimap(&document)?;

    loop {
        let version = current_version(&document).unwrap_or_default();
        let known_nodes = collect_node_ids(&document);
        let scroll = ScrollState::read(&document);
        scroll.save(&window);

        let fragment = fetch_text(&window, &format!("/fragment?version={version}")).await?;
        replace_root(&document, &fragment)?;
        mark_new_nodes(&document, &known_nodes)?;
        scroll.restore(&document);
        install_minimap(&document)?;
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
        let _ = update_minimap_viewport(&document);
    });
    window.add_event_listener_with_callback("resize", closure.as_ref().unchecked_ref())?;
    closure.forget();
    Ok(())
}

fn install_minimap(document: &Document) -> Result<(), JsValue> {
    update_minimap_viewport(document)?;

    let scroll_document = document.clone();
    if let Some(graph) = scroll_element(document, ".graph-wrap") {
        let closure = Closure::<dyn FnMut()>::new(move || {
            let _ = update_minimap_viewport(&scroll_document);
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

fn update_minimap_viewport(document: &Document) -> Result<(), JsValue> {
    let Some(graph) = scroll_element(document, ".graph-wrap") else {
        return Ok(());
    };
    let Some(viewport) = scroll_element(document, ".minimap-viewport") else {
        return Ok(());
    };

    viewport.set_attribute("x", &graph.scroll_left().to_string())?;
    viewport.set_attribute("y", &graph.scroll_top().to_string())?;
    viewport.set_attribute("width", &graph.client_width().to_string())?;
    viewport.set_attribute("height", &graph.client_height().to_string())?;
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
    let target_left = ratio_x * graph_width - f64::from(graph.client_width()) / 2.0;
    let target_top = ratio_y * graph_height - f64::from(graph.client_height()) / 2.0;

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
        let detail = scroll_element(document, ".node-detail:target");

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
        if let Some(detail) = scroll_element(document, ".node-detail:target") {
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
