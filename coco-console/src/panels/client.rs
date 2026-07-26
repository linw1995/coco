use leptos::prelude::*;
use leptos::{
    ev,
    leptos_dom::helpers::{location_hash, request_animation_frame, window_event_listener},
};

use super::{NODE_DETAIL_PANEL_ID, PanelSelection};

const MOBILE_VIEWPORT_MAX_WIDTH: i32 = 860;
pub const PROVIDER_CONTEXT_RENDERED_EVENT: &str = "coco-provider-context-rendered";

pub fn subscribe_to_panel_selection(selection: RwSignal<PanelSelection>) {
    Effect::new(move || {
        request_animation_frame(move || selection.set(current_panel_selection()));
        let listener = window_event_listener(ev::hashchange, move |_| {
            selection.set(current_panel_selection());
        });
        on_cleanup(move || listener.remove());
    });
}

fn current_panel_selection() -> PanelSelection {
    PanelSelection::from_hash(location_hash().as_deref().unwrap_or_default())
}

pub fn reveal_node_detail_on_mobile() {
    let Some(document) = web_sys::window().and_then(|window| window.document()) else {
        return;
    };
    let Some(viewport_width) = document.document_element().map(|root| root.client_width()) else {
        return;
    };
    reveal_node_detail(document, viewport_width);
}

fn reveal_node_detail(document: web_sys::Document, viewport_width: i32) {
    if viewport_width > MOBILE_VIEWPORT_MAX_WIDTH {
        return;
    }
    request_animation_frame(move || {
        if let Some(detail) = document.get_element_by_id(NODE_DETAIL_PANEL_ID) {
            detail.scroll_into_view();
        }
    });
}

pub fn notify_provider_context_rendered() {
    request_animation_frame(|| {
        let Ok(event) = web_sys::Event::new(PROVIDER_CONTEXT_RENDERED_EVENT) else {
            return;
        };
        if let Some(window) = web_sys::window() {
            let _ = window.dispatch_event(&event);
        }
    });
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, rc::Rc};

    use js_sys::Promise;
    use wasm_bindgen::{JsValue, UnwrapThrowExt, closure::Closure};
    use wasm_bindgen_futures::JsFuture;
    use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

    use super::*;

    wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    async fn graph_items_mobile_node_detail_reveal_scrolls_target_into_view() {
        let window = web_sys::window().expect_throw("window should be available");
        let document = window
            .document()
            .expect_throw("document should be available");
        let root = document
            .create_element("div")
            .expect_throw("test root should be created");
        let detail = document
            .create_element("section")
            .expect_throw("detail should be created");
        detail.set_id(NODE_DETAIL_PANEL_ID);
        let scroll_invoked = Rc::new(Cell::new(false));
        let callback_invoked = Rc::clone(&scroll_invoked);
        let scroll_into_view = Closure::<dyn FnMut()>::new(move || callback_invoked.set(true));
        js_sys::Reflect::set(
            detail.as_ref(),
            &JsValue::from_str("scrollIntoView"),
            scroll_into_view.as_ref(),
        )
        .expect_throw("scrollIntoView should be replaceable");
        root.append_child(&detail)
            .expect_throw("detail should be mounted");
        document
            .body()
            .expect_throw("document body should be available")
            .append_child(&root)
            .expect_throw("test root should be mounted");

        reveal_node_detail(document, MOBILE_VIEWPORT_MAX_WIDTH);
        next_animation_frame().await;

        assert!(scroll_invoked.get());
        root.remove();
    }

    async fn next_animation_frame() {
        let promise = Promise::new(&mut |resolve, _| {
            request_animation_frame(move || {
                resolve
                    .call0(&JsValue::UNDEFINED)
                    .expect_throw("animation frame promise should resolve");
            });
        });
        JsFuture::from(promise)
            .await
            .expect_throw("animation frame should run");
    }
}
