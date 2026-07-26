use leptos::prelude::*;
use leptos::{
    ev,
    leptos_dom::helpers::{location_hash, request_animation_frame, window_event_listener},
};
use wasm_bindgen::JsCast;

use crate::api::GraphViewportEdgeKind;

use super::{AnchorRangeRequest, NODE_DETAIL_PANEL_ID, PanelSelection};

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

pub fn subscribe_to_anchor_range(selection: RwSignal<Option<AnchorRangeRequest>>) {
    let click_listener = window_event_listener(ev::click, move |event| {
        let Some(request) = anchor_range_request_from_event(&event) else {
            return;
        };
        event.prevent_default();
        event.stop_propagation();
        selection.set(Some(request));
    });
    let keyboard_listener = window_event_listener(ev::keydown, move |event| {
        let Some(request) = anchor_range_request_from_key_event(&event) else {
            return;
        };
        event.prevent_default();
        event.stop_propagation();
        selection.set(Some(request));
    });
    on_cleanup(move || {
        click_listener.remove();
        keyboard_listener.remove();
    });
}

fn current_panel_selection() -> PanelSelection {
    PanelSelection::from_hash(location_hash().as_deref().unwrap_or_default())
}

fn anchor_range_request_from_event(event: &web_sys::MouseEvent) -> Option<AnchorRangeRequest> {
    if event.button() != 0 || anchor_range_event_has_modifier(event) {
        return None;
    }
    anchor_range_request_from_target(event.target()?)
}

fn anchor_range_request_from_key_event(
    event: &web_sys::KeyboardEvent,
) -> Option<AnchorRangeRequest> {
    if !matches!(event.key().as_str(), "Enter" | " ") || anchor_range_key_has_modifier(event) {
        return None;
    }
    anchor_range_request_from_target(event.target()?)
}

fn anchor_range_event_has_modifier(event: &web_sys::MouseEvent) -> bool {
    [
        event.alt_key(),
        event.ctrl_key(),
        event.meta_key(),
        event.shift_key(),
    ]
    .contains(&true)
}

fn anchor_range_key_has_modifier(event: &web_sys::KeyboardEvent) -> bool {
    [
        event.alt_key(),
        event.ctrl_key(),
        event.meta_key(),
        event.shift_key(),
    ]
    .contains(&true)
}

fn anchor_range_request_from_target(target: web_sys::EventTarget) -> Option<AnchorRangeRequest> {
    let trigger = target
        .dyn_into::<web_sys::Element>()
        .ok()?
        .closest("[data-anchor-range=\"true\"][data-edge-kind][data-source-id][data-target-id]")
        .ok()
        .flatten()?;
    anchor_range_request_from_trigger(&trigger)
}

fn anchor_range_request_from_trigger(trigger: &web_sys::Element) -> Option<AnchorRangeRequest> {
    Some(AnchorRangeRequest {
        source: trigger.get_attribute("data-source-id")?,
        target: trigger.get_attribute("data-target-id")?,
        kind: trigger
            .get_attribute("data-edge-kind")
            .and_then(|kind| GraphViewportEdgeKind::from_key_part(&kind))?,
    })
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
    use web_sys::{Element, KeyboardEvent, KeyboardEventInit, MouseEvent, MouseEventInit};

    use super::*;

    wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    fn graph_items_anchor_range_request_reads_edge_trigger() {
        let (fixture, child) = anchor_range_fixture();

        let request = request_from_dispatched_click(&child, &MouseEventInit::new())
            .expect("plain click should select an anchor range");
        assert_eq!(request.source, "source");
        assert_eq!(request.target, "target");
        assert_eq!(request.kind, GraphViewportEdgeKind::Merge);

        let modified_click = MouseEventInit::new();
        modified_click.set_ctrl_key(true);
        assert!(request_from_dispatched_click(&child, &modified_click).is_none());

        let request = request_from_dispatched_key(&child, "Enter")
            .expect("Enter should select an anchor range");
        assert_eq!(request.kind, GraphViewportEdgeKind::Merge);
        assert!(request_from_dispatched_key(&child, " ").is_some());
        assert!(request_from_dispatched_key(&child, "Escape").is_none());

        fixture.remove();
    }

    #[wasm_bindgen_test]
    fn graph_items_anchor_range_subscription_tracks_pointer_and_keyboard_activation() {
        let owner = Owner::new();
        owner.set();
        let selection = RwSignal::new(None);
        subscribe_to_anchor_range(selection);
        let (fixture, child) = anchor_range_fixture();

        let click = MouseEventInit::new();
        click.set_bubbles(true);
        _ = request_from_dispatched_click(&child, &click);
        let selected = selection
            .get_untracked()
            .expect("bubbled click should update the range");
        assert_eq!(selected.kind, GraphViewportEdgeKind::Merge);

        selection.set(None);
        let key = KeyboardEventInit::new();
        key.set_bubbles(true);
        key.set_key("Enter");
        let event = KeyboardEvent::new_with_keyboard_event_init_dict("keydown", &key)
            .expect_throw("keyboard event should be created");
        child
            .dispatch_event(&event)
            .expect_throw("keyboard event should dispatch");
        assert!(selection.get_untracked().is_some());

        owner.cleanup();
        fixture.remove();
    }

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

    fn request_from_dispatched_click(
        target: &Element,
        init: &MouseEventInit,
    ) -> Option<AnchorRangeRequest> {
        let event = MouseEvent::new_with_mouse_event_init_dict("click", init)
            .expect_throw("click event should be created");
        target
            .dispatch_event(&event)
            .expect_throw("click event should dispatch");
        anchor_range_request_from_event(&event)
    }

    fn request_from_dispatched_key(target: &Element, key: &str) -> Option<AnchorRangeRequest> {
        let init = KeyboardEventInit::new();
        init.set_key(key);
        let event = KeyboardEvent::new_with_keyboard_event_init_dict("keydown", &init)
            .expect_throw("keyboard event should be created");
        target
            .dispatch_event(&event)
            .expect_throw("keyboard event should dispatch");
        anchor_range_request_from_key_event(&event)
    }

    fn anchor_range_fixture() -> (Element, Element) {
        let document = web_sys::window()
            .expect_throw("window should be available")
            .document()
            .expect_throw("document should be available");
        let fixture = document
            .create_element("div")
            .expect_throw("fixture should be created");
        fixture.set_inner_html(
            r#"
            <button data-anchor-range="true"
                    data-edge-kind="merge_parent"
                    data-source-id="source"
                    data-target-id="target">
              <span></span>
            </button>
            "#,
        );
        document
            .body()
            .expect_throw("document body should be available")
            .append_child(&fixture)
            .expect_throw("fixture should be mounted");
        let child = fixture
            .query_selector("span")
            .expect_throw("child query should succeed")
            .expect_throw("child should exist");
        (fixture, child)
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
