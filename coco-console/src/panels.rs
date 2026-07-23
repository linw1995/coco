use leptos::prelude::*;
#[cfg(target_arch = "wasm32")]
use serde::de::DeserializeOwned;

use crate::api::{NodeDetailResponse, PanelNode, ProviderContextItem, ProviderContextResponse};

#[cfg(target_arch = "wasm32")]
use leptos::{
    ev,
    leptos_dom::helpers::{location_hash, request_animation_frame, window_event_listener},
    task::spawn_local,
};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::{JsCast, JsValue};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::JsFuture;
#[cfg(target_arch = "wasm32")]
use web_sys::Response;

const NODE_TARGET_PREFIX: &str = "detail-";
#[cfg(target_arch = "wasm32")]
pub const PROVIDER_CONTEXT_RENDERED_EVENT: &str = "coco-provider-context-rendered";

#[cfg(any(target_arch = "wasm32", test))]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PanelSelection {
    pub target: Option<String>,
    pub context: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(not(any(target_arch = "wasm32", test)), allow(dead_code))]
enum PanelState<T> {
    Empty,
    Loading,
    Ready(T),
}

#[cfg(any(target_arch = "wasm32", test))]
impl PanelSelection {
    pub fn from_hash(hash: &str) -> Self {
        let hash = hash.strip_prefix('#').unwrap_or(hash);
        let (target, query) = hash
            .split_once('?')
            .map_or((hash, None), |(target, query)| (target, Some(query)));
        let target = target
            .starts_with(NODE_TARGET_PREFIX)
            .then(|| target.to_owned());
        let context = query.and_then(provider_context_target);

        Self { target, context }
    }
}

#[island]
pub fn NodeDetailPanel(graph_mode: String) -> impl IntoView {
    let state = RwSignal::new(PanelState::Empty);

    #[cfg(target_arch = "wasm32")]
    {
        let selection = use_panel_selection();
        let selected_target = Memo::new(move |_| selection.get().target);
        Effect::new(move || {
            let Some(target) = selected_target.get() else {
                state.set(PanelState::Empty);
                return;
            };
            state.set(PanelState::Loading);
            let url = node_detail_url(Some(&target), &graph_mode);
            spawn_local(async move {
                let response = fetch_panel_data::<NodeDetailResponse>(url).await;
                if selected_target.get_untracked().as_deref() == Some(&target) {
                    state.set(PanelState::Ready(response));
                }
            });
        });
    }

    #[cfg(not(target_arch = "wasm32"))]
    let _ = graph_mode;

    view! {
        <div class="panel-content">
            <NodeDetailPanelContent state=state/>
        </div>
    }
}

#[island]
pub fn ProviderContextPanel(graph_mode: String) -> impl IntoView {
    let state = RwSignal::new(PanelState::Empty);

    #[cfg(target_arch = "wasm32")]
    {
        let selection = use_panel_selection();
        let selected_context = Memo::new(move |_| {
            let selection = selection.get();
            selection.target.map(|target| (target, selection.context))
        });
        Effect::new(move || {
            let Some(request) = selected_context.get() else {
                state.set(PanelState::Empty);
                return;
            };
            state.set(PanelState::Loading);
            let url = provider_context_url(Some(&request.0), request.1.as_deref(), &graph_mode);
            spawn_local(async move {
                let response = fetch_panel_data::<ProviderContextResponse>(url).await;
                if selected_context.get_untracked().as_ref() == Some(&request) {
                    if response.is_ok() {
                        notify_provider_context_rendered();
                    }
                    state.set(PanelState::Ready(response));
                }
            });
        });
    }

    #[cfg(not(target_arch = "wasm32"))]
    let _ = graph_mode;

    view! {
        <div class="panel-content">
            <ProviderContextPanelContent state=state/>
        </div>
    }
}

#[cfg(target_arch = "wasm32")]
fn use_panel_selection() -> RwSignal<PanelSelection> {
    let selection = RwSignal::new(PanelSelection::default());
    Effect::new(move || {
        selection.set(current_panel_selection());
    });
    let listener = window_event_listener(ev::hashchange, move |_| {
        selection.set(current_panel_selection());
    });
    on_cleanup(move || listener.remove());
    selection
}

#[cfg(target_arch = "wasm32")]
fn current_panel_selection() -> PanelSelection {
    PanelSelection::from_hash(location_hash().as_deref().unwrap_or_default())
}

#[cfg(any(target_arch = "wasm32", test))]
fn node_detail_url(target: Option<&str>, graph_mode: &str) -> String {
    target
        .map(|target| {
            format!(
                "/api/node-detail?target={}&mode={graph_mode}",
                percent_encode(target)
            )
        })
        .unwrap_or_else(|| format!("/api/node-detail?mode={graph_mode}"))
}

#[cfg(any(target_arch = "wasm32", test))]
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

#[cfg(target_arch = "wasm32")]
async fn fetch_panel_data<T>(url: String) -> Result<T, String>
where
    T: DeserializeOwned,
{
    let window = web_sys::window().ok_or_else(|| "window is unavailable".to_owned())?;
    let response = JsFuture::from(window.fetch_with_str(&url))
        .await
        .map_err(js_error_message)?
        .dyn_into::<Response>()
        .map_err(js_error_message)?;
    if !response.ok() {
        return Err(format!("request failed with status {}", response.status()));
    }
    let text = JsFuture::from(response.text().map_err(js_error_message)?)
        .await
        .map_err(js_error_message)?
        .as_string()
        .ok_or_else(|| "response text is not a string".to_owned())?;
    serde_json::from_str(&text).map_err(|error| format!("invalid panel response: {error}"))
}

#[cfg(target_arch = "wasm32")]
fn js_error_message(error: JsValue) -> String {
    error.as_string().unwrap_or_else(|| format!("{error:?}"))
}

#[cfg(target_arch = "wasm32")]
fn notify_provider_context_rendered() {
    request_animation_frame(|| {
        let Ok(event) = web_sys::Event::new(PROVIDER_CONTEXT_RENDERED_EVENT) else {
            return;
        };
        if let Some(window) = web_sys::window() {
            let _ = window.dispatch_event(&event);
        }
    });
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct NodeDetailViewModel {
    class: &'static str,
    target: String,
    status_label: &'static str,
    status_message: String,
    status_detail_label: &'static str,
    status_detail: String,
    node: Option<PanelNode>,
}

impl NodeDetailViewModel {
    fn from_state(state: PanelState<Result<NodeDetailResponse, String>>) -> Self {
        let mut model = Self {
            class: "node-details node-details-default",
            target: String::new(),
            status_label: "Selection",
            status_message: "Select a node to inspect its content.".to_owned(),
            status_detail_label: "",
            status_detail: String::new(),
            node: None,
        };
        match state {
            PanelState::Empty | PanelState::Ready(Ok(NodeDetailResponse::Default)) => {}
            PanelState::Loading => {
                model.class = "node-details node-details-loading";
                model.status_message = "Loading node detail...".to_owned();
            }
            PanelState::Ready(Ok(NodeDetailResponse::Missing { target })) => {
                model.status_message = "The selected node is no longer available.".to_owned();
                model.status_detail_label = "Target";
                model.status_detail = target;
            }
            PanelState::Ready(Ok(NodeDetailResponse::Found { node })) => {
                model.class = "node-details node-detail";
                model.target = format!("{NODE_TARGET_PREFIX}{}", node.id);
                model.node = Some(node);
            }
            PanelState::Ready(Err(error)) => {
                model.status_label = "Error";
                model.status_message = "Failed to load node detail.".to_owned();
                model.status_detail_label = "Reason";
                model.status_detail = error;
            }
        }
        model
    }
}

#[component]
fn NodeDetailPanelContent(
    state: RwSignal<PanelState<Result<NodeDetailResponse, String>>>,
) -> impl IntoView {
    let model = Memo::new(move |_| NodeDetailViewModel::from_state(state.get()));
    let has_node = move || model.get().node.is_some();

    view! {
        <section
            id=move || model.get().target
            class=move || model.get().class
        >
            <h2>"Node"</h2>
            <dl class="detail-list">
                <div hidden=has_node>
                    <dt>{move || model.get().status_label}</dt>
                    <dd>{move || model.get().status_message}</dd>
                </div>
                <div hidden=move || model.get().status_detail.is_empty()>
                    <dt>{move || model.get().status_detail_label}</dt>
                    <dd>{move || model.get().status_detail}</dd>
                </div>
                <div hidden=move || !has_node()>
                    <dt>"Id"</dt>
                    <dd>{move || model.get().node.map(|node| node.id).unwrap_or_default()}</dd>
                </div>
                <div hidden=move || !has_node()>
                    <dt>"Kind"</dt>
                    <dd>{move || model.get().node.map(|node| node.kind).unwrap_or_default()}</dd>
                </div>
                <div hidden=move || !has_node()>
                    <dt>"Role"</dt>
                    <dd>{move || model.get().node.map(|node| node.role).unwrap_or_default()}</dd>
                </div>
                <div hidden=move || !has_node()>
                    <dt>"Created"</dt>
                    <dd>{move || model.get().node.map(|node| node.created_at).unwrap_or_default()}</dd>
                </div>
                <div hidden=move || !has_node()><dt>"Labels"</dt><dd>"None"</dd></div>
                <div hidden=move || !has_node()>
                    <dt>"Content"</dt>
                    <dd>{move || model.get().node.map(|node| node.content).unwrap_or_default()}</dd>
                </div>
            </dl>
        </section>
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ProviderContextViewModel {
    class: &'static str,
    message: String,
    detail: String,
    items: Vec<ProviderContextItem>,
}

impl ProviderContextViewModel {
    fn from_state(state: PanelState<Result<ProviderContextResponse, String>>) -> Self {
        let mut model = Self {
            class: "provider-context-section provider-context-default",
            message: "Select a node to inspect its provider context.".to_owned(),
            detail: String::new(),
            items: Vec::new(),
        };
        match state {
            PanelState::Empty | PanelState::Ready(Ok(ProviderContextResponse::Default)) => {}
            PanelState::Loading => {
                model.class = "provider-context-section provider-context-loading";
                model.message = "Loading provider context...".to_owned();
            }
            PanelState::Ready(Ok(ProviderContextResponse::Missing { target })) => {
                model.message = "The selected node is no longer available.".to_owned();
                model.detail = target;
            }
            PanelState::Ready(Ok(ProviderContextResponse::Found { items })) => {
                model.class = "provider-context-section";
                if items.is_empty() {
                    model.message = "No provider context nodes.".to_owned();
                } else {
                    model.message.clear();
                    model.items = items;
                }
            }
            PanelState::Ready(Err(error)) => {
                model.message = "Failed to load provider context.".to_owned();
                model.detail = error;
            }
        }
        model
    }
}

#[component]
fn ProviderContextPanelContent(
    state: RwSignal<PanelState<Result<ProviderContextResponse, String>>>,
) -> impl IntoView {
    let model = Memo::new(move |_| ProviderContextViewModel::from_state(state.get()));

    view! {
        <section class=move || model.get().class>
            <h2>"Provider Context"</h2>
            <p
                class="provider-context-empty"
                hidden=move || model.get().message.is_empty()
            >
                {move || model.get().message}
            </p>
            <p
                class="provider-context-target"
                hidden=move || model.get().detail.is_empty()
            >
                {move || model.get().detail}
            </p>
            <ol
                class="provider-context-list"
                hidden=move || model.get().items.is_empty()
            >
                <For
                    each=move || model.get().items
                    key=|item| (item.node.id.clone(), item.context_target.clone())
                    children=move |item| view! { <ProviderContextRow item=item/> }
                />
            </ol>
        </section>
    }
}

#[component]
fn ProviderContextRow(item: ProviderContextItem) -> impl IntoView {
    let visible = item.point.is_some();
    let class = provider_context_node_class(visible, item.selected);
    let node_target = format!("{NODE_TARGET_PREFIX}{}", item.node.id);
    let target = format!("#{node_target}?context={}", item.context_target);
    let graph_point = item
        .point
        .map(|point| {
            view! {
                <span
                    class="provider-context-node-graph-point"
                    data-node-target=node_target.clone()
                    data-node-x=point.x.to_string()
                    data-node-y=point.y.to_string()
                ></span>
            }
            .into_any()
        })
        .into_iter()
        .collect::<Vec<_>>();

    view! {
        <li class=class>
            <a class="provider-context-node-link" href=target>
                {graph_point}
                <div class="provider-context-node-head">
                    <span>{item.node.short_id}</span>
                    <span>{item.node.kind}</span>
                    <span>{item.node.role}</span>
                </div>
                <time>{item.node.created_at}</time>
                <p>{item.node.summary}</p>
            </a>
        </li>
    }
}

fn provider_context_node_class(visible: bool, selected: bool) -> &'static str {
    match (visible, selected) {
        (true, true) => "provider-context-node visible selected",
        (true, false) => "provider-context-node visible",
        (false, true) => "provider-context-node selected",
        (false, false) => "provider-context-node",
    }
}

#[cfg(any(target_arch = "wasm32", test))]
fn provider_context_target(query: &str) -> Option<String> {
    query.split('&').find_map(|part| {
        let (name, value) = part.split_once('=')?;
        (name == "context" && value.starts_with(NODE_TARGET_PREFIX)).then(|| value.to_owned())
    })
}

#[cfg(any(target_arch = "wasm32", test))]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selection_parses_node_and_provider_context_targets() {
        assert_eq!(
            PanelSelection::from_hash("#detail-node?context=detail-context&ignored=value"),
            PanelSelection {
                target: Some("detail-node".to_owned()),
                context: Some("detail-context".to_owned()),
            }
        );
    }

    #[test]
    fn selection_rejects_unrelated_hash_values() {
        assert_eq!(
            PanelSelection::from_hash("#section?context=invalid"),
            PanelSelection::default()
        );
        assert_eq!(PanelSelection::from_hash(""), PanelSelection::default());
    }

    #[test]
    fn panel_urls_encode_selection_independently() {
        assert_eq!(
            node_detail_url(Some("detail-node/value"), "all"),
            "/api/node-detail?target=detail-node%2Fvalue&mode=all"
        );
        assert_eq!(node_detail_url(None, "all"), "/api/node-detail?mode=all");
        assert_eq!(
            provider_context_url(Some("detail-node"), Some("detail-context/value"), "anchors"),
            "/api/provider-context?mode=anchors&target=detail-node&context=detail-context%2Fvalue"
        );
        assert_eq!(
            provider_context_url(None, None, "all"),
            "/api/provider-context?mode=all"
        );
    }

    #[test]
    fn panel_islands_render_independent_server_fallbacks() {
        let node = view! { <NodeDetailPanel graph_mode="all".to_owned()/> }.to_html();
        let provider = view! { <ProviderContextPanel graph_mode="all".to_owned()/> }.to_html();

        assert!(node.contains("leptos-island"));
        assert!(node.contains("panel-content"));
        assert!(node.contains("Select a node to inspect its content."));
        assert!(!node.contains("Provider Context"));
        assert_eq!(node.matches("<section").count(), 1);
        assert!(provider.contains("leptos-island"));
        assert!(provider.contains("panel-content"));
        assert!(provider.contains("Select a node to inspect its provider context."));
        assert!(!provider.contains("<h2>Node</h2>"));
        assert_eq!(provider.matches("<section").count(), 1);
    }

    #[test]
    fn panel_states_render_empty_loading_and_ready_content() {
        let empty = NodeDetailViewModel::from_state(PanelState::Empty);
        let loading = NodeDetailViewModel::from_state(PanelState::Loading);
        let ready =
            NodeDetailViewModel::from_state(PanelState::Ready(Ok(NodeDetailResponse::Found {
                node: PanelNode {
                    id: "node-1".to_owned(),
                    short_id: "node-1".to_owned(),
                    kind: "text".to_owned(),
                    role: "User".to_owned(),
                    created_at: "now".to_owned(),
                    content: "content".to_owned(),
                    summary: "summary".to_owned(),
                },
            })));
        let error = NodeDetailViewModel::from_state(PanelState::Ready(Err("failed".to_owned())));
        let provider_loading = ProviderContextViewModel::from_state(PanelState::Loading);
        let provider_ready = ProviderContextViewModel::from_state(PanelState::Ready(Ok(
            ProviderContextResponse::Found { items: Vec::new() },
        )));

        assert_eq!(
            empty.status_message,
            "Select a node to inspect its content."
        );
        assert_eq!(loading.status_message, "Loading node detail...");
        assert_eq!(
            ready.node.as_ref().map(|node| node.id.as_str()),
            Some("node-1")
        );
        assert_eq!(error.status_message, "Failed to load node detail.");
        assert_eq!(provider_loading.message, "Loading provider context...");
        assert_eq!(provider_ready.message, "No provider context nodes.");
    }

    #[test]
    fn panel_components_render_typed_success_and_error_states() {
        let owner = Owner::new();
        owner.set();
        let node_state = RwSignal::new(PanelState::Ready(Ok(NodeDetailResponse::Found {
            node: PanelNode {
                id: "node-1".to_owned(),
                short_id: "node-1".to_owned(),
                kind: "text".to_owned(),
                role: "User".to_owned(),
                created_at: "now".to_owned(),
                content: "<script>alert(1)</script>".to_owned(),
                summary: "summary".to_owned(),
            },
        })));
        let provider_state = RwSignal::new(PanelState::Ready(Ok(ProviderContextResponse::Found {
            items: Vec::new(),
        })));
        let node_error_state = RwSignal::new(PanelState::Ready(Err("node failed".to_owned())));
        let provider_error_state =
            RwSignal::new(PanelState::Ready(Err("provider failed".to_owned())));
        let node = view! {
            <NodeDetailPanelContent state=node_state/>
        }
        .to_html();
        let provider = view! {
            <ProviderContextPanelContent state=provider_state/>
        }
        .to_html();
        let node_error = view! { <NodeDetailPanelContent state=node_error_state/> }.to_html();
        let provider_error =
            view! { <ProviderContextPanelContent state=provider_error_state/> }.to_html();

        assert!(node.contains("&lt;script&gt;"));
        assert!(!node.contains("<script>"));
        assert!(provider.contains("No provider context nodes."));
        assert!(node_error.contains("Failed to load node detail."));
        assert!(node_error.contains("node failed"));
        assert!(provider_error.contains("Failed to load provider context."));
        assert!(provider_error.contains("provider failed"));

        owner.cleanup();
    }
}

#[cfg(all(test, target_arch = "wasm32"))]
mod wasm_tests {
    use super::*;

    use wasm_bindgen::UnwrapThrowExt;
    use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

    wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    fn graph_items_panel_selection_signals_track_hash_changes_independently() {
        let owner = Owner::new();
        owner.set();
        let window = web_sys::window().expect_throw("window should be available");
        window
            .location()
            .set_hash("detail-node")
            .expect_throw("initial hash should be set");
        let node_selection = use_panel_selection();
        let context_selection = use_panel_selection();

        window
            .location()
            .set_hash("detail-node?context=detail-context")
            .expect_throw("provider context hash should be set");
        window
            .dispatch_event(&web_sys::Event::new("hashchange").expect_throw("event should build"))
            .expect_throw("hashchange should dispatch");

        let expected = PanelSelection {
            target: Some("detail-node".to_owned()),
            context: Some("detail-context".to_owned()),
        };
        assert_eq!(node_selection.get_untracked(), expected);
        assert_eq!(context_selection.get_untracked(), expected);

        owner.cleanup();
        window
            .location()
            .set_hash("")
            .expect_throw("hash should be cleared");
    }

    #[wasm_bindgen_test]
    async fn graph_items_panel_fetch_deserializes_typed_data() {
        let response = fetch_panel_data::<NodeDetailResponse>(
            "data:application/json,%7B%22status%22%3A%22default%22%7D".to_owned(),
        )
        .await
        .expect("typed panel data should be fetched");

        assert_eq!(response, NodeDetailResponse::Default);
    }
}
