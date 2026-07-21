use leptos::prelude::*;
use serde::de::DeserializeOwned;

use crate::api::{NodeDetailResponse, PanelNode, ProviderContextItem, ProviderContextResponse};

#[cfg(target_arch = "wasm32")]
use leptos::{
    ev,
    leptos_dom::helpers::{location_hash, request_animation_frame, window_event_listener},
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

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PanelSelection {
    pub target: Option<String>,
    pub context: Option<String>,
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
    let selection = use_panel_selection();
    let detail = LocalResource::new(move || {
        let target = selection.get().target;
        let url = node_detail_url(target.as_deref(), &graph_mode);
        fetch_panel_data::<NodeDetailResponse>(url)
    });

    view! {
        <Suspense fallback=|| view! { <NodeDetailDefault/> }>
            {move || Suspend::new(async move {
                match detail.await {
                    Ok(response) => view! {
                        <div class="panel-content"><NodeDetailContent response=response/></div>
                    }.into_any(),
                    Err(error) => view! {
                        <div class="panel-content"><NodeDetailError error=error/></div>
                    }.into_any(),
                }
            })}
        </Suspense>
    }
}

#[island]
pub fn ProviderContextPanel(graph_mode: String) -> impl IntoView {
    let selection = use_panel_selection();
    let provider_context = LocalResource::new(move || {
        let selection = selection.get();
        let url = provider_context_url(
            selection.target.as_deref(),
            selection.context.as_deref(),
            &graph_mode,
        );
        fetch_panel_data::<ProviderContextResponse>(url)
    });

    view! {
        <Suspense fallback=|| view! { <ProviderContextDefault/> }>
            {move || Suspend::new(async move {
                match provider_context.await {
                    Ok(response) => {
                        notify_provider_context_rendered();
                        view! {
                            <div class="panel-content">
                                <ProviderContextContent response=response/>
                            </div>
                        }.into_any()
                    }
                    Err(error) => view! {
                        <div class="panel-content"><ProviderContextError error=error/></div>
                    }.into_any(),
                }
            })}
        </Suspense>
    }
}

fn use_panel_selection() -> RwSignal<PanelSelection> {
    let selection = RwSignal::new(current_panel_selection());
    #[cfg(target_arch = "wasm32")]
    {
        let listener = window_event_listener(ev::hashchange, move |_| {
            selection.set(current_panel_selection());
        });
        on_cleanup(move || listener.remove());
    }
    selection
}

fn current_panel_selection() -> PanelSelection {
    #[cfg(target_arch = "wasm32")]
    {
        PanelSelection::from_hash(location_hash().as_deref().unwrap_or_default())
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        PanelSelection::default()
    }
}

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

async fn fetch_panel_data<T>(url: String) -> Result<T, String>
where
    T: DeserializeOwned,
{
    #[cfg(target_arch = "wasm32")]
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
    #[cfg(not(target_arch = "wasm32"))]
    {
        let _ = url;
        std::future::pending().await
    }
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

#[cfg(not(target_arch = "wasm32"))]
fn notify_provider_context_rendered() {}

#[component]
fn NodeDetailContent(response: NodeDetailResponse) -> AnyView {
    match response {
        NodeDetailResponse::Default => view! { <NodeDetailDefault/> }.into_any(),
        NodeDetailResponse::Missing { target } => {
            view! { <NodeDetailMissing target=target/> }.into_any()
        }
        NodeDetailResponse::Found { node } => view! { <NodeDetail node=node/> }.into_any(),
    }
}

#[component]
fn NodeDetail(node: PanelNode) -> impl IntoView {
    let target = format!("{NODE_TARGET_PREFIX}{}", node.id);
    view! {
        <section id=target class="node-details node-detail">
            <h2>"Node"</h2>
            <dl class="detail-list">
                <div><dt>"Id"</dt><dd>{node.id}</dd></div>
                <div><dt>"Kind"</dt><dd>{node.kind}</dd></div>
                <div><dt>"Role"</dt><dd>{node.role}</dd></div>
                <div><dt>"Created"</dt><dd>{node.created_at}</dd></div>
                <div><dt>"Labels"</dt><dd>"None"</dd></div>
                <div><dt>"Content"</dt><dd>{node.content}</dd></div>
            </dl>
        </section>
    }
}

#[component]
fn NodeDetailDefault() -> impl IntoView {
    view! {
        <section class="node-details node-details-default">
            <h2>"Node"</h2>
            <dl class="detail-list">
                <div>
                    <dt>"Selection"</dt>
                    <dd>"Select a node to inspect its content."</dd>
                </div>
            </dl>
        </section>
    }
}

#[component]
fn NodeDetailMissing(target: String) -> impl IntoView {
    view! {
        <section class="node-details node-details-default">
            <h2>"Node"</h2>
            <dl class="detail-list">
                <div>
                    <dt>"Selection"</dt>
                    <dd>"The selected node is no longer available."</dd>
                </div>
                <div><dt>"Target"</dt><dd>{target}</dd></div>
            </dl>
        </section>
    }
}

#[component]
fn NodeDetailError(error: String) -> impl IntoView {
    view! {
        <section class="node-details node-details-default">
            <h2>"Node"</h2>
            <dl class="detail-list">
                <div><dt>"Error"</dt><dd>"Failed to load node detail."</dd></div>
                <div><dt>"Reason"</dt><dd>{error}</dd></div>
            </dl>
        </section>
    }
}

#[component]
fn ProviderContextContent(response: ProviderContextResponse) -> AnyView {
    match response {
        ProviderContextResponse::Default => view! { <ProviderContextDefault/> }.into_any(),
        ProviderContextResponse::Missing { target } => {
            view! { <ProviderContextMissing target=target/> }.into_any()
        }
        ProviderContextResponse::Found { items } => {
            view! { <ProviderContextList items=items/> }.into_any()
        }
    }
}

#[component]
fn ProviderContextDefault() -> impl IntoView {
    view! {
        <section class="provider-context-section provider-context-default">
            <h2>"Provider Context"</h2>
            <p class="provider-context-empty">"Select a node to inspect its provider context."</p>
        </section>
    }
}

#[component]
fn ProviderContextList(items: Vec<ProviderContextItem>) -> AnyView {
    if items.is_empty() {
        view! {
            <section class="provider-context-section">
                <h2>"Provider Context"</h2>
                <p class="provider-context-empty">"No provider context nodes."</p>
            </section>
        }
        .into_any()
    } else {
        view! {
            <section class="provider-context-section">
                <h2>"Provider Context"</h2>
                <ol class="provider-context-list">
                    {items.into_iter().map(|item| view! { <ProviderContextRow item=item/> }).collect::<Vec<_>>()}
                </ol>
            </section>
        }
        .into_any()
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

#[component]
fn ProviderContextMissing(target: String) -> impl IntoView {
    view! {
        <section class="provider-context-section provider-context-default">
            <h2>"Provider Context"</h2>
            <p class="provider-context-empty">"The selected node is no longer available."</p>
            <p class="provider-context-target">{target}</p>
        </section>
    }
}

#[component]
fn ProviderContextError(error: String) -> impl IntoView {
    view! {
        <section class="provider-context-section provider-context-default">
            <h2>"Provider Context"</h2>
            <p class="provider-context-empty">"Failed to load provider context."</p>
            <p class="provider-context-target">{error}</p>
        </section>
    }
}

#[cfg(any(target_arch = "wasm32", test))]
fn provider_context_target(query: &str) -> Option<String> {
    query.split('&').find_map(|part| {
        let (name, value) = part.split_once('=')?;
        (name == "context" && value.starts_with(NODE_TARGET_PREFIX)).then(|| value.to_owned())
    })
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
        assert!(node.contains("Select a node to inspect its content."));
        assert!(!node.contains("Provider Context"));
        assert!(provider.contains("leptos-island"));
        assert!(provider.contains("Select a node to inspect its provider context."));
        assert!(!provider.contains("<h2>Node</h2>"));
    }

    #[test]
    fn panel_components_render_typed_success_and_error_states() {
        let node = view! {
            <NodeDetailContent response=NodeDetailResponse::Found {
                node: PanelNode {
                    id: "node-1".to_owned(),
                    short_id: "node-1".to_owned(),
                    kind: "text".to_owned(),
                    role: "User".to_owned(),
                    created_at: "now".to_owned(),
                    content: "<script>alert(1)</script>".to_owned(),
                    summary: "summary".to_owned(),
                },
            }/>
        }
        .to_html();
        let provider = view! {
            <ProviderContextContent response=ProviderContextResponse::Found {
                items: Vec::new(),
            }/>
        }
        .to_html();
        let node_error = view! { <NodeDetailError error="node failed".to_owned()/> }.to_html();
        let provider_error =
            view! { <ProviderContextError error="provider failed".to_owned()/> }.to_html();

        assert!(node.contains("&lt;script&gt;"));
        assert!(!node.contains("<script>"));
        assert!(provider.contains("No provider context nodes."));
        assert!(node_error.contains("Failed to load node detail."));
        assert!(node_error.contains("node failed"));
        assert!(provider_error.contains("Failed to load provider context."));
        assert!(provider_error.contains("provider failed"));
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
