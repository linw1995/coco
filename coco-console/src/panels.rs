use leptos::prelude::*;
use leptos::server_fn::codec::GetUrl;

use crate::api::{NodeDetailResponse, PanelNode, ProviderContextItem, ProviderContextResponse};

#[cfg(target_arch = "wasm32")]
use leptos::{
    ev,
    leptos_dom::helpers::{location_hash, request_animation_frame, window_event_listener},
};
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct LoadedPanel<K, T> {
    request: K,
    response: Result<T, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ProviderContextRequest {
    target: String,
    context: Option<String>,
}

#[server(prefix = "/api/panels", endpoint = "node-detail", input = GetUrl)]
async fn load_node_detail(target: String) -> Result<NodeDetailResponse, ServerFnError> {
    let context = expect_context::<crate::host::PanelServerContext>();
    context
        .node_detail(target)
        .await
        .map_err(|error| ServerFnError::ServerError(error.to_string()))
}

#[server(
    prefix = "/api/panels",
    endpoint = "provider-context",
    input = GetUrl
)]
async fn load_provider_context(
    target: String,
    context: Option<String>,
    graph_mode: String,
) -> Result<ProviderContextResponse, ServerFnError> {
    let server = expect_context::<crate::host::PanelServerContext>();
    server
        .provider_context(target, context, graph_mode)
        .await
        .map_err(|error| ServerFnError::ServerError(error.to_string()))
}

#[island]
pub fn NodeDetailPanel() -> impl IntoView {
    view! { <NodeDetailPanelBody/> }
}

#[component]
fn NodeDetailPanelBody() -> impl IntoView {
    let selection = use_panel_selection();
    let selected_target = Memo::new(move |_| selection.get().target);
    let detail = LocalResource::new(move || {
        let request = selected_target.get();
        async move {
            let target = request?;
            let response = load_node_detail(target.clone())
                .await
                .map_err(|error| error.to_string());
            Some(LoadedPanel {
                request: target,
                response,
            })
        }
    });

    view! {
        <div class="panel-content">
            {move || node_detail_view(selected_target.get(), detail.get().flatten())}
        </div>
    }
}

#[island]
pub fn ProviderContextPanel(graph_mode: String) -> impl IntoView {
    view! { <ProviderContextPanelBody graph_mode/> }
}

#[component]
fn ProviderContextPanelBody(graph_mode: String) -> impl IntoView {
    let selection = use_panel_selection();
    let selected_context = Memo::new(move |_| {
        let selection = selection.get();
        selection.target.map(|target| ProviderContextRequest {
            target,
            context: selection.context,
        })
    });
    let provider_context = LocalResource::new(move || {
        let request = selected_context.get();
        let graph_mode = graph_mode.clone();
        async move {
            let request = request?;
            let response =
                load_provider_context(request.target.clone(), request.context.clone(), graph_mode)
                    .await
                    .map_err(|error| error.to_string());
            Some(LoadedPanel { request, response })
        }
    });
    Effect::new(move || {
        let current = selected_context.get();
        let loaded = provider_context.get().flatten();
        if loaded.is_some_and(|loaded| {
            Some(&loaded.request) == current.as_ref() && loaded.response.is_ok()
        }) {
            notify_provider_context_rendered();
        }
    });

    view! {
        <div class="panel-content">
            {move || {
                provider_context_view(
                    selected_context.get(),
                    provider_context.get().flatten(),
                )
            }}
        </div>
    }
}

fn use_panel_selection() -> RwSignal<PanelSelection> {
    let selection = RwSignal::new(PanelSelection::default());
    Effect::new(move || subscribe_to_panel_selection(selection));
    selection
}

#[cfg(target_arch = "wasm32")]
fn subscribe_to_panel_selection(selection: RwSignal<PanelSelection>) {
    selection.set(current_panel_selection());
    let listener = window_event_listener(ev::hashchange, move |_| {
        selection.set(current_panel_selection());
    });
    on_cleanup(move || listener.remove());
}

#[cfg(not(target_arch = "wasm32"))]
fn subscribe_to_panel_selection(_selection: RwSignal<PanelSelection>) {}

#[cfg(target_arch = "wasm32")]
fn current_panel_selection() -> PanelSelection {
    PanelSelection::from_hash(location_hash().as_deref().unwrap_or_default())
}

fn node_detail_view(
    current: Option<String>,
    loaded: Option<LoadedPanel<String, NodeDetailResponse>>,
) -> AnyView {
    match (current.as_ref(), loaded) {
        (None, _) => view! { <NodeDetailDefault/> }.into_any(),
        (Some(current), Some(loaded)) if &loaded.request == current => match loaded.response {
            Ok(response) => view! { <NodeDetailContent response=response/> }.into_any(),
            Err(error) => view! { <NodeDetailError error=error/> }.into_any(),
        },
        _ => view! { <NodeDetailLoading/> }.into_any(),
    }
}

fn provider_context_view(
    current: Option<ProviderContextRequest>,
    loaded: Option<LoadedPanel<ProviderContextRequest, ProviderContextResponse>>,
) -> AnyView {
    match (current.as_ref(), loaded) {
        (None, _) => view! { <ProviderContextDefault/> }.into_any(),
        (Some(current), Some(loaded)) if &loaded.request == current => match loaded.response {
            Ok(response) => view! { <ProviderContextContent response=response/> }.into_any(),
            Err(error) => view! { <ProviderContextError error=error/> }.into_any(),
        },
        _ => view! { <ProviderContextLoading/> }.into_any(),
    }
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
fn NodeDetailLoading() -> impl IntoView {
    view! {
        <section class="node-details node-details-loading">
            <h2>"Node"</h2>
            <dl class="detail-list">
                <div><dt>"Selection"</dt><dd>"Loading node detail..."</dd></div>
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
fn ProviderContextLoading() -> impl IntoView {
    view! {
        <section class="provider-context-section provider-context-loading">
            <h2>"Provider Context"</h2>
            <p class="provider-context-empty">"Loading provider context..."</p>
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
    fn panel_islands_render_independent_server_fallbacks() {
        let node = view! { <NodeDetailPanel/> }.to_html();
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
    fn panel_results_ignore_responses_for_previous_selections() {
        let node = node_detail_view(
            Some("detail-new".to_owned()),
            Some(LoadedPanel {
                request: "detail-old".to_owned(),
                response: Ok(NodeDetailResponse::Default),
            }),
        )
        .to_html();
        let provider = provider_context_view(
            Some(ProviderContextRequest {
                target: "detail-new".to_owned(),
                context: None,
            }),
            Some(LoadedPanel {
                request: ProviderContextRequest {
                    target: "detail-old".to_owned(),
                    context: None,
                },
                response: Ok(ProviderContextResponse::Default),
            }),
        )
        .to_html();

        assert!(node.contains("Loading node detail..."));
        assert!(provider.contains("Loading provider context..."));
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

    use any_spawner::Executor;
    use js_sys::Promise;
    use wasm_bindgen::{JsValue, UnwrapThrowExt};
    use wasm_bindgen_futures::JsFuture;
    use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

    wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    async fn graph_items_panel_selection_signals_track_hash_changes_independently() {
        _ = Executor::init_wasm_bindgen();
        let owner = Owner::new();
        owner.set();
        let window = web_sys::window().expect_throw("window should be available");
        window
            .location()
            .set_hash("detail-node")
            .expect_throw("initial hash should be set");
        let node_selection = use_panel_selection();
        let context_selection = use_panel_selection();
        next_task().await;

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

    async fn next_task() {
        JsFuture::from(Promise::resolve(&JsValue::NULL))
            .await
            .expect_throw("task should resolve");
    }
}
