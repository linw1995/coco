use leptos::prelude::*;

#[cfg(target_arch = "wasm32")]
use leptos::{
    ev,
    leptos_dom::helpers::{location_hash, window_event_listener},
};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::{JsCast, JsValue};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::JsFuture;
#[cfg(target_arch = "wasm32")]
use web_sys::Response;

#[cfg(any(target_arch = "wasm32", test))]
const NODE_TARGET_PREFIX: &str = "detail-";

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
        fetch_panel_html(url)
    });

    view! {
        <Suspense fallback=render_default_node_detail>
            {move || Suspend::new(async move {
                match detail.await {
                    Ok(html) => render_panel_html(html),
                    Err(error) => render_node_detail_error(error),
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
        fetch_panel_html(url)
    });

    view! {
        <Suspense fallback=render_default_provider_context>
            {move || Suspend::new(async move {
                match provider_context.await {
                    Ok(html) => render_panel_html(html),
                    Err(error) => render_provider_context_error(error),
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

async fn fetch_panel_html(url: String) -> Result<String, String> {
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
        return JsFuture::from(response.text().map_err(js_error_message)?)
            .await
            .map_err(js_error_message)?
            .as_string()
            .ok_or_else(|| "response text is not a string".to_owned());
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

fn render_panel_html(html: String) -> AnyView {
    view! { <div class="panel-fragment" inner_html=html></div> }.into_any()
}

fn render_default_node_detail() -> AnyView {
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
    .into_any()
}

fn render_node_detail_error(error: String) -> AnyView {
    view! {
        <section class="node-details node-details-default">
            <h2>"Node"</h2>
            <dl class="detail-list">
                <div><dt>"Error"</dt><dd>"Failed to load node detail."</dd></div>
                <div><dt>"Reason"</dt><dd>{error}</dd></div>
            </dl>
        </section>
    }
    .into_any()
}

fn render_default_provider_context() -> AnyView {
    view! {
        <section class="provider-context-section provider-context-default">
            <h2>"Provider Context"</h2>
            <p class="provider-context-empty">"Select a node to inspect its provider context."</p>
        </section>
    }
    .into_any()
}

fn render_provider_context_error(error: String) -> AnyView {
    view! {
        <section class="provider-context-section provider-context-default">
            <h2>"Provider Context"</h2>
            <p class="provider-context-empty">"Failed to load provider context."</p>
            <p class="provider-context-target">{error}</p>
        </section>
    }
    .into_any()
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
        assert_eq!(
            provider_context_url(Some("detail-node"), Some("detail-context/value"), "anchors"),
            "/api/provider-context?mode=anchors&target=detail-node&context=detail-context%2Fvalue"
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
}
