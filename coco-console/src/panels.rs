use coco_types::{
    AnchorPayload, Kind, Node, PromptAttachment, SessionAnchor, SessionAnchorPatch,
    SkillInvocationAnchor, SkillInvocationMode, Tool, ToolResult, ToolUse,
};
use leptos::prelude::*;
use leptos::server_fn::codec::GetUrl;

use crate::api::{NodeDetailResponse, ProviderContextItem, ProviderContextResponse};

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
    request_animation_frame(move || selection.set(current_panel_selection()));
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
        NodeDetailResponse::Found { node } => view! { <NodeDetail node=*node/> }.into_any(),
    }
}

#[component]
fn NodeDetail(node: Node) -> impl IntoView {
    let target = format!("{NODE_TARGET_PREFIX}{}", node.id);
    let kind = node_kind_name(&node.kind);
    let kind_class = format!("node-details node-detail kind-{kind}");
    let title = humanize_kind(kind);
    let role = format!("{:?}", node.role);
    let short_id = shorten_id(&node.id);
    let full_id = node.id;
    let parent = (!node.parent.is_empty()).then_some(node.parent);
    let merge_parents = match &node.kind {
        Kind::Anchor(anchor) => anchor
            .merge_parents()
            .iter()
            .map(|parent| {
                (
                    if parent.is_shadow() {
                        "Shadow"
                    } else {
                        "Merge"
                    },
                    parent.node_id().to_owned(),
                )
            })
            .collect::<Vec<_>>(),
        Kind::ToolUse(_) | Kind::ToolResult(_) | Kind::Text(_) | Kind::Failure(_) => Vec::new(),
    };
    view! {
        <section id=target class=kind_class>
            <header class="node-detail-header">
                <div>
                    <p class="node-detail-eyebrow">"Node detail"</p>
                    <h2>{title}</h2>
                </div>
                <span class="node-detail-role">{role}</span>
            </header>
            <dl class="node-detail-meta">
                <div>
                    <dt>"Id"</dt>
                    <dd><code title=full_id>{short_id}</code></dd>
                </div>
                <div><dt>"Created"</dt><dd>{node.created_at.to_string()}</dd></div>
                {parent.map(|parent| view! {
                    <div class="node-detail-meta-wide">
                        <dt>"Parent"</dt>
                        <dd><code>{parent}</code></dd>
                    </div>
                })}
                {merge_parents.into_iter().map(|(kind, node_id)| view! {
                    <div class="node-detail-meta-wide">
                        <dt>{format!("{kind} parent")}</dt>
                        <dd><code>{node_id}</code></dd>
                    </div>
                }).collect::<Vec<_>>()}
            </dl>
            <NodeDetailBody kind=node.kind/>
        </section>
    }
}

#[component]
fn NodeDetailBody(kind: Kind) -> AnyView {
    match kind {
        Kind::Anchor(anchor) => {
            view! { <AnchorDetailBody payload=anchor.payload/> }.into_any()
        }
        Kind::ToolUse(items) => view! {
            <div class="node-detail-body node-detail-items">
                {items.into_iter().map(|item| view! { <ToolUseDetail item=item/> }).collect::<Vec<_>>()}
            </div>
        }
        .into_any(),
        Kind::ToolResult(items) => view! {
            <div class="node-detail-body node-detail-items">
                {items.into_iter().map(|item| view! { <ToolResultDetail item=item/> }).collect::<Vec<_>>()}
            </div>
        }
        .into_any(),
        Kind::Text(text) => view! {
            <div class="node-detail-body">
                <DetailTextBlock label="Text" content=text/>
            </div>
        }
        .into_any(),
        Kind::Failure(message) => view! {
            <div class="node-detail-body">
                <section class="node-detail-section node-detail-failure">
                    <h3>"Failure"</h3>
                    <pre>{message}</pre>
                </section>
            </div>
        }
        .into_any(),
    }
}

#[component]
fn AnchorDetailBody(payload: AnchorPayload) -> AnyView {
    match payload {
        AnchorPayload::Session(session) => {
            view! { <SessionAnchorDetail session=*session/> }.into_any()
        }
        AnchorPayload::SessionPatch(patch) => {
            view! { <SessionPatchDetail patch=patch/> }.into_any()
        }
        AnchorPayload::Prompt(prompt) => view! {
            <div class="node-detail-body">
                <DetailTextBlock label="Prompt" content=prompt.prompt/>
                <PromptAttachments attachments=prompt.attachments/>
            </div>
        }
        .into_any(),
        AnchorPayload::SkillInvocation(invocation) => {
            view! { <SkillInvocationDetail invocation=invocation/> }.into_any()
        }
        AnchorPayload::SkillResult(result) => view! {
            <div class="node-detail-body">
                <dl class="node-detail-properties">
                    <div><dt>"Skill"</dt><dd>{result.skill_name}</dd></div>
                </dl>
                <DetailTextBlock label="Output" content=result.output/>
            </div>
        }
        .into_any(),
    }
}

#[component]
fn SessionAnchorDetail(session: SessionAnchor) -> impl IntoView {
    let SessionAnchor {
        role,
        provider_profile,
        provider,
        model,
        tools,
        system_prompt,
        prompt,
        temperature,
        max_tokens,
        additional_params,
        enable_coco_shim,
        active_skill,
    } = session;
    let session_role = role.as_str();
    let tools = tools.into_iter().map(|tool| tool.name).collect();
    let provider = provider.unwrap_or_else(|| "Profile default".to_owned());
    let provider_profile = provider_profile.unwrap_or_else(|| "Default".to_owned());
    let temperature = temperature.map_or_else(|| "Default".to_owned(), |value| value.to_string());
    let max_tokens = max_tokens.map_or_else(|| "Default".to_owned(), |value| value.to_string());
    let additional_params = additional_params.map(|value| pretty_json(&value));
    view! {
        <div class="node-detail-body session-detail">
            <dl class="node-detail-properties">
                <div><dt>"Session role"</dt><dd>{humanize_kind(session_role)}</dd></div>
                <div><dt>"Model"</dt><dd>{model}</dd></div>
                <div><dt>"Provider"</dt><dd>{provider}</dd></div>
                <div><dt>"Profile"</dt><dd>{provider_profile}</dd></div>
                <div><dt>"Temperature"</dt><dd>{temperature}</dd></div>
                <div><dt>"Max tokens"</dt><dd>{max_tokens}</dd></div>
                <div>
                    <dt>"CoCo shim"</dt>
                    <dd>{if enable_coco_shim { "Enabled" } else { "Disabled" }}</dd>
                </div>
            </dl>
            <DetailTags label="Tools" values=tools empty="None configured"/>
            {active_skill.map(|skill| view! {
                <section class="node-detail-callout">
                    <span>"Active skill"</span>
                    <strong>{skill.name}</strong>
                    {skill.handoff.map(|handoff| view! { <p>{handoff}</p> })}
                </section>
            })}
            <DetailTextBlock label="Prompt" content=prompt/>
            <DetailTextBlock label="System prompt" content=system_prompt/>
            {additional_params.map(|params| view! {
                <DetailCodeBlock label="Additional params" content=params/>
            })}
        </div>
    }
}

#[component]
fn SessionPatchDetail(patch: SessionAnchorPatch) -> impl IntoView {
    let fields = session_patch_fields(patch);
    let content = if fields.is_empty() {
        view! { <p class="node-detail-empty">"No configuration changes."</p> }.into_any()
    } else {
        view! {
            <dl class="node-detail-properties patch-properties">
                {fields.into_iter().map(|(name, value)| view! {
                    <div><dt>{name}</dt><dd>{value}</dd></div>
                }).collect::<Vec<_>>()}
            </dl>
        }
        .into_any()
    };
    view! {
        <div class="node-detail-body">
            <section class="node-detail-section">
                <h3>"Configuration changes"</h3>
                {content}
            </section>
        </div>
    }
}

#[component]
fn SkillInvocationDetail(invocation: SkillInvocationAnchor) -> impl IntoView {
    let (mode, prompt) = match invocation.mode {
        SkillInvocationMode::InheritContext => ("Inherit context", None),
        SkillInvocationMode::Handoff { prompt } => ("Handoff", Some(prompt)),
    };
    view! {
        <div class="node-detail-body">
            <dl class="node-detail-properties">
                <div><dt>"Skill"</dt><dd>{invocation.skill_name}</dd></div>
                <div><dt>"Mode"</dt><dd>{mode}</dd></div>
            </dl>
            {prompt.map(|prompt| view! {
                <DetailTextBlock label="Handoff prompt" content=prompt/>
            })}
        </div>
    }
}

fn session_patch_fields(patch: SessionAnchorPatch) -> Vec<(String, String)> {
    [
        patch.role.map(|value| patch_field("Role", value.as_str())),
        patch.provider_profile.map(|value| {
            patch_field(
                "Provider profile",
                value.unwrap_or_else(|| "None".to_owned()),
            )
        }),
        patch
            .provider
            .map(|value| patch_field("Provider", value.unwrap_or_else(|| "None".to_owned()))),
        patch.model.map(|value| patch_field("Model", value)),
        patch
            .tools
            .map(|value| patch_field("Tools", format_patch_tools(value))),
        patch
            .system_prompt
            .map(|value| patch_field("System prompt", value)),
        patch.temperature.map(|value| {
            patch_field(
                "Temperature",
                value.map_or_else(|| "None".to_owned(), |value| value.to_string()),
            )
        }),
        patch.max_tokens.map(|value| {
            patch_field(
                "Max tokens",
                value.map_or_else(|| "None".to_owned(), |value| value.to_string()),
            )
        }),
        patch.additional_params.map(|value| {
            patch_field(
                "Additional params",
                value.map_or_else(|| "None".to_owned(), |value| pretty_json(&value)),
            )
        }),
        patch
            .enable_coco_shim
            .map(|value| patch_field("CoCo shim", format_coco_shim(value))),
    ]
    .into_iter()
    .flatten()
    .collect()
}

fn patch_field(name: &str, value: impl Into<String>) -> (String, String) {
    (name.to_owned(), value.into())
}

fn format_patch_tools(tools: Vec<Tool>) -> String {
    if tools.is_empty() {
        "None".to_owned()
    } else {
        tools
            .into_iter()
            .map(|tool| tool.name)
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn format_coco_shim(enabled: bool) -> &'static str {
    if enabled { "Enabled" } else { "Disabled" }
}

fn node_kind_name(kind: &Kind) -> &'static str {
    kind.anchor_payload_kind()
        .map(|kind| kind.as_str())
        .unwrap_or_else(|| kind.tag().as_str())
}

fn shorten_id(id: &str) -> String {
    id.chars().take(8).collect()
}

fn pretty_json(value: &serde_json::Value) -> String {
    serde_json::to_string_pretty(value).expect("JSON value should serialize")
}

fn format_file_size(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = KIB * 1024;
    if bytes >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{bytes} B")
    }
}

#[component]
fn DetailTags(label: &'static str, values: Vec<String>, empty: &'static str) -> impl IntoView {
    view! {
        <section class="node-detail-section">
            <h3>{label}</h3>
            {if values.is_empty() {
                view! { <p class="node-detail-empty">{empty}</p> }.into_any()
            } else {
                view! {
                    <ul class="node-detail-tags">
                        {values.into_iter().map(|value| view! { <li>{value}</li> }).collect::<Vec<_>>()}
                    </ul>
                }
                .into_any()
            }}
        </section>
    }
}

#[component]
fn DetailTextBlock(label: &'static str, content: String) -> impl IntoView {
    view! {
        <section class="node-detail-section">
            <h3>{label}</h3>
            <pre>{if content.is_empty() { "Empty".to_owned() } else { content }}</pre>
        </section>
    }
}

#[component]
fn DetailCodeBlock(label: &'static str, content: String) -> impl IntoView {
    view! {
        <section class="node-detail-section">
            <h3>{label}</h3>
            <pre class="node-detail-code">{content}</pre>
        </section>
    }
}

#[component]
fn ToolUseDetail(item: ToolUse) -> impl IntoView {
    let input = pretty_json(&item.input);
    view! {
        <article class="node-detail-item">
            <header>
                <strong>{item.name}</strong>
                <code>{item.id}</code>
            </header>
            <DetailCodeBlock label="Input" content=input/>
        </article>
    }
}

#[component]
fn ToolResultDetail(item: ToolResult) -> impl IntoView {
    view! {
        <article class="node-detail-item">
            <header>
                <strong>"Tool result"</strong>
                <code>{item.id}</code>
            </header>
            <DetailTextBlock label="Output" content=item.output/>
        </article>
    }
}

#[component]
fn PromptAttachments(attachments: Vec<PromptAttachment>) -> impl IntoView {
    if attachments.is_empty() {
        None
    } else {
        Some(view! {
            <section class="node-detail-section">
                <h3>"Attachments"</h3>
                <ul class="node-detail-attachments">
                    {attachments.into_iter().map(|attachment| match attachment {
                        PromptAttachment::Image(image) => {
                            let dimensions = match (image.width, image.height) {
                                (Some(width), Some(height)) => Some(format!("{width} × {height}")),
                                _ => None,
                            };
                            let file_size = image.file_size.map(format_file_size);
                            view! {
                                <li>
                                    <strong>"Image"</strong>
                                    <code>{image.id}</code>
                                    <span>
                                        {[
                                            image.media_type,
                                            dimensions,
                                            file_size,
                                        ].into_iter().flatten().collect::<Vec<_>>().join(" · ")}
                                    </span>
                                </li>
                            }
                        }
                    }).collect::<Vec<_>>()}
                </ul>
            </section>
        })
    }
}

fn humanize_kind(kind: &str) -> String {
    kind.split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            chars
                .next()
                .map(|first| first.to_uppercase().chain(chars).collect())
                .unwrap_or_default()
        })
        .collect::<Vec<String>>()
        .join(" ")
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
    use coco_types::{Anchor, PromptAnchor, SessionRole, SkillResultAnchor};

    fn test_node(kind: Kind) -> Node {
        serde_json::from_value(serde_json::json!({
            "id": "node-1",
            "parent": "parent-node",
            "created_at": "2026-07-25T00:00:00Z",
            "role": "User",
            "metadata": null,
            "kind": kind,
        }))
        .expect("test node should deserialize")
    }

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
                node: Box::new(test_node(Kind::Text("<script>alert(1)</script>".to_owned()))),
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

    #[test]
    fn node_detail_bodies_render_kind_specific_fields() {
        let session = view! {
            <NodeDetailBody kind=Kind::Anchor(Anchor::session(
                Vec::new(),
                SessionAnchor {
                    role: SessionRole::Runner,
                    provider_profile: Some("default".to_owned()),
                    provider: Some("openai".to_owned()),
                    model: "gpt-test".to_owned(),
                    tools: vec![Tool {
                        name: "exec_command".to_owned(),
                        description: "Run a command".to_owned(),
                        input_schema: serde_json::json!({"type": "object"}),
                    }],
                    system_prompt: "System instructions".to_owned(),
                    prompt: "User prompt".to_owned(),
                    temperature: Some(0.7),
                    max_tokens: Some(1024),
                    additional_params: Some(serde_json::json!({"seed": 7})),
                    enable_coco_shim: true,
                    active_skill: None,
                },
            ))/>
        }
        .to_html();
        let tool_use = view! {
            <NodeDetailBody kind=Kind::tool_uses(vec![
                ToolUse {
                    id: "toolu-1".to_owned(),
                    name: "exec_command".to_owned(),
                    input: serde_json::json!({"cmd": "true"}),
                },
            ])/>
        }
        .to_html();
        let session_patch = view! {
            <NodeDetailBody kind=Kind::Anchor(Anchor::session_patch(
                Vec::new(),
                SessionAnchorPatch {
                    model: Some("gpt-patched".to_owned()),
                    ..SessionAnchorPatch::default()
                },
            ))/>
        }
        .to_html();
        let prompt = view! {
            <NodeDetailBody kind=Kind::Anchor(Anchor::prompt(
                Vec::new(),
                PromptAnchor {
                    prompt: "Review this".to_owned(),
                    attachments: Vec::new(),
                },
            ))/>
        }
        .to_html();
        let skill_invocation = view! {
            <NodeDetailBody kind=Kind::Anchor(Anchor::skill_invocation(
                Vec::new(),
                SkillInvocationAnchor {
                    skill_name: "review".to_owned(),
                    mode: SkillInvocationMode::Handoff {
                        prompt: "Inspect the change".to_owned(),
                    },
                },
            ))/>
        }
        .to_html();
        let skill_result = view! {
            <NodeDetailBody kind=Kind::Anchor(Anchor::skill_result(
                Vec::new(),
                SkillResultAnchor {
                    skill_name: "review".to_owned(),
                    output: "Looks good".to_owned(),
                },
            ))/>
        }
        .to_html();
        let tool_result = view! {
            <NodeDetailBody kind=Kind::tool_results(vec![
                ToolResult {
                    id: "toolu-1".to_owned(),
                    output: "done".to_owned(),
                },
            ])/>
        }
        .to_html();
        let text = view! {
            <NodeDetailBody kind=Kind::Text("Plain response".to_owned())/>
        }
        .to_html();
        let failure = view! {
            <NodeDetailBody kind=Kind::Failure("backend unavailable".to_owned())/>
        }
        .to_html();

        for expected in [
            "Session role",
            "gpt-test",
            "exec_command",
            "System instructions",
            "Additional params",
        ] {
            assert!(session.contains(expected));
        }
        for expected in ["exec_command", "toolu-1", "cmd", "true"] {
            assert!(tool_use.contains(expected));
        }
        assert!(session_patch.contains("gpt-patched"));
        assert!(prompt.contains("Review this"));
        assert!(skill_invocation.contains("Inspect the change"));
        assert!(skill_result.contains("Looks good"));
        assert!(tool_result.contains("done"));
        assert!(text.contains("Plain response"));
        assert!(failure.contains("node-detail-failure"));
        assert!(failure.contains("backend unavailable"));
    }

    #[test]
    fn session_patch_fields_format_every_change() {
        let fields = session_patch_fields(SessionAnchorPatch {
            role: Some(SessionRole::Runner),
            provider_profile: Some(None),
            provider: Some(Some("openai".to_owned())),
            model: Some("gpt-test".to_owned()),
            tools: Some(vec![Tool {
                name: "exec_command".to_owned(),
                description: "Run a command".to_owned(),
                input_schema: serde_json::json!({"type": "object"}),
            }]),
            system_prompt: Some("Updated instructions".to_owned()),
            temperature: Some(None),
            max_tokens: Some(Some(2048)),
            additional_params: Some(Some(serde_json::json!({"seed": 7}))),
            enable_coco_shim: Some(false),
        });

        assert_eq!(
            fields,
            vec![
                ("Role".to_owned(), "runner".to_owned()),
                ("Provider profile".to_owned(), "None".to_owned()),
                ("Provider".to_owned(), "openai".to_owned()),
                ("Model".to_owned(), "gpt-test".to_owned()),
                ("Tools".to_owned(), "exec_command".to_owned()),
                (
                    "System prompt".to_owned(),
                    "Updated instructions".to_owned()
                ),
                ("Temperature".to_owned(), "None".to_owned()),
                ("Max tokens".to_owned(), "2048".to_owned()),
                (
                    "Additional params".to_owned(),
                    "{\n  \"seed\": 7\n}".to_owned()
                ),
                ("CoCo shim".to_owned(), "Disabled".to_owned()),
            ]
        );
        assert!(session_patch_fields(SessionAnchorPatch::default()).is_empty());
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
    async fn graph_items_panel_selection_signals_read_initial_hash_and_track_changes_independently()
    {
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
        let loaded_selection = LocalResource::new(move || {
            let selection = node_selection.get();
            async move { selection }
        });
        assert_eq!(node_selection.get_untracked(), PanelSelection::default());
        assert_eq!(context_selection.get_untracked(), PanelSelection::default());
        next_animation_frame().await;
        next_animation_frame().await;
        next_task().await;
        let expected_initial = PanelSelection {
            target: Some("detail-node".to_owned()),
            context: None,
        };
        assert_eq!(node_selection.get_untracked(), expected_initial);
        assert_eq!(context_selection.get_untracked(), expected_initial);
        assert_eq!(loaded_selection.await, expected_initial);

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
