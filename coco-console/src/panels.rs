use std::{collections::BTreeMap, sync::Arc, time::Duration};

use coco_types::{
    AnchorPayload, Kind, Node, PromptAttachment, SessionAnchor, SessionAnchorPatch,
    SkillInvocationAnchor, SkillInvocationMode, Tool, ToolResult, ToolUse,
};
#[cfg(target_arch = "wasm32")]
use leptos::leptos_dom::helpers::set_timeout;
use leptos::prelude::*;
use leptos::server_fn::codec::GetUrl;
use serde::{Deserialize, Serialize};

use crate::api::{
    AnchorRangeResponse, CodeHighlightKind, CodeHighlightRange, GraphPointLink,
    GraphViewportEdgeKind, JsonHighlightKind, JsonHighlightRange, MarkdownDocument, MarkdownNode,
    NodeDetailResponse, ProviderContextBranch, ProviderContextItem, ProviderContextResponse,
    ShellHighlightKind, ShellHighlightToken, ToolInputJsonHighlight, ToolInputShellHighlight,
    ToolUseInputLink,
};
use crate::graph_render::graph_point_href;

#[cfg(target_arch = "wasm32")]
mod client;
#[cfg(target_arch = "wasm32")]
pub use client::{PROVIDER_CONTEXT_RENDERED_EVENT, notify_graph_revision};

const NODE_TARGET_PREFIX: &str = "detail-";
const MAX_PROVIDER_CONTEXT_LOAD_RETRIES: u8 = 3;
#[cfg(not(target_arch = "wasm32"))]
pub const NODE_DETAIL_PANEL_ID: &str = "node-detail-panel";

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PanelSelection {
    pub target: Option<String>,
    pub context: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct InitialProviderContext {
    pub target: String,
    pub context: Option<String>,
    pub response: ProviderContextResponse,
    pub items: Vec<ProviderContextItem>,
}

#[cfg(any(target_arch = "wasm32", test))]
impl PanelSelection {
    pub fn from_hash(hash: &str) -> Self {
        let hash = hash.strip_prefix('#').unwrap_or(hash);
        let (target, query) = hash
            .split_once('?')
            .map_or((hash, None), |(target, query)| (target, Some(query)));
        let target = decode_url_component(target);
        let target = target.starts_with(NODE_TARGET_PREFIX).then_some(target);
        let context = query.and_then(provider_context_target);

        Self { target, context }
    }

    pub fn from_query(query: &str) -> Self {
        let query = query.strip_prefix('?').unwrap_or(query);
        let target =
            query_parameter(query, "target").filter(|value| value.starts_with(NODE_TARGET_PREFIX));
        let context = provider_context_target(query);

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

#[derive(Clone, Debug, PartialEq, Eq)]
struct ProviderContextPayload {
    response: ProviderContextResponse,
    items: Vec<ProviderContextItem>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LoadedProviderContext {
    request: ProviderContextRequest,
    id: String,
    targets: Vec<String>,
}

enum PanelDetailPayload {
    Node(NodeDetailResponse),
}

#[cfg_attr(not(test), leptos::prelude::lazy(panel_detail))]
async fn render_panel_detail(payload: PanelDetailPayload) -> AnyView {
    panel_detail_view(payload)
}

fn panel_detail_view(payload: PanelDetailPayload) -> AnyView {
    match payload {
        PanelDetailPayload::Node(response) => view! { <NodeDetailContent response/> }.into_any(),
    }
}

#[server(prefix = "/api/panels", endpoint = "node-detail", input = GetUrl)]
async fn load_node_detail(
    target: String,
    graph_mode: String,
) -> Result<NodeDetailResponse, ServerFnError> {
    let context = expect_context::<crate::host::PanelServerContext>();
    context
        .node_detail(target, graph_mode)
        .await
        .map_err(|error| ServerFnError::ServerError(error.to_string()))
}

#[server(prefix = "/api/panels", endpoint = "anchor-range", input = GetUrl)]
pub async fn load_anchor_range(
    source: String,
    target: String,
    kind: GraphViewportEdgeKind,
) -> Result<AnchorRangeResponse, ServerFnError> {
    let context = expect_context::<crate::host::PanelServerContext>();
    context
        .anchor_range(source, target, kind)
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

#[server(
    prefix = "/api/panels",
    endpoint = "provider-context-items",
    input = GetUrl
)]
async fn load_provider_context_items(
    node_ids: Vec<String>,
    graph_mode: String,
) -> Result<Vec<ProviderContextItem>, ServerFnError> {
    let server = expect_context::<crate::host::PanelServerContext>();
    server
        .provider_context_items(node_ids, graph_mode)
        .await
        .map_err(|error| ServerFnError::ServerError(error.to_string()))
}

#[island]
pub fn NodeDetailPanel(graph_mode: String) -> impl IntoView {
    view! { <NodeDetailPanelBody graph_mode/> }
}

#[component]
fn NodeDetailPanelBody(graph_mode: String) -> impl IntoView {
    let selection = use_panel_selection(PanelSelection::default());
    let graph_revision = use_graph_revision();
    let selected_target = Memo::new(move |_| selection.get().target);
    let detail = LocalResource::new(move || {
        graph_revision.track();
        let request = selected_target.get();
        let graph_mode = graph_mode.clone();
        async move {
            let target = request?;
            let response = load_node_detail(target.clone(), graph_mode)
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
pub fn ProviderContextPanel(
    graph_mode: String,
    initial: Option<InitialProviderContext>,
) -> impl IntoView {
    view! { <ProviderContextPanelBody graph_mode initial/> }
}

#[component]
fn ProviderContextPanelBody(
    graph_mode: String,
    initial: Option<InitialProviderContext>,
) -> impl IntoView {
    let initial_request = initial.as_ref().map(|initial| ProviderContextRequest {
        target: initial.target.clone(),
        context: initial.context.clone(),
    });
    let initial_loaded = initial.map(|initial| LoadedPanel {
        request: ProviderContextRequest {
            target: initial.target,
            context: initial.context,
        },
        response: Ok(ProviderContextPayload {
            response: initial.response,
            items: initial.items,
        }),
    });
    let selection = use_panel_selection(PanelSelection {
        target: initial_request
            .as_ref()
            .map(|request| request.target.clone()),
        context: initial_request
            .as_ref()
            .and_then(|request| request.context.clone()),
    });
    let loaded_context = RwSignal::new(loaded_provider_context(initial_loaded.as_ref()));
    let provider_request = Memo::new(move |_| {
        provider_context_request(selection.get(), loaded_context.get().as_ref())
    });
    let graph_revision = use_graph_revision();
    let pending_initial_request = RwSignal::new(initial_request);
    let initial_for_resource = initial_loaded.clone();
    let resource_graph_mode = graph_mode.clone();
    let provider_context = LocalResource::new(move || {
        graph_revision.track();
        let request = provider_request.get();
        let initial = (pending_initial_request.get_untracked() == request)
            .then(|| initial_for_resource.clone())
            .flatten();
        if initial.is_some() {
            pending_initial_request.set(None);
        }
        let graph_mode = resource_graph_mode.clone();
        async move {
            let request = request?;
            if let Some(initial) = initial {
                return Some(initial);
            }
            let response =
                load_provider_context(request.target.clone(), request.context.clone(), graph_mode)
                    .await
                    .map_err(|error| error.to_string());
            Some(LoadedPanel {
                request,
                response: response.map(|response| ProviderContextPayload {
                    response,
                    items: Vec::new(),
                }),
            })
        }
    });
    Effect::new(move || {
        let loaded = provider_context.get().flatten();
        if loaded.is_some() {
            loaded_context.set(loaded_provider_context(loaded.as_ref()));
        }
    });
    view! {
        <div class="panel-content">
            {move || {
                provider_context_view(
                    provider_request.get(),
                    provider_context.get().flatten(),
                    initial_loaded.clone(),
                    graph_mode.clone(),
                )
            }}
        </div>
    }
}

fn provider_context_request(
    selection: PanelSelection,
    loaded: Option<&LoadedProviderContext>,
) -> Option<ProviderContextRequest> {
    let target = selection.target?;
    if let Some(loaded) = loaded
        && selection.context.as_deref() == Some(loaded.id.as_str())
        && loaded.targets.contains(&target)
    {
        return Some(loaded.request.clone());
    }
    Some(ProviderContextRequest {
        target,
        context: selection.context,
    })
}

fn loaded_provider_context(
    loaded: Option<&LoadedPanel<ProviderContextRequest, ProviderContextPayload>>,
) -> Option<LoadedProviderContext> {
    let loaded = loaded?;
    let ProviderContextResponse::Found {
        context_target,
        node_ids,
        ..
    } = &loaded.response.as_ref().ok()?.response
    else {
        return None;
    };
    if node_ids.is_empty() {
        return None;
    }
    let targets = node_ids
        .iter()
        .map(|node_id| format!("{NODE_TARGET_PREFIX}{node_id}"))
        .collect();
    Some(LoadedProviderContext {
        request: loaded.request.clone(),
        id: context_target.clone(),
        targets,
    })
}

fn use_panel_selection(initial: PanelSelection) -> RwSignal<PanelSelection> {
    let selection = RwSignal::new(initial);
    #[cfg(target_arch = "wasm32")]
    client::subscribe_to_panel_selection(selection);
    selection
}

fn use_graph_revision() -> RwSignal<u64> {
    let revision = RwSignal::new(0);
    #[cfg(target_arch = "wasm32")]
    client::subscribe_to_graph_revision(revision);
    revision
}

fn node_detail_view(
    current: Option<String>,
    loaded: Option<LoadedPanel<String, NodeDetailResponse>>,
) -> AnyView {
    match (current.as_ref(), loaded) {
        (None, _) => view! { <NodeDetailDefault/> }.into_any(),
        (Some(current), Some(loaded)) if &loaded.request == current => match loaded.response {
            Ok(response) => {
                Suspend::new(render_panel_detail(PanelDetailPayload::Node(response))).into_any()
            }
            Err(error) => view! { <NodeDetailError error=error/> }.into_any(),
        },
        _ => view! { <NodeDetailLoading/> }.into_any(),
    }
}

fn provider_context_view(
    current: Option<ProviderContextRequest>,
    loaded: Option<LoadedPanel<ProviderContextRequest, ProviderContextPayload>>,
    initial: Option<LoadedPanel<ProviderContextRequest, ProviderContextPayload>>,
    graph_mode: String,
) -> AnyView {
    let loaded = match (loaded, initial) {
        (Some(mut loaded), Some(initial)) if loaded.request == initial.request => {
            if let (Ok(loaded), Ok(initial)) = (&mut loaded.response, initial.response)
                && loaded.items.is_empty()
            {
                loaded.items = initial.items;
            }
            Some(loaded)
        }
        (Some(loaded), _) => Some(loaded),
        (None, Some(initial)) if current.as_ref() == Some(&initial.request) => Some(initial),
        (None, _) => None,
    };
    match (current.as_ref(), loaded) {
        (None, _) => view! { <ProviderContextDefault/> }.into_any(),
        (Some(current), Some(loaded)) if &loaded.request == current => match loaded.response {
            Ok(payload) => view! { <ProviderContextContent payload graph_mode/> }.into_any(),
            Err(error) => view! { <ProviderContextError error=error/> }.into_any(),
        },
        _ => view! { <ProviderContextLoading/> }.into_any(),
    }
}

#[component]
fn NodeDetailContent(response: NodeDetailResponse) -> AnyView {
    match response {
        NodeDetailResponse::Default => view! { <NodeDetailDefault/> }.into_any(),
        NodeDetailResponse::Missing { target } => {
            view! { <NodeDetailMissing target=target/> }.into_any()
        }
        NodeDetailResponse::Found {
            node,
            parent_graph_links,
            markdown_documents,
            tool_use_input_links,
            tool_input_shell_highlights,
            tool_input_json_highlights,
        } => view! {
            <NodeDetail
                node=*node
                parent_graph_links
                markdown_documents
                tool_use_input_links
                tool_input_shell_highlights
                tool_input_json_highlights
            />
        }
        .into_any(),
    }
}

#[component]
fn NodeDetail(
    node: Node,
    #[prop(default = BTreeMap::new())] parent_graph_links: BTreeMap<String, GraphPointLink>,
    markdown_documents: Vec<MarkdownDocument>,
    tool_use_input_links: Vec<ToolUseInputLink>,
    tool_input_shell_highlights: Vec<ToolInputShellHighlight>,
    tool_input_json_highlights: Vec<ToolInputJsonHighlight>,
) -> impl IntoView {
    let markdown_documents = Arc::new(markdown_documents);
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
                        <ParentRef
                            label="Parent".to_owned()
                            graph_link=parent_graph_links.get(&parent).copied()
                            node_id=parent
                        />
                    </div>
                })}
                {merge_parents.into_iter().map(|(kind, node_id)| {
                    let graph_link = parent_graph_links.get(&node_id).copied();
                    view! {
                    <div class="node-detail-meta-wide">
                        <dt>{format!("{kind} parent")}</dt>
                        <ParentRef label=format!("{kind} parent") node_id graph_link/>
                    </div>
                }}).collect::<Vec<_>>()}
            </dl>
            <NodeDetailBody
                kind=node.kind
                markdown_documents
                tool_use_input_links
                tool_input_shell_highlights
                tool_input_json_highlights
            />
        </section>
    }
}

#[component]
fn ParentRef(
    label: String,
    node_id: String,
    #[prop(optional_no_strip)] graph_link: Option<GraphPointLink>,
) -> impl IntoView {
    let target = format!("{NODE_TARGET_PREFIX}{node_id}");
    let href = graph_link.map_or_else(
        || format!("#{target}"),
        |link| graph_point_href(&target, link.point, link.local, "all"),
    );
    let aria_label = format!("Jump to {label}: {node_id}");
    let title = node_id.clone();
    let point = graph_link.filter(|link| link.local).map(|link| link.point);

    view! {
        <dd class="node-detail-parent-ref">
            <code title=title>{node_id}</code>
            <a
                class="node-detail-parent-link"
                href=href
                aria-label=aria_label
                data-node-target=target
                data-node-x=point.map(|point| point.x)
                data-node-y=point.map(|point| point.y)
            >"Jump"</a>
        </dd>
    }
}

#[component]
fn NodeDetailBody(
    kind: Kind,
    #[prop(default = Arc::new(Vec::new()))] markdown_documents: Arc<Vec<MarkdownDocument>>,
    #[prop(default = Vec::new())] tool_use_input_links: Vec<ToolUseInputLink>,
    #[prop(default = Vec::new())] tool_input_shell_highlights: Vec<ToolInputShellHighlight>,
    #[prop(default = Vec::new())] tool_input_json_highlights: Vec<ToolInputJsonHighlight>,
) -> AnyView {
    match kind {
        Kind::Anchor(anchor) => view! {
            <AnchorDetailBody payload=anchor.payload markdown_documents/>
        }
        .into_any(),
        Kind::ToolUse(items) => view! {
            <div class="node-detail-body node-detail-items">
                {items.into_iter().enumerate().map(|(index, item)| {
                    let input_links = tool_use_input_links
                        .iter()
                        .filter(|link| link.tool_use_index == index && link.tool_use_id == item.id)
                        .cloned()
                        .collect();
                    let shell_highlights = tool_input_shell_highlights
                        .iter()
                        .filter(|highlight| {
                            highlight.tool_use_index == index && highlight.tool_use_id == item.id
                        })
                        .cloned()
                        .collect();
                    let json_highlight_ranges = tool_input_json_highlights
                        .iter()
                        .find(|highlight| {
                            highlight.tool_use_index == index && highlight.tool_use_id == item.id
                        })
                        .map(|highlight| highlight.ranges.clone())
                        .unwrap_or_default();
                    view! {
                        <ToolUseDetail
                            item=item
                            input_links
                            shell_highlights
                            json_highlight_ranges
                        />
                    }
                }).collect::<Vec<_>>()}
            </div>
        }
        .into_any(),
        Kind::ToolResult(items) => view! {
            <div class="node-detail-body node-detail-items">
                {items.into_iter().map(|item| {
                    view! {
                        <ToolResultDetail
                            item=item
                            markdown_documents=markdown_documents.clone()
                        />
                    }
                }).collect::<Vec<_>>()}
            </div>
        }
        .into_any(),
        Kind::Text(text) => view! {
            <div class="node-detail-body">
                <DetailTextBlock label="Text" content=text markdown_documents/>
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
fn AnchorDetailBody(
    payload: AnchorPayload,
    markdown_documents: Arc<Vec<MarkdownDocument>>,
) -> AnyView {
    match payload {
        AnchorPayload::Session(session) => view! {
            <SessionAnchorDetail session=*session markdown_documents/>
        }
        .into_any(),
        AnchorPayload::SessionPatch(patch) => {
            view! { <SessionPatchDetail patch=patch/> }.into_any()
        }
        AnchorPayload::Prompt(prompt) => view! {
            <div class="node-detail-body">
                <DetailTextBlock
                    label="Prompt"
                    content=prompt.prompt
                    markdown_documents
                />
                <PromptAttachments attachments=prompt.attachments/>
            </div>
        }
        .into_any(),
        AnchorPayload::SkillInvocation(invocation) => view! {
            <SkillInvocationDetail invocation=invocation markdown_documents/>
        }
        .into_any(),
        AnchorPayload::SkillResult(result) => view! {
            <div class="node-detail-body">
                <dl class="node-detail-properties">
                    <div><dt>"Skill"</dt><dd>{result.skill_name}</dd></div>
                </dl>
                <DetailTextBlock
                    label="Output"
                    content=result.output
                    markdown_documents
                />
            </div>
        }
        .into_any(),
    }
}

#[component]
fn SessionAnchorDetail(
    session: SessionAnchor,
    markdown_documents: Arc<Vec<MarkdownDocument>>,
) -> impl IntoView {
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
            <DetailTextBlock
                label="Prompt"
                content=prompt
                markdown_documents=markdown_documents.clone()
            />
            <DetailTextBlock
                label="System prompt"
                content=system_prompt
                markdown_documents
            />
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
fn SkillInvocationDetail(
    invocation: SkillInvocationAnchor,
    markdown_documents: Arc<Vec<MarkdownDocument>>,
) -> impl IntoView {
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
                <DetailTextBlock
                    label="Handoff prompt"
                    content=prompt
                    markdown_documents=markdown_documents.clone()
                />
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
fn DetailTextBlock(
    label: &'static str,
    content: String,
    #[prop(default = Arc::new(Vec::new()))] markdown_documents: Arc<Vec<MarkdownDocument>>,
) -> impl IntoView {
    let document = markdown_documents
        .iter()
        .find(|document| document.source == content)
        .cloned();
    let body = if content.is_empty() {
        view! { <pre>"Empty"</pre> }.into_any()
    } else if let Some(document) = document {
        view! {
            <div class="markdown-body">
                {render_markdown_nodes(document.blocks)}
            </div>
        }
        .into_any()
    } else {
        view! { <pre>{content}</pre> }.into_any()
    };
    view! {
        <section class="node-detail-section">
            <h3>{label}</h3>
            {body}
        </section>
    }
}

pub fn render_markdown_nodes(nodes: Vec<MarkdownNode>) -> Vec<AnyView> {
    nodes.into_iter().map(render_markdown_node).collect()
}

fn render_markdown_node(node: MarkdownNode) -> AnyView {
    match node {
        node @ (MarkdownNode::Text { .. }
        | MarkdownNode::Emphasis { .. }
        | MarkdownNode::Strong { .. }
        | MarkdownNode::Strikethrough { .. }
        | MarkdownNode::InlineCode { .. }
        | MarkdownNode::Link { .. }
        | MarkdownNode::LineBreak) => render_markdown_inline(node),
        node => render_markdown_block(node),
    }
}

fn render_markdown_inline(node: MarkdownNode) -> AnyView {
    match node {
        MarkdownNode::Text { text } => text.into_any(),
        MarkdownNode::Emphasis { children } => {
            view! { <em>{render_markdown_nodes(children)}</em> }.into_any()
        }
        MarkdownNode::Strong { children } => {
            view! { <strong>{render_markdown_nodes(children)}</strong> }.into_any()
        }
        MarkdownNode::Strikethrough { children } => {
            view! { <del>{render_markdown_nodes(children)}</del> }.into_any()
        }
        MarkdownNode::InlineCode { code } => view! { <code>{code}</code> }.into_any(),
        MarkdownNode::Link {
            destination,
            children,
        } => render_markdown_link(destination, children),
        MarkdownNode::LineBreak => view! { <br/> }.into_any(),
        _ => unreachable!("block nodes are rendered separately"),
    }
}

fn render_markdown_link(destination: String, children: Vec<MarkdownNode>) -> AnyView {
    if safe_markdown_link(&destination) {
        view! {
            <a href=destination>{render_markdown_nodes(children)}</a>
        }
        .into_any()
    } else {
        view! { <span>{render_markdown_nodes(children)}</span> }.into_any()
    }
}

fn render_markdown_block(node: MarkdownNode) -> AnyView {
    match node {
        MarkdownNode::Paragraph { children } => {
            view! { <p>{render_markdown_nodes(children)}</p> }.into_any()
        }
        MarkdownNode::UnorderedList { items } => view! {
            <ul>
                {items.into_iter().map(|children| {
                    view! { <li>{render_markdown_nodes(children)}</li> }
                }).collect::<Vec<_>>()}
            </ul>
        }
        .into_any(),
        MarkdownNode::OrderedList { start, items } => view! {
            <ol start=start>
                {items.into_iter().map(|children| {
                    view! { <li>{render_markdown_nodes(children)}</li> }
                }).collect::<Vec<_>>()}
            </ol>
        }
        .into_any(),
        MarkdownNode::BlockQuote { children } => {
            view! { <blockquote>{render_markdown_nodes(children)}</blockquote> }.into_any()
        }
        node => render_markdown_leaf_block(node),
    }
}

fn render_markdown_leaf_block(node: MarkdownNode) -> AnyView {
    match node {
        MarkdownNode::Heading { level, children } => render_markdown_heading(level, children),
        MarkdownNode::CodeBlock {
            language,
            code,
            highlights,
        } => view! {
            <pre class="markdown-code" data-language=language>
                <code>{highlighted_code_views(code, &highlights)}</code>
            </pre>
        }
        .into_any(),
        MarkdownNode::ThematicBreak => view! { <hr/> }.into_any(),
        _ => unreachable!("inline and structural block nodes are rendered separately"),
    }
}

impl CodeHighlightKind {
    fn class(self) -> Option<&'static str> {
        match self {
            Self::Plain => None,
            Self::Comment => Some("syntax-comment"),
            Self::Constant => Some("syntax-constant"),
            Self::Function => Some("syntax-function"),
            Self::Keyword => Some("syntax-keyword"),
            Self::Number => Some("syntax-number"),
            Self::Operator => Some("syntax-operator"),
            Self::Property => Some("syntax-property"),
            Self::String => Some("syntax-string"),
            Self::Type => Some("syntax-type"),
            Self::Variable => Some("syntax-variable"),
        }
    }
}

pub fn highlighted_code_views(source: String, ranges: &[CodeHighlightRange]) -> Vec<AnyView> {
    highlighted_source_views(
        source,
        ranges
            .iter()
            .map(|range| (range.kind.class(), range.start, range.end)),
    )
}

fn render_markdown_heading(level: u8, children: Vec<MarkdownNode>) -> AnyView {
    match level {
        1 => view! { <h1>{render_markdown_nodes(children)}</h1> }.into_any(),
        2 => view! { <h2>{render_markdown_nodes(children)}</h2> }.into_any(),
        3 => view! { <h3>{render_markdown_nodes(children)}</h3> }.into_any(),
        4 => view! { <h4>{render_markdown_nodes(children)}</h4> }.into_any(),
        5 => view! { <h5>{render_markdown_nodes(children)}</h5> }.into_any(),
        _ => view! { <h6>{render_markdown_nodes(children)}</h6> }.into_any(),
    }
}

fn safe_markdown_link(destination: &str) -> bool {
    if destination.chars().any(char::is_control) {
        return false;
    }
    let normalized = destination.trim().to_ascii_lowercase();
    normalized.starts_with("http://")
        || normalized.starts_with("https://")
        || normalized.starts_with("mailto:")
        || normalized.starts_with('/')
        || normalized.starts_with('#')
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ToolInputView {
    List,
    Raw,
}

impl JsonHighlightKind {
    fn class(self) -> Option<&'static str> {
        match self {
            Self::Plain => None,
            Self::Key => Some("json-key"),
            Self::String => Some("json-string"),
            Self::Number => Some("json-number"),
            Self::Boolean => Some("json-boolean"),
            Self::Null => Some("json-null"),
            Self::Escape => Some("json-escape"),
        }
    }
}

impl ShellHighlightKind {
    fn class(self) -> Option<&'static str> {
        match self {
            Self::Plain => None,
            Self::Command => Some("shell-command"),
            Self::Option => Some("shell-option"),
            Self::String => Some("shell-string"),
            Self::Variable => Some("shell-variable"),
            Self::Operator => Some("shell-operator"),
            Self::Comment => Some("shell-comment"),
            Self::Keyword => Some("shell-keyword"),
        }
    }
}

#[component]
fn ToolInput(
    input: serde_json::Value,
    #[prop(default = Vec::new())] input_links: Vec<ToolUseInputLink>,
    #[prop(default = Vec::new())] shell_highlights: Vec<ToolInputShellHighlight>,
    #[prop(default = Vec::new())] json_highlight_ranges: Vec<JsonHighlightRange>,
) -> impl IntoView {
    let view = ArcRwSignal::new(ToolInputView::List);
    let list_class_view = view.clone();
    let list_pressed_view = view.clone();
    let list_click_view = view.clone();
    let raw_class_view = view.clone();
    let raw_pressed_view = view.clone();
    let raw_click_view = view.clone();
    let content_view = view;
    let list_input = input.clone();
    let list_input_links = input_links;
    let list_shell_highlights = shell_highlights;
    let raw_json_highlight_ranges = json_highlight_ranges;

    view! {
        <section class="node-detail-section tool-input">
            <div class="tool-input-heading">
                <h3>"Input"</h3>
                <div class="tool-input-view-toggle" role="group" aria-label="Input view">
                    <button
                        type="button"
                        class:active=move || list_class_view.get() == ToolInputView::List
                        aria-pressed=move || {
                            if list_pressed_view.get() == ToolInputView::List {
                                "true"
                            } else {
                                "false"
                            }
                        }
                        on:click=move |_| list_click_view.set(ToolInputView::List)
                    >
                        "List"
                    </button>
                    <button
                        type="button"
                        class:active=move || raw_class_view.get() == ToolInputView::Raw
                        aria-pressed=move || {
                            if raw_pressed_view.get() == ToolInputView::Raw {
                                "true"
                            } else {
                                "false"
                            }
                        }
                        on:click=move |_| raw_click_view.set(ToolInputView::Raw)
                    >
                        "Raw JSON"
                    </button>
                </div>
            </div>
            {move || match content_view.get() {
                ToolInputView::List => {
                    view! {
                        <ToolInputList
                            input=list_input.clone()
                            input_links=list_input_links.clone()
                            shell_highlights=list_shell_highlights.clone()
                        />
                    }
                    .into_any()
                }
                ToolInputView::Raw => {
                    view! {
                        <ToolInputRaw
                            input=input.clone()
                            ranges=raw_json_highlight_ranges.clone()
                        />
                    }
                    .into_any()
                }
            }}
        </section>
    }
}

#[component]
fn ToolInputList(
    input: serde_json::Value,
    #[prop(default = Vec::new())] input_links: Vec<ToolUseInputLink>,
    #[prop(default = Vec::new())] shell_highlights: Vec<ToolInputShellHighlight>,
) -> impl IntoView {
    tool_input_list(&input, "", &input_links, &shell_highlights)
}

fn tool_input_list(
    value: &serde_json::Value,
    pointer: &str,
    input_links: &[ToolUseInputLink],
    shell_highlights: &[ToolInputShellHighlight],
) -> AnyView {
    let entries = match value {
        serde_json::Value::Object(values) => values
            .iter()
            .map(|(key, value)| {
                let pointer = json_pointer_child(pointer, key);
                tool_input_list_entry(
                    key.clone(),
                    "json-key",
                    value,
                    pointer,
                    input_links,
                    shell_highlights,
                )
            })
            .collect::<Vec<_>>(),
        serde_json::Value::Array(values) => values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                let label = index.to_string();
                let pointer = json_pointer_child(pointer, &label);
                tool_input_list_entry(
                    label,
                    "json-index",
                    value,
                    pointer,
                    input_links,
                    shell_highlights,
                )
            })
            .collect::<Vec<_>>(),
        _ => {
            return view! {
                <ul class="tool-input-list tool-input-list-root">
                    <li class="tool-input-entry tool-input-entry-root">
                        {tool_input_scalar(value)}
                    </li>
                </ul>
            }
            .into_any();
        }
    };

    if entries.is_empty() {
        let description = match value {
            serde_json::Value::Object(_) => "Empty object",
            serde_json::Value::Array(_) => "Empty array",
            _ => unreachable!("only containers can produce an empty entry list"),
        };
        view! { <p class="tool-input-empty">{description}</p> }.into_any()
    } else {
        let class = if pointer.is_empty() {
            "tool-input-list tool-input-list-root"
        } else {
            "tool-input-list"
        };
        view! { <ul class=class>{entries}</ul> }.into_any()
    }
}

fn tool_input_list_entry(
    label: String,
    label_class: &'static str,
    value: &serde_json::Value,
    pointer: String,
    input_links: &[ToolUseInputLink],
    shell_highlights: &[ToolInputShellHighlight],
) -> AnyView {
    if label == "cmd"
        && let serde_json::Value::String(command) = value
    {
        let tokens = shell_highlights
            .iter()
            .find(|highlight| highlight.input_pointer == pointer)
            .map(|highlight| highlight.tokens.clone())
            .unwrap_or_else(|| {
                vec![ShellHighlightToken {
                    kind: ShellHighlightKind::Plain,
                    text: command.clone(),
                }]
            });
        return tool_input_shell_entry(label, label_class, tokens, pointer);
    }

    let summary = match value {
        serde_json::Value::Object(values) => {
            Some(format_container_summary("Object", values.len(), "field"))
        }
        serde_json::Value::Array(values) => {
            Some(format_container_summary("Array", values.len(), "item"))
        }
        _ => None,
    };
    if let Some(summary) = summary {
        let children = tool_input_list(value, &pointer, input_links, shell_highlights);
        view! {
            <li class="tool-input-entry tool-input-container" data-json-pointer=pointer>
                <div class="tool-input-entry-heading">
                    <span class=label_class>{label}</span>
                    <span class="tool-input-summary">{summary}</span>
                </div>
                {children}
            </li>
        }
        .into_any()
    } else {
        let target = match value {
            serde_json::Value::String(value) => input_links
                .iter()
                .find(|link| link.input_pointer == pointer && link.value == *value)
                .map(|link| format!("#{}", link.target)),
            serde_json::Value::Number(_)
            | serde_json::Value::Bool(_)
            | serde_json::Value::Null
            | serde_json::Value::Object(_)
            | serde_json::Value::Array(_) => None,
        };
        let scalar = tool_input_scalar(value);
        view! {
            <li class="tool-input-entry" data-json-pointer=pointer>
                <span class=label_class>{label}</span>
                {match target {
                    Some(target) => view! {
                        <a
                            class="tool-input-value-link"
                            href=target
                            title="Open exec_command"
                        >
                            {scalar}
                        </a>
                    }
                    .into_any(),
                    None => scalar,
                }}
            </li>
        }
        .into_any()
    }
}

fn tool_input_shell_entry(
    label: String,
    label_class: &'static str,
    tokens: Vec<ShellHighlightToken>,
    pointer: String,
) -> AnyView {
    let tokens = tokens
        .into_iter()
        .map(|token| match token.kind.class() {
            Some(class) => view! { <span class=class>{token.text}</span> }.into_any(),
            None => token.text.into_any(),
        })
        .collect::<Vec<_>>();

    view! {
        <li
            class="tool-input-entry tool-input-entry-shell"
            data-json-pointer=pointer
        >
            <span class=label_class>{label}</span>
            <pre class="tool-input-shell" aria-label="Shell command">
                <code>{tokens}</code>
            </pre>
        </li>
    }
    .into_any()
}

fn tool_input_scalar(value: &serde_json::Value) -> AnyView {
    let (class, text) = match value {
        serde_json::Value::String(value) => ("json-string", value.clone()),
        serde_json::Value::Number(value) => ("json-number", value.to_string()),
        serde_json::Value::Bool(value) => ("json-boolean", value.to_string()),
        serde_json::Value::Null => ("json-null", "null".to_owned()),
        serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
            unreachable!("containers should render as nested lists")
        }
    };
    view! { <span class=class>{text}</span> }.into_any()
}

fn json_pointer_child(parent: &str, child: &str) -> String {
    let escaped = child.replace('~', "~0").replace('/', "~1");
    format!("{parent}/{escaped}")
}

fn format_container_summary(kind: &str, len: usize, item: &str) -> String {
    let suffix = if len == 1 {
        item.to_owned()
    } else {
        format!("{item}s")
    };
    format!("{kind} · {len} {suffix}")
}

#[component]
fn ToolInputRaw(
    input: serde_json::Value,
    #[prop(default = Vec::new())] ranges: Vec<JsonHighlightRange>,
) -> impl IntoView {
    let source = pretty_json(&input);
    let tokens = highlighted_json_views(source, &ranges);

    view! {
        <pre class="node-detail-code tool-input-raw" aria-label="Raw JSON input">
            <code>{tokens}</code>
        </pre>
    }
}

fn highlighted_json_views(source: String, ranges: &[JsonHighlightRange]) -> Vec<AnyView> {
    highlighted_source_views(
        source,
        ranges
            .iter()
            .map(|range| (range.kind.class(), range.start, range.end)),
    )
}

fn highlighted_source_views(
    source: String,
    ranges: impl IntoIterator<Item = (Option<&'static str>, usize, usize)>,
) -> Vec<AnyView> {
    let mut tokens = Vec::new();
    let mut cursor = 0;
    for (class, start, end) in ranges {
        if start < cursor
            || end <= start
            || end > source.len()
            || !source.is_char_boundary(start)
            || !source.is_char_boundary(end)
        {
            return vec![source.into_any()];
        }
        if cursor < start {
            tokens.push(source[cursor..start].to_owned().into_any());
        }
        let text = source[start..end].to_owned();
        tokens.push(match class {
            Some(class) => view! { <span class=class>{text}</span> }.into_any(),
            None => text.into_any(),
        });
        cursor = end;
    }
    if cursor < source.len() {
        tokens.push(source[cursor..].to_owned().into_any());
    }
    tokens
}

#[component]
fn ToolUseDetail(
    item: ToolUse,
    input_links: Vec<ToolUseInputLink>,
    shell_highlights: Vec<ToolInputShellHighlight>,
    json_highlight_ranges: Vec<JsonHighlightRange>,
) -> impl IntoView {
    view! {
        <article class="node-detail-item">
            <header>
                <strong>{item.name}</strong>
                <code>{item.id}</code>
            </header>
            <ToolInput
                input=item.input
                input_links
                shell_highlights
                json_highlight_ranges
            />
        </article>
    }
}

#[component]
fn ToolResultDetail(
    item: ToolResult,
    markdown_documents: Arc<Vec<MarkdownDocument>>,
) -> impl IntoView {
    view! {
        <article class="node-detail-item">
            <header>
                <strong>"Tool result"</strong>
                <code>{item.id}</code>
            </header>
            <DetailTextBlock
                label="Output"
                content=item.output
                markdown_documents
            />
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
fn ProviderContextContent(payload: ProviderContextPayload, graph_mode: String) -> AnyView {
    #[cfg(target_arch = "wasm32")]
    Effect::new(client::notify_provider_context_rendered);
    match payload.response {
        ProviderContextResponse::Default => view! { <ProviderContextDefault/> }.into_any(),
        ProviderContextResponse::Missing { target } => {
            view! { <ProviderContextMissing target=target/> }.into_any()
        }
        ProviderContextResponse::Found {
            context_target,
            selected_id,
            node_ids,
            branches,
            ..
        } => view! {
            <ProviderContextList
                context_target
                selected_id
                node_ids
                branches
                initial_items=payload.items
                graph_mode
            />
        }
        .into_any(),
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
fn ProviderContextList(
    context_target: String,
    selected_id: String,
    node_ids: Vec<String>,
    branches: Vec<ProviderContextBranch>,
    initial_items: Vec<ProviderContextItem>,
    graph_mode: String,
) -> AnyView {
    if node_ids.is_empty() {
        view! {
            <section class="provider-context-section">
                <h2>"Provider Context"</h2>
                <p class="provider-context-empty">"No provider context nodes."</p>
            </section>
        }
        .into_any()
    } else {
        let scroll_root = NodeRef::<leptos::html::Section>::new();
        let mut initial_items = initial_items
            .into_iter()
            .map(|item| (item.node.id.clone(), item))
            .collect::<BTreeMap<_, _>>();
        view! {
            <section node_ref=scroll_root class="provider-context-section">
                <h2>"Provider Context"</h2>
                <ProviderContextBranches
                    selected_id=selected_id.clone()
                    branches
                />
                <ol class="provider-context-list">
                    {node_ids
                        .into_iter()
                        .enumerate()
                        .map(|(index, node_id)| {
                            let initial_item = initial_items.remove(&node_id);
                            view! {
                                <ProviderContextRow
                                    context_target=context_target.clone()
                                    selected=node_id == selected_id
                                    llm_context_start={index > 0 && node_id == selected_id}
                                    node_id
                                    initial_item
                                    scroll_root
                                    graph_mode=graph_mode.clone()
                                />
                            }
                        })
                        .collect::<Vec<_>>()}
                </ol>
            </section>
        }
        .into_any()
    }
}

#[component]
fn ProviderContextBranches(
    selected_id: String,
    branches: Vec<ProviderContextBranch>,
) -> impl IntoView {
    let links = branches
        .into_iter()
        .map(|branch| {
            let active = branch.head_node_id == selected_id;
            let label = if branch.name.is_empty() {
                "(default)".to_owned()
            } else {
                branch.name
            };
            let href = format!("#{NODE_TARGET_PREFIX}{}", branch.head_node_id);
            view! {
                <a
                    class="provider-context-branch"
                    href=href
                    title=branch.head_node_id
                    data-context-target=branch.context_target
                    aria-current=active.then_some("true")
                >
                    {label}
                </a>
            }
        })
        .collect::<Vec<_>>();
    (!links.is_empty()).then(|| view! { <nav class="provider-context-branches">{links}</nav> })
}

#[component]
fn ProviderContextRow(
    context_target: String,
    selected: bool,
    llm_context_start: bool,
    node_id: String,
    initial_item: Option<ProviderContextItem>,
    scroll_root: NodeRef<leptos::html::Section>,
    graph_mode: String,
) -> impl IntoView {
    let row_ref = NodeRef::<leptos::html::Li>::new();
    #[cfg(not(target_arch = "wasm32"))]
    let _ = scroll_root;
    let item = RwSignal::new(initial_item);
    let should_load = RwSignal::new(false);
    let retry_attempt = RwSignal::new(0_u8);
    let load_error = RwSignal::new(None::<String>);
    #[cfg(target_arch = "wasm32")]
    if item.get_untracked().is_none() {
        client::load_provider_context_row_when_visible(row_ref, scroll_root, should_load);
    }

    let loaded_item = LocalResource::new({
        let node_id = node_id.clone();
        move || {
            let should_load = should_load.get();
            retry_attempt.track();
            let node_id = node_id.clone();
            let graph_mode = graph_mode.clone();
            async move {
                if !should_load || item.get_untracked().is_some() {
                    return None;
                }
                Some(
                    load_provider_context_items(vec![node_id], graph_mode)
                        .await
                        .map_err(|error| error.to_string()),
                )
            }
        }
    });
    #[cfg(target_arch = "wasm32")]
    let rendered_node_id = node_id.clone();
    Effect::new(move || {
        let Some(Some(result)) = loaded_item.get() else {
            return;
        };
        match result {
            Ok(items) => {
                if let Some(loaded) = items.into_iter().next() {
                    load_error.set(None);
                    item.set(Some(loaded));
                    #[cfg(target_arch = "wasm32")]
                    client::notify_selected_provider_context_row_rendered(&rendered_node_id);
                }
            }
            Err(error) => {
                load_error.set(Some(error));
                #[cfg(target_arch = "wasm32")]
                if let Some(delay) = provider_context_retry_delay(retry_attempt.get_untracked()) {
                    set_timeout(move || retry_attempt.update(|attempt| *attempt += 1), delay);
                }
            }
        }
    });

    let class = move || {
        provider_context_node_class(
            item.with(|item| item.as_ref().is_some_and(|item| item.point.is_some())),
            selected,
            llm_context_start,
        )
    };
    let content = move || {
        let failed = load_error.with(Option::is_some);
        let retrying = failed && provider_context_retry_delay(retry_attempt.get()).is_some();
        provider_context_row_content(
            &context_target,
            &node_id,
            selected,
            item.get(),
            should_load.get(),
            failed,
            retrying,
        )
    };

    view! {
        <li node_ref=row_ref class=class>{content}</li>
    }
}

fn provider_context_row_content(
    context_target: &str,
    node_id: &str,
    selected: bool,
    item: Option<ProviderContextItem>,
    requested: bool,
    failed: bool,
    retrying: bool,
) -> AnyView {
    let node_target = format!("{NODE_TARGET_PREFIX}{node_id}");
    let target = format!("#{node_target}?context={context_target}");
    let Some(item) = item else {
        let message = match (requested, failed, retrying) {
            (_, true, true) => "Retrying node summary...",
            (_, true, false) => "Failed to load node summary.",
            (true, false, _) => "Loading node summary...",
            (false, false, _) => "Scroll to load node summary...",
        };
        return view! {
            <a
                class="provider-context-node-link provider-context-node-placeholder"
                href=target
                data-node-id=node_id.to_owned()
                data-node-target=node_target
                aria-current=selected.then_some("true")
                aria-busy=(!failed || retrying).then_some("true")
            >
                <div class="provider-context-node-head">
                    <span>{provider_context_short_id(node_id)}</span>
                </div>
                <p>{message}</p>
            </a>
        }
        .into_any();
    };
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
        <a
            class="provider-context-node-link"
            href=target
            data-node-id=node_id.to_owned()
            data-node-target=node_target
            aria-current=selected.then_some("true")
        >
            {graph_point}
            <div class="provider-context-node-head">
                <span>{item.node.short_id}</span>
                <span>{item.node.kind}</span>
                <span>{item.node.role}</span>
            </div>
            <time>{item.node.created_at}</time>
            <p>{item.node.summary}</p>
        </a>
    }
    .into_any()
}

fn provider_context_retry_delay(attempt: u8) -> Option<Duration> {
    (attempt < MAX_PROVIDER_CONTEXT_LOAD_RETRIES).then(|| Duration::from_millis(250_u64 << attempt))
}

fn provider_context_short_id(node_id: &str) -> String {
    node_id.chars().take(12).collect()
}

fn provider_context_node_class(visible: bool, selected: bool, llm_context_start: bool) -> String {
    let mut class = "provider-context-node".to_owned();
    if visible {
        class.push_str(" visible");
    }
    if selected {
        class.push_str(" selected");
    }
    if llm_context_start {
        class.push_str(" llm-context-start");
    }
    class
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
    query_parameter(query, "context").filter(|value| value.starts_with(NODE_TARGET_PREFIX))
}

#[cfg(any(target_arch = "wasm32", test))]
fn query_parameter(query: &str, name: &str) -> Option<String> {
    url::form_urlencoded::parse(query.as_bytes())
        .find_map(|(key, value)| (key == name).then(|| value.into_owned()))
}

#[cfg(any(target_arch = "wasm32", test))]
fn decode_url_component(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && index + 2 < bytes.len()
            && let Some(byte) = decode_hex_pair(bytes[index + 1], bytes[index + 2])
        {
            decoded.push(byte);
            index += 3;
            continue;
        }
        decoded.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

#[cfg(any(target_arch = "wasm32", test))]
fn decode_hex_pair(high: u8, low: u8) -> Option<u8> {
    Some(decode_hex_digit(high)? << 4 | decode_hex_digit(low)?)
}

#[cfg(any(target_arch = "wasm32", test))]
fn decode_hex_digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
fn test_json_range(source: &str, text: &str, kind: JsonHighlightKind) -> JsonHighlightRange {
    let start = source.find(text).expect("test JSON token should exist");
    JsonHighlightRange {
        kind,
        start,
        end: start + text.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::ProviderContextNode;
    use coco_types::{Anchor, MergeParent, PromptAnchor, SessionRole, SkillResultAnchor};

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
        assert_eq!(
            PanelSelection::from_query(
                "?mode=all&target=detail-node&context=detail-context&ignored=value"
            ),
            PanelSelection {
                target: Some("detail-node".to_owned()),
                context: Some("detail-context".to_owned()),
            }
        );
        assert_eq!(
            PanelSelection::from_hash("#detail-my%20branch?context=detail-root%20branch"),
            PanelSelection {
                target: Some("detail-my branch".to_owned()),
                context: Some("detail-root branch".to_owned()),
            }
        );
        assert_eq!(
            PanelSelection::from_hash("#detail-feature%3Fx?context=detail-root%26y"),
            PanelSelection {
                target: Some("detail-feature?x".to_owned()),
                context: Some("detail-root&y".to_owned()),
            }
        );
        assert_eq!(
            PanelSelection::from_query("?target=detail-my+branch&context=detail-root%20branch"),
            PanelSelection {
                target: Some("detail-my branch".to_owned()),
                context: Some("detail-root branch".to_owned()),
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
        assert_eq!(
            PanelSelection::from_query("?target=section&context=invalid"),
            PanelSelection::default()
        );
    }

    #[test]
    fn provider_context_request_reuses_the_request_that_loaded_the_context() {
        let cached_request = ProviderContextRequest {
            target: "detail-first".to_owned(),
            context: None,
        };
        let loaded = LoadedProviderContext {
            request: cached_request.clone(),
            id: "detail-root-context-branch".to_owned(),
            targets: vec!["detail-first".to_owned(), "detail-second".to_owned()],
        };

        assert_eq!(
            provider_context_request(
                PanelSelection {
                    target: Some("detail-outside".to_owned()),
                    context: Some(loaded.id.clone()),
                },
                Some(&loaded),
            ),
            Some(ProviderContextRequest {
                target: "detail-outside".to_owned(),
                context: Some(loaded.id.clone()),
            })
        );
        assert_eq!(
            provider_context_request(
                PanelSelection {
                    target: Some("detail-second".to_owned()),
                    context: Some(loaded.id.clone()),
                },
                Some(&loaded),
            ),
            Some(cached_request)
        );
    }

    #[test]
    fn loaded_provider_context_tracks_its_id_and_node_targets() {
        let loaded = LoadedPanel {
            request: ProviderContextRequest {
                target: "detail-first".to_owned(),
                context: None,
            },
            response: Ok(ProviderContextPayload {
                response: ProviderContextResponse::Found {
                    context_target: "detail-root-context-branch".to_owned(),
                    previous_context_target: None,
                    selected_id: "first".to_owned(),
                    node_ids: vec!["first".to_owned()],
                    branches: Vec::new(),
                },
                items: vec![ProviderContextItem {
                    node: ProviderContextNode {
                        id: "first".to_owned(),
                        short_id: "first".to_owned(),
                        kind: "text".to_owned(),
                        role: "assistant".to_owned(),
                        created_at: "2026-08-01T00:00:00Z".to_owned(),
                        summary: "First".to_owned(),
                    },
                    point: None,
                }],
            }),
        };

        assert_eq!(
            loaded_provider_context(Some(&loaded)),
            Some(LoadedProviderContext {
                request: loaded.request,
                id: "detail-root-context-branch".to_owned(),
                targets: vec!["detail-first".to_owned()],
            })
        );
    }

    #[test]
    fn panel_islands_render_independent_server_fallbacks() {
        let node = view! { <NodeDetailPanel graph_mode="all".to_owned()/> }.to_html();
        let provider = view! {
            <ProviderContextPanel graph_mode="all".to_owned() initial=None/>
        }
        .to_html();

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
                response: Ok(ProviderContextPayload {
                    response: ProviderContextResponse::Default,
                    items: Vec::new(),
                }),
            }),
            None,
            "all".to_owned(),
        )
        .to_html();

        assert!(node.contains("Loading node detail..."));
        assert!(provider.contains("Loading provider context..."));
    }

    #[test]
    fn panel_components_render_typed_success_and_error_states() {
        let node = panel_detail_view(PanelDetailPayload::Node(NodeDetailResponse::Found {
            node: Box::new(test_node(Kind::Text(
                "<script>alert(1)</script>".to_owned(),
            ))),
            parent_graph_links: BTreeMap::new(),
            markdown_documents: Vec::new(),
            tool_use_input_links: Vec::new(),
            tool_input_shell_highlights: Vec::new(),
            tool_input_json_highlights: Vec::new(),
        }))
        .to_html();
        let provider = view! { <ProviderContextContent
            payload=ProviderContextPayload {
                response: ProviderContextResponse::Found {
                    context_target: String::new(),
                    previous_context_target: None,
                    selected_id: String::new(),
                    node_ids: Vec::new(),
                    branches: Vec::new(),
                },
                items: Vec::new(),
            }
            graph_mode="all".to_owned()
        /> }
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
    fn provider_context_ssr_renders_initial_items_and_deferred_id_placeholders() {
        let owner = Owner::new();
        owner.set();
        let initial_id = "11111111111111111111111111111111";
        let deferred_id = "22222222222222222222222222222222";
        let provider = view! {
            <ProviderContextContent
                graph_mode="all".to_owned()
                payload=ProviderContextPayload {
                    response: ProviderContextResponse::Found {
                        context_target: "detail-context".to_owned(),
                        previous_context_target: None,
                        selected_id: initial_id.to_owned(),
                        node_ids: vec![initial_id.to_owned(), deferred_id.to_owned()],
                        branches: vec![ProviderContextBranch {
                            name: "main".to_owned(),
                            head_node_id: deferred_id.to_owned(),
                            context_target: "detail-main-context".to_owned(),
                        }],
                    },
                    items: vec![ProviderContextItem {
                        node: ProviderContextNode {
                            id: initial_id.to_owned(),
                            short_id: "111111111111".to_owned(),
                            kind: "text".to_owned(),
                            role: "user".to_owned(),
                            created_at: "2026-08-01T00:00:00Z".to_owned(),
                            summary: "Server-rendered summary".to_owned(),
                        },
                        point: None,
                    }],
                }
            />
        }
        .to_html();

        assert!(provider.contains("Server-rendered summary"));
        assert!(provider.contains("provider-context-branch"));
        assert!(provider.contains(&format!("#detail-{deferred_id}")));
        assert!(provider.contains(deferred_id));
        assert!(provider.contains("Scroll to load node summary..."));
        assert!(provider.contains("aria-busy=\"true\""));
    }

    #[test]
    fn provider_context_separates_the_llm_context_start_from_newer_nodes() {
        let owner = Owner::new();
        owner.set();
        let newer_id = "11111111111111111111111111111111";
        let selected_id = "22222222222222222222222222222222";
        let older_id = "33333333333333333333333333333333";
        let provider = view! {
            <ProviderContextContent
                graph_mode="all".to_owned()
                payload=ProviderContextPayload {
                    response: ProviderContextResponse::Found {
                        context_target: "detail-context".to_owned(),
                        previous_context_target: None,
                        selected_id: selected_id.to_owned(),
                        node_ids: vec![
                            newer_id.to_owned(),
                            selected_id.to_owned(),
                            older_id.to_owned(),
                        ],
                        branches: Vec::new(),
                    },
                    items: Vec::new(),
                }
            />
        }
        .to_html();

        assert_eq!(provider.matches("llm-context-start").count(), 1);
        assert!(provider.contains("class=\"provider-context-node selected llm-context-start\""));
    }

    #[test]
    fn provider_context_deferred_load_retries_are_bounded() {
        assert_eq!(
            (0..=MAX_PROVIDER_CONTEXT_LOAD_RETRIES)
                .map(provider_context_retry_delay)
                .collect::<Vec<_>>(),
            [
                Some(Duration::from_millis(250)),
                Some(Duration::from_millis(500)),
                Some(Duration::from_secs(1)),
                None,
            ]
        );

        let failed = provider_context_row_content(
            "detail-context",
            "deferred-node",
            false,
            None,
            true,
            true,
            false,
        )
        .to_html();
        assert!(failed.contains("Failed to load node summary."));
        assert!(!failed.contains("aria-busy=\"true\""));
    }

    #[test]
    fn node_detail_parent_refs_link_to_each_parent() {
        let node = test_node(Kind::Anchor(Anchor::prompt(
            vec![
                MergeParent::merge("merge-node"),
                MergeParent::shadow("shadow-node"),
            ],
            PromptAnchor {
                prompt: "Follow the parent refs".to_owned(),
                attachments: Vec::new(),
            },
        )));
        let parent_graph_links = BTreeMap::from([
            (
                "parent-node".to_owned(),
                GraphPointLink {
                    point: crate::api::Point { x: 120, y: 80 },
                    local: true,
                },
            ),
            (
                "merge-node".to_owned(),
                GraphPointLink {
                    point: crate::api::Point { x: 240, y: 160 },
                    local: false,
                },
            ),
            (
                "shadow-node".to_owned(),
                GraphPointLink {
                    point: crate::api::Point { x: 360, y: 240 },
                    local: true,
                },
            ),
        ]);
        let detail = view! {
            <NodeDetail
                node
                parent_graph_links
                markdown_documents=Vec::new()
                tool_use_input_links=Vec::new()
                tool_input_shell_highlights=Vec::new()
                tool_input_json_highlights=Vec::new()
            />
        }
        .to_html();

        assert_eq!(
            detail.matches(r#"class="node-detail-parent-link""#).count(),
            3
        );
        for (label, node_id) in [
            ("Parent", "parent-node"),
            ("Merge parent", "merge-node"),
            ("Shadow parent", "shadow-node"),
        ] {
            assert!(
                detail.contains(&format!(r#"aria-label="Jump to {label}: {node_id}""#)),
                "{detail}"
            );
        }
        assert!(
            detail.contains(r##"href="#detail-parent-node""##)
                && detail.contains(r#"data-node-target="detail-parent-node""#)
                && detail.contains(r#"data-node-x="120" data-node-y="80""#),
            "{detail}"
        );
        assert!(
            detail.contains(r##"href="#detail-shadow-node""##),
            "{detail}"
        );
        assert!(
            detail.contains(
                r#"href="/?mode=all&amp;graph_focus_target=detail-merge-node&amp;graph_focus_x=240&amp;graph_focus_y=160#detail-merge-node""#
            ) && detail.contains(r#"data-node-target="detail-merge-node""#),
            "{detail}"
        );
        assert!(!detail.contains(r#"data-node-x="240""#), "{detail}");
    }

    #[test]
    fn node_detail_renders_server_parsed_markdown_safely() {
        let source = "# Rendered\n\n**safe** [bad](javascript:alert(1)) <script>";
        let node = view! {
            <NodeDetailContent response=NodeDetailResponse::Found {
                node: Box::new(test_node(Kind::Text(source.to_owned()))),
                parent_graph_links: BTreeMap::new(),
                markdown_documents: vec![MarkdownDocument {
                    source: source.to_owned(),
                    blocks: vec![
                        MarkdownNode::Heading {
                            level: 1,
                            children: vec![MarkdownNode::Text {
                                text: "Rendered".to_owned(),
                            }],
                        },
                        MarkdownNode::Paragraph {
                            children: vec![
                                MarkdownNode::Strong {
                                    children: vec![MarkdownNode::Text {
                                        text: "safe".to_owned(),
                                    }],
                                },
                                MarkdownNode::Text {
                                    text: " ".to_owned(),
                                },
                                MarkdownNode::Link {
                                    destination: "javascript:alert(1)".to_owned(),
                                    children: vec![MarkdownNode::Text {
                                        text: "bad".to_owned(),
                                    }],
                                },
                                MarkdownNode::Text {
                                    text: " <script>".to_owned(),
                                },
                            ],
                        },
                    ],
                }],
                tool_use_input_links: Vec::new(),
                tool_input_shell_highlights: Vec::new(),
                tool_input_json_highlights: Vec::new(),
            }/>
        }
        .to_html();

        assert!(node.contains(r#"class="markdown-body""#), "{node}");
        assert!(node.contains("<h1>Rendered"), "{node}");
        assert!(node.contains("<strong>safe"), "{node}");
        assert!(node.contains("&lt;script&gt;"), "{node}");
        assert!(!node.contains("<script>"), "{node}");
        assert!(!node.contains("href=\"javascript:"), "{node}");
    }

    #[test]
    fn markdown_renderer_supports_all_simple_nodes() {
        let text = |value: &str| MarkdownNode::Text {
            text: value.to_owned(),
        };
        let mut nodes = (1..=6)
            .map(|level| MarkdownNode::Heading {
                level,
                children: vec![text(&format!("heading-{level}"))],
            })
            .collect::<Vec<_>>();
        nodes.extend([
            MarkdownNode::Paragraph {
                children: vec![
                    MarkdownNode::Emphasis {
                        children: vec![text("emphasis")],
                    },
                    MarkdownNode::Strong {
                        children: vec![text("strong")],
                    },
                    MarkdownNode::Strikethrough {
                        children: vec![text("deleted")],
                    },
                    MarkdownNode::InlineCode {
                        code: "inline".to_owned(),
                    },
                    MarkdownNode::Link {
                        destination: "https://example.com".to_owned(),
                        children: vec![text("link")],
                    },
                    MarkdownNode::LineBreak,
                ],
            },
            MarkdownNode::UnorderedList {
                items: vec![vec![text("bullet")]],
            },
            MarkdownNode::OrderedList {
                start: 3,
                items: vec![vec![text("ordered")]],
            },
            MarkdownNode::BlockQuote {
                children: vec![MarkdownNode::Paragraph {
                    children: vec![text("quote")],
                }],
            },
            MarkdownNode::CodeBlock {
                language: Some("rust".to_owned()),
                code: "fn main() {}".to_owned(),
                highlights: Vec::new(),
            },
            MarkdownNode::ThematicBreak,
        ]);

        let rendered = view! {
            <div>{render_markdown_nodes(nodes)}</div>
        }
        .to_html();

        for tag in [
            "<h1",
            "<h2",
            "<h3",
            "<h4",
            "<h5",
            "<h6",
            "<em",
            "<strong",
            "<del",
            "<code",
            "<a",
            "<br",
            "<ul",
            "<ol",
            "<blockquote",
            "<pre",
            "<hr",
        ] {
            assert!(rendered.contains(tag), "missing {tag}: {rendered}");
        }
        assert!(rendered.contains(r#"href="https://example.com""#));
        assert!(rendered.contains(r#"start="3""#));
        assert!(rendered.contains(r#"data-language="rust""#));
    }

    #[test]
    fn code_highlight_kinds_map_to_css_classes() {
        assert_eq!(CodeHighlightKind::Plain.class(), None);
        for (kind, class) in [
            (CodeHighlightKind::Comment, "syntax-comment"),
            (CodeHighlightKind::Constant, "syntax-constant"),
            (CodeHighlightKind::Function, "syntax-function"),
            (CodeHighlightKind::Keyword, "syntax-keyword"),
            (CodeHighlightKind::Number, "syntax-number"),
            (CodeHighlightKind::Operator, "syntax-operator"),
            (CodeHighlightKind::Property, "syntax-property"),
            (CodeHighlightKind::String, "syntax-string"),
            (CodeHighlightKind::Type, "syntax-type"),
            (CodeHighlightKind::Variable, "syntax-variable"),
        ] {
            assert_eq!(kind.class(), Some(class));
        }
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
    fn tool_input_defaults_to_an_html_safe_nested_list() {
        let input = view! {
            <ToolInput input=serde_json::json!({
                "<command>": "printf </script>",
                "nested": {
                    "slash/key": [true, null],
                },
            })/>
        }
        .to_html();

        assert!(input.contains(r#"role="group" aria-label="Input view""#));
        assert!(input.contains(r#">List</button>"#));
        assert!(input.contains(r#">Raw JSON</button>"#));
        assert!(input.contains(r#"aria-pressed="true""#), "{input}");
        assert!(input.contains("tool-input-list tool-input-list-root"));
        assert!(input.contains(r#"data-json-pointer="/nested/slash~1key""#));
        assert!(input.contains("&lt;command&gt;"));
        assert!(input.contains("printf &lt;/script&gt;"));
        assert!(!input.contains("<script>"));
        assert!(!input.contains("tool-input-raw"));
    }

    #[test]
    fn tool_input_cmd_renders_highlighted_shell_without_changing_the_command() {
        let tokens = vec![
            ShellHighlightToken {
                kind: ShellHighlightKind::Keyword,
                text: "if".to_owned(),
            },
            ShellHighlightToken {
                kind: ShellHighlightKind::Plain,
                text: " ".to_owned(),
            },
            ShellHighlightToken {
                kind: ShellHighlightKind::Command,
                text: "printf".to_owned(),
            },
            ShellHighlightToken {
                kind: ShellHighlightKind::Plain,
                text: " ".to_owned(),
            },
            ShellHighlightToken {
                kind: ShellHighlightKind::Option,
                text: "-v".to_owned(),
            },
            ShellHighlightToken {
                kind: ShellHighlightKind::Plain,
                text: " ".to_owned(),
            },
            ShellHighlightToken {
                kind: ShellHighlightKind::String,
                text: "'</script>'".to_owned(),
            },
            ShellHighlightToken {
                kind: ShellHighlightKind::Plain,
                text: " ".to_owned(),
            },
            ShellHighlightToken {
                kind: ShellHighlightKind::Variable,
                text: "$HOME".to_owned(),
            },
            ShellHighlightToken {
                kind: ShellHighlightKind::Plain,
                text: " ".to_owned(),
            },
            ShellHighlightToken {
                kind: ShellHighlightKind::Operator,
                text: "|".to_owned(),
            },
            ShellHighlightToken {
                kind: ShellHighlightKind::Plain,
                text: " ".to_owned(),
            },
            ShellHighlightToken {
                kind: ShellHighlightKind::Comment,
                text: "# inspect".to_owned(),
            },
        ];
        let command = tokens
            .iter()
            .map(|token| token.text.as_str())
            .collect::<String>();
        let input = view! {
            <ToolInput
                input=serde_json::json!({
                    "cmd": command,
                    "tty": false,
                })
                shell_highlights=vec![ToolInputShellHighlight {
                    tool_use_index: 0,
                    tool_use_id: "exec".to_owned(),
                    input_pointer: "/cmd".to_owned(),
                    tokens,
                }]
            />
        }
        .to_html();
        for class in [
            "shell-command",
            "shell-option",
            "shell-string",
            "shell-variable",
            "shell-operator",
            "shell-comment",
            "shell-keyword",
        ] {
            assert!(input.contains(&format!(r#"class="{class}""#)), "{input}");
        }
        assert!(input.contains(r#"aria-label="Shell command""#));
        assert!(input.contains("&lt;/script&gt;"));
        assert!(!input.contains("<script>"));
    }

    #[test]
    fn tool_input_link_targets_only_the_matching_tool_item_and_pointer() {
        let detail = view! {
            <NodeDetailContent response=NodeDetailResponse::Found {
                node: Box::new(test_node(Kind::tool_uses(vec![
                    ToolUse {
                        id: "write-1".to_owned(),
                        name: "write_stdin".to_owned(),
                        input: serde_json::json!({"session_id": "exec-1"}),
                    },
                    ToolUse {
                        id: "write-2".to_owned(),
                        name: "write_stdin".to_owned(),
                        input: serde_json::json!({"session_id": "exec-1"}),
                    },
                ]))),
                parent_graph_links: BTreeMap::new(),
                markdown_documents: Vec::new(),
                tool_use_input_links: vec![ToolUseInputLink {
                    tool_use_index: 1,
                    tool_use_id: "write-2".to_owned(),
                    input_pointer: "/session_id".to_owned(),
                    value: "exec-1".to_owned(),
                    target: "detail-exec-node".to_owned(),
                }],
                tool_input_shell_highlights: Vec::new(),
                tool_input_json_highlights: Vec::new(),
            }/>
        }
        .to_html();

        assert_eq!(
            detail.matches(r#"data-json-pointer="/session_id""#).count(),
            2
        );
        assert_eq!(
            detail.matches(r#"class="tool-input-value-link""#).count(),
            1
        );
        assert!(detail.contains(r##"href="#detail-exec-node""##));
        assert!(detail.contains(r#"title="Open exec_command""#));
    }

    #[test]
    fn raw_tool_input_renders_server_json_highlights_and_escapes_html() {
        let value = serde_json::json!({
            "<key>": "</script>",
            "number": 42,
            "boolean": false,
            "nothing": null,
            "escaped": "line\nbreak",
        });
        let source = pretty_json(&value);
        let mut ranges = vec![
            test_json_range(&source, "\"<key>\"", JsonHighlightKind::Key),
            test_json_range(&source, "\"</script>\"", JsonHighlightKind::String),
            test_json_range(&source, "42", JsonHighlightKind::Number),
            test_json_range(&source, "false", JsonHighlightKind::Boolean),
            test_json_range(&source, "null", JsonHighlightKind::Null),
            test_json_range(&source, "\\n", JsonHighlightKind::Escape),
        ];
        ranges.sort_by_key(|range| range.start);
        let raw = view! {
            <ToolInputRaw
                input=value
                ranges
            />
        }
        .to_html();

        for class in [
            "json-key",
            "json-string",
            "json-number",
            "json-boolean",
            "json-null",
            "json-escape",
        ] {
            assert!(raw.contains(&format!(r#"class="{class}""#)));
        }
        assert!(raw.contains("&lt;key&gt;"));
        assert!(raw.contains("&lt;/script&gt;"));
        assert!(!raw.contains("<script>"));
    }

    #[test]
    fn raw_tool_input_falls_back_to_plain_json_for_invalid_ranges() {
        let value = serde_json::json!({"unsafe": "</script>"});
        let raw = view! {
            <ToolInputRaw
                input=value
                ranges=vec![JsonHighlightRange {
                    kind: JsonHighlightKind::String,
                    start: 1,
                    end: usize::MAX,
                }]
            />
        }
        .to_html();

        assert!(raw.contains("&lt;/script&gt;"));
        assert!(!raw.contains("<script>"));
        assert!(!raw.contains("json-string"));
    }

    #[test]
    fn tool_input_json_pointers_follow_rfc_6901_escaping() {
        assert_eq!(json_pointer_child("", "slash/key"), "/slash~1key");
        assert_eq!(
            json_pointer_child("/slash~1key", "~nested"),
            "/slash~1key/~0nested"
        );
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

    use crate::api::ProviderContextNode;
    use any_spawner::Executor;
    use js_sys::Promise;
    use leptos::leptos_dom::helpers::request_animation_frame;
    use wasm_bindgen::{JsCast, JsValue, UnwrapThrowExt};
    use wasm_bindgen_futures::JsFuture;
    use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

    wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    async fn graph_items_provider_context_scroll_loads_a_deferred_row() {
        _ = Executor::init_wasm_bindgen();
        let window = web_sys::window().expect_throw("window should be available");
        let document = window
            .document()
            .expect_throw("document should be available");
        let root = document
            .create_element("div")
            .expect_throw("test root should be created")
            .unchecked_into::<web_sys::HtmlElement>();
        root.set_id("provider-context-scroll-test");
        let style = document
            .create_element("style")
            .expect_throw("test style should be created");
        style.set_text_content(Some(
            "#provider-context-scroll-test .provider-context-section { display: block; height: 48px; overflow-y: auto; }\
             #provider-context-scroll-test .provider-context-node { height: 48px; }",
        ));
        document
            .body()
            .expect_throw("document body should be available")
            .append_child(&style)
            .expect_throw("test style should be mounted");
        document
            .body()
            .expect_throw("document body should be available")
            .append_child(&root)
            .expect_throw("test root should be mounted");
        let node_ids = (0..4)
            .map(|index| format!("node-{index}"))
            .collect::<Vec<_>>();
        let initial_items = node_ids[..3]
            .iter()
            .map(|node_id| ProviderContextItem {
                node: ProviderContextNode {
                    id: node_id.clone(),
                    short_id: node_id.clone(),
                    kind: "text".to_owned(),
                    role: "user".to_owned(),
                    created_at: "2026-08-01T00:00:00Z".to_owned(),
                    summary: format!("Summary for {node_id}"),
                },
                point: None,
            })
            .collect();
        let mounted = leptos::mount::mount_to(root.clone(), move || {
            view! {
                <ProviderContextContent
                    graph_mode="all".to_owned()
                    payload=ProviderContextPayload {
                        response: ProviderContextResponse::Found {
                            context_target: "detail-context".to_owned(),
                            previous_context_target: None,
                            selected_id: "node-0".to_owned(),
                            node_ids,
                            branches: Vec::new(),
                        },
                        items: initial_items,
                    }
                />
            }
        });
        next_animation_frame().await;
        next_animation_frame().await;

        let section = root
            .query_selector(".provider-context-section")
            .expect_throw("provider context query should succeed")
            .expect_throw("provider context should be rendered")
            .unchecked_into::<web_sys::HtmlElement>();
        let deferred = root
            .query_selector(".provider-context-node:last-child")
            .expect_throw("deferred row query should succeed")
            .expect_throw("deferred row should be rendered");
        assert!(
            deferred
                .text_content()
                .unwrap_or_default()
                .contains("Scroll to load node summary...")
        );

        section.set_scroll_top(section.scroll_height());
        section
            .dispatch_event(&web_sys::Event::new("scroll").expect_throw("event should build"))
            .expect_throw("scroll should dispatch");
        for _ in 0..4 {
            next_animation_frame().await;
        }
        assert!(
            !deferred
                .text_content()
                .unwrap_or_default()
                .contains("Scroll to load node summary...")
        );

        drop(mounted);
        root.remove();
        style.remove();
    }

    #[wasm_bindgen_test]
    async fn graph_items_tool_input_switches_between_list_and_raw_json() {
        let window = web_sys::window().expect_throw("window should be available");
        let document = window
            .document()
            .expect_throw("document should be available");
        let root = document
            .create_element("div")
            .expect_throw("test root should be created")
            .unchecked_into::<web_sys::HtmlElement>();
        document
            .body()
            .expect_throw("document body should be available")
            .append_child(&root)
            .expect_throw("test root should be mounted");
        let input = serde_json::json!({
            "command": "true",
            "timeout": 30,
            "enabled": true,
            "optional": null,
        });
        let source = pretty_json(&input);
        let boolean_start = source
            .rfind("true")
            .expect("test JSON boolean should exist");
        let mut json_highlight_ranges = vec![
            test_json_range(&source, "\"command\"", JsonHighlightKind::Key),
            test_json_range(&source, "\"true\"", JsonHighlightKind::String),
            test_json_range(&source, "30", JsonHighlightKind::Number),
            JsonHighlightRange {
                kind: JsonHighlightKind::Boolean,
                start: boolean_start,
                end: boolean_start + "true".len(),
            },
            test_json_range(&source, "null", JsonHighlightKind::Null),
        ];
        json_highlight_ranges.sort_by_key(|range| range.start);
        let mounted = leptos::mount::mount_to(root.clone(), move || {
            view! {
                <ToolInput input json_highlight_ranges
                />
            }
        });
        let list_toggle = root
            .query_selector(".tool-input-view-toggle button:first-child")
            .expect_throw("list toggle query should succeed")
            .expect_throw("list toggle should be rendered")
            .unchecked_into::<web_sys::HtmlElement>();
        let raw_toggle = root
            .query_selector(".tool-input-view-toggle button:last-child")
            .expect_throw("raw toggle query should succeed")
            .expect_throw("raw toggle should be rendered")
            .unchecked_into::<web_sys::HtmlElement>();

        assert_eq!(
            list_toggle.get_attribute("aria-pressed").as_deref(),
            Some("true")
        );
        assert!(
            root.query_selector(".tool-input-list")
                .unwrap_throw()
                .is_some()
        );
        assert!(
            root.query_selector(".tool-input-raw")
                .unwrap_throw()
                .is_none()
        );

        raw_toggle.click();
        next_task().await;

        assert_eq!(
            raw_toggle.get_attribute("aria-pressed").as_deref(),
            Some("true")
        );
        assert!(
            root.query_selector(".tool-input-list")
                .unwrap_throw()
                .is_none()
        );
        for selector in [
            ".tool-input-raw .json-key",
            ".tool-input-raw .json-string",
            ".tool-input-raw .json-number",
            ".tool-input-raw .json-boolean",
            ".tool-input-raw .json-null",
        ] {
            assert!(root.query_selector(selector).unwrap_throw().is_some());
        }

        list_toggle.click();
        next_task().await;

        assert_eq!(
            list_toggle.get_attribute("aria-pressed").as_deref(),
            Some("true")
        );
        assert!(
            root.query_selector(".tool-input-list")
                .unwrap_throw()
                .is_some()
        );
        assert!(
            root.query_selector(".tool-input-raw")
                .unwrap_throw()
                .is_none()
        );

        drop(mounted);
        root.remove();
    }

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
        let node_selection = use_panel_selection(PanelSelection::default());
        let context_selection = use_panel_selection(PanelSelection::default());
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
