use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{ConnectInfo, Path, RawQuery, Request, State};
use axum::http::{HeaderValue, Method, StatusCode, Uri, header};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use coco_mem::{Kind, Node, Store, StoreError, ToolSessionStore};
use futures_util::{StreamExt, stream};
use leptos::prelude::provide_context;
use leptos_axum::handle_server_fns_with_context;
use serde::Serialize;
use snafu::prelude::*;

use super::CLIENT_ASSET_VERSION;
use super::config::ConsoleConfig;
use super::error::{
    BindConsoleSnafu, ConfigureConsoleSocketSnafu, JoinConsoleServerSnafu, ServeConsoleSnafu,
    StoreSnafu,
};
use super::publisher::ConsolePublisher;
use super::render::render_index_page;
use crate::Result;
use crate::api::{
    AnchorRangeNode, AnchorRangePath, AnchorRangeResponse, GraphViewportEdgeKind,
    NodeDetailResponse, Point as ApiPoint, ProviderContextItem, ProviderContextNode,
    ProviderContextResponse, ToolUseInputLink,
};
use crate::host::api::{GraphViewportDiffRequest, GraphViewportKnownItems, GraphViewportRequest};
use crate::host::web_graph_runtime::WebGraphRuntime;
use crate::host::web_graph_view::{
    NodeView, ViewMode, node_id_from_target, provider_context_for_node, tool_use_input_links,
    write_stdin_session_ids,
};

const STYLE_CSS: &str = include_str!("style.css");
const THIRD_PARTY_NOTICES: &str = include_str!("../../../THIRD_PARTY_NOTICES.html");
const COCO_CONSOLE_JS: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/pkg/coco_console.js"));
const COCO_CONSOLE_WASM: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/pkg/coco_console_bg.wasm"));

#[derive(Clone)]
struct AppState<S> {
    store: S,
    web_graph: WebGraphRuntime,
}

#[async_trait]
trait PanelDataSource: Send + Sync {
    async fn node_detail(&self, target: String) -> Result<NodeDetailResponse>;

    async fn anchor_range(
        &self,
        source: String,
        target: String,
        kind: GraphViewportEdgeKind,
    ) -> Result<AnchorRangeResponse>;

    async fn provider_context(
        &self,
        target: String,
        context: Option<String>,
        graph_mode: String,
    ) -> Result<ProviderContextResponse>;
}

#[derive(Clone)]
pub struct PanelServerContext {
    source: Arc<dyn PanelDataSource>,
}

impl PanelServerContext {
    fn new<S>(state: AppState<S>) -> Self
    where
        S: Store + Clone + Send + Sync + 'static,
    {
        Self {
            source: Arc::new(state),
        }
    }

    pub async fn node_detail(&self, target: String) -> Result<NodeDetailResponse> {
        self.source.node_detail(target).await
    }

    pub async fn anchor_range(
        &self,
        source: String,
        target: String,
        kind: GraphViewportEdgeKind,
    ) -> Result<AnchorRangeResponse> {
        self.source.anchor_range(source, target, kind).await
    }

    pub async fn provider_context(
        &self,
        target: String,
        context: Option<String>,
        graph_mode: String,
    ) -> Result<ProviderContextResponse> {
        self.source
            .provider_context(target, context, graph_mode)
            .await
    }
}

#[async_trait]
impl<S> PanelDataSource for AppState<S>
where
    S: Store + Clone + Send + Sync + 'static,
{
    async fn node_detail(&self, target: String) -> Result<NodeDetailResponse> {
        load_node_detail(self, &target).await
    }

    async fn anchor_range(
        &self,
        source: String,
        target: String,
        kind: GraphViewportEdgeKind,
    ) -> Result<AnchorRangeResponse> {
        load_anchor_range(self, &source, &target, kind).await
    }

    async fn provider_context(
        &self,
        target: String,
        context: Option<String>,
        graph_mode: String,
    ) -> Result<ProviderContextResponse> {
        load_provider_context(
            self,
            &target,
            context.as_deref(),
            view_mode_from_value(&graph_mode),
        )
        .await
    }
}

#[derive(Debug)]
pub struct ConsoleServerHandle {
    addr: SocketAddr,
    task: tokio::task::JoinHandle<Result<()>>,
}

impl ConsoleServerHandle {
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub async fn wait(self) -> Result<()> {
        let mut handle = self;
        handle.wait_mut().await
    }

    pub async fn wait_mut(&mut self) -> Result<()> {
        (&mut self.task).await.context(JoinConsoleServerSnafu)?
    }

    pub async fn shutdown(self) -> Result<()> {
        self.task.abort();
        match self.task.await {
            Ok(result) => result,
            Err(source) if source.is_cancelled() => Ok(()),
            Err(source) => Err(source).context(JoinConsoleServerSnafu),
        }
    }
}

pub async fn start_console_server_with_graph_store_path<S>(
    config: ConsoleConfig,
    store: S,
    publisher: ConsolePublisher,
    graph_store_path: PathBuf,
) -> Result<ConsoleServerHandle>
where
    S: Store + Clone + Send + Sync + 'static,
{
    let web_graph = WebGraphRuntime::open(graph_store_path, publisher).await?;
    let source_changes = web_graph.subscribe_source_changes();
    let listener =
        TcpListener::bind(config.addr).context(BindConsoleSnafu { addr: config.addr })?;
    listener
        .set_nonblocking(true)
        .context(ConfigureConsoleSocketSnafu { addr: config.addr })?;
    let listener = tokio::net::TcpListener::from_std(listener)
        .context(ConfigureConsoleSocketSnafu { addr: config.addr })?;
    let addr = listener
        .local_addr()
        .context(ConfigureConsoleSocketSnafu { addr: config.addr })?;
    let state = AppState { store, web_graph };
    let task = tokio::spawn(async move {
        serve_console(listener, state, source_changes)
            .await
            .context(ServeConsoleSnafu { addr })
    });

    Ok(ConsoleServerHandle { addr, task })
}

async fn serve_console<S>(
    listener: tokio::net::TcpListener,
    state: AppState<S>,
    source_changes: tokio::sync::watch::Receiver<u64>,
) -> io::Result<()>
where
    S: Store + Clone + Send + Sync + 'static,
{
    let web_graph = state.web_graph.clone();
    let server = axum::serve(
        listener,
        router(state).into_make_service_with_connect_info::<SocketAddr>(),
    );
    tokio::select! {
        result = server => result,
        never = web_graph.drive(source_changes) => match never {},
    }
}

fn router<S>(state: AppState<S>) -> Router
where
    S: Store + Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/", get(index_page::<S>).post(method_not_allowed))
        .route("/index.html", get(index_page::<S>))
        .route("/style.css", get(style_css))
        .route("/third-party-notices.html", get(third_party_notices))
        .route("/api/graph/viewport", get(graph_viewport::<S>))
        .route(
            "/api/graph/viewport/items/diff",
            get(graph_viewport_items_diff_get::<S>).post(graph_viewport_items_diff_post::<S>),
        )
        .route(
            "/api/graph/viewport/diff",
            get(graph_viewport_diff_get::<S>).post(graph_viewport_diff_post::<S>),
        )
        .route("/api/panels/{*fn_name}", get(panel_server_function::<S>))
        .route("/api/node-detail", get(node_detail::<S>))
        .route("/api/provider-context", get(provider_context::<S>))
        .route("/events", get(event_stream::<S>))
        .route("/pkg/{version}/coco_console.js", get(client_js))
        .route("/pkg/{version}/coco_console_bg.wasm", get(client_wasm))
        .fallback(not_found)
        .with_state(state)
        .layer(middleware::from_fn(access_log))
}

async fn panel_server_function<S>(State(state): State<AppState<S>>, request: Request) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    let context = PanelServerContext::new(state);
    handle_server_fns_with_context(move || provide_context(context.clone()), request)
        .await
        .into_response()
}

async fn access_log(
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    method: Method,
    uri: Uri,
    request: Request,
    next: Next,
) -> Response {
    let started_at = Instant::now();
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    tracing::info!(
        %peer_addr,
        method = %method,
        path = uri.path(),
        status = response.status().as_u16(),
        duration_ms = started_at.elapsed().as_millis(),
        "console access"
    );
    response
}

async fn index_page<S>(State(state): State<AppState<S>>, RawQuery(query): RawQuery) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    let query = parse_query(query.as_deref().unwrap_or_default());
    html_response(render_index_page(
        view_mode_from_query(&query),
        state.web_graph.current_revision(),
    ))
}

async fn style_css() -> Response {
    response_with_body(
        StatusCode::OK,
        "text/css; charset=utf-8",
        Body::from(STYLE_CSS),
    )
}

async fn third_party_notices() -> Response {
    response_with_body(
        StatusCode::OK,
        "text/html; charset=utf-8",
        Body::from(THIRD_PARTY_NOTICES),
    )
}

async fn graph_viewport<S>(State(state): State<AppState<S>>, RawQuery(query): RawQuery) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    let query = parse_query(query.as_deref().unwrap_or_default());
    let mode = view_mode_from_query(&query);
    let request = viewport_request_from_query(&query);
    let response = match query.version() {
        Some(version) => state.web_graph.viewport_after(mode, version, request).await,
        None => state.web_graph.viewport(mode, request).await,
    };
    match response {
        Ok(response) => json_response(&response, "graph viewport"),
        Err(error) => plain_error(error.to_string()),
    }
}

async fn graph_viewport_diff_get<S>(
    State(state): State<AppState<S>>,
    RawQuery(query): RawQuery,
) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    graph_viewport_diff_response(state, parse_query(query.as_deref().unwrap_or_default())).await
}

async fn graph_viewport_diff_post<S>(
    State(state): State<AppState<S>>,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    let mut query = parse_query(query.as_deref().unwrap_or_default());
    query
        .pairs
        .extend(parse_query(&String::from_utf8_lossy(&body)).pairs);
    graph_viewport_diff_response(state, query).await
}

async fn graph_viewport_items_diff_get<S>(
    State(state): State<AppState<S>>,
    RawQuery(query): RawQuery,
) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    graph_viewport_items_diff_response(state, parse_query(query.as_deref().unwrap_or_default()))
        .await
}

async fn graph_viewport_items_diff_post<S>(
    State(state): State<AppState<S>>,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    let mut query = parse_query(query.as_deref().unwrap_or_default());
    query
        .pairs
        .extend(parse_query(&String::from_utf8_lossy(&body)).pairs);
    graph_viewport_items_diff_response(state, query).await
}

async fn graph_viewport_diff_response<S>(state: AppState<S>, query: QueryParams) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    match state
        .web_graph
        .viewport_diff(
            view_mode_from_query(&query),
            viewport_diff_request_from_query(&query),
        )
        .await
    {
        Ok(response) => json_response(&response, "graph viewport diff"),
        Err(error) => plain_error(error.to_string()),
    }
}

async fn graph_viewport_items_diff_response<S>(state: AppState<S>, query: QueryParams) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    let Some(version) = query.version() else {
        return graph_viewport_diff_response(state, query).await;
    };
    match state
        .web_graph
        .viewport_diff_after(
            view_mode_from_query(&query),
            version,
            viewport_diff_request_from_query(&query),
        )
        .await
    {
        Ok(response) => json_response(&response, "graph viewport items diff"),
        Err(error) => plain_error(error.to_string()),
    }
}

async fn node_detail<S>(State(state): State<AppState<S>>, RawQuery(query): RawQuery) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    let query = parse_query(query.as_deref().unwrap_or_default());
    let Some(target) = query.get("target") else {
        return json_response(&NodeDetailResponse::Default, "node detail");
    };
    match load_node_detail(&state, target).await {
        Ok(response) => json_response(&response, "node detail"),
        Err(error) => plain_error(error.to_string()),
    }
}

async fn load_node_detail<S>(state: &AppState<S>, target: &str) -> Result<NodeDetailResponse>
where
    S: Store + Clone + Send + Sync + 'static,
{
    let Some(node_id) = node_id_from_target(target) else {
        return Ok(NodeDetailResponse::Missing {
            target: target.to_owned(),
        });
    };
    match state.store.get_node(node_id).await {
        Ok(node) => {
            let tool_use_input_links =
                resolve_tool_use_input_links(&state.web_graph, &node).await?;
            let highlights = super::syntax_highlight::tool_input_syntax_highlights(&node);
            Ok(NodeDetailResponse::Found {
                node: Box::new(node),
                tool_use_input_links,
                tool_input_shell_highlights: highlights.shell,
                tool_input_json_highlights: highlights.json,
            })
        }
        Err(error) if is_missing_node(&error) => Ok(NodeDetailResponse::Missing {
            target: target.to_owned(),
        }),
        Err(source) => Err(source).context(StoreSnafu),
    }
}

async fn resolve_tool_use_input_links<S>(store: &S, node: &Node) -> Result<Vec<ToolUseInputLink>>
where
    S: ToolSessionStore + Sync,
{
    let mut exec_node_ids_by_session = HashMap::new();
    let mut resolved_sessions = HashSet::new();
    for session_id in write_stdin_session_ids(node) {
        if !resolved_sessions.insert(session_id) {
            continue;
        }
        if let Some(exec_node_id) = store
            .find_exec_command_node_id_for_session(&node.id, session_id)
            .await
            .context(StoreSnafu)?
        {
            exec_node_ids_by_session.insert(session_id.to_owned(), exec_node_id);
        }
    }
    Ok(tool_use_input_links(node, &exec_node_ids_by_session))
}

async fn load_anchor_range<S>(
    state: &AppState<S>,
    source_id: &str,
    target_id: &str,
    kind: GraphViewportEdgeKind,
) -> Result<AnchorRangeResponse>
where
    S: Store + Clone + Send + Sync + 'static,
{
    let endpoint_ids = [source_id.to_owned(), target_id.to_owned()];
    let edges = state
        .web_graph
        .incident_edges(ViewMode::Anchors, &endpoint_ids)
        .await?;
    if !edges
        .iter()
        .any(|edge| edge.kind == kind && edge.source_id == source_id && edge.target_id == target_id)
    {
        return Ok(AnchorRangeResponse::Missing);
    }

    let Some(paths) = anchor_range_paths(state, source_id, target_id, kind).await? else {
        return Ok(AnchorRangeResponse::Missing);
    };
    Ok(AnchorRangeResponse::Found { paths })
}

async fn anchor_range_paths<S>(
    state: &AppState<S>,
    source_id: &str,
    target_id: &str,
    displayed_kind: GraphViewportEdgeKind,
) -> Result<Option<Vec<AnchorRangePath>>>
where
    S: Store + Clone + Send + Sync + 'static,
{
    let target = match state.store.get_node(target_id).await {
        Ok(node) => node,
        Err(error) if is_missing_node(&error) => return Ok(None),
        Err(source) => return Err(source).context(StoreSnafu),
    };
    let Kind::Anchor(anchor) = &target.kind else {
        return Ok(None);
    };
    let mut parents = Vec::with_capacity(anchor.merge_parents().len() + 1);
    if !target.parent.is_empty() {
        parents.push((GraphViewportEdgeKind::Primary, target.parent.clone()));
    }
    parents.extend(anchor.merge_parents().iter().map(|parent| {
        (
            if parent.is_shadow() {
                GraphViewportEdgeKind::Shadow
            } else {
                GraphViewportEdgeKind::Merge
            },
            parent.node_id().to_owned(),
        )
    }));

    let mut paths = Vec::new();
    for (kind, parent_id) in parents {
        let Some(nodes) = load_primary_path(state, source_id, &parent_id).await? else {
            continue;
        };
        if nodes[1..]
            .iter()
            .any(|node| node.kind.anchor_payload_kind().is_some())
        {
            continue;
        }
        let mut nodes = nodes
            .into_iter()
            .enumerate()
            .map(|(index, node)| {
                anchor_range_node(node, (index > 0).then_some(GraphViewportEdgeKind::Primary))
            })
            .collect::<Vec<_>>();
        nodes.push(anchor_range_node(target.clone(), Some(kind)));
        paths.push(AnchorRangePath { nodes });
    }
    let first_kind = paths
        .first()
        .and_then(|path| path.nodes.last())
        .and_then(|node| node.incoming_edge);
    if first_kind != Some(displayed_kind) {
        return Ok(None);
    }
    Ok(Some(paths))
}

async fn load_primary_path<S>(
    state: &AppState<S>,
    source_id: &str,
    target_id: &str,
) -> Result<Option<Vec<Node>>>
where
    S: Store + Clone + Send + Sync + 'static,
{
    let mut nodes = match state.store.log(source_id, target_id).await {
        Ok(nodes) => nodes,
        Err(error)
            if is_missing_node(&error) || matches!(error, StoreError::RefsNotConnected { .. }) =>
        {
            return Ok(None);
        }
        Err(source) => return Err(source).context(StoreSnafu),
    };
    nodes.reverse();
    if nodes.is_empty()
        || nodes.first().is_none_or(|node| node.id != source_id)
        || nodes.last().is_none_or(|node| node.id != target_id)
        || nodes
            .first()
            .is_none_or(|node| node.kind.anchor_payload_kind().is_none())
    {
        return Ok(None);
    }
    Ok(Some(nodes))
}

fn anchor_range_node(
    node: coco_mem::Node,
    incoming_edge: Option<GraphViewportEdgeKind>,
) -> AnchorRangeNode {
    let node = NodeView::from(&node);
    AnchorRangeNode {
        id: node.id,
        short_id: node.short_id,
        kind: node.kind,
        role: node.role,
        summary: node.summary,
        incoming_edge,
    }
}

async fn provider_context<S>(
    State(state): State<AppState<S>>,
    RawQuery(query): RawQuery,
) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    let query = parse_query(query.as_deref().unwrap_or_default());
    let Some(target) = query.get("target") else {
        return json_response(&ProviderContextResponse::Default, "provider context");
    };
    match load_provider_context(
        &state,
        target,
        query.get("context"),
        view_mode_from_query(&query),
    )
    .await
    {
        Ok(response) => json_response(&response, "provider context"),
        Err(error) => plain_error(error.to_string()),
    }
}

async fn load_provider_context<S>(
    state: &AppState<S>,
    target: &str,
    context: Option<&str>,
    view_mode: ViewMode,
) -> Result<ProviderContextResponse>
where
    S: Store + Clone + Send + Sync + 'static,
{
    let Some(node_id) = node_id_from_target(target) else {
        return Ok(ProviderContextResponse::Missing {
            target: target.to_owned(),
        });
    };
    let node = match state.store.get_node(node_id).await {
        Ok(node) => node,
        Err(error) if is_missing_node(&error) => {
            return Ok(ProviderContextResponse::Missing {
                target: target.to_owned(),
            });
        }
        Err(source) => return Err(source).context(StoreSnafu),
    };
    let selection = provider_context_for_node(&state.store, &node.id, context).await?;
    let Some(selection) = selection else {
        return Ok(ProviderContextResponse::Found { items: Vec::new() });
    };
    let node_ids = selection
        .context
        .nodes
        .iter()
        .map(|node| node.node.id.clone())
        .collect::<Vec<_>>();
    let points = state.web_graph.node_points(view_mode, &node_ids).await?;
    let items = selection
        .context
        .nodes
        .into_iter()
        .map(|node| ProviderContextItem {
            context_target: selection.context.id.clone(),
            selected: node.node.id == selection.selected_id,
            point: points.get(&node.node.id).map(|point| ApiPoint {
                x: point.x,
                y: point.y,
            }),
            node: provider_context_node(node.node),
        })
        .collect();
    Ok(ProviderContextResponse::Found { items })
}

fn provider_context_node(node: NodeView) -> ProviderContextNode {
    ProviderContextNode {
        id: node.id,
        short_id: node.short_id,
        kind: node.kind,
        role: node.role,
        created_at: node.created_at,
        summary: node.summary,
    }
}

fn is_missing_node(error: &StoreError) -> bool {
    matches!(
        error,
        StoreError::NotFound { .. } | StoreError::AmbiguousNodePrefix { .. }
    )
}

async fn event_stream<S>(State(state): State<AppState<S>>) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    let mut revisions = state.web_graph.subscribe();
    let current = *revisions.borrow_and_update();
    let initial = stream::once(async move {
        Ok::<_, Infallible>(Event::default().event("graph").data(current.to_string()))
    });
    let changes = stream::unfold(revisions, |mut revisions| async move {
        revisions.changed().await.ok()?;
        let revision = *revisions.borrow_and_update();
        Some((
            Ok::<_, Infallible>(Event::default().event("graph").data(revision.to_string())),
            revisions,
        ))
    });
    Sse::new(initial.chain(changes)).into_response()
}

async fn client_js(Path(version): Path<String>) -> Response {
    if version != CLIENT_ASSET_VERSION {
        return not_found().await;
    }
    response_with_body(
        StatusCode::OK,
        "text/javascript; charset=utf-8",
        Body::from(COCO_CONSOLE_JS),
    )
}

async fn client_wasm(Path(version): Path<String>) -> Response {
    if version != CLIENT_ASSET_VERSION {
        return not_found().await;
    }
    response_with_body(
        StatusCode::OK,
        "application/wasm",
        Body::from(COCO_CONSOLE_WASM),
    )
}

async fn method_not_allowed() -> Response {
    response_with_body(
        StatusCode::METHOD_NOT_ALLOWED,
        "text/plain; charset=utf-8",
        Body::from("method not allowed"),
    )
}

async fn not_found() -> Response {
    response_with_body(
        StatusCode::NOT_FOUND,
        "text/plain; charset=utf-8",
        Body::from("not found"),
    )
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct QueryParams {
    pairs: Vec<(String, String)>,
}

impl QueryParams {
    fn get(&self, key: &str) -> Option<&str> {
        self.pairs
            .iter()
            .find_map(|(candidate, value)| (candidate == key).then_some(value.as_str()))
    }

    fn get_all(&self, key: &str) -> Vec<String> {
        self.pairs
            .iter()
            .filter_map(|(candidate, value)| (candidate == key).then_some(value.clone()))
            .collect()
    }

    fn contains_key(&self, key: &str) -> bool {
        self.pairs.iter().any(|(candidate, _)| candidate == key)
    }

    fn i32(&self, key: &str) -> Option<i32> {
        self.get(key)?.parse().ok()
    }

    fn u64(&self, key: &str) -> Option<u64> {
        self.get(key)?.parse().ok()
    }

    fn version(&self) -> Option<u64> {
        self.u64("version")
    }
}

fn parse_query(query: &str) -> QueryParams {
    QueryParams {
        pairs: query
            .split('&')
            .filter(|part| !part.is_empty())
            .filter_map(|part| {
                let (key, value) = part.split_once('=')?;
                Some((percent_decode(key), percent_decode(value)))
            })
            .collect(),
    }
}

fn percent_decode(value: &str) -> String {
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

fn decode_hex_pair(high: u8, low: u8) -> Option<u8> {
    Some(decode_hex_digit(high)? << 4 | decode_hex_digit(low)?)
}

fn decode_hex_digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn viewport_request_from_query(query: &QueryParams) -> GraphViewportRequest {
    let default = GraphViewportRequest::default();
    GraphViewportRequest {
        x: query.i32("x").unwrap_or(default.x),
        y: query.i32("y").unwrap_or(default.y),
        width: query.i32("width").unwrap_or(default.width),
        height: query.i32("height").unwrap_or(default.height),
        overscan: query.i32("overscan").unwrap_or(default.overscan),
    }
}

fn viewport_diff_request_from_query(query: &QueryParams) -> GraphViewportDiffRequest {
    let current = viewport_request_from_query(query);
    GraphViewportDiffRequest {
        previous: GraphViewportRequest {
            x: query.i32("previous_x").unwrap_or(current.x),
            y: query.i32("previous_y").unwrap_or(current.y),
            width: query.i32("previous_width").unwrap_or(current.width),
            height: query.i32("previous_height").unwrap_or(current.height),
            overscan: query.i32("previous_overscan").unwrap_or(current.overscan),
        },
        current,
        known: known_items_from_query(query),
    }
}

fn view_mode_from_query(query: &QueryParams) -> ViewMode {
    if query.get("mode") == Some("all") || query.get("all").is_some_and(is_truthy_query_value) {
        ViewMode::All
    } else {
        ViewMode::Anchors
    }
}

fn view_mode_from_value(value: &str) -> ViewMode {
    if value == "all" {
        ViewMode::All
    } else {
        ViewMode::Anchors
    }
}

fn is_truthy_query_value(value: &str) -> bool {
    matches!(value, "1" | "true" | "yes" | "on")
}

fn known_items_from_query(query: &QueryParams) -> Option<GraphViewportKnownItems> {
    let known = GraphViewportKnownItems {
        nodes: query.get_all("known_node"),
        node_fingerprints: known_fingerprints_from_query(query, "known_node_fingerprint"),
        edges: query.get_all("known_edge"),
        edge_fingerprints: known_fingerprints_from_query(query, "known_edge_fingerprint"),
    };
    (query.contains_key("known") || !known.nodes.is_empty() || !known.edges.is_empty())
        .then_some(known)
}

fn known_fingerprints_from_query(
    query: &QueryParams,
    key: &str,
) -> std::collections::BTreeMap<String, String> {
    query
        .get_all(key)
        .into_iter()
        .filter_map(|value| {
            let (item_key, fingerprint) = value.rsplit_once(':')?;
            Some((item_key.to_owned(), fingerprint.to_owned()))
        })
        .collect()
}

fn json_response<T>(value: &T, name: &str) -> Response
where
    T: Serialize,
{
    match serde_json::to_vec(value) {
        Ok(body) => response_with_body(
            StatusCode::OK,
            "application/json; charset=utf-8",
            Body::from(body),
        ),
        Err(error) => plain_error(format!("failed to serialize {name}: {error}")),
    }
}

fn html_response(body: String) -> Response {
    response_with_body(StatusCode::OK, "text/html; charset=utf-8", Body::from(body))
}

fn plain_error(message: impl Into<String>) -> Response {
    response_with_body(
        StatusCode::INTERNAL_SERVER_ERROR,
        "text/plain; charset=utf-8",
        Body::from(message.into()),
    )
}

fn response_with_body(status: StatusCode, content_type: &'static str, body: Body) -> Response {
    let mut response = Response::new(body);
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use coco_mem::{
        Anchor, BranchStore, Kind, MergeParent, NewNode, NodeStore, PromptAnchor, Role,
        SessionAnchor, SessionRole, SqliteStore, ToolResult, ToolUse,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use crate::ConsoleStore;
    use crate::host::web_graph_view::node_target_id;

    struct RecordingToolSessionStore {
        targets: HashMap<String, String>,
        calls: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait]
    impl ToolSessionStore for RecordingToolSessionStore {
        async fn find_exec_command_node_id_for_session(
            &self,
            _head_node_id: &str,
            session_id: &str,
        ) -> coco_mem::StoreResult<Option<String>> {
            self.calls.lock().unwrap().push(session_id.to_owned());
            Ok(self.targets.get(session_id).cloned())
        }
    }

    fn test_node(kind: Kind) -> Node {
        serde_json::from_value(serde_json::json!({
            "id": "write-node",
            "parent": "parent-node",
            "created_at": "2026-07-25T00:00:00Z",
            "role": "LLM",
            "metadata": null,
            "kind": kind,
        }))
        .expect("test node should deserialize")
    }

    #[test]
    fn query_parser_decodes_repeated_values() {
        let query = parse_query("mode=all&known_node=node%3Aa&known_node=node%3Ab");

        assert_eq!(view_mode_from_query(&query), ViewMode::All);
        assert_eq!(query.get_all("known_node"), ["node:a", "node:b"]);
    }

    #[test]
    fn viewport_query_is_normalized_by_runtime_contract() {
        let query = parse_query("x=-1&y=2&width=300&height=400&overscan=20&previous_x=10&known=1");
        let request = viewport_diff_request_from_query(&query);

        assert_eq!(request.current.x, -1);
        assert_eq!(request.previous.x, 10);
        assert!(request.known.is_some());
    }

    #[test]
    fn malformed_percent_encoding_is_preserved() {
        assert_eq!(percent_decode("a%2Gb"), "a%2Gb");
    }

    #[tokio::test]
    async fn tool_input_link_resolution_uses_only_the_narrow_store_and_deduplicates_sessions() {
        let store = RecordingToolSessionStore {
            targets: HashMap::from([
                ("exec-1".to_owned(), "exec-node-1".to_owned()),
                ("exec-2".to_owned(), "exec-node-2".to_owned()),
            ]),
            calls: std::sync::Mutex::new(Vec::new()),
        };
        let node = test_node(Kind::tool_uses(vec![
            ToolUse {
                id: "write-1".to_owned(),
                name: "write_stdin".to_owned(),
                input: serde_json::json!({"session_id": "exec-1"}),
            },
            ToolUse {
                id: "write-1-repeat".to_owned(),
                name: "write_stdin".to_owned(),
                input: serde_json::json!({"session_id": "exec-1"}),
            },
            ToolUse {
                id: "write-2".to_owned(),
                name: "write_stdin".to_owned(),
                input: serde_json::json!({"session_id": "exec-2"}),
            },
            ToolUse {
                id: "write-missing".to_owned(),
                name: "write_stdin".to_owned(),
                input: serde_json::json!({"session_id": "missing"}),
            },
            ToolUse {
                id: "write-missing-repeat".to_owned(),
                name: "write_stdin".to_owned(),
                input: serde_json::json!({"session_id": "missing"}),
            },
            ToolUse {
                id: "write-invalid".to_owned(),
                name: "write_stdin".to_owned(),
                input: serde_json::json!({"session_id": 42}),
            },
        ]));

        let links = resolve_tool_use_input_links(&store, &node).await.unwrap();

        assert_eq!(
            links
                .iter()
                .map(|link| (link.tool_use_index, link.target.as_str()))
                .collect::<Vec<_>>(),
            [
                (0, "detail-exec-node-1"),
                (1, "detail-exec-node-1"),
                (2, "detail-exec-node-2"),
            ]
        );
        assert_eq!(
            *store.calls.lock().unwrap(),
            ["exec-1", "exec-2", "missing"]
        );
    }

    #[tokio::test]
    async fn client_assets_require_the_current_build_version() {
        let js = client_js(Path(CLIENT_ASSET_VERSION.to_owned())).await;
        assert_eq!(js.status(), StatusCode::OK);
        assert_eq!(
            to_bytes(js.into_body(), usize::MAX).await.unwrap(),
            COCO_CONSOLE_JS
        );

        let wasm = client_wasm(Path(CLIENT_ASSET_VERSION.to_owned())).await;
        assert_eq!(wasm.status(), StatusCode::OK);
        assert_eq!(
            to_bytes(wasm.into_body(), usize::MAX).await.unwrap(),
            COCO_CONSOLE_WASM
        );

        assert_eq!(
            client_js(Path("stale-build".to_owned())).await.status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            client_wasm(Path("stale-build".to_owned())).await.status(),
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn viewport_and_node_detail_use_the_persistent_graph_and_source_store() {
        let source = SqliteStore::open_temporary().await.unwrap();
        let publisher = ConsolePublisher::new();
        let store = ConsoleStore::new(source.clone(), publisher.clone());
        let node_id = store
            .append(NewNode {
                parent: store.root_id(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("direct detail".to_owned()),
            })
            .await
            .unwrap();
        let web_graph = WebGraphRuntime::open(source.store_path(), publisher)
            .await
            .unwrap();
        web_graph.catch_up().await.unwrap();
        let state = AppState { store, web_graph };

        let viewport =
            graph_viewport(State(state.clone()), RawQuery(Some("mode=all".to_owned()))).await;
        let viewport_body = to_bytes(viewport.into_body(), usize::MAX).await.unwrap();
        let viewport: crate::api::GraphViewportResponse =
            serde_json::from_slice(&viewport_body).unwrap();
        assert!(viewport.nodes.iter().any(|node| node.id == node_id));

        let detail = node_detail(
            State(state.clone()),
            RawQuery(Some(format!("target={}", node_target_id(&node_id)))),
        )
        .await;
        let detail_body = to_bytes(detail.into_body(), usize::MAX).await.unwrap();
        let detail: NodeDetailResponse = serde_json::from_slice(&detail_body).unwrap();
        let NodeDetailResponse::Found { node, .. } = detail else {
            panic!("node detail should be found");
        };
        assert_eq!(node.kind, Kind::Text("direct detail".to_owned()));
        assert_eq!(node.id, node_id);

        let request = Request::builder()
            .uri(format!(
                "/api/panels/node-detail?target={}",
                node_target_id(&node_id)
            ))
            .body(Body::empty())
            .unwrap();
        let detail = panel_server_function(State(state), request).await;
        let detail_body = to_bytes(detail.into_body(), usize::MAX).await.unwrap();
        let detail: NodeDetailResponse = serde_json::from_slice(&detail_body).unwrap();
        assert!(matches!(detail, NodeDetailResponse::Found { node, .. } if node.id == node_id));
    }

    #[tokio::test]
    async fn node_detail_links_write_stdin_session_to_its_exec_command_detail() {
        let source = SqliteStore::open_temporary().await.unwrap();
        let publisher = ConsolePublisher::new();
        let store = ConsoleStore::new(source.clone(), publisher.clone());
        let exec_id = store
            .append(NewNode {
                parent: store.root_id(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "exec-call".to_owned(),
                    name: "exec_command".to_owned(),
                    input: serde_json::json!({"cmd": "sleep 10"}),
                }),
            })
            .await
            .unwrap();
        let result_id = store
            .append(NewNode {
                parent: exec_id.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::tool_result(ToolResult {
                    id: "exec-call".to_owned(),
                    output: "Process running\nsession_id: exec-1\nexit_status: running\n"
                        .to_owned(),
                }),
            })
            .await
            .unwrap();
        let write_id = store
            .append(NewNode {
                parent: result_id,
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "write-call".to_owned(),
                    name: "write_stdin".to_owned(),
                    input: serde_json::json!({"session_id": "exec-1", "chars": ""}),
                }),
            })
            .await
            .unwrap();
        let web_graph = WebGraphRuntime::open(source.store_path(), publisher)
            .await
            .unwrap();
        let state = AppState { store, web_graph };

        let detail = load_node_detail(&state, &node_target_id(&write_id))
            .await
            .unwrap();
        let NodeDetailResponse::Found {
            tool_use_input_links,
            ..
        } = detail
        else {
            panic!("write_stdin detail should be found");
        };
        assert_eq!(
            tool_use_input_links,
            [crate::api::ToolUseInputLink {
                tool_use_index: 0,
                tool_use_id: "write-call".to_owned(),
                input_pointer: "/session_id".to_owned(),
                value: "exec-1".to_owned(),
                target: node_target_id(&exec_id),
            }]
        );

        let linked_detail = load_node_detail(&state, &tool_use_input_links[0].target)
            .await
            .unwrap();
        assert!(
            matches!(linked_detail, NodeDetailResponse::Found { node, .. } if node.id == exec_id)
        );
    }

    #[tokio::test]
    async fn anchor_range_expands_primary_merge_and_shadow_paths() {
        let source = SqliteStore::open_temporary().await.unwrap();
        let publisher = ConsolePublisher::new();
        let store = ConsoleStore::new(source.clone(), publisher.clone());
        let source_anchor = store
            .append(NewNode {
                parent: store.root_id(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "source".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        let detail = store
            .append(NewNode {
                parent: source_anchor.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::Text("expanded detail".to_owned()),
            })
            .await
            .unwrap();
        let target_anchor = store
            .append(NewNode {
                parent: detail.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "target".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        let second_merge_detail = store
            .append(NewNode {
                parent: source_anchor.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::Text("second merge detail".to_owned()),
            })
            .await
            .unwrap();
        let next_anchor = store
            .append(NewNode {
                parent: target_anchor.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    vec![
                        MergeParent::merge(detail.clone()),
                        MergeParent::merge(second_merge_detail.clone()),
                    ],
                    PromptAnchor {
                        prompt: "next".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        let shadow_detail = store
            .append(NewNode {
                parent: source_anchor.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::Text("shadow detail".to_owned()),
            })
            .await
            .unwrap();
        let shadow_target = store
            .append(NewNode {
                parent: next_anchor.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    vec![MergeParent::shadow(shadow_detail.clone())],
                    PromptAnchor {
                        prompt: "shadow target".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        let web_graph = WebGraphRuntime::open(source.store_path(), publisher)
            .await
            .unwrap();
        web_graph.catch_up().await.unwrap();
        let state = AppState { store, web_graph };

        let primary = load_anchor_range(
            &state,
            &source_anchor,
            &target_anchor,
            GraphViewportEdgeKind::Primary,
        )
        .await
        .unwrap();
        let AnchorRangeResponse::Found { paths } = primary else {
            panic!("primary anchor range should exist");
        };
        assert_eq!(paths.len(), 1);
        assert_eq!(
            paths[0]
                .nodes
                .iter()
                .map(|node| node.id.as_str())
                .collect::<Vec<_>>(),
            [&source_anchor, &detail, &target_anchor]
        );
        let merge = load_anchor_range(
            &state,
            &source_anchor,
            &next_anchor,
            GraphViewportEdgeKind::Merge,
        )
        .await
        .unwrap();
        let AnchorRangeResponse::Found { paths } = merge else {
            panic!("merge anchor relationship should exist");
        };
        assert_eq!(paths.len(), 2);
        assert_eq!(
            paths[0]
                .nodes
                .iter()
                .map(|node| node.id.as_str())
                .collect::<Vec<_>>(),
            [&source_anchor, &detail, &next_anchor]
        );
        assert_eq!(
            paths[0]
                .nodes
                .iter()
                .map(|node| node.incoming_edge)
                .collect::<Vec<_>>(),
            [
                None,
                Some(GraphViewportEdgeKind::Primary),
                Some(GraphViewportEdgeKind::Merge),
            ]
        );
        assert_eq!(
            paths[1]
                .nodes
                .iter()
                .map(|node| node.id.as_str())
                .collect::<Vec<_>>(),
            [&source_anchor, &second_merge_detail, &next_anchor]
        );
        assert_eq!(
            paths[1]
                .nodes
                .iter()
                .map(|node| node.incoming_edge)
                .collect::<Vec<_>>(),
            [
                None,
                Some(GraphViewportEdgeKind::Primary),
                Some(GraphViewportEdgeKind::Merge),
            ]
        );

        let shadow = load_anchor_range(
            &state,
            &source_anchor,
            &shadow_target,
            GraphViewportEdgeKind::Shadow,
        )
        .await
        .unwrap();
        let AnchorRangeResponse::Found { paths } = shadow else {
            panic!("shadow anchor relationship should exist");
        };
        assert_eq!(paths.len(), 1);
        assert_eq!(
            paths[0]
                .nodes
                .iter()
                .map(|node| node.id.as_str())
                .collect::<Vec<_>>(),
            [&source_anchor, &shadow_detail, &shadow_target]
        );
        assert_eq!(
            paths[0]
                .nodes
                .iter()
                .map(|node| node.incoming_edge)
                .collect::<Vec<_>>(),
            [
                None,
                Some(GraphViewportEdgeKind::Primary),
                Some(GraphViewportEdgeKind::Shadow),
            ]
        );
    }

    #[tokio::test]
    async fn provider_context_uses_persistent_layout_points() {
        let source = SqliteStore::open_temporary().await.unwrap();
        let publisher = ConsolePublisher::new();
        let store = ConsoleStore::new(source.clone(), publisher.clone());
        let session_id = store
            .append(NewNode {
                parent: store.root_id(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(
                    Vec::new(),
                    SessionAnchor {
                        role: SessionRole::Orchestrator,
                        provider_profile: None,
                        provider: Some("openai".to_owned()),
                        model: "test-model".to_owned(),
                        tools: Vec::new(),
                        system_prompt: "test system prompt".to_owned(),
                        prompt: "test prompt".to_owned(),
                        temperature: None,
                        max_tokens: None,
                        additional_params: None,
                        enable_coco_shim: false,
                        active_skill: None,
                    },
                )),
            })
            .await
            .unwrap();
        store.fork("main", &session_id).await.unwrap();
        let selected_id = store
            .append(NewNode {
                parent: session_id.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("provider context selection".to_owned()),
            })
            .await
            .unwrap();
        store
            .set_branch_head("main", &session_id, &selected_id)
            .await
            .unwrap();
        let web_graph = WebGraphRuntime::open(source.store_path(), publisher)
            .await
            .unwrap();
        web_graph.catch_up().await.unwrap();
        let state = AppState { store, web_graph };

        let response = provider_context(
            State(state.clone()),
            RawQuery(Some(format!(
                "target={}&mode=all",
                node_target_id(&selected_id)
            ))),
        )
        .await;
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(!String::from_utf8_lossy(&body).contains("\"content\":"));
        let response: ProviderContextResponse = serde_json::from_slice(&body).unwrap();
        let ProviderContextResponse::Found { items } = response else {
            panic!("provider context should be found");
        };
        let selected = items
            .iter()
            .find(|item| item.selected)
            .expect("selected provider context item should exist");
        assert_eq!(selected.node.summary, "provider context selection");
        assert!(selected.point.is_some());

        let request = Request::builder()
            .uri(format!(
                "/api/panels/provider-context?target={}&graph_mode=all",
                node_target_id(&selected_id)
            ))
            .body(Body::empty())
            .unwrap();
        let response = panel_server_function(State(state), request).await;
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let response: ProviderContextResponse = serde_json::from_slice(&body).unwrap();
        assert!(matches!(
            response,
            ProviderContextResponse::Found { items } if items.iter().any(|item| item.selected)
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn server_serves_third_party_notices_and_shuts_down() {
        let source = SqliteStore::open_temporary().await.unwrap();
        let publisher = ConsolePublisher::new();
        let store = ConsoleStore::new(source.clone(), publisher.clone());
        let handle = start_console_server_with_graph_store_path(
            ConsoleConfig {
                addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            },
            store,
            publisher,
            source.store_path().to_path_buf(),
        )
        .await
        .unwrap();

        assert_ne!(handle.addr().port(), 0);
        let mut stream = tokio::net::TcpStream::connect(handle.addr()).await.unwrap();
        stream
            .write_all(
                b"GET /third-party-notices.html HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();

        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(response.contains("content-type: text/html; charset=utf-8\r\n"));
        assert!(response.contains("CoCo Third-Party Notices"));
        assert!(response.contains("Apache License 2.0"));
        handle.shutdown().await.unwrap();
    }
}
