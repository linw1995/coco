use std::convert::Infallible;
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{ConnectInfo, RawQuery, Request, State};
use axum::http::{HeaderValue, Method, StatusCode, Uri, header};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use coco_mem::{Store, StoreError};
use futures_util::{StreamExt, stream};
use leptos::prelude::provide_context;
use leptos_axum::handle_server_fns_with_context;
use serde::Serialize;
use snafu::prelude::*;

use super::config::ConsoleConfig;
use super::error::{
    BindConsoleSnafu, ConfigureConsoleSocketSnafu, JoinConsoleServerSnafu, ServeConsoleSnafu,
    StoreSnafu,
};
use super::publisher::ConsolePublisher;
use super::render::render_index_page;
use crate::Result;
use crate::api::{
    NodeDetailResponse, PanelNode, Point as ApiPoint, ProviderContextItem, ProviderContextNode,
    ProviderContextResponse,
};
use crate::host::api::{GraphViewportDiffRequest, GraphViewportKnownItems, GraphViewportRequest};
use crate::host::web_graph_runtime::WebGraphRuntime;
use crate::host::web_graph_view::{
    NodeView, ViewMode, node_id_from_target, provider_context_for_node,
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
        .route("/pkg/coco_console.js", get(client_js))
        .route("/pkg/coco_console_bg.wasm", get(client_wasm))
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
        Ok(node) => Ok(NodeDetailResponse::Found {
            node: panel_node(NodeView::from(&node)),
        }),
        Err(error) if is_missing_node(&error) => Ok(NodeDetailResponse::Missing {
            target: target.to_owned(),
        }),
        Err(source) => Err(source).context(StoreSnafu),
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

fn panel_node(node: NodeView) -> PanelNode {
    PanelNode {
        id: node.id,
        short_id: node.short_id,
        kind: node.kind,
        role: node.role,
        created_at: node.created_at,
        content: node.content,
        summary: node.summary,
    }
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

async fn client_js() -> Response {
    response_with_body(
        StatusCode::OK,
        "text/javascript; charset=utf-8",
        Body::from(COCO_CONSOLE_JS),
    )
}

async fn client_wasm() -> Response {
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
        Anchor, BranchStore, Kind, NewNode, NodeStore, Role, SessionAnchor, SessionRole,
        SqliteStore,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use crate::ConsoleStore;
    use crate::host::web_graph_view::node_target_id;

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
        let NodeDetailResponse::Found { node } = detail else {
            panic!("node detail should be found");
        };
        assert_eq!(node.content, "direct detail");
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
        assert!(matches!(detail, NodeDetailResponse::Found { node } if node.id == node_id));
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
