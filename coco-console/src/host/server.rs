use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{ConnectInfo, RawQuery, Request, State};
use axum::http::{HeaderValue, Method, StatusCode, Uri, header};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use coco_mem::Store;
use futures_util::{StreamExt, stream};
use serde::Serialize;
use snafu::prelude::*;
use std::convert::Infallible;
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use crate::Result;
use crate::api::{
    GraphCanvas, GraphViewport, GraphViewportDiffResponse, GraphViewportItems,
    GraphViewportResponse,
};
use crate::config::ConsoleConfig;
use crate::error::{
    BindConsoleSnafu, ConfigureConsoleSocketSnafu, JoinConsoleServerSnafu, ServeConsoleSnafu,
};
use crate::graph::{GraphMode, GraphSnapshot};
use crate::host::api::{GraphViewportDiffRequest, GraphViewportKnownItems, GraphViewportRequest};
use crate::host::cache::ConsoleGraphCache;
use crate::publisher::ConsolePublisher;
use crate::render::{
    render_fragment, render_graph_node_detail_fragment, render_loading_fragment,
    render_loading_index_page, render_materialized_fragment, render_node_detail_fragment,
    render_provider_context_fragment, render_provider_context_items_fragment,
    render_provider_context_missing_fragment,
};

const STYLE_CSS: &str = include_str!("style.css");
const COCO_CONSOLE_JS: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/pkg/coco_console.js"));
const COCO_CONSOLE_WASM: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/pkg/coco_console_bg.wasm"));

#[derive(Clone)]
struct AppState<S> {
    cache: ConsoleGraphCache<S>,
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

#[cfg(test)]
fn start_console_server<S>(
    config: ConsoleConfig,
    store: S,
    publisher: ConsolePublisher,
) -> Result<ConsoleServerHandle>
where
    S: Store + Clone + Send + Sync + 'static,
{
    start_console_server_with_graph_store_path(config, store, publisher, None)
}

pub fn start_console_server_with_graph_store_path<S>(
    config: ConsoleConfig,
    store: S,
    publisher: ConsolePublisher,
    graph_store_path: Option<PathBuf>,
) -> Result<ConsoleServerHandle>
where
    S: Store + Clone + Send + Sync + 'static,
{
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
    let cache = match graph_store_path {
        Some(path) => ConsoleGraphCache::new_with_persistent_store_path(store, publisher, path)?,
        None => ConsoleGraphCache::new(store, publisher),
    };
    let state = AppState { cache };
    let task = tokio::spawn(async move {
        serve_console(listener, state)
            .await
            .context(ServeConsoleSnafu { addr })
    });

    Ok(ConsoleServerHandle { addr, task })
}

async fn serve_console<S>(listener: tokio::net::TcpListener, state: AppState<S>) -> io::Result<()>
where
    S: Store + Clone + Send + Sync + 'static,
{
    axum::serve(
        listener,
        router(state).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
}

fn router<S>(state: AppState<S>) -> Router
where
    S: Store + Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/", get(index_page::<S>).post(method_not_allowed))
        .route("/index.html", get(index_page::<S>))
        .route("/style.css", get(style_css))
        .route("/api/graph", get(graph_json::<S>))
        .route("/api/graph/viewport", get(graph_viewport::<S>))
        .route(
            "/api/graph/viewport/items/diff",
            get(graph_viewport_items_diff_get::<S>).post(graph_viewport_items_diff_post::<S>),
        )
        .route(
            "/api/graph/viewport/diff",
            get(graph_viewport_diff_get::<S>).post(graph_viewport_diff_post::<S>),
        )
        .route("/api/node-detail", get(node_detail::<S>))
        .route("/api/provider-context", get(provider_context::<S>))
        .route("/fragment", get(fragment::<S>))
        .route("/events", get(event_stream::<S>))
        .route("/pkg/coco_console.js", get(client_js))
        .route("/pkg/coco_console_bg.wasm", get(client_wasm))
        .fallback(not_found)
        .with_state(state)
        .layer(middleware::from_fn(access_log))
}

async fn access_log(
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    method: Method,
    uri: Uri,
    request: Request,
    next: Next,
) -> Response {
    let access_log = AccessLog::new(peer_addr, method.as_str(), uri.path());
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    access_log.log(response.status().as_u16());
    response
}

struct AccessLog {
    peer_addr: SocketAddr,
    method: String,
    path: String,
    started_at: Instant,
}

impl AccessLog {
    fn new(peer_addr: SocketAddr, method: &str, path: &str) -> Self {
        Self {
            peer_addr,
            method: method.to_owned(),
            path: path.to_owned(),
            started_at: Instant::now(),
        }
    }

    fn log(&self, status: u16) {
        tracing::info!(
            peer_addr = %self.peer_addr,
            method = %self.method,
            path = %self.path,
            status,
            duration_ms = self.started_at.elapsed().as_millis(),
            "console access"
        );
    }

    #[cfg(test)]
    fn log_error(&self, error: &io::Error) {
        tracing::warn!(
            peer_addr = %self.peer_addr,
            method = %self.method,
            path = %self.path,
            duration_ms = self.started_at.elapsed().as_millis(),
            error = %error,
            "console access failed"
        );
    }
}

async fn index_page<S>(State(state): State<AppState<S>>, RawQuery(query): RawQuery) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    let query = parse_query(query.as_deref().unwrap_or_default());
    let mode = graph_mode_from_query(&query);
    html_response(render_loading_index_page(
        mode,
        state.cache.current_version(),
    ))
}

async fn style_css() -> Response {
    response_with_body(
        StatusCode::OK,
        "text/css; charset=utf-8",
        Body::from(STYLE_CSS),
    )
}

async fn graph_json<S>(State(state): State<AppState<S>>, RawQuery(query): RawQuery) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    let query = parse_query(query.as_deref().unwrap_or_default());
    let mode = graph_mode_from_query(&query);
    if state.cache.has_materialized_viewports() {
        let _ = state
            .cache
            .viewport_current_ready_or_schedule(mode, GraphViewportRequest::default());
        return json_response(
            &loading_snapshot(mode, state.cache.current_version()),
            "graph",
        );
    }

    match state.cache.snapshot_current_ready_or_schedule(mode) {
        Some(snapshot) => json_response(&snapshot, "graph"),
        None => json_response(
            &loading_snapshot(mode, state.cache.current_version()),
            "graph",
        ),
    }
}

async fn graph_viewport<S>(State(state): State<AppState<S>>, RawQuery(query): RawQuery) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    let query = parse_query(query.as_deref().unwrap_or_default());
    let mode = graph_mode_from_query(&query);
    let request = viewport_request_from_query(&query);
    let response = match query.version() {
        Some(version) => match state.cache.viewport_after(mode, version, request).await {
            Ok(response) => response,
            Err(error) => return plain_error(error.to_string()),
        },
        None => match state
            .cache
            .viewport_current_ready_or_schedule(mode, request)
        {
            Some(response) => response,
            None => {
                return json_response(
                    &empty_graph_viewport_response(state.cache.current_version(), request),
                    "graph viewport",
                );
            }
        },
    };
    json_response(&response, "graph viewport")
}

fn empty_graph_viewport_diff_pending_response(
    version: u64,
    request: GraphViewportDiffRequest,
) -> Response {
    json_response(
        &empty_graph_viewport_diff_response(version, request),
        "graph viewport diff",
    )
}

async fn graph_viewport_diff_get<S>(
    State(state): State<AppState<S>>,
    RawQuery(query): RawQuery,
) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    let query = parse_query(query.as_deref().unwrap_or_default());
    graph_viewport_diff_response(state, query).await
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
    let body = String::from_utf8_lossy(&body);
    query.pairs.extend(parse_query(&body).pairs);
    graph_viewport_diff_response(state, query).await
}

async fn graph_viewport_items_diff_get<S>(
    State(state): State<AppState<S>>,
    RawQuery(query): RawQuery,
) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    let query = parse_query(query.as_deref().unwrap_or_default());
    graph_viewport_items_diff_response_from_query(state, query).await
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
    let body = String::from_utf8_lossy(&body);
    query.pairs.extend(parse_query(&body).pairs);
    graph_viewport_items_diff_response_from_query(state, query).await
}

async fn graph_viewport_diff_response<S>(state: AppState<S>, query: QueryParams) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    let request = viewport_diff_request_from_query(&query);
    let mode = graph_mode_from_query(&query);
    let response = match state
        .cache
        .viewport_diff_current_ready_or_schedule(mode, request.clone())
    {
        Some(response) => response,
        None => {
            return empty_graph_viewport_diff_pending_response(
                state.cache.current_version(),
                request,
            );
        }
    };
    json_response(&response, "graph viewport diff")
}

async fn graph_viewport_items_diff_response_from_query<S>(
    state: AppState<S>,
    query: QueryParams,
) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    match query.version() {
        Some(observed_version) => {
            graph_viewport_items_diff_response(
                state,
                viewport_diff_request_from_query(&query),
                observed_version,
                known_canvas_from_query(&query),
                graph_mode_from_query(&query),
            )
            .await
        }
        None => graph_viewport_diff_response(state, query).await,
    }
}

async fn graph_viewport_items_diff_response<S>(
    state: AppState<S>,
    request: GraphViewportDiffRequest,
    mut observed_version: u64,
    known_canvas: Option<GraphCanvas>,
    mode: GraphMode,
) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    loop {
        let response = match state
            .cache
            .viewport_diff_after(mode, observed_version, request.clone())
            .await
        {
            Ok(response) => response,
            Err(error) => return plain_error(error.to_string()),
        };
        if viewport_diff_has_changes(&response, request.known.as_ref())
            || known_canvas != Some(response.canvas)
        {
            return json_response(&response, "graph viewport items diff");
        }
        observed_version = response.version;
    }
}

fn viewport_diff_has_changes(
    response: &crate::api::GraphViewportDiffResponse,
    known: Option<&GraphViewportKnownItems>,
) -> bool {
    viewport_diff_has_key_changes(response)
        || known.is_some_and(|known| viewport_diff_has_fingerprint_changes(response, known))
}

fn viewport_diff_has_key_changes(response: &crate::api::GraphViewportDiffResponse) -> bool {
    !response.added.lanes.is_empty()
        || !response.added.nodes.is_empty()
        || !response.added.edges.is_empty()
        || !response.removed.is_empty()
}

fn viewport_diff_has_fingerprint_changes(
    response: &crate::api::GraphViewportDiffResponse,
    known: &GraphViewportKnownItems,
) -> bool {
    response.updated.lanes.iter().any(|lane| {
        known
            .lane_fingerprints
            .get(&lane.key)
            .is_none_or(|fingerprint| fingerprint != &lane.fingerprint())
    }) || response.updated.nodes.iter().any(|node| {
        known
            .node_fingerprints
            .get(&node.key)
            .is_none_or(|fingerprint| fingerprint != &node.fingerprint())
    }) || response.updated.edges.iter().any(|edge| {
        known
            .edge_fingerprints
            .get(&edge.key)
            .is_none_or(|fingerprint| fingerprint != &edge.fingerprint())
    })
}

async fn fragment<S>(State(state): State<AppState<S>>, RawQuery(query): RawQuery) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    let query = parse_query(query.as_deref().unwrap_or_default());
    let mode = graph_mode_from_query(&query);
    if state.cache.has_materialized_viewports() {
        match query.version() {
            Some(version) => {
                if let Err(error) = state
                    .cache
                    .viewport_after(mode, version, GraphViewportRequest::default())
                    .await
                {
                    return plain_error(error.to_string());
                }
            }
            None => {
                let _ = state
                    .cache
                    .viewport_current_ready_or_schedule(mode, GraphViewportRequest::default());
            }
        }
        return match state
            .cache
            .materialized_fragment_current_ready_or_schedule(mode)
        {
            Ok(Some(shell)) => html_response(render_materialized_fragment(&shell)),
            Ok(None) => html_response(render_loading_fragment(mode, state.cache.current_version())),
            Err(error) => plain_error(error.to_string()),
        };
    }
    match query.version() {
        Some(version) => {
            if let Some(snapshot) = state.cache.snapshot_current_ready(mode)
                && snapshot.version > version
            {
                return html_response(render_fragment(&snapshot));
            }
            let materialized = match state
                .cache
                .viewport_after(mode, version, GraphViewportRequest::default())
                .await
            {
                Ok(response) => response,
                Err(error) => return plain_error(error.to_string()),
            };
            if let Some(snapshot) = state.cache.snapshot_current_ready(mode)
                && snapshot.version >= materialized.version
            {
                return html_response(render_fragment(&snapshot));
            }
            html_response(render_loading_fragment(mode, materialized.version))
        }
        None => {
            if let Some(snapshot) = state.cache.snapshot_current_ready(mode) {
                return html_response(render_fragment(&snapshot));
            }
            let _ = state
                .cache
                .viewport_current_ready_or_schedule(mode, GraphViewportRequest::default());
            html_response(render_loading_fragment(mode, state.cache.current_version()))
        }
    }
}

async fn node_detail<S>(State(state): State<AppState<S>>, RawQuery(query): RawQuery) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    let query = parse_query(query.as_deref().unwrap_or_default());
    let mode = graph_mode_from_query(&query);
    if state.cache.has_materialized_viewports() {
        let Some(target) = query.get("target") else {
            return html_response(render_node_detail_fragment(
                &loading_snapshot(mode, state.cache.current_version()),
                None,
            ));
        };
        return match state
            .cache
            .node_detail_current_ready_or_schedule(mode, target)
        {
            Ok(Some(node)) => html_response(render_graph_node_detail_fragment(&node)),
            Ok(None) => html_response(render_node_detail_fragment(
                &loading_snapshot(mode, state.cache.current_version()),
                Some(target),
            )),
            Err(error) => plain_error(error.to_string()),
        };
    }
    let snapshot = match graph_snapshot_for_query(&state.cache, mode, &query).await {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => {
            return html_response(render_node_detail_fragment(
                &loading_snapshot(mode, state.cache.current_version()),
                query.get("target"),
            ));
        }
        Err(error) => return plain_error(error.to_string()),
    };
    html_response(render_node_detail_fragment(&snapshot, query.get("target")))
}

async fn provider_context<S>(
    State(state): State<AppState<S>>,
    RawQuery(query): RawQuery,
) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    let query = parse_query(query.as_deref().unwrap_or_default());
    let mode = graph_mode_from_query(&query);
    if query.get("target").is_none() {
        return html_response(render_provider_context_fragment(
            &loading_snapshot(mode, state.cache.current_version()),
            None,
            query.get("context"),
        ));
    }
    if state.cache.has_materialized_viewports() {
        let target = query
            .get("target")
            .expect("target was checked before materialized provider context lookup");
        return match state.cache.provider_context_current_ready_or_schedule(
            mode,
            target,
            query.get("context"),
        ) {
            Ok(Some(items)) => html_response(render_provider_context_items_fragment(items)),
            Ok(None) => html_response(render_provider_context_missing_fragment(target)),
            Err(error) => plain_error(error.to_string()),
        };
    }
    let snapshot = match graph_snapshot_for_query(&state.cache, mode, &query).await {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => {
            return html_response(render_provider_context_fragment(
                &loading_snapshot(mode, state.cache.current_version()),
                query.get("target"),
                query.get("context"),
            ));
        }
        Err(error) => return plain_error(error.to_string()),
    };
    html_response(render_provider_context_fragment(
        &snapshot,
        query.get("target"),
        query.get("context"),
    ))
}

async fn event_stream<S>(State(state): State<AppState<S>>) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    let current_version = state.cache.current_version();
    let rx = state.cache.subscribe();
    let progress_rx = state.cache.subscribe_progress();
    let invalidations = state.cache.subscribe_invalidations();
    let cache = state.cache.clone();
    let initial_progress = graph_progress_event(&cache);
    let initial = stream::iter([
        Ok::<_, Infallible>(
            Event::default()
                .event("graph")
                .data(current_version.to_string()),
        ),
        Ok::<_, Infallible>(initial_progress),
    ]);
    let changes = stream::unfold(
        (rx, progress_rx, invalidations, cache),
        |(mut rx, mut progress_rx, mut invalidations, cache)| async move {
            loop {
                tokio::select! {
                    changed = rx.changed() => {
                        if changed.is_err() {
                            return None;
                        }
                        let version = *rx.borrow_and_update();
                        return Some((
                            Ok::<_, Infallible>(
                                Event::default().event("graph").data(version.to_string()),
                            ),
                            (rx, progress_rx, invalidations, cache),
                        ));
                    }
                    changed = progress_rx.changed() => {
                        if changed.is_err() {
                            return None;
                        }
                        progress_rx.borrow_and_update();
                        return Some((
                            Ok::<_, Infallible>(
                                graph_progress_event(&cache),
                            ),
                            (rx, progress_rx, invalidations, cache),
                        ));
                    }
                    changed = invalidations.changed() => {
                        if changed.is_err() {
                            return None;
                        }
                        cache.rebuild_requested_modes();
                    }
                }
            }
        },
    );

    Sse::new(initial.chain(changes)).into_response()
}

fn graph_progress_event<S>(cache: &ConsoleGraphCache<S>) -> Event
where
    S: Store + Clone + Send + Sync + 'static,
{
    let data = serde_json::to_string(&cache.rebuild_statuses())
        .unwrap_or_else(|error| format!("{{\"error\":\"{error}\"}}"));
    Event::default().event("graph-progress").data(data)
}

fn loading_snapshot(mode: GraphMode, version: u64) -> GraphSnapshot {
    GraphSnapshot {
        version,
        mode,
        root_id: String::new(),
        nodes: Vec::new(),
        edges: Vec::new(),
        branches: Vec::new(),
        provider_contexts: Vec::new(),
    }
}

fn empty_graph_viewport_response(
    version: u64,
    request: GraphViewportRequest,
) -> GraphViewportResponse {
    let request = request.normalized();
    GraphViewportResponse {
        version,
        canvas: empty_graph_canvas(request),
        viewport: GraphViewport {
            x: request.x,
            y: request.y,
            width: request.width,
            height: request.height,
            overscan: request.overscan,
        },
        lanes: Vec::new(),
        nodes: Vec::new(),
        edges: Vec::new(),
    }
}

fn empty_graph_viewport_diff_response(
    version: u64,
    request: GraphViewportDiffRequest,
) -> GraphViewportDiffResponse {
    let previous = request.previous.normalized();
    let current = request.current.normalized();
    GraphViewportDiffResponse {
        version,
        canvas: empty_graph_canvas(current),
        previous_viewport: GraphViewport {
            x: previous.x,
            y: previous.y,
            width: previous.width,
            height: previous.height,
            overscan: previous.overscan,
        },
        viewport: GraphViewport {
            x: current.x,
            y: current.y,
            width: current.width,
            height: current.height,
            overscan: current.overscan,
        },
        added: GraphViewportItems::default(),
        updated: GraphViewportItems::default(),
        removed: Vec::new(),
    }
}

fn empty_graph_canvas(request: GraphViewportRequest) -> GraphCanvas {
    GraphCanvas {
        width: request.width.max(1),
        height: request.height.max(1),
    }
}

async fn graph_snapshot_for_query<S>(
    cache: &ConsoleGraphCache<S>,
    mode: GraphMode,
    query: &QueryParams,
) -> Result<Option<Arc<GraphSnapshot>>>
where
    S: Store + Clone + Send + Sync + 'static,
{
    match query.version() {
        Some(version) => cache.snapshot_after(mode, version).await.map(Some),
        None => Ok(cache.snapshot_current_ready_or_schedule(mode)),
    }
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
    let known = known_items_from_query(query);
    GraphViewportDiffRequest {
        previous: GraphViewportRequest {
            x: query.i32("previous_x").unwrap_or(current.x),
            y: query.i32("previous_y").unwrap_or(current.y),
            width: query.i32("previous_width").unwrap_or(current.width),
            height: query.i32("previous_height").unwrap_or(current.height),
            overscan: query.i32("previous_overscan").unwrap_or(current.overscan),
        },
        current,
        known,
    }
}

fn graph_mode_from_query(query: &QueryParams) -> GraphMode {
    if query.get("mode") == Some("all") || query.get("all").is_some_and(is_truthy_query_value) {
        GraphMode::All
    } else {
        GraphMode::Anchors
    }
}

fn is_truthy_query_value(value: &str) -> bool {
    matches!(value, "1" | "true" | "yes" | "on")
}

fn known_canvas_from_query(query: &QueryParams) -> Option<GraphCanvas> {
    Some(GraphCanvas {
        width: query.i32("canvas_width")?,
        height: query.i32("canvas_height")?,
    })
}

fn known_items_from_query(query: &QueryParams) -> Option<GraphViewportKnownItems> {
    let known = GraphViewportKnownItems {
        lanes: query.get_all("known_lane"),
        lane_fingerprints: known_fingerprints_from_query(query, "known_lane_fingerprint"),
        nodes: query.get_all("known_node"),
        node_fingerprints: known_fingerprints_from_query(query, "known_node_fingerprint"),
        edges: query.get_all("known_edge"),
        edge_fingerprints: known_fingerprints_from_query(query, "known_edge_fingerprint"),
    };
    (query.contains_key("known")
        || !known.lanes.is_empty()
        || !known.nodes.is_empty()
        || !known.edges.is_empty())
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
    use super::{
        AppState, fragment, graph_json, graph_viewport_diff_response,
        graph_viewport_items_diff_response_from_query, node_detail, parse_query, provider_context,
        start_console_server, viewport_diff_has_changes, viewport_diff_request_from_query,
    };
    use crate::api::{
        GraphCanvas, GraphViewportDiffResponse, GraphViewportEdge, GraphViewportEdgeKind,
        GraphViewportItems, GraphViewportRemovedItem, Point,
    };
    use crate::graph::{GraphMode, build_graph_snapshot, node_target_id};
    use crate::host::api::{GraphViewportKnownItems, GraphViewportRequest};
    use crate::layout::layout_graph_viewport;
    use crate::{ConsoleConfig, ConsoleGraphCache, ConsolePublisher, ConsoleStore};
    use axum::body::to_bytes;
    use axum::extract::{RawQuery, State};
    use coco_mem::{
        Anchor, BranchStore, Kind, MemoryStore, NewNode, NodeStore, PersistentStore, PromptAnchor,
        Role, SessionAnchor, SessionRole, Store,
    };
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::time::{Duration, timeout};

    fn test_config() -> ConsoleConfig {
        ConsoleConfig {
            addr: "127.0.0.1:0".parse().unwrap(),
        }
    }

    fn get_request(path: &str) -> String {
        format!("GET {path} HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n")
    }

    fn known_viewport_query(
        version: u64,
        viewport: GraphViewportRequest,
        rendered: &crate::api::GraphViewportResponse,
    ) -> String {
        let mut query = format!(
            "version={version}&mode=all&x={}&y={}&width={}&height={}&overscan={}&known=1&canvas_width={}&canvas_height={}",
            viewport.x,
            viewport.y,
            viewport.width,
            viewport.height,
            viewport.overscan,
            rendered.canvas.width,
            rendered.canvas.height,
        );
        for lane in &rendered.lanes {
            append_known_item(&mut query, "lane", &lane.key, &lane.fingerprint());
        }
        for node in &rendered.nodes {
            append_known_item(&mut query, "node", &node.key, &node.fingerprint());
        }
        for edge in &rendered.edges {
            append_known_item(&mut query, "edge", &edge.key, &edge.fingerprint());
        }
        query
    }

    fn append_known_item(query: &mut String, kind: &str, key: &str, fingerprint: &str) {
        query.push_str("&known_");
        query.push_str(kind);
        query.push('=');
        query.push_str(key);
        query.push_str("&known_");
        query.push_str(kind);
        query.push_str("_fingerprint=");
        query.push_str(key);
        query.push(':');
        query.push_str(fingerprint);
    }

    fn app_state<S>(store: S, publisher: ConsolePublisher) -> AppState<S>
    where
        S: Store + Clone + Send + Sync + 'static,
    {
        AppState {
            cache: ConsoleGraphCache::new(store, publisher),
        }
    }

    fn temp_store_path() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "coco-console-server-test-{}-{nanos}",
            std::process::id()
        ))
    }

    fn test_session_anchor() -> SessionAnchor {
        SessionAnchor {
            role: SessionRole::Orchestrator,
            provider_profile: None,
            provider: Some("openai".to_owned()),
            model: "gpt-4.1-mini".to_owned(),
            tools: Vec::new(),
            system_prompt: "You are helpful.".to_owned(),
            prompt: "Start".to_owned(),
            temperature: None,
            max_tokens: None,
            additional_params: None,
            enable_coco_shim: false,
            active_skill: None,
        }
    }

    async fn response_bytes_from(bytes: &[u8]) -> Vec<u8> {
        let handle = start_console_server(
            test_config(),
            MemoryStore::new(),
            crate::ConsolePublisher::new(),
        )
        .unwrap();
        let mut client = tokio::net::TcpStream::connect(handle.addr()).await.unwrap();
        client.write_all(bytes).await.unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        handle.shutdown().await.unwrap();

        response
    }

    async fn response_from(bytes: &[u8]) -> String {
        let response = response_bytes_from(bytes).await;
        String::from_utf8(response).unwrap()
    }

    #[tokio::test]
    async fn start_console_server_accepts_requests() {
        let handle = start_console_server(
            test_config(),
            MemoryStore::new(),
            crate::ConsolePublisher::new(),
        )
        .unwrap();
        let mut stream = tokio::net::TcpStream::connect(handle.addr()).await.unwrap();
        stream
            .write_all(get_request("/style.css").as_bytes())
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        handle.shutdown().await.unwrap();

        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(response.contains("content-type: text/css; charset=utf-8"));
    }

    #[test]
    fn access_log_records_success_and_error_results() {
        let subscriber = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::INFO)
            .finish();
        tracing::dispatcher::with_default(&tracing::Dispatch::new(subscriber), || {
            let access_log =
                super::AccessLog::new("127.0.0.1:12345".parse().unwrap(), "GET", "/fragment");

            access_log.log(200);
            access_log.log_error(&std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "client closed",
            ));
        });
    }

    #[tokio::test]
    async fn write_event_stream_writes_initial_and_changed_events() {
        let publisher = ConsolePublisher::new();
        let store = ConsoleStore::new(MemoryStore::new(), publisher.clone());
        let handle = start_console_server(test_config(), store.clone(), publisher.clone()).unwrap();
        let mut stream = tokio::net::TcpStream::connect(handle.addr()).await.unwrap();
        stream
            .write_all(b"GET /events HTTP/1.1\r\nhost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut response = vec![0; 256];

        let mut initial = Vec::new();
        while !contains_bytes(&initial, b"event: graph\ndata: 0")
            || !contains_bytes(&initial, b"event: graph-progress")
        {
            let read = stream.read(&mut response).await.unwrap();
            assert_ne!(read, 0);
            initial.extend_from_slice(&response[..read]);
        }
        let initial = String::from_utf8_lossy(&initial);
        assert!(initial.starts_with("HTTP/1.1 200 OK"), "{initial}");
        assert!(initial.contains("content-type: text/event-stream"));

        let mut trigger = tokio::net::TcpStream::connect(handle.addr()).await.unwrap();
        trigger
            .write_all(get_request("/api/graph").as_bytes())
            .await
            .unwrap();
        let mut trigger_response = Vec::new();
        trigger.read_to_end(&mut trigger_response).await.unwrap();

        store
            .append(NewNode {
                parent: store.root_id(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("changed".to_owned()),
            })
            .unwrap();

        let mut invalidated = Vec::new();
        while !contains_bytes(&invalidated, b"event: graph\ndata: 1") {
            let read = stream.read(&mut response).await.unwrap();
            assert_ne!(read, 0);
            invalidated.extend_from_slice(&response[..read]);
        }

        handle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn versioned_viewport_items_diff_waits_past_empty_known_diff() {
        let publisher = ConsolePublisher::new();
        let store = ConsoleStore::new(MemoryStore::new(), publisher.clone());
        let root = store.root_id();
        store.fork("main", &root).unwrap();
        let first = store
            .append(NewNode {
                parent: root.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("visible".to_owned()),
            })
            .unwrap();
        store.set_branch_head("main", &root, &first).unwrap();
        let viewport = GraphViewportRequest::default();
        let state = app_state(store.clone(), publisher.clone());
        let snapshot = state.cache.current_snapshot(GraphMode::All).await;
        let version = snapshot.version;
        let rendered = layout_graph_viewport(&snapshot, viewport);
        let query = known_viewport_query(version, viewport, &rendered);
        let mut task = tokio::spawn(graph_viewport_items_diff_response_from_query(
            state,
            parse_query(&query),
        ));

        publisher.mark_changed();

        assert!(
            timeout(Duration::from_millis(50), &mut task).await.is_err(),
            "an unrelated version bump must not complete the viewport item diff when known items are unchanged"
        );

        let next = store
            .append(NewNode {
                parent: first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("next visible".to_owned()),
            })
            .unwrap();
        store.set_branch_head("main", &first, &next).unwrap();

        let response = timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        assert!(response.status().is_success());
    }

    #[tokio::test]
    async fn versioned_viewport_items_diff_returns_known_payload_changes() {
        let publisher = ConsolePublisher::new();
        let store = ConsoleStore::new(MemoryStore::new(), publisher.clone());
        let root = store.root_id();
        let first = store
            .append(NewNode {
                parent: root.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("first".to_owned()),
            })
            .unwrap();
        let second = store
            .append(NewNode {
                parent: first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("second".to_owned()),
            })
            .unwrap();
        store.fork("main", &first).unwrap();
        store.fork("draft", &second).unwrap();
        let viewport = GraphViewportRequest::default();
        let state = app_state(store.clone(), publisher.clone());
        let snapshot = state.cache.current_snapshot(GraphMode::All).await;
        let version = snapshot.version;
        let rendered = layout_graph_viewport(&snapshot, viewport);
        let query = known_viewport_query(version, viewport, &rendered);
        let task = tokio::spawn(graph_viewport_items_diff_response_from_query(
            state,
            parse_query(&query),
        ));

        store.set_branch_head("main", &first, &second).unwrap();

        let response = timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        assert!(response.status().is_success());
    }

    #[tokio::test]
    async fn versioned_viewport_items_diff_waits_for_newer_version() {
        let publisher = ConsolePublisher::new();
        let store = ConsoleStore::new(MemoryStore::new(), publisher.clone());
        let viewport = GraphViewportRequest::default();
        let state = app_state(store, publisher.clone());
        let snapshot = state.cache.current_snapshot(GraphMode::All).await;
        let query = format!(
            "version={}&mode=all&x={}&y={}&width={}&height={}&overscan={}&known=1",
            snapshot.version,
            viewport.x,
            viewport.y,
            viewport.width,
            viewport.height,
            viewport.overscan,
        );
        let mut task = tokio::spawn(graph_viewport_items_diff_response_from_query(
            state,
            parse_query(&query),
        ));

        assert!(
            timeout(Duration::from_millis(50), &mut task).await.is_err(),
            "a matching observed version must keep the viewport item diff pending"
        );

        publisher.mark_changed();
        let response = timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        assert!(response.status().is_success());
    }

    #[tokio::test]
    async fn viewport_diff_returns_immediate_patch_even_with_version_query() {
        let publisher = ConsolePublisher::new();
        let store = ConsoleStore::new(MemoryStore::new(), publisher.clone());
        let root = store.root_id();
        store.fork("main", &root).unwrap();
        let version = publisher.current_version();
        let viewport = GraphViewportRequest::default();
        let snapshot = build_graph_snapshot(&store, version).unwrap();
        let rendered = layout_graph_viewport(&snapshot, viewport);
        let query = known_viewport_query(version, viewport, &rendered);
        let state = app_state(store, publisher);
        let task = tokio::spawn(graph_viewport_diff_response(state, parse_query(&query)));

        let response = timeout(Duration::from_millis(50), task)
            .await
            .unwrap()
            .unwrap();
        assert!(response.status().is_success());
    }

    #[tokio::test]
    async fn handle_connection_serves_graph_viewport_items_diff_get_without_version() {
        let response = response_from(
            b"GET /api/graph/viewport/items/diff?x=0&y=0&width=640&height=360&known_node=node:stale HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n",
        )
        .await;

        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(response.contains("\"removed\":[]"), "{response}");
        assert!(
            !response.contains("\"key\":\"node:stale\""),
            "pending snapshots must not remove previously rendered items: {response}"
        );
    }

    #[tokio::test]
    async fn handle_connection_serves_graph_viewport_items_diff_post_without_version() {
        let body = "x=0&y=0&width=640&height=360&known_node=node:stale";
        let request = format!(
            "POST /api/graph/viewport/items/diff HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let response = response_from(request.as_bytes()).await;

        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(response.contains("\"removed\":[]"), "{response}");
        assert!(
            !response.contains("\"key\":\"node:stale\""),
            "pending snapshots must not remove previously rendered items: {response}"
        );
    }

    #[test]
    fn viewport_diff_detects_updated_edge_payload_changes() {
        let edge = GraphViewportEdge {
            key: "edge:primary_parent:root:child".to_owned(),
            kind: GraphViewportEdgeKind::PrimaryParent,
            source_id: "root".to_owned(),
            target_id: "child".to_owned(),
            source: Point { x: 0, y: 0 },
            target: Point { x: 100, y: 100 },
            route_slot: 0,
            target_port_offset: 0.0,
        };
        let response = GraphViewportDiffResponse {
            version: 1,
            canvas: GraphCanvas {
                width: 100,
                height: 100,
            },
            previous_viewport: crate::api::GraphViewport {
                x: 0,
                y: 0,
                width: 100,
                height: 100,
                overscan: 0,
            },
            viewport: crate::api::GraphViewport {
                x: 0,
                y: 0,
                width: 100,
                height: 100,
                overscan: 0,
            },
            added: GraphViewportItems::default(),
            updated: GraphViewportItems {
                lanes: Vec::new(),
                nodes: Vec::new(),
                edges: vec![edge.clone()],
            },
            removed: Vec::<GraphViewportRemovedItem>::new(),
        };
        let mut edge_fingerprints = BTreeMap::new();
        edge_fingerprints.insert(edge.key.clone(), "stale".to_owned());
        let known = GraphViewportKnownItems {
            edge_fingerprints,
            ..GraphViewportKnownItems::default()
        };

        assert!(viewport_diff_has_changes(&response, Some(&known)));
    }

    #[tokio::test]
    async fn versioned_fragment_waits_for_newer_version() {
        let publisher = ConsolePublisher::new();
        let handle =
            start_console_server(test_config(), MemoryStore::new(), publisher.clone()).unwrap();
        let mut initial = tokio::net::TcpStream::connect(handle.addr()).await.unwrap();
        initial
            .write_all(b"GET /fragment HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut initial_response = Vec::new();
        timeout(
            Duration::from_secs(1),
            initial.read_to_end(&mut initial_response),
        )
        .await
        .unwrap()
        .unwrap();
        let initial_response = String::from_utf8_lossy(&initial_response);
        assert!(
            initial_response.starts_with("HTTP/1.1 200 OK"),
            "{initial_response}"
        );

        let mut stream = tokio::net::TcpStream::connect(handle.addr()).await.unwrap();
        stream
            .write_all(b"GET /fragment?version=0 HTTP/1.1\r\nhost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut response = vec![0; 256];

        assert!(
            timeout(Duration::from_millis(50), stream.read(&mut response))
                .await
                .is_err(),
            "a matching observed version must keep the fragment request pending"
        );

        publisher.mark_changed();
        let read = timeout(Duration::from_secs(1), stream.read(&mut response))
            .await
            .unwrap()
            .unwrap();
        let response = String::from_utf8_lossy(&response[..read]);
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        handle.shutdown().await.unwrap();
    }

    fn assert_response(response: &str, status: &str, content_type: &str, body: &str) {
        assert!(response.starts_with(status), "{response}");
        assert!(response.contains(content_type), "{response}");
        assert!(response.ends_with(body), "{response}");
    }

    fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
        haystack
            .windows(needle.len())
            .any(|window| window == needle)
    }

    #[test]
    fn parse_query_extracts_path_parameters() {
        let query = parse_query("ignored=true&version=42");

        assert_eq!(query.get("ignored"), Some("true"));
        assert_eq!(query.version(), Some(42));
    }

    #[test]
    fn parse_query_parses_form_body() {
        let query = parse_query("version=42&known_node=n1");

        assert_eq!(query.version(), Some(42));
        assert_eq!(query.get_all("known_node"), vec!["n1"]);
    }

    #[test]
    fn parse_query_ignores_invalid_or_missing_version_query() {
        let invalid = parse_query("version=bad");
        let missing = parse_query("");

        assert_eq!(invalid.version(), None);
        assert_eq!(missing.version(), None);
    }

    #[tokio::test]
    async fn handle_connection_rejects_non_get_requests() {
        let response =
            response_from(b"POST / HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n").await;

        assert_response(
            &response,
            "HTTP/1.1 405 Method Not Allowed",
            "content-type: text/plain; charset=utf-8",
            "method not allowed",
        );
    }

    #[tokio::test]
    async fn handle_connection_serves_graph_viewport_diff_post_body() {
        let body = "x=0&y=0&width=640&height=360&known_node=node:stale&known_edge=edge:primary_parent:base:stale";
        let request = format!(
            "POST /api/graph/viewport/diff HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let response = response_from(request.as_bytes()).await;

        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(
            response.contains("\"removed\":[]"),
            "pending snapshots must not remove previously rendered items: {response}"
        );
        assert!(!response.contains("\"key\":\"node:stale\""), "{response}");
        assert!(
            !response.contains("\"key\":\"edge:primary_parent:base:stale\""),
            "{response}"
        );
    }

    #[tokio::test]
    async fn handle_connection_returns_not_found_for_unknown_path() {
        let response = response_from(get_request("/missing").as_bytes()).await;

        assert_response(
            &response,
            "HTTP/1.1 404 Not Found",
            "content-type: text/plain; charset=utf-8",
            "not found",
        );
    }

    #[tokio::test]
    async fn handle_connection_serves_index_page() {
        let response = response_from(get_request("/index.html").as_bytes()).await;

        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(
            response.contains("content-type: text/html; charset=utf-8"),
            "{response}"
        );
        assert!(response.contains("<!doctype html>"), "{response}");
        assert!(response.contains("data-version=\"0\""), "{response}");
    }

    #[tokio::test]
    async fn handle_connection_serves_style_css() {
        let response = response_from(get_request("/style.css").as_bytes()).await;

        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(
            response.contains("content-type: text/css; charset=utf-8"),
            "{response}"
        );
        assert!(response.contains("font-family"), "{response}");
        assert!(
            response.contains(".node-detail.node-detail-selected"),
            "{response}"
        );
        assert!(
            response.contains("body:has(.node-detail.node-detail-selected)"),
            "{response}"
        );
    }

    #[tokio::test]
    async fn handle_connection_serves_graph_json() {
        let response = response_from(get_request("/api/graph").as_bytes()).await;

        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(
            response.contains("content-type: application/json; charset=utf-8"),
            "{response}"
        );
        assert!(response.contains("\"version\":0"), "{response}");
        assert!(response.contains("\"nodes\""), "{response}");
    }

    #[tokio::test]
    async fn graph_json_schedules_materialization_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open_or_migrate_fs(&path).unwrap();
        let publisher = ConsolePublisher::new();
        writer.fork("main", &writer.root_id()).unwrap();
        publisher.mark_changed();
        let state = AppState {
            cache: ConsoleGraphCache::new_with_persistent_store_path(
                MemoryStore::new(),
                publisher,
                path.clone(),
            )
            .unwrap(),
        };

        let response = graph_json(State(state.clone()), RawQuery(None)).await;
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json = String::from_utf8(body.to_vec()).unwrap();

        assert!(json.contains("\"root_id\":\"\""));
        assert!(json.contains("\"nodes\":[]"));
        assert!(state.cache.snapshot_current_ready(GraphMode::All).is_none());

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn graph_json_ignores_cached_full_snapshot_in_materialized_mode() {
        let path = temp_store_path();
        let writer = PersistentStore::open_or_migrate_fs(&path).unwrap();
        let publisher = ConsolePublisher::new();
        let root = writer.root_id();
        writer.fork("main", &root).unwrap();
        let text = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("cached full snapshot node".to_owned()),
            })
            .unwrap();
        writer.set_branch_head("main", &root, &text).unwrap();
        publisher.mark_changed();
        let state = AppState {
            cache: ConsoleGraphCache::new_with_persistent_store_path(
                MemoryStore::new(),
                publisher,
                path.clone(),
            )
            .unwrap(),
        };
        state.cache.current_snapshot(GraphMode::All).await;

        let response =
            graph_json(State(state.clone()), RawQuery(Some("mode=all".to_owned()))).await;
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json = String::from_utf8(body.to_vec()).unwrap();

        assert!(json.contains("\"nodes\":[]"), "{json}");
        assert!(!json.contains("cached full snapshot node"), "{json}");

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn handle_connection_serves_graph_viewport_json() {
        let response = response_from(
            get_request("/api/graph/viewport?x=10&y=20&width=640&height=360&overscan=40")
                .as_bytes(),
        )
        .await;

        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(
            response.contains("content-type: application/json; charset=utf-8"),
            "{response}"
        );
        assert!(response.contains("\"canvas\""), "{response}");
        assert!(response.contains("\"viewport\""), "{response}");
        assert!(response.contains("\"x\":10"), "{response}");
        assert!(response.contains("\"width\":640"), "{response}");
    }

    #[tokio::test]
    async fn handle_connection_serves_graph_viewport_diff_json() {
        let response = response_from(
            get_request(
                "/api/graph/viewport/diff?previous_x=0&previous_y=0&previous_width=320&previous_height=180&x=10&y=20&width=640&height=360&overscan=40",
            )
            .as_bytes(),
        )
        .await;

        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(
            response.contains("content-type: application/json; charset=utf-8"),
            "{response}"
        );
        assert!(response.contains("\"previous_viewport\""), "{response}");
        assert!(response.contains("\"added\""), "{response}");
        assert!(response.contains("\"updated\""), "{response}");
        assert!(response.contains("\"removed\""), "{response}");
    }

    #[tokio::test]
    async fn handle_connection_serves_graph_viewport_diff_with_known_keys() {
        let response = response_from(
            get_request(
                "/api/graph/viewport/diff?x=0&y=0&width=640&height=360&known_node=node:stale&known_edge=edge:primary_parent:base:stale",
            )
            .as_bytes(),
        )
        .await;

        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(
            response.contains("\"removed\":[]"),
            "pending snapshots must not remove previously rendered items: {response}"
        );
        assert!(!response.contains("\"key\":\"node:stale\""), "{response}");
        assert!(
            !response.contains("\"key\":\"edge:primary_parent:base:stale\""),
            "{response}"
        );
    }

    #[tokio::test]
    async fn handle_connection_serves_graph_viewport_diff_with_empty_known_set() {
        let response = response_from(
            get_request("/api/graph/viewport/diff?x=0&y=0&width=640&height=360&known=1").as_bytes(),
        )
        .await;

        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(response.contains("\"added\""), "{response}");
        assert!(response.contains("\"nodes\":[]"), "{response}");
    }

    #[test]
    fn viewport_diff_query_parses_repeated_known_keys() {
        let query = parse_query(
            "known_node=node%3Abase&known_node=node:merged&known_lane=lane%3Amain&known_edge=edge%3Aprimary_parent%3Abase%3Amerged",
        );

        let request = viewport_diff_request_from_query(&query);
        let known = request.known.expect("known keys should parse");

        assert_eq!(known.lanes, vec!["lane:main"]);
        assert_eq!(known.nodes, vec!["node:base", "node:merged"]);
        assert_eq!(known.edges, vec!["edge:primary_parent:base:merged"]);
    }

    #[test]
    fn viewport_diff_query_parses_empty_known_set_marker() {
        let query = parse_query("known=1");

        let request = viewport_diff_request_from_query(&query);
        let known = request.known.expect("empty known set should parse");

        assert!(known.lanes.is_empty());
        assert!(known.nodes.is_empty());
        assert!(known.edges.is_empty());
    }

    #[tokio::test]
    async fn handle_connection_serves_fragment() {
        let response = response_from(get_request("/fragment").as_bytes()).await;

        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(
            response.contains("content-type: text/html; charset=utf-8"),
            "{response}"
        );
        assert!(response.contains("data-version=\"0\""), "{response}");
    }

    #[tokio::test]
    async fn fragment_schedules_materialization_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open_or_migrate_fs(&path).unwrap();
        let publisher = ConsolePublisher::new();
        writer.fork("main", &writer.root_id()).unwrap();
        publisher.mark_changed();
        let state = AppState {
            cache: ConsoleGraphCache::new_with_persistent_store_path(
                MemoryStore::new(),
                publisher,
                path.clone(),
            )
            .unwrap(),
        };

        let response = fragment(State(state.clone()), RawQuery(None)).await;
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();

        assert!(html.contains("Loading graph"));
        assert!(state.cache.snapshot_current_ready(GraphMode::All).is_none());

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn fragment_uses_materialized_shell_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open_or_migrate_fs(&path).unwrap();
        let publisher = ConsolePublisher::new();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), test_session_anchor())),
            })
            .unwrap();
        writer.fork("main", &session).unwrap();
        let prompt = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "visible shell prompt".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .unwrap();
        writer.set_branch_head("main", &session, &prompt).unwrap();
        publisher.mark_changed();
        let seed_cache = ConsoleGraphCache::new_with_persistent_store_path(
            MemoryStore::new(),
            publisher.clone(),
            path.clone(),
        )
        .unwrap();
        seed_cache.current_snapshot(GraphMode::Anchors).await;
        drop(seed_cache);

        let state = AppState {
            cache: ConsoleGraphCache::new_with_persistent_store_path(
                MemoryStore::new(),
                publisher,
                path.clone(),
            )
            .unwrap(),
        };

        let response = fragment(
            State(state.clone()),
            RawQuery(Some("mode=anchors".to_owned())),
        )
        .await;
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();

        assert!(html.contains("2 nodes / 1 edges / Anchors"), "{html}");
        assert!(html.contains("<strong>main</strong>"), "{html}");
        assert!(html.contains("time-scale-tick"), "{html}");
        assert!(!html.contains("Loading graph / Anchors"), "{html}");
        assert!(
            state
                .cache
                .snapshot_current_ready(GraphMode::Anchors)
                .is_none()
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn handle_connection_serves_node_detail_fragment() {
        let response = response_from(get_request("/api/node-detail").as_bytes()).await;

        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(
            response.contains("content-type: text/html; charset=utf-8"),
            "{response}"
        );
        assert!(
            response.contains("Select a node to inspect its content."),
            "{response}"
        );
    }

    #[tokio::test]
    async fn node_detail_uses_materialized_facts_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open_or_migrate_fs(&path).unwrap();
        let publisher = ConsolePublisher::new();
        let root = writer.root_id();
        writer.fork("main", &root).unwrap();
        let text = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("single node detail".to_owned()),
            })
            .unwrap();
        writer.set_branch_head("main", &root, &text).unwrap();
        publisher.mark_changed();
        let state = AppState {
            cache: ConsoleGraphCache::new_with_persistent_store_path(
                MemoryStore::new(),
                publisher,
                path.clone(),
            )
            .unwrap(),
        };
        let query = format!("target={}&mode=all", node_target_id(&text));

        let response = node_detail(State(state.clone()), RawQuery(Some(query))).await;
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();

        assert!(html.contains("single node detail"), "{html}");
        assert!(state.cache.snapshot_current_ready(GraphMode::All).is_none());

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn node_detail_reads_hidden_context_node_incrementally() {
        let path = temp_store_path();
        let writer = PersistentStore::open_or_migrate_fs(&path).unwrap();
        let publisher = ConsolePublisher::new();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), test_session_anchor())),
            })
            .unwrap();
        writer.fork("main", &session).unwrap();
        let hidden_text = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("hidden targeted materialized detail".to_owned()),
            })
            .unwrap();
        let prompt = writer
            .append(NewNode {
                parent: hidden_text.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "visible prompt after hidden detail".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .unwrap();
        writer.set_branch_head("main", &session, &prompt).unwrap();
        publisher.mark_changed();
        let state = AppState {
            cache: ConsoleGraphCache::new_with_persistent_store_path(
                MemoryStore::new(),
                publisher,
                path.clone(),
            )
            .unwrap(),
        };
        state.cache.current_snapshot(GraphMode::Anchors).await;
        let query = format!("mode=anchors&target={}", node_target_id(&hidden_text));

        let response = node_detail(State(state.clone()), RawQuery(Some(query))).await;
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();

        assert!(
            html.contains("hidden targeted materialized detail"),
            "{html}"
        );
        assert!(html.contains("<dd>None</dd>"), "{html}");

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn handle_connection_serves_provider_context_fragment() {
        let response = response_from(get_request("/api/provider-context").as_bytes()).await;

        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(
            response.contains("content-type: text/html; charset=utf-8"),
            "{response}"
        );
        assert!(
            response.contains("Select a node to inspect its provider context."),
            "{response}"
        );
    }

    #[tokio::test]
    async fn provider_context_default_avoids_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open_or_migrate_fs(&path).unwrap();
        let publisher = ConsolePublisher::new();
        writer.fork("main", &writer.root_id()).unwrap();
        publisher.mark_changed();
        let state = AppState {
            cache: ConsoleGraphCache::new_with_persistent_store_path(
                MemoryStore::new(),
                publisher,
                path.clone(),
            )
            .unwrap(),
        };

        let response = provider_context(State(state.clone()), RawQuery(None)).await;
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();

        assert!(
            html.contains("Select a node to inspect its provider context."),
            "{html}"
        );
        assert!(state.cache.snapshot_current_ready(GraphMode::All).is_none());

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn provider_context_uses_materialized_facts_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open_or_migrate_fs(&path).unwrap();
        let publisher = ConsolePublisher::new();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), test_session_anchor())),
            })
            .unwrap();
        writer.fork("main", &session).unwrap();
        let hidden_text = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("hidden context item".to_owned()),
            })
            .unwrap();
        let prompt = writer
            .append(NewNode {
                parent: hidden_text.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "visible context prompt".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .unwrap();
        writer.set_branch_head("main", &session, &prompt).unwrap();
        publisher.mark_changed();
        let seed_cache = ConsoleGraphCache::new_with_persistent_store_path(
            MemoryStore::new(),
            publisher.clone(),
            path.clone(),
        )
        .unwrap();
        seed_cache.current_snapshot(GraphMode::Anchors).await;
        drop(seed_cache);

        let state = AppState {
            cache: ConsoleGraphCache::new_with_persistent_store_path(
                MemoryStore::new(),
                publisher,
                path.clone(),
            )
            .unwrap(),
        };
        let visible_query = format!("mode=anchors&target={}", node_target_id(&prompt));

        let response = provider_context(State(state.clone()), RawQuery(Some(visible_query))).await;
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let visible_html = String::from_utf8(body.to_vec()).unwrap();

        assert!(
            visible_html.contains("visible context prompt"),
            "{visible_html}"
        );
        assert!(
            visible_html.contains("hidden context item"),
            "{visible_html}"
        );
        assert!(visible_html.contains("data-node-x="), "{visible_html}");
        assert!(
            state
                .cache
                .snapshot_current_ready(GraphMode::Anchors)
                .is_none()
        );

        let hidden_query = format!("mode=anchors&target={}", node_target_id(&hidden_text));
        let response = provider_context(State(state.clone()), RawQuery(Some(hidden_query))).await;
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let hidden_html = String::from_utf8(body.to_vec()).unwrap();

        assert!(hidden_html.contains("hidden context item"), "{hidden_html}");
        assert!(
            hidden_html.contains("class=\"provider-context-node selected\""),
            "{hidden_html}"
        );
        assert!(
            state
                .cache
                .snapshot_current_ready(GraphMode::Anchors)
                .is_none()
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn handle_connection_serves_assets() {
        let js = response_bytes_from(get_request("/pkg/coco_console.js").as_bytes()).await;
        let wasm = response_bytes_from(get_request("/pkg/coco_console_bg.wasm").as_bytes()).await;

        assert!(js.starts_with(b"HTTP/1.1 200 OK"), "{js:?}");
        assert!(contains_bytes(
            &js,
            b"content-type: text/javascript; charset=utf-8"
        ));
        assert!(contains_bytes(&js, b"import"));

        assert!(wasm.starts_with(b"HTTP/1.1 200 OK"), "{wasm:?}");
        assert!(contains_bytes(&wasm, b"content-type: application/wasm"));
        assert!(contains_bytes(&wasm, b"\0asm"));
    }
}
