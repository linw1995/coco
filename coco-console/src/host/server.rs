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
use std::time::Instant;

use crate::config::ConsoleConfig;
use crate::error::{
    BindConsoleSnafu, ConfigureConsoleSocketSnafu, JoinConsoleServerSnafu, ServeConsoleSnafu,
};
use crate::graph::build_graph_snapshot;
use crate::host::api::{GraphViewportDiffRequest, GraphViewportKnownItems, GraphViewportRequest};
use crate::layout::{layout_graph_viewport, layout_graph_viewport_diff};
use crate::publisher::ConsolePublisher;
use crate::render::{render_fragment, render_index_page, render_node_detail_fragment};
use crate::{Error, Result};

const STYLE_CSS: &str = include_str!("style.css");
const COCO_CONSOLE_JS: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/pkg/coco_console.js"));
const COCO_CONSOLE_WASM: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/pkg/coco_console_bg.wasm"));

#[derive(Clone)]
struct AppState<S> {
    store: S,
    publisher: ConsolePublisher,
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

pub fn start_console_server<S>(
    config: ConsoleConfig,
    store: S,
    publisher: ConsolePublisher,
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
    let state = AppState { store, publisher };
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
            "/api/graph/viewport/diff",
            get(graph_viewport_diff_get::<S>).post(graph_viewport_diff_post::<S>),
        )
        .route("/api/node-detail", get(node_detail::<S>))
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

async fn index_page<S>(State(state): State<AppState<S>>) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    match build_graph_snapshot(&state.store, state.publisher.current_version()) {
        Ok(snapshot) => html_response(render_index_page(&snapshot)),
        Err(error) => error_response(error),
    }
}

async fn style_css() -> Response {
    response_with_body(
        StatusCode::OK,
        "text/css; charset=utf-8",
        Body::from(STYLE_CSS),
    )
}

async fn graph_json<S>(State(state): State<AppState<S>>) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    match build_graph_snapshot(&state.store, state.publisher.current_version()) {
        Ok(snapshot) => json_response(&snapshot, "graph"),
        Err(error) => error_response(error),
    }
}

async fn graph_viewport<S>(State(state): State<AppState<S>>, RawQuery(query): RawQuery) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    let query = parse_query(query.as_deref().unwrap_or_default());
    wait_for_newer_version(&state.publisher, query.version()).await;
    let snapshot = match build_graph_snapshot(&state.store, state.publisher.current_version()) {
        Ok(snapshot) => snapshot,
        Err(error) => return error_response(error),
    };
    let response = layout_graph_viewport(&snapshot, viewport_request_from_query(&query));
    json_response(&response, "graph viewport")
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

async fn graph_viewport_diff_response<S>(state: AppState<S>, query: QueryParams) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    wait_for_newer_version(&state.publisher, query.version()).await;
    let snapshot = match build_graph_snapshot(&state.store, state.publisher.current_version()) {
        Ok(snapshot) => snapshot,
        Err(error) => return error_response(error),
    };
    let response = layout_graph_viewport_diff(&snapshot, viewport_diff_request_from_query(&query));
    json_response(&response, "graph viewport diff")
}

async fn fragment<S>(State(state): State<AppState<S>>, RawQuery(query): RawQuery) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    let query = parse_query(query.as_deref().unwrap_or_default());
    wait_for_newer_version(&state.publisher, query.version()).await;
    match build_graph_snapshot(&state.store, state.publisher.current_version()) {
        Ok(snapshot) => html_response(render_fragment(&snapshot)),
        Err(error) => error_response(error),
    }
}

async fn node_detail<S>(State(state): State<AppState<S>>, RawQuery(query): RawQuery) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    let query = parse_query(query.as_deref().unwrap_or_default());
    wait_for_newer_version(&state.publisher, query.version()).await;
    match build_graph_snapshot(&state.store, state.publisher.current_version()) {
        Ok(snapshot) => html_response(render_node_detail_fragment(&snapshot, query.get("target"))),
        Err(error) => error_response(error),
    }
}

async fn event_stream<S>(State(state): State<AppState<S>>) -> Response
where
    S: Store + Clone + Send + Sync + 'static,
{
    let current_version = state.publisher.current_version();
    let rx = state.publisher.subscribe();
    let initial = stream::once(async move {
        Ok::<_, Infallible>(
            Event::default()
                .event("graph")
                .data(current_version.to_string()),
        )
    });
    let changes = stream::unfold(rx, |mut rx| async move {
        if rx.changed().await.is_err() {
            return None;
        }
        let version = *rx.borrow_and_update();
        Some((
            Ok::<_, Infallible>(Event::default().event("graph").data(version.to_string())),
            rx,
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

fn known_items_from_query(query: &QueryParams) -> Option<GraphViewportKnownItems> {
    let known = GraphViewportKnownItems {
        lanes: query.get_all("known_lane"),
        nodes: query.get_all("known_node"),
        edges: query.get_all("known_edge"),
    };
    (query.contains_key("known")
        || !known.lanes.is_empty()
        || !known.nodes.is_empty()
        || !known.edges.is_empty())
    .then_some(known)
}

async fn wait_for_newer_version(publisher: &ConsolePublisher, observed_version: Option<u64>) {
    let Some(observed_version) = observed_version else {
        return;
    };
    if publisher.current_version() > observed_version {
        return;
    }

    let mut rx = publisher.subscribe();
    while *rx.borrow_and_update() <= observed_version {
        if rx.changed().await.is_err() {
            return;
        }
    }
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

fn error_response(error: Error) -> Response {
    plain_error(error.to_string())
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
    use super::{parse_query, start_console_server, viewport_diff_request_from_query};
    use crate::{ConsoleConfig, ConsolePublisher};
    use coco_mem::MemoryStore;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn test_config() -> ConsoleConfig {
        ConsoleConfig {
            addr: "127.0.0.1:0".parse().unwrap(),
        }
    }

    fn get_request(path: &str) -> String {
        format!("GET {path} HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n")
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
        let handle =
            start_console_server(test_config(), MemoryStore::new(), publisher.clone()).unwrap();
        let mut stream = tokio::net::TcpStream::connect(handle.addr()).await.unwrap();
        stream
            .write_all(b"GET /events HTTP/1.1\r\nhost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut response = vec![0; 256];

        let mut initial = Vec::new();
        while !contains_bytes(&initial, b"event: graph\ndata: 0") {
            let read = stream.read(&mut response).await.unwrap();
            assert_ne!(read, 0);
            initial.extend_from_slice(&response[..read]);
        }
        let initial = String::from_utf8_lossy(&initial);
        assert!(initial.starts_with("HTTP/1.1 200 OK"), "{initial}");
        assert!(initial.contains("content-type: text/event-stream"));

        publisher.mark_changed();
        let mut changed = Vec::new();
        while !contains_bytes(&changed, b"event: graph\ndata: 1") {
            let read = stream.read(&mut response).await.unwrap();
            assert_ne!(read, 0);
            changed.extend_from_slice(&response[..read]);
        }

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
        assert!(response.contains("\"kind\":\"node\""), "{response}");
        assert!(response.contains("\"key\":\"node:stale\""), "{response}");
        assert!(response.contains("\"kind\":\"edge\""), "{response}");
        assert!(
            response.contains("\"key\":\"edge:primary_parent:base:stale\""),
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
        assert!(response.contains("\"kind\":\"node\""), "{response}");
        assert!(response.contains("\"key\":\"node:stale\""), "{response}");
        assert!(response.contains("\"kind\":\"edge\""), "{response}");
        assert!(
            response.contains("\"key\":\"edge:primary_parent:base:stale\""),
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
