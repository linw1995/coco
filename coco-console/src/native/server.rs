use coco_mem::Store;
use snafu::prelude::*;
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::config::ConsoleConfig;
use crate::error::{
    BindConsoleSnafu, ConfigureConsoleSocketSnafu, JoinConsoleServerSnafu, ServeConsoleSnafu,
};
use crate::graph::build_graph_snapshot;
use crate::layout::{
    GraphViewportDiffRequest, GraphViewportKnownItems, GraphViewportRequest, layout_graph_viewport,
    layout_graph_viewport_diff,
};
use crate::publisher::ConsolePublisher;
use crate::render::{render_fragment, render_index_page};
use crate::{Error, Result};

const REQUEST_HEADER_LIMIT: usize = 16 * 1024;
const REQUEST_BODY_LIMIT: usize = 1024 * 1024;
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
        serve_console(listener, state, addr)
            .await
            .context(ServeConsoleSnafu { addr })
    });

    Ok(ConsoleServerHandle { addr, task })
}

async fn serve_console<S>(
    listener: tokio::net::TcpListener,
    state: AppState<S>,
    _addr: SocketAddr,
) -> io::Result<()>
where
    S: Store + Clone + Send + Sync + 'static,
{
    loop {
        let (stream, peer_addr) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            let _ = handle_connection(stream, peer_addr, state).await;
        });
    }
}

async fn handle_connection<S>(
    mut stream: tokio::net::TcpStream,
    peer_addr: SocketAddr,
    state: AppState<S>,
) -> io::Result<()>
where
    S: Store + Clone + Send + Sync + 'static,
{
    let Some(request) = read_request(&mut stream).await? else {
        return Ok(());
    };

    handle_request(stream, peer_addr, request, state).await
}

async fn handle_request<S>(
    mut stream: tokio::net::TcpStream,
    peer_addr: SocketAddr,
    request: HttpRequest,
    state: AppState<S>,
) -> io::Result<()>
where
    S: Store + Clone + Send + Sync + 'static,
{
    let access_log = AccessLog::new(peer_addr, &request);
    let result = handle_request_inner(&mut stream, request, state).await;
    match &result {
        Ok(status) => access_log.log(*status),
        Err(error) => access_log.log_error(error),
    }
    result.map(|_| ())
}

async fn handle_request_inner<S>(
    stream: &mut tokio::net::TcpStream,
    request: HttpRequest,
    state: AppState<S>,
) -> io::Result<u16>
where
    S: Store + Clone + Send + Sync + 'static,
{
    if request.method == "GET" {
        return handle_get_request(stream, request, state).await;
    }

    if request.method == "POST" && request.path == "/api/graph/viewport/diff" {
        let params = request.params();
        return write_graph_viewport_diff_json(stream, state, request.version(), &params).await;
    }

    write_response(
        stream,
        405,
        "Method Not Allowed",
        "text/plain; charset=utf-8",
        b"method not allowed",
    )
    .await?;
    Ok(405)
}

async fn handle_get_request<S>(
    stream: &mut tokio::net::TcpStream,
    request: HttpRequest,
    state: AppState<S>,
) -> io::Result<u16>
where
    S: Store + Clone + Send + Sync + 'static,
{
    match request.path.as_str() {
        "/" | "/index.html" => write_index_page(stream, &state).await,
        "/style.css" => {
            write_response(
                stream,
                200,
                "OK",
                "text/css; charset=utf-8",
                STYLE_CSS.as_bytes(),
            )
            .await?;
            Ok(200)
        }
        "/api/graph" => write_graph_json(stream, &state).await,
        "/api/graph/viewport" => {
            write_graph_viewport_json(stream, state, request.version(), &request.query).await
        }
        "/api/graph/viewport/diff" => {
            write_graph_viewport_diff_json(stream, state, request.version(), &request.query).await
        }
        "/fragment" => write_fragment(stream, state, request.version()).await,
        "/events" => write_event_stream(stream, state.publisher).await,
        "/pkg/coco_console.js" => {
            write_response(
                stream,
                200,
                "OK",
                "text/javascript; charset=utf-8",
                COCO_CONSOLE_JS,
            )
            .await?;
            Ok(200)
        }
        "/pkg/coco_console_bg.wasm" => {
            write_response(stream, 200, "OK", "application/wasm", COCO_CONSOLE_WASM).await?;
            Ok(200)
        }
        _ => {
            write_response(
                stream,
                404,
                "Not Found",
                "text/plain; charset=utf-8",
                b"not found",
            )
            .await?;
            Ok(404)
        }
    }
}

struct AccessLog {
    peer_addr: SocketAddr,
    method: String,
    path: String,
    started_at: Instant,
}

impl AccessLog {
    fn new(peer_addr: SocketAddr, request: &HttpRequest) -> Self {
        Self {
            peer_addr,
            method: request.method.clone(),
            path: request.path.clone(),
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

async fn write_index_page<S>(
    stream: &mut tokio::net::TcpStream,
    state: &AppState<S>,
) -> io::Result<u16>
where
    S: Store + Clone + Send + Sync + 'static,
{
    match build_graph_snapshot(&state.store, state.publisher.current_version()) {
        Ok(snapshot) => {
            let body = render_index_page(&snapshot);
            write_response(
                stream,
                200,
                "OK",
                "text/html; charset=utf-8",
                body.as_bytes(),
            )
            .await?;
            Ok(200)
        }
        Err(error) => write_error(stream, error).await,
    }
}

async fn write_graph_json<S>(
    stream: &mut tokio::net::TcpStream,
    state: &AppState<S>,
) -> io::Result<u16>
where
    S: Store + Clone + Send + Sync + 'static,
{
    let snapshot = match build_graph_snapshot(&state.store, state.publisher.current_version()) {
        Ok(snapshot) => snapshot,
        Err(error) => return write_error(stream, error).await,
    };
    match serde_json::to_vec(&snapshot) {
        Ok(body) => {
            write_response(stream, 200, "OK", "application/json; charset=utf-8", &body).await?;
            Ok(200)
        }
        Err(error) => {
            write_plain_error(stream, format!("failed to serialize graph: {error}")).await
        }
    }
}

async fn write_graph_viewport_json<S>(
    stream: &mut tokio::net::TcpStream,
    state: AppState<S>,
    observed_version: Option<u64>,
    query: &QueryParams,
) -> io::Result<u16>
where
    S: Store + Clone + Send + Sync + 'static,
{
    wait_for_newer_version(&state.publisher, observed_version).await;
    let snapshot = match build_graph_snapshot(&state.store, state.publisher.current_version()) {
        Ok(snapshot) => snapshot,
        Err(error) => return write_error(stream, error).await,
    };
    let response = layout_graph_viewport(&snapshot, viewport_request_from_query(query));
    match serde_json::to_vec(&response) {
        Ok(body) => {
            write_response(stream, 200, "OK", "application/json; charset=utf-8", &body).await?;
            Ok(200)
        }
        Err(error) => {
            write_plain_error(
                stream,
                format!("failed to serialize graph viewport: {error}"),
            )
            .await
        }
    }
}

async fn write_graph_viewport_diff_json<S>(
    stream: &mut tokio::net::TcpStream,
    state: AppState<S>,
    observed_version: Option<u64>,
    query: &QueryParams,
) -> io::Result<u16>
where
    S: Store + Clone + Send + Sync + 'static,
{
    wait_for_newer_version(&state.publisher, observed_version).await;
    let snapshot = match build_graph_snapshot(&state.store, state.publisher.current_version()) {
        Ok(snapshot) => snapshot,
        Err(error) => return write_error(stream, error).await,
    };
    let response = layout_graph_viewport_diff(&snapshot, viewport_diff_request_from_query(query));
    match serde_json::to_vec(&response) {
        Ok(body) => {
            write_response(stream, 200, "OK", "application/json; charset=utf-8", &body).await?;
            Ok(200)
        }
        Err(error) => {
            write_plain_error(
                stream,
                format!("failed to serialize graph viewport diff: {error}"),
            )
            .await
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct HttpRequest {
    method: String,
    path: String,
    query: QueryParams,
    body: QueryParams,
}

impl HttpRequest {
    fn version(&self) -> Option<u64> {
        self.query
            .u64("version")
            .or_else(|| self.body.u64("version"))
    }

    fn params(&self) -> QueryParams {
        let mut params = self.query.clone();
        params.pairs.extend(self.body.pairs.clone());
        params
    }
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
}

async fn read_request(stream: &mut tokio::net::TcpStream) -> io::Result<Option<HttpRequest>> {
    let mut buffer = Vec::new();
    let mut chunk = [0; 1024];

    while buffer.len() < REQUEST_HEADER_LIMIT {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if buffer.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }

    if buffer.is_empty() {
        return Ok(None);
    }

    let Some(header_end) = buffer.windows(4).position(|window| window == b"\r\n\r\n") else {
        return Ok(None);
    };
    let header_end = header_end + 4;
    let header = String::from_utf8_lossy(&buffer[..header_end]);
    let mut parts = header.lines().next().unwrap_or_default().split_whitespace();
    let Some(method) = parts.next() else {
        return Ok(None);
    };
    let Some(target) = parts.next() else {
        return Ok(None);
    };
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let content_length = content_length_from_header(&header).unwrap_or_default();
    if content_length > REQUEST_BODY_LIMIT {
        return Ok(None);
    }
    let mut body = buffer[header_end..].to_vec();
    while body.len() < content_length {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..read]);
    }
    body.truncate(content_length);
    let body = String::from_utf8_lossy(&body);

    Ok(Some(HttpRequest {
        method: method.to_owned(),
        path: path.to_owned(),
        query: parse_query(query),
        body: parse_query(&body),
    }))
}

fn content_length_from_header(header: &str) -> Option<usize> {
    header.lines().skip(1).find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("content-length")
            .then(|| value.trim().parse().ok())
            .flatten()
    })
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

async fn write_response(
    stream: &mut tokio::net::TcpStream,
    status: u16,
    reason: &str,
    content_type: &str,
    body: &[u8],
) -> io::Result<()> {
    let header = format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\ncache-control: no-store\r\nconnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(body).await
}

async fn write_error(stream: &mut tokio::net::TcpStream, error: Error) -> io::Result<u16> {
    write_plain_error(stream, error.to_string()).await
}

async fn write_plain_error(
    stream: &mut tokio::net::TcpStream,
    message: impl AsRef<str>,
) -> io::Result<u16> {
    write_response(
        stream,
        500,
        "Internal Server Error",
        "text/plain; charset=utf-8",
        message.as_ref().as_bytes(),
    )
    .await?;
    Ok(500)
}

async fn write_fragment<S>(
    stream: &mut tokio::net::TcpStream,
    state: AppState<S>,
    observed_version: Option<u64>,
) -> io::Result<u16>
where
    S: Store + Clone + Send + Sync + 'static,
{
    wait_for_newer_version(&state.publisher, observed_version).await;
    match build_graph_snapshot(&state.store, state.publisher.current_version()) {
        Ok(snapshot) => {
            let body = render_fragment(&snapshot);
            write_response(
                stream,
                200,
                "OK",
                "text/html; charset=utf-8",
                body.as_bytes(),
            )
            .await?;
            Ok(200)
        }
        Err(error) => write_error(stream, error).await,
    }
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

async fn write_event_stream(
    stream: &mut tokio::net::TcpStream,
    publisher: ConsolePublisher,
) -> io::Result<u16> {
    stream
        .write_all(
            b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream; charset=utf-8\r\ncache-control: no-store\r\nconnection: keep-alive\r\n\r\n",
        )
        .await?;
    write_graph_event(stream, publisher.current_version()).await?;

    let mut rx = publisher.subscribe();
    while rx.changed().await.is_ok() {
        let version = *rx.borrow_and_update();
        write_graph_event(stream, version).await?;
    }

    Ok(200)
}

async fn write_graph_event(stream: &mut tokio::net::TcpStream, version: u64) -> io::Result<()> {
    stream
        .write_all(format!("event: graph\ndata: {version}\n\n").as_bytes())
        .await
}

#[cfg(test)]
mod tests {
    use super::{
        AppState, handle_connection, parse_query, read_request, viewport_diff_request_from_query,
    };
    use coco_mem::MemoryStore;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn test_state() -> AppState<MemoryStore> {
        AppState {
            store: MemoryStore::new(),
            publisher: crate::ConsolePublisher::new(),
        }
    }

    async fn read_request_from(bytes: &[u8]) -> Option<super::HttpRequest> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::spawn({
            let bytes = bytes.to_vec();
            async move {
                let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
                stream.write_all(&bytes).await.unwrap();
            }
        });
        let (mut stream, _) = listener.accept().await.unwrap();

        let request = read_request(&mut stream).await.unwrap();
        client.await.unwrap();
        request
    }

    async fn response_bytes_from(bytes: &[u8]) -> Vec<u8> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, peer_addr) = listener.accept().await.unwrap();
            handle_connection(stream, peer_addr, test_state())
                .await
                .unwrap();
        });

        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        client.write_all(bytes).await.unwrap();
        client.shutdown().await.unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        server.await.unwrap();

        response
    }

    async fn response_from(bytes: &[u8]) -> String {
        let response = response_bytes_from(bytes).await;
        String::from_utf8(response).unwrap()
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

    #[tokio::test]
    async fn read_request_parses_path_and_version_query() {
        let request = read_request_from(
            b"GET /fragment?ignored=true&version=42 HTTP/1.1\r\nhost: localhost\r\n\r\n",
        )
        .await
        .expect("request should parse");

        assert_eq!(request.method, "GET");
        assert_eq!(request.path, "/fragment");
        assert_eq!(request.version(), Some(42));
    }

    #[tokio::test]
    async fn read_request_parses_form_body() {
        let request = read_request_from(
            b"POST /api/graph/viewport/diff HTTP/1.1\r\nhost: localhost\r\ncontent-length: 24\r\n\r\nversion=42&known_node=n1",
        )
        .await
        .expect("request should parse");

        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/api/graph/viewport/diff");
        assert_eq!(request.version(), Some(42));
        assert_eq!(request.body.get_all("known_node"), vec!["n1"]);
    }

    #[tokio::test]
    async fn read_request_ignores_invalid_or_missing_version_query() {
        let invalid = read_request_from(b"GET /fragment?version=bad HTTP/1.1\r\n\r\n")
            .await
            .expect("request should parse");
        let missing = read_request_from(b"GET /fragment HTTP/1.1\r\n\r\n")
            .await
            .expect("request should parse");

        assert_eq!(invalid.version(), None);
        assert_eq!(missing.version(), None);
    }

    #[tokio::test]
    async fn read_request_returns_none_for_empty_or_incomplete_request_line() {
        assert_eq!(read_request_from(b"").await, None);
        assert_eq!(read_request_from(b"GET\r\n\r\n").await, None);
    }

    #[tokio::test]
    async fn handle_connection_rejects_non_get_requests() {
        let response = response_from(b"POST / HTTP/1.1\r\nhost: localhost\r\n\r\n").await;

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
            "POST /api/graph/viewport/diff HTTP/1.1\r\nhost: localhost\r\ncontent-length: {}\r\n\r\n{}",
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
        let response = response_from(b"GET /missing HTTP/1.1\r\nhost: localhost\r\n\r\n").await;

        assert_response(
            &response,
            "HTTP/1.1 404 Not Found",
            "content-type: text/plain; charset=utf-8",
            "not found",
        );
    }

    #[tokio::test]
    async fn handle_connection_serves_index_page() {
        let response = response_from(b"GET /index.html HTTP/1.1\r\nhost: localhost\r\n\r\n").await;

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
        let response = response_from(b"GET /style.css HTTP/1.1\r\nhost: localhost\r\n\r\n").await;

        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(
            response.contains("content-type: text/css; charset=utf-8"),
            "{response}"
        );
        assert!(response.contains("font-family"), "{response}");
    }

    #[tokio::test]
    async fn handle_connection_serves_graph_json() {
        let response = response_from(b"GET /api/graph HTTP/1.1\r\nhost: localhost\r\n\r\n").await;

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
            b"GET /api/graph/viewport?x=10&y=20&width=640&height=360&overscan=40 HTTP/1.1\r\nhost: localhost\r\n\r\n",
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
            b"GET /api/graph/viewport/diff?previous_x=0&previous_y=0&previous_width=320&previous_height=180&x=10&y=20&width=640&height=360&overscan=40 HTTP/1.1\r\nhost: localhost\r\n\r\n",
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
            b"GET /api/graph/viewport/diff?x=0&y=0&width=640&height=360&known_node=node:stale&known_edge=edge:primary_parent:base:stale HTTP/1.1\r\nhost: localhost\r\n\r\n",
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
            b"GET /api/graph/viewport/diff?x=0&y=0&width=640&height=360&known=1 HTTP/1.1\r\nhost: localhost\r\n\r\n",
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
        let response = response_from(b"GET /fragment HTTP/1.1\r\nhost: localhost\r\n\r\n").await;

        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(
            response.contains("content-type: text/html; charset=utf-8"),
            "{response}"
        );
        assert!(response.contains("data-version=\"0\""), "{response}");
    }

    #[tokio::test]
    async fn handle_connection_serves_assets() {
        let js =
            response_bytes_from(b"GET /pkg/coco_console.js HTTP/1.1\r\nhost: localhost\r\n\r\n")
                .await;
        let wasm = response_bytes_from(
            b"GET /pkg/coco_console_bg.wasm HTTP/1.1\r\nhost: localhost\r\n\r\n",
        )
        .await;

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
