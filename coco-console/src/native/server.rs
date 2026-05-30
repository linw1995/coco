use coco_mem::Store;
use snafu::prelude::*;
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::config::ConsoleConfig;
use crate::error::{
    BindConsoleSnafu, ConfigureConsoleSocketSnafu, JoinConsoleServerSnafu, ServeConsoleSnafu,
};
use crate::graph::build_graph_snapshot;
use crate::publisher::ConsolePublisher;
use crate::render::{render_fragment, render_index_page};
use crate::{Error, Result};

const REQUEST_HEADER_LIMIT: usize = 16 * 1024;
const STYLE_CSS: &str = include_str!("style.css");
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
        let (stream, _) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            let _ = handle_connection(stream, state).await;
        });
    }
}

async fn handle_connection<S>(
    mut stream: tokio::net::TcpStream,
    state: AppState<S>,
) -> io::Result<()>
where
    S: Store + Clone + Send + Sync + 'static,
{
    let Some(request) = read_request(&mut stream).await? else {
        return Ok(());
    };

    handle_request(stream, request, state).await
}

async fn handle_request<S>(
    mut stream: tokio::net::TcpStream,
    request: HttpRequest,
    state: AppState<S>,
) -> io::Result<()>
where
    S: Store + Clone + Send + Sync + 'static,
{
    if request.method != "GET" {
        return write_response(
            &mut stream,
            405,
            "Method Not Allowed",
            "text/plain; charset=utf-8",
            b"method not allowed",
        )
        .await;
    }

    handle_get_request(stream, request.path, request.version, state).await
}

async fn handle_get_request<S>(
    mut stream: tokio::net::TcpStream,
    path: String,
    observed_version: Option<u64>,
    state: AppState<S>,
) -> io::Result<()>
where
    S: Store + Clone + Send + Sync + 'static,
{
    match path.as_str() {
        "/" | "/index.html" => write_index_page(&mut stream, &state).await,
        "/style.css" => {
            write_response(
                &mut stream,
                200,
                "OK",
                "text/css; charset=utf-8",
                STYLE_CSS.as_bytes(),
            )
            .await
        }
        "/api/graph" => write_graph_json(&mut stream, &state).await,
        "/fragment" => write_fragment(&mut stream, state, observed_version).await,
        "/events" => write_event_stream(stream, state.publisher).await,
        "/pkg/coco_console.js" => {
            write_asset_file(
                &mut stream,
                "coco_console.js",
                "text/javascript; charset=utf-8",
            )
            .await
        }
        "/pkg/coco_console_bg.wasm" => {
            write_asset_file(&mut stream, "coco_console_bg.wasm", "application/wasm").await
        }
        _ => {
            write_response(
                &mut stream,
                404,
                "Not Found",
                "text/plain; charset=utf-8",
                b"not found",
            )
            .await
        }
    }
}

async fn write_index_page<S>(
    stream: &mut tokio::net::TcpStream,
    state: &AppState<S>,
) -> io::Result<()>
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
            .await
        }
        Err(error) => write_error(stream, error).await,
    }
}

async fn write_graph_json<S>(
    stream: &mut tokio::net::TcpStream,
    state: &AppState<S>,
) -> io::Result<()>
where
    S: Store + Clone + Send + Sync + 'static,
{
    let snapshot = match build_graph_snapshot(&state.store, state.publisher.current_version()) {
        Ok(snapshot) => snapshot,
        Err(error) => return write_error(stream, error).await,
    };
    match serde_json::to_vec(&snapshot) {
        Ok(body) => {
            write_response(stream, 200, "OK", "application/json; charset=utf-8", &body).await
        }
        Err(error) => {
            write_plain_error(stream, format!("failed to serialize graph: {error}")).await
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct HttpRequest {
    method: String,
    path: String,
    version: Option<u64>,
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

    let header = String::from_utf8_lossy(&buffer);
    let mut parts = header.lines().next().unwrap_or_default().split_whitespace();
    let Some(method) = parts.next() else {
        return Ok(None);
    };
    let Some(target) = parts.next() else {
        return Ok(None);
    };
    let (path, query) = target.split_once('?').unwrap_or((target, ""));

    Ok(Some(HttpRequest {
        method: method.to_owned(),
        path: path.to_owned(),
        version: parse_version_query(query),
    }))
}

fn parse_version_query(query: &str) -> Option<u64> {
    query.split('&').find_map(|part| {
        let (key, value) = part.split_once('=')?;
        (key == "version").then(|| value.parse().ok()).flatten()
    })
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

async fn write_error(stream: &mut tokio::net::TcpStream, error: Error) -> io::Result<()> {
    write_plain_error(stream, error.to_string()).await
}

async fn write_plain_error(
    stream: &mut tokio::net::TcpStream,
    message: impl AsRef<str>,
) -> io::Result<()> {
    write_response(
        stream,
        500,
        "Internal Server Error",
        "text/plain; charset=utf-8",
        message.as_ref().as_bytes(),
    )
    .await
}

async fn write_fragment<S>(
    stream: &mut tokio::net::TcpStream,
    state: AppState<S>,
    observed_version: Option<u64>,
) -> io::Result<()>
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
            .await
        }
        Err(error) => write_error(stream, error).await,
    }
}

async fn write_asset_file(
    stream: &mut tokio::net::TcpStream,
    name: &str,
    content_type: &str,
) -> io::Result<()> {
    let Some(path) = find_asset_file(name) else {
        return write_response(
            stream,
            404,
            "Not Found",
            "text/plain; charset=utf-8",
            b"console wasm asset not found",
        )
        .await;
    };

    match std::fs::read(path) {
        Ok(body) => write_response(stream, 200, "OK", content_type, &body).await,
        Err(error) => {
            write_plain_error(stream, format!("failed to read wasm asset: {error}")).await
        }
    }
}

fn find_asset_file(name: &str) -> Option<PathBuf> {
    let configured = std::env::var_os("COCO_CONSOLE_ASSET_DIR")
        .map(PathBuf::from)
        .map(|dir| dir.join(name));
    configured
        .into_iter()
        .chain(asset_candidates(name))
        .find(|path| path.is_file())
}

fn asset_candidates(name: &str) -> impl Iterator<Item = PathBuf> + '_ {
    [Path::new("coco-console/pkg"), Path::new("pkg")]
        .into_iter()
        .map(move |dir| dir.join(name))
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
    mut stream: tokio::net::TcpStream,
    publisher: ConsolePublisher,
) -> io::Result<()> {
    stream
        .write_all(
            b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream; charset=utf-8\r\ncache-control: no-store\r\nconnection: keep-alive\r\n\r\n",
        )
        .await?;
    write_graph_event(&mut stream, publisher.current_version()).await?;

    let mut rx = publisher.subscribe();
    while rx.changed().await.is_ok() {
        let version = *rx.borrow_and_update();
        write_graph_event(&mut stream, version).await?;
    }

    Ok(())
}

async fn write_graph_event(stream: &mut tokio::net::TcpStream, version: u64) -> io::Result<()> {
    stream
        .write_all(format!("event: graph\ndata: {version}\n\n").as_bytes())
        .await
}

#[cfg(test)]
mod tests {
    use super::{AppState, handle_connection, read_request};
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
            let (stream, _) = listener.accept().await.unwrap();
            handle_connection(stream, test_state()).await.unwrap();
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
        assert_eq!(request.version, Some(42));
    }

    #[tokio::test]
    async fn read_request_ignores_invalid_or_missing_version_query() {
        let invalid = read_request_from(b"GET /fragment?version=bad HTTP/1.1\r\n\r\n")
            .await
            .expect("request should parse");
        let missing = read_request_from(b"GET /fragment HTTP/1.1\r\n\r\n")
            .await
            .expect("request should parse");

        assert_eq!(invalid.version, None);
        assert_eq!(missing.version, None);
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
