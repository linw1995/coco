use coco_mem::Store;
use serde::Serialize;
use snafu::prelude::*;
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::config::ConsoleConfig;
use crate::error::{
    BindConsoleSnafu, ConfigureConsoleSocketSnafu, JoinConsoleServerSnafu, ServeConsoleSnafu,
};
use crate::graph::{
    GraphEntityKind, build_entity_collection, build_graph_snapshot, build_node_detail,
};
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

    match request.path.as_str() {
        "/" | "/index.html" => {
            match build_graph_snapshot(&state.store, state.publisher.current_version()) {
                Ok(snapshot) => {
                    let body = render_index_page(&snapshot);
                    write_response(
                        &mut stream,
                        200,
                        "OK",
                        "text/html; charset=utf-8",
                        body.as_bytes(),
                    )
                    .await
                }
                Err(error) => write_error(&mut stream, error).await,
            }
        }
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
        "/api/graph" => match build_graph_snapshot(&state.store, state.publisher.current_version())
        {
            Ok(snapshot) => write_json_response(&mut stream, &snapshot).await,
            Err(error) => write_error(&mut stream, error).await,
        },
        "/api/node" => {
            let Some(id) = request.query_value("id") else {
                return write_response(
                    &mut stream,
                    400,
                    "Bad Request",
                    "text/plain; charset=utf-8",
                    b"missing node id",
                )
                .await;
            };
            match build_node_detail(&state.store, &id) {
                Ok(node) => write_json_response(&mut stream, &node).await,
                Err(error) => write_error(&mut stream, error).await,
            }
        }
        "/api/entities" => {
            let Some(kind) = request
                .query_value("kind")
                .and_then(|value| GraphEntityKind::parse(&value))
            else {
                return write_response(
                    &mut stream,
                    400,
                    "Bad Request",
                    "text/plain; charset=utf-8",
                    b"unknown entity kind",
                )
                .await;
            };
            match build_entity_collection(&state.store, kind) {
                Ok(collection) => write_json_response(&mut stream, &collection).await,
                Err(error) => write_error(&mut stream, error).await,
            }
        }
        "/fragment" => write_fragment(&mut stream, state, request.version).await,
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

#[derive(Debug, PartialEq, Eq)]
struct HttpRequest {
    method: String,
    path: String,
    query: String,
    version: Option<u64>,
}

impl HttpRequest {
    fn query_value(&self, key: &str) -> Option<String> {
        parse_query_value(&self.query, key)
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
        query: query.to_owned(),
        version: parse_version_query(query),
    }))
}

fn parse_version_query(query: &str) -> Option<u64> {
    parse_query_value(query, "version").and_then(|value| value.parse().ok())
}

pub(super) fn parse_query_value(query: &str, name: &str) -> Option<String> {
    query.split('&').find_map(|part| {
        let (key, value) = part.split_once('=')?;
        (key == name).then(|| percent_decode_query_value(value))
    })
}

fn percent_decode_query_value(value: &str) -> String {
    let mut decoded = Vec::new();
    let bytes = value.as_bytes();
    let mut index = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'%' if index + 2 < bytes.len() => {
                let high = hex_value(bytes[index + 1]);
                let low = hex_value(bytes[index + 2]);
                if let (Some(high), Some(low)) = (high, low) {
                    decoded.push((high << 4) | low);
                    index += 3;
                } else {
                    decoded.push(bytes[index]);
                    index += 1;
                }
            }
            b'+' => {
                decoded.push(b' ');
                index += 1;
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }

    String::from_utf8_lossy(&decoded).into_owned()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

async fn write_json_response(
    stream: &mut tokio::net::TcpStream,
    value: &impl Serialize,
) -> io::Result<()> {
    match serde_json::to_vec(value) {
        Ok(body) => {
            write_response(stream, 200, "OK", "application/json; charset=utf-8", &body).await
        }
        Err(error) => {
            write_plain_error(stream, format!("failed to serialize response: {error}")).await
        }
    }
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
