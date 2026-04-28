use coco_mem::Store;
use snafu::prelude::*;
use std::io;
use std::net::{SocketAddr, TcpListener};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::config::ConsoleConfig;
use crate::error::{
    BindConsoleSnafu, ConfigureConsoleSocketSnafu, JoinConsoleServerSnafu, ServeConsoleSnafu,
};
use crate::graph::build_graph_snapshot;
use crate::publisher::ConsolePublisher;
use crate::render::render_index;
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
            match render_index(&state.store, state.publisher.current_version()) {
                Ok(body) => {
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
            Ok(snapshot) => match serde_json::to_vec(&snapshot) {
                Ok(body) => {
                    write_response(
                        &mut stream,
                        200,
                        "OK",
                        "application/json; charset=utf-8",
                        &body,
                    )
                    .await
                }
                Err(error) => {
                    write_plain_error(&mut stream, format!("failed to serialize graph: {error}"))
                        .await
                }
            },
            Err(error) => write_error(&mut stream, error).await,
        },
        "/events" => write_event_stream(stream, state.publisher).await,
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
    let path = target.split('?').next().unwrap_or("/");

    Ok(Some(HttpRequest {
        method: method.to_owned(),
        path: path.to_owned(),
    }))
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
