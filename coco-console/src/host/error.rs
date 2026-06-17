use std::io;
use std::net::SocketAddr;

use snafu::prelude::*;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to bind console address {addr}: {source}"))]
    BindConsole { addr: SocketAddr, source: io::Error },

    #[snafu(display("Failed to configure console socket for {addr}: {source}"))]
    ConfigureConsoleSocket { addr: SocketAddr, source: io::Error },

    #[snafu(display("Console server {addr} failed: {source}"))]
    ServeConsole { addr: SocketAddr, source: io::Error },

    #[snafu(display("Console server task failed: {source}"))]
    JoinConsoleServer { source: tokio::task::JoinError },

    #[snafu(display(
        "Console graph rebuild failed for {mode} at store version {source_version}: {message}"
    ))]
    ConsoleGraphRebuild {
        mode: &'static str,
        source_version: u64,
        message: String,
    },

    #[snafu(display("{source}"))]
    Store { source: coco_mem::StoreError },
}

pub type Result<T> = std::result::Result<T, Error>;
