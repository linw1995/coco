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

    #[snafu(display("Web graph store operation failed: {source}"))]
    WebGraphStore {
        source: crate::host::web_graph_store::Error,
    },

    #[snafu(display("Web graph model operation failed: {source}"))]
    WebGraphModel { source: crate::web_graph::Error },

    #[snafu(display("Web graph store is not initialized"))]
    WebGraphNotInitialized,

    #[snafu(display(
        "Web graph {layout} layout is missing parent {parent_id} while placing {node_id}"
    ))]
    WebGraphParentPlacementMissing {
        layout: &'static str,
        node_id: String,
        parent_id: String,
    },

    #[snafu(display("Web graph references missing source node {node_id}"))]
    WebGraphSourceNodeMissing { node_id: String },

    #[snafu(display("Web graph revision {revision} cannot be advanced"))]
    WebGraphRevisionExhausted { revision: u64 },

    #[snafu(display("{source}"))]
    Store { source: coco_mem::StoreError },
}

pub type Result<T> = std::result::Result<T, Error>;
