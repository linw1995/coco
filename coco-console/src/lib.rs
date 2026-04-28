mod config;
mod error;
mod graph;
mod layout;
mod publisher;
mod render;
mod server;
mod store;

#[cfg(test)]
mod tests;

pub use config::{ConsoleConfig, default_console_addr};
pub use error::{Error, Result};
pub use graph::{
    GraphBranch, GraphEdge, GraphEdgeKind, GraphNode, GraphSnapshot, build_graph_snapshot,
};
pub use publisher::ConsolePublisher;
pub use server::{ConsoleServerHandle, start_console_server};
pub use store::ConsoleStore;
