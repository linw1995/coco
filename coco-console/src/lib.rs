pub mod api;

#[cfg(target_arch = "wasm32")]
mod client;

#[cfg(any(target_arch = "wasm32", test))]
mod viewport;

#[cfg(not(target_arch = "wasm32"))]
mod host {
    pub mod config;
    pub mod error;
    pub mod graph;
    pub mod layout;
    pub mod publisher;
    pub mod render;
    pub mod server;
    pub mod store;

    pub use config::{ConsoleConfig, default_console_addr};
    pub use error::{Error, Result};
    pub use graph::{
        GraphBranch, GraphEdge, GraphEdgeKind, GraphNode, GraphSnapshot, build_graph_snapshot,
    };
    pub use layout::{layout_graph_viewport, layout_graph_viewport_diff};
    pub use publisher::ConsolePublisher;
    pub use server::{ConsoleServerHandle, start_console_server};
    pub use store::ConsoleStore;

    #[cfg(test)]
    mod tests;
}

pub use api::{
    GraphCanvas, GraphViewport, GraphViewportDiffRequest, GraphViewportDiffResponse,
    GraphViewportEdge, GraphViewportEdgeKind, GraphViewportItemKind, GraphViewportItems,
    GraphViewportKnownItems, GraphViewportLane, GraphViewportNode, GraphViewportRemovedItem,
    GraphViewportRequest, GraphViewportResponse, Point,
};
#[cfg(not(target_arch = "wasm32"))]
pub use host::{
    ConsoleConfig, ConsolePublisher, ConsoleServerHandle, ConsoleStore, Error, GraphBranch,
    GraphEdge, GraphEdgeKind, GraphNode, GraphSnapshot, Result, build_graph_snapshot,
    default_console_addr, layout_graph_viewport, layout_graph_viewport_diff, start_console_server,
};
#[cfg(not(target_arch = "wasm32"))]
use host::{config, error, graph, layout, publisher, render};
