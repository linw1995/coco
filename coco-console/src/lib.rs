#[cfg(target_arch = "wasm32")]
mod client;

#[cfg(not(target_arch = "wasm32"))]
mod native {
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
        GraphBranch, GraphEdge, GraphEdgeKind, GraphEntityCollection, GraphEntityCounts,
        GraphEntityKind, GraphJob, GraphNode, GraphPreset, GraphQueue, GraphQueueMessage,
        GraphSession, GraphSkill, GraphSnapshot, build_entity_collection, build_graph_snapshot,
        build_node_detail,
    };
    pub use publisher::ConsolePublisher;
    pub use server::{ConsoleServerHandle, start_console_server};
    pub use store::ConsoleStore;

    #[cfg(test)]
    mod tests;
}

#[cfg(not(target_arch = "wasm32"))]
pub use native::{
    ConsoleConfig, ConsolePublisher, ConsoleServerHandle, ConsoleStore, Error, GraphBranch,
    GraphEdge, GraphEdgeKind, GraphEntityCollection, GraphEntityCounts, GraphEntityKind, GraphJob,
    GraphNode, GraphPreset, GraphQueue, GraphQueueMessage, GraphSession, GraphSkill, GraphSnapshot,
    Result, build_entity_collection, build_graph_snapshot, build_node_detail, default_console_addr,
    start_console_server,
};
#[cfg(not(target_arch = "wasm32"))]
use native::{config, error, graph, layout, publisher, render};
