mod api;

#[cfg(not(target_arch = "wasm32"))]
mod schema;

#[cfg(not(target_arch = "wasm32"))]
mod host {
    pub mod api;
    pub mod cache;
    pub mod config;
    pub mod error;
    pub mod graph;
    pub mod layout;
    pub mod publisher;
    pub mod render;
    pub mod server;
    pub mod snapshot_store;
    pub mod store;

    pub use config::ConsoleConfig;
    pub use error::{Error, Result};
    pub use graph::{GraphMode, GraphSnapshot, build_graph_snapshot_with_mode};
    pub use publisher::ConsolePublisher;
    pub use server::{ConsoleServerHandle, start_console_server_with_graph_store_path};
    pub use store::ConsoleStore;

    #[cfg(test)]
    #[path = "v2_tests.rs"]
    mod tests;
}

// Host tests compile viewport so its pure geometry logic stays covered without a wasm test runner.
#[cfg(any(target_arch = "wasm32", test))]
mod wasm {
    #[cfg(target_arch = "wasm32")]
    pub mod client;
    pub mod refresh;
    pub mod viewport;
}

#[cfg(not(target_arch = "wasm32"))]
pub use host::{
    ConsoleConfig, ConsolePublisher, ConsoleServerHandle, ConsoleStore, Error, GraphMode,
    GraphSnapshot, Result, build_graph_snapshot_with_mode,
    start_console_server_with_graph_store_path,
};
#[cfg(not(target_arch = "wasm32"))]
use host::{config, error, graph, layout, publisher, render};
#[cfg(target_arch = "wasm32")]
use wasm::viewport;
