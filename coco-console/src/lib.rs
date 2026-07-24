mod api;
mod panels;

#[allow(dead_code)]
mod web_graph;

#[cfg(not(target_arch = "wasm32"))]
mod host {
    mod api;
    mod config;
    mod error;
    mod publisher;
    mod render;
    mod server;
    mod store;
    mod web_graph_order;
    mod web_graph_runtime;
    #[allow(dead_code)]
    mod web_graph_store;
    mod web_graph_view;

    pub use config::ConsoleConfig;
    pub use error::{Error, Result};
    pub use publisher::ConsolePublisher;
    pub use server::{
        ConsoleServerHandle, PanelServerContext, start_console_server_with_graph_store_path,
    };
    pub use store::ConsoleStore;
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
    ConsoleConfig, ConsolePublisher, ConsoleServerHandle, ConsoleStore, Error, Result,
    start_console_server_with_graph_store_path,
};
#[cfg(target_arch = "wasm32")]
use wasm::viewport;
