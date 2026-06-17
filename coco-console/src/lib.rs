mod api;

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
    pub mod store;

    pub use cache::ConsoleGraphCache;
    pub use config::ConsoleConfig;
    pub use error::{Error, Result};
    pub use publisher::ConsolePublisher;
    pub use server::{ConsoleServerHandle, start_console_server};
    pub use store::ConsoleStore;

    #[cfg(test)]
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
    ConsoleConfig, ConsoleGraphCache, ConsolePublisher, ConsoleServerHandle, ConsoleStore, Error,
    Result, start_console_server,
};
#[cfg(not(target_arch = "wasm32"))]
use host::{config, error, graph, layout, publisher, render};
#[cfg(target_arch = "wasm32")]
use wasm::viewport;
