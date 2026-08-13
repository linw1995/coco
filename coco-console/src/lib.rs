mod api;
mod graph_render;
mod panels;

#[cfg(all(target_arch = "wasm32", test))]
use graph_render::truncate_label;
#[cfg(target_arch = "wasm32")]
use graph_render::{
    GRAPH_FOCUS_TARGET_QUERY, GRAPH_FOCUS_X_QUERY, GRAPH_FOCUS_Y_QUERY, JobDestination,
    bezier_path, edge_hit_target_id, edge_hit_target_label, edge_kind_label, edge_style,
    error_node_href, job_destination_focus, job_destination_href, job_focus, job_href,
    node_group_class, node_label, node_title_text, percent_encode, render_element_id,
};
#[cfg(not(target_arch = "wasm32"))]
use graph_render::{GraphCanvas, GraphCanvasModel};
#[cfg(all(test, not(target_arch = "wasm32")))]
use graph_render::{JobDestination, error_node_href, job_destination_href, job_href};

#[allow(dead_code)]
mod web_graph;

#[cfg(not(target_arch = "wasm32"))]
mod host {
    const CLIENT_ASSET_VERSION: &str = env!("COCO_CONSOLE_ASSET_VERSION");

    mod api;
    mod config;
    mod error;
    mod markdown;
    mod publisher;
    mod render;
    mod server;
    mod store;
    mod syntax_highlight;
    mod web_graph_order;
    mod web_graph_runtime;
    #[allow(dead_code)]
    mod web_graph_store;
    mod web_graph_view;

    use publisher::{
        mark_jobs_changed, mark_source_dirty, subscribe_job_changes, subscribe_source_changes,
    };
    use web_graph_runtime::WebGraphRuntime;

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
    pub mod anchor_range;
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
