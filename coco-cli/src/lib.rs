mod app;
mod cli;
mod env;
mod error;
mod logging;
mod store;

pub use app::daemon::resolve_default_daemon_socket_path;
pub use app::run;
pub use cli::Cli;
pub use error::{Error, Result};
pub use logging::{InitTracingError, LoggingGuard, init_tracing};

pub const COCO_DAEMON_SOCKET_ENV: &str = "COCO_DAEMON_SOCKET";

#[cfg(test)]
mod tests;
