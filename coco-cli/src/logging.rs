use std::env;
use std::error::Error as StdError;
use std::fmt;
use std::path::{Path, PathBuf};

use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

const COCO_LOG_DIR_ENV: &str = "COCO_LOG_DIR";
const COCO_LOG_FILTER_ENV: &str = "COCO_LOG";

pub struct LoggingGuard {
    _stderr_guard: WorkerGuard,
    _file_guard: Option<WorkerGuard>,
}

#[derive(Debug)]
pub enum InitTracingError {
    SetGlobalDefault(tracing_subscriber::util::TryInitError),
}

impl fmt::Display for InitTracingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SetGlobalDefault(source) => {
                write!(
                    formatter,
                    "failed to initialize tracing subscriber: {source}"
                )
            }
        }
    }
}

impl StdError for InitTracingError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::SetGlobalDefault(source) => Some(source),
        }
    }
}

pub fn init_tracing() -> Result<LoggingGuard, InitTracingError> {
    let log_dir = resolve_log_dir();
    let file_writer = build_file_writer(&log_dir);
    let (stderr_writer, stderr_guard) = tracing_appender::non_blocking(std::io::stderr());

    let stderr_layer = tracing_subscriber::fmt::layer()
        .json()
        .with_ansi(false)
        .with_writer(stderr_writer)
        .with_filter(filter_from_env());

    if let Some((file_writer, file_guard)) = file_writer {
        let file_layer = tracing_subscriber::fmt::layer()
            .json()
            .with_ansi(false)
            .with_writer(file_writer)
            .with_filter(filter_from_env());

        tracing_subscriber::registry()
            .with(stderr_layer)
            .with(file_layer)
            .try_init()
            .map_err(InitTracingError::SetGlobalDefault)?;

        return Ok(LoggingGuard {
            _stderr_guard: stderr_guard,
            _file_guard: Some(file_guard),
        });
    }

    tracing_subscriber::registry()
        .with(stderr_layer)
        .try_init()
        .map_err(InitTracingError::SetGlobalDefault)?;

    Ok(LoggingGuard {
        _stderr_guard: stderr_guard,
        _file_guard: None,
    })
}

fn build_file_writer(log_dir: &Path) -> Option<(NonBlocking, WorkerGuard)> {
    std::fs::create_dir_all(log_dir).ok()?;
    let file_appender = RollingFileAppender::builder()
        .rotation(Rotation::DAILY)
        .filename_prefix("coco.log")
        .build(log_dir)
        .ok()?;

    Some(tracing_appender::non_blocking(file_appender))
}

fn filter_from_env() -> EnvFilter {
    EnvFilter::try_from_env(COCO_LOG_FILTER_ENV)
        .or_else(|_| EnvFilter::try_from_default_env())
        .unwrap_or_else(|_| EnvFilter::new(default_filter()))
}

fn default_filter() -> &'static str {
    "warn,coco_cli=info,coco_core=info,coco_llm=info,coco_mem=info,coco_console=info"
}

fn resolve_log_dir() -> PathBuf {
    env::var_os(COCO_LOG_DIR_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(default_log_dir)
}

fn default_log_dir() -> PathBuf {
    env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/state")))
        .unwrap_or_else(env::temp_dir)
        .join("coco")
        .join("logs")
}
