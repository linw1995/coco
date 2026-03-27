use std::io;
use std::path::PathBuf;

use snafu::prelude::*;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Prompt text is empty"))]
    EmptyPrompt,

    #[snafu(display("Missing configuration value {name:?}"))]
    MissingConfiguration { name: &'static str },

    #[snafu(display("Invalid provider value {value:?} from {source_name:?}"))]
    InvalidProviderConfiguration {
        source_name: &'static str,
        value: String,
    },

    #[snafu(display("Failed to read stdin: {source}"))]
    ReadStdin { source: io::Error },

    #[snafu(display("Failed to read store from {path:?}: {source}"))]
    ReadStore { path: PathBuf, source: io::Error },

    #[snafu(display("Failed to parse store from {path:?}: {source}"))]
    ParseStore {
        path: PathBuf,
        source: serde_json::Error,
    },

    #[snafu(display("Failed to create store directory for {path:?}: {source}"))]
    CreateStoreDirectory { path: PathBuf, source: io::Error },

    #[snafu(display("Failed to serialize store for {path:?}: {source}"))]
    SerializeStore {
        path: PathBuf,
        source: serde_json::Error,
    },

    #[snafu(display("Failed to write store to {path:?}: {source}"))]
    WriteStore { path: PathBuf, source: io::Error },

    #[snafu(display("{source}"))]
    Core { source: coco_core::Error },

    #[snafu(display("{source}"))]
    Llm { source: coco_llm::Error },
}

pub type Result<T> = std::result::Result<T, Error>;
