use snafu::prelude::*;
use std::io;

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

    #[snafu(display("Invalid tool value {value:?} from {source_name:?}"))]
    InvalidToolConfiguration {
        source_name: &'static str,
        value: String,
    },

    #[snafu(display("Reference {reference:?} did not match any branch or node ID"))]
    UnknownShowReference { reference: String },

    #[snafu(display("Node prefix {prefix:?} matched multiple node IDs: {matches:?}"))]
    AmbiguousNodePrefix {
        prefix: String,
        matches: Vec<String>,
    },

    #[snafu(display("Failed to read stdin: {source}"))]
    ReadStdin { source: io::Error },

    #[snafu(display("{source}"))]
    Store { source: coco_mem::StoreError },

    #[snafu(display("{source}"))]
    Core { source: coco_core::Error },

    #[snafu(display("{source}"))]
    Llm { source: coco_llm::Error },
}

pub type Result<T> = std::result::Result<T, Error>;
