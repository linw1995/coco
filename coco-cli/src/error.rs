use snafu::prelude::*;
use std::fmt;
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

    #[snafu(display("Failed to parse session additional params JSON: {source}"))]
    ParseSessionAdditionalParams { source: serde_json::Error },

    #[snafu(display(
        "Session additional params must be a JSON object, got {kind}",
        kind = JsonValueKind(value)
    ))]
    InvalidSessionAdditionalParamsType { value: serde_json::Value },

    #[snafu(display("Reference {reference:?} did not match any branch or node ID"))]
    UnknownShowReference { reference: String },

    #[snafu(display("Node prefix {prefix:?} matched multiple node IDs: {matches:?}"))]
    AmbiguousNodePrefix {
        prefix: String,
        matches: Vec<String>,
    },

    #[snafu(display("Failed to read stdin: {source}"))]
    ReadStdin { source: io::Error },

    #[snafu(display("Failed to resolve current executable: {source}"))]
    ResolveCurrentExe { source: io::Error },

    #[snafu(display("Failed to spawn prompt worker: {source}"))]
    SpawnPromptWorker { source: io::Error },

    #[snafu(display("{source}"))]
    Store { source: coco_mem::StoreError },

    #[snafu(display("{source}"))]
    Core { source: coco_core::Error },

    #[snafu(display("{source}"))]
    CoreEngine { source: coco_core::EngineError },

    #[snafu(display("{source}"))]
    Llm { source: coco_llm::Error },
}

pub type Result<T> = std::result::Result<T, Error>;

struct JsonValueKind<'a>(&'a serde_json::Value);

impl fmt::Display for JsonValueKind<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self.0 {
            serde_json::Value::Null => "null",
            serde_json::Value::Bool(_) => "boolean",
            serde_json::Value::Number(_) => "number",
            serde_json::Value::String(_) => "string",
            serde_json::Value::Array(_) => "array",
            serde_json::Value::Object(_) => "object",
        };
        f.write_str(kind)
    }
}
