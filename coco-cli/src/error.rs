use snafu::prelude::*;
use std::fmt;
use std::io;
use std::path::PathBuf;

type JsonValueKindCheck = fn(&serde_json::Value) -> bool;

const JSON_VALUE_KIND_CHECKS: &[(JsonValueKindCheck, &str)] = &[
    (serde_json::Value::is_null, "null"),
    (serde_json::Value::is_boolean, "boolean"),
    (serde_json::Value::is_number, "number"),
    (serde_json::Value::is_string, "string"),
    (serde_json::Value::is_array, "array"),
    (serde_json::Value::is_object, "object"),
];

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Prompt text is empty"))]
    EmptyPrompt,

    #[snafu(display("Failed to resolve current directory: {source}"))]
    ResolveCurrentDir { source: io::Error },

    #[snafu(display("Failed to read config file {path:?}: {source}"))]
    ReadConfigFile { path: PathBuf, source: io::Error },

    #[snafu(display("Failed to parse config file {path:?}: {source}"))]
    ParseConfigFile {
        path: PathBuf,
        source: toml::de::Error,
    },

    #[snafu(display("Preset requires --model or a provider profile default model"))]
    MissingPresetModel,

    #[snafu(display(
        "No provider profile selected; pass --provider-profile. Available profiles: {available:?}"
    ))]
    MissingProviderProfileSelection { available: Vec<String> },

    #[snafu(display("Provider profile {profile:?} requires default_model for session create"))]
    MissingProviderProfileModel { profile: String },

    #[snafu(display(
        "Invalid provider secret reference {value:?} for profile {profile:?} key {key:?}"
    ))]
    InvalidProviderSecretReference {
        profile: String,
        key: String,
        value: String,
    },

    #[snafu(display("Invalid channel secret reference {value:?} for channel {channel:?}"))]
    InvalidChannelSecretReference { channel: String, value: String },

    #[snafu(display("Failed to read channel secret env var {name:?}: {source}"))]
    ReadChannelSecretEnv {
        name: String,
        source: std::env::VarError,
    },

    #[snafu(display("Invalid tool value {value:?} from {source_name:?}"))]
    InvalidToolConfiguration {
        source_name: &'static str,
        value: String,
    },

    #[snafu(display("Failed to parse session additional params JSON: {source}"))]
    ParseSessionAdditionalParams { source: serde_json::Error },

    #[snafu(display("Failed to parse preset additional params JSON: {source}"))]
    ParsePresetAdditionalParams { source: serde_json::Error },

    #[snafu(display(
        "Session additional params must be a JSON object, got {kind}",
        kind = JsonValueKind(value)
    ))]
    InvalidSessionAdditionalParamsType { value: serde_json::Value },

    #[snafu(display(
        "Preset additional params must be a JSON object, got {kind}",
        kind = JsonValueKind(value)
    ))]
    InvalidPresetAdditionalParamsType { value: serde_json::Value },

    #[snafu(display("Reference {reference:?} did not match any branch or node ID"))]
    UnknownShowReference { reference: String },

    #[snafu(display("Node prefix {prefix:?} matched multiple node IDs: {matches:?}"))]
    AmbiguousNodePrefix {
        prefix: String,
        matches: Vec<String>,
    },

    #[snafu(display("Failed to read stdin: {source}"))]
    ReadStdin { source: io::Error },

    #[snafu(display("Failed to read skill file {path:?}: {source}"))]
    ReadSkillFile { path: PathBuf, source: io::Error },

    #[snafu(display("Failed to read skill script directory {path:?}: {source}"))]
    ReadSkillScriptDirectory { path: PathBuf, source: io::Error },

    #[snafu(display("Invalid skill script path {path:?}: {message}"))]
    InvalidSkillScriptPath { path: PathBuf, message: String },

    #[snafu(display(
        "skill run requires a parent tool use context; run it through exec_command or pass --parent-tool-use-id"
    ))]
    MissingSkillInvocationParent,

    #[snafu(display("skill run requires a branch context; run it through exec_command"))]
    MissingSkillRunBranch,

    #[snafu(display("skill invocation parent {parent_tool_use_id:?} has no session ancestor"))]
    MissingSkillInvocationSession { parent_tool_use_id: String },

    #[snafu(display("skill invocation parent {parent_tool_use_id:?} is not a tool use node"))]
    InvalidSkillInvocationParent { parent_tool_use_id: String },

    #[snafu(display("skill run --handoff must not be empty"))]
    InvalidSkillRunHandoff,

    #[snafu(display("Failed to resolve current executable: {source}"))]
    ResolveCurrentExe { source: io::Error },

    #[snafu(display("Failed to spawn prompt worker: {source}"))]
    SpawnPromptWorker { source: io::Error },

    #[snafu(display("Current store cannot be shared with a prompt worker process"))]
    StoreRuntimePathUnavailable,

    #[snafu(display("Failed to resolve daemon socket root: {source}"))]
    ResolveDaemonSocketRoot { source: io::Error },

    #[snafu(display("Failed to bind daemon socket {path:?}: {source}"))]
    BindDaemonSocket { path: PathBuf, source: io::Error },

    #[snafu(display("Daemon server task failed: {source}"))]
    JoinDaemonServer { source: tokio::task::JoinError },

    #[snafu(display("Channel task failed: {source}"))]
    JoinChannelTask { source: tokio::task::JoinError },

    #[snafu(display("Message queue task failed: {source}"))]
    JoinMessageQueueTask { source: tokio::task::JoinError },

    #[snafu(display("{source}"))]
    Channel { source: coco_channel::Error },

    #[snafu(display("{source}"))]
    Console { source: coco_console::Error },

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
        f.write_str(json_value_kind(self.0))
    }
}

fn json_value_kind(value: &serde_json::Value) -> &'static str {
    JSON_VALUE_KIND_CHECKS
        .iter()
        .find_map(|(matches, kind)| matches(value).then_some(*kind))
        .expect("serde_json::Value variants should be covered")
}
