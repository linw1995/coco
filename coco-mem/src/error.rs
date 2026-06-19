use std::path::PathBuf;

use snafu::prelude::*;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum StoreError {
    #[snafu(display("Parent ID {id:?} not found"))]
    ParentNotFound { id: String },

    #[snafu(display("Merge parent ID {id:?} is duplicated"))]
    DuplicateMergeParent { id: String },

    #[snafu(display("Merge parent ID {id:?} matches the primary parent"))]
    MergeParentMatchesParent { id: String },

    #[snafu(display("Anchor has multiple shadow parents: {ids:?}"))]
    MultipleShadowParents { ids: Vec<String> },

    #[snafu(display("ID {id:?} not found"))]
    NotFound { id: String },

    #[snafu(display("Node prefix {prefix:?} matched multiple node IDs: {matches:?}"))]
    AmbiguousNodePrefix {
        prefix: String,
        matches: Vec<String>,
    },

    #[snafu(display("ID {id:?} is not an anchor"))]
    InvalidAnchor { id: String },

    #[snafu(display("Branch {name:?} not found"))]
    BranchNotFound { name: String },

    #[snafu(display("Branch {name:?} already exists"))]
    BranchExists { name: String },

    #[snafu(display("Preset {name:?} not found"))]
    PresetNotFound { name: String },

    #[snafu(display("Preset {name:?} version {version} not found"))]
    PresetVersionNotFound { name: String, version: u64 },

    #[snafu(display("Provider profile {name:?} not found"))]
    ProviderProfileNotFound { name: String },

    #[snafu(display("Branch {name:?} moved from {expected:?} to {actual:?}"))]
    BranchHeadMoved {
        name: String,
        expected: String,
        actual: String,
    },

    #[snafu(display("Session state for branch {name:?} moved from {expected:?} to {actual:?}"))]
    SessionStateMoved {
        name: String,
        expected: String,
        actual: String,
    },

    #[snafu(display("Prompt job {job_id:?} not found"))]
    PromptJobNotFound { job_id: String },

    #[snafu(display("Prompt job {job_id:?} already exists"))]
    PromptJobAlreadyExists { job_id: String },

    #[snafu(display("Prompt job {job_id:?} moved from {expected:?} to {actual:?}"))]
    PromptJobMoved {
        job_id: String,
        expected: String,
        actual: String,
    },

    #[snafu(display("Prompt job {job_id:?} cannot move from {current:?} to {next:?}"))]
    PromptJobInvalidStatusTransition {
        job_id: String,
        current: String,
        next: String,
    },

    #[snafu(display("Branch {branch:?} already has an active prompt job {job_id:?}"))]
    PromptJobActiveOnBranch { branch: String, job_id: String },

    #[snafu(display("Skill {name:?} already exists for role {role:?}"))]
    SkillAlreadyExists { role: String, name: String },

    #[snafu(display("Skill {name:?} not found for role {role:?}"))]
    SkillNotFound { role: String, name: String },

    #[snafu(display("Skill {name:?} version {version} not found for role {role:?}"))]
    SkillVersionNotFound {
        role: String,
        name: String,
        version: u64,
    },

    #[snafu(display("Skill {name:?} update is empty for role {role:?}"))]
    SkillUpdateEmpty { role: String, name: String },

    #[snafu(display("Invalid skill name {name:?}: {message}"))]
    InvalidSkillName { name: String, message: String },

    #[snafu(display("Ref {base_ref:?} is not an ancestor of {head_ref:?}"))]
    RefsNotConnected { base_ref: String, head_ref: String },

    #[snafu(display("Branch {branch:?} has no session anchor"))]
    MissingSessionAnchor { branch: String },

    #[snafu(display("Session handoff prompt must not be empty"))]
    InvalidSessionHandoffPrompt,

    #[snafu(display("Store path {path:?} is not a directory"))]
    StorePathIsNotDirectory { path: PathBuf },

    #[snafu(display("Store at {path:?} is locked by another process"))]
    StoreLocked { path: PathBuf },

    #[snafu(display("Store at {path:?} was opened read-only"))]
    StoreReadOnly { path: PathBuf },

    #[snafu(display("Failed to create or access store directory {path:?}: {source}"))]
    WriteStoreDirectory {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to read or write store metadata {path:?}: {source}"))]
    WriteStoreMeta {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to parse store metadata {path:?}: {source}"))]
    ParseStoreMeta {
        path: PathBuf,
        source: serde_json::Error,
    },

    #[snafu(display("Failed to read or write store log {path:?}: {source}"))]
    WriteStoreLog {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to parse store log {path:?} at line {line}: {source}"))]
    ParseStoreLog {
        path: PathBuf,
        line: usize,
        source: serde_json::Error,
    },

    #[snafu(display("Failed to serialize store record for {path:?}: {source}"))]
    SerializeStoreRecord {
        path: PathBuf,
        source: serde_json::Error,
    },

    #[snafu(display("Failed to start SQLite store runtime: {source}"))]
    StartSqliteRuntime { source: std::io::Error },

    #[snafu(display("Failed to connect to SQLite store {path:?}: {source}"))]
    ConnectSqliteStore {
        path: PathBuf,
        source: diesel::ConnectionError,
    },

    #[snafu(display("Failed to query SQLite store {path:?}: {source}"))]
    QuerySqliteStore {
        path: PathBuf,
        source: diesel::result::Error,
    },

    #[snafu(display("Failed to parse SQLite store value {column:?} in {path:?}: {source}"))]
    ParseSqliteStoreValue {
        path: PathBuf,
        column: String,
        source: serde_json::Error,
    },

    #[snafu(display("Corrupted store at {path:?}: {message}"))]
    CorruptedStore { path: PathBuf, message: String },
}

pub type StoreResult<T> = std::result::Result<T, StoreError>;
