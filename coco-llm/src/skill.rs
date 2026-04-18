use std::path::PathBuf;

use async_trait::async_trait;
use coco_mem::SessionRole;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillToolRequest {
    pub base_branch: String,
    pub parent_tool_use_id: String,
    pub skill_name: String,
    pub skill_description: String,
    pub skill_path: String,
    pub skill_body: String,
    pub session_role: SessionRole,
    pub enable_coco_shim: bool,
    pub task: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillToolRunResult {
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillToolHandoff {
    pub skill_name: String,
    pub merge_parent: String,
    pub output: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillToolExecutionResult {
    pub result: SkillToolRunResult,
    pub handoff: SkillToolHandoff,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchSkillToolRequest {
    pub workspace_root: PathBuf,
    pub session_role: SessionRole,
    pub query: String,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UseSkillToolRequest {
    pub workspace_root: PathBuf,
    pub session_branch: String,
    pub session_role: SessionRole,
    pub parent_tool_use_id: String,
    pub skill_name: String,
    pub task: Option<String>,
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ExecutorError {
    #[snafu(display("skill tool executor is unavailable"))]
    Unavailable,

    #[snafu(display("{message}"))]
    OperationFailed {
        message: String,
        handoff: Option<SkillToolHandoff>,
        #[snafu(source(false))]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
}

impl ExecutorError {
    pub fn skill_handoff(&self) -> Option<&SkillToolHandoff> {
        match self {
            Self::OperationFailed { handoff, .. } => handoff.as_ref(),
            Self::Unavailable => None,
        }
    }
}

#[async_trait]
pub trait SkillToolExecutor: Send + Sync {
    async fn search_skill_tool(
        &self,
        request: SearchSkillToolRequest,
    ) -> std::result::Result<String, ExecutorError>;

    async fn execute_skill_tool(
        &self,
        request: UseSkillToolRequest,
    ) -> std::result::Result<SkillToolExecutionResult, ExecutorError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchHandoff {
    pub tool_id: String,
    pub skill_name: String,
    pub merge_parent: String,
    pub output: String,
}
