use std::path::PathBuf;

use async_trait::async_trait;
use coco_mem::{SessionRole, SkillScript};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillInvocationRequest {
    pub workspace_root: PathBuf,
    pub base_branch: String,
    pub parent_tool_use_id: String,
    pub skill_name: String,
    pub skill_description: String,
    pub skill_path: String,
    pub skill_body: String,
    #[serde(default)]
    pub scripts: Vec<SkillScript>,
    pub session_role: SessionRole,
    pub enable_coco_shim: bool,
    pub handoff: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillInvocationResult {
    pub text: String,
    pub response_node_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillSearchRequest {
    pub workspace_root: PathBuf,
    pub session_role: SessionRole,
    pub query: String,
    pub limit: usize,
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ExecutorError {
    #[snafu(display("skill tool executor is unavailable"))]
    Unavailable,

    #[snafu(display("{message}"))]
    OperationFailed {
        message: String,
        #[snafu(source(false))]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
}

#[async_trait]
pub trait SkillSearchExecutor: Send + Sync {
    async fn search_skill(
        &self,
        request: SkillSearchRequest,
    ) -> std::result::Result<String, ExecutorError>;
}
