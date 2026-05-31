use std::path::PathBuf;

use coco_mem::Tool;
use serde_json::Value;
use snafu::prelude::*;

use crate::{SkillSearchRequest, ToolInvocationContext, ToolRuntimeEnv};

#[derive(Debug, Clone)]
pub struct SkillToolRuntime {
    definition: Tool,
    workspace_root: PathBuf,
    context: ToolRuntimeEnv,
}

#[derive(Debug, Snafu)]
pub enum SkillToolError {
    #[snafu(display("skill tool expects a JSON object input"))]
    InvalidInputType,

    #[snafu(display("skill tool arguments must be valid JSON: {source}"))]
    ParseArgs { source: serde_json::Error },

    #[snafu(display("skill tool `limit` must be a positive integer"))]
    InvalidLimit,
}

impl From<crate::ExecutorError> for rig::tool::ToolError {
    fn from(error: crate::ExecutorError) -> Self {
        Self::ToolCallError(Box::new(error))
    }
}

impl From<SkillToolError> for rig::tool::ToolError {
    fn from(error: SkillToolError) -> Self {
        Self::ToolCallError(Box::new(error))
    }
}

pub fn search_runtime(
    definition: Tool,
    workspace_root: PathBuf,
    context: ToolRuntimeEnv,
) -> SkillToolRuntime {
    SkillToolRuntime {
        definition,
        workspace_root,
        context,
    }
}

#[cfg(test)]
pub fn search_runtime_tool(
    definition: Tool,
    workspace_root: PathBuf,
    context: ToolRuntimeEnv,
) -> Box<dyn rig::tool::ToolDyn> {
    Box::new(search_runtime(definition, workspace_root, context))
}

fn parse_search_request(
    workspace_root: PathBuf,
    session_role: coco_mem::SessionRole,
    args: Value,
) -> std::result::Result<SkillSearchRequest, SkillToolError> {
    let object = args.as_object().context(InvalidInputTypeSnafu)?;
    let query = object
        .get("query")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_owned();
    let limit = match object.get("limit").and_then(Value::as_u64) {
        Some(0) => return InvalidLimitSnafu.fail(),
        Some(limit) => usize::try_from(limit).ok().context(InvalidLimitSnafu)?,
        None => 10,
    };

    Ok(SkillSearchRequest {
        workspace_root,
        session_role,
        query,
        limit,
    })
}

impl SkillToolRuntime {
    pub fn tool_definition(&self) -> rig::completion::ToolDefinition {
        rig::completion::ToolDefinition {
            name: self.definition.name.clone(),
            description: self.definition.description.clone(),
            parameters: self.definition.input_schema.clone(),
        }
    }

    pub async fn execute(
        &self,
        args: String,
        _invocation: ToolInvocationContext,
    ) -> std::result::Result<String, rig::tool::ToolError> {
        let workspace_root = self.workspace_root.clone();
        let context = self.context.clone();

        let args: Value = serde_json::from_str(&args).context(ParseArgsSnafu)?;

        let request = parse_search_request(workspace_root, context.session_role, args)?;
        Ok(context.skill_executor.search_skill(request).await?)
    }
}

impl rig::tool::ToolDyn for SkillToolRuntime {
    fn name(&self) -> String {
        self.definition.name.clone()
    }

    fn definition(
        &self,
        _prompt: String,
    ) -> rig::wasm_compat::WasmBoxedFuture<'_, rig::completion::ToolDefinition> {
        let definition = self.tool_definition();
        Box::pin(async move { definition })
    }

    fn call(
        &self,
        args: String,
    ) -> rig::wasm_compat::WasmBoxedFuture<'_, std::result::Result<String, rig::tool::ToolError>>
    {
        Box::pin(async move { self.execute(args, ToolInvocationContext::default()).await })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use tokio::sync::Mutex;

    use super::*;
    use crate::SkillSearchExecutor;

    fn search_tool_definition() -> Tool {
        crate::builtin_tool_definition("search_skill").expect("builtin tool should exist")
    }

    fn run_context(executor: Arc<dyn crate::SkillSearchExecutor>) -> ToolRuntimeEnv {
        ToolRuntimeEnv {
            session_branch: "main".to_owned(),
            session_role: coco_mem::SessionRole::Orchestrator,
            current_skill_name: None,
            active_skill: None,
            store_path: None,
            enable_coco_shim: false,
            cli_bridge: crate::UnifiedExecCliBridgeHandle::default(),
            skill_executor: crate::SkillSearchExecutorHandle::new(executor),
        }
    }

    #[derive(Debug)]
    struct FakeExecutor {
        search_requests: Arc<Mutex<Vec<SkillSearchRequest>>>,
    }

    #[async_trait]
    impl SkillSearchExecutor for FakeExecutor {
        async fn search_skill(
            &self,
            request: SkillSearchRequest,
        ) -> std::result::Result<String, crate::ExecutorError> {
            self.search_requests.lock().await.push(request);
            Ok(r#"{
  "skills": [
    {
      "name": "openai-docs",
      "description": "Find OpenAI API documentation.",
      "path": "/tmp/openai-docs/SKILL.md"
    }
  ]
}"#
            .to_owned())
        }
    }

    #[tokio::test]
    async fn search_skill_runtime_delegates_to_executor() {
        let search_requests = Arc::new(Mutex::new(Vec::new()));
        let executor = Arc::new(FakeExecutor {
            search_requests: search_requests.clone(),
        });
        let workspace_root = std::env::temp_dir().join("skill-tool-search");
        let runtime = search_runtime_tool(
            search_tool_definition(),
            workspace_root.clone(),
            run_context(executor),
        );

        let output = runtime
            .call(r#"{"query":"openai docs","limit":1}"#.to_owned())
            .await
            .unwrap();
        let value: Value = serde_json::from_str(&output).unwrap();
        let requests = search_requests.lock().await;

        assert_eq!(value["skills"].as_array().unwrap().len(), 1);
        assert_eq!(value["skills"][0]["name"], "openai-docs");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].workspace_root, workspace_root);
        assert_eq!(
            requests[0].session_role,
            coco_mem::SessionRole::Orchestrator
        );
        assert_eq!(requests[0].query, "openai docs");
        assert_eq!(requests[0].limit, 1);
    }

    #[tokio::test]
    async fn invalid_json_is_rejected_before_delegation() {
        let search_requests = Arc::new(Mutex::new(Vec::new()));
        let executor = Arc::new(FakeExecutor {
            search_requests: search_requests.clone(),
        });
        let runtime = search_runtime_tool(
            search_tool_definition(),
            std::env::temp_dir().join("skill-tool-invalid-json"),
            run_context(executor),
        );

        let error = runtime.call("{".to_owned()).await.unwrap_err();

        assert!(error.to_string().contains("must be valid JSON"));
        assert!(search_requests.lock().await.is_empty());
    }
}
