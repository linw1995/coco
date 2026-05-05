use std::fmt::Write as _;
use std::path::PathBuf;

use coco_mem::Tool;
use serde_json::Value;
use snafu::prelude::*;

use crate::{
    SearchSkillToolRequest, ToolExecutionOutcome, ToolInvocationContext, ToolRuntimeEnv,
    UseSkillToolRequest,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SkillToolKind {
    Search,
    Use,
}

#[derive(Debug, Clone)]
pub(crate) struct SkillToolRuntime {
    definition: Tool,
    workspace_root: PathBuf,
    context: ToolRuntimeEnv,
    kind: SkillToolKind,
}

#[derive(Debug, Snafu)]
pub enum SkillToolError {
    #[snafu(display("skill tool expects a JSON object input"))]
    InvalidInputType,

    #[snafu(display("skill tool arguments must be valid JSON: {source}"))]
    ParseArgs { source: serde_json::Error },

    #[snafu(display("skill tool `limit` must be a positive integer"))]
    InvalidLimit,

    #[snafu(display("use_skill requires a string field `name`"))]
    MissingSkillName,

    #[snafu(display("use_skill field `handoff` must be a non-empty string when present"))]
    InvalidHandoff,

    #[snafu(display("use_skill requires a persisted tool_use node context"))]
    MissingToolUseNode,

    #[snafu(display("failed to serialize skill tool output: {source}"))]
    SerializeOutput { source: serde_json::Error },
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

pub(crate) fn search_runtime(
    definition: Tool,
    workspace_root: PathBuf,
    context: ToolRuntimeEnv,
) -> SkillToolRuntime {
    SkillToolRuntime {
        definition,
        workspace_root,
        context,
        kind: SkillToolKind::Search,
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

pub(crate) fn run_runtime(
    definition: Tool,
    workspace_root: PathBuf,
    context: ToolRuntimeEnv,
) -> SkillToolRuntime {
    SkillToolRuntime {
        definition,
        workspace_root,
        context,
        kind: SkillToolKind::Use,
    }
}

#[cfg(test)]
pub fn run_runtime_tool(
    definition: Tool,
    workspace_root: PathBuf,
    context: ToolRuntimeEnv,
) -> Box<dyn rig::tool::ToolDyn> {
    Box::new(run_runtime(definition, workspace_root, context))
}

fn parse_search_request(
    workspace_root: PathBuf,
    session_role: coco_mem::SessionRole,
    args: Value,
) -> std::result::Result<SearchSkillToolRequest, SkillToolError> {
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

    Ok(SearchSkillToolRequest {
        workspace_root,
        session_role,
        query,
        limit,
    })
}

fn parse_use_request(
    workspace_root: PathBuf,
    session_branch: String,
    session_role: coco_mem::SessionRole,
    parent_tool_use_id: String,
    args: Value,
) -> std::result::Result<UseSkillToolRequest, SkillToolError> {
    let object = args.as_object().context(InvalidInputTypeSnafu)?;
    let skill_name = object
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .context(MissingSkillNameSnafu)?;
    let handoff = object
        .get("handoff")
        .map(|value| {
            value
                .as_str()
                .map(str::trim)
                .filter(|handoff| !handoff.is_empty())
                .map(str::to_owned)
                .context(InvalidHandoffSnafu)
        })
        .transpose()?;
    Ok(UseSkillToolRequest {
        workspace_root,
        session_branch,
        session_role,
        parent_tool_use_id,
        skill_name,
        handoff,
    })
}

impl SkillToolRuntime {
    pub fn tool_definition(&self) -> rig::completion::ToolDefinition {
        let mut description = self.definition.description.clone();
        if matches!(self.kind, SkillToolKind::Use)
            && let Some(skill_name) = &self.context.current_skill_name
        {
            write!(
                &mut description,
                "\n\nCurrent skill status: already executing `{skill_name}`. Do not call `use_skill` for `{skill_name}` again; apply the loaded skill instructions directly."
            )
            .expect("writing to String should not fail");
        }

        rig::completion::ToolDefinition {
            name: self.definition.name.clone(),
            description,
            parameters: self.definition.input_schema.clone(),
        }
    }

    pub async fn execute(
        &self,
        args: String,
        invocation: ToolInvocationContext,
    ) -> std::result::Result<ToolExecutionOutcome, rig::tool::ToolError> {
        let workspace_root = self.workspace_root.clone();
        let context = self.context.clone();
        let kind = self.kind;

        let args: Value = serde_json::from_str(&args).context(ParseArgsSnafu)?;

        match kind {
            SkillToolKind::Search => {
                let request = parse_search_request(workspace_root, context.session_role, args)?;
                let output = context.skill_executor.search_skill_tool(request).await?;
                Ok(ToolExecutionOutcome::tool_result(output))
            }
            SkillToolKind::Use => {
                let parent_tool_use_id = invocation
                    .persisted_tool_use_node_id
                    .context(MissingToolUseNodeSnafu)?;
                let request = parse_use_request(
                    workspace_root,
                    context.session_branch.clone(),
                    context.session_role,
                    parent_tool_use_id,
                    args,
                )?;
                let skill_name = request.skill_name.clone();
                let handoff = request.handoff.clone();
                match context.skill_executor.execute_skill_tool(request).await {
                    Ok(result) => {
                        let output = serde_json::to_string_pretty(&result.result)
                            .context(SerializeOutputSnafu)?;
                        if handoff.is_some() {
                            Ok(ToolExecutionOutcome::skill_handoff_result(
                                skill_name,
                                output,
                                result.response_node_id,
                                result.result.text,
                            ))
                        } else {
                            Ok(ToolExecutionOutcome::tool_result(output))
                        }
                    }
                    Err(error) => Ok(ToolExecutionOutcome::tool_result(error.to_string())),
                }
            }
        }
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
        Box::pin(async move {
            match self.execute(args, ToolInvocationContext::default()).await {
                Ok(outcome) => Ok(outcome.provider_output().to_owned()),
                Err(source) => Err(source),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use async_trait::async_trait;
    use tokio::sync::Mutex;

    use super::*;
    use crate::{SkillToolExecutionResult, SkillToolExecutor, SkillToolRunResult};

    fn search_tool_definition() -> Tool {
        crate::builtin_tool_definition("search_skill").expect("builtin tool should exist")
    }

    fn run_tool_definition() -> Tool {
        crate::builtin_tool_definition("use_skill").expect("builtin tool should exist")
    }

    fn run_context(executor: Arc<dyn crate::SkillToolExecutor>) -> ToolRuntimeEnv {
        ToolRuntimeEnv {
            session_branch: "main".to_owned(),
            session_role: coco_mem::SessionRole::Orchestrator,
            current_skill_name: None,
            active_skill: None,
            store_path: None,
            enable_coco_shim: false,
            cli_bridge: crate::UnifiedExecCliBridgeHandle::default(),
            skill_executor: crate::SkillToolExecutorHandle::new(executor),
        }
    }

    #[derive(Debug)]
    struct FakeExecutor {
        search_requests: Arc<Mutex<Vec<SearchSkillToolRequest>>>,
        use_requests: Arc<Mutex<Vec<UseSkillToolRequest>>>,
    }

    #[async_trait]
    impl SkillToolExecutor for FakeExecutor {
        async fn search_skill_tool(
            &self,
            request: SearchSkillToolRequest,
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

        async fn execute_skill_tool(
            &self,
            request: UseSkillToolRequest,
        ) -> std::result::Result<SkillToolExecutionResult, crate::ExecutorError> {
            self.use_requests.lock().await.push(request);
            Ok(SkillToolExecutionResult {
                result: SkillToolRunResult {
                    text: "Executed skill result".to_owned(),
                },
                response_node_id: "child-response-node".to_owned(),
            })
        }
    }

    #[tokio::test]
    async fn search_skill_runtime_delegates_to_executor() {
        let search_requests = Arc::new(Mutex::new(Vec::new()));
        let executor = Arc::new(FakeExecutor {
            search_requests: search_requests.clone(),
            use_requests: Arc::new(Mutex::new(Vec::new())),
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
    async fn use_skill_runtime_executes_on_child_branch_via_executor() {
        let use_requests = Arc::new(Mutex::new(Vec::new()));
        let executor = Arc::new(FakeExecutor {
            search_requests: Arc::new(Mutex::new(Vec::new())),
            use_requests: use_requests.clone(),
        });
        let workspace_root = std::env::temp_dir().join("skill-tool-use");
        let runtime = run_runtime(
            run_tool_definition(),
            workspace_root.clone(),
            run_context(executor),
        );

        let outcome = runtime
            .execute(
                r#"{"name":"find-skills"}"#.to_owned(),
                ToolInvocationContext {
                    persisted_tool_use_node_id: Some("tool-use-node".to_owned()),
                },
            )
            .await
            .unwrap();
        let requests = use_requests.lock().await;
        let ToolExecutionOutcome::ToolResult { provider_output } = outcome else {
            panic!("expected regular tool result without handoff");
        };
        let value: Value = serde_json::from_str(&provider_output).unwrap();

        assert_eq!(value.as_object().expect("expected JSON object").len(), 1);
        assert_eq!(value["text"], "Executed skill result");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].workspace_root, workspace_root);
        assert_eq!(requests[0].session_branch, "main");
        assert_eq!(
            requests[0].session_role,
            coco_mem::SessionRole::Orchestrator
        );
        assert_eq!(requests[0].parent_tool_use_id, "tool-use-node");
        assert_eq!(requests[0].skill_name, "find-skills");
        assert_eq!(requests[0].handoff, None);
    }

    #[tokio::test]
    async fn use_skill_runtime_passes_string_handoff_as_terminal_result() {
        let use_requests = Arc::new(Mutex::new(Vec::new()));
        let executor = Arc::new(FakeExecutor {
            search_requests: Arc::new(Mutex::new(Vec::new())),
            use_requests: use_requests.clone(),
        });
        let runtime = run_runtime(
            run_tool_definition(),
            Path::new("/tmp").to_path_buf(),
            run_context(executor),
        );

        let outcome = runtime
            .execute(
                r#"{"name":"find-skills","handoff":"Summarize the docs decision."}"#.to_owned(),
                ToolInvocationContext {
                    persisted_tool_use_node_id: Some("tool-use-node".to_owned()),
                },
            )
            .await
            .unwrap();
        let requests = use_requests.lock().await;
        let ToolExecutionOutcome::SkillResult {
            skill_name,
            provider_output,
            merge_parent,
            terminal_text,
        } = outcome
        else {
            panic!("expected handoff use_skill to return a terminal skill result");
        };
        let value: Value = serde_json::from_str(&provider_output).unwrap();

        assert_eq!(value["text"], "Executed skill result");
        assert_eq!(skill_name, "find-skills");
        assert_eq!(merge_parent, "child-response-node");
        assert_eq!(terminal_text.as_deref(), Some("Executed skill result"));
        assert_eq!(
            requests[0].handoff.as_deref(),
            Some("Summarize the docs decision.")
        );
    }

    #[test]
    fn use_skill_definition_reports_current_skill_status() {
        let executor = Arc::new(FakeExecutor {
            search_requests: Arc::new(Mutex::new(Vec::new())),
            use_requests: Arc::new(Mutex::new(Vec::new())),
        });
        let mut context = run_context(executor);
        context.current_skill_name = Some("catgirl-role".to_owned());
        let runtime = run_runtime(
            run_tool_definition(),
            Path::new("/tmp").to_path_buf(),
            context,
        );

        let definition = runtime.tool_definition();

        assert!(
            definition
                .description
                .contains("Current skill status: already executing `catgirl-role`.")
        );
        assert!(
            definition
                .description
                .contains("Do not call `use_skill` for `catgirl-role` again")
        );
    }

    #[tokio::test]
    async fn invalid_json_is_rejected_before_delegation() {
        let search_requests = Arc::new(Mutex::new(Vec::new()));
        let use_requests = Arc::new(Mutex::new(Vec::new()));
        let executor = Arc::new(FakeExecutor {
            search_requests: search_requests.clone(),
            use_requests: use_requests.clone(),
        });
        let runtime = run_runtime_tool(
            run_tool_definition(),
            Path::new("/tmp").to_path_buf(),
            run_context(executor),
        );

        let error = runtime.call("{".to_owned()).await.unwrap_err();

        assert!(error.to_string().contains("must be valid JSON"));
        assert!(search_requests.lock().await.is_empty());
        assert!(use_requests.lock().await.is_empty());
    }

    #[derive(Debug)]
    struct FailingExecutor;

    #[async_trait]
    impl SkillToolExecutor for FailingExecutor {
        async fn search_skill_tool(
            &self,
            _request: SearchSkillToolRequest,
        ) -> std::result::Result<String, crate::ExecutorError> {
            unreachable!("search_skill_tool should not be called")
        }

        async fn execute_skill_tool(
            &self,
            _request: UseSkillToolRequest,
        ) -> std::result::Result<SkillToolExecutionResult, crate::ExecutorError> {
            Err(crate::ExecutorError::OperationFailed {
                message: "delegated failure".to_owned(),
                source: None,
            })
        }
    }

    #[tokio::test]
    async fn use_skill_runtime_returns_executor_errors_as_tool_results() {
        let workspace_root = std::env::temp_dir().join("skill-tool-use-failure");
        let runtime = run_runtime(
            run_tool_definition(),
            workspace_root,
            run_context(Arc::new(FailingExecutor)),
        );

        let outcome = runtime
            .execute(
                r#"{"name":"find-skills"}"#.to_owned(),
                ToolInvocationContext {
                    persisted_tool_use_node_id: Some("tool-use-node".to_owned()),
                },
            )
            .await
            .unwrap();
        let ToolExecutionOutcome::ToolResult { provider_output } = outcome else {
            panic!("expected tool result outcome");
        };

        assert_eq!(provider_output, "delegated failure");
    }
}
