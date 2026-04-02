use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};

use async_trait::async_trait;
use snafu::prelude::*;

use crate::{
    BashToolCliBridge, BashToolCliBridgeError, CocoCliRuntimeRequest, CocoCliRuntimeResponse,
    CompletionBackend, LlmService, SkillToolExecutionResult, SkillToolExecutor,
    SkillToolExecutorError, SkillToolRequest, Store, WorkflowFailedSnafu,
};

type CocoCliForwarder<B, S> = dyn Fn(
        CocoCliRuntimeRequest,
        Arc<LlmService<B, S>>,
    ) -> Pin<Box<dyn Future<Output = CocoCliRuntimeResponse> + Send>>
    + Send
    + Sync;

pub struct LlmRuntimeBridge<B, S>
where
    B: CompletionBackend,
    S: Store,
{
    llm: Weak<LlmService<B, S>>,
    cli_forwarder: Arc<CocoCliForwarder<B, S>>,
}

impl<B, S> std::fmt::Debug for LlmRuntimeBridge<B, S>
where
    B: CompletionBackend,
    S: Store,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("LlmRuntimeBridge(..)")
    }
}

impl<B, S> LlmRuntimeBridge<B, S>
where
    B: CompletionBackend + 'static,
    S: Store + 'static,
{
    pub fn new<F, Fut>(llm: Weak<LlmService<B, S>>, cli_forwarder: F) -> Self
    where
        F: Fn(CocoCliRuntimeRequest, Arc<LlmService<B, S>>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CocoCliRuntimeResponse> + Send + 'static,
    {
        Self {
            llm,
            cli_forwarder: Arc::new(move |request, llm| Box::pin(cli_forwarder(request, llm))),
        }
    }
}

#[async_trait]
impl<B, S> BashToolCliBridge for LlmRuntimeBridge<B, S>
where
    B: CompletionBackend + 'static,
    S: Store + 'static,
{
    async fn execute_coco_cli(
        &self,
        request: CocoCliRuntimeRequest,
    ) -> std::result::Result<CocoCliRuntimeResponse, BashToolCliBridgeError> {
        let llm = self
            .llm
            .upgrade()
            .ok_or(BashToolCliBridgeError::Unavailable)?;
        Ok((self.cli_forwarder)(request, llm).await)
    }
}

#[async_trait]
impl<B, S> SkillToolExecutor for LlmRuntimeBridge<B, S>
where
    B: CompletionBackend + 'static,
    S: Store + 'static,
{
    async fn execute_skill_tool(
        &self,
        request: SkillToolRequest,
    ) -> std::result::Result<SkillToolExecutionResult, SkillToolExecutorError> {
        let llm = self
            .llm
            .upgrade()
            .ok_or(SkillToolExecutorError::ExecutorUnavailable)?;
        llm.use_skill_workflow(request)
            .await
            .context(WorkflowFailedSnafu)
    }
}
