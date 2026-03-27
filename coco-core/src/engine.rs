use std::sync::Arc;

use async_trait::async_trait;
use coco_llm::{CompletionBackend, LlmService, PromptRequest, RigBackend};

use crate::EngineError;

#[async_trait]
pub trait ConversationEngine: Send + Sync {
    async fn complete(
        &self,
        branch: &str,
        prompt: &str,
    ) -> std::result::Result<String, EngineError>;
}

#[derive(Clone)]
pub struct LlmConversationEngine<B = RigBackend> {
    service: Arc<LlmService<B>>,
}

impl LlmConversationEngine<RigBackend> {
    pub fn with_service(service: Arc<LlmService<RigBackend>>) -> Self {
        Self::new(service)
    }
}

impl<B> LlmConversationEngine<B> {
    pub fn new(service: Arc<LlmService<B>>) -> Self {
        Self { service }
    }
}

#[async_trait]
impl<B> ConversationEngine for LlmConversationEngine<B>
where
    B: CompletionBackend,
{
    async fn complete(
        &self,
        branch: &str,
        prompt: &str,
    ) -> std::result::Result<String, EngineError> {
        self.service
            .prompt(PromptRequest {
                branch: branch.to_owned(),
                prompt: prompt.to_owned(),
                merge_parents: vec![],
            })
            .await
            .map(|result| result.text)
            .map_err(EngineError::from)
    }
}
