use std::sync::Arc;

use async_trait::async_trait;
use coco_llm::coco_mem::{MemoryStore, Store};
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
pub struct LlmConversationEngine<B = RigBackend, S = MemoryStore>
where
    S: Store,
{
    service: Arc<LlmService<B, S>>,
}

impl LlmConversationEngine<RigBackend, MemoryStore> {
    pub fn with_service(service: Arc<LlmService<RigBackend, MemoryStore>>) -> Self {
        Self::new(service)
    }
}

impl<B, S> LlmConversationEngine<B, S>
where
    S: Store,
{
    pub fn new(service: Arc<LlmService<B, S>>) -> Self {
        Self { service }
    }
}

#[async_trait]
impl<B, S> ConversationEngine for LlmConversationEngine<B, S>
where
    B: CompletionBackend,
    S: Store,
{
    async fn complete(
        &self,
        branch: &str,
        prompt: &str,
    ) -> std::result::Result<String, EngineError> {
        let result = self
            .service
            .prompt(PromptRequest {
                branch: branch.to_owned(),
                prompt: prompt.to_owned(),
                merge_parents: vec![],
            })
            .await?;

        Ok(result.text)
    }
}
