use std::sync::Arc;

use coco_llm::coco_mem::{MemoryStore, Store};
use coco_llm::{CompletionBackend, LlmService, PromptRequest, RigBackend};

use crate::EngineError;

#[derive(Clone)]
pub struct ConversationEngine<B = RigBackend, S = MemoryStore>
where
    S: Store,
{
    service: Arc<LlmService<B, S>>,
}

impl ConversationEngine<RigBackend, MemoryStore> {
    pub fn with_service(service: Arc<LlmService<RigBackend, MemoryStore>>) -> Self {
        Self::new(service)
    }
}

impl<B, S> ConversationEngine<B, S>
where
    B: CompletionBackend,
    S: Store,
{
    pub fn new(service: Arc<LlmService<B, S>>) -> Self {
        Self { service }
    }

    pub async fn reply(
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
