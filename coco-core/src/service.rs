use snafu::prelude::*;

use coco_llm::CompletionBackend;
use coco_llm::coco_mem::{BranchStore, JobStore, NodeStore, RuntimeStore, SessionStore};
use std::collections::HashSet;

use crate::{
    BatchPromptRequest, BatchPromptResult, BranchResolver, ConversationEngine, EngineError, Error,
    InboundMessage, OutboundMessage,
    error::{BranchResolveFailedSnafu, InvalidInputSnafu},
};

type Result<T> = std::result::Result<T, Error>;

pub struct CoreService<R, B, S>
where
    B: CompletionBackend,
{
    resolver: R,
    engine: ConversationEngine<B, S>,
}

impl<R, B, S> CoreService<R, B, S>
where
    B: CompletionBackend,
{
    pub fn new(resolver: R, engine: ConversationEngine<B, S>) -> Self {
        Self { resolver, engine }
    }
}

impl<R, B, S> CoreService<R, B, S>
where
    R: BranchResolver,
    B: CompletionBackend + 'static,
    S: NodeStore + BranchStore + SessionStore + JobStore + RuntimeStore,
{
    pub async fn handle_message(&self, message: InboundMessage) -> Result<OutboundMessage> {
        let text = message.text.trim();
        ensure!(
            !text.is_empty(),
            InvalidInputSnafu {
                message: "message text is empty".to_owned(),
            }
        );

        let branch = self
            .resolver
            .resolve_branch(&message)
            .context(BranchResolveFailedSnafu {
                channel_kind: message.channel_kind,
                conversation_id: message.conversation_id.clone(),
            })?;

        match self.engine.reply(&branch, text).await {
            Ok(text) => Ok(OutboundMessage { text }),
            Err(EngineError::SessionMissing { branch }) => Err(Error::MissingSession { branch }),
            Err(source @ EngineError::EngineFailed { .. }) => {
                let branch = branch.clone();
                Err(Error::ConversationFailed { branch, source })
            }
        }
    }

    pub async fn handle_batch_prompt(
        &self,
        request: BatchPromptRequest,
    ) -> Result<BatchPromptResult> {
        ensure!(
            !request.items.is_empty(),
            InvalidInputSnafu {
                message: "batch prompt items are empty".to_owned(),
            }
        );
        ensure!(
            request.max_concurrency > 0,
            InvalidInputSnafu {
                message: "batch prompt max_concurrency must be greater than zero".to_owned(),
            }
        );

        let mut seen = HashSet::new();
        for item in &request.items {
            ensure!(
                !item.prompt.trim().is_empty(),
                InvalidInputSnafu {
                    message: format!("batch prompt text is empty for branch {:?}", item.branch),
                }
            );
            ensure!(
                seen.insert(item.branch.as_str()),
                InvalidInputSnafu {
                    message: format!("batch prompt branch {:?} is duplicated", item.branch),
                }
            );
        }

        Ok(self
            .engine
            .reply_many(request.items, request.max_concurrency)
            .await)
    }
}
