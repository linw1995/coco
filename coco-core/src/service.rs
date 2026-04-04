use snafu::prelude::*;

use coco_llm::CompletionBackend;
use coco_llm::coco_mem::Store;

use crate::{
    BranchResolver, ConversationEngine, EngineError, Error, InboundMessage, OutboundMessage,
    error::{BranchResolveFailedSnafu, InvalidInputSnafu},
};

type Result<T> = std::result::Result<T, Error>;

pub struct CoreService<R, B, S>
where
    B: CompletionBackend,
    S: Store,
{
    resolver: R,
    engine: ConversationEngine<B, S>,
}

impl<R, B, S> CoreService<R, B, S>
where
    B: CompletionBackend,
    S: Store,
{
    pub fn new(resolver: R, engine: ConversationEngine<B, S>) -> Self {
        Self { resolver, engine }
    }
}

impl<R, B, S> CoreService<R, B, S>
where
    R: BranchResolver,
    B: CompletionBackend,
    S: Store,
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
}
