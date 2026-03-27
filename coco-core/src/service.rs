use snafu::prelude::*;

use crate::{
    BranchResolver, ConversationEngine, EngineError, Error, InboundMessage, OutboundMessage,
    error::InvalidInputSnafu,
};

type Result<T> = std::result::Result<T, Error>;

pub struct CoreService<R, E> {
    resolver: R,
    engine: E,
}

impl<R, E> CoreService<R, E> {
    pub fn new(resolver: R, engine: E) -> Self {
        Self { resolver, engine }
    }
}

impl<R, E> CoreService<R, E>
where
    R: BranchResolver,
    E: ConversationEngine,
{
    pub async fn handle_message(&self, message: InboundMessage) -> Result<OutboundMessage> {
        let text = message.text.trim();
        ensure!(
            !text.is_empty(),
            InvalidInputSnafu {
                message: "message text is empty".to_owned(),
            }
        );

        let branch = self.resolver.resolve_branch(&message).map_err(|source| {
            Error::BranchResolveFailed {
                channel_kind: message.channel_kind,
                conversation_id: message.conversation_id.clone(),
                source,
            }
        })?;

        match self.engine.complete(&branch, text).await {
            Ok(text) => Ok(OutboundMessage { text }),
            Err(EngineError::SessionMissing { branch }) => Err(Error::MissingSession { branch }),
            Err(source @ EngineError::EngineFailed { .. }) => {
                let branch = branch.clone();
                Err(Error::ConversationFailed { branch, source })
            }
        }
    }
}
