use coco_llm::CompletionBackend;
use coco_llm::coco_mem::{BranchStore, JobStore, NodeStore, RuntimeStore, SessionStore};
use indoc::formatdoc;
use snafu::prelude::*;
use std::collections::HashSet;

use crate::{
    BatchPromptRequest, BatchPromptResult, BranchResolver, ConversationEngine, EngineError, Error,
    InboundMessage, OutboundMessage,
    error::{BranchResolveFailedSnafu, InvalidInputSnafu},
};

type Result<T> = std::result::Result<T, Error>;

pub struct CoreService<R, B, S> {
    resolver: R,
    engine: ConversationEngine<B, S>,
}

impl<R, B, S> CoreService<R, B, S> {
    pub fn new(resolver: R, engine: ConversationEngine<B, S>) -> Self {
        Self { resolver, engine }
    }
}

impl<R, B, S> CoreService<R, B, S>
where
    R: BranchResolver,
    B: CompletionBackend + 'static,
    S: NodeStore
        + BranchStore
        + SessionStore
        + JobStore
        + RuntimeStore
        + Clone
        + Send
        + Sync
        + 'static,
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

        let prompt = channel_prompt(&message, text);
        match self.engine.reply(&branch, &prompt).await {
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

fn channel_prompt(message: &InboundMessage, text: &str) -> String {
    if message.channel_kind != coco_channel::ChannelKind::Telegram {
        return text.to_owned();
    }

    let reply_to_message_id = message.source_message_id.as_deref().unwrap_or("unknown");

    formatdoc!(
        "
        You are handling an inbound Telegram message.

        Telegram reply target:
        - chat_id: {chat_id}
        - reply_to_message_id: {reply_to_message_id}

        Task policy:
        - Treat the incoming message as the user's task request, not as text to acknowledge.
        - Complete the requested work using the available tools before sending the final Telegram reply.
        - Use `telegram` only for Telegram delivery; do not delegate the user task itself to the Telegram skill.
        - If the work is long-running, you may send one progress update through the `telegram` skill, then continue working.
        - Send the final user-facing result by calling the `telegram` skill through `use_skill`.
        - Use the target chat_id and reply_to_message_id above for the first Telegram reply.
        - After the final Telegram skill call completes, return a short local completion note such as `Telegram task completed.`.
        - Do not finish after an acknowledgement-only Telegram reply unless the incoming message only asked for acknowledgement.
        - Do not put the user-facing Telegram reply only in plain final text; it must be sent by the skill.

        Incoming message:
        {text}
        ",
        chat_id = message.conversation_id,
    )
}
