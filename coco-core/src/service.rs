use coco_llm::CompletionBackend;
use coco_llm::coco_mem::{BranchStore, JobStore, NodeStore, SessionStore, SkillStore};
use indoc::formatdoc;
use snafu::prelude::*;
use std::collections::HashSet;

use crate::{
    BatchPromptRequest, BatchPromptResult, BranchResolver, ConversationEngine, EngineError, Error,
    InboundMessage, OutboundMessage, TelegramInboundMessage,
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
        + SkillStore
        + Clone
        + Send
        + Sync
        + 'static,
{
    pub async fn handle_message(&self, message: InboundMessage) -> Result<OutboundMessage> {
        let text = message.text().trim();
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
                channel_kind: message.channel_kind(),
                conversation_id: message.conversation_id().to_owned(),
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
    match message {
        InboundMessage::Telegram(telegram) => telegram_prompt(telegram, text),
        InboundMessage::Cli(_) => text.to_owned(),
    }
}

fn telegram_prompt(message: &TelegramInboundMessage, text: &str) -> String {
    let reply_to_message_id = message.source_message_id().unwrap_or("unknown");

    formatdoc!(
        "
        You are handling an inbound Telegram message.

        Telegram reply target:
        - chat_id: {chat_id}
        - reply_to_message_id: {reply_to_message_id}

        Required response policy:
        - Treat the incoming message as the user's actual request, not as text to acknowledge.
        - Complete the requested work using any needed tools or skills before sending the final Telegram reply.
        - Use the `telegram` skill only for Telegram delivery; do not delegate the user task itself to the `telegram` skill.
        - If the work is long-running, you may send one progress update through the `telegram` skill, then continue working.
        - Call `coco skill run telegram --handoff <reply task>` through `exec_command` for the final user-facing reply only after the reply content is ready to send.
        - Use the target chat_id and reply_to_message_id above for the first Telegram message; if you send no progress update, the final reply is that first message.
        - Do not use the `telegram` skill merely to acknowledge the request unless the incoming message only asks for acknowledgement.
        - Do not finish after an acknowledgement-only Telegram reply unless the incoming message only asked for acknowledgement.
        - Do not put the user-facing Telegram reply only in plain final text; the Telegram reply itself must be sent by the skill.
        - After the final Telegram skill call completes, return a local completion note. If you handled multiple distinct tasks, include a concise multi-task summary in that final text that lists each task and its outcome.

        Incoming message:
        {text}
        ",
        chat_id = message.chat_id(),
    )
}
