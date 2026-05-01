use snafu::prelude::*;

use coco_llm::coco_mem::{BranchStore, JobStore, NodeStore, RuntimeStore, SessionStore};
use coco_llm::{CompletionBackend, SessionConfigPatch, builtin_tool_definition};
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
        let session_patch = channel_session_patch(&message);

        match self
            .engine
            .reply_with_session_patch(&branch, &prompt, vec![], session_patch)
            .await
        {
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

    format!(
        "You are handling an inbound Telegram message.\n\nTelegram reply target:\n- chat_id: {chat_id}\n- reply_to_message_id: {reply_to_message_id}\n\nRequired response policy:\n- Reply by calling the `telegram` skill through `use_skill`.\n- Use the target chat_id and reply_to_message_id above when sending the Telegram reply.\n- Do not deliver the Telegram reply as plain final text.\n\nIncoming message:\n{text}",
        chat_id = message.conversation_id,
    )
}

fn channel_session_patch(message: &InboundMessage) -> Option<SessionConfigPatch> {
    if message.channel_kind != coco_channel::ChannelKind::Telegram {
        return None;
    }

    Some(SessionConfigPatch {
        tools: Some(
            ["exec_command", "use_skill"]
                .into_iter()
                .map(|name| {
                    builtin_tool_definition(name)
                        .expect("telegram channel tools should be built-in definitions")
                })
                .collect(),
        ),
        ..SessionConfigPatch::default()
    })
}
