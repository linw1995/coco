use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelKind {
    Cli,
    Scheduler,
    Telegram,
    Discord,
}

impl ChannelKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cli => "cli",
            Self::Scheduler => "scheduler",
            Self::Telegram => "telegram",
            Self::Discord => "discord",
        }
    }
}

impl fmt::Display for ChannelKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct InboundMessage {
    pub channel_kind: ChannelKind,
    pub conversation_id: String,
    pub sender_id: String,
    pub source_message_id: Option<String>,
    pub text: String,
}

impl InboundMessage {
    pub fn cli(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        Self::new(ChannelKind::Cli, conversation_id, sender_id, text)
    }

    pub fn telegram(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        Self::new(ChannelKind::Telegram, conversation_id, sender_id, text)
    }

    pub fn scheduler(
        branch: impl Into<String>,
        sender_id: impl Into<String>,
        task_id: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        let mut message = Self::new(ChannelKind::Scheduler, branch, sender_id, text);
        message.source_message_id = Some(task_id.into());
        message
    }

    pub fn telegram_with_message_id(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        source_message_id: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        let mut message = Self::telegram(conversation_id, sender_id, text);
        message.source_message_id = Some(source_message_id.into());
        message
    }

    pub fn discord(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        Self::new(ChannelKind::Discord, conversation_id, sender_id, text)
    }

    fn new(
        channel_kind: ChannelKind,
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        Self {
            channel_kind,
            conversation_id: conversation_id.into(),
            sender_id: sender_id.into(),
            source_message_id: None,
            text: text.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OutboundMessage {
    pub text: String,
}
