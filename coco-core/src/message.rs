use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelKind {
    Cli,
    Telegram,
    Discord,
}

impl ChannelKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cli => "cli",
            Self::Telegram => "telegram",
            Self::Discord => "discord",
        }
    }
}

impl fmt::Display for ChannelKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundMessage {
    pub channel_kind: ChannelKind,
    pub conversation_id: String,
    pub sender_id: String,
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
            text: text.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundMessage {
    pub text: String,
}
