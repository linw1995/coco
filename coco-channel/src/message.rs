use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelKind {
    Cli,
    Telegram,
}

impl ChannelKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cli => "cli",
            Self::Telegram => "telegram",
        }
    }
}

impl fmt::Display for ChannelKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum InboundMessage {
    Cli(ChannelInboundMessage),
    Telegram(TelegramInboundMessage),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChannelInboundMessage {
    conversation_id: String,
    sender_id: String,
    text: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TelegramInboundMessage {
    chat_id: String,
    sender_id: String,
    source_message_id: Option<String>,
    text: String,
    image_attachments: Vec<TelegramImageAttachment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelegramImageAttachment {
    file_id: String,
    file_unique_id: Option<String>,
    width: Option<u32>,
    height: Option<u32>,
    file_size: Option<u64>,
}

impl InboundMessage {
    pub fn cli(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        Self::Cli(ChannelInboundMessage::new(conversation_id, sender_id, text))
    }

    pub fn telegram(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        Self::Telegram(TelegramInboundMessage::new(
            conversation_id,
            sender_id,
            text,
        ))
    }

    pub fn telegram_with_images(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        text: impl Into<String>,
        image_attachments: Vec<TelegramImageAttachment>,
    ) -> Self {
        Self::Telegram(TelegramInboundMessage::new_with_images(
            conversation_id,
            sender_id,
            text,
            image_attachments,
        ))
    }

    pub fn telegram_with_message_id(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        source_message_id: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        Self::Telegram(TelegramInboundMessage::with_message_id(
            conversation_id,
            sender_id,
            source_message_id,
            text,
        ))
    }

    pub fn telegram_with_message_id_and_images(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        source_message_id: impl Into<String>,
        text: impl Into<String>,
        image_attachments: Vec<TelegramImageAttachment>,
    ) -> Self {
        Self::Telegram(TelegramInboundMessage::with_message_id_and_images(
            conversation_id,
            sender_id,
            source_message_id,
            text,
            image_attachments,
        ))
    }

    pub fn channel_kind(&self) -> ChannelKind {
        match self {
            Self::Cli(_) => ChannelKind::Cli,
            Self::Telegram(_) => ChannelKind::Telegram,
        }
    }

    pub fn conversation_id(&self) -> &str {
        match self {
            Self::Cli(message) => message.conversation_id(),
            Self::Telegram(message) => message.chat_id(),
        }
    }

    pub fn sender_id(&self) -> &str {
        match self {
            Self::Cli(message) => message.sender_id(),
            Self::Telegram(message) => message.sender_id(),
        }
    }

    pub fn source_message_id(&self) -> Option<&str> {
        match self {
            Self::Cli(_) => None,
            Self::Telegram(message) => message.source_message_id(),
        }
    }

    pub fn text(&self) -> &str {
        match self {
            Self::Cli(message) => message.text(),
            Self::Telegram(message) => message.text(),
        }
    }

    pub fn into_text(self) -> String {
        match self {
            Self::Cli(message) => message.into_text(),
            Self::Telegram(message) => message.into_text(),
        }
    }

    pub fn has_image_attachments(&self) -> bool {
        match self {
            Self::Cli(_) => false,
            Self::Telegram(message) => message.has_image_attachments(),
        }
    }
}

impl ChannelInboundMessage {
    fn new(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        Self {
            conversation_id: conversation_id.into(),
            sender_id: sender_id.into(),
            text: text.into(),
        }
    }

    pub fn conversation_id(&self) -> &str {
        self.conversation_id.as_str()
    }

    pub fn sender_id(&self) -> &str {
        self.sender_id.as_str()
    }

    pub fn text(&self) -> &str {
        self.text.as_str()
    }

    pub fn into_text(self) -> String {
        self.text
    }
}

impl TelegramInboundMessage {
    pub fn new(
        chat_id: impl Into<String>,
        sender_id: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        Self::from_parts(chat_id, sender_id, None, text, Vec::new())
    }

    pub fn new_with_images(
        chat_id: impl Into<String>,
        sender_id: impl Into<String>,
        text: impl Into<String>,
        image_attachments: Vec<TelegramImageAttachment>,
    ) -> Self {
        Self::from_parts(chat_id, sender_id, None, text, image_attachments)
    }

    pub fn with_message_id(
        chat_id: impl Into<String>,
        sender_id: impl Into<String>,
        source_message_id: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        Self::from_parts(
            chat_id,
            sender_id,
            Some(source_message_id.into()),
            text,
            Vec::new(),
        )
    }

    pub fn with_message_id_and_images(
        chat_id: impl Into<String>,
        sender_id: impl Into<String>,
        source_message_id: impl Into<String>,
        text: impl Into<String>,
        image_attachments: Vec<TelegramImageAttachment>,
    ) -> Self {
        Self::from_parts(
            chat_id,
            sender_id,
            Some(source_message_id.into()),
            text,
            image_attachments,
        )
    }

    pub fn chat_id(&self) -> &str {
        self.chat_id.as_str()
    }

    pub fn sender_id(&self) -> &str {
        self.sender_id.as_str()
    }

    pub fn source_message_id(&self) -> Option<&str> {
        self.source_message_id.as_deref()
    }

    pub fn text(&self) -> &str {
        self.text.as_str()
    }

    pub fn into_text(self) -> String {
        self.text
    }

    pub fn image_attachments(&self) -> &[TelegramImageAttachment] {
        &self.image_attachments
    }

    pub fn has_image_attachments(&self) -> bool {
        !self.image_attachments.is_empty()
    }

    fn from_parts(
        chat_id: impl Into<String>,
        sender_id: impl Into<String>,
        source_message_id: Option<String>,
        text: impl Into<String>,
        image_attachments: Vec<TelegramImageAttachment>,
    ) -> Self {
        Self {
            chat_id: chat_id.into(),
            sender_id: sender_id.into(),
            source_message_id,
            text: text.into(),
            image_attachments,
        }
    }
}

impl TelegramImageAttachment {
    pub fn from_parts(
        file_id: impl Into<String>,
        file_unique_id: Option<String>,
        width: Option<u32>,
        height: Option<u32>,
        file_size: Option<u64>,
    ) -> Self {
        Self {
            file_id: file_id.into(),
            file_unique_id,
            width,
            height,
            file_size,
        }
    }

    pub fn file_id(&self) -> &str {
        self.file_id.as_str()
    }

    pub fn file_unique_id(&self) -> Option<&str> {
        self.file_unique_id.as_deref()
    }

    pub fn width(&self) -> Option<u32> {
        self.width
    }

    pub fn height(&self) -> Option<u32> {
        self.height
    }

    pub fn file_size(&self) -> Option<u64> {
        self.file_size
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OutboundMessage {
    pub text: String,
}
