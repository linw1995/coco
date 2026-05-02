use std::collections::BTreeSet;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;

use crate::error::TelegramTransportSnafu;
use crate::{ChannelRuntime, Error, InboundMessage, MessageHandler, Result};

const DEFAULT_POLL_TIMEOUT_SECS: u64 = 30;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelegramChannelConfig {
    pub token: String,
    pub poll_timeout_secs: u64,
    pub allowed_chat_ids: BTreeSet<String>,
}

impl TelegramChannelConfig {
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
            poll_timeout_secs: DEFAULT_POLL_TIMEOUT_SECS,
            allowed_chat_ids: BTreeSet::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetUpdatesRequest {
    pub offset: Option<i64>,
    pub timeout_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetUpdatesResponse {
    pub updates: Vec<TelegramUpdate>,
}

#[async_trait]
pub trait TelegramTransport: Send + Sync {
    async fn get_updates(&self, request: GetUpdatesRequest) -> Result<GetUpdatesResponse>;
}

#[derive(Debug, Clone)]
pub struct TelegramChannel<T = ReqwestTelegramTransport> {
    transport: T,
    poll_timeout_secs: u64,
    allowed_chat_ids: BTreeSet<String>,
    offset: Option<i64>,
}

impl TelegramChannel<ReqwestTelegramTransport> {
    pub fn from_config(config: TelegramChannelConfig) -> Result<Self> {
        if config.token.trim().is_empty() {
            return Err(Error::invalid_input("Telegram token is empty"));
        }

        Ok(Self::new(
            ReqwestTelegramTransport::new(config.token),
            config.poll_timeout_secs,
            config.allowed_chat_ids,
        ))
    }
}

impl<T> TelegramChannel<T> {
    pub fn new(transport: T, poll_timeout_secs: u64, allowed_chat_ids: BTreeSet<String>) -> Self {
        Self {
            transport,
            poll_timeout_secs,
            allowed_chat_ids,
            offset: None,
        }
    }

    pub fn offset(&self) -> Option<i64> {
        self.offset
    }
}

impl<T> TelegramChannel<T>
where
    T: TelegramTransport,
{
    pub async fn run_once<H>(&mut self, handler: &H) -> Result<usize>
    where
        H: MessageHandler,
    {
        let response = self
            .transport
            .get_updates(GetUpdatesRequest {
                offset: self.offset,
                timeout_secs: self.poll_timeout_secs,
            })
            .await?;
        let mut handled = 0;

        for update in response.updates {
            self.offset = Some(update.update_id + 1);
            let Some(inbound) = update.to_inbound_message() else {
                continue;
            };
            if !self.allowed_chat_ids.is_empty()
                && !self.allowed_chat_ids.contains(&inbound.conversation_id)
            {
                tracing::info!(
                    conversation_id = %inbound.conversation_id,
                    sender_id = %inbound.sender_id,
                    text = %inbound.text,
                    allowed_chat_ids = ?self.allowed_chat_ids,
                    "filtered telegram inbound message by allowed_chat_ids"
                );
                continue;
            }

            handler.handle(inbound).await?;
            handled += 1;
        }

        Ok(handled)
    }
}

#[async_trait]
impl<T> ChannelRuntime for TelegramChannel<T>
where
    T: TelegramTransport + Send,
{
    async fn run<H>(mut self, handler: &H) -> Result<()>
    where
        H: MessageHandler,
    {
        loop {
            self.run_once(handler).await?;
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReqwestTelegramTransport {
    client: reqwest::Client,
    base_url: String,
}

impl ReqwestTelegramTransport {
    pub fn new(token: impl Into<String>) -> Self {
        let token = token.into();
        Self {
            client: reqwest::Client::new(),
            base_url: format!("https://api.telegram.org/bot{token}"),
        }
    }
}

#[async_trait]
impl TelegramTransport for ReqwestTelegramTransport {
    async fn get_updates(&self, request: GetUpdatesRequest) -> Result<GetUpdatesResponse> {
        let request = TelegramGetUpdatesRequest {
            offset: request.offset,
            timeout: request.timeout_secs,
            allowed_updates: ["message"],
        };
        let updates = self
            .post_api::<_, Vec<TelegramUpdate>>("getUpdates", &request)
            .await?;
        Ok(GetUpdatesResponse { updates })
    }
}

impl ReqwestTelegramTransport {
    async fn post_api<Request, Response>(&self, method: &str, request: &Request) -> Result<Response>
    where
        Request: Serialize + Sync,
        Response: for<'de> Deserialize<'de>,
    {
        let response = self
            .client
            .post(format!("{}/{}", self.base_url, method))
            .json(request)
            .timeout(Duration::from_secs(DEFAULT_POLL_TIMEOUT_SECS + 5))
            .send()
            .await
            .context(RequestSnafu)
            .context(TelegramTransportSnafu)?
            .error_for_status()
            .context(RequestSnafu)
            .context(TelegramTransportSnafu)?
            .json::<TelegramApiResponse<Response>>()
            .await
            .context(RequestSnafu)
            .context(TelegramTransportSnafu)?;

        response.into_result().context(TelegramTransportSnafu)
    }
}

#[derive(Debug, Snafu)]
pub enum TelegramError {
    #[snafu(display("Telegram API request failed: {source}"))]
    Request { source: reqwest::Error },

    #[snafu(display("Telegram API returned an error: {description}"))]
    Api { description: String },

    #[snafu(display("Telegram API response is missing result"))]
    MissingResult,
}

#[derive(Debug, Serialize)]
struct TelegramGetUpdatesRequest<'a> {
    offset: Option<i64>,
    timeout: u64,
    allowed_updates: [&'a str; 1],
}

#[derive(Debug, Deserialize)]
struct TelegramApiResponse<T> {
    ok: bool,
    result: Option<T>,
    description: Option<String>,
}

impl<T> TelegramApiResponse<T> {
    fn into_result(self) -> std::result::Result<T, TelegramError> {
        if !self.ok {
            return ApiSnafu {
                description: self
                    .description
                    .unwrap_or_else(|| "unknown Telegram API error".to_owned()),
            }
            .fail();
        }

        self.result.context(MissingResultSnafu)
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct TelegramUpdate {
    pub update_id: i64,
    pub message: Option<TelegramMessage>,
}

impl TelegramUpdate {
    pub fn to_inbound_message(&self) -> Option<InboundMessage> {
        let message = self.message.as_ref()?;
        let text = message.text.as_ref()?;
        let sender_id = message
            .from
            .as_ref()
            .map(|user| user.id.to_string())
            .unwrap_or_else(|| message.chat.id.to_string());

        Some(InboundMessage::telegram_with_message_id(
            message.chat.id.to_string(),
            sender_id,
            message.message_id.to_string(),
            text.clone(),
        ))
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct TelegramMessage {
    pub message_id: i64,
    pub chat: TelegramChat,
    pub from: Option<TelegramUser>,
    pub text: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct TelegramChat {
    pub id: i64,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct TelegramUser {
    pub id: i64,
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use super::*;
    use crate::OutboundMessage;

    #[derive(Default)]
    struct FakeTransport {
        update_batches: Mutex<VecDeque<Vec<TelegramUpdate>>>,
        get_updates_requests: Mutex<Vec<GetUpdatesRequest>>,
    }

    impl FakeTransport {
        fn with_updates(updates: Vec<TelegramUpdate>) -> Self {
            Self {
                update_batches: Mutex::new(VecDeque::from([updates])),
                get_updates_requests: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl TelegramTransport for FakeTransport {
        async fn get_updates(&self, request: GetUpdatesRequest) -> Result<GetUpdatesResponse> {
            self.get_updates_requests.lock().unwrap().push(request);
            Ok(GetUpdatesResponse {
                updates: self
                    .update_batches
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or_default(),
            })
        }
    }

    #[derive(Default)]
    struct RecordingHandler {
        messages: Mutex<Vec<InboundMessage>>,
    }

    impl RecordingHandler {
        fn messages(&self) -> Vec<InboundMessage> {
            self.messages.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl MessageHandler for RecordingHandler {
        async fn handle(&self, message: InboundMessage) -> Result<OutboundMessage> {
            self.messages.lock().unwrap().push(message);
            Ok(OutboundMessage {
                text: "ignored by telegram channel".to_owned(),
            })
        }
    }

    #[tokio::test]
    async fn run_once_maps_text_update_without_sending_reply() {
        let transport = FakeTransport::with_updates(vec![text_update(100, 42, 7, "hello")]);
        let mut channel = TelegramChannel::new(transport, 30, BTreeSet::new());
        let handler = RecordingHandler::default();

        let handled = channel.run_once(&handler).await.unwrap();

        assert_eq!(handled, 1);
        assert_eq!(channel.offset(), Some(101));
        assert_eq!(
            handler.messages(),
            vec![InboundMessage {
                channel_kind: crate::ChannelKind::Telegram,
                conversation_id: "42".to_owned(),
                sender_id: "7".to_owned(),
                source_message_id: Some("1000".to_owned()),
                text: "hello".to_owned(),
            }]
        );
    }

    #[tokio::test]
    async fn run_once_ignores_non_text_updates_and_advances_offset() {
        let transport = FakeTransport::with_updates(vec![TelegramUpdate {
            update_id: 100,
            message: Some(TelegramMessage {
                message_id: 1000,
                chat: TelegramChat { id: 42 },
                from: Some(TelegramUser { id: 7 }),
                text: None,
            }),
        }]);
        let mut channel = TelegramChannel::new(transport, 30, BTreeSet::new());
        let handler = RecordingHandler::default();

        let handled = channel.run_once(&handler).await.unwrap();

        assert_eq!(handled, 0);
        assert_eq!(channel.offset(), Some(101));
        assert!(handler.messages().is_empty());
    }

    #[tokio::test]
    async fn run_once_filters_disallowed_chats() {
        let transport = FakeTransport::with_updates(vec![text_update(100, 42, 7, "hello")]);
        let mut allowed_chat_ids = BTreeSet::new();
        allowed_chat_ids.insert("99".to_owned());
        let mut channel = TelegramChannel::new(transport, 30, allowed_chat_ids);
        let handler = RecordingHandler::default();

        let handled = channel.run_once(&handler).await.unwrap();

        assert_eq!(handled, 0);
        assert_eq!(channel.offset(), Some(101));
        assert!(handler.messages().is_empty());
    }

    #[test]
    fn update_maps_missing_user_to_chat_sender() {
        let update = TelegramUpdate {
            update_id: 100,
            message: Some(TelegramMessage {
                message_id: 1000,
                chat: TelegramChat { id: -42 },
                from: None,
                text: Some("hello".to_owned()),
            }),
        };

        let message = update.to_inbound_message().unwrap();

        assert_eq!(message.conversation_id, "-42");
        assert_eq!(message.sender_id, "-42");
        assert_eq!(message.source_message_id.as_deref(), Some("1000"));
        assert_eq!(message.text, "hello");
    }

    fn text_update(update_id: i64, chat_id: i64, user_id: i64, text: &str) -> TelegramUpdate {
        TelegramUpdate {
            update_id,
            message: Some(TelegramMessage {
                message_id: 1000,
                chat: TelegramChat { id: chat_id },
                from: Some(TelegramUser { id: user_id }),
                text: Some(text.to_owned()),
            }),
        }
    }
}
