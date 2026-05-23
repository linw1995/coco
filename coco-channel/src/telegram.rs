use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;

use crate::error::TelegramTransportSnafu;
use crate::{
    ChannelRuntime, Error, InboundMessage, MessageHandler, Result, TelegramImageAttachment,
    TelegramInboundMessage,
};

const DEFAULT_POLL_TIMEOUT_SECS: u64 = 30;
const POLL_TIMEOUT_GRACE_SECS: u64 = 15;
const CONNECT_TIMEOUT_SECS: u64 = 10;
const TRANSPORT_RETRY_DELAY_SECS: u64 = 5;

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
        let response = self.get_updates().await?;
        self.handle_updates(handler, response).await
    }

    async fn run_once_with_poll_logging<H>(&mut self, handler: &H) -> Result<usize>
    where
        H: MessageHandler,
    {
        let response = self.get_updates_with_logging().await?;
        self.handle_updates(handler, response).await
    }

    async fn get_updates(&self) -> Result<GetUpdatesResponse> {
        self.transport.get_updates(self.get_updates_request()).await
    }

    async fn get_updates_with_logging(&self) -> Result<GetUpdatesResponse> {
        let request = self.get_updates_request();
        let poll_offset = request.offset;
        let poll_timeout_secs = request.timeout_secs;
        let request_timeout_secs = request_timeout_for_poll(poll_timeout_secs).as_secs();
        let started = Instant::now();

        match self.transport.get_updates(request).await {
            Ok(response) => {
                tracing::debug!(
                    poll_offset = ?poll_offset,
                    poll_timeout_secs,
                    request_timeout_secs,
                    elapsed_ms = elapsed_ms(started),
                    update_count = response.updates.len(),
                    "telegram channel polling completed"
                );
                Ok(response)
            }
            Err(error) if error.is_transport_failure() => {
                tracing::warn!(
                    error = %error,
                    transport_error_kind = ?transport_failure_kind(&error),
                    transport_status = ?transport_failure_status(&error),
                    poll_offset = ?poll_offset,
                    poll_timeout_secs,
                    request_timeout_secs,
                    elapsed_ms = elapsed_ms(started),
                    retry_delay_secs = TRANSPORT_RETRY_DELAY_SECS,
                    "telegram channel polling failed; retrying"
                );
                Err(error)
            }
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    transport_error_kind = ?transport_failure_kind(&error),
                    transport_status = ?transport_failure_status(&error),
                    poll_offset = ?poll_offset,
                    poll_timeout_secs,
                    request_timeout_secs,
                    elapsed_ms = elapsed_ms(started),
                    "telegram channel polling failed"
                );
                Err(error)
            }
        }
    }

    fn get_updates_request(&self) -> GetUpdatesRequest {
        GetUpdatesRequest {
            offset: self.offset,
            timeout_secs: self.poll_timeout_secs,
        }
    }

    async fn handle_updates<H>(
        &mut self,
        handler: &H,
        response: GetUpdatesResponse,
    ) -> Result<usize>
    where
        H: MessageHandler,
    {
        let mut handled = 0;

        for update in response.updates {
            self.offset = Some(update.update_id + 1);
            let Some(inbound) = update.to_inbound_message() else {
                continue;
            };
            if !self.allowed_chat_ids.is_empty()
                && !self.allowed_chat_ids.contains(inbound.conversation_id())
            {
                tracing::info!(
                    conversation_id = %inbound.conversation_id(),
                    sender_id = %inbound.sender_id(),
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

    async fn sleep_after_transport_error(&self) {
        tokio::time::sleep(Duration::from_secs(TRANSPORT_RETRY_DELAY_SECS)).await;
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
            match self.run_once_with_poll_logging(handler).await {
                Ok(_) => {}
                Err(error) if error.is_transport_failure() => {
                    self.sleep_after_transport_error().await;
                }
                Err(error) => return Err(error),
            }
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
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(CONNECT_TIMEOUT_SECS))
                // Keep Telegram long polling on HTTP/1.1. HTTP/2 is not required by the
                // Bot API and has known stability issues around cancelled keep-alive streams.
                .http1_only()
                .build()
                .expect("telegram reqwest client config should be valid"),
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
            .post_api::<_, Vec<TelegramUpdate>>(
                "getUpdates",
                &request,
                request_timeout_for_poll(request.timeout),
            )
            .await?;
        Ok(GetUpdatesResponse { updates })
    }
}

impl ReqwestTelegramTransport {
    async fn post_api<Request, Response>(
        &self,
        method: &str,
        request: &Request,
        request_timeout: Duration,
    ) -> Result<Response>
    where
        Request: Serialize + Sync,
        Response: for<'de> Deserialize<'de>,
    {
        let response = self
            .client
            .post(format!("{}/{}", self.base_url, method))
            .json(request)
            .timeout(request_timeout)
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

fn request_timeout_for_poll(poll_timeout_secs: u64) -> Duration {
    Duration::from_secs(poll_timeout_secs.saturating_add(POLL_TIMEOUT_GRACE_SECS))
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}

fn transport_failure_kind(error: &Error) -> Option<&'static str> {
    match error {
        Error::TelegramTransport { source } => Some(source.kind()),
        Error::Transport { .. } => Some("transport"),
        Error::Handler { .. } | Error::InvalidInput { .. } => None,
    }
}

fn transport_failure_status(error: &Error) -> Option<u16> {
    match error {
        Error::TelegramTransport { source } => source.status_code(),
        Error::Transport { .. } | Error::Handler { .. } | Error::InvalidInput { .. } => None,
    }
}

#[derive(Debug, Snafu)]
pub enum TelegramError {
    #[snafu(display("Telegram API request failed: {source}"))]
    Request {
        #[snafu(source(from(reqwest::Error, reqwest::Error::without_url)))]
        source: reqwest::Error,
    },

    #[snafu(display("Telegram API returned an error: {description}"))]
    Api { description: String },

    #[snafu(display("Telegram API response is missing result"))]
    MissingResult,
}

impl TelegramError {
    fn kind(&self) -> &'static str {
        match self {
            Self::Request { source } if source.is_connect() && source.is_timeout() => {
                "connect_timeout"
            }
            Self::Request { source } if source.is_timeout() => "timeout",
            Self::Request { source } if source.is_connect() => "connect",
            Self::Request { source } if source.is_status() => "status",
            Self::Request { source } if source.is_decode() => "decode",
            Self::Request { source } if source.is_body() => "body",
            Self::Request { .. } => "request",
            Self::Api { .. } => "api",
            Self::MissingResult => "missing_result",
        }
    }

    fn status_code(&self) -> Option<u16> {
        match self {
            Self::Request { source } => source.status().map(|status| status.as_u16()),
            Self::Api { .. } | Self::MissingResult => None,
        }
    }
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
        let text = message
            .text
            .as_deref()
            .or(message.caption.as_deref())
            .unwrap_or_default();
        let image_attachments = message.image_attachments();
        if text.is_empty() && image_attachments.is_empty() {
            return None;
        }
        let sender_id = message
            .from
            .as_ref()
            .map(|user| user.id.to_string())
            .unwrap_or_else(|| message.chat.id.to_string());

        Some(InboundMessage::Telegram(
            TelegramInboundMessage::with_message_id_and_images(
                message.chat.id.to_string(),
                sender_id,
                message.message_id.to_string(),
                text.to_owned(),
                image_attachments,
            ),
        ))
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct TelegramMessage {
    pub message_id: i64,
    pub chat: TelegramChat,
    pub from: Option<TelegramUser>,
    pub text: Option<String>,
    pub caption: Option<String>,
    #[serde(default)]
    pub photo: Vec<TelegramPhotoSize>,
}

impl TelegramMessage {
    fn image_attachments(&self) -> Vec<TelegramImageAttachment> {
        self.photo
            .iter()
            .max_by_key(|photo| {
                (
                    u64::from(photo.width) * u64::from(photo.height),
                    photo.file_size.unwrap_or(0),
                )
            })
            .map(|photo| {
                TelegramImageAttachment::from_parts(
                    photo.file_id.clone(),
                    Some(photo.file_unique_id.clone()),
                    Some(photo.width),
                    Some(photo.height),
                    photo.file_size,
                )
            })
            .into_iter()
            .collect()
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct TelegramPhotoSize {
    pub file_id: String,
    pub file_unique_id: String,
    pub width: u32,
    pub height: u32,
    pub file_size: Option<u64>,
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
    use tokio::io::AsyncWriteExt;

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

    struct FailingTransport;

    #[async_trait]
    impl TelegramTransport for FailingTransport {
        async fn get_updates(&self, _request: GetUpdatesRequest) -> Result<GetUpdatesResponse> {
            Err(Error::transport(std::io::Error::other("temporary outage")))
        }
    }

    struct InvalidInputTransport;

    #[async_trait]
    impl TelegramTransport for InvalidInputTransport {
        async fn get_updates(&self, _request: GetUpdatesRequest) -> Result<GetUpdatesResponse> {
            Err(Error::invalid_input("invalid poll request"))
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

    struct FailingHandler;

    #[async_trait]
    impl MessageHandler for FailingHandler {
        async fn handle(&self, _message: InboundMessage) -> Result<OutboundMessage> {
            Err(Error::handler(std::io::Error::other("handler failed")))
        }
    }

    #[test]
    fn from_config_builds_reqwest_transport() {
        let mut allowed_chat_ids = BTreeSet::new();
        allowed_chat_ids.insert("42".to_owned());
        let config = TelegramChannelConfig {
            token: "123456:secret-token".to_owned(),
            poll_timeout_secs: 30,
            allowed_chat_ids: allowed_chat_ids.clone(),
        };

        let channel = TelegramChannel::from_config(config).unwrap();

        assert_eq!(channel.poll_timeout_secs, 30);
        assert_eq!(channel.allowed_chat_ids, allowed_chat_ids);
        assert_eq!(channel.offset(), None);
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
            vec![InboundMessage::telegram_with_message_id(
                "42", "7", "1000", "hello"
            )]
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
                caption: None,
                photo: Vec::new(),
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
                caption: None,
                photo: Vec::new(),
            }),
        };

        let message = update.to_inbound_message().unwrap();

        assert_eq!(message.conversation_id(), "-42");
        assert_eq!(message.sender_id(), "-42");
        assert_eq!(message.source_message_id(), Some("1000"));
        assert_eq!(message.text(), "hello");
    }

    #[tokio::test]
    async fn run_once_maps_photo_update_without_text() {
        let transport = FakeTransport::with_updates(vec![photo_update(100, 42, 7, None)]);
        let mut channel = TelegramChannel::new(transport, 30, BTreeSet::new());
        let handler = RecordingHandler::default();

        let handled = channel.run_once(&handler).await.unwrap();

        assert_eq!(handled, 1);
        assert_eq!(channel.offset(), Some(101));
        let messages = handler.messages();
        assert_eq!(messages.len(), 1);
        let InboundMessage::Telegram(message) = &messages[0] else {
            panic!("expected telegram inbound message");
        };
        assert_eq!(message.chat_id(), "42");
        assert_eq!(message.sender_id(), "7");
        assert_eq!(message.source_message_id(), Some("1000"));
        assert_eq!(message.text(), "");
        assert_eq!(message.image_attachments().len(), 1);
        let image = &message.image_attachments()[0];
        assert_eq!(image.file_id(), "large-file-id");
        assert_eq!(image.file_unique_id(), Some("large-unique-id"));
        assert_eq!(image.width(), Some(1280));
        assert_eq!(image.height(), Some(960));
        assert_eq!(image.file_size(), Some(200_000));
    }

    #[test]
    fn update_maps_photo_caption_as_text() {
        let update = photo_update(100, 42, 7, Some("what is in this image?"));

        let message = update.to_inbound_message().unwrap();

        assert_eq!(message.text(), "what is in this image?");
        let InboundMessage::Telegram(message) = message else {
            panic!("expected telegram inbound message");
        };
        assert_eq!(message.image_attachments().len(), 1);
    }

    #[tokio::test]
    async fn run_retries_transport_failures_instead_of_exiting() {
        let channel = TelegramChannel::new(FailingTransport, 30, BTreeSet::new());
        let handler = RecordingHandler::default();

        let task = tokio::spawn(async move { channel.run(&handler).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(!task.is_finished());
        task.abort();
    }

    #[tokio::test]
    async fn run_returns_non_transport_poll_errors() {
        let channel = TelegramChannel::new(InvalidInputTransport, 30, BTreeSet::new());
        let handler = RecordingHandler::default();

        let result = channel.run(&handler).await;

        assert!(matches!(result, Err(Error::InvalidInput { .. })));
    }

    #[tokio::test]
    async fn run_still_returns_handler_failures() {
        let channel = TelegramChannel::new(
            FakeTransport::with_updates(vec![text_update(100, 42, 7, "hello")]),
            30,
            BTreeSet::new(),
        );

        let result = channel.run(&FailingHandler).await;

        assert!(matches!(result, Err(Error::Handler { .. })));
    }

    #[tokio::test]
    async fn request_error_display_does_not_include_token_url() {
        let token = "123456:secret-token";
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);

        let source = reqwest::Client::new()
            .post(format!("http://{address}/bot{token}/getUpdates"))
            .timeout(Duration::from_millis(100))
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .unwrap_err();
        assert!(source.url().unwrap().as_str().contains(token));

        let error = Err::<(), _>(source).context(RequestSnafu).unwrap_err();
        let message = error.to_string();

        assert!(!message.contains(token));
        assert!(!message.contains("/bot"));
    }

    #[tokio::test]
    async fn request_error_kind_preserves_status_without_url() {
        let token = "123456:secret-token";
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream
                .write_all(
                    b"HTTP/1.1 429 Too Many Requests\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
                )
                .await
                .unwrap();
        });

        let source = reqwest::Client::new()
            .post(format!("http://{address}/bot{token}/getUpdates"))
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .unwrap_err();
        assert!(source.url().unwrap().as_str().contains(token));

        let error = Err::<(), _>(source).context(RequestSnafu).unwrap_err();

        assert_eq!(error.kind(), "status");
        assert_eq!(error.status_code(), Some(429));
        assert!(!error.to_string().contains(token));

        server.await.unwrap();
    }

    #[test]
    fn get_updates_request_timeout_tracks_poll_timeout() {
        assert_eq!(request_timeout_for_poll(30), Duration::from_secs(45));
        assert_eq!(request_timeout_for_poll(120), Duration::from_secs(135));
        assert_eq!(
            request_timeout_for_poll(u64::MAX),
            Duration::from_secs(u64::MAX)
        );
    }

    fn text_update(update_id: i64, chat_id: i64, user_id: i64, text: &str) -> TelegramUpdate {
        TelegramUpdate {
            update_id,
            message: Some(TelegramMessage {
                message_id: 1000,
                chat: TelegramChat { id: chat_id },
                from: Some(TelegramUser { id: user_id }),
                text: Some(text.to_owned()),
                caption: None,
                photo: Vec::new(),
            }),
        }
    }

    fn photo_update(
        update_id: i64,
        chat_id: i64,
        user_id: i64,
        caption: Option<&str>,
    ) -> TelegramUpdate {
        TelegramUpdate {
            update_id,
            message: Some(TelegramMessage {
                message_id: 1000,
                chat: TelegramChat { id: chat_id },
                from: Some(TelegramUser { id: user_id }),
                text: None,
                caption: caption.map(str::to_owned),
                photo: vec![
                    TelegramPhotoSize {
                        file_id: "small-file-id".to_owned(),
                        file_unique_id: "small-unique-id".to_owned(),
                        width: 320,
                        height: 240,
                        file_size: Some(20_000),
                    },
                    TelegramPhotoSize {
                        file_id: "large-file-id".to_owned(),
                        file_unique_id: "large-unique-id".to_owned(),
                        width: 1280,
                        height: 960,
                        file_size: Some(200_000),
                    },
                ],
            }),
        }
    }
}
