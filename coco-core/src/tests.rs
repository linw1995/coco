use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use coco_llm::coco_mem::MemoryStore;
use coco_llm::{
    BackendCompletion, BackendError, CompletionBackend, LlmService, Provider, SessionConfig,
    SessionSnapshot,
};

use crate::{
    BranchResolveError, BranchResolver, ChannelKind, ConversationEngine, CoreService, EngineError,
    Error, FixedBranchResolver, InboundMessage,
};

type FakeResponseQueue =
    Arc<Mutex<HashMap<String, VecDeque<std::result::Result<String, BackendError>>>>>;

#[derive(Debug)]
struct FailingResolver;

impl BranchResolver for FailingResolver {
    fn resolve_branch(
        &self,
        _message: &InboundMessage,
    ) -> std::result::Result<String, BranchResolveError> {
        Err(BranchResolveError::ResolveFailed {
            message: "resolver failed".to_owned(),
        })
    }
}

#[derive(Debug, Clone)]
struct FakeBackend {
    responses: FakeResponseQueue,
}

impl FakeBackend {
    fn with_responses(entries: &[(&str, &[std::result::Result<&str, BackendError>])]) -> Self {
        let responses = entries
            .iter()
            .map(|(branch, responses)| {
                (
                    (*branch).to_owned(),
                    responses
                        .iter()
                        .map(|response| {
                            response
                                .as_ref()
                                .map(|text| (*text).to_owned())
                                .map_err(Clone::clone)
                        })
                        .collect(),
                )
            })
            .collect();

        Self {
            responses: Arc::new(Mutex::new(responses)),
        }
    }
}

#[async_trait]
impl CompletionBackend for FakeBackend {
    async fn complete(
        &self,
        _session: SessionSnapshot,
        request: coco_llm::ResolvedCompletionRequest,
    ) -> std::result::Result<BackendCompletion, BackendError> {
        let mut responses = self.responses.lock().unwrap();
        let queue = responses
            .get_mut(&request.branch)
            .expect("missing fake backend queue");
        let next = queue.pop_front().expect("missing fake backend response");
        next.map(|text| BackendCompletion { text })
    }
}

fn session_config(branch: &str) -> SessionConfig {
    SessionConfig {
        branch: branch.to_owned(),
        merge_parents: vec![],
        provider: Provider::OpenAi,
        model: "gpt-4.1-mini".to_owned(),
        system_prompt: "You are helpful.".to_owned(),
        prompt: "".to_owned(),
        tools: vec![],
        temperature: None,
        max_tokens: None,
        additional_params: None,
    }
}

#[tokio::test]
async fn core_service_routes_message_to_engine() {
    let store = MemoryStore::new();
    let backend = FakeBackend::with_responses(&[("main", &[Ok("hello from llm")])]);
    let llm = Arc::new(LlmService::new(store, backend));
    llm.create_session(session_config("main")).await.unwrap();
    let service = CoreService::new(
        FixedBranchResolver::new("main"),
        ConversationEngine::new(llm),
    );

    let response = service
        .handle_message(InboundMessage::cli("conversation", "user", "hello"))
        .await
        .unwrap();

    assert_eq!(response.text, "hello from llm");
}

#[tokio::test]
async fn core_service_returns_missing_session() {
    let store = MemoryStore::new();
    let backend = FakeBackend::with_responses(&[]);
    let llm = Arc::new(LlmService::new(store, backend));
    let service = CoreService::new(
        FixedBranchResolver::new("main"),
        ConversationEngine::new(llm),
    );

    let error = service
        .handle_message(InboundMessage::cli("conversation", "user", "hello"))
        .await
        .unwrap_err();

    assert!(matches!(error, Error::MissingSession { branch } if branch == "main"));
}

#[tokio::test]
async fn core_service_returns_branch_resolution_error_context() {
    let store = MemoryStore::new();
    let backend = FakeBackend::with_responses(&[]);
    let llm = Arc::new(LlmService::new(store, backend));
    let service = CoreService::new(FailingResolver, ConversationEngine::new(llm));

    let error = service
        .handle_message(InboundMessage::telegram("chat-1", "user-1", "hello"))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        Error::BranchResolveFailed {
            channel_kind: ChannelKind::Telegram,
            conversation_id,
            source: BranchResolveError::ResolveFailed { .. },
        } if conversation_id == "chat-1"
    ));
}

#[tokio::test]
async fn llm_engine_calls_prompt_and_returns_text() {
    let store = MemoryStore::new();
    let backend = FakeBackend::with_responses(&[("main", &[Ok("hello from llm")])]);
    let llm = Arc::new(LlmService::new(store, backend));
    llm.create_session(session_config("main")).await.unwrap();
    let engine = ConversationEngine::new(llm);

    let response = engine.complete("main", "hello").await.unwrap();

    assert_eq!(response, "hello from llm");
}

#[tokio::test]
async fn llm_engine_maps_missing_session_error() {
    let store = MemoryStore::new();
    let backend = FakeBackend::with_responses(&[]);
    let llm = Arc::new(LlmService::new(store, backend));
    let engine = ConversationEngine::new(llm);

    let error = engine.complete("main", "hello").await.unwrap_err();

    assert!(matches!(error, EngineError::SessionMissing { branch } if branch == "main"));
}

#[tokio::test]
async fn core_service_rejects_empty_message_text() {
    let store = MemoryStore::new();
    let backend = FakeBackend::with_responses(&[]);
    let llm = Arc::new(LlmService::new(store, backend));
    let service = CoreService::new(
        FixedBranchResolver::new("main"),
        ConversationEngine::new(llm),
    );

    let error = service
        .handle_message(InboundMessage::discord("channel", "user", "   "))
        .await
        .unwrap_err();

    assert!(matches!(error, Error::InvalidInput { message } if message == "message text is empty"));
}
