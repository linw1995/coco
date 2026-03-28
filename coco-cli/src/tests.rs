use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::io::Cursor;
use std::sync::{Arc, Mutex, OnceLock};

use coco_llm::coco_mem::Store;
use coco_llm::{
    BackendCompletion, BackendError, CompletionBackend, Provider, ResolvedCompletionRequest,
    SessionSnapshot,
};
use tempfile::{TempDir, tempdir};
use tokio::sync::Mutex as AsyncMutex;

use crate::{
    Cli,
    app::{resolve_session_config, run_with_backend},
    cli::{Command, PromptCommand, SessionCommand, SessionCreateCommand, SessionSubcommand},
    store::open_store,
};

type FakeResponseQueue =
    Arc<Mutex<HashMap<String, VecDeque<std::result::Result<String, BackendError>>>>>;

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

#[async_trait::async_trait]
impl CompletionBackend for FakeBackend {
    async fn complete(
        &self,
        _session: SessionSnapshot,
        request: ResolvedCompletionRequest,
    ) -> std::result::Result<BackendCompletion, BackendError> {
        let mut responses = self.responses.lock().unwrap();
        let queue = responses
            .get_mut(&request.branch)
            .expect("missing fake backend queue");
        let next = queue.pop_front().expect("missing fake backend response");
        next.map(|text| BackendCompletion { text })
    }
}

fn prompt_cli(store_path: std::path::PathBuf, branch: Option<&str>, text: &[&str]) -> Cli {
    Cli {
        store_path,
        command: Command::Prompt(PromptCommand {
            branch: branch.unwrap_or("main").to_owned(),
            text: text.iter().map(|part| (*part).to_owned()).collect(),
        }),
    }
}

fn session_create_cli(store_path: std::path::PathBuf, branch: Option<&str>) -> Cli {
    Cli {
        store_path,
        command: Command::Session(SessionCommand {
            command: SessionSubcommand::Create(SessionCreateCommand {
                branch: branch.unwrap_or("main").to_owned(),
                system_prompt: "You are helpful.".to_owned(),
                prompt: "".to_owned(),
                temperature: Some(0.2),
                max_tokens: Some(64),
            }),
        }),
    }
}

fn temp_store_path() -> (TempDir, std::path::PathBuf) {
    let tempdir = tempdir().unwrap();
    let store_path = tempdir.path().join("store");
    (tempdir, store_path)
}

async fn with_coco_env_async<T, F, Fut>(entries: &[(&str, &str)], run: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    static ENV_LOCK: OnceLock<AsyncMutex<()>> = OnceLock::new();

    let _guard = ENV_LOCK.get_or_init(|| AsyncMutex::new(())).lock().await;
    let previous: Vec<_> = entries
        .iter()
        .map(|(name, _)| ((*name).to_owned(), std::env::var(name).ok()))
        .collect();

    for (name, value) in entries {
        unsafe {
            std::env::set_var(name, value);
        }
    }

    let output = run().await;

    for (name, value) in previous {
        match value {
            Some(value) => unsafe {
                std::env::set_var(name, value);
            },
            None => unsafe {
                std::env::remove_var(name);
            },
        }
    }

    output
}

#[tokio::test]
async fn prompt_uses_main_branch_by_default() {
    let (_tempdir, store_path) = temp_store_path();
    with_coco_env_async(
        &[("COCO_PROVIDER", "openai"), ("COCO_MODEL", "gpt-4.1-mini")],
        || async {
            run_with_backend(
                session_create_cli(store_path.clone(), None),
                &mut Cursor::new(""),
                FakeBackend::with_responses(&[]),
            )
            .await
            .unwrap();
        },
    )
    .await;

    let output = run_with_backend(
        prompt_cli(store_path, None, &["hello"]),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[("main", &[Ok("world")])]),
    )
    .await
    .unwrap();

    assert_eq!(output, Some("world".to_owned()));
}

#[tokio::test]
async fn prompt_supports_explicit_branch_override() {
    let (_tempdir, store_path) = temp_store_path();
    with_coco_env_async(
        &[("COCO_PROVIDER", "openai"), ("COCO_MODEL", "gpt-4.1-mini")],
        || async {
            run_with_backend(
                session_create_cli(store_path.clone(), Some("draft")),
                &mut Cursor::new(""),
                FakeBackend::with_responses(&[]),
            )
            .await
            .unwrap();
        },
    )
    .await;

    let output = run_with_backend(
        prompt_cli(store_path, Some("draft"), &["hello"]),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[("draft", &[Ok("world")])]),
    )
    .await
    .unwrap();

    assert_eq!(output, Some("world".to_owned()));
}

#[tokio::test]
async fn prompt_reads_text_from_stdin() {
    let (_tempdir, store_path) = temp_store_path();
    with_coco_env_async(
        &[("COCO_PROVIDER", "openai"), ("COCO_MODEL", "gpt-4.1-mini")],
        || async {
            run_with_backend(
                session_create_cli(store_path.clone(), None),
                &mut Cursor::new(""),
                FakeBackend::with_responses(&[]),
            )
            .await
            .unwrap();
        },
    )
    .await;

    let output = run_with_backend(
        prompt_cli(store_path, None, &[]),
        &mut Cursor::new("hello from stdin\n"),
        FakeBackend::with_responses(&[("main", &[Ok("world")])]),
    )
    .await
    .unwrap();

    assert_eq!(output, Some("world".to_owned()));
}

#[tokio::test]
async fn prompt_returns_missing_session_when_branch_does_not_exist() {
    let (_tempdir, store_path) = temp_store_path();

    let error = run_with_backend(
        prompt_cli(store_path, None, &["hello"]),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap_err();

    assert!(
        matches!(error, crate::Error::Core { source: coco_core::Error::MissingSession { branch } } if branch == "main")
    );
}

#[tokio::test]
async fn session_create_persists_branch_for_future_prompt_calls() {
    let (_tempdir, store_path) = temp_store_path();
    with_coco_env_async(
        &[("COCO_PROVIDER", "openai"), ("COCO_MODEL", "gpt-4.1-mini")],
        || async {
            run_with_backend(
                session_create_cli(store_path.clone(), Some("main")),
                &mut Cursor::new(""),
                FakeBackend::with_responses(&[]),
            )
            .await
            .unwrap();
        },
    )
    .await;

    let store = open_store(&store_path).unwrap();
    assert_eq!(store.get_branch_head("main").unwrap().len(), 64);

    let output = run_with_backend(
        prompt_cli(store_path, Some("main"), &["hello"]),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[("main", &[Ok("persisted")])]),
    )
    .await
    .unwrap();

    assert_eq!(output, Some("persisted".to_owned()));
}

#[test]
fn resolve_session_config_reads_coco_prefixed_env_only() {
    let config = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(with_coco_env_async(
            &[
                ("COCO_PROVIDER", "anthropic"),
                ("COCO_MODEL", "claude-sonnet-4-20250514"),
            ],
            || async {
                resolve_session_config(SessionCreateCommand {
                    branch: "main".to_owned(),
                    system_prompt: "You are helpful.".to_owned(),
                    prompt: "".to_owned(),
                    temperature: Some(0.2),
                    max_tokens: Some(64),
                })
                .unwrap()
            },
        ));

    assert_eq!(config.provider, Provider::Anthropic);
    assert_eq!(config.model, "claude-sonnet-4-20250514");
}
