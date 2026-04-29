use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs;
use std::future::Future;
use std::io::Cursor;
use std::sync::{Arc, Mutex, OnceLock};

use clap::Parser;
use coco_llm::coco_mem::{
    Anchor, BackendMetadata, BranchStore, JobStore, Kind, NewNode, NodeStore, PromptAnchor,
    ProviderMetadata, Role, SessionAnchor, SessionRole, SessionStore, SkillResultAnchor,
    ToolResult, ToolUse,
};
use coco_llm::{
    BackendError, BackendEvent, BackendEventPayload, BackendTurn, CompletionBackend,
    CompletionMessage, CompletionToolCall, Provider, ProviderRuntimeConfig, StepContext,
};
use coco_mem::{BranchConfigStore, ProviderProfile, ProviderProfileStore, SessionState};
use serde_json::{Value, json};
use tempfile::{TempDir, tempdir};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::{Mutex as AsyncMutex, Notify};

use crate::{
    Cli,
    app::{
        config::ProviderProfiles,
        daemon::{resume_incomplete_jobs, start_daemon_server},
        resolve_session_config, run_forwarded_with_services, run_with_backend,
        runtime::{ForwardedRuntimeInputs, RuntimeServices},
    },
    cli::{
        Command, DaemonSubcommand, PresetCommand, PresetSetCommand, PresetSubcommand,
        PromptBranchStatusCommand, PromptCommand, PromptRunCommand, PromptStatusCommand,
        PromptSubcommand, PromptWorkerCommand, SessionBranchCommand, SessionCloseCommand,
        SessionCommand, SessionCreateCommand, SessionFeedbackCommand, SessionForkCommand,
        SessionGraphCommand, SessionMergeCommand, SessionPrCommand, SessionRebaseCommand,
        SessionShowCommand, SessionSubcommand,
    },
    store::open_store,
};

type FakeResponseQueue =
    Arc<Mutex<HashMap<String, VecDeque<std::result::Result<BackendTurn, BackendError>>>>>;

fn submit_prompt_job<S>(store: &S, branch: &str, prompt: &str) -> coco_mem::Job
where
    S: BranchStore + JobStore + NodeStore,
{
    let parent = store.get_branch_head(branch).unwrap();
    let prompt_anchor_id = store
        .append(NewNode {
            parent,
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::prompt(
                vec![],
                PromptAnchor {
                    prompt: prompt.to_owned(),
                },
            )),
        })
        .unwrap();
    store.submit_job(branch, &prompt_anchor_id).unwrap()
}

#[derive(Debug, Clone)]
struct FakeBackend {
    responses: FakeResponseQueue,
}

impl FakeBackend {
    fn finished_turn(text: &str) -> BackendTurn {
        BackendTurn {
            message: CompletionMessage::assistant(text.to_owned()),
            events: vec![],
            tool_calls: vec![],
            final_text: Some(text.to_owned()),
            trace_persisted: false,
        }
    }

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
                                .map(|text| Self::finished_turn(text))
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
    async fn step(&self, ctx: StepContext<'_>) -> std::result::Result<BackendTurn, BackendError> {
        let mut responses = self.responses.lock().unwrap();
        let queue = responses
            .get_mut(&ctx.request.branch)
            .expect("missing fake backend queue");
        queue.pop_front().expect("missing fake backend response")
    }
}

#[derive(Debug, Clone, Default)]
struct UseSkillBackend {
    main_turn: Arc<Mutex<usize>>,
}

#[derive(Debug, Clone, Default)]
struct UseSkillFailureBackend {
    main_turn: Arc<Mutex<usize>>,
}

#[derive(Debug, Clone)]
struct BlockingBackend {
    started: Arc<Notify>,
    release: Arc<Notify>,
}

#[async_trait::async_trait]
impl CompletionBackend for BlockingBackend {
    async fn step(&self, _ctx: StepContext<'_>) -> std::result::Result<BackendTurn, BackendError> {
        self.started.notify_waiters();
        self.release.notified().await;
        Ok(FakeBackend::finished_turn("done"))
    }
}

#[async_trait::async_trait]
impl CompletionBackend for UseSkillBackend {
    async fn step(&self, ctx: StepContext<'_>) -> std::result::Result<BackendTurn, BackendError> {
        if ctx.request.branch == "main" {
            let mut main_turn = self.main_turn.lock().unwrap();
            let current_turn = *main_turn;
            *main_turn += 1;
            return match current_turn {
                0 => {
                    let tool_call: CompletionToolCall = serde_json::from_value(json!({
                        "id": "tool-call-1",
                        "call_id": "tool-call-1",
                        "function": {
                            "name": "use_skill",
                            "arguments": {
                                "name": "fast-rust"
                            }
                        },
                        "signature": null,
                        "additional_params": null
                    }))
                    .unwrap();
                    Ok(BackendTurn {
                        message: CompletionMessage::assistant("delegating".to_owned()),
                        events: vec![
                            BackendEvent::new(BackendEventPayload::ToolUse(ToolUse {
                                id: tool_call.id.clone(),
                                name: tool_call.function.name.clone(),
                                input: tool_call.function.arguments.clone(),
                            }))
                            .with_metadata(Some(ProviderMetadata::new(tool_call.call_id.clone()))),
                        ],
                        tool_calls: vec![tool_call],
                        final_text: None,
                        trace_persisted: false,
                    })
                }
                1 => Ok(FakeBackend::finished_turn("base done")),
                other => panic!("unexpected main turn {other}"),
            };
        }

        if ctx.request.branch.starts_with("main/skill/fast-rust-") {
            return Ok(FakeBackend::finished_turn("delegated output"));
        }

        panic!("unexpected branch {:?}", ctx.request.branch);
    }
}

#[async_trait::async_trait]
impl CompletionBackend for UseSkillFailureBackend {
    async fn step(&self, ctx: StepContext<'_>) -> std::result::Result<BackendTurn, BackendError> {
        if ctx.request.branch == "main" {
            let mut main_turn = self.main_turn.lock().unwrap();
            let current_turn = *main_turn;
            *main_turn += 1;
            return match current_turn {
                0 => {
                    let tool_call: CompletionToolCall = serde_json::from_value(json!({
                        "id": "tool-call-1",
                        "call_id": "tool-call-1",
                        "function": {
                            "name": "use_skill",
                            "arguments": {
                                "name": "fast-rust"
                            }
                        },
                        "signature": null,
                        "additional_params": null
                    }))
                    .unwrap();
                    Ok(BackendTurn {
                        message: CompletionMessage::assistant("delegating".to_owned()),
                        events: vec![
                            BackendEvent::new(BackendEventPayload::ToolUse(ToolUse {
                                id: tool_call.id.clone(),
                                name: tool_call.function.name.clone(),
                                input: tool_call.function.arguments.clone(),
                            }))
                            .with_metadata(Some(ProviderMetadata::new(tool_call.call_id.clone()))),
                        ],
                        tool_calls: vec![tool_call],
                        final_text: None,
                        trace_persisted: false,
                    })
                }
                1 => Ok(FakeBackend::finished_turn("base recovered")),
                other => panic!("unexpected main turn {other}"),
            };
        }

        if ctx.request.branch.starts_with("main/skill/fast-rust-") {
            return Err(BackendError::Failed {
                message: "delegated failure".to_owned(),
            });
        }

        panic!("unexpected branch {:?}", ctx.request.branch);
    }
}

fn prompt_cli(store_path: std::path::PathBuf, branch: Option<&str>, text: &[&str]) -> Cli {
    Cli {
        daemon_socket: None,
        store_path,
        command: Command::Prompt(PromptCommand {
            command: None,
            run: PromptRunCommand {
                branch: branch.unwrap_or("main").to_owned(),
                asynchronous: false,
                text: text.iter().map(|part| (*part).to_owned()).collect(),
            },
        }),
    }
}

fn prompt_worker_cli(store_path: std::path::PathBuf, job: &str) -> Cli {
    Cli {
        daemon_socket: None,
        store_path,
        command: Command::Prompt(PromptCommand {
            command: Some(PromptSubcommand::Worker(PromptWorkerCommand {
                job: job.to_owned(),
            })),
            run: PromptRunCommand {
                branch: "main".to_owned(),
                asynchronous: false,
                text: vec![],
            },
        }),
    }
}

fn prompt_status_cli(store_path: std::path::PathBuf, job: &str) -> Cli {
    Cli {
        daemon_socket: None,
        store_path,
        command: Command::Prompt(PromptCommand {
            command: Some(PromptSubcommand::Status(PromptStatusCommand {
                job: job.to_owned(),
            })),
            run: PromptRunCommand {
                branch: "main".to_owned(),
                asynchronous: false,
                text: vec![],
            },
        }),
    }
}

fn prompt_branch_status_cli(
    store_path: std::path::PathBuf,
    job: &str,
    branch: Option<&str>,
) -> Cli {
    Cli {
        daemon_socket: None,
        store_path,
        command: Command::Prompt(PromptCommand {
            command: Some(PromptSubcommand::BranchStatus(PromptBranchStatusCommand {
                job: job.to_owned(),
                branch: branch.map(str::to_owned),
            })),
            run: PromptRunCommand {
                branch: "main".to_owned(),
                asynchronous: false,
                text: vec![],
            },
        }),
    }
}

fn session_create_cli(store_path: std::path::PathBuf, branch: Option<&str>) -> Cli {
    Cli {
        daemon_socket: None,
        store_path,
        command: Command::Session(SessionCommand {
            command: SessionSubcommand::Create(SessionCreateCommand {
                branch: branch.unwrap_or("main").to_owned(),
                role: crate::cli::CliSessionRole::Orchestrator,
                provider_profile: None,
                system_prompt: "You are helpful.".to_owned(),
                prompt: "".to_owned(),
                temperature: Some(0.2),
                max_tokens: Some(64),
                additional_params: None,
                tools: vec![],
            }),
        }),
    }
}

fn session_list_cli(store_path: std::path::PathBuf) -> Cli {
    Cli {
        daemon_socket: None,
        store_path,
        command: Command::Session(SessionCommand {
            command: SessionSubcommand::List,
        }),
    }
}

fn session_fork_cli(store_path: std::path::PathBuf, branch: &str, from_ref: Option<&str>) -> Cli {
    Cli {
        daemon_socket: None,
        store_path,
        command: Command::Session(SessionCommand {
            command: SessionSubcommand::Fork(SessionForkCommand {
                branch: branch.to_owned(),
                from_ref: from_ref.unwrap_or("main").to_owned(),
            }),
        }),
    }
}

fn session_get_cli(store_path: std::path::PathBuf, branch: Option<&str>) -> Cli {
    Cli {
        daemon_socket: None,
        store_path,
        command: Command::Session(SessionCommand {
            command: SessionSubcommand::Get(SessionBranchCommand {
                branch: branch.unwrap_or("main").to_owned(),
            }),
        }),
    }
}

fn session_graph_cli(store_path: std::path::PathBuf) -> Cli {
    Cli {
        daemon_socket: None,
        store_path,
        command: Command::Session(SessionCommand {
            command: SessionSubcommand::Graph(SessionGraphCommand {}),
        }),
    }
}

fn session_show_cli(store_path: std::path::PathBuf, reference: &str, json: bool) -> Cli {
    Cli {
        daemon_socket: None,
        store_path,
        command: Command::Session(SessionCommand {
            command: SessionSubcommand::Show(SessionShowCommand {
                reference: reference.to_owned(),
                json,
            }),
        }),
    }
}

fn session_delete_cli(store_path: std::path::PathBuf, branch: Option<&str>) -> Cli {
    Cli {
        daemon_socket: None,
        store_path,
        command: Command::Session(SessionCommand {
            command: SessionSubcommand::Delete(SessionBranchCommand {
                branch: branch.unwrap_or("main").to_owned(),
            }),
        }),
    }
}

fn session_rebase_cli(store_path: std::path::PathBuf, branch: Option<&str>) -> Cli {
    Cli {
        daemon_socket: None,
        store_path,
        command: Command::Session(SessionCommand {
            command: SessionSubcommand::Rebase(SessionRebaseCommand {
                branch: branch.unwrap_or("main").to_owned(),
                preset: None,
                provider_profile: None,
                role: Some(crate::cli::CliSessionRole::Runner),
                provider: Some(crate::cli::CliProvider::Anthropic),
                model: Some("claude-sonnet-4-20250514".to_owned()),
                system_prompt: Some("You are precise.".to_owned()),
                prompt: Some("Start with a plan.".to_owned()),
                temperature: None,
                clear_temperature: true,
                max_tokens: Some(256),
                clear_max_tokens: false,
                tools: vec![],
                clear_tools: false,
            }),
        }),
    }
}

fn session_reopen_cli(store_path: std::path::PathBuf, branch: Option<&str>) -> Cli {
    Cli {
        daemon_socket: None,
        store_path,
        command: Command::Session(SessionCommand {
            command: SessionSubcommand::Reopen(SessionBranchCommand {
                branch: branch.unwrap_or("main").to_owned(),
            }),
        }),
    }
}

fn session_pr_cli(
    store_path: std::path::PathBuf,
    branch: Option<&str>,
    target_branch: &str,
) -> Cli {
    Cli {
        daemon_socket: None,
        store_path,
        command: Command::Session(SessionCommand {
            command: SessionSubcommand::Pr(SessionPrCommand {
                branch: branch.unwrap_or("main").to_owned(),
                target_branch: target_branch.to_owned(),
            }),
        }),
    }
}

fn session_close_cli(
    store_path: std::path::PathBuf,
    branch: Option<&str>,
    target_branch: &str,
) -> Cli {
    Cli {
        daemon_socket: None,
        store_path,
        command: Command::Session(SessionCommand {
            command: SessionSubcommand::Close(SessionCloseCommand {
                branch: branch.unwrap_or("main").to_owned(),
                target_branch: target_branch.to_owned(),
            }),
        }),
    }
}

fn session_merge_cli(
    store_path: std::path::PathBuf,
    branch: Option<&str>,
    target_branch: Option<&str>,
    prompt: &str,
) -> Cli {
    Cli {
        daemon_socket: None,
        store_path,
        command: Command::Session(SessionCommand {
            command: SessionSubcommand::Merge(SessionMergeCommand {
                branch: branch.unwrap_or("main").to_owned(),
                target_branch: target_branch.map(str::to_owned),
                prompt: prompt.to_owned(),
            }),
        }),
    }
}

fn session_feedback_cli(
    store_path: std::path::PathBuf,
    branch: Option<&str>,
    prompt: &str,
    from_ref: Option<&str>,
) -> Cli {
    Cli {
        daemon_socket: None,
        store_path,
        command: Command::Session(SessionCommand {
            command: SessionSubcommand::Feedback(SessionFeedbackCommand {
                branch: branch.unwrap_or("main").to_owned(),
                prompt: prompt.to_owned(),
                from_ref: from_ref.map(str::to_owned),
            }),
        }),
    }
}

fn temp_store_path() -> (TempDir, std::path::PathBuf) {
    let tempdir = tempdir().unwrap();
    let store_path = tempdir.path().join("store");
    (tempdir, store_path)
}

fn provider_profile(
    name: &str,
    provider: &str,
    api_key_secret: &str,
    base_url: Option<&str>,
    default_model: Option<&str>,
) -> (String, ProviderProfile) {
    let mut secrets = BTreeMap::new();
    secrets.insert("api_key".to_owned(), api_key_secret.to_owned());
    (
        name.to_owned(),
        ProviderProfile {
            provider: provider.to_owned(),
            secrets,
            base_url: base_url.map(str::to_owned),
            default_model: default_model.map(str::to_owned),
        },
    )
}

fn test_provider_profiles() -> ProviderProfiles {
    ProviderProfiles::from_profiles(HashMap::from([provider_profile(
        "openai-codex",
        "chatgpt",
        "${CHATGPT_ACCESS_TOKEN}",
        None,
        Some("gpt-5.4"),
    )]))
}

fn shared_test_provider_profiles() -> &'static ProviderProfiles {
    static PROVIDER_PROFILES: OnceLock<ProviderProfiles> = OnceLock::new();
    PROVIDER_PROFILES.get_or_init(test_provider_profiles)
}

fn llm_with_test_provider_config<B, S>(store: S, backend: B) -> Arc<coco_llm::LlmService<B, S>> {
    let mut provider_configs = HashMap::new();
    provider_configs.insert(
        "openai-codex".to_owned(),
        ProviderRuntimeConfig {
            provider: Provider::ChatGpt,
            secrets: BTreeMap::new(),
            base_url: None,
            default_model: Some("gpt-5.4".to_owned()),
        },
    );
    Arc::new(
        coco_llm::LlmService::builder(store, backend)
            .with_provider_configs(provider_configs)
            .build(),
    )
}

async fn run_with_backend_and_provider_profiles<B, R>(
    cli: Cli,
    reader: &mut R,
    backend: B,
    provider_profiles: ProviderProfiles,
) -> crate::Result<Option<String>>
where
    B: CompletionBackend + 'static,
    R: std::io::Read,
{
    let store = open_store(&cli.store_path)?;
    let provider_configs = HashMap::from_iter(
        provider_profiles
            .list_provider_profiles()
            .unwrap()
            .into_iter()
            .map(|(name, profile)| {
                (
                    name,
                    ProviderRuntimeConfig {
                        provider: Provider::parse(&profile.provider).unwrap(),
                        secrets: BTreeMap::new(),
                        base_url: profile.base_url,
                        default_model: profile.default_model,
                    },
                )
            }),
    );
    let llm = Arc::new(
        coco_llm::LlmService::builder(store.clone(), backend)
            .with_provider_configs(provider_configs)
            .build(),
    );

    crate::app::runtime::run_with_services(
        cli,
        reader,
        RuntimeServices {
            shared_store: &store,
            llm: &llm,
            provider_profiles: &provider_profiles,
            shared_engine: None,
        },
        false,
    )
    .await
}

fn append_prompt_anchor(
    store: &impl NodeStore,
    parent: &str,
    prompt: &str,
    merge_parents: &[&str],
) -> String {
    store
        .append(NewNode {
            parent: parent.to_owned(),
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::prompt(
                merge_parents.iter().map(|id| (*id).to_owned()).collect(),
                PromptAnchor {
                    prompt: prompt.to_owned(),
                },
            )),
        })
        .unwrap()
}

fn append_session_anchor(store: &impl NodeStore, parent: &str, prompt: &str) -> String {
    store
        .append(NewNode {
            parent: parent.to_owned(),
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::session(
                vec![],
                SessionAnchor {
                    role: SessionRole::Runner,
                    provider_profile: None,
                    provider: Some("openai".to_owned()),
                    model: "gpt-4.1-mini".to_owned(),
                    tools: vec![],
                    system_prompt: "You are helpful.".to_owned(),
                    prompt: prompt.to_owned(),
                    temperature: Some(0.2),
                    max_tokens: Some(64),
                    additional_params: None,
                    enable_coco_shim: true,
                },
            )),
        })
        .unwrap()
}

fn append_tool_use_node(store: &impl NodeStore, parent: &str, id: &str, name: &str) -> String {
    store
        .append(NewNode {
            parent: parent.to_owned(),
            role: Role::LLM,
            metadata: BackendMetadata::builder()
                .provider(&ProviderMetadata::new(Some(id.to_owned())))
                .build(),
            kind: Kind::ToolUse(ToolUse {
                id: id.to_owned(),
                name: name.to_owned(),
                input: json!({
                    "cmd": "echo hello",
                }),
            }),
        })
        .unwrap()
}

fn append_tool_result_node(store: &impl NodeStore, parent: &str, id: &str, output: &str) -> String {
    store
        .append(NewNode {
            parent: parent.to_owned(),
            role: Role::System,
            metadata: BackendMetadata::builder()
                .provider(&ProviderMetadata::new(Some(id.to_owned())))
                .build(),
            kind: Kind::ToolResult(ToolResult {
                id: id.to_owned(),
                output: output.to_owned(),
            }),
        })
        .unwrap()
}

fn append_text_node(store: &impl NodeStore, parent: &str, text: &str) -> String {
    store
        .append(NewNode {
            parent: parent.to_owned(),
            role: Role::LLM,
            metadata: None,
            kind: Kind::Text(text.to_owned()),
        })
        .unwrap()
}

fn append_failure_node(store: &impl NodeStore, parent: &str, message: &str) -> String {
    store
        .append(NewNode {
            parent: parent.to_owned(),
            role: Role::System,
            metadata: None,
            kind: Kind::Failure(message.to_owned()),
        })
        .unwrap()
}

fn append_skill_result_anchor(
    store: &impl NodeStore,
    parent: &str,
    merge_parent: &str,
    tool_id: &str,
    skill_name: &str,
    output: &str,
) -> String {
    store
        .append(NewNode {
            parent: parent.to_owned(),
            role: Role::System,
            metadata: BackendMetadata::builder()
                .provider(&ProviderMetadata::new(Some(tool_id.to_owned())))
                .build(),
            kind: Kind::Anchor(Anchor::skill_result(
                vec![merge_parent.to_owned()],
                SkillResultAnchor {
                    tool_id: tool_id.to_owned(),
                    skill_name: skill_name.to_owned(),
                    output: output.to_owned(),
                },
            )),
        })
        .unwrap()
}

async fn with_coco_env_async<T, F, Fut>(entries: &[(&str, &str)], run: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    let entries = entries
        .iter()
        .map(|(name, value)| (*name, Some(*value)))
        .collect::<Vec<_>>();
    with_coco_env_state_async(&entries, run).await
}

async fn without_coco_env_async<T, F, Fut>(names: &[&str], run: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    let entries = names.iter().map(|name| (*name, None)).collect::<Vec<_>>();
    with_coco_env_state_async(&entries, run).await
}

async fn with_coco_env_state_async<T, F, Fut>(entries: &[(&str, Option<&str>)], run: F) -> T
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
        match value {
            Some(value) => unsafe {
                std::env::set_var(name, value);
            },
            None => unsafe {
                std::env::remove_var(name);
            },
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
async fn prompt_wires_skill_executor_for_use_skill() {
    let (tempdir, store_path) = temp_store_path();
    let skills_root = tempdir.path().join("skills");
    let skill_dir = skills_root.join("fast-rust");
    std::fs::create_dir_all(&skill_dir).unwrap();
    std::fs::write(
        skill_dir.join("SKILL.md"),
        r#"---
name: "fast-rust"
description: "Review Rust changes."
session_role: "runner"
enable_coco_shim: true
---

# Fast Rust
"#,
    )
    .unwrap();
    let skills_env = std::env::join_paths([skills_root.as_path()]).unwrap();

    with_coco_env_async(
        &[
            ("COCO_PROVIDER", "openai"),
            ("COCO_MODEL", "gpt-4.1-mini"),
            ("COCO_TOOLS", "use_skill"),
            ("COCO_SKILLS_DIRS", skills_env.to_str().unwrap()),
        ],
        || async {
            run_with_backend(
                session_create_cli(store_path.clone(), Some("main")),
                &mut Cursor::new(""),
                UseSkillBackend::default(),
            )
            .await
            .unwrap();
        },
    )
    .await;

    let output = with_coco_env_async(
        &[
            ("COCO_PROVIDER", "openai"),
            ("COCO_MODEL", "gpt-4.1-mini"),
            ("COCO_TOOLS", "use_skill"),
            ("COCO_SKILLS_DIRS", skills_env.to_str().unwrap()),
        ],
        || async {
            run_with_backend(
                prompt_cli(store_path.clone(), Some("main"), &["delegate"]),
                &mut Cursor::new(""),
                UseSkillBackend::default(),
            )
            .await
        },
    )
    .await
    .unwrap();

    assert_eq!(output, Some("base done".to_owned()));

    let store = open_store(&store_path).unwrap();
    let jobs = store.list_jobs().unwrap();
    assert_eq!(jobs.len(), 1);
    assert!(jobs.values().all(|job| job.branch == "main"));

    let session_states = store.list_session_states().unwrap();
    assert!(
        !session_states
            .keys()
            .any(|branch| branch.starts_with("main/skill/fast-rust-"))
    );

    let ancestry = store.ancestry("main").unwrap();
    let tool_use = ancestry
        .iter()
        .find(|node| {
            matches!(
                &node.kind,
                Kind::ToolUse(ToolUse { id, name, .. })
                    if id == "tool-call-1" && name == "use_skill"
            )
        })
        .expect("expected use_skill tool use on main");
    let skill_result = ancestry
        .iter()
        .find_map(|node| match &node.kind {
            Kind::Anchor(anchor) => anchor
                .as_skill_result()
                .map(|result| (node, anchor, result)),
            _ => None,
        })
        .expect("expected use_skill skill result on main");
    assert_eq!(skill_result.2.tool_id, "tool-call-1");
    assert_eq!(skill_result.2.skill_name, "fast-rust");
    let value: serde_json::Value = serde_json::from_str(&skill_result.2.output).unwrap();
    assert_eq!(value["text"], "delegated output");
    assert_eq!(skill_result.0.parent, tool_use.id);
    assert_eq!(skill_result.1.merge_parents().len(), 1);
    let child_response = store
        .get_node(&skill_result.1.merge_parents()[0])
        .expect("skill result merge parent should exist");
    assert!(matches!(
        &child_response.kind,
        Kind::Text(text) if text == "delegated output"
    ));

    let children = store.list_children(&tool_use.id).unwrap();
    let child_session_anchor = children
        .iter()
        .find_map(|node| match &node.kind {
            Kind::Anchor(anchor) => anchor.as_session().map(|session| (node, session)),
            _ => None,
        })
        .expect("expected child session anchor under use_skill");
    assert_eq!(child_session_anchor.0.parent, tool_use.id);
    assert_eq!(child_session_anchor.1.role, SessionRole::Runner);
    assert!(child_session_anchor.1.enable_coco_shim);
    assert!(
        child_session_anchor
            .1
            .prompt
            .contains("You are executing the skill `fast-rust`")
    );
    assert!(
        !child_session_anchor
            .1
            .prompt
            .contains("Additional task from caller:")
    );
    let child_session_children = store.list_children(&child_session_anchor.0.id).unwrap();
    assert!(!child_session_children.iter().any(|node| matches!(
        &node.kind,
        Kind::Anchor(anchor) if anchor.as_prompt().is_some()
    )));
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
async fn prompt_keeps_failed_use_skill_child_visible_under_tool_use() {
    let (_tempdir, store_path) = temp_store_path();
    let skills_root = tempdir().unwrap();
    std::fs::create_dir_all(skills_root.path().join("fast-rust")).unwrap();
    std::fs::write(
        skills_root.path().join("fast-rust").join("SKILL.md"),
        r#"---
name: "fast-rust"
description: "Review Rust changes."
session_role: "runner"
enable_coco_shim: true
---

# Fast Rust
"#,
    )
    .unwrap();
    let skills_env = std::env::join_paths([skills_root.path()]).unwrap();

    with_coco_env_async(
        &[
            ("COCO_PROVIDER", "openai"),
            ("COCO_MODEL", "gpt-4.1-mini"),
            ("COCO_TOOLS", "use_skill"),
            ("COCO_SKILLS_DIRS", skills_env.to_str().unwrap()),
        ],
        || async {
            run_with_backend(
                session_create_cli(store_path.clone(), Some("main")),
                &mut Cursor::new(""),
                UseSkillFailureBackend::default(),
            )
            .await
            .unwrap();
        },
    )
    .await;

    let output = with_coco_env_async(
        &[
            ("COCO_PROVIDER", "openai"),
            ("COCO_MODEL", "gpt-4.1-mini"),
            ("COCO_TOOLS", "use_skill"),
            ("COCO_SKILLS_DIRS", skills_env.to_str().unwrap()),
        ],
        || async {
            run_with_backend(
                prompt_cli(store_path.clone(), Some("main"), &["delegate"]),
                &mut Cursor::new(""),
                UseSkillFailureBackend::default(),
            )
            .await
        },
    )
    .await
    .unwrap();

    assert_eq!(output, Some("base recovered".to_owned()));

    let store = open_store(&store_path).unwrap();
    let ancestry = store.ancestry("main").unwrap();
    let tool_use = ancestry
        .iter()
        .find(|node| {
            matches!(
                &node.kind,
                Kind::ToolUse(ToolUse { id, name, .. })
                    if id == "tool-call-1" && name == "use_skill"
            )
        })
        .expect("expected use_skill tool use on main");
    let children = store.list_children(&tool_use.id).unwrap();
    let child_session_anchor = children
        .iter()
        .find_map(|node| match &node.kind {
            Kind::Anchor(anchor) => anchor.as_session().map(|session| (node, session)),
            _ => None,
        })
        .expect("expected child session anchor under tool use");
    let tool_result = children
        .iter()
        .find_map(|node| match &node.kind {
            Kind::ToolResult(result) if result.id == "tool-call-1" => Some(result),
            _ => None,
        })
        .expect("expected use_skill failure as parent tool result");

    assert_eq!(tool_result.output, "Backend call failed: delegated failure");
    assert!(!children.iter().any(|node| matches!(
        &node.kind,
        Kind::Anchor(anchor) if anchor.as_skill_result().is_some()
    )));
    assert_eq!(child_session_anchor.0.parent, tool_use.id);
    assert_eq!(child_session_anchor.1.role, SessionRole::Runner);
    assert!(child_session_anchor.1.enable_coco_shim);
    assert!(
        child_session_anchor
            .1
            .prompt
            .contains("You are executing the skill `fast-rust`")
    );

    let child_session_children = store.list_children(&child_session_anchor.0.id).unwrap();
    assert!(child_session_children.iter().any(|node| matches!(
        &node.kind,
        Kind::Failure(text) if text == "delegated failure"
    )));
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
async fn prompt_persists_single_job_even_without_async() {
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

    let output = run_with_backend(
        prompt_cli(store_path.clone(), Some("main"), &["hello"]),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[("main", &[Ok("done")])]),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(output, "done");

    let jobs = open_store(&store_path).unwrap().list_jobs().unwrap();
    assert_eq!(jobs.len(), 1);
    let job_id = jobs.keys().next().unwrap().clone();
    let status_output = run_with_backend(
        prompt_status_cli(store_path, &job_id),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();
    let value: Value = serde_json::from_str(&status_output).unwrap();
    assert_eq!(value["job"]["status"], "finished");
    assert!(value["job"]["finished_at"].is_string());
    assert_eq!(value["base_node"]["kind"], "prompt");
    assert_eq!(value["base_node"]["prompt"], "hello");
    assert_eq!(value["base_node"]["merge_parents"], json!([]));
    assert!(value["base_node"]["node_id"].is_string());
    assert!(value["job"]["head"].is_string());
}

#[tokio::test]
async fn prompt_worker_persists_job_results_and_status_queries() {
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
    let job = submit_prompt_job(&store, "main", "hello");

    run_with_backend(
        prompt_worker_cli(store_path.clone(), &job.job_id),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[("main", &[Ok("main done")])]),
    )
    .await
    .unwrap();

    let output = run_with_backend(
        prompt_status_cli(store_path.clone(), &job.job_id),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();
    let value: Value = serde_json::from_str(&output).unwrap();
    assert_eq!(value["job"]["status"], "finished");
    assert!(value["job"]["finished_at"].is_string());
    assert_eq!(value["job"]["branch"], "main");
    assert_eq!(value["base_node"]["kind"], "prompt");
    assert_eq!(value["base_node"]["prompt"], "hello");
    assert_eq!(value["base_node"]["merge_parents"], json!([]));
    assert!(value["base_node"]["node_id"].is_string());
    assert!(value["job"]["head"].is_string());

    let branch_output = run_with_backend(
        prompt_branch_status_cli(store_path, &job.job_id, Some("main")),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();
    let branch_value: Value = serde_json::from_str(&branch_output).unwrap();
    assert_eq!(branch_value["branch"], "main");
    assert!(branch_value["head"].is_string());
}

#[tokio::test]
async fn prompt_branch_status_reports_running_task_progress() {
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
    let job = submit_prompt_job(&store, "main", "hello progress");

    let backend = BlockingBackend {
        started: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
    };
    let started = backend.started.clone();
    let release = backend.release.clone();
    let job_id = job.job_id.clone();
    let worker = tokio::spawn({
        let store_path = store_path.clone();
        let backend = backend.clone();
        let job_id = job_id.clone();
        async move {
            run_with_backend(
                prompt_worker_cli(store_path, &job_id),
                &mut Cursor::new(""),
                backend,
            )
            .await
            .unwrap();
        }
    });

    started.notified().await;

    let output = run_with_backend(
        prompt_branch_status_cli(store_path.clone(), &job_id, Some("main")),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();
    let value: Value = serde_json::from_str(&output).unwrap();
    assert_eq!(value["status"], "running");
    assert!(value["finished_at"].is_null());
    assert_eq!(value["head"], json!(job.base));

    release.notify_waiters();
    worker.await.unwrap();

    let status_output = run_with_backend(
        prompt_status_cli(store_path, &job_id),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();
    let value: Value = serde_json::from_str(&status_output).unwrap();
    assert_eq!(value["job"]["status"], "finished");
    assert!(value["job"]["finished_at"].is_string());
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

#[tokio::test]
async fn session_fork_creates_active_branch_from_reference() {
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

    let source_head_id = open_store(&store_path)
        .unwrap()
        .get_branch_head("main")
        .unwrap();

    let output = run_with_backend(
        session_fork_cli(store_path.clone(), "draft", Some("main")),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(
        serde_json::from_str::<Value>(&output).unwrap(),
        json!({
            "branch": "draft",
            "head_id": source_head_id,
            "role": "orchestrator",
            "state": "Active",
        })
    );

    let store = open_store(&store_path).unwrap();
    assert_eq!(store.get_branch_head("draft").unwrap(), source_head_id);
    assert_eq!(
        store.get_session_state("draft").unwrap(),
        SessionState::Active
    );
}

#[tokio::test]
async fn session_list_returns_sorted_branches_with_states() {
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

    let store = open_store(&store_path).unwrap();
    let base_head_id = store.get_branch_head("main").unwrap();

    run_with_backend(
        session_pr_cli(store_path.clone(), Some("draft"), "main"),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap();

    let output = run_with_backend(
        session_list_cli(store_path),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();

    let value: Value = serde_json::from_str(&output).unwrap();
    assert_eq!(
        value,
        json!([
            {
                "branch": "draft",
                "head_id": store.get_branch_head("draft").unwrap(),
                "role": "orchestrator",
                "state": {
                    "Attached": {
                        "target_branch": "main",
                        "base_head_id": base_head_id,
                    }
                }
            },
            {
                "branch": "main",
                "head_id": store.get_branch_head("main").unwrap(),
                "role": "orchestrator",
                "state": "Active"
            }
        ])
    );
}

#[tokio::test]
async fn session_get_returns_state_and_visible_anchor() {
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

    let output = run_with_backend(
        session_get_cli(store_path.clone(), Some("main")),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();

    let value: Value = serde_json::from_str(&output).unwrap();
    assert_eq!(value["branch"], "main");
    assert_eq!(value["role"], "orchestrator");
    assert_eq!(value["state"], json!("Active"));
    assert_eq!(value["anchor"]["role"], "orchestrator");
    assert_eq!(value["anchor"]["provider_profile"], "openai-codex");
    assert_eq!(value["anchor"]["provider"], Value::Null);
    assert_eq!(value["anchor"]["model"], "gpt-5.4");
    assert_eq!(value["anchor"]["system_prompt"], "You are helpful.");
    assert_eq!(value["anchor"]["prompt"], "");
    assert_eq!(value["anchor"]["temperature"], json!(0.2));
    assert_eq!(value["anchor"]["max_tokens"], json!(64));
    assert_eq!(value["anchor"]["tools"], json!([]));

    let store = open_store(&store_path).unwrap();
    assert_eq!(
        value["head_id"],
        json!(store.get_branch_head("main").unwrap())
    );
}

#[tokio::test]
async fn session_create_persists_additional_params() {
    let (_tempdir, store_path) = temp_store_path();
    with_coco_env_async(
        &[("COCO_PROVIDER", "openai"), ("COCO_MODEL", "gpt-4.1-mini")],
        || async {
            let cli = Cli {
                daemon_socket: None,
                store_path: store_path.clone(),
                command: Command::Session(SessionCommand {
                    command: SessionSubcommand::Create(SessionCreateCommand {
                        branch: "main".to_owned(),
                        role: crate::cli::CliSessionRole::Orchestrator,
                        provider_profile: None,
                        system_prompt: "You are helpful.".to_owned(),
                        prompt: "".to_owned(),
                        temperature: Some(0.2),
                        max_tokens: Some(64),
                        additional_params: Some(
                            "{\"service_tier\":\"priority\",\"reasoning_effort\":\"medium\"}"
                                .to_owned(),
                        ),
                        tools: vec![],
                    }),
                }),
            };

            run_with_backend(cli, &mut Cursor::new(""), FakeBackend::with_responses(&[]))
                .await
                .unwrap();
        },
    )
    .await;

    let output = run_with_backend(
        session_get_cli(store_path, Some("main")),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();

    let value: Value = serde_json::from_str(&output).unwrap();
    assert_eq!(
        value["anchor"]["additional_params"],
        json!({
            "service_tier": "priority",
            "reasoning_effort": "medium",
        })
    );
}

#[tokio::test]
async fn session_create_uses_single_provider_profile_when_env_is_absent() {
    let (_tempdir, store_path) = temp_store_path();

    without_coco_env_async(&["COCO_PROVIDER", "COCO_MODEL"], || async {
        run_with_backend(
            session_create_cli(store_path.clone(), Some("main")),
            &mut Cursor::new(""),
            FakeBackend::with_responses(&[]),
        )
        .await
        .unwrap();
    })
    .await;

    let output = run_with_backend(
        session_get_cli(store_path, Some("main")),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();
    let value: Value = serde_json::from_str(&output).unwrap();
    assert_eq!(value["anchor"]["provider_profile"], "openai-codex");
    assert_eq!(value["anchor"]["provider"], Value::Null);
    assert_eq!(value["anchor"]["model"], "gpt-5.4");
}

#[tokio::test]
async fn session_graph_reports_empty_store() {
    let (_tempdir, store_path) = temp_store_path();

    let output = run_with_backend(
        session_graph_cli(store_path),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(output, "No sessions found.");
}

#[tokio::test]
async fn session_graph_shows_branch_labels_on_head_node() {
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

    run_with_backend(
        prompt_cli(store_path.clone(), Some("main"), &["hello", "world"]),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[("main", &[Ok("assistant reply")])]),
    )
    .await
    .unwrap();

    run_with_backend(
        session_fork_cli(store_path.clone(), "draft", Some("main")),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap();

    let output = run_with_backend(
        session_graph_cli(store_path),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();

    assert!(output.contains("[draft, main] assistant reply"));
    assert!(output.contains("* "));
}

#[tokio::test]
async fn session_delete_removes_branch_and_session_state() {
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
            run_with_backend(
                session_fork_cli(store_path.clone(), "draft", Some("main")),
                &mut Cursor::new(""),
                FakeBackend::with_responses(&[]),
            )
            .await
            .unwrap();
        },
    )
    .await;

    let output = run_with_backend(
        session_delete_cli(store_path.clone(), Some("draft")),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(
        serde_json::from_str::<Value>(&output).unwrap(),
        json!({"branch": "draft"})
    );

    let store = open_store(&store_path).unwrap();
    let err = store.get_branch_head("draft").unwrap_err();
    assert!(matches!(err, coco_mem::StoreError::BranchNotFound { name } if name == "draft"));
    let err = store.get_session_state("draft").unwrap_err();
    assert!(matches!(err, coco_mem::StoreError::BranchNotFound { name } if name == "draft"));
}

#[tokio::test]
async fn session_rebase_updates_visible_session_config() {
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

    let original_head = open_store(&store_path)
        .unwrap()
        .get_branch_head("main")
        .unwrap();

    let rebase_output = run_with_backend(
        session_rebase_cli(store_path.clone(), Some("main")),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();

    let rebase_value: Value = serde_json::from_str(&rebase_output).unwrap();
    assert_eq!(rebase_value["branch"], "main");
    assert_ne!(rebase_value["head_id"], json!(original_head));

    let get_output = run_with_backend(
        session_get_cli(store_path, Some("main")),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();

    let value: Value = serde_json::from_str(&get_output).unwrap();
    assert_eq!(value["role"], "runner");
    assert_eq!(value["anchor"]["role"], "runner");
    assert_eq!(value["anchor"]["provider"], "anthropic");
    assert_eq!(value["anchor"]["model"], "claude-sonnet-4-20250514");
    assert_eq!(value["anchor"]["system_prompt"], "You are precise.");
    assert_eq!(value["anchor"]["prompt"], "Start with a plan.");
    assert_eq!(value["anchor"]["temperature"], Value::Null);
    assert_eq!(value["anchor"]["max_tokens"], json!(256));
    assert_eq!(value["anchor"]["tools"], json!([]));
}

#[tokio::test]
async fn session_pr_close_and_reopen_commands_update_persisted_state() {
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
    let base_head_id = store.get_branch_head("main").unwrap();

    let pr_output = run_with_backend(
        session_pr_cli(store_path.clone(), Some("main"), "main"),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&pr_output).unwrap(),
        json!({
            "branch": "main",
            "target_branch": "main",
            "base_head_id": base_head_id,
            "state": {
                "Attached": {
                    "target_branch": "main",
                    "base_head_id": store.get_branch_head("main").unwrap(),
                }
            }
        })
    );

    let close_output = run_with_backend(
        session_close_cli(store_path.clone(), Some("main"), "main"),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&close_output).unwrap(),
        json!({
            "branch": "main",
            "state": {
                "Paused": {
                    "target_branch": "main",
                    "reason": "Closed",
                }
            }
        })
    );

    run_with_backend(
        session_reopen_cli(store_path.clone(), Some("main")),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap();

    assert_eq!(
        open_store(&store_path)
            .unwrap()
            .get_session_state("main")
            .unwrap(),
        SessionState::Active
    );
}

#[tokio::test]
async fn session_merge_and_feedback_commands_create_handoff_anchors() {
    let (_tempdir, store_path) = temp_store_path();
    with_coco_env_async(
        &[("COCO_PROVIDER", "openai"), ("COCO_MODEL", "gpt-4.1-mini")],
        || async {
            run_with_backend(
                session_create_cli(store_path.clone(), Some("base")),
                &mut Cursor::new(""),
                FakeBackend::with_responses(&[]),
            )
            .await
            .unwrap();
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

    run_with_backend(
        session_pr_cli(store_path.clone(), Some("main"), "base"),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap();

    let store = open_store(&store_path).unwrap();
    let source_head_id = store.get_branch_head("main").unwrap();
    let base_head_id = store.get_branch_head("base").unwrap();

    let merge_output = run_with_backend(
        session_merge_cli(store_path.clone(), Some("main"), None, "handoff to base"),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();
    let merge_value = serde_json::from_str::<Value>(&merge_output).unwrap();
    let merged_anchor_id = merge_value["merged_anchor_id"].as_str().unwrap().to_owned();
    assert_eq!(merge_value["branch"], "main");
    assert_eq!(merge_value["target_branch"], "base");
    assert_eq!(merge_value["source_head_id"], json!(source_head_id));
    assert_eq!(
        merge_value["state"],
        json!({
            "Paused": {
                "target_branch": "base",
                "reason": {
                    "Merged": {
                        "merged_anchor_id": merged_anchor_id,
                    }
                }
            }
        })
    );

    let store = open_store(&store_path).unwrap();
    let merged_anchor = store.get_node(&merged_anchor_id).unwrap();
    let Kind::Anchor(anchor) = merged_anchor.kind else {
        panic!("expected merged prompt anchor");
    };
    assert_eq!(merged_anchor.parent, base_head_id);
    assert_eq!(anchor.merge_parents(), [source_head_id.as_str()]);
    assert_eq!(
        anchor.as_prompt().expect("expected prompt anchor").prompt,
        "handoff to base"
    );

    let merged_feedback_source_id =
        append_prompt_anchor(&store, &merged_anchor_id, "review note", &[]);
    store
        .set_branch_head("base", &merged_anchor_id, &merged_feedback_source_id)
        .unwrap();
    store
        .set_session_state(
            "main",
            None,
            SessionState::Attached {
                target_branch: "base".to_owned(),
                base_head_id: merged_anchor_id.clone(),
            },
        )
        .unwrap();

    let main_head_before_feedback = store.get_branch_head("main").unwrap();
    let feedback_output = run_with_backend(
        session_feedback_cli(
            store_path.clone(),
            Some("main"),
            "address review note",
            Some("base"),
        ),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();
    let feedback_value = serde_json::from_str::<Value>(&feedback_output).unwrap();
    let feedback_anchor_id = feedback_value["feedback_anchor_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(feedback_value["target_branch"], "base");
    assert_eq!(
        feedback_value["base_head_id"],
        json!(merged_feedback_source_id)
    );
    assert_eq!(
        feedback_value["source_anchor_id"],
        json!(merged_feedback_source_id)
    );
    assert_eq!(
        feedback_value["state"],
        json!({
            "Attached": {
                "target_branch": "base",
                "base_head_id": merged_feedback_source_id,
            }
        })
    );

    let store = open_store(&store_path).unwrap();
    let feedback_anchor = store.get_node(&feedback_anchor_id).unwrap();
    let Kind::Anchor(anchor) = feedback_anchor.kind else {
        panic!("expected feedback prompt anchor");
    };
    assert_eq!(feedback_anchor.parent, main_head_before_feedback);
    assert_eq!(anchor.merge_parents(), [merged_feedback_source_id.as_str()]);
    assert_eq!(
        anchor.as_prompt().expect("expected prompt anchor").prompt,
        "address review note"
    );
}

#[tokio::test]
async fn session_graph_renders_global_dag_with_non_anchor_merge_parent() {
    let (_tempdir, store_path) = temp_store_path();
    with_coco_env_async(
        &[("COCO_PROVIDER", "openai"), ("COCO_MODEL", "gpt-4.1-mini")],
        || async {
            run_with_backend(
                session_create_cli(store_path.clone(), Some("base")),
                &mut Cursor::new(""),
                FakeBackend::with_responses(&[]),
            )
            .await
            .unwrap();
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

    run_with_backend(
        prompt_cli(store_path.clone(), Some("main"), &["hello", "world"]),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[("main", &[Ok("assistant reply")])]),
    )
    .await
    .unwrap();

    run_with_backend(
        session_pr_cli(store_path.clone(), Some("main"), "base"),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap();
    run_with_backend(
        session_merge_cli(store_path.clone(), Some("main"), None, "handoff to base"),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap();

    let output = run_with_backend(
        session_graph_cli(store_path),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();

    assert!(output.contains("handoff to base merge=["));
    assert!(output.contains("base] handoff to base"));
    assert!(output.contains("main@Paused(base,merged)] assistant reply"));
    assert!(output.contains('\\') || output.contains('/'));
}

#[tokio::test]
async fn session_graph_keeps_merge_parent_visible_after_source_branch_delete() {
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
            run_with_backend(
                session_fork_cli(store_path.clone(), "draft", Some("main")),
                &mut Cursor::new(""),
                FakeBackend::with_responses(&[]),
            )
            .await
            .unwrap();
        },
    )
    .await;

    let store = open_store(&store_path).unwrap();
    let draft_head = append_prompt_anchor(
        &store,
        &store.get_branch_head("draft").unwrap(),
        "draft merge parent",
        &[],
    );
    store
        .set_branch_head(
            "draft",
            &store.get_branch_head("draft").unwrap(),
            &draft_head,
        )
        .unwrap();

    run_with_backend(
        session_delete_cli(store_path.clone(), Some("draft")),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap();

    let store = open_store(&store_path).unwrap();
    let main_head = store.get_branch_head("main").unwrap();
    let merged_head =
        append_prompt_anchor(&store, &main_head, "merge after delete", &[&draft_head]);
    store
        .set_branch_head("main", &main_head, &merged_head)
        .unwrap();

    let output = run_with_backend(
        session_graph_cli(store_path),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();

    assert!(output.contains("merge after delete merge=["));
    assert!(output.contains("draft merge parent"));
    assert!(output.contains('\\') || output.contains('/'));
}

#[tokio::test]
async fn session_graph_shows_tool_and_failure_nodes() {
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
    let session_head = store.get_branch_head("main").unwrap();
    let tool_use_id = append_tool_use_node(&store, &session_head, "tool-1", "bash");
    store
        .set_branch_head("main", &session_head, &tool_use_id)
        .unwrap();
    let tool_result_id = append_tool_result_node(&store, &tool_use_id, "tool-1", "hello");
    store
        .set_branch_head("main", &tool_use_id, &tool_result_id)
        .unwrap();
    let failure_id = append_failure_node(&store, &tool_result_id, "command failed");
    store
        .set_branch_head("main", &tool_result_id, &failure_id)
        .unwrap();

    let output = run_with_backend(
        session_graph_cli(store_path),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();

    assert!(output.contains("tool_use"));
    assert!(output.contains("tool_result"));
    assert!(output.contains("[main] command failed"));
}

#[tokio::test]
async fn session_graph_and_show_render_skill_result_anchor() {
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
    let session_head = store.get_branch_head("main").unwrap();
    let tool_use_id = append_tool_use_node(&store, &session_head, "tool-1", "use_skill");
    store
        .set_branch_head("main", &session_head, &tool_use_id)
        .unwrap();
    let skill_result_id = append_skill_result_anchor(
        &store,
        &tool_use_id,
        &session_head,
        "tool-1",
        "find-skills",
        "Delegated result",
    );
    store
        .set_branch_head("main", &tool_use_id, &skill_result_id)
        .unwrap();

    let graph_output = run_with_backend(
        session_graph_cli(store_path.clone()),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(graph_output.contains("skill_result"));
    assert!(graph_output.contains("Delegated result"));
    let skill_result_line = graph_output
        .lines()
        .position(|line| line.contains("skill_result"))
        .expect("expected skill_result line");
    let tool_use_line = graph_output
        .lines()
        .position(|line| line.contains("tool_use"))
        .expect("expected tool_use line");
    assert!(skill_result_line < tool_use_line);

    let show_output = run_with_backend(
        session_show_cli(store_path, "main", false),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(show_output.contains("kind: skill_result"));
    assert!(show_output.contains("tool_id: tool-1"));
    assert!(show_output.contains("skill_name: find-skills"));
    assert!(show_output.contains("Delegated result"));
}

#[tokio::test]
async fn session_graph_places_skill_child_branch_on_the_right() {
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
    let session_head = store.get_branch_head("main").unwrap();
    let tool_use_id = append_tool_use_node(&store, &session_head, "tool-1", "use_skill");
    store
        .set_branch_head("main", &session_head, &tool_use_id)
        .unwrap();
    let child_session_id = append_session_anchor(
        &store,
        &tool_use_id,
        "You are executing the skill `fast-rust` on an isolated branch.",
    );
    let child_output_id = append_text_node(&store, &child_session_id, "Delegated result");
    let tool_result_id =
        append_tool_result_node(&store, &tool_use_id, "tool-1", "Delegated result");
    store
        .set_branch_head("main", &tool_use_id, &tool_result_id)
        .unwrap();

    let graph_output = run_with_backend(
        session_graph_cli(store_path.clone()),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();

    let short_id = |id: &str| id.chars().take(8).collect::<String>();
    assert!(graph_output.contains(&format!("* {} tool_result", short_id(&tool_result_id))));
    assert!(graph_output.contains(&format!("| * {} text", short_id(&child_output_id))));
    assert!(graph_output.contains(&format!("| * {} session", short_id(&child_session_id))));
    assert!(graph_output.contains(&format!("* {} tool_use", short_id(&tool_use_id))));
}

#[tokio::test]
async fn session_show_resolves_branch_to_head_node_text_output() {
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

    run_with_backend(
        prompt_cli(store_path.clone(), Some("main"), &["hello"]),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[("main", &[Ok("assistant reply")])]),
    )
    .await
    .unwrap();

    let head_id = open_store(&store_path)
        .unwrap()
        .get_branch_head("main")
        .unwrap();
    let output = run_with_backend(
        session_show_cli(store_path, "main", false),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();

    assert!(output.contains("ref: main"));
    assert!(output.contains(&format!("resolved_id: {head_id}")));
    assert!(output.contains("children: []"));
    assert!(output.contains("kind: text"));
    assert!(output.contains("assistant reply"));
}

#[tokio::test]
async fn session_show_outputs_json_for_node_id_reference() {
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

    let head_id = open_store(&store_path)
        .unwrap()
        .get_branch_head("main")
        .unwrap();
    let prefix = &head_id[..12];
    let output = run_with_backend(
        session_show_cli(store_path, prefix, true),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();
    let value = serde_json::from_str::<Value>(&output).unwrap();

    assert_eq!(value["ref"], json!(prefix));
    assert_eq!(value["resolved_id"], value["node"]["id"]);
    assert_eq!(value["resolved_id"], json!(head_id));
    assert_eq!(value["children"], json!([]));
    assert!(matches!(&value["node"]["kind"], Value::Object(_)));
}

#[tokio::test]
async fn session_show_outputs_children_ids_for_primary_and_merge_edges() {
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
    let session_id = store.get_branch_head("main").unwrap();
    let primary_child_id = append_prompt_anchor(&store, &session_id, "primary child", &[]);
    let merge_child_id =
        append_prompt_anchor(&store, &primary_child_id, "merge child", &[&session_id]);

    let output = run_with_backend(
        session_show_cli(store_path, &session_id, false),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();

    assert!(output.contains(&format!("children: [{primary_child_id}, {merge_child_id}]")));
}

#[tokio::test]
async fn session_show_json_includes_children_ids() {
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
    let session_id = store.get_branch_head("main").unwrap();
    let primary_child_id = append_prompt_anchor(&store, &session_id, "primary child", &[]);
    let merge_child_id =
        append_prompt_anchor(&store, &primary_child_id, "merge child", &[&session_id]);

    let output = run_with_backend(
        session_show_cli(store_path, &session_id[..12], true),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();
    let value = serde_json::from_str::<Value>(&output).unwrap();

    assert_eq!(value["children"], json!([primary_child_id, merge_child_id]));
}

#[tokio::test]
async fn session_show_resolves_short_node_prefix_after_source_branch_delete() {
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
            run_with_backend(
                session_fork_cli(store_path.clone(), "draft", Some("main")),
                &mut Cursor::new(""),
                FakeBackend::with_responses(&[]),
            )
            .await
            .unwrap();
        },
    )
    .await;

    let store = open_store(&store_path).unwrap();
    let draft_head = store.get_branch_head("draft").unwrap();
    let draft_node_id = append_prompt_anchor(&store, &draft_head, "deleted branch node", &[]);
    store
        .set_branch_head("draft", &draft_head, &draft_node_id)
        .unwrap();

    run_with_backend(
        session_delete_cli(store_path.clone(), Some("draft")),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap();

    let prefix = &draft_node_id[..8];
    let output = run_with_backend(
        session_show_cli(store_path, prefix, false),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();

    assert!(output.contains(&format!("ref: {prefix}")));
    assert!(output.contains(&format!("resolved_id: {draft_node_id}")));
    assert!(output.contains("deleted branch node"));
}

#[tokio::test]
async fn session_show_reports_ambiguous_short_node_prefix() {
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
    let root_id = store.root_id();
    let session_id = store.get_branch_head("main").unwrap();
    let mut ids = vec![root_id, session_id];
    for index in 0..32 {
        ids.push(append_prompt_anchor(
            &store,
            &store.get_branch_head("main").unwrap(),
            &format!("node-{index}"),
            &[],
        ));
    }
    let (prefix, matches) = ids
        .into_iter()
        .fold(HashMap::<String, Vec<String>>::new(), |mut groups, id| {
            groups.entry(id[..1].to_owned()).or_default().push(id);
            groups
        })
        .into_iter()
        .find_map(|(prefix, matches)| (matches.len() > 1).then_some((prefix, matches)))
        .expect("expected at least one ambiguous one-character prefix");

    let err = run_with_backend(
        session_show_cli(store_path, &prefix, false),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap_err();

    assert!(matches!(
        err,
        crate::Error::AmbiguousNodePrefix {
            prefix: actual_prefix,
            matches: actual_matches,
        } if actual_prefix == prefix && actual_matches.len() == matches.len()
    ));
}

#[test]
fn session_show_parses_reference_as_positional_argument() {
    let cli = Cli::try_parse_from(["coco-cli", "session", "show", "main", "--json"]).unwrap();

    let Command::Session(command) = cli.command else {
        panic!("expected session command");
    };
    let SessionSubcommand::Show(command) = command.command else {
        panic!("expected show command");
    };

    assert_eq!(command.reference, "main");
    assert!(command.json);
}

#[test]
fn skill_show_parses_name_and_role_flags() {
    let cli = Cli::try_parse_from([
        "coco-cli",
        "skill",
        "show",
        "--role",
        "runner",
        "--name",
        "coco-runner",
    ])
    .unwrap();

    let Command::Skill(command) = cli.command else {
        panic!("expected skill command");
    };
    let crate::cli::SkillSubcommand::Show(command) = command.command else {
        panic!("expected skill show command");
    };

    assert_eq!(command.role, crate::cli::CliSessionRole::Runner);
    assert_eq!(command.name, "coco-runner");
}

#[tokio::test]
async fn skill_commands_manage_versions_in_store() {
    let (_tempdir, store_path) = temp_store_path();
    let skill_file = store_path.with_file_name("skill.md");
    let skill_name = "custom-orchestrator";
    fs::write(&skill_file, "# v1\n").unwrap();

    run_with_backend(
        Cli::try_parse_from([
            "coco-cli",
            "--store-path",
            store_path.to_str().unwrap(),
            "skill",
            "add",
            "--role",
            "orchestrator",
            "--name",
            skill_name,
            "--description",
            "first",
            "--file",
            skill_file.to_str().unwrap(),
            "--enable-coco-shim",
        ])
        .unwrap(),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap();

    fs::write(&skill_file, "# v2\n").unwrap();
    let update_output = run_with_backend(
        Cli::try_parse_from([
            "coco-cli",
            "--store-path",
            store_path.to_str().unwrap(),
            "skill",
            "update",
            "--role",
            "orchestrator",
            "--name",
            skill_name,
            "--description",
            "second",
            "--file",
            skill_file.to_str().unwrap(),
        ])
        .unwrap(),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();
    let update_json: serde_json::Value = serde_json::from_str(&update_output).unwrap();
    assert_eq!(update_json["current_version"], 2);
    assert_eq!(update_json["available_versions"], json!([1, 2]));

    let rollback_output = run_with_backend(
        Cli::try_parse_from([
            "coco-cli",
            "--store-path",
            store_path.to_str().unwrap(),
            "skill",
            "rollback",
            "--role",
            "orchestrator",
            "--name",
            skill_name,
            "--to-version",
            "1",
        ])
        .unwrap(),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();
    let rollback_json: serde_json::Value = serde_json::from_str(&rollback_output).unwrap();
    assert_eq!(rollback_json["current_version"], 3);
    assert_eq!(rollback_json["available_versions"], json!([1, 2, 3]));

    let show_output = run_with_backend(
        Cli::try_parse_from([
            "coco-cli",
            "--store-path",
            store_path.to_str().unwrap(),
            "skill",
            "show",
            "--role",
            "orchestrator",
            "--name",
            skill_name,
        ])
        .unwrap(),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();
    let show_json: serde_json::Value = serde_json::from_str(&show_output).unwrap();
    assert_eq!(show_json["current_version"], 3);
    assert_eq!(show_json["versions"][2]["body"], "# v1");
}

#[tokio::test]
async fn skill_show_reads_default_initialized_skill() {
    let (_tempdir, store_path) = temp_store_path();

    let output = run_with_backend(
        Cli::try_parse_from([
            "coco-cli",
            "--store-path",
            store_path.to_str().unwrap(),
            "skill",
            "show",
            "--role",
            "runner",
            "--name",
            "coco-runner",
        ])
        .unwrap(),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();

    let show_json: serde_json::Value = serde_json::from_str(&output).unwrap();
    assert_eq!(show_json["name"], "coco-runner");
    assert_eq!(show_json["current_version"], 1);
}

#[tokio::test]
async fn skill_list_defaults_to_text_and_supports_json() {
    let (_tempdir, store_path) = temp_store_path();

    let text_output = run_with_backend(
        Cli::try_parse_from([
            "coco-cli",
            "--store-path",
            store_path.to_str().unwrap(),
            "skill",
            "list",
        ])
        .unwrap(),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();

    assert!(serde_json::from_str::<serde_json::Value>(&text_output).is_err());
    assert!(text_output.contains("coco-orchestrator"));
    assert!(text_output.contains("coco-runner"));

    let json_output = run_with_backend(
        Cli::try_parse_from([
            "coco-cli",
            "--store-path",
            store_path.to_str().unwrap(),
            "skill",
            "list",
            "--json",
        ])
        .unwrap(),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();

    let skills: serde_json::Value = serde_json::from_str(&json_output).unwrap();
    assert!(
        skills
            .as_array()
            .unwrap()
            .iter()
            .any(|skill| { skill["role"] == "runner" && skill["name"] == "coco-runner" })
    );
}

#[test]
fn preset_set_parses_name_and_patch_flags() {
    let cli = Cli::try_parse_from([
        "coco-cli",
        "preset",
        "set",
        "--name",
        "coding",
        "--role",
        "runner",
        "--provider-profile",
        "anthropic-main",
        "--model",
        "claude-sonnet-4-20250514",
        "--system-prompt",
        "You are precise.",
        "--tool",
        "bash",
        "--additional-params",
        "{\"reasoning_effort\":\"medium\"}",
        "--enable-coco-shim",
    ])
    .unwrap();

    let Command::Preset(command) = cli.command else {
        panic!("expected preset command");
    };
    let PresetSubcommand::Set(command) = command.command else {
        panic!("expected preset set command");
    };

    assert_eq!(command.name, "coding");
    assert_eq!(command.role, crate::cli::CliSessionRole::Runner);
    assert_eq!(command.provider_profile, "anthropic-main");
    assert_eq!(command.model.as_deref(), Some("claude-sonnet-4-20250514"));
    assert_eq!(command.system_prompt, "You are precise.");
    assert_eq!(command.tools, vec![crate::cli::CliTool::Bash]);
    assert!(command.enable_coco_shim);
}

#[test]
fn provider_profiles_load_from_cwd_config_toml() {
    let tempdir = tempdir().unwrap();
    let config_path = tempdir.path().join("config.toml");
    std::fs::write(
        &config_path,
        r#"[providers.work-openai]
provider = "openai"
base_url = "https://openai.example.test/v1"
default_model = "gpt-4.1-mini"

[providers.work-openai.secrets]
api_key = "${COCO_WORK_OPENAI_API_KEY}"
"#,
    )
    .unwrap();

    let profiles = crate::app::config::load_provider_profiles_from(&config_path).unwrap();
    let profile = profiles.get_provider_profile("work-openai").unwrap();
    assert_eq!(profile.provider, "openai");
    assert_eq!(
        profile.secrets.get("api_key").map(String::as_str),
        Some("${COCO_WORK_OPENAI_API_KEY}")
    );
    assert_eq!(profile.default_model.as_deref(), Some("gpt-4.1-mini"));
}

#[tokio::test]
async fn preset_can_reference_provider_profile_id() {
    let (_tempdir, store_path) = temp_store_path();
    let provider_profiles = ProviderProfiles::from_profiles(HashMap::from([provider_profile(
        "work-openai",
        "openai",
        "${COCO_WORK_OPENAI_API_KEY}",
        None,
        Some("gpt-4.1-mini"),
    )]));

    let output = run_with_backend_and_provider_profiles(
        Cli::try_parse_from([
            "coco-cli",
            "--store-path",
            store_path.to_str().unwrap(),
            "preset",
            "set",
            "--name",
            "coding",
            "--role",
            "orchestrator",
            "--provider-profile",
            "work-openai",
            "--system-prompt",
            "You are helpful.",
        ])
        .unwrap(),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
        provider_profiles,
    )
    .await
    .unwrap()
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&output).unwrap();
    assert_eq!(value["config"]["provider_profile"], "work-openai");
    assert_eq!(value["config"]["model"], "gpt-4.1-mini");
}

#[tokio::test]
async fn preset_commands_manage_versions_in_store() {
    let (_tempdir, store_path) = temp_store_path();
    let preset_name = "coding";

    let provider_profiles = ProviderProfiles::from_profiles(HashMap::from([
        provider_profile("openai", "openai", "${OPENAI_API_KEY}", None, None),
        provider_profile("anthropic", "anthropic", "${ANTHROPIC_API_KEY}", None, None),
    ]));

    let first_output = run_with_backend_and_provider_profiles(
        Cli::try_parse_from([
            "coco-cli",
            "--store-path",
            store_path.to_str().unwrap(),
            "preset",
            "set",
            "--name",
            preset_name,
            "--role",
            "orchestrator",
            "--provider-profile",
            "openai",
            "--model",
            "gpt-4.1-mini",
            "--system-prompt",
            "You are helpful.",
            "--temperature",
            "0.2",
            "--max-tokens",
            "64",
            "--additional-params",
            "{\"reasoning_effort\":\"low\"}",
            "--tool",
            "bash",
        ])
        .unwrap(),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
        provider_profiles.clone(),
    )
    .await
    .unwrap()
    .unwrap();
    let first_json: serde_json::Value = serde_json::from_str(&first_output).unwrap();
    assert_eq!(first_json["name"], preset_name);
    assert_eq!(first_json["current_version"], 1);
    assert_eq!(first_json["config"]["role"], "orchestrator");
    assert_eq!(first_json["config"]["provider_profile"], "openai");
    assert_eq!(first_json["config"]["model"], "gpt-4.1-mini");
    assert_eq!(
        first_json["config"]["additional_params"],
        json!({"reasoning_effort": "low"})
    );
    assert_eq!(first_json["config"]["tools"][0]["name"], "bash");

    let second_output = run_with_backend_and_provider_profiles(
        Cli::try_parse_from([
            "coco-cli",
            "--store-path",
            store_path.to_str().unwrap(),
            "preset",
            "set",
            "--name",
            preset_name,
            "--role",
            "runner",
            "--provider-profile",
            "anthropic",
            "--model",
            "claude-sonnet-4-20250514",
            "--system-prompt",
            "You are strict.",
            "--disable-coco-shim",
        ])
        .unwrap(),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
        provider_profiles,
    )
    .await
    .unwrap()
    .unwrap();
    let second_json: serde_json::Value = serde_json::from_str(&second_output).unwrap();
    assert_eq!(second_json["current_version"], 2);
    assert_eq!(second_json["available_versions"], json!([1, 2]));
    assert_eq!(second_json["config"]["role"], "runner");
    assert_eq!(second_json["config"]["provider_profile"], "anthropic");
    assert_eq!(second_json["config"]["model"], "claude-sonnet-4-20250514");
    assert_eq!(second_json["config"]["temperature"], json!(null));
    assert_eq!(second_json["config"]["max_tokens"], json!(null));
    assert_eq!(second_json["config"]["tools"], json!([]));
    assert_eq!(second_json["config"]["additional_params"], json!(null));
    assert_eq!(second_json["config"]["enable_coco_shim"], false);
    let persisted = open_store(&store_path)
        .unwrap()
        .get_branch_config(preset_name)
        .unwrap();
    assert_eq!(persisted.temperature, None);
    assert_eq!(persisted.max_tokens, None);
    assert_eq!(persisted.additional_params, None);

    let list_output = run_with_backend(
        Cli::try_parse_from([
            "coco-cli",
            "--store-path",
            store_path.to_str().unwrap(),
            "preset",
            "list",
        ])
        .unwrap(),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();
    let list_json: serde_json::Value = serde_json::from_str(&list_output).unwrap();
    assert_eq!(list_json.as_array().unwrap().len(), 1);
    assert_eq!(list_json[0]["name"], preset_name);

    let show_output = run_with_backend(
        Cli::try_parse_from([
            "coco-cli",
            "--store-path",
            store_path.to_str().unwrap(),
            "preset",
            "show",
            "--name",
            preset_name,
        ])
        .unwrap(),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();
    let show_json: serde_json::Value = serde_json::from_str(&show_output).unwrap();
    assert_eq!(show_json["versions"][0]["config"]["model"], "gpt-4.1-mini");
    assert_eq!(
        show_json["versions"][1]["config"]["model"],
        "claude-sonnet-4-20250514"
    );

    let rollback_output = run_with_backend(
        Cli::try_parse_from([
            "coco-cli",
            "--store-path",
            store_path.to_str().unwrap(),
            "preset",
            "rollback",
            "--name",
            preset_name,
            "--to-version",
            "1",
        ])
        .unwrap(),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();
    let rollback_json: serde_json::Value = serde_json::from_str(&rollback_output).unwrap();
    assert_eq!(rollback_json["current_version"], 3);
    assert_eq!(rollback_json["config"]["model"], "gpt-4.1-mini");

    let delete_output = run_with_backend(
        Cli::try_parse_from([
            "coco-cli",
            "--store-path",
            store_path.to_str().unwrap(),
            "preset",
            "delete",
            "--name",
            preset_name,
        ])
        .unwrap(),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&delete_output).unwrap(),
        json!({ "name": preset_name })
    );
    assert!(
        open_store(&store_path)
            .unwrap()
            .list_branch_config_records()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn session_rebase_applies_preset_patch() {
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

    let provider_profiles = ProviderProfiles::from_profiles(HashMap::from([provider_profile(
        "anthropic-main",
        "anthropic",
        "${ANTHROPIC_API_KEY}",
        None,
        None,
    )]));

    run_with_backend_and_provider_profiles(
        Cli {
            daemon_socket: None,
            store_path: store_path.clone(),
            command: Command::Preset(PresetCommand {
                command: PresetSubcommand::Set(PresetSetCommand {
                    name: "runner-defaults".to_owned(),
                    role: crate::cli::CliSessionRole::Runner,
                    provider_profile: "anthropic-main".to_owned(),
                    model: Some("claude-sonnet-4-20250514".to_owned()),
                    system_prompt: "You are precise.".to_owned(),
                    prompt: "Start with a plan.".to_owned(),
                    temperature: None,
                    max_tokens: Some(256),
                    tools: vec![],
                    additional_params: Some("{\"reasoning_effort\":\"medium\"}".to_owned()),
                    enable_coco_shim: true,
                    disable_coco_shim: false,
                }),
            }),
        },
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
        provider_profiles.clone(),
    )
    .await
    .unwrap();

    let output = run_with_backend_and_provider_profiles(
        Cli::try_parse_from([
            "coco-cli",
            "--store-path",
            store_path.to_str().unwrap(),
            "session",
            "rebase",
            "--preset",
            "runner-defaults",
            "--branch",
            "main",
        ])
        .unwrap(),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
        provider_profiles,
    )
    .await
    .unwrap()
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&output).unwrap();
    assert_eq!(value["branch"], "main");
    assert!(value["head_id"].as_str().is_some());

    let session_output = run_with_backend(
        session_get_cli(store_path, Some("main")),
        &mut Cursor::new(""),
        FakeBackend::with_responses(&[]),
    )
    .await
    .unwrap()
    .unwrap();
    let session_json: serde_json::Value = serde_json::from_str(&session_output).unwrap();
    assert_eq!(session_json["role"], "runner");
    assert_eq!(session_json["anchor"]["provider_profile"], "anthropic-main");
    assert_eq!(session_json["anchor"]["provider"], json!(null));
    assert_eq!(session_json["anchor"]["model"], "claude-sonnet-4-20250514");
    assert_eq!(session_json["anchor"]["system_prompt"], "You are precise.");
    assert_eq!(session_json["anchor"]["prompt"], "Start with a plan.");
    assert_eq!(session_json["anchor"]["temperature"], json!(null));
    assert_eq!(session_json["anchor"]["max_tokens"], 256);
    assert_eq!(
        session_json["anchor"]["additional_params"],
        json!({"reasoning_effort": "medium"})
    );
    assert_eq!(session_json["anchor"]["tools"], json!([]));
    assert_eq!(session_json["anchor"]["enable_coco_shim"], true);
}

#[tokio::test]
async fn forwarded_runtime_prompt_uses_branch_env_when_flag_is_omitted() {
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

    let store = open_store(&store_path).unwrap();
    let llm = llm_with_test_provider_config(
        store.clone(),
        FakeBackend::with_responses(&[("draft", &[Ok("world")])]),
    );

    let response = run_forwarded_with_services(
        ForwardedRuntimeInputs {
            args: &["prompt".to_owned(), "hello".to_owned()],
            stdin: &[],
            branch_env: Some("draft"),
            session_role: Some(SessionRole::Orchestrator),
            store_path_env: None,
        },
        RuntimeServices {
            shared_store: &store,
            llm: &llm,
            provider_profiles: shared_test_provider_profiles(),
            shared_engine: None,
        },
    )
    .await;

    assert_eq!(response.exit_code, 0);
    assert_eq!(response.stdout, "world\n");
    assert!(response.stderr.is_empty());
}

#[tokio::test]
async fn forwarded_runtime_prompt_keeps_explicit_branch_over_env_default() {
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
    let llm = llm_with_test_provider_config(
        store.clone(),
        FakeBackend::with_responses(&[("main", &[Ok("main-response")])]),
    );

    let response = run_forwarded_with_services(
        ForwardedRuntimeInputs {
            args: &[
                "prompt".to_owned(),
                "--branch".to_owned(),
                "main".to_owned(),
                "hello".to_owned(),
            ],
            stdin: &[],
            branch_env: Some("draft"),
            session_role: Some(SessionRole::Orchestrator),
            store_path_env: None,
        },
        RuntimeServices {
            shared_store: &store,
            llm: &llm,
            provider_profiles: shared_test_provider_profiles(),
            shared_engine: None,
        },
    )
    .await;

    assert_eq!(response.exit_code, 0);
    assert_eq!(response.stdout, "main-response\n");
    assert!(response.stderr.is_empty());
}

#[tokio::test]
async fn forwarded_runtime_runner_prompt_help_hides_write_entrypoints() {
    let (_tempdir, store_path) = temp_store_path();
    let store = open_store(&store_path).unwrap();
    let llm = llm_with_test_provider_config(store.clone(), FakeBackend::with_responses(&[]));

    let response = run_forwarded_with_services(
        ForwardedRuntimeInputs {
            args: &["prompt".to_owned(), "--help".to_owned()],
            stdin: &[],
            branch_env: Some("main"),
            session_role: Some(SessionRole::Runner),
            store_path_env: None,
        },
        RuntimeServices {
            shared_store: &store,
            llm: &llm,
            provider_profiles: shared_test_provider_profiles(),
            shared_engine: None,
        },
    )
    .await;

    assert_eq!(response.exit_code, 0);
    assert!(response.stdout.contains("Usage: coco prompt"));
    assert!(response.stdout.contains("status"));
    assert!(response.stdout.contains("branch-status"));
    assert!(!response.stdout.contains("[TEXT]"));
    assert!(!response.stdout.contains("--async"));
    assert!(!response.stdout.contains("--store-path"));
    assert!(response.stderr.is_empty());
}

#[tokio::test]
async fn forwarded_runtime_runner_session_help_hides_write_subcommands() {
    let (_tempdir, store_path) = temp_store_path();
    let store = open_store(&store_path).unwrap();
    let llm = llm_with_test_provider_config(store.clone(), FakeBackend::with_responses(&[]));

    let response = run_forwarded_with_services(
        ForwardedRuntimeInputs {
            args: &["session".to_owned(), "--help".to_owned()],
            stdin: &[],
            branch_env: Some("main"),
            session_role: Some(SessionRole::Runner),
            store_path_env: None,
        },
        RuntimeServices {
            shared_store: &store,
            llm: &llm,
            provider_profiles: shared_test_provider_profiles(),
            shared_engine: None,
        },
    )
    .await;

    assert_eq!(response.exit_code, 0);
    assert!(response.stdout.contains("Usage: coco session"));
    assert!(response.stdout.contains("list"));
    assert!(response.stdout.contains("get"));
    assert!(response.stdout.contains("graph"));
    assert!(response.stdout.contains("show"));
    assert!(!response.stdout.contains("create"));
    assert!(!response.stdout.contains("merge"));
    assert!(!response.stdout.contains("--store-path"));
    assert!(response.stderr.is_empty());
}

#[tokio::test]
async fn forwarded_runtime_orchestrator_help_hides_store_path_option() {
    let (_tempdir, store_path) = temp_store_path();
    let store = open_store(&store_path).unwrap();
    let llm = llm_with_test_provider_config(store.clone(), FakeBackend::with_responses(&[]));

    let response = run_forwarded_with_services(
        ForwardedRuntimeInputs {
            args: &["prompt".to_owned(), "--help".to_owned()],
            stdin: &[],
            branch_env: Some("main"),
            session_role: Some(SessionRole::Orchestrator),
            store_path_env: None,
        },
        RuntimeServices {
            shared_store: &store,
            llm: &llm,
            provider_profiles: shared_test_provider_profiles(),
            shared_engine: None,
        },
    )
    .await;

    assert_eq!(response.exit_code, 0);
    assert!(response.stdout.contains("Usage: coco prompt"));
    assert!(!response.stdout.contains("--store-path"));
    assert!(response.stderr.is_empty());
}

#[tokio::test]
async fn forwarded_runtime_runner_write_commands_fail_via_parser_errors() {
    let (_tempdir, store_path) = temp_store_path();
    let store = open_store(&store_path).unwrap();
    let llm = llm_with_test_provider_config(store.clone(), FakeBackend::with_responses(&[]));

    let prompt_response = run_forwarded_with_services(
        ForwardedRuntimeInputs {
            args: &["prompt".to_owned(), "hello".to_owned()],
            stdin: &[],
            branch_env: Some("main"),
            session_role: Some(SessionRole::Runner),
            store_path_env: None,
        },
        RuntimeServices {
            shared_store: &store,
            llm: &llm,
            provider_profiles: shared_test_provider_profiles(),
            shared_engine: None,
        },
    )
    .await;

    assert_eq!(prompt_response.exit_code, 2);
    assert!(
        prompt_response
            .stderr
            .contains("unrecognized subcommand 'hello'")
    );
    assert!(
        prompt_response
            .stderr
            .contains("Usage: coco prompt <COMMAND>")
    );

    let session_response = run_forwarded_with_services(
        ForwardedRuntimeInputs {
            args: &[
                "session".to_owned(),
                "create".to_owned(),
                "--help".to_owned(),
            ],
            stdin: &[],
            branch_env: Some("main"),
            session_role: Some(SessionRole::Runner),
            store_path_env: None,
        },
        RuntimeServices {
            shared_store: &store,
            llm: &llm,
            provider_profiles: shared_test_provider_profiles(),
            shared_engine: None,
        },
    )
    .await;

    assert_eq!(session_response.exit_code, 2);
    assert!(
        session_response
            .stderr
            .contains("unrecognized subcommand 'create'")
    );
    assert!(
        session_response
            .stderr
            .contains("Usage: coco session <COMMAND>")
    );

    let skill_response = run_forwarded_with_services(
        ForwardedRuntimeInputs {
            args: &["skill".to_owned(), "add".to_owned(), "--help".to_owned()],
            stdin: &[],
            branch_env: Some("main"),
            session_role: Some(SessionRole::Runner),
            store_path_env: None,
        },
        RuntimeServices {
            shared_store: &store,
            llm: &llm,
            provider_profiles: shared_test_provider_profiles(),
            shared_engine: None,
        },
    )
    .await;

    assert_eq!(skill_response.exit_code, 2);
    assert!(
        skill_response
            .stderr
            .contains("unrecognized subcommand 'skill'")
    );

    let preset_response = run_forwarded_with_services(
        ForwardedRuntimeInputs {
            args: &["preset".to_owned(), "list".to_owned()],
            stdin: &[],
            branch_env: Some("main"),
            session_role: Some(SessionRole::Runner),
            store_path_env: None,
        },
        RuntimeServices {
            shared_store: &store,
            llm: &llm,
            provider_profiles: shared_test_provider_profiles(),
            shared_engine: None,
        },
    )
    .await;

    assert_eq!(preset_response.exit_code, 2);
    assert!(
        preset_response
            .stderr
            .contains("unrecognized subcommand 'preset'")
    );
}

#[tokio::test]
async fn forwarded_runtime_rejects_store_path_override() {
    let (_tempdir, store_path) = temp_store_path();
    let store = open_store(&store_path).unwrap();
    let llm = llm_with_test_provider_config(store.clone(), FakeBackend::with_responses(&[]));

    let response = run_forwarded_with_services(
        ForwardedRuntimeInputs {
            args: &[
                "--store-path".to_owned(),
                "/tmp/override".to_owned(),
                "session".to_owned(),
                "list".to_owned(),
            ],
            stdin: &[],
            branch_env: Some("main"),
            session_role: Some(SessionRole::Orchestrator),
            store_path_env: None,
        },
        RuntimeServices {
            shared_store: &store,
            llm: &llm,
            provider_profiles: shared_test_provider_profiles(),
            shared_engine: None,
        },
    )
    .await;

    assert_eq!(response.exit_code, 1);
    assert!(response.stderr.contains("\"--store-path\""));
    assert!(!response.stderr.contains("Usage:"));
}

#[tokio::test]
async fn daemon_server_executes_forwarded_cli_requests_over_socket() {
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

    let socket_dir = tempfile::Builder::new()
        .prefix("coco-daemon-test-")
        .tempdir_in("/tmp")
        .unwrap();
    let socket_path = socket_dir.path().join("coco.sock");
    let store = open_store(&store_path).unwrap();
    let llm = llm_with_test_provider_config(
        store.clone(),
        FakeBackend::with_responses(&[("main", &[Ok("daemon-response")])]),
    );
    let engine = Arc::new(coco_core::ConversationEngine::new(llm.clone()));
    let server = match start_daemon_server(
        &socket_path,
        &store,
        &llm,
        shared_test_provider_profiles(),
        &engine,
        None,
        None,
    ) {
        Ok(server) => server,
        Err(crate::Error::BindDaemonSocket { source, .. })
            if source.kind() == std::io::ErrorKind::PermissionDenied =>
        {
            return;
        }
        Err(error) => panic!("failed to start daemon server: {error}"),
    };

    let request = coco_llm::CocoCliRuntimeRequest {
        args: vec!["prompt".to_owned(), "hello".to_owned()],
        stdin: Vec::new(),
        branch_env: Some("main".to_owned()),
        session_role: Some(SessionRole::Orchestrator),
        store_path_env: None,
    };

    let payload = serde_json::to_vec(&request).unwrap();
    let mut stream = UnixStream::connect(&socket_path).await.unwrap();
    stream.write_all(&payload).await.unwrap();
    stream.shutdown().await.unwrap();

    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let response: coco_llm::CocoCliRuntimeResponse = serde_json::from_slice(&response).unwrap();

    assert_eq!(response.exit_code, 0);
    assert_eq!(response.stdout, "daemon-response\n");
    assert!(response.stderr.is_empty());

    server.shutdown().await.unwrap();
}

#[test]
fn daemon_serve_enables_console_by_default() {
    let cli = Cli::parse_from(["coco", "daemon", "serve"]);
    let Command::Daemon(command) = cli.command else {
        panic!("expected daemon command");
    };
    let DaemonSubcommand::Serve(command) = command.command;

    assert!(!command.no_console);
    assert_eq!(
        command.console_addr,
        std::net::SocketAddr::from(([127, 0, 0, 1], 17667))
    );
}

#[test]
fn daemon_serve_allows_disabling_console_and_overriding_addr() {
    let cli = Cli::parse_from([
        "coco",
        "daemon",
        "serve",
        "--no-console",
        "--console-addr",
        "127.0.0.1:0",
    ]);
    let Command::Daemon(command) = cli.command else {
        panic!("expected daemon command");
    };
    let DaemonSubcommand::Serve(command) = command.command;

    assert!(command.no_console);
    assert_eq!(
        command.console_addr,
        std::net::SocketAddr::from(([127, 0, 0, 1], 0))
    );
}

#[tokio::test]
async fn daemon_startup_resumes_incomplete_jobs() {
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
    let job = submit_prompt_job(&store, "main", "resume me");
    store
        .set_job_status(
            &job.job_id,
            coco_mem::JobStatus::Queued,
            coco_mem::JobStatus::Running,
        )
        .unwrap();

    let llm = llm_with_test_provider_config(
        store.clone(),
        FakeBackend::with_responses(&[("main", &[Ok("recovered after daemon start")])]),
    );
    let engine = coco_core::ConversationEngine::new(llm);

    resume_incomplete_jobs(&engine).await.unwrap();

    let resumed_job = store.get_job(&job.job_id).unwrap();
    assert_eq!(resumed_job.status, coco_mem::JobStatus::Finished);
    let head = store.get_branch_head("main").unwrap();
    let node = store.get_node(&head).unwrap();
    match node.kind {
        Kind::Text(text) => assert_eq!(text, "recovered after daemon start"),
        other => panic!("expected text node at branch head, got {other:?}"),
    }
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
                    role: crate::cli::CliSessionRole::Orchestrator,
                    provider_profile: None,
                    system_prompt: "You are helpful.".to_owned(),
                    prompt: "".to_owned(),
                    temperature: Some(0.2),
                    max_tokens: Some(64),
                    additional_params: None,
                    tools: vec![],
                })
                .unwrap()
            },
        ));

    assert_eq!(config.provider, Provider::ChatGpt);
    assert_eq!(config.model, "gpt-5.4");
    assert_eq!(config.role, SessionRole::Orchestrator);
}

#[test]
fn resolve_session_config_accepts_chatgpt_provider() {
    let config = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(with_coco_env_async(
            &[
                ("COCO_PROVIDER", "chatgpt"),
                ("COCO_MODEL", "gpt-5.3-codex"),
            ],
            || async {
                resolve_session_config(SessionCreateCommand {
                    branch: "main".to_owned(),
                    role: crate::cli::CliSessionRole::Orchestrator,
                    provider_profile: None,
                    system_prompt: "You are helpful.".to_owned(),
                    prompt: "".to_owned(),
                    temperature: Some(0.2),
                    max_tokens: Some(64),
                    additional_params: None,
                    tools: vec![],
                })
                .unwrap()
            },
        ));

    assert_eq!(config.provider, Provider::ChatGpt);
    assert_eq!(config.model, "gpt-5.4");
}

#[test]
fn resolve_session_config_reads_tools_from_env() {
    let config = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(with_coco_env_async(
            &[
                ("COCO_PROVIDER", "openai"),
                ("COCO_MODEL", "gpt-4.1-mini"),
                ("COCO_TOOLS", "bash,search_skill,use_skill"),
            ],
            || async {
                resolve_session_config(SessionCreateCommand {
                    branch: "main".to_owned(),
                    role: crate::cli::CliSessionRole::Orchestrator,
                    provider_profile: None,
                    system_prompt: "You are helpful.".to_owned(),
                    prompt: "".to_owned(),
                    temperature: Some(0.2),
                    max_tokens: Some(64),
                    additional_params: None,
                    tools: vec![],
                })
                .unwrap()
            },
        ));

    assert_eq!(
        config
            .tools
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>(),
        vec!["bash", "search_skill", "use_skill"]
    );
}

#[test]
fn resolve_session_config_parses_additional_params_json_object() {
    let config = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(with_coco_env_async(
            &[("COCO_PROVIDER", "openai"), ("COCO_MODEL", "gpt-4.1-mini")],
            || async {
                resolve_session_config(SessionCreateCommand {
                    branch: "main".to_owned(),
                    role: crate::cli::CliSessionRole::Orchestrator,
                    provider_profile: None,
                    system_prompt: "You are helpful.".to_owned(),
                    prompt: "".to_owned(),
                    temperature: Some(0.2),
                    max_tokens: Some(64),
                    additional_params: Some(
                        "{\"service_tier\":\"priority\",\"reasoning_effort\":\"low\"}".to_owned(),
                    ),
                    tools: vec![],
                })
                .unwrap()
            },
        ));

    assert_eq!(
        config.additional_params,
        Some(json!({
            "service_tier": "priority",
            "reasoning_effort": "low",
        }))
    );
}

#[test]
fn resolve_session_config_rejects_non_object_additional_params() {
    let error = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(with_coco_env_async(
            &[("COCO_PROVIDER", "openai"), ("COCO_MODEL", "gpt-4.1-mini")],
            || async {
                resolve_session_config(SessionCreateCommand {
                    branch: "main".to_owned(),
                    role: crate::cli::CliSessionRole::Orchestrator,
                    provider_profile: None,
                    system_prompt: "You are helpful.".to_owned(),
                    prompt: "".to_owned(),
                    temperature: Some(0.2),
                    max_tokens: Some(64),
                    additional_params: Some("[1,2,3]".to_owned()),
                    tools: vec![],
                })
                .unwrap_err()
            },
        ));

    assert!(matches!(
        error,
        crate::Error::InvalidSessionAdditionalParamsType { value } if value == json!([1, 2, 3])
    ));
}
