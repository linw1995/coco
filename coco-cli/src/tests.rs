use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::io::Cursor;
use std::sync::{Arc, Mutex, OnceLock};

use coco_llm::coco_mem::{Anchor, Kind, NewNode, PromptAnchor, Role, Store};
use coco_llm::{
    BackendError, BackendEvent, BackendRun, CompletionBackend, Provider, ResolvedCompletionRequest,
    ResolvedSession,
};
use coco_mem::SessionState;
use serde_json::{Value, json};
use tempfile::{TempDir, tempdir};
use tokio::sync::Mutex as AsyncMutex;

use crate::{
    Cli,
    app::{resolve_session_config, run_forwarded_with_services, run_with_backend},
    cli::{
        Command, PromptCommand, SessionBranchCommand, SessionCloseCommand, SessionCommand,
        SessionCreateCommand, SessionFeedbackCommand, SessionForkCommand, SessionMergeCommand,
        SessionPrCommand, SessionRebaseCommand, SessionSubcommand,
    },
    store::open_store,
};

type FakeResponseQueue =
    Arc<Mutex<HashMap<String, VecDeque<std::result::Result<BackendRun, BackendError>>>>>;

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
                                .map(|text| {
                                    BackendRun::succeeded(
                                        (*text).to_owned(),
                                        vec![BackendEvent::AssistantText((*text).to_owned())],
                                    )
                                })
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
        _session: ResolvedSession,
        request: ResolvedCompletionRequest,
    ) -> std::result::Result<BackendRun, BackendError> {
        let mut responses = self.responses.lock().unwrap();
        let queue = responses
            .get_mut(&request.branch)
            .expect("missing fake backend queue");
        queue.pop_front().expect("missing fake backend response")
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
                tools: vec![],
            }),
        }),
    }
}

fn session_list_cli(store_path: std::path::PathBuf) -> Cli {
    Cli {
        store_path,
        command: Command::Session(SessionCommand {
            command: SessionSubcommand::List,
        }),
    }
}

fn session_fork_cli(store_path: std::path::PathBuf, branch: &str, from_ref: Option<&str>) -> Cli {
    Cli {
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
        store_path,
        command: Command::Session(SessionCommand {
            command: SessionSubcommand::Get(SessionBranchCommand {
                branch: branch.unwrap_or("main").to_owned(),
            }),
        }),
    }
}

fn session_rebase_cli(store_path: std::path::PathBuf, branch: Option<&str>) -> Cli {
    Cli {
        store_path,
        command: Command::Session(SessionCommand {
            command: SessionSubcommand::Rebase(SessionRebaseCommand {
                branch: branch.unwrap_or("main").to_owned(),
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

fn append_prompt_anchor(
    store: &impl Store,
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
    assert_eq!(value["state"], json!("Active"));
    assert_eq!(value["anchor"]["provider"], "openai");
    assert_eq!(value["anchor"]["model"], "gpt-4.1-mini");
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
    let llm = Arc::new(coco_llm::LlmService::new(
        store.clone(),
        FakeBackend::with_responses(&[("draft", &[Ok("world")])]),
    ));

    let response = run_forwarded_with_services(
        &["prompt".to_owned(), "hello".to_owned()],
        &[],
        Some("draft"),
        None,
        &store,
        &llm,
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
    let llm = Arc::new(coco_llm::LlmService::new(
        store.clone(),
        FakeBackend::with_responses(&[("main", &[Ok("main-response")])]),
    ));

    let response = run_forwarded_with_services(
        &[
            "prompt".to_owned(),
            "--branch".to_owned(),
            "main".to_owned(),
            "hello".to_owned(),
        ],
        &[],
        Some("draft"),
        None,
        &store,
        &llm,
    )
    .await;

    assert_eq!(response.exit_code, 0);
    assert_eq!(response.stdout, "main-response\n");
    assert!(response.stderr.is_empty());
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
                    tools: vec![],
                })
                .unwrap()
            },
        ));

    assert_eq!(config.provider, Provider::Anthropic);
    assert_eq!(config.model, "claude-sonnet-4-20250514");
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
                ("COCO_TOOLS", "bash"),
            ],
            || async {
                resolve_session_config(SessionCreateCommand {
                    branch: "main".to_owned(),
                    system_prompt: "You are helpful.".to_owned(),
                    prompt: "".to_owned(),
                    temperature: Some(0.2),
                    max_tokens: Some(64),
                    tools: vec![],
                })
                .unwrap()
            },
        ));

    assert_eq!(config.tools.len(), 1);
    assert_eq!(config.tools[0].name, "bash");
}
