use std::io::Read;
use std::sync::Arc;

use clap::{Args, Parser, Subcommand};
use coco_core::ConversationEngine;
use coco_llm::{CocoCliRuntimeResponse, CompletionBackend, LlmService};
use coco_mem::{MergeParent, SessionRole, Store};

use super::{
    config::ProviderProfiles, daemon::run_daemon_command, preset::run_preset_command,
    prompt::run_prompt_command, session::run_session_command, skill::run_skill_command,
};
use crate::{
    Cli, Result,
    cli::{
        Command, PresetCommand, PromptBranchStatusCommand, PromptCommand, PromptRunCommand,
        PromptStatusCommand, PromptSubcommand, SessionBranchCommand, SessionCommand,
        SessionGraphCommand, SessionShowCommand, SessionSubcommand, SkillCommand,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ForwardedRuntimeScope {
    Orchestrator,
    Runner,
}

pub(crate) struct RuntimeServices<'a, B, S>
where
    B: CompletionBackend + 'static,
    S: Store + Clone + Send + Sync + 'static,
{
    pub shared_store: &'a S,
    pub llm: &'a Arc<LlmService<B, S>>,
    pub provider_profiles: &'a ProviderProfiles,
    pub shared_engine: Option<&'a Arc<ConversationEngine<B, S>>>,
}

pub(crate) struct ForwardedRuntimeInputs<'a> {
    pub args: &'a [String],
    pub stdin: &'a [u8],
    pub branch_env: Option<&'a str>,
    pub session_role: Option<SessionRole>,
    pub store_path_env: Option<&'a str>,
    pub parent_tool_use_id_env: Option<&'a str>,
}

#[derive(Debug, Parser)]
#[command(name = "coco")]
struct ForwardedCli {
    #[command(subcommand)]
    command: ForwardedCommand,
}

#[derive(Debug, Subcommand)]
enum ForwardedCommand {
    Preset(PresetCommand),
    Prompt(PromptCommand),
    Session(SessionCommand),
    Skill(SkillCommand),
}

#[derive(Debug, Parser)]
#[command(name = "coco")]
struct RunnerCli {
    #[command(subcommand)]
    command: RunnerCommand,
}

#[derive(Debug, Subcommand)]
enum RunnerCommand {
    Prompt(RunnerPromptCommand),
    Session(RunnerSessionCommand),
}

#[derive(Debug, Args)]
struct RunnerPromptCommand {
    #[command(subcommand)]
    command: RunnerPromptSubcommand,
}

#[derive(Debug, Subcommand)]
enum RunnerPromptSubcommand {
    Status(PromptStatusCommand),
    #[command(name = "branch-status")]
    BranchStatus(PromptBranchStatusCommand),
}

#[derive(Debug, Args)]
struct RunnerSessionCommand {
    #[command(subcommand)]
    command: RunnerSessionSubcommand,
}

#[derive(Debug, Subcommand)]
enum RunnerSessionSubcommand {
    List,
    Get(SessionBranchCommand),
    Graph(SessionGraphCommand),
    Show(SessionShowCommand),
}

impl RunnerCli {
    fn into_cli(self) -> Cli {
        let command = match self.command {
            RunnerCommand::Prompt(command) => Command::Prompt(PromptCommand {
                command: Some(match command.command {
                    RunnerPromptSubcommand::Status(command) => PromptSubcommand::Status(command),
                    RunnerPromptSubcommand::BranchStatus(command) => {
                        PromptSubcommand::BranchStatus(command)
                    }
                }),
                run: PromptRunCommand {
                    branch: "main".to_owned(),
                    asynchronous: false,
                    json: false,
                    text: vec![],
                    role: None,
                    tools: vec![],
                    clear_tools: false,
                    merge_parents: vec![],
                },
            }),
            RunnerCommand::Session(command) => Command::Session(SessionCommand {
                command: match command.command {
                    RunnerSessionSubcommand::List => {
                        SessionSubcommand::List(crate::cli::SessionListCommand { json: false })
                    }
                    RunnerSessionSubcommand::Get(command) => SessionSubcommand::Get(command),
                    RunnerSessionSubcommand::Graph(command) => SessionSubcommand::Graph(command),
                    RunnerSessionSubcommand::Show(command) => SessionSubcommand::Show(command),
                },
            }),
        };

        Cli {
            daemon_socket: None,
            store_path: default_forwarded_store_path(),
            command,
        }
    }
}

impl ForwardedCli {
    fn into_cli(self) -> Cli {
        Cli {
            daemon_socket: None,
            store_path: default_forwarded_store_path(),
            command: match self.command {
                ForwardedCommand::Preset(command) => Command::Preset(command),
                ForwardedCommand::Prompt(command) => Command::Prompt(command),
                ForwardedCommand::Session(command) => Command::Session(command),
                ForwardedCommand::Skill(command) => Command::Skill(command),
            },
        }
    }
}

pub async fn run_with_services<B, R, S>(
    cli: Cli,
    reader: &mut R,
    services: RuntimeServices<'_, B, S>,
    forwarded_runtime: bool,
) -> Result<Option<String>>
where
    B: CompletionBackend + 'static,
    R: Read,
    S: Store + Clone + Send + Sync + 'static,
{
    tracing::debug!(
        command = command_name(&cli.command),
        store_path = %cli.store_path.display(),
        forwarded_runtime,
        "dispatching cli command"
    );
    match cli.command {
        Command::Preset(command) => {
            run_preset_command(command, services.shared_store, services.provider_profiles).await
        }
        Command::Prompt(command) => {
            run_prompt_command(
                command,
                reader,
                services.shared_store,
                services.llm,
                services.shared_engine.map(AsRef::as_ref),
                forwarded_runtime,
            )
            .await
        }
        Command::Session(command) => {
            run_session_command(
                command,
                services.shared_store,
                services.llm,
                services.provider_profiles,
            )
            .await
        }
        Command::Skill(command) => run_skill_command(command, services.shared_store).await,
        Command::Daemon(command) => {
            run_daemon_command(
                command,
                services.shared_store,
                services.llm,
                services.provider_profiles,
                &Default::default(),
                None,
            )
            .await
        }
    }
}

pub async fn run_forwarded_with_services<B, S>(
    inputs: ForwardedRuntimeInputs<'_>,
    services: RuntimeServices<'_, B, S>,
) -> CocoCliRuntimeResponse
where
    B: CompletionBackend + 'static,
    S: Store + Clone + Send + Sync + 'static,
{
    let scope = forwarded_runtime_scope(inputs.session_role);
    tracing::debug!(
        scope = forwarded_runtime_scope_name(scope),
        arg_count = inputs.args.len(),
        stdin_bytes = inputs.stdin.len(),
        has_branch_env = inputs.branch_env.is_some(),
        has_store_path_env = inputs.store_path_env.is_some(),
        "handling forwarded runtime request"
    );
    if contains_store_path_flag(inputs.args) {
        tracing::warn!(
            scope = forwarded_runtime_scope_name(scope),
            "rejected forwarded runtime request with store path override"
        );
        return unsupported_store_path_response();
    }

    let argv = std::iter::once("coco".to_owned())
        .chain(inputs.args.iter().cloned())
        .collect::<Vec<_>>();
    let mut cli = match parse_forwarded_cli(&argv, scope) {
        Ok(cli) => cli,
        Err(response) => return response,
    };

    apply_forwarded_defaults(
        &mut cli,
        inputs.args,
        scope,
        inputs.branch_env,
        inputs.store_path_env,
        inputs.parent_tool_use_id_env,
    );

    let response =
        match run_with_services(cli, &mut std::io::Cursor::new(inputs.stdin), services, true).await
        {
            Ok(Some(output)) => CocoCliRuntimeResponse {
                exit_code: 0,
                stdout: format!("{output}\n"),
                stderr: String::new(),
            },
            Ok(None) => CocoCliRuntimeResponse {
                exit_code: 0,
                stdout: String::new(),
                stderr: String::new(),
            },
            Err(error) => {
                tracing::warn!(
                    scope = forwarded_runtime_scope_name(scope),
                    error = %error,
                    "forwarded runtime command failed"
                );
                CocoCliRuntimeResponse {
                    exit_code: 1,
                    stdout: String::new(),
                    stderr: format!("{error}\n"),
                }
            }
        };
    tracing::debug!(
        scope = forwarded_runtime_scope_name(scope),
        exit_code = response.exit_code,
        stdout_bytes = response.stdout.len(),
        stderr_bytes = response.stderr.len(),
        "forwarded runtime request completed"
    );
    response
}

fn command_name(command: &Command) -> &'static str {
    match command {
        Command::Preset(_) => "preset",
        Command::Prompt(_) => "prompt",
        Command::Session(_) => "session",
        Command::Skill(_) => "skill",
        Command::Daemon(_) => "daemon",
    }
}

fn forwarded_runtime_scope(session_role: Option<SessionRole>) -> ForwardedRuntimeScope {
    match session_role {
        Some(SessionRole::Runner) => ForwardedRuntimeScope::Runner,
        Some(SessionRole::Orchestrator) | None => ForwardedRuntimeScope::Orchestrator,
    }
}

fn forwarded_runtime_scope_name(scope: ForwardedRuntimeScope) -> &'static str {
    match scope {
        ForwardedRuntimeScope::Orchestrator => "orchestrator",
        ForwardedRuntimeScope::Runner => "runner",
    }
}

fn parse_forwarded_cli(
    argv: &[String],
    scope: ForwardedRuntimeScope,
) -> std::result::Result<Cli, CocoCliRuntimeResponse> {
    match scope {
        ForwardedRuntimeScope::Orchestrator => ForwardedCli::try_parse_from(argv.iter().cloned())
            .map(ForwardedCli::into_cli)
            .map_err(clap_error_response),
        ForwardedRuntimeScope::Runner => RunnerCli::try_parse_from(argv.iter().cloned())
            .map(RunnerCli::into_cli)
            .map_err(clap_error_response),
    }
}

fn default_forwarded_store_path() -> std::path::PathBuf {
    ".coco-store".into()
}

fn clap_error_response(error: clap::Error) -> CocoCliRuntimeResponse {
    let output = error.to_string();
    if error.use_stderr() {
        CocoCliRuntimeResponse {
            exit_code: error.exit_code(),
            stdout: String::new(),
            stderr: output,
        }
    } else {
        CocoCliRuntimeResponse {
            exit_code: error.exit_code(),
            stdout: output,
            stderr: String::new(),
        }
    }
}

fn unsupported_store_path_response() -> CocoCliRuntimeResponse {
    CocoCliRuntimeResponse {
        exit_code: 1,
        stdout: String::new(),
        stderr: "coco command \"--store-path\" is not available in bash tool runtime\n".to_owned(),
    }
}

fn contains_store_path_flag(args: &[String]) -> bool {
    args.iter()
        .any(|arg| arg == "--store-path" || arg.starts_with("--store-path="))
}

fn has_explicit_flag(args: &[String], name: &str) -> bool {
    let long = format!("--{name}");
    let long_eq = format!("--{name}=");
    args.iter()
        .any(|arg| arg == &long || arg.starts_with(&long_eq))
}

fn apply_forwarded_defaults(
    cli: &mut Cli,
    args: &[String],
    scope: ForwardedRuntimeScope,
    branch_env: Option<&str>,
    store_path_env: Option<&str>,
    parent_tool_use_id_env: Option<&str>,
) {
    if let Some(store_path_env) = store_path_env {
        cli.store_path = std::path::PathBuf::from(store_path_env);
    }

    if scope == ForwardedRuntimeScope::Orchestrator
        && let Some(parent_tool_use_id) = parent_tool_use_id_env
    {
        apply_forwarded_shadow_parent(cli, parent_tool_use_id.to_owned());
    }

    if has_explicit_flag(args, "branch") {
        return;
    }
    let Some(branch_env) = branch_env else {
        return;
    };
    let branch = branch_env.to_owned();

    match &mut cli.command {
        Command::Preset(_) => {}
        Command::Prompt(command) => {
            if command.command.is_none() {
                command.run.branch = branch;
            }
        }
        Command::Session(command) => match &mut command.command {
            SessionSubcommand::Create(command) => command.branch = branch,
            SessionSubcommand::Fork(_) => {}
            SessionSubcommand::List(_) => {}
            SessionSubcommand::Get(command) => command.branch = branch,
            SessionSubcommand::Graph(_) => {}
            SessionSubcommand::Show(_) => {}
            SessionSubcommand::Delete(command) => command.branch = branch,
            SessionSubcommand::Rebase(command) => command.branch = branch,
            SessionSubcommand::Reopen(command) => command.branch = branch,
            SessionSubcommand::Pr(command) => command.branch = branch,
            SessionSubcommand::Close(command) => command.branch = branch,
            SessionSubcommand::Merge(command) => command.branch = branch,
            SessionSubcommand::Feedback(command) => command.branch = branch,
        },
        Command::Skill(_) => {}
        Command::Daemon(_) => {}
    }
}

fn apply_forwarded_shadow_parent(cli: &mut Cli, shadow_parent: String) {
    if let Command::Prompt(command) = &mut cli.command {
        match &mut command.command {
            None => command
                .run
                .merge_parents
                .push(MergeParent::shadow(shadow_parent)),
            Some(PromptSubcommand::Worker(command)) => command
                .merge_parents
                .push(MergeParent::shadow(shadow_parent)),
            Some(PromptSubcommand::Status(_)) | Some(PromptSubcommand::BranchStatus(_)) => {}
        }
    }
}
