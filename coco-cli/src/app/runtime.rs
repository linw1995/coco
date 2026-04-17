use std::io::Read;
use std::sync::Arc;

use clap::{Args, Parser, Subcommand};
use coco_llm::{CocoCliRuntimeResponse, CompletionBackend, LlmService};
use coco_mem::{FsStore, SessionRole};

use super::{prompt::run_prompt_command, session::run_session_command};
use crate::{
    Cli, Result,
    cli::{
        Command, PromptBranchStatusCommand, PromptCommand, PromptRunCommand, PromptStatusCommand,
        PromptSubcommand, SessionBranchCommand, SessionCommand, SessionGraphCommand,
        SessionShowCommand, SessionSubcommand,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ForwardedRuntimeScope {
    Orchestrator,
    Runner,
}

#[derive(Debug, Parser)]
#[command(name = "coco")]
struct ForwardedCli {
    #[command(subcommand)]
    command: Command,
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
                    text: vec![],
                },
            }),
            RunnerCommand::Session(command) => Command::Session(SessionCommand {
                command: match command.command {
                    RunnerSessionSubcommand::List => SessionSubcommand::List,
                    RunnerSessionSubcommand::Get(command) => SessionSubcommand::Get(command),
                    RunnerSessionSubcommand::Graph(command) => SessionSubcommand::Graph(command),
                    RunnerSessionSubcommand::Show(command) => SessionSubcommand::Show(command),
                },
            }),
        };

        Cli {
            store_path: default_forwarded_store_path(),
            command,
        }
    }
}

impl ForwardedCli {
    fn into_cli(self) -> Cli {
        Cli {
            store_path: default_forwarded_store_path(),
            command: self.command,
        }
    }
}

pub async fn run_with_services<B, R>(
    cli: Cli,
    reader: &mut R,
    shared_store: &FsStore,
    llm: &Arc<LlmService<B, FsStore>>,
    forwarded_runtime: bool,
) -> Result<Option<String>>
where
    B: CompletionBackend + 'static,
    R: Read,
{
    match cli.command {
        Command::Prompt(command) => {
            run_prompt_command(command, reader, shared_store, llm, forwarded_runtime).await
        }
        Command::Session(command) => run_session_command(command, shared_store, llm).await,
    }
}

pub async fn run_forwarded_with_services<B>(
    args: &[String],
    stdin: &[u8],
    branch_env: Option<&str>,
    session_role: Option<SessionRole>,
    store_path_env: Option<&str>,
    shared_store: &FsStore,
    llm: &Arc<LlmService<B, FsStore>>,
) -> CocoCliRuntimeResponse
where
    B: CompletionBackend + 'static,
{
    let scope = forwarded_runtime_scope(session_role);
    if contains_store_path_flag(args) {
        return unsupported_store_path_response();
    }

    let argv = std::iter::once("coco".to_owned())
        .chain(args.iter().cloned())
        .collect::<Vec<_>>();
    let mut cli = match parse_forwarded_cli(&argv, scope) {
        Ok(cli) => cli,
        Err(response) => return response,
    };

    apply_forwarded_defaults(&mut cli, args, branch_env, store_path_env);

    match run_with_services(
        cli,
        &mut std::io::Cursor::new(stdin),
        shared_store,
        llm,
        true,
    )
    .await
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
        Err(error) => CocoCliRuntimeResponse {
            exit_code: 1,
            stdout: String::new(),
            stderr: format!("{error}\n"),
        },
    }
}

fn forwarded_runtime_scope(session_role: Option<SessionRole>) -> ForwardedRuntimeScope {
    match session_role {
        Some(SessionRole::Runner) => ForwardedRuntimeScope::Runner,
        Some(SessionRole::Orchestrator) | None => ForwardedRuntimeScope::Orchestrator,
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
    branch_env: Option<&str>,
    store_path_env: Option<&str>,
) {
    if let Some(store_path_env) = store_path_env {
        cli.store_path = std::path::PathBuf::from(store_path_env);
    }

    if has_explicit_flag(args, "branch") {
        return;
    }
    let Some(branch_env) = branch_env else {
        return;
    };
    let branch = branch_env.to_owned();

    match &mut cli.command {
        Command::Prompt(command) => {
            if command.command.is_none() {
                command.run.branch = branch;
            }
        }
        Command::Session(command) => match &mut command.command {
            SessionSubcommand::Create(command) => command.branch = branch,
            SessionSubcommand::Fork(_) => {}
            SessionSubcommand::List => {}
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
    }
}
