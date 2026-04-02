use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use coco_core::{ConversationEngine, CoreService, FixedBranchResolver, InboundMessage};
use coco_llm::{CocoCliRuntimeResponse, CompletionBackend, LlmService};
use coco_mem::FsStore;
use snafu::prelude::*;

use super::{prompt::resolve_prompt_input, session::run_session_command};
use crate::{
    Cli, Result,
    cli::{Command, SessionSubcommand},
    error::CoreSnafu,
};

pub async fn run_with_services<B, R>(
    cli: Cli,
    reader: &mut R,
    shared_store: &FsStore,
    llm: &Arc<LlmService<B, FsStore>>,
) -> Result<Option<String>>
where
    B: CompletionBackend,
    R: Read,
{
    match cli.command {
        Command::Prompt(command) => {
            let input = resolve_prompt_input(&command, reader)?;
            let service = CoreService::new(
                FixedBranchResolver::new(command.branch),
                ConversationEngine::new(llm.clone()),
            );
            let response = service
                .handle_message(InboundMessage::cli("cli", "cli", input))
                .await
                .context(CoreSnafu)?;
            Ok(Some(response.text))
        }
        Command::Session(command) => run_session_command(command, shared_store, llm).await,
    }
}

pub async fn run_forwarded_with_services<B>(
    args: &[String],
    stdin: &[u8],
    branch_env: Option<&str>,
    store_path_env: Option<&str>,
    shared_store: &FsStore,
    llm: &Arc<LlmService<B, FsStore>>,
) -> CocoCliRuntimeResponse
where
    B: CompletionBackend,
{
    let argv = std::iter::once("coco-cli".to_owned())
        .chain(args.iter().cloned())
        .collect::<Vec<_>>();
    let mut cli = match Cli::try_parse_from(argv) {
        Ok(cli) => cli,
        Err(error) => {
            let output = error.to_string();
            return if error.use_stderr() {
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
            };
        }
    };

    apply_forwarded_defaults(&mut cli, args, branch_env, store_path_env);

    match run_with_services(cli, &mut std::io::Cursor::new(stdin), shared_store, llm).await {
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
    if !has_explicit_flag(args, "store-path")
        && let Some(store_path_env) = store_path_env
    {
        cli.store_path = PathBuf::from(store_path_env);
    }

    if has_explicit_flag(args, "branch") {
        return;
    }
    let Some(branch_env) = branch_env else {
        return;
    };
    let branch = branch_env.to_owned();

    match &mut cli.command {
        Command::Prompt(command) => command.branch = branch,
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
