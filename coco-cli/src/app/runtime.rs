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
        SessionGraphCommand, SessionShowCommand, SessionSubcommand, SkillCommand, SkillSubcommand,
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
                    enable_coco_shim: false,
                    disable_coco_shim: false,
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
        Command::Skill(command) => {
            run_skill_command(command, services.shared_store, services.llm).await
        }
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
        stderr: "coco command \"--store-path\" is not available in unified exec runtime\n"
            .to_owned(),
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
    apply_forwarded_store_path(cli, store_path_env);
    apply_forwarded_orchestrator_defaults(cli, scope, branch_env, parent_tool_use_id_env);
    apply_forwarded_branch_default(cli, args, branch_env);
}

fn apply_forwarded_store_path(cli: &mut Cli, store_path_env: Option<&str>) {
    if let Some(store_path_env) = store_path_env {
        cli.store_path = std::path::PathBuf::from(store_path_env);
    }
}

fn apply_forwarded_orchestrator_defaults(
    cli: &mut Cli,
    scope: ForwardedRuntimeScope,
    branch_env: Option<&str>,
    parent_tool_use_id_env: Option<&str>,
) {
    if scope != ForwardedRuntimeScope::Orchestrator {
        return;
    }

    let Some(parent_tool_use_id) = parent_tool_use_id_env else {
        return;
    };
    let parent_tool_use_id = parent_tool_use_id.to_owned();
    apply_forwarded_shadow_parent(cli, parent_tool_use_id.clone());
    apply_forwarded_skill_parent(cli, parent_tool_use_id, branch_env.map(str::to_owned));
}

fn apply_forwarded_branch_default(cli: &mut Cli, args: &[String], branch_env: Option<&str>) {
    let Some(branch) = forwarded_branch_default(args, branch_env) else {
        return;
    };

    if let Some(slot) = forwarded_branch_slot(cli) {
        *slot = branch.to_owned();
    }
}

fn forwarded_branch_default<'a>(args: &[String], branch_env: Option<&'a str>) -> Option<&'a str> {
    if has_explicit_flag(args, "branch") {
        None
    } else {
        branch_env
    }
}

fn forwarded_branch_slot(cli: &mut Cli) -> Option<&mut String> {
    match &mut cli.command {
        Command::Preset(_) | Command::Skill(_) | Command::Daemon(_) => None,
        Command::Prompt(command) => forwarded_prompt_branch_slot(command),
        Command::Session(command) => forwarded_session_branch_slot(command),
    }
}

fn forwarded_prompt_branch_slot(command: &mut PromptCommand) -> Option<&mut String> {
    command.command.is_none().then_some(&mut command.run.branch)
}

fn forwarded_session_branch_slot(command: &mut SessionCommand) -> Option<&mut String> {
    let slot = forwarded_session_branch_slot_fn(&command.command)?;
    slot(&mut command.command)
}

type ForwardedSessionBranchSlotFn = for<'a> fn(&'a mut SessionSubcommand) -> Option<&'a mut String>;

fn forwarded_session_branch_slot_fn(
    command: &SessionSubcommand,
) -> Option<ForwardedSessionBranchSlotFn> {
    forwarded_session_primary_branch_slot_fn(command)
        .or_else(|| forwarded_session_secondary_branch_slot_fn(command))
}

fn forwarded_session_primary_branch_slot_fn(
    command: &SessionSubcommand,
) -> Option<ForwardedSessionBranchSlotFn> {
    if matches!(
        command,
        SessionSubcommand::Create(_)
            | SessionSubcommand::Get(_)
            | SessionSubcommand::Delete(_)
            | SessionSubcommand::Reopen(_)
    ) {
        return Some(forwarded_session_basic_branch_slot);
    }
    if matches!(
        command,
        SessionSubcommand::Rebase(_) | SessionSubcommand::Handoff(_)
    ) {
        return Some(forwarded_session_rebase_branch_slot);
    }
    None
}

fn forwarded_session_secondary_branch_slot_fn(
    command: &SessionSubcommand,
) -> Option<ForwardedSessionBranchSlotFn> {
    if matches!(
        command,
        SessionSubcommand::Pr(_) | SessionSubcommand::Close(_)
    ) {
        return Some(forwarded_session_pr_branch_slot);
    }
    if matches!(
        command,
        SessionSubcommand::Merge(_) | SessionSubcommand::Feedback(_)
    ) {
        return Some(forwarded_session_merge_branch_slot);
    }
    None
}

fn forwarded_session_basic_branch_slot(command: &mut SessionSubcommand) -> Option<&mut String> {
    match command {
        SessionSubcommand::Create(command) => Some(&mut command.branch),
        SessionSubcommand::Get(command)
        | SessionSubcommand::Delete(command)
        | SessionSubcommand::Reopen(command) => Some(&mut command.branch),
        SessionSubcommand::Fork(_)
        | SessionSubcommand::List(_)
        | SessionSubcommand::Graph(_)
        | SessionSubcommand::Show(_)
        | SessionSubcommand::Rebase(_)
        | SessionSubcommand::Handoff(_)
        | SessionSubcommand::Pr(_)
        | SessionSubcommand::Close(_)
        | SessionSubcommand::Merge(_)
        | SessionSubcommand::Feedback(_) => None,
    }
}

fn forwarded_session_rebase_branch_slot(command: &mut SessionSubcommand) -> Option<&mut String> {
    match command {
        SessionSubcommand::Rebase(command) | SessionSubcommand::Handoff(command) => {
            Some(&mut command.branch)
        }
        _ => None,
    }
}

fn forwarded_session_pr_branch_slot(command: &mut SessionSubcommand) -> Option<&mut String> {
    match command {
        SessionSubcommand::Pr(command) => Some(&mut command.branch),
        SessionSubcommand::Close(command) => Some(&mut command.branch),
        _ => None,
    }
}

fn forwarded_session_merge_branch_slot(command: &mut SessionSubcommand) -> Option<&mut String> {
    match command {
        SessionSubcommand::Merge(command) => Some(&mut command.branch),
        SessionSubcommand::Feedback(command) => Some(&mut command.branch),
        _ => None,
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

fn apply_forwarded_skill_parent(cli: &mut Cli, parent_tool_use_id: String, branch: Option<String>) {
    if let Command::Skill(command) = &mut cli.command
        && let SkillSubcommand::Run(command) = &mut command.command
    {
        if command.parent_tool_use_id.is_none() {
            command.parent_tool_use_id = Some(parent_tool_use_id);
        }
        if command.branch.is_none() {
            command.branch = branch;
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{ForwardedRuntimeScope, command_name, parse_forwarded_cli};
    use crate::cli::{Cli, Command, PromptSubcommand, SessionSubcommand};

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    #[test]
    fn command_name_covers_all_cli_variants() {
        let cases = [
            (["coco", "preset", "list"].as_slice(), "preset"),
            (
                ["coco", "prompt", "status", "--job", "job-1"].as_slice(),
                "prompt",
            ),
            (["coco", "session", "list"].as_slice(), "session"),
            (["coco", "skill", "list"].as_slice(), "skill"),
            (
                ["coco", "daemon", "serve", "--no-console"].as_slice(),
                "daemon",
            ),
        ];

        for (argv, expected) in cases {
            let cli = Cli::try_parse_from(argv).unwrap();
            assert_eq!(command_name(&cli.command), expected);
        }
    }

    #[test]
    fn runner_cli_maps_prompt_status_commands() {
        let cli = parse_forwarded_cli(
            &strings(&["coco", "prompt", "status", "--job", "job-1", "--json"]),
            ForwardedRuntimeScope::Runner,
        )
        .unwrap();

        let Command::Prompt(command) = cli.command else {
            panic!("expected prompt command");
        };
        let Some(PromptSubcommand::Status(status)) = command.command else {
            panic!("expected prompt status command");
        };
        assert_eq!(status.job, "job-1");
        assert!(status.json);
        assert_eq!(command.run.branch, "main");
        assert!(command.run.text.is_empty());

        let cli = parse_forwarded_cli(
            &strings(&[
                "coco",
                "prompt",
                "branch-status",
                "--job",
                "job-2",
                "--branch",
                "draft",
                "--json",
            ]),
            ForwardedRuntimeScope::Runner,
        )
        .unwrap();

        let Command::Prompt(command) = cli.command else {
            panic!("expected prompt command");
        };
        let Some(PromptSubcommand::BranchStatus(status)) = command.command else {
            panic!("expected prompt branch-status command");
        };
        assert_eq!(status.job, "job-2");
        assert_eq!(status.branch.as_deref(), Some("draft"));
        assert!(status.json);
        assert_eq!(command.run.branch, "main");
        assert!(command.run.text.is_empty());
    }

    #[test]
    fn runner_cli_maps_session_commands() {
        let cli = parse_forwarded_cli(
            &strings(&["coco", "session", "list"]),
            ForwardedRuntimeScope::Runner,
        )
        .unwrap();

        let Command::Session(command) = cli.command else {
            panic!("expected session command");
        };
        let SessionSubcommand::List(list) = command.command else {
            panic!("expected session list command");
        };
        assert!(!list.json);

        let cli = parse_forwarded_cli(
            &strings(&["coco", "session", "get", "--branch", "draft", "--json"]),
            ForwardedRuntimeScope::Runner,
        )
        .unwrap();

        let Command::Session(command) = cli.command else {
            panic!("expected session command");
        };
        let SessionSubcommand::Get(get) = command.command else {
            panic!("expected session get command");
        };
        assert_eq!(get.branch, "draft");
        assert!(get.json);

        let cli = parse_forwarded_cli(
            &strings(&["coco", "session", "graph", "--json", "--all"]),
            ForwardedRuntimeScope::Runner,
        )
        .unwrap();

        let Command::Session(command) = cli.command else {
            panic!("expected session command");
        };
        let SessionSubcommand::Graph(graph) = command.command else {
            panic!("expected session graph command");
        };
        assert!(graph.json);
        assert!(graph.all);

        let cli = parse_forwarded_cli(
            &strings(&["coco", "session", "show", "node-1", "--json"]),
            ForwardedRuntimeScope::Runner,
        )
        .unwrap();

        let Command::Session(command) = cli.command else {
            panic!("expected session command");
        };
        let SessionSubcommand::Show(show) = command.command else {
            panic!("expected session show command");
        };
        assert_eq!(show.reference, "node-1");
        assert!(show.json);
    }
}
