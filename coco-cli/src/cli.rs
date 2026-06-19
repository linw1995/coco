use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::COCO_DAEMON_SOCKET_ENV;

pub use daemon::{
    DaemonCommand, DaemonProfileCommand, DaemonProfileGraphCommand, DaemonProfileSubcommand,
    DaemonSubcommand,
};
pub use preset::{
    PresetCommand, PresetNameCommand, PresetRollbackCommand, PresetSetCommand, PresetSubcommand,
};
pub use prompt::{
    PromptCommand, PromptListCommand, PromptRunCommand, PromptStatusCommand, PromptSubcommand,
    PromptWorkerCommand,
};
pub use session::{SessionBranchCommand, SessionGraphCommand, SessionShowCommand};
#[cfg(test)]
pub use session::{
    SessionCloseCommand, SessionFeedbackCommand, SessionForkCommand, SessionMergeCommand,
    SessionPrCommand,
};
pub use session::{
    SessionCommand, SessionCreateCommand, SessionHandoffCommand, SessionListCommand,
    SessionRebaseCommand, SessionSubcommand,
};
pub use skill::{
    SkillAddCommand, SkillCommand, SkillListCommand, SkillRollbackCommand, SkillRunCommand,
    SkillShowCommand, SkillSubcommand, SkillUpdateCommand,
};
pub use types::{CliProvider, CliSessionRole, CliTool};

mod daemon;
mod preset;
mod prompt;
mod session;
mod skill;
mod types;

#[derive(Debug, Parser)]
#[command(name = "coco")]
pub struct Cli {
    // The daemon socket is OS-scoped rather than project-scoped.
    // Use it only when the caller explicitly wants to talk to a long-lived
    // host-level daemon; project-level execution should stay local.
    #[arg(long, global = true, env = COCO_DAEMON_SOCKET_ENV)]
    pub daemon_socket: Option<PathBuf>,

    #[arg(
        long,
        global = true,
        env = "COCO_STORE_PATH",
        default_value = ".coco-store"
    )]
    pub store_path: PathBuf,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Preset(PresetCommand),
    #[command(alias = "prompt")]
    Job(PromptCommand),
    Session(SessionCommand),
    Skill(SkillCommand),
    Daemon(DaemonCommand),
}

impl Command {
    // Clap exposes the matched subcommand name through ArgMatches and the
    // generated Command metadata, but the typed enum value does not retain
    // that token after parsing. Keep the mapping explicit and test it against
    // clap's generated metadata.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Preset(_) => "preset",
            Self::Job(_) => "job",
            Self::Session(_) => "session",
            Self::Skill(_) => "skill",
            Self::Daemon(_) => "daemon",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Cli;
    use clap::{CommandFactory, Parser};

    #[test]
    fn command_name_matches_clap_subcommand_names() {
        let cases = [
            (["coco", "preset", "list"].as_slice(), "preset"),
            (
                ["coco", "job", "status", "--job", "job-1"].as_slice(),
                "job",
            ),
            (["coco", "session", "list"].as_slice(), "session"),
            (["coco", "skill", "list"].as_slice(), "skill"),
            (
                ["coco", "daemon", "serve", "--no-console"].as_slice(),
                "daemon",
            ),
        ];
        let expected_names = cases
            .iter()
            .map(|(_, expected)| *expected)
            .collect::<Vec<_>>();
        let clap_command = Cli::command();
        let clap_names = clap_command
            .get_subcommands()
            .map(|command| command.get_name())
            .collect::<Vec<_>>();

        assert_eq!(clap_names, expected_names);

        for (args, expected) in cases {
            let cli = Cli::try_parse_from(args).expect("command name test args should parse");
            assert_eq!(cli.command.name(), expected);
        }
    }

    #[test]
    fn prompt_alias_uses_job_command_name() {
        let cli = Cli::try_parse_from(["coco", "prompt", "hello"])
            .expect("prompt alias should parse as job");

        assert_eq!(cli.command.name(), "job");
    }
}
