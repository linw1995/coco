use std::fmt;
use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::COCO_DAEMON_SOCKET_ENV;

pub use daemon::{DaemonCommand, DaemonSubcommand};
pub use preset::{
    PresetCommand, PresetNameCommand, PresetRollbackCommand, PresetSetCommand, PresetSubcommand,
};
pub use prompt::{
    PromptBranchStatusCommand, PromptCommand, PromptRunCommand, PromptStatusCommand,
    PromptSubcommand, PromptWorkerCommand,
};
pub use session::{SessionBranchCommand, SessionGraphCommand, SessionShowCommand};
#[cfg(test)]
pub use session::{
    SessionCloseCommand, SessionFeedbackCommand, SessionForkCommand, SessionMergeCommand,
    SessionPrCommand,
};
pub use session::{
    SessionCommand, SessionCreateCommand, SessionListCommand, SessionRebaseCommand,
    SessionSubcommand,
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
    Prompt(PromptCommand),
    Session(SessionCommand),
    Skill(SkillCommand),
    Daemon(DaemonCommand),
}

impl Command {
    fn display_name(&self) -> &'static str {
        match self {
            Self::Preset(_) => "preset",
            Self::Prompt(_) => "prompt",
            Self::Session(_) => "session",
            Self::Skill(_) => "skill",
            Self::Daemon(_) => "daemon",
        }
    }
}

impl fmt::Display for Command {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.display_name())
    }
}

#[cfg(test)]
mod tests {
    use super::Cli;
    use clap::Parser;

    #[test]
    fn command_display_returns_top_level_cli_names() {
        let cases = [
            (["coco", "preset", "list"].as_slice(), "preset"),
            (["coco", "prompt", "hello"].as_slice(), "prompt"),
            (["coco", "session", "list"].as_slice(), "session"),
            (["coco", "skill", "list"].as_slice(), "skill"),
            (
                ["coco", "daemon", "serve", "--no-console"].as_slice(),
                "daemon",
            ),
        ];

        for (args, expected) in cases {
            let cli = Cli::try_parse_from(args).expect("command display test args should parse");
            assert_eq!(cli.command.to_string(), expected);
        }
    }
}
