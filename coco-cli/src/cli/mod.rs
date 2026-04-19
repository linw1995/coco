use std::path::PathBuf;

use clap::{Parser, Subcommand};

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
pub use session::{SessionCommand, SessionCreateCommand, SessionRebaseCommand, SessionSubcommand};
pub use skill::{
    SkillAddCommand, SkillCommand, SkillListCommand, SkillRollbackCommand, SkillShowCommand,
    SkillSubcommand, SkillUpdateCommand,
};
pub use types::{CliProvider, CliSessionRole, CliTool};

mod prompt;
mod session;
mod skill;
mod types;

#[derive(Debug, Parser)]
#[command(name = "coco")]
pub struct Cli {
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
    Prompt(PromptCommand),
    Session(SessionCommand),
    Skill(SkillCommand),
}
