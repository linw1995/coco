use std::path::PathBuf;

use clap::{Parser, Subcommand};

pub use prompt::PromptCommand;
#[cfg(test)]
pub use session::{
    SessionBranchCommand, SessionCloseCommand, SessionFeedbackCommand, SessionForkCommand,
    SessionGraphCommand, SessionMergeCommand, SessionPrCommand, SessionShowCommand,
};
pub use session::{SessionCommand, SessionCreateCommand, SessionRebaseCommand, SessionSubcommand};
pub use types::{CliProvider, CliTool};

mod prompt;
mod session;
mod types;

#[derive(Debug, Parser)]
#[command(name = "coco-cli")]
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
}
