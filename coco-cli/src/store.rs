use std::path::Path;

use snafu::prelude::*;

use crate::{
    Result,
    cli::{Command, PresetSubcommand, PromptSubcommand, SessionSubcommand, SkillSubcommand},
    error::StoreSnafu,
};
use coco_mem::FsStore;

#[cfg(test)]
pub fn open_store(path: &Path) -> Result<FsStore> {
    FsStore::open(path).context(StoreSnafu)
}

pub fn open_store_for_command(path: &Path, command: &Command) -> Result<FsStore> {
    if command_is_read_only(command) && path.exists() {
        return FsStore::open_read_only(path).context(StoreSnafu);
    }

    FsStore::open(path).context(StoreSnafu)
}

fn command_is_read_only(command: &Command) -> bool {
    match command {
        Command::Preset(command) => matches!(
            &command.command,
            PresetSubcommand::List(_) | PresetSubcommand::Show(_)
        ),
        Command::Mq(_) => false,
        Command::Prompt(command) => matches!(
            &command.command,
            Some(PromptSubcommand::Status(_)) | Some(PromptSubcommand::BranchStatus(_))
        ),
        Command::Session(command) => matches!(
            &command.command,
            SessionSubcommand::List(_)
                | SessionSubcommand::Get(_)
                | SessionSubcommand::Graph(_)
                | SessionSubcommand::Show(_)
        ),
        Command::Skill(command) => matches!(
            &command.command,
            SkillSubcommand::List(_) | SkillSubcommand::Show(_)
        ),
        Command::Daemon(_) => false,
    }
}
