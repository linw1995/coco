use std::path::Path;

use snafu::prelude::*;

use crate::{
    Result,
    cli::DaemonSubcommand,
    cli::{Command, PresetSubcommand, PromptSubcommand, SessionSubcommand, SkillSubcommand},
    error::StoreSnafu,
};
use coco_mem::PersistentStore;

#[cfg(test)]
pub fn open_store(path: &Path) -> Result<PersistentStore> {
    PersistentStore::open(path).context(StoreSnafu)
}

pub fn open_store_for_command(path: &Path, command: &Command) -> Result<PersistentStore> {
    if command_is_read_only(command) && path.exists() {
        return PersistentStore::open_read_only_or_upgrade_schema(path).context(StoreSnafu);
    }

    PersistentStore::open(path).context(StoreSnafu)
}

fn command_is_read_only(command: &Command) -> bool {
    match command {
        Command::Preset(command) => matches!(
            &command.command,
            PresetSubcommand::List(_) | PresetSubcommand::Show(_)
        ),
        Command::Job(command) => matches!(
            &command.command,
            Some(PromptSubcommand::List(_)) | Some(PromptSubcommand::Status(_))
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
        Command::Daemon(command) => matches!(&command.command, DaemonSubcommand::Profile(_)),
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::command_is_read_only;
    use crate::cli::Cli;

    #[test]
    fn daemon_profile_graph_is_read_only() {
        let cli = Cli::parse_from(["coco", "daemon", "profile", "graph"]);

        assert!(command_is_read_only(&cli.command));
    }

    #[test]
    fn daemon_serve_is_not_read_only() {
        let cli = Cli::parse_from(["coco", "daemon", "serve"]);

        assert!(!command_is_read_only(&cli.command));
    }
}
