use std::path::PathBuf;

use clap::{Args, Subcommand};

use super::CliSessionRole;

#[derive(Debug, Args)]
pub struct SkillCommand {
    #[command(subcommand)]
    pub command: SkillSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum SkillSubcommand {
    Add(SkillAddCommand),
    Update(SkillUpdateCommand),
    Rollback(SkillRollbackCommand),
    List(SkillListCommand),
    Show(SkillShowCommand),
}

#[derive(Debug, Args)]
pub struct SkillAddCommand {
    #[arg(long, value_enum)]
    pub role: CliSessionRole,

    #[arg(long)]
    pub name: String,

    #[arg(long)]
    pub description: String,

    #[arg(long)]
    pub file: PathBuf,

    #[arg(long)]
    pub enable_coco_shim: bool,
}

#[derive(Debug, Args)]
pub struct SkillUpdateCommand {
    #[arg(long, value_enum)]
    pub role: CliSessionRole,

    #[arg(long)]
    pub name: String,

    #[arg(long)]
    pub description: Option<String>,

    #[arg(long)]
    pub file: Option<PathBuf>,

    #[arg(long, conflicts_with = "disable_coco_shim")]
    pub enable_coco_shim: bool,

    #[arg(long, conflicts_with = "enable_coco_shim")]
    pub disable_coco_shim: bool,
}

#[derive(Debug, Args)]
pub struct SkillRollbackCommand {
    #[arg(long, value_enum)]
    pub role: CliSessionRole,

    #[arg(long)]
    pub name: String,

    #[arg(long)]
    pub to_version: u64,
}

#[derive(Debug, Args)]
pub struct SkillListCommand {
    #[arg(long, value_enum)]
    pub role: Option<CliSessionRole>,

    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct SkillShowCommand {
    #[arg(long, value_enum)]
    pub role: CliSessionRole,

    #[arg(long)]
    pub name: String,

    #[arg(long)]
    pub json: bool,
}
