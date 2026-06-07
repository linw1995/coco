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
    Run(SkillRunCommand),
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

    #[arg(long = "script")]
    pub scripts: Vec<PathBuf>,

    #[arg(long)]
    pub script_dir: Option<PathBuf>,

    #[arg(long)]
    pub enable_coco_shim: bool,

    #[arg(long)]
    pub json: bool,
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

    #[arg(long = "script", conflicts_with = "clear_scripts")]
    pub scripts: Vec<PathBuf>,

    #[arg(long, conflicts_with = "clear_scripts")]
    pub script_dir: Option<PathBuf>,

    #[arg(long, conflicts_with_all = ["scripts", "script_dir"])]
    pub clear_scripts: bool,

    #[arg(long, conflicts_with = "disable_coco_shim")]
    pub enable_coco_shim: bool,

    #[arg(long, conflicts_with = "enable_coco_shim")]
    pub disable_coco_shim: bool,

    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct SkillRollbackCommand {
    #[arg(long, value_enum)]
    pub role: CliSessionRole,

    #[arg(long)]
    pub name: String,

    #[arg(long)]
    pub to_version: u64,

    #[arg(long)]
    pub json: bool,
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

#[derive(Debug, Args)]
pub struct SkillRunCommand {
    pub name: String,

    #[arg(long, required = true)]
    pub handoff: String,

    #[arg(long, hide = true)]
    pub parent_tool_use_id: Option<String>,

    #[arg(long, hide = true)]
    pub branch: Option<String>,

    #[arg(long)]
    pub json: bool,
}
