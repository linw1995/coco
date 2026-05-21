use clap::{Args, Subcommand};
use coco_mem::MergeParent;

use super::{CliSessionRole, CliTool};

#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true, subcommand_negates_reqs = true)]
pub struct PromptCommand {
    #[command(subcommand)]
    pub command: Option<PromptSubcommand>,

    #[command(flatten)]
    pub run: PromptRunCommand,
}

#[derive(Debug, Args)]
pub struct PromptRunCommand {
    #[arg(long, env = "COCO_BRANCH", default_value = "main")]
    pub branch: String,

    #[arg(long = "async")]
    pub asynchronous: bool,

    #[arg(long)]
    pub json: bool,

    #[arg(value_name = "TEXT")]
    pub text: Vec<String>,

    #[arg(long, value_enum)]
    pub role: Option<CliSessionRole>,

    #[arg(long = "tool", value_enum, conflicts_with = "clear_tools")]
    pub tools: Vec<CliTool>,

    #[arg(long)]
    pub clear_tools: bool,

    #[arg(long, conflicts_with = "disable_coco_shim")]
    pub enable_coco_shim: bool,

    #[arg(long, conflicts_with = "enable_coco_shim")]
    pub disable_coco_shim: bool,

    #[arg(skip)]
    pub merge_parents: Vec<MergeParent>,
}

#[derive(Debug, Subcommand)]
pub enum PromptSubcommand {
    Status(PromptStatusCommand),
    #[command(name = "branch-status")]
    BranchStatus(PromptBranchStatusCommand),
    #[command(hide = true)]
    Worker(PromptWorkerCommand),
}

#[derive(Debug, Args)]
pub struct PromptStatusCommand {
    #[arg(long)]
    pub job: String,

    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct PromptBranchStatusCommand {
    #[arg(long)]
    pub job: String,

    #[arg(long)]
    pub branch: Option<String>,

    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct PromptWorkerCommand {
    #[arg(long)]
    pub job: String,

    #[arg(skip)]
    pub merge_parents: Vec<MergeParent>,
}
