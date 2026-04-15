use clap::{Args, Subcommand};

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

    #[arg(value_name = "TEXT")]
    pub text: Vec<String>,
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
}

#[derive(Debug, Args)]
pub struct PromptBranchStatusCommand {
    #[arg(long)]
    pub job: String,

    #[arg(long)]
    pub branch: Option<String>,
}

#[derive(Debug, Args)]
pub struct PromptWorkerCommand {
    #[arg(long)]
    pub job: String,
}
