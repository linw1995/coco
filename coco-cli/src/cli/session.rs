use clap::{Args, Subcommand};

use super::{CliProvider, CliSessionRole, CliTool};

#[derive(Debug, Args)]
pub struct SessionCommand {
    #[command(subcommand)]
    pub command: SessionSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum SessionSubcommand {
    Create(SessionCreateCommand),
    Fork(SessionForkCommand),
    List,
    Get(SessionBranchCommand),
    Graph(SessionGraphCommand),
    Show(SessionShowCommand),
    Delete(SessionBranchCommand),
    Rebase(SessionRebaseCommand),
    #[command(name = "reopen")]
    Reopen(SessionBranchCommand),
    #[command(name = "pr")]
    Pr(SessionPrCommand),
    #[command(name = "close")]
    Close(SessionCloseCommand),
    #[command(name = "merge")]
    Merge(SessionMergeCommand),
    Feedback(SessionFeedbackCommand),
}

#[derive(Debug, Args)]
pub struct SessionCreateCommand {
    #[arg(long, env = "COCO_BRANCH", default_value = "main")]
    pub branch: String,

    #[arg(long, value_enum, default_value = "orchestrator")]
    pub role: CliSessionRole,

    #[arg(long)]
    pub provider_profile: Option<String>,

    #[arg(long)]
    pub system_prompt: String,

    #[arg(long, default_value = "")]
    pub prompt: String,

    #[arg(long)]
    pub temperature: Option<f64>,

    #[arg(long)]
    pub max_tokens: Option<u64>,

    #[arg(long, value_name = "JSON")]
    pub additional_params: Option<String>,

    #[arg(long = "tool", value_enum)]
    pub tools: Vec<CliTool>,
}

#[derive(Debug, Args)]
pub struct SessionForkCommand {
    #[arg(long)]
    pub branch: String,

    #[arg(long, default_value = "main")]
    pub from_ref: String,
}

#[derive(Debug, Args)]
pub struct SessionBranchCommand {
    #[arg(long, env = "COCO_BRANCH", default_value = "main")]
    pub branch: String,
}

#[derive(Debug, Args)]
pub struct SessionGraphCommand {}

#[derive(Debug, Args)]
pub struct SessionShowCommand {
    #[arg(value_name = "REF")]
    pub reference: String,

    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct SessionRebaseCommand {
    #[arg(long, env = "COCO_BRANCH", default_value = "main")]
    pub branch: String,

    #[arg(long)]
    pub preset: Option<String>,

    #[arg(long)]
    pub provider_profile: Option<String>,

    #[arg(long, value_enum)]
    pub role: Option<CliSessionRole>,

    #[arg(long)]
    pub provider: Option<CliProvider>,

    #[arg(long)]
    pub model: Option<String>,

    #[arg(long)]
    pub system_prompt: Option<String>,

    #[arg(long)]
    pub prompt: Option<String>,

    #[arg(long, conflicts_with = "clear_temperature")]
    pub temperature: Option<f64>,

    #[arg(long)]
    pub clear_temperature: bool,

    #[arg(long, conflicts_with = "clear_max_tokens")]
    pub max_tokens: Option<u64>,

    #[arg(long)]
    pub clear_max_tokens: bool,

    #[arg(long = "tool", value_enum, conflicts_with = "clear_tools")]
    pub tools: Vec<CliTool>,

    #[arg(long)]
    pub clear_tools: bool,
}

#[derive(Debug, Args)]
pub struct SessionPrCommand {
    #[arg(long, env = "COCO_BRANCH", default_value = "main")]
    pub branch: String,

    #[arg(long)]
    pub target_branch: String,
}

#[derive(Debug, Args)]
pub struct SessionCloseCommand {
    #[arg(long, env = "COCO_BRANCH", default_value = "main")]
    pub branch: String,

    #[arg(long, default_value = "")]
    pub target_branch: String,
}

#[derive(Debug, Args)]
pub struct SessionMergeCommand {
    #[arg(long, env = "COCO_BRANCH", default_value = "main")]
    pub branch: String,

    #[arg(long)]
    pub target_branch: Option<String>,

    #[arg(long)]
    pub prompt: String,
}

#[derive(Debug, Args)]
pub struct SessionFeedbackCommand {
    #[arg(long, env = "COCO_BRANCH", default_value = "main")]
    pub branch: String,

    #[arg(long)]
    pub prompt: String,

    #[arg(long)]
    pub from_ref: Option<String>,
}
