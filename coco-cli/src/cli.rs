use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use coco_llm::Provider;
use coco_mem::Tool;

#[derive(Debug, Parser)]
#[command(name = "coco-cli")]
pub struct Cli {
    #[arg(long, global = true, default_value = ".coco-store")]
    pub store_path: PathBuf,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Prompt(PromptCommand),
    Session(SessionCommand),
}

#[derive(Debug, Args)]
pub struct PromptCommand {
    #[arg(long, default_value = "main")]
    pub branch: String,

    #[arg(value_name = "TEXT")]
    pub text: Vec<String>,
}

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
    #[arg(long, default_value = "main")]
    pub branch: String,

    #[arg(long)]
    pub system_prompt: String,

    #[arg(long, default_value = "")]
    pub prompt: String,

    #[arg(long)]
    pub temperature: Option<f64>,

    #[arg(long)]
    pub max_tokens: Option<u64>,

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
    #[arg(long, default_value = "main")]
    pub branch: String,
}

#[derive(Debug, Args)]
pub struct SessionRebaseCommand {
    #[arg(long, default_value = "main")]
    pub branch: String,

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
    #[arg(long, default_value = "main")]
    pub branch: String,

    #[arg(long)]
    pub target_branch: String,
}

#[derive(Debug, Args)]
pub struct SessionCloseCommand {
    #[arg(long, default_value = "main")]
    pub branch: String,

    #[arg(long, default_value = "")]
    pub target_branch: String,
}

#[derive(Debug, Args)]
pub struct SessionMergeCommand {
    #[arg(long, default_value = "main")]
    pub branch: String,

    #[arg(long)]
    pub target_branch: Option<String>,

    #[arg(long)]
    pub prompt: String,
}

#[derive(Debug, Args)]
pub struct SessionFeedbackCommand {
    #[arg(long, default_value = "main")]
    pub branch: String,

    #[arg(long)]
    pub prompt: String,

    #[arg(long)]
    pub from_ref: Option<String>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum CliProvider {
    Openai,
    Anthropic,
}

impl CliProvider {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "openai" => Some(Self::Openai),
            "anthropic" => Some(Self::Anthropic),
            _ => None,
        }
    }
}

impl From<CliProvider> for Provider {
    fn from(value: CliProvider) -> Self {
        match value {
            CliProvider::Openai => Provider::OpenAi,
            CliProvider::Anthropic => Provider::Anthropic,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum CliTool {
    Bash,
}

impl CliTool {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "bash" => Some(Self::Bash),
            _ => None,
        }
    }

    pub fn to_tool(self) -> Tool {
        match self {
            Self::Bash => Tool {
                name: "bash".to_owned(),
                description: "Run a bash command.".to_owned(),
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "command": {
                            "type": "string",
                            "description": "The bash command to execute."
                        },
                        "workdir": {
                            "type": "string",
                            "description": "Optional working directory."
                        },
                        "timeout_ms": {
                            "type": "integer",
                            "description": "Optional timeout in milliseconds."
                        }
                    },
                    "required": ["command"],
                    "additionalProperties": false
                }),
            },
        }
    }
}
