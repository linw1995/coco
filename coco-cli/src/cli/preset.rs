use clap::{Args, Subcommand};

use super::{CliProvider, CliSessionRole, CliTool};

#[derive(Debug, Args)]
pub struct PresetCommand {
    #[command(subcommand)]
    pub command: PresetSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum PresetSubcommand {
    Set(PresetSetCommand),
    List,
    Show(PresetNameCommand),
    Rollback(PresetRollbackCommand),
    Delete(PresetNameCommand),
}

#[derive(Debug, Args)]
pub struct PresetSetCommand {
    #[arg(long)]
    pub name: String,

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

    #[arg(
        long = "additional-params",
        value_name = "JSON",
        conflicts_with = "clear_additional_params"
    )]
    pub additional_params: Option<String>,

    #[arg(long)]
    pub clear_additional_params: bool,

    #[arg(long, conflicts_with = "disable_coco_shim")]
    pub enable_coco_shim: bool,

    #[arg(long, conflicts_with = "enable_coco_shim")]
    pub disable_coco_shim: bool,
}

#[derive(Debug, Args)]
pub struct PresetNameCommand {
    #[arg(long)]
    pub name: String,
}

#[derive(Debug, Args)]
pub struct PresetRollbackCommand {
    #[arg(long)]
    pub name: String,

    #[arg(long)]
    pub to_version: u64,
}
