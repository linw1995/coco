use clap::{Args, Subcommand};

use super::{CliSessionRole, CliTool};

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
    pub role: CliSessionRole,

    #[arg(long)]
    pub provider_profile: String,

    #[arg(long)]
    pub model: Option<String>,

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

    #[arg(long = "additional-params", value_name = "JSON")]
    pub additional_params: Option<String>,

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
