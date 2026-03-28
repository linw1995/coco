use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use coco_llm::Provider;

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
