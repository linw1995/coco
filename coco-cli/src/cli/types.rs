use clap::ValueEnum;
use coco_llm::Provider;
use coco_mem::SessionRole;

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum CliProvider {
    Openai,
    Anthropic,
    Chatgpt,
}

impl CliProvider {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "openai" => Some(Self::Openai),
            "anthropic" => Some(Self::Anthropic),
            "chatgpt" => Some(Self::Chatgpt),
            _ => None,
        }
    }
}

impl From<CliProvider> for Provider {
    fn from(value: CliProvider) -> Self {
        match value {
            CliProvider::Openai => Provider::OpenAi,
            CliProvider::Anthropic => Provider::Anthropic,
            CliProvider::Chatgpt => Provider::ChatGpt,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum CliTool {
    #[value(name = "exec_command")]
    ExecCommand,
    #[value(name = "write_stdin")]
    WriteStdin,
    #[value(name = "search_skill")]
    SearchSkill,
    #[value(name = "load_image")]
    LoadImage,
}

impl CliTool {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "exec_command" => Some(Self::ExecCommand),
            "write_stdin" => Some(Self::WriteStdin),
            "search_skill" => Some(Self::SearchSkill),
            "load_image" => Some(Self::LoadImage),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::ExecCommand => "exec_command",
            Self::WriteStdin => "write_stdin",
            Self::SearchSkill => "search_skill",
            Self::LoadImage => "load_image",
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum CliSessionRole {
    Orchestrator,
    Runner,
}

impl From<CliSessionRole> for SessionRole {
    fn from(value: CliSessionRole) -> Self {
        match value {
            CliSessionRole::Orchestrator => SessionRole::Orchestrator,
            CliSessionRole::Runner => SessionRole::Runner,
        }
    }
}
