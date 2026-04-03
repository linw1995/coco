use clap::ValueEnum;
use coco_llm::Provider;

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
    #[value(name = "bash")]
    Bash,
    #[value(name = "search_skill")]
    SearchSkill,
    #[value(name = "use_skill")]
    UseSkill,
}

impl CliTool {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "bash" => Some(Self::Bash),
            "search_skill" => Some(Self::SearchSkill),
            "use_skill" => Some(Self::UseSkill),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Bash => "bash",
            Self::SearchSkill => "search_skill",
            Self::UseSkill => "use_skill",
        }
    }
}
