use clap::ValueEnum;
use coco_llm::Provider;
use coco_mem::Tool;

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
