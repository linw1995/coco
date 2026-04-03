use coco_mem::Tool;

pub fn builtin_tool_definition(name: &str) -> Option<Tool> {
    match name {
        "bash" => Some(Tool {
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
        }),
        _ => None,
    }
}
