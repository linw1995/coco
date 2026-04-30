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
        "search_skill" => Some(Tool {
            name: "search_skill".to_owned(),
            description: "Search installed skills.".to_owned(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Optional search query matched against skill name, description, and body."
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Optional maximum number of matches to return."
                    }
                },
                "additionalProperties": false
            }),
        }),
        "use_skill" => Some(Tool {
            name: "use_skill".to_owned(),
            description: "Run an installed skill on an isolated child branch.".to_owned(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "The exact skill name returned by search_skill."
                    }
                },
                "required": ["name"],
                "additionalProperties": false
            }),
        }),
        _ => None,
    }
}
