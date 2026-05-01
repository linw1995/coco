use coco_mem::Tool;

pub fn builtin_tool_definition(name: &str) -> Option<Tool> {
    match name {
        "exec_command" => Some(Tool {
            name: "exec_command".to_owned(),
            description:
                "Runs a command, returning output or a session_id for ongoing interaction."
                    .to_owned(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "cmd": {
                        "type": "string",
                        "description": "Shell command to execute."
                    },
                    "workdir": {
                        "type": "string",
                        "description": "Optional working directory to run the command in. Relative paths resolve under the configured workspace; defaults to the workspace root."
                    },
                    "shell": {
                        "type": "string",
                        "description": "Optional shell binary to launch. Defaults to the user's SHELL, or bash when SHELL is unset."
                    },
                    "yield_time_ms": {
                        "type": "integer",
                        "description": "How long to wait in milliseconds for output before yielding. This is a time slice, not a process timeout; defaults to 1000."
                    },
                    "max_output_tokens": {
                        "type": "integer",
                        "description": "Maximum approximate number of tokens to return. Excess output is truncated from the front."
                    }
                },
                "required": ["cmd"],
                "additionalProperties": false
            }),
        }),
        "write_stdin" => Some(Tool {
            name: "write_stdin".to_owned(),
            description:
                "Writes characters to an existing exec_command session and returns recent output."
                    .to_owned(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "session_id": {
                        "type": "integer",
                        "description": "Identifier of the running exec_command session."
                    },
                    "chars": {
                        "type": "string",
                        "description": "Bytes to write to stdin. May be empty to poll for recent output."
                    },
                    "yield_time_ms": {
                        "type": "integer",
                        "description": "How long to wait in milliseconds for output before yielding. Defaults to 1000."
                    },
                    "max_output_tokens": {
                        "type": "integer",
                        "description": "Maximum approximate number of tokens to return. Excess output is truncated from the front."
                    }
                },
                "required": ["session_id"],
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
