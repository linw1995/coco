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
                    "tty": {
                        "type": "boolean",
                        "description": "Whether to allocate a PTY for interactive commands. Defaults to false; write_stdin is only available for tty sessions."
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
                        "type": "string",
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
            description: "Search installed skills. To run a returned skill, call `coco skill run <name>` through `exec_command`; pass `--handoff <text>` for a bounded handoff, or omit it to inherit context.".to_owned(),
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
        "load_image" => Some(Tool {
            name: "load_image".to_owned(),
            description: "Load an image into model context only when the task depends on visual content. Supports local workspace paths and remote image URLs.".to_owned(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "source": {
                        "type": "string",
                        "enum": ["local_path", "url"],
                        "description": "Where to load the image from."
                    },
                    "path": {
                        "type": "string",
                        "description": "Local image path for source=local_path. Relative paths resolve under the configured workspace."
                    },
                    "url": {
                        "type": "string",
                        "description": "Remote image URL for source=url."
                    },
                    "media_type": {
                        "type": "string",
                        "description": "Optional image MIME type such as image/jpeg or image/png. Required for source=url."
                    }
                },
                "oneOf": [
                    {
                        "properties": {
                            "source": { "type": "string", "const": "local_path" }
                        },
                        "required": ["source", "path"]
                    },
                    {
                        "properties": {
                            "source": { "type": "string", "const": "url" }
                        },
                        "required": ["source", "url", "media_type"]
                    }
                ],
                "additionalProperties": false
            }),
        }),
        _ => None,
    }
}
