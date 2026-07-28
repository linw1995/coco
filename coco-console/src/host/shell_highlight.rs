use coco_types::{Kind, Node};
use tree_sitter_highlight::{HighlightConfiguration, HighlightEvent, Highlighter};

use crate::api::{ShellHighlightKind, ShellHighlightToken, ToolInputShellHighlight};

const HIGHLIGHT_NAMES: &[&str] = &[
    "comment", "constant", "embedded", "function", "keyword", "number", "operator", "property",
    "string",
];

pub fn tool_input_shell_highlights(node: &Node) -> Vec<ToolInputShellHighlight> {
    let Kind::ToolUse(items) = &node.kind else {
        return Vec::new();
    };
    let Some(configuration) = bash_highlight_configuration() else {
        return Vec::new();
    };
    let mut highlighter = Highlighter::new();
    let mut highlights = Vec::new();
    for (tool_use_index, item) in items.iter().enumerate() {
        collect_shell_highlights(
            &item.input,
            "",
            tool_use_index,
            &item.id,
            configuration,
            &mut highlighter,
            &mut highlights,
        );
    }
    highlights
}

fn bash_highlight_configuration() -> Option<&'static HighlightConfiguration> {
    static CONFIGURATION: std::sync::OnceLock<Option<HighlightConfiguration>> =
        std::sync::OnceLock::new();
    CONFIGURATION
        .get_or_init(|| {
            let mut configuration = HighlightConfiguration::new(
                tree_sitter_bash::LANGUAGE.into(),
                "bash",
                tree_sitter_bash::HIGHLIGHT_QUERY,
                "",
                "",
            )
            .ok()?;
            configuration.configure(HIGHLIGHT_NAMES);
            Some(configuration)
        })
        .as_ref()
}

fn collect_shell_highlights(
    value: &serde_json::Value,
    pointer: &str,
    tool_use_index: usize,
    tool_use_id: &str,
    configuration: &HighlightConfiguration,
    highlighter: &mut Highlighter,
    highlights: &mut Vec<ToolInputShellHighlight>,
) {
    match value {
        serde_json::Value::Object(values) => {
            for (key, value) in values {
                let pointer = json_pointer_child(pointer, key);
                if key == "cmd"
                    && let serde_json::Value::String(command) = value
                {
                    highlights.push(ToolInputShellHighlight {
                        tool_use_index,
                        tool_use_id: tool_use_id.to_owned(),
                        input_pointer: pointer.clone(),
                        tokens: highlighted_bash_tokens(command, configuration, highlighter),
                    });
                }
                collect_shell_highlights(
                    value,
                    &pointer,
                    tool_use_index,
                    tool_use_id,
                    configuration,
                    highlighter,
                    highlights,
                );
            }
        }
        serde_json::Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                let pointer = json_pointer_child(pointer, &index.to_string());
                collect_shell_highlights(
                    value,
                    &pointer,
                    tool_use_index,
                    tool_use_id,
                    configuration,
                    highlighter,
                    highlights,
                );
            }
        }
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => {}
    }
}

fn highlighted_bash_tokens(
    source: &str,
    configuration: &HighlightConfiguration,
    highlighter: &mut Highlighter,
) -> Vec<ShellHighlightToken> {
    let Ok(events) = highlighter.highlight(configuration, source.as_bytes(), None, |_| None) else {
        return vec![ShellHighlightToken {
            kind: ShellHighlightKind::Plain,
            text: source.to_owned(),
        }];
    };
    let mut tokens = Vec::new();
    let mut highlight_stack = Vec::new();
    for event in events {
        let Ok(event) = event else {
            return vec![ShellHighlightToken {
                kind: ShellHighlightKind::Plain,
                text: source.to_owned(),
            }];
        };
        match event {
            HighlightEvent::Source { start, end } => {
                push_token(
                    &mut tokens,
                    active_highlight_kind(&highlight_stack),
                    &source[start..end],
                );
            }
            HighlightEvent::HighlightStart(highlight) => highlight_stack.push(highlight.0),
            HighlightEvent::HighlightEnd => {
                highlight_stack.pop();
            }
        }
    }
    if tokens.is_empty() {
        push_token(&mut tokens, ShellHighlightKind::Plain, source);
    }
    tokens
}

fn active_highlight_kind(highlights: &[usize]) -> ShellHighlightKind {
    highlights
        .iter()
        .copied()
        .map(highlight_kind)
        .max_by_key(|kind| highlight_priority(*kind))
        .unwrap_or(ShellHighlightKind::Plain)
}

fn highlight_kind(highlight: usize) -> ShellHighlightKind {
    match HIGHLIGHT_NAMES.get(highlight).copied() {
        Some("comment") => ShellHighlightKind::Comment,
        Some("constant") => ShellHighlightKind::Option,
        Some("embedded" | "property") => ShellHighlightKind::Variable,
        Some("function") => ShellHighlightKind::Command,
        Some("keyword") => ShellHighlightKind::Keyword,
        Some("number" | "operator") => ShellHighlightKind::Operator,
        Some("string") => ShellHighlightKind::String,
        _ => ShellHighlightKind::Plain,
    }
}

fn highlight_priority(kind: ShellHighlightKind) -> u8 {
    match kind {
        ShellHighlightKind::Plain => 0,
        ShellHighlightKind::Operator => 1,
        ShellHighlightKind::Command => 2,
        ShellHighlightKind::String => 3,
        ShellHighlightKind::Variable => 4,
        ShellHighlightKind::Option => 5,
        ShellHighlightKind::Keyword => 6,
        ShellHighlightKind::Comment => 7,
    }
}

fn push_token(tokens: &mut Vec<ShellHighlightToken>, kind: ShellHighlightKind, text: &str) {
    if text.is_empty() {
        return;
    }
    if let Some(previous) = tokens.last_mut().filter(|token| token.kind == kind) {
        previous.text.push_str(text);
    } else {
        tokens.push(ShellHighlightToken {
            kind,
            text: text.to_owned(),
        });
    }
}

fn json_pointer_child(parent: &str, child: &str) -> String {
    let escaped = child.replace('~', "~0").replace('/', "~1");
    format!("{parent}/{escaped}")
}

#[cfg(test)]
mod tests {
    use coco_types::ToolUse;

    use super::*;

    #[test]
    fn tree_sitter_highlights_nested_bash_without_changing_source() {
        let source = concat!(
            "FOO=(one two) printf '%s' start\n",
            "if [[ foo == bar || baz == qux ]]; then\n",
            "  case \"$x\" in a) printf '%s' ok;; esac\n",
            "fi"
        );
        let configuration = bash_highlight_configuration().unwrap();
        let tokens = highlighted_bash_tokens(source, configuration, &mut Highlighter::new());
        let commands = tokens
            .iter()
            .filter(|token| token.kind == ShellHighlightKind::Command)
            .map(|token| token.text.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            tokens
                .iter()
                .map(|token| token.text.as_str())
                .collect::<String>(),
            source
        );
        assert_eq!(commands, vec!["printf", "printf"]);
    }

    #[test]
    fn tool_input_highlights_track_nested_cmd_pointers() {
        let node: Node = serde_json::from_value(serde_json::json!({
            "id": "node",
            "parent": "",
            "created_at": "2026-07-25T00:00:00Z",
            "role": "LLM",
            "metadata": null,
            "kind": Kind::tool_uses(vec![ToolUse {
                id: "exec".to_owned(),
                name: "exec_command".to_owned(),
                input: serde_json::json!({
                    "steps": [{"cmd": "printf '%s' ok"}],
                }),
            }]),
        }))
        .expect("test node should deserialize");
        let highlights = tool_input_shell_highlights(&node);

        assert_eq!(highlights.len(), 1);
        assert_eq!(highlights[0].tool_use_index, 0);
        assert_eq!(highlights[0].tool_use_id, "exec");
        assert_eq!(highlights[0].input_pointer, "/steps/0/cmd");
    }
}
