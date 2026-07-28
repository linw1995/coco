use coco_types::{Kind, Node};
use tree_sitter_highlight::{HighlightConfiguration, HighlightEvent, Highlighter};

use crate::api::{
    JsonHighlightKind, JsonHighlightRange, ShellHighlightKind, ShellHighlightToken,
    ToolInputJsonHighlight, ToolInputShellHighlight,
};

const BASH_HIGHLIGHT_NAMES: &[&str] = &[
    "comment", "constant", "embedded", "function", "keyword", "number", "operator", "property",
    "string",
];
const JSON_HIGHLIGHT_NAMES: &[&str] = &[
    "comment",
    "constant.builtin",
    "escape",
    "number",
    "string",
    "string.special.key",
];

#[derive(Default)]
pub struct ToolInputSyntaxHighlights {
    pub shell: Vec<ToolInputShellHighlight>,
    pub json: Vec<ToolInputJsonHighlight>,
}

pub fn tool_input_syntax_highlights(node: &Node) -> ToolInputSyntaxHighlights {
    let Kind::ToolUse(items) = &node.kind else {
        return ToolInputSyntaxHighlights::default();
    };
    let mut result = ToolInputSyntaxHighlights::default();
    let mut bash_highlighter = Highlighter::new();
    let mut json_highlighter = Highlighter::new();
    for (tool_use_index, item) in items.iter().enumerate() {
        if let Some(configuration) = json_highlight_configuration()
            && let Ok(source) = serde_json::to_string_pretty(&item.input)
        {
            result.json.push(ToolInputJsonHighlight {
                tool_use_index,
                tool_use_id: item.id.clone(),
                ranges: highlighted_json_ranges(&source, configuration, &mut json_highlighter),
            });
        }
        if let Some(configuration) = bash_highlight_configuration() {
            collect_shell_highlights(
                &item.input,
                "",
                tool_use_index,
                &item.id,
                configuration,
                &mut bash_highlighter,
                &mut result.shell,
            );
        }
    }
    result
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
            configuration.configure(BASH_HIGHLIGHT_NAMES);
            Some(configuration)
        })
        .as_ref()
}

fn json_highlight_configuration() -> Option<&'static HighlightConfiguration> {
    static CONFIGURATION: std::sync::OnceLock<Option<HighlightConfiguration>> =
        std::sync::OnceLock::new();
    CONFIGURATION
        .get_or_init(|| {
            // tree-sitter-highlight gives later matches precedence for the same node.
            // Repeat the key rule after the official query so keys remain more specific than strings.
            let highlights_query = format!(
                "{}\n(pair key: (_) @string.special.key)",
                tree_sitter_json::HIGHLIGHTS_QUERY
            );
            let mut configuration = HighlightConfiguration::new(
                tree_sitter_json::LANGUAGE.into(),
                "json",
                &highlights_query,
                "",
                "",
            )
            .ok()?;
            configuration.configure(JSON_HIGHLIGHT_NAMES);
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
    highlighted_tokens(
        source,
        configuration,
        highlighter,
        ShellHighlightKind::Plain,
        active_bash_highlight_kind,
    )
    .into_iter()
    .map(|token| ShellHighlightToken {
        kind: token.kind,
        text: token.text,
    })
    .collect()
}

fn active_bash_highlight_kind(highlights: &[usize], _source: &str) -> ShellHighlightKind {
    highlights
        .iter()
        .copied()
        .map(bash_highlight_kind)
        .max_by_key(|kind| bash_highlight_priority(*kind))
        .unwrap_or(ShellHighlightKind::Plain)
}

fn bash_highlight_kind(highlight: usize) -> ShellHighlightKind {
    match BASH_HIGHLIGHT_NAMES.get(highlight).copied() {
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

fn bash_highlight_priority(kind: ShellHighlightKind) -> u8 {
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

fn highlighted_json_ranges(
    source: &str,
    configuration: &HighlightConfiguration,
    highlighter: &mut Highlighter,
) -> Vec<JsonHighlightRange> {
    let mut offset = 0;
    highlighted_tokens(
        source,
        configuration,
        highlighter,
        JsonHighlightKind::Plain,
        active_json_highlight_kind,
    )
    .into_iter()
    .filter_map(|token| {
        let start = offset;
        offset += token.text.len();
        (token.kind != JsonHighlightKind::Plain).then_some(JsonHighlightRange {
            kind: token.kind,
            start,
            end: offset,
        })
    })
    .collect()
}

fn active_json_highlight_kind(highlights: &[usize], source: &str) -> JsonHighlightKind {
    highlights
        .iter()
        .copied()
        .map(|highlight| json_highlight_kind(highlight, source))
        .max_by_key(|kind| json_highlight_priority(*kind))
        .unwrap_or(JsonHighlightKind::Plain)
}

fn json_highlight_kind(highlight: usize, source: &str) -> JsonHighlightKind {
    match JSON_HIGHLIGHT_NAMES.get(highlight).copied() {
        Some("constant.builtin") if source == "null" => JsonHighlightKind::Null,
        Some("constant.builtin") => JsonHighlightKind::Boolean,
        Some("escape") => JsonHighlightKind::Escape,
        Some("number") => JsonHighlightKind::Number,
        Some("string.special.key") => JsonHighlightKind::Key,
        Some("string") => JsonHighlightKind::String,
        _ => JsonHighlightKind::Plain,
    }
}

fn json_highlight_priority(kind: JsonHighlightKind) -> u8 {
    match kind {
        JsonHighlightKind::Plain => 0,
        JsonHighlightKind::Boolean | JsonHighlightKind::Null => 1,
        JsonHighlightKind::Number => 2,
        JsonHighlightKind::String => 3,
        JsonHighlightKind::Key => 4,
        JsonHighlightKind::Escape => 5,
    }
}

#[derive(Debug, PartialEq, Eq)]
struct HighlightToken<K> {
    kind: K,
    text: String,
}

fn highlighted_tokens<K>(
    source: &str,
    configuration: &HighlightConfiguration,
    highlighter: &mut Highlighter,
    plain_kind: K,
    active_kind: impl Fn(&[usize], &str) -> K,
) -> Vec<HighlightToken<K>>
where
    K: Copy + Eq,
{
    let Ok(events) = highlighter.highlight(configuration, source.as_bytes(), None, |_| None) else {
        return vec![HighlightToken {
            kind: plain_kind,
            text: source.to_owned(),
        }];
    };
    let mut tokens = Vec::new();
    let mut highlight_stack = Vec::new();
    for event in events {
        let Ok(event) = event else {
            return vec![HighlightToken {
                kind: plain_kind,
                text: source.to_owned(),
            }];
        };
        match event {
            HighlightEvent::Source { start, end } => push_token(
                &mut tokens,
                active_kind(&highlight_stack, &source[start..end]),
                &source[start..end],
            ),
            HighlightEvent::HighlightStart(highlight) => highlight_stack.push(highlight.0),
            HighlightEvent::HighlightEnd => {
                highlight_stack.pop();
            }
        }
    }
    if tokens.is_empty() {
        push_token(&mut tokens, plain_kind, source);
    }
    tokens
}

fn push_token<K>(tokens: &mut Vec<HighlightToken<K>>, kind: K, text: &str)
where
    K: Copy + Eq,
{
    if text.is_empty() {
        return;
    }
    if let Some(previous) = tokens.last_mut().filter(|token| token.kind == kind) {
        previous.text.push_str(text);
    } else {
        tokens.push(HighlightToken {
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
        let highlights = tool_input_syntax_highlights(&node).shell;

        assert_eq!(highlights.len(), 1);
        assert_eq!(highlights[0].tool_use_index, 0);
        assert_eq!(highlights[0].tool_use_id, "exec");
        assert_eq!(highlights[0].input_pointer, "/steps/0/cmd");
    }

    #[test]
    fn tree_sitter_returns_valid_pretty_json_ranges() {
        let source = serde_json::to_string_pretty(&serde_json::json!({
            "<key>": "line\nbreak",
            "number": 42,
            "boolean": false,
            "nothing": null,
        }))
        .unwrap();
        let configuration = json_highlight_configuration().unwrap();
        let ranges = highlighted_json_ranges(&source, configuration, &mut Highlighter::new());

        for kind in [
            JsonHighlightKind::Key,
            JsonHighlightKind::String,
            JsonHighlightKind::Number,
            JsonHighlightKind::Boolean,
            JsonHighlightKind::Null,
            JsonHighlightKind::Escape,
        ] {
            assert!(ranges.iter().any(|range| range.kind == kind), "{kind:?}");
        }
        for range in &ranges {
            assert!(source.is_char_boundary(range.start));
            assert!(source.is_char_boundary(range.end));
            assert!(range.start < range.end);
        }
        assert!(
            ranges
                .iter()
                .filter(|range| range.kind == JsonHighlightKind::Key)
                .all(|range| source[range.start..range.end].starts_with('"'))
        );
    }

    #[test]
    fn json_highlights_track_tool_item_identity() {
        let node: Node = serde_json::from_value(serde_json::json!({
            "id": "node",
            "parent": "",
            "created_at": "2026-07-25T00:00:00Z",
            "role": "LLM",
            "metadata": null,
            "kind": Kind::tool_uses(vec![
                ToolUse {
                    id: "first".to_owned(),
                    name: "exec_command".to_owned(),
                    input: serde_json::json!({"cmd": "true"}),
                },
                ToolUse {
                    id: "second".to_owned(),
                    name: "write_stdin".to_owned(),
                    input: serde_json::json!({"session_id": 42}),
                },
            ]),
        }))
        .expect("test node should deserialize");
        let highlights = tool_input_syntax_highlights(&node).json;

        assert_eq!(highlights.len(), 2);
        assert_eq!(highlights[0].tool_use_index, 0);
        assert_eq!(highlights[0].tool_use_id, "first");
        assert!(!highlights[0].ranges.is_empty());
        assert_eq!(highlights[1].tool_use_index, 1);
        assert_eq!(highlights[1].tool_use_id, "second");
        assert!(!highlights[1].ranges.is_empty());
        let response = serde_json::to_string(&highlights).unwrap();
        assert!(!response.contains("\"cmd\""));
        assert!(!response.contains("\"session_id\""));
    }
}
