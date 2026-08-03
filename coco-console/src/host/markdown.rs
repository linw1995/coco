use std::collections::HashSet;

use coco_types::{AnchorPayload, Kind, Node, SkillInvocationMode};
use tree_sitter::Node as SyntaxNode;
use tree_sitter_md::{MarkdownParser, MarkdownTree};

use crate::api::{MarkdownDocument, MarkdownNode};

pub fn node_markdown_documents(node: &Node) -> Vec<MarkdownDocument> {
    let mut sources = Vec::new();
    match &node.kind {
        Kind::Text(text) => sources.push(text.as_str()),
        Kind::Anchor(anchor) => match &anchor.payload {
            AnchorPayload::Session(session) => {
                sources.push(session.prompt.as_str());
                sources.push(session.system_prompt.as_str());
            }
            AnchorPayload::Prompt(prompt) => sources.push(prompt.prompt.as_str()),
            AnchorPayload::SkillInvocation(invocation) => {
                if let SkillInvocationMode::Handoff { prompt } = &invocation.mode {
                    sources.push(prompt.as_str());
                }
            }
            AnchorPayload::SkillResult(result) => sources.push(result.output.as_str()),
            AnchorPayload::SessionPatch(_) => {}
        },
        Kind::ToolUse(_) | Kind::ToolResult(_) | Kind::Failure(_) => {}
    }

    let mut seen = HashSet::new();
    let mut parser = MarkdownParser::default();
    sources
        .into_iter()
        .filter(|source| !source.is_empty() && seen.insert(*source))
        .filter_map(|source| parse_document(&mut parser, source))
        .collect()
}

pub fn markdown_document(source: &str) -> Option<MarkdownDocument> {
    parse_document(&mut MarkdownParser::default(), source)
}

fn parse_document(parser: &mut MarkdownParser, source: &str) -> Option<MarkdownDocument> {
    let tree = parser.parse(source.as_bytes(), None)?;
    let blocks = block_children(&tree, tree.block_tree().root_node(), source);
    Some(MarkdownDocument {
        source: source.to_owned(),
        blocks,
    })
}

fn block_children(tree: &MarkdownTree, node: SyntaxNode<'_>, source: &str) -> Vec<MarkdownNode> {
    let mut cursor = node.walk();
    node.named_children(&mut cursor)
        .flat_map(|child| render_block(tree, child, source))
        .collect()
}

fn render_block(tree: &MarkdownTree, node: SyntaxNode<'_>, source: &str) -> Vec<MarkdownNode> {
    match node.kind() {
        "document" | "section" => block_children(tree, node, source),
        "paragraph" => vec![MarkdownNode::Paragraph {
            children: block_inline(tree, node, source),
        }],
        "atx_heading" | "setext_heading" => vec![render_heading(tree, node, source)],
        "list" => vec![render_list(tree, node, source)],
        "block_quote" => vec![MarkdownNode::BlockQuote {
            children: block_children(tree, node, source),
        }],
        _ => render_leaf_block(tree, node, source),
    }
}

fn render_heading(tree: &MarkdownTree, node: SyntaxNode<'_>, source: &str) -> MarkdownNode {
    let level = match node.kind() {
        "atx_heading" => atx_heading_level(node),
        "setext_heading" => setext_heading_level(node),
        _ => 1,
    };
    MarkdownNode::Heading {
        level,
        children: block_inline(tree, node, source),
    }
}

fn render_leaf_block(tree: &MarkdownTree, node: SyntaxNode<'_>, source: &str) -> Vec<MarkdownNode> {
    match node.kind() {
        "fenced_code_block" => vec![render_fenced_code(node, source)],
        "indented_code_block" => vec![MarkdownNode::CodeBlock {
            language: None,
            code: dedent_code(node, source),
        }],
        "thematic_break" => vec![MarkdownNode::ThematicBreak],
        "html_block" => vec![MarkdownNode::CodeBlock {
            language: None,
            code: node_text(node, source).to_owned(),
        }],
        "pipe_table" => vec![MarkdownNode::CodeBlock {
            language: None,
            code: node_text(node, source).to_owned(),
        }],
        "link_reference_definition"
        | "minus_metadata"
        | "plus_metadata"
        | "block_quote_marker"
        | "block_continuation"
        | "list_marker_dot"
        | "list_marker_minus"
        | "list_marker_parenthesis"
        | "list_marker_plus"
        | "list_marker_star"
        | "task_list_marker_checked"
        | "task_list_marker_unchecked" => Vec::new(),
        _ => {
            let children = block_children(tree, node, source);
            if children.is_empty() {
                vec![MarkdownNode::Paragraph {
                    children: vec![text_node(node_text(node, source).trim_end())],
                }]
            } else {
                children
            }
        }
    }
}

fn block_inline(tree: &MarkdownTree, node: SyntaxNode<'_>, source: &str) -> Vec<MarkdownNode> {
    let mut cursor = node.walk();
    node.named_children(&mut cursor)
        .find(|child| child.kind() == "inline")
        .and_then(|inline| tree.inline_tree(&inline))
        .map(|tree| inline_children(tree.root_node(), source))
        .unwrap_or_else(|| vec![text_node(node_text(node, source).trim_end())])
}

fn render_list(tree: &MarkdownTree, node: SyntaxNode<'_>, source: &str) -> MarkdownNode {
    let mut cursor = node.walk();
    let items = node
        .named_children(&mut cursor)
        .filter(|child| child.kind() == "list_item")
        .map(|item| render_list_item(tree, item, source))
        .collect::<Vec<_>>();
    let marker = node
        .named_child(0)
        .and_then(|item| item.named_child(0))
        .map(|marker| (marker.kind(), node_text(marker, source)));

    match marker {
        Some(("list_marker_dot" | "list_marker_parenthesis", marker)) => {
            let start = marker
                .trim()
                .trim_end_matches(['.', ')'])
                .parse()
                .unwrap_or(1);
            MarkdownNode::OrderedList { start, items }
        }
        _ => MarkdownNode::UnorderedList { items },
    }
}

fn render_list_item(tree: &MarkdownTree, node: SyntaxNode<'_>, source: &str) -> Vec<MarkdownNode> {
    let mut cursor = node.walk();
    let marker = node
        .named_children(&mut cursor)
        .find(|child| {
            matches!(
                child.kind(),
                "task_list_marker_checked" | "task_list_marker_unchecked"
            )
        })
        .map(|marker| format!("{} ", node_text(marker, source)));
    let mut blocks = block_children(tree, node, source);
    if let (Some(marker), Some(MarkdownNode::Paragraph { children })) = (marker, blocks.first_mut())
    {
        prepend_text(children, &marker);
    }
    blocks
}

fn render_fenced_code(node: SyntaxNode<'_>, source: &str) -> MarkdownNode {
    let opening_fence = opening_fence(node_text(node, source), node.start_position().column);
    let mut cursor = node.walk();
    let mut language = None;
    let mut code = String::new();
    for child in node.named_children(&mut cursor) {
        match child.kind() {
            "info_string" => {
                language = child
                    .named_child(0)
                    .map(|language| node_text(language, source).trim().to_owned())
                    .filter(|language| !language.is_empty());
            }
            "code_fence_content" => code = node_text(child, source).to_owned(),
            _ => {}
        }
    }
    if let Some((marker, minimum_length, indentation)) = opening_fence {
        strip_leaked_closing_fence(&mut code, marker, minimum_length, indentation);
    }
    MarkdownNode::CodeBlock { language, code }
}

fn opening_fence(block: &str, start_column: usize) -> Option<(u8, usize, usize)> {
    let line = block.lines().next()?.as_bytes();
    let indentation = line.iter().take_while(|byte| **byte == b' ').count();
    let opening = &line[indentation..];
    let marker = *opening.first()?;
    if !matches!(marker, b'`' | b'~') {
        return None;
    }
    let length = opening.iter().take_while(|byte| **byte == marker).count();
    (length >= 3).then_some((marker, length, start_column + indentation))
}

fn strip_leaked_closing_fence(
    code: &mut String,
    marker: u8,
    minimum_length: usize,
    opening_indentation: usize,
) {
    let line_start = code.rfind('\n').map_or(0, |index| index + 1);
    let candidate = &code.as_bytes()[line_start..];
    let indentation = candidate.iter().take_while(|byte| **byte == b' ').count();
    if indentation.abs_diff(opening_indentation) > 3 {
        return;
    }
    let marker_length = candidate[indentation..]
        .iter()
        .take_while(|byte| **byte == marker)
        .count();
    let remainder = &candidate[indentation + marker_length..];
    if marker_length >= minimum_length && remainder.iter().all(|byte| matches!(byte, b' ' | b'\t'))
    {
        // tree-sitter-md includes an EOF closing fence in code_fence_content when no newline follows.
        code.truncate(line_start);
    }
}

fn inline_children(node: SyntaxNode<'_>, source: &str) -> Vec<MarkdownNode> {
    let mut children = Vec::new();
    let mut cursor = node.walk();
    let mut offset = node.start_byte();
    for child in node.children(&mut cursor) {
        if offset < child.start_byte() {
            push_text(&mut children, &source[offset..child.start_byte()]);
        }
        render_inline(child, source, &mut children);
        offset = child.end_byte();
    }
    if offset < node.end_byte() {
        push_text(&mut children, &source[offset..node.end_byte()]);
    }
    children
}

fn render_inline(node: SyntaxNode<'_>, source: &str, rendered: &mut Vec<MarkdownNode>) {
    match node.kind() {
        "emphasis" | "strong_emphasis" | "strikethrough" => {
            rendered.push(render_emphasis(node, source));
        }
        "code_span" => rendered.push(MarkdownNode::InlineCode {
            code: code_span_text(node, source),
        }),
        "inline_link" => rendered.push(render_link(node, source)),
        "image" => render_image_description(node, source, rendered),
        _ => render_inline_atom(node, source, rendered),
    }
}

fn render_emphasis(node: SyntaxNode<'_>, source: &str) -> MarkdownNode {
    let children = inline_children(node, source);
    match node.kind() {
        "emphasis" => MarkdownNode::Emphasis { children },
        "strong_emphasis" => MarkdownNode::Strong { children },
        "strikethrough" => MarkdownNode::Strikethrough { children },
        _ => MarkdownNode::Text {
            text: node_text(node, source).to_owned(),
        },
    }
}

fn render_image_description(node: SyntaxNode<'_>, source: &str, rendered: &mut Vec<MarkdownNode>) {
    let mut cursor = node.walk();
    if let Some(description) = node
        .named_children(&mut cursor)
        .find(|child| child.kind() == "image_description")
    {
        rendered.extend(inline_children(description, source));
    }
}

fn render_inline_atom(node: SyntaxNode<'_>, source: &str, rendered: &mut Vec<MarkdownNode>) {
    match node.kind() {
        "uri_autolink" | "email_autolink" => rendered.push(render_autolink(node, source)),
        "hard_line_break" => rendered.push(MarkdownNode::LineBreak),
        "backslash_escape" => push_text(
            rendered,
            node_text(node, source)
                .strip_prefix('\\')
                .unwrap_or_default(),
        ),
        "entity_reference" | "numeric_character_reference" => {
            let decoded = html_escape::decode_html_entities(node_text(node, source));
            push_text(rendered, &decoded);
        }
        "emphasis_delimiter" | "code_span_delimiter" => {}
        _ if node.is_named() => {
            let children = inline_children(node, source);
            if children.is_empty() {
                push_text(rendered, node_text(node, source));
            } else {
                rendered.extend(children);
            }
        }
        _ => push_text(rendered, node_text(node, source)),
    }
}

fn render_autolink(node: SyntaxNode<'_>, source: &str) -> MarkdownNode {
    let text = node_text(node, source)
        .trim_start_matches('<')
        .trim_end_matches('>')
        .to_owned();
    let destination = if node.kind() == "email_autolink" {
        format!("mailto:{text}")
    } else {
        text.clone()
    };
    MarkdownNode::Link {
        destination,
        children: vec![text_node(&text)],
    }
}

fn render_link(node: SyntaxNode<'_>, source: &str) -> MarkdownNode {
    let mut cursor = node.walk();
    let mut destination = String::new();
    let mut children = Vec::new();
    for child in node.named_children(&mut cursor) {
        match child.kind() {
            "link_text" => children = inline_children(child, source),
            "link_destination" => {
                destination =
                    html_escape::decode_html_entities(node_text(child, source)).into_owned();
            }
            _ => {}
        }
    }
    MarkdownNode::Link {
        destination,
        children,
    }
}

fn code_span_text(node: SyntaxNode<'_>, source: &str) -> String {
    let text = node_text(node, source);
    let delimiter_len = text.bytes().take_while(|byte| *byte == b'`').count();
    normalize_code_span(
        text.get(delimiter_len..text.len().saturating_sub(delimiter_len))
            .unwrap_or_default(),
    )
}

fn normalize_code_span(text: &str) -> String {
    let normalized = text.replace('\n', " ");
    if normalized.starts_with(' ')
        && normalized.ends_with(' ')
        && normalized.bytes().any(|byte| byte != b' ')
    {
        normalized[1..normalized.len() - 1].to_owned()
    } else {
        normalized
    }
}

fn atx_heading_level(node: SyntaxNode<'_>) -> u8 {
    node.named_child(0)
        .and_then(|marker| marker.kind().strip_prefix("atx_h"))
        .and_then(|level| level.strip_suffix("_marker"))
        .and_then(|level| level.parse().ok())
        .unwrap_or(1)
}

fn setext_heading_level(node: SyntaxNode<'_>) -> u8 {
    let mut cursor = node.walk();
    node.named_children(&mut cursor)
        .find_map(|child| match child.kind() {
            "setext_h1_underline" => Some(1),
            "setext_h2_underline" => Some(2),
            _ => None,
        })
        .unwrap_or(1)
}

fn dedent_code(node: SyntaxNode<'_>, source: &str) -> String {
    let text = node_text(node, source);
    let container_indent = node.start_position().column;
    text.split_inclusive('\n')
        .enumerate()
        .map(|(index, line)| {
            let indent = 4 + usize::from(index > 0) * container_indent;
            let offset = indentation_offset(line, indent);
            &line[offset..]
        })
        .collect()
}

fn indentation_offset(line: &str, target_columns: usize) -> usize {
    let mut columns = 0;
    let mut offset = 0;
    for (index, byte) in line.bytes().enumerate() {
        if columns >= target_columns {
            break;
        }
        match byte {
            b' ' => columns += 1,
            b'\t' => columns += 4 - columns % 4,
            _ => break,
        }
        offset = index + 1;
    }
    offset
}

fn node_text<'a>(node: SyntaxNode<'_>, source: &'a str) -> &'a str {
    &source[node.start_byte()..node.end_byte()]
}

fn text_node(text: &str) -> MarkdownNode {
    MarkdownNode::Text {
        text: text.to_owned(),
    }
}

fn push_text(rendered: &mut Vec<MarkdownNode>, text: &str) {
    if text.is_empty() {
        return;
    }
    if let Some(MarkdownNode::Text { text: previous }) = rendered.last_mut() {
        previous.push_str(text);
    } else {
        rendered.push(text_node(text));
    }
}

fn prepend_text(rendered: &mut Vec<MarkdownNode>, text: &str) {
    if let Some(MarkdownNode::Text { text: first }) = rendered.first_mut() {
        first.insert_str(0, text);
    } else {
        rendered.insert(0, text_node(text));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coco_types::ToolResult;

    fn parse(source: &str) -> MarkdownDocument {
        parse_document(&mut MarkdownParser::default(), source).unwrap()
    }

    fn test_node(kind: Kind) -> Node {
        serde_json::from_value(serde_json::json!({
            "id": "node-1",
            "parent": "parent-node",
            "created_at": "2026-07-25T00:00:00Z",
            "role": "User",
            "metadata": null,
            "kind": kind,
        }))
        .expect("test node should deserialize")
    }

    #[test]
    fn tree_sitter_builds_simple_markdown_document() {
        let document = parse(concat!(
            "# Heading *one*\n\n",
            "Paragraph with **bold**, `code`, and [link](https://example.com).\n\n",
            "- first\n- second\n\n",
            "```rust\nfn main() {}\n```\n",
        ));

        assert!(matches!(
            &document.blocks[0],
            MarkdownNode::Heading { level: 1, .. }
        ));
        assert!(matches!(
            &document.blocks[1],
            MarkdownNode::Paragraph { children }
                if children.iter().any(|child| matches!(child, MarkdownNode::Strong { .. }))
                    && children.iter().any(|child| matches!(child, MarkdownNode::InlineCode { code } if code == "code"))
                    && children.iter().any(|child| matches!(child, MarkdownNode::Link { destination, .. } if destination == "https://example.com"))
        ));
        assert!(matches!(
            &document.blocks[2],
            MarkdownNode::UnorderedList { items }
                if items == &vec![
                    vec![MarkdownNode::Paragraph {
                        children: vec![text_node("first")],
                    }],
                    vec![MarkdownNode::Paragraph {
                        children: vec![text_node("second")],
                    }],
                ]
        ));
        assert!(matches!(
            &document.blocks[3],
            MarkdownNode::CodeBlock { language: Some(language), code }
                if language == "rust" && code == "fn main() {}\n"
        ));
    }

    #[test]
    fn markdown_text_round_trips_without_delimiters() {
        let document = parse("plain *emphasis* and **strong**");
        assert_eq!(
            document.blocks,
            vec![MarkdownNode::Paragraph {
                children: vec![
                    text_node("plain "),
                    MarkdownNode::Emphasis {
                        children: vec![text_node("emphasis")],
                    },
                    text_node(" and "),
                    MarkdownNode::Strong {
                        children: vec![text_node("strong")],
                    },
                ],
            }]
        );
    }

    #[test]
    fn tree_sitter_handles_supported_block_and_inline_variants() {
        let document = parse(concat!(
            "Setext heading\n--------------\n\n",
            "1. ordered\n2. list\n\n",
            "> quote\n\n",
            "    indented code\n\n",
            "***\n\n",
            "~~gone~~ <https://example.com> <user@example.com> ![alt](image.png)  \n",
            "next \\* literal\n",
        ));
        let rendered = format!("{:?}", document.blocks);

        assert!(matches!(
            &document.blocks[0],
            MarkdownNode::Heading { level: 2, .. }
        ));
        assert!(matches!(
            &document.blocks[1],
            MarkdownNode::OrderedList { start: 1, items } if items.len() == 2
        ));
        assert!(matches!(
            &document.blocks[2],
            MarkdownNode::BlockQuote { .. }
        ));
        assert!(matches!(
            &document.blocks[3],
            MarkdownNode::CodeBlock { language: None, .. }
        ));
        assert!(rendered.contains("indented code"));
        assert!(matches!(&document.blocks[4], MarkdownNode::ThematicBreak));
        for expected in [
            "Strikethrough",
            "https://example.com",
            "mailto:user@example.com",
            "LineBreak",
            "alt",
            "* literal",
        ] {
            assert!(rendered.contains(expected), "{rendered}");
        }
    }

    #[test]
    fn tool_results_remain_preformatted_text() {
        let node = test_node(Kind::ToolResult(vec![ToolResult {
            id: "tool-1".to_owned(),
            output: "first line\nsecond line".to_owned(),
        }]));

        assert!(node_markdown_documents(&node).is_empty());
    }

    #[test]
    fn code_spans_follow_commonmark_space_normalization() {
        assert_eq!(normalize_code_span("  code  "), " code ");
        assert_eq!(normalize_code_span("   "), "   ");
        assert_eq!(normalize_code_span("code\nspan"), "code span");
        assert_eq!(normalize_code_span(" code "), "code");
    }

    #[test]
    fn character_references_are_decoded_as_text() {
        let document = parse("A &amp; B &#35; C &#x1F600;");

        assert_eq!(
            document.blocks,
            vec![MarkdownNode::Paragraph {
                children: vec![text_node("A & B # C 😀")],
            }]
        );
    }

    #[test]
    fn unsupported_tables_remain_preformatted() {
        let source = "| A | B |\n| - | - |\n| 1 | 2 |\n";
        let document = parse(source);

        assert_eq!(
            document.blocks,
            vec![MarkdownNode::CodeBlock {
                language: None,
                code: source.to_owned(),
            }]
        );
    }

    #[test]
    fn fenced_code_preserves_trailing_blank_lines() {
        let document = parse("```\nline\n\n```\n");

        assert_eq!(
            document.blocks,
            vec![MarkdownNode::CodeBlock {
                language: None,
                code: "line\n\n".to_owned(),
            }]
        );
    }

    #[test]
    fn fenced_code_at_eof_excludes_its_closing_fence() {
        for (source, expected) in [
            ("```bash\ncommand\n```", "command\n"),
            ("~~~text\ncontent\n~~~", "content\n"),
        ] {
            let document = parse(source);
            assert!(
                matches!(
                    document.blocks.as_slice(),
                    [MarkdownNode::CodeBlock { code, .. }] if code == expected
                ),
                "{:#?}",
                document.blocks
            );
        }
    }

    #[test]
    fn nested_fenced_code_at_eof_excludes_its_closing_fence() {
        let document = parse("10. item\n\n    ```text\n    content\n    ```");

        assert!(
            matches!(
                &document.blocks[0],
                MarkdownNode::OrderedList { items, .. }
                    if items[0].contains(&MarkdownNode::CodeBlock {
                        language: Some("text".to_owned()),
                        code: "content\n".to_owned(),
                    })
            ),
            "{:#?}",
            document.blocks
        );
    }

    #[test]
    fn longer_fence_preserves_shorter_fence_in_code() {
        let document = parse("````markdown\n```\n````");

        assert_eq!(
            document.blocks,
            vec![MarkdownNode::CodeBlock {
                language: Some("markdown".to_owned()),
                code: "```\n".to_owned(),
            }]
        );
    }

    #[test]
    fn builtin_skill_fenced_code_blocks_exclude_closing_fences() {
        for (name, source) in [
            (
                "coco-orchestrator",
                include_str!("../../../coco-mem/src/default_skills/coco-orchestrator.md"),
            ),
            (
                "cronjob",
                include_str!("../../../coco-mem/src/default_skills/cronjob.md"),
            ),
        ] {
            let document = parse(source.trim());
            for block in document.blocks {
                if let MarkdownNode::CodeBlock { code, .. } = block {
                    assert!(
                        !code.trim_end().ends_with("```"),
                        "{name} closing fence leaked into code: {code:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn nested_indented_code_removes_container_indentation() {
        let document = parse("- item\n\n      first\n        second\n");

        assert!(
            matches!(
                &document.blocks[0],
                MarkdownNode::UnorderedList { items }
                    if items[0].contains(&MarkdownNode::CodeBlock {
                        language: None,
                        code: "first\n  second\n".to_owned(),
                    })
            ),
            "{:#?}",
            document.blocks
        );
    }

    #[test]
    fn indented_code_preserves_content_indentation_and_line_endings() {
        let document = parse("      first\n    second\n");

        assert_eq!(
            document.blocks,
            vec![MarkdownNode::CodeBlock {
                language: None,
                code: "  first\nsecond\n".to_owned(),
            }]
        );
    }

    #[test]
    fn multiline_html_remains_preformatted() {
        let source = "<pre>\nfirst\nsecond\n</pre>\n";
        let document = parse(source);

        assert_eq!(
            document.blocks,
            vec![MarkdownNode::CodeBlock {
                language: None,
                code: source.to_owned(),
            }]
        );
    }

    #[test]
    fn task_list_markers_remain_literal_text() {
        let document = parse("- [x] deployed\n- [ ] pending\n");

        assert_eq!(
            document.blocks,
            vec![MarkdownNode::UnorderedList {
                items: vec![
                    vec![MarkdownNode::Paragraph {
                        children: vec![text_node("[x] deployed")],
                    }],
                    vec![MarkdownNode::Paragraph {
                        children: vec![text_node("[ ] pending")],
                    }],
                ],
            }]
        );
    }

    #[test]
    fn tab_prefixed_indented_code_removes_four_columns() {
        let document = parse("\tcommand\n");

        assert_eq!(
            document.blocks,
            vec![MarkdownNode::CodeBlock {
                language: None,
                code: "command\n".to_owned(),
            }]
        );
    }
}
