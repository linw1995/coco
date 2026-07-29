use std::collections::HashSet;

use coco_types::{AnchorPayload, Kind, Node, SkillInvocationMode};
use tree_sitter::Node as SyntaxNode;
use tree_sitter_md::{MarkdownParser, MarkdownTree};

use crate::api::{MarkdownDocument, MarkdownNode};

pub fn node_markdown_documents(node: &Node) -> Vec<MarkdownDocument> {
    let mut sources = Vec::new();
    match &node.kind {
        Kind::Text(text) => sources.push(text.as_str()),
        Kind::ToolResult(items) => {
            sources.extend(items.iter().map(|item| item.output.as_str()));
        }
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
        Kind::ToolUse(_) | Kind::Failure(_) => {}
    }

    let mut seen = HashSet::new();
    let mut parser = MarkdownParser::default();
    sources
        .into_iter()
        .filter(|source| !source.is_empty() && seen.insert(*source))
        .filter_map(|source| parse_document(&mut parser, source))
        .collect()
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
            code: dedent_code(node_text(node, source)),
        }],
        "thematic_break" => vec![MarkdownNode::ThematicBreak],
        "html_block" | "pipe_table" => vec![MarkdownNode::Paragraph {
            children: vec![text_node(node_text(node, source).trim_end())],
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
        .map(|item| block_children(tree, item, source))
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

fn render_fenced_code(node: SyntaxNode<'_>, source: &str) -> MarkdownNode {
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
    MarkdownNode::CodeBlock {
        language,
        code: code.trim_end_matches('\n').to_owned(),
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
            "link_destination" => destination = node_text(child, source).to_owned(),
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
    text.get(delimiter_len..text.len().saturating_sub(delimiter_len))
        .unwrap_or_default()
        .trim_matches(' ')
        .replace('\n', " ")
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

fn dedent_code(source: &str) -> String {
    source
        .lines()
        .map(|line| line.strip_prefix("    ").unwrap_or(line))
        .collect::<Vec<_>>()
        .join("\n")
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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(source: &str) -> MarkdownDocument {
        parse_document(&mut MarkdownParser::default(), source).unwrap()
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
                if language == "rust" && code == "fn main() {}"
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
}
