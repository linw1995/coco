use crate::api::{GraphBezierRoute, GraphViewportEdge, GraphViewportEdgeKind, GraphViewportNode};

pub fn edge_style(kind: GraphViewportEdgeKind) -> (&'static str, &'static str) {
    match kind {
        GraphViewportEdgeKind::Primary => ("edge primary-parent", "url(#arrowhead)"),
        GraphViewportEdgeKind::Merge => ("edge merge-parent", "url(#merge-arrowhead)"),
        GraphViewportEdgeKind::Shadow => ("edge shadow-parent", "url(#shadow-arrowhead)"),
    }
}

pub fn edge_kind_label(kind: GraphViewportEdgeKind) -> &'static str {
    match kind {
        GraphViewportEdgeKind::Primary => "Primary parent",
        GraphViewportEdgeKind::Merge => "Merge parent",
        GraphViewportEdgeKind::Shadow => "Shadow parent",
    }
}

pub fn edge_hit_target_label(edge: &GraphViewportEdge) -> String {
    let kind = edge_kind_label(edge.kind).to_lowercase();
    format!(
        "Expand {kind} relationship from {} to {}",
        edge.source_id, edge.target_id
    )
}

pub fn edge_hit_target_id(key: &str) -> String {
    format!("{}-hit", render_element_id(key))
}

pub fn node_group_class(node: &GraphViewportNode) -> String {
    let mut class = format!("node {}", css_token(&node.kind));
    if !node.labels.is_empty() {
        class.push_str(" active");
    }
    class
}

pub fn node_title_text(node: &GraphViewportNode) -> String {
    let labels = if node.labels.is_empty() {
        String::new()
    } else {
        format!(" [{}]", node.labels.join(", "))
    };
    format!(
        "{} · {}{}: {}",
        node.short_id, node.kind, labels, node.summary
    )
}

pub fn node_label(node: &GraphViewportNode) -> String {
    if node.labels.is_empty() {
        node.short_id.clone()
    } else {
        let mut labels = node
            .labels
            .iter()
            .take(2)
            .map(|label| truncate_label(label, 12))
            .collect::<Vec<_>>();
        if node.labels.len() > labels.len() {
            labels.push(format!("+{}", node.labels.len() - labels.len()));
        }
        format!("{} {}", node.short_id, labels.join(" · "))
    }
}

pub fn bezier_path(route: GraphBezierRoute) -> String {
    format!(
        "M {} {} C {} {}, {} {}, {} {}",
        route.source.x,
        route.source.y,
        route.control_1.x,
        route.control_1.y,
        route.control_2.x,
        route.control_2.y,
        route.target.x,
        route.target.y,
    )
}

pub fn render_element_id(key: &str) -> String {
    format!("graph-render-{}", percent_encode(key))
}

fn css_token(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect()
}

pub fn percent_encode(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                vec![byte as char]
            }
            _ => format!("%{byte:02X}").chars().collect(),
        })
        .collect()
}

pub fn truncate_label(label: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    let mut chars = label.chars();
    let prefix = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        let truncated = prefix
            .chars()
            .take(max_chars.saturating_sub(1))
            .collect::<String>();
        format!("{truncated}…")
    } else {
        prefix
    }
}
