// @generated automatically by Diesel CLI.

diesel::table! {
    web_graph_edge_routes (layout_kind, edge_kind, source_id, target_id) {
        layout_kind -> Text,
        edge_kind -> Text,
        source_id -> Text,
        target_id -> Text,
        source_x -> Integer,
        source_y -> Integer,
        control_1_x -> Integer,
        control_1_y -> Integer,
        control_2_x -> Integer,
        control_2_y -> Integer,
        target_x -> Integer,
        target_y -> Integer,
    }
}

diesel::table! {
    web_graph_edges (edge_kind, source_id, target_id) {
        edge_kind -> Text,
        source_id -> Text,
        target_id -> Text,
    }
}

diesel::table! {
    web_graph_layouts (layout_kind) {
        layout_kind -> Text,
        canvas_width -> Integer,
        canvas_height -> Integer,
    }
}

diesel::table! {
    web_graph_node_placements (layout_kind, node_id) {
        layout_kind -> Text,
        node_id -> Text,
        x -> Integer,
        y -> Integer,
        outgoing_edge_count -> BigInt,
    }
}

diesel::table! {
    web_graph_node_spatial_index (rowid) {
        rowid -> Integer,
        spatial_id -> Nullable<Integer>,
        min_x -> Nullable<Integer>,
        max_x -> Nullable<Integer>,
        min_y -> Nullable<Integer>,
        max_y -> Nullable<Integer>,
        layout_kind -> Nullable<Binary>,
    }
}

diesel::table! {
    web_graph_node_spatial_items (spatial_id) {
        spatial_id -> Integer,
        layout_kind -> Text,
        node_id -> Text,
    }
}

diesel::table! {
    web_graph_nodes (node_id) {
        node_id -> Text,
    }
}

diesel::table! {
    web_graph_route_spatial_index (rowid) {
        rowid -> Integer,
        spatial_id -> Nullable<Integer>,
        min_x -> Nullable<Integer>,
        max_x -> Nullable<Integer>,
        min_y -> Nullable<Integer>,
        max_y -> Nullable<Integer>,
        layout_kind -> Nullable<Binary>,
    }
}

diesel::table! {
    web_graph_route_spatial_items (spatial_id) {
        spatial_id -> Integer,
        layout_kind -> Text,
        edge_kind -> Text,
        source_id -> Text,
        target_id -> Text,
    }
}

diesel::table! {
    web_graph_state (id) {
        id -> Integer,
        format_version -> Integer,
        revision -> BigInt,
        source_version -> BigInt,
        source_cursor_row_id -> Nullable<BigInt>,
        source_cursor_node_id -> Nullable<Text>,
    }
}

diesel::joinable!(web_graph_edge_routes -> web_graph_layouts (layout_kind));
diesel::joinable!(web_graph_node_placements -> web_graph_layouts (layout_kind));
diesel::joinable!(web_graph_node_placements -> web_graph_nodes (node_id));

diesel::allow_tables_to_appear_in_same_query!(
    web_graph_edge_routes,
    web_graph_edges,
    web_graph_layouts,
    web_graph_node_placements,
    web_graph_node_spatial_index,
    web_graph_node_spatial_items,
    web_graph_nodes,
    web_graph_route_spatial_index,
    web_graph_route_spatial_items,
    web_graph_state,
);
