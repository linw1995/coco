diesel::table! {
    console_graph_edge_routes (mode, edge_key) {
        mode -> Text,
        edge_key -> Text,
        edge_kind -> Text,
        source_id -> Text,
        target_id -> Text,
        source_x -> Integer,
        source_y -> Integer,
        target_x -> Integer,
        target_y -> Integer,
        route_slot -> Integer,
        target_port_offset -> Double,
        min_x -> Integer,
        min_y -> Integer,
        max_x -> Integer,
        max_y -> Integer,
    }
}

diesel::table! {
    console_graph_materializations (mode) {
        mode -> Text,
        source_version -> BigInt,
        coordinate_space -> Text,
        world_min_x -> Integer,
        world_min_y -> Integer,
        world_max_x -> Integer,
        world_max_y -> Integer,
        updated_at -> Text,
    }
}

diesel::table! {
    console_graph_node_locations (mode, node_key) {
        mode -> Text,
        node_key -> Text,
        node_id -> Text,
        node_target -> Text,
        short_id -> Text,
        node_kind -> Text,
        summary -> Text,
        labels_json -> Text,
        lane_key -> Text,
        lane_label -> Text,
        lane_y -> Integer,
        x -> Integer,
        y -> Integer,
        min_x -> Integer,
        min_y -> Integer,
        max_x -> Integer,
        max_y -> Integer,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    console_graph_edge_routes,
    console_graph_materializations,
    console_graph_node_locations,
);
