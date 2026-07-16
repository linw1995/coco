diesel::table! {
    console_graph_edge_routes (generation, mode, edge_key) {
        generation -> BigInt,
        mode -> Text,
        edge_key -> Text,
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
        min_x -> Integer,
        min_y -> Integer,
        max_x -> Integer,
        max_y -> Integer,
    }
}

diesel::table! {
    console_graph_generation_state (id) {
        id -> Integer,
        active_generation -> BigInt,
        next_generation -> BigInt,
    }
}

diesel::table! {
    console_graph_materializations (generation, mode) {
        generation -> BigInt,
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
    console_graph_node_locations (generation, mode, node_id) {
        generation -> BigInt,
        mode -> Text,
        node_id -> Text,
        node_key -> Text,
        node_target -> Text,
        short_id -> Text,
        node_kind -> Text,
        summary -> Text,
        labels_json -> Text,
        rank -> Integer,
        sort_order -> Integer,
        x -> Integer,
        y -> Integer,
        created_at -> Text,
        created_at_ns -> BigInt,
        min_x -> Integer,
        min_y -> Integer,
        max_x -> Integer,
        max_y -> Integer,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    console_graph_edge_routes,
    console_graph_generation_state,
    console_graph_materializations,
    console_graph_node_locations,
);
