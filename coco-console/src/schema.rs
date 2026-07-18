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
        active_overlay_run_id -> Nullable<BigInt>,
    }
}

diesel::table! {
    console_graph_overlay_runs (run_id) {
        run_id -> BigInt,
        base_generation -> BigInt,
        baseline_source_revision -> BigInt,
        baseline_publication_epoch -> BigInt,
        target_source_version -> BigInt,
        target_source_revision -> BigInt,
        target_publication_epoch -> BigInt,
        status -> Text,
        phase -> Text,
        cursor_node_id -> Text,
        cursor_endpoint -> Text,
        cursor_mode -> Text,
        cursor_work_kind -> Text,
        cursor_key -> Text,
        owner_id -> Text,
        lease_epoch -> BigInt,
        lease_expires_at_ms -> BigInt,
        created_at -> Text,
        updated_at -> Text,
        completed_at -> Nullable<Text>,
    }
}

diesel::table! {
    console_graph_build_edge_port_tombstones (run_id, mode, edge_key) {
        run_id -> BigInt,
        mode -> Text,
        edge_key -> Text,
    }
}

diesel::table! {
    console_graph_build_edge_route_tombstones (run_id, mode, edge_key) {
        run_id -> BigInt,
        mode -> Text,
        edge_key -> Text,
    }
}

diesel::table! {
    console_graph_generation_source_revisions (generation) {
        generation -> BigInt,
        source_revision -> BigInt,
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
    console_graph_materialization_shells (generation, mode) {
        generation -> BigInt,
        mode -> Text,
        node_count -> BigInt,
        edge_count -> BigInt,
    }
}

diesel::table! {
    console_graph_materialization_time_ticks (generation, mode, sample_index) {
        generation -> BigInt,
        mode -> Text,
        sample_index -> Integer,
        node_id -> Text,
        node_target -> Text,
        x -> Integer,
        y -> Integer,
        created_at -> Text,
        created_at_ns -> BigInt,
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

diesel::table! {
    console_graph_source_branch_history (branch_name, source_revision) {
        branch_name -> Text,
        source_revision -> BigInt,
        contribution_generation -> Nullable<BigInt>,
        head_id -> Nullable<Text>,
        state_json -> Nullable<Text>,
        removed -> Integer,
    }
}

diesel::table! {
    console_graph_source_branch_names (branch_name) {
        branch_name -> Text,
        first_source_revision -> BigInt,
    }
}

diesel::table! {
    console_graph_source_branch_nodes (branch_name, contribution_generation, node_id) {
        branch_name -> Text,
        contribution_generation -> BigInt,
        node_id -> Text,
    }
}

diesel::table! {
    console_graph_source_branches (name) {
        name -> Text,
        head_id -> Text,
        state_json -> Text,
        contribution_generation -> BigInt,
    }
}

diesel::table! {
    console_graph_source_dynamic_branch_scan_origins (scan_id, branch_name) {
        scan_id -> BigInt,
        branch_name -> Text,
    }
}

diesel::table! {
    console_graph_source_dynamic_branch_scan_results (scan_id, branch_name) {
        scan_id -> BigInt,
        branch_name -> Text,
    }
}

diesel::table! {
    console_graph_source_dynamic_branch_scans (scan_id) {
        scan_id -> BigInt,
        scan_kind -> Text,
        request_key -> Text,
        mutation_revision -> Nullable<BigInt>,
        source_revision -> BigInt,
        raw_refresh_id_upper_bound -> BigInt,
        dirty_node_id -> Nullable<Text>,
        targeted_limit -> Nullable<BigInt>,
        status -> Text,
        origin_branch_cursor -> Nullable<Text>,
        active_origin_branch_name -> Nullable<Text>,
        origin_raw_node_cursor -> Nullable<Text>,
        origin_raw_traversal_cursor -> Nullable<Text>,
        origin_raw_refresh_id_cursor -> Nullable<BigInt>,
        completed_origin_node_id -> Nullable<Text>,
        active_origin_node_id -> Nullable<Text>,
        candidate_raw_branch_cursor -> Nullable<Text>,
        candidate_raw_refresh_id_cursor -> Nullable<BigInt>,
        candidate_raw_traversal_cursor -> Nullable<Text>,
        result_count -> BigInt,
        exceeded_limit -> Integer,
        owner_id -> Text,
        lease_epoch -> BigInt,
        lease_expires_at_ms -> BigInt,
        created_at -> Text,
        updated_at -> Text,
    }
}

diesel::table! {
    console_graph_source_mutation_event_runs (revision) {
        revision -> BigInt,
        phase -> Text,
        branch_cursor -> Nullable<Text>,
        dirty_parent_cursor -> Nullable<Text>,
        active_dirty_parent_id -> Nullable<Text>,
        peer_branch_cursor -> Nullable<Text>,
        created_at -> Text,
        updated_at -> Text,
    }
}

diesel::table! {
    console_graph_source_mutation_journal_state (id) {
        id -> Integer,
        consumed_revision -> BigInt,
        initialized -> Integer,
    }
}

diesel::table! {
    console_graph_source_node_relations (parent_id, child_id) {
        parent_id -> Text,
        child_id -> Text,
    }
}

diesel::table! {
    console_graph_source_nodes (node_id) {
        node_id -> Text,
        parent_id -> Text,
        node_json -> Text,
        relation_cursor_offset -> BigInt,
        relation_ingest_complete -> Integer,
    }
}

diesel::table! {
    console_graph_source_refresh_queue (refresh_id, node_id, traversal_kind) {
        refresh_id -> BigInt,
        branch_name -> Text,
        node_id -> Text,
        traversal_kind -> Text,
        processed -> Integer,
        node_committed -> Integer,
        force_child_scan -> Integer,
        child_cursor_relation_revision -> Nullable<BigInt>,
        child_cursor_node_id -> Nullable<Text>,
        parent_cursor_offset -> BigInt,
        parent_traversal_complete -> Integer,
        child_scan_required -> Integer,
        child_high_watermark_frozen -> Integer,
        child_high_watermark_relation_revision -> Nullable<BigInt>,
        child_high_watermark_node_id -> Nullable<Text>,
    }
}

diesel::table! {
    console_graph_source_state (id) {
        id -> Integer,
        next_generation -> BigInt,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    console_graph_build_edge_port_tombstones,
    console_graph_build_edge_route_tombstones,
    console_graph_edge_routes,
    console_graph_generation_state,
    console_graph_generation_source_revisions,
    console_graph_materializations,
    console_graph_materialization_shells,
    console_graph_materialization_time_ticks,
    console_graph_node_locations,
    console_graph_overlay_runs,
    console_graph_source_branch_history,
    console_graph_source_branch_names,
    console_graph_source_branch_nodes,
    console_graph_source_branches,
    console_graph_source_dynamic_branch_scan_origins,
    console_graph_source_dynamic_branch_scan_results,
    console_graph_source_dynamic_branch_scans,
    console_graph_source_mutation_event_runs,
    console_graph_source_mutation_journal_state,
    console_graph_source_node_relations,
    console_graph_source_nodes,
    console_graph_source_refresh_queue,
    console_graph_source_state,
);
