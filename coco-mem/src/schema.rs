// @generated automatically by Diesel CLI.

diesel::table! {
    branches (name) {
        name -> Text,
        head_id -> Text,
    }
}

diesel::table! {
    jobs (job_id) {
        job_id -> Text,
        created_at -> Text,
        finished_at -> Nullable<Text>,
        branch -> Text,
        work_branch -> Text,
        base -> Text,
        status -> Text,
    }
}

diesel::table! {
    message_queue_items (queue, message_id) {
        queue -> Text,
        message_id -> Text,
        created_at -> Text,
        payload_json -> Text,
    }
}

diesel::table! {
    node_anchor_sessions (node_id) {
        node_id -> Text,
        role -> Text,
        provider_profile -> Nullable<Text>,
        provider -> Nullable<Text>,
        model -> Text,
        system_prompt -> Text,
        prompt -> Text,
        temperature -> Nullable<Double>,
        max_tokens -> Nullable<Text>,
        additional_params_json -> Nullable<Text>,
        enable_coco_shim -> Bool,
        active_skill_name -> Nullable<Text>,
        active_skill_handoff -> Nullable<Text>,
    }
}

diesel::table! {
    node_anchor_session_tools (node_id, ordinal) {
        node_id -> Text,
        ordinal -> Integer,
        name -> Text,
        description -> Text,
        input_schema_json -> Text,
    }
}

diesel::table! {
    node_anchor_skill_invocations (node_id) {
        node_id -> Text,
        skill_name -> Text,
        mode -> Text,
        prompt -> Nullable<Text>,
    }
}

diesel::table! {
    node_anchor_skill_results (node_id) {
        node_id -> Text,
        skill_name -> Text,
        output -> Text,
    }
}

diesel::table! {
    node_anchor_session_patch_tools (node_id, ordinal) {
        node_id -> Text,
        ordinal -> Integer,
        name -> Text,
        description -> Text,
        input_schema_json -> Text,
    }
}

diesel::table! {
    node_anchor_prompt_attachments (node_id, ordinal) {
        node_id -> Text,
        ordinal -> Integer,
        kind -> Text,
        attachment_id -> Text,
        width -> Nullable<BigInt>,
        height -> Nullable<BigInt>,
        file_size -> Nullable<Text>,
        media_type -> Nullable<Text>,
    }
}

diesel::table! {
    node_anchor_session_patches (node_id) {
        node_id -> Text,
        role -> Nullable<Text>,
        provider_profile_present -> Bool,
        provider_profile -> Nullable<Text>,
        provider_present -> Bool,
        provider -> Nullable<Text>,
        model -> Nullable<Text>,
        tools_present -> Bool,
        system_prompt -> Nullable<Text>,
        temperature_present -> Bool,
        temperature -> Nullable<Double>,
        max_tokens_present -> Bool,
        max_tokens -> Nullable<Text>,
        additional_params_present -> Bool,
        additional_params_json -> Nullable<Text>,
        enable_coco_shim -> Nullable<Bool>,
    }
}

diesel::table! {
    node_metadata (node_id, ordinal) {
        node_id -> Text,
        ordinal -> Integer,
        execution_id -> Nullable<Text>,
        call_id -> Nullable<Text>,
    }
}

diesel::table! {
    node_relations (child_node_id, kind, ordinal) {
        child_node_id -> Text,
        parent_node_id -> Text,
        kind -> Text,
        ordinal -> Integer,
        created_revision -> BigInt,
    }
}

diesel::table! {
    graph_relation_state (singleton) {
        singleton -> Integer,
        current_revision -> BigInt,
        baseline_revision -> BigInt,
    }
}

diesel::table! {
    graph_child_adjacency (parent_node_id, child_node_id) {
        parent_node_id -> Text,
        child_node_id -> Text,
        first_created_revision -> BigInt,
    }
}

diesel::table! {
    graph_mutation_events (revision) {
        revision -> BigInt,
    }
}

diesel::table! {
    graph_mutation_event_branch_changes (revision, name) {
        revision -> BigInt,
        name -> Text,
        kind -> Text,
        head_id -> Nullable<Text>,
        state_json -> Nullable<Text>,
    }
}

diesel::table! {
    graph_mutation_event_dirty_parents (revision, parent_id) {
        revision -> BigInt,
        parent_id -> Text,
    }
}

diesel::table! {
    graph_mutation_event_branch_change_prune_staging (revision, name) {
        revision -> BigInt,
        name -> Text,
        kind -> Text,
        head_id -> Nullable<Text>,
        state_json -> Nullable<Text>,
    }
}

diesel::table! {
    graph_mutation_event_dirty_parent_prune_staging (revision, parent_id) {
        revision -> BigInt,
        parent_id -> Text,
    }
}

diesel::table! {
    graph_branch_history (name, revision) {
        name -> Text,
        revision -> BigInt,
        head_id -> Nullable<Text>,
        state_json -> Nullable<Text>,
        removed -> Bool,
    }
}

diesel::table! {
    graph_branch_names (name) {
        name -> Text,
        first_revision -> BigInt,
    }
}

diesel::table! {
    node_tool_results (node_id, ordinal) {
        node_id -> Text,
        ordinal -> Integer,
        tool_result_id -> Text,
        output -> Text,
    }
}

diesel::table! {
    node_tool_uses (node_id, ordinal) {
        node_id -> Text,
        ordinal -> Integer,
        tool_use_id -> Text,
        name -> Text,
        input_json -> Text,
    }
}

diesel::table! {
    nodes (id) {
        id -> Text,
        parent_id -> Text,
        created_at -> Text,
        role -> Text,
        kind -> Text,
        metadata_present -> Bool,
        content -> Nullable<Text>,
    }
}

diesel::table! {
    preset_version_tools (preset_name, version, ordinal) {
        preset_name -> Text,
        version -> Text,
        ordinal -> Integer,
        name -> Text,
        description -> Text,
        input_schema_json -> Text,
    }
}

diesel::table! {
    preset_versions (preset_name, version) {
        preset_name -> Text,
        version -> Text,
        created_at -> Text,
        role -> Text,
        provider_profile -> Text,
        model -> Text,
        system_prompt -> Text,
        prompt -> Text,
        temperature -> Nullable<Double>,
        max_tokens -> Nullable<Text>,
        additional_params_json -> Nullable<Text>,
        enable_coco_shim -> Bool,
    }
}

diesel::table! {
    presets (name) {
        name -> Text,
        current_version -> Text,
    }
}

diesel::table! {
    sessions (branch_name) {
        branch_name -> Text,
        state -> Text,
        target_branch -> Nullable<Text>,
        base_head_id -> Nullable<Text>,
        pause_reason -> Nullable<Text>,
        merged_anchor_id -> Nullable<Text>,
    }
}

diesel::table! {
    skills (role, name) {
        role -> Text,
        name -> Text,
        current_version -> Text,
    }
}

diesel::table! {
    skill_version_scripts (role, skill_name, version, ordinal) {
        role -> Text,
        skill_name -> Text,
        version -> Text,
        ordinal -> Integer,
        path -> Text,
        content -> Text,
    }
}

diesel::table! {
    skill_versions (role, skill_name, version) {
        role -> Text,
        skill_name -> Text,
        version -> Text,
        id -> Text,
        created_at -> Text,
        description -> Text,
        body -> Text,
        enable_coco_shim -> Bool,
    }
}

diesel::joinable!(branches -> nodes (head_id));
diesel::joinable!(node_anchor_sessions -> nodes (node_id));
diesel::joinable!(node_anchor_session_tools -> node_anchor_sessions (node_id));
diesel::joinable!(node_anchor_skill_invocations -> nodes (node_id));
diesel::joinable!(node_anchor_skill_results -> nodes (node_id));
diesel::joinable!(node_anchor_prompt_attachments -> nodes (node_id));
diesel::joinable!(node_anchor_session_patch_tools -> node_anchor_session_patches (node_id));
diesel::joinable!(node_anchor_session_patches -> nodes (node_id));
diesel::joinable!(node_metadata -> nodes (node_id));
diesel::joinable!(node_tool_results -> nodes (node_id));
diesel::joinable!(node_tool_uses -> nodes (node_id));
diesel::joinable!(sessions -> branches (branch_name));

diesel::allow_tables_to_appear_in_same_query!(
    branches,
    jobs,
    message_queue_items,
    node_anchor_prompt_attachments,
    node_anchor_session_patch_tools,
    node_anchor_session_patches,
    node_anchor_sessions,
    node_anchor_session_tools,
    node_anchor_skill_invocations,
    node_anchor_skill_results,
    node_metadata,
    node_relations,
    graph_child_adjacency,
    graph_relation_state,
    graph_mutation_events,
    graph_mutation_event_branch_changes,
    graph_mutation_event_dirty_parents,
    graph_mutation_event_branch_change_prune_staging,
    graph_mutation_event_dirty_parent_prune_staging,
    graph_branch_history,
    graph_branch_names,
    node_tool_results,
    node_tool_uses,
    nodes,
    preset_version_tools,
    preset_versions,
    presets,
    sessions,
    skill_version_scripts,
    skill_versions,
    skills,
);
