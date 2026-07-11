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
    node_anchors (node_id) {
        node_id -> Text,
        kind -> Text,
        session_role -> Nullable<Text>,
        provider_profile -> Nullable<Text>,
        provider -> Nullable<Text>,
        model -> Nullable<Text>,
        prompt -> Nullable<Text>,
        skill_name -> Nullable<Text>,
        skill_invocation_mode -> Nullable<Text>,
        kind_json -> Text,
        session_system_prompt -> Nullable<Text>,
        session_temperature -> Nullable<Double>,
        session_max_tokens -> Nullable<Text>,
        session_additional_params_json -> Nullable<Text>,
        session_enable_coco_shim -> Nullable<Bool>,
        session_active_skill_name -> Nullable<Text>,
        session_active_skill_handoff -> Nullable<Text>,
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
    node_anchor_session_patch_tools (node_id, ordinal) {
        node_id -> Text,
        ordinal -> Integer,
        name -> Text,
        description -> Text,
        input_schema_json -> Text,
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
    presets (name) {
        name -> Text,
        record_json -> Text,
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
        state_json -> Text,
    }
}

diesel::table! {
    skills (role, name) {
        role -> Text,
        name -> Text,
        record_json -> Text,
    }
}

diesel::table! {
    store_meta (key) {
        key -> Text,
        value_json -> Text,
    }
}

diesel::joinable!(branches -> nodes (head_id));
diesel::joinable!(node_anchors -> nodes (node_id));
diesel::joinable!(node_anchor_session_tools -> node_anchors (node_id));
diesel::joinable!(node_anchor_session_patch_tools -> node_anchor_session_patches (node_id));
diesel::joinable!(node_anchor_session_patches -> node_anchors (node_id));
diesel::joinable!(node_metadata -> nodes (node_id));
diesel::joinable!(node_tool_results -> nodes (node_id));
diesel::joinable!(node_tool_uses -> nodes (node_id));
diesel::joinable!(sessions -> branches (branch_name));

diesel::allow_tables_to_appear_in_same_query!(
    branches,
    jobs,
    message_queue_items,
    node_anchor_session_patch_tools,
    node_anchor_session_patches,
    node_anchor_session_tools,
    node_anchors,
    node_metadata,
    node_relations,
    node_tool_results,
    node_tool_uses,
    nodes,
    presets,
    sessions,
    skills,
    store_meta,
);
