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
        payload_json -> Text,
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
    node_relations (child_node_id, kind, ordinal) {
        child_node_id -> Text,
        parent_node_id -> Text,
        kind -> Text,
        ordinal -> Integer,
    }
}

diesel::table! {
    nodes (id) {
        id -> Text,
        parent_id -> Text,
        created_at -> Text,
        role -> Text,
        metadata_json -> Nullable<Text>,
        kind_json -> Text,
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
diesel::joinable!(sessions -> branches (branch_name));

diesel::allow_tables_to_appear_in_same_query!(
    branches,
    jobs,
    message_queue_items,
    node_relations,
    nodes,
    presets,
    sessions,
    skills,
    store_meta,
);
