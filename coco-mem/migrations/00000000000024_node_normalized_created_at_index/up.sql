CREATE INDEX nodes_kind_normalized_created_at_id_idx ON nodes (
    kind,
    CAST(strftime('%s', created_at) AS INTEGER),
    CASE
        WHEN instr(created_at, '.') = 0 THEN 0
        ELSE CAST(
            substr(
                replace(substr(created_at, instr(created_at, '.') + 1), 'Z', '') || '000000000',
                1,
                9
            ) AS INTEGER
        )
    END,
    id
);
