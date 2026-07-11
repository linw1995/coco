CREATE TEMP TABLE node_anchor_migration_guard (
    complete INTEGER NOT NULL CHECK (complete = 1)
);

INSERT INTO node_anchor_migration_guard (complete)
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1
        FROM nodes
        WHERE (kind = 'anchor' AND anchor_kind IS NULL)
           OR (
                kind <> 'anchor'
                AND (
                    anchor_kind IS NOT NULL
                    OR anchor_session_role IS NOT NULL
                    OR anchor_provider_profile IS NOT NULL
                    OR anchor_provider IS NOT NULL
                    OR anchor_model IS NOT NULL
                    OR anchor_prompt IS NOT NULL
                    OR anchor_skill_name IS NOT NULL
                    OR anchor_skill_invocation_mode IS NOT NULL
                )
           )
    ) THEN 1
    ELSE 0
END;

DROP TABLE node_anchor_migration_guard;

CREATE TABLE node_anchors (
    node_id TEXT PRIMARY KEY NOT NULL,
    kind TEXT NOT NULL,
    session_role TEXT,
    provider_profile TEXT,
    provider TEXT,
    model TEXT,
    prompt TEXT,
    skill_name TEXT,
    skill_invocation_mode TEXT,
    kind_json TEXT NOT NULL,
    FOREIGN KEY (node_id) REFERENCES nodes(id)
);

INSERT INTO node_anchors (
    node_id,
    kind,
    session_role,
    provider_profile,
    provider,
    model,
    prompt,
    skill_name,
    skill_invocation_mode,
    kind_json
)
SELECT
    id,
    anchor_kind,
    anchor_session_role,
    anchor_provider_profile,
    anchor_provider,
    anchor_model,
    anchor_prompt,
    anchor_skill_name,
    anchor_skill_invocation_mode,
    kind_json
FROM nodes
WHERE kind = 'anchor';

CREATE INDEX node_anchors_kind_idx ON node_anchors(kind);

UPDATE nodes
SET kind_json = '{"Anchor":null}'
WHERE kind = 'anchor';

DROP INDEX nodes_anchor_kind_created_at_id_idx;
ALTER TABLE nodes DROP COLUMN anchor_kind;
ALTER TABLE nodes DROP COLUMN anchor_session_role;
ALTER TABLE nodes DROP COLUMN anchor_provider_profile;
ALTER TABLE nodes DROP COLUMN anchor_provider;
ALTER TABLE nodes DROP COLUMN anchor_model;
ALTER TABLE nodes DROP COLUMN anchor_prompt;
ALTER TABLE nodes DROP COLUMN anchor_skill_name;
ALTER TABLE nodes DROP COLUMN anchor_skill_invocation_mode;
