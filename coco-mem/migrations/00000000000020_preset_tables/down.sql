CREATE TEMP TABLE preset_relational_migration_guard (
    complete INTEGER NOT NULL CHECK (complete = 1)
);

INSERT INTO preset_relational_migration_guard (complete)
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1
        FROM presets AS preset
        WHERE NOT EXISTS (
            SELECT 1
            FROM preset_versions AS version
            WHERE version.preset_name = preset.name
              AND version.version = preset.current_version
        )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM preset_versions AS version
        LEFT JOIN presets AS preset ON preset.name = version.preset_name
        WHERE preset.name IS NULL
           OR json_type(version.version) IS NOT 'integer'
           OR substr(version.version, 1, 1) = '-'
           OR length(version.version) > 20
           OR (
               length(version.version) = 20
               AND version.version > '18446744073709551615'
           )
           OR (
               version.max_tokens IS NOT NULL
               AND (
                   json_type(version.max_tokens) IS NOT 'integer'
                   OR substr(version.max_tokens, 1, 1) = '-'
                   OR length(version.max_tokens) > 20
                   OR (
                       length(version.max_tokens) = 20
                       AND version.max_tokens > '18446744073709551615'
                   )
               )
           )
           OR (
               version.additional_params_json IS NOT NULL
               AND NOT json_valid(version.additional_params_json)
           )
           OR version.enable_coco_shim NOT IN (0, 1)
    )
    AND NOT EXISTS (
        SELECT 1
        FROM preset_version_tools AS tool
        LEFT JOIN preset_versions AS version
          ON version.preset_name = tool.preset_name
         AND version.version = tool.version
        WHERE version.preset_name IS NULL
           OR tool.ordinal < 0
           OR tool.ordinal <> (
               SELECT COUNT(*)
               FROM preset_version_tools AS preceding
               WHERE preceding.preset_name = tool.preset_name
                 AND preceding.version = tool.version
                 AND preceding.ordinal < tool.ordinal
           )
           OR NOT json_valid(tool.input_schema_json)
    )
    THEN 1
    ELSE 0
END;

DROP TABLE preset_relational_migration_guard;

CREATE TEMP TABLE preset_version_tool_json (
    preset_name TEXT NOT NULL,
    version TEXT NOT NULL,
    tools_json TEXT NOT NULL,
    PRIMARY KEY (preset_name, version)
);

INSERT INTO preset_version_tool_json (preset_name, version, tools_json)
SELECT
    version.preset_name,
    version.version,
    COALESCE(
        (
            SELECT json_group_array(json(ordered_tool.tool_json))
            FROM (
                SELECT json_object(
                    'name', tool.name,
                    'description', tool.description,
                    'input_schema', json(tool.input_schema_json)
                ) AS tool_json
                FROM preset_version_tools AS tool
                WHERE tool.preset_name = version.preset_name
                  AND tool.version = version.version
                ORDER BY tool.ordinal
            ) AS ordered_tool
        ),
        '[]'
    )
FROM preset_versions AS version;

CREATE TEMP TABLE preset_version_record_json (
    preset_name TEXT NOT NULL,
    version TEXT NOT NULL,
    record_json TEXT NOT NULL,
    PRIMARY KEY (preset_name, version)
);

INSERT INTO preset_version_record_json (preset_name, version, record_json)
SELECT
    version.preset_name,
    version.version,
    json_object(
        'version', json(version.version),
        'created_at', version.created_at,
        'role', version.role,
        'provider_profile', version.provider_profile,
        'model', version.model,
        'tools', json(tool.tools_json),
        'system_prompt', version.system_prompt,
        'prompt', version.prompt,
        'temperature', version.temperature,
        'max_tokens', json(version.max_tokens),
        'enable_coco_shim', json(
            CASE WHEN version.enable_coco_shim THEN 'true' ELSE 'false' END
        )
    )
FROM preset_versions AS version
JOIN preset_version_tool_json AS tool
  ON tool.preset_name = version.preset_name
 AND tool.version = version.version;

-- The flattened legacy format distinguishes an absent field from an explicit JSON null.
UPDATE preset_version_record_json AS record
SET record_json = json_set(
    record.record_json,
    '$.additional_params',
    json(version.additional_params_json)
)
FROM preset_versions AS version
WHERE version.preset_name = record.preset_name
  AND version.version = record.version
  AND version.additional_params_json IS NOT NULL;

CREATE TEMP TABLE preset_record_json (
    name TEXT PRIMARY KEY NOT NULL,
    record_json TEXT NOT NULL
);

INSERT INTO preset_record_json (name, record_json)
SELECT
    preset.name,
    json_object(
        'name', preset.name,
        'current_version', json(preset.current_version),
        'versions', json(
            COALESCE(
                (
                    SELECT json_group_object(version.version, json(version.record_json))
                    FROM preset_version_record_json AS version
                    WHERE version.preset_name = preset.name
                ),
                '{}'
            )
        )
    )
FROM presets AS preset;

DROP TABLE presets;
DROP TABLE preset_version_tools;
DROP TABLE preset_versions;

CREATE TABLE presets (
    name TEXT PRIMARY KEY NOT NULL,
    record_json TEXT NOT NULL
);

INSERT INTO presets (name, record_json)
SELECT name, record_json
FROM preset_record_json;

DROP TABLE preset_record_json;
DROP TABLE preset_version_record_json;
DROP TABLE preset_version_tool_json;
