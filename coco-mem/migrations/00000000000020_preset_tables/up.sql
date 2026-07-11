CREATE TEMP TABLE preset_json_migration_guard (
    complete INTEGER NOT NULL CHECK (complete = 1)
);

INSERT INTO preset_json_migration_guard (complete)
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1
        FROM presets
        WHERE NOT json_valid(record_json)
    ) THEN 1
    ELSE 0
END;

DELETE FROM preset_json_migration_guard;

INSERT INTO preset_json_migration_guard (complete)
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1
        FROM presets AS preset
        WHERE json_type(preset.record_json) IS NOT 'object'
           OR json_type(preset.record_json, '$.name') IS NOT 'text'
           OR json_extract(preset.record_json, '$.name') IS NOT preset.name
           OR json_type(preset.record_json, '$.current_version') IS NOT 'integer'
           OR json_extract(preset.record_json, '$.current_version') < 0
           OR length(preset.record_json -> '$.current_version') > 20
           OR (
               length(preset.record_json -> '$.current_version') = 20
               AND preset.record_json -> '$.current_version' > '18446744073709551615'
           )
           OR json_type(preset.record_json, '$.versions') IS NOT 'object'
           OR NOT EXISTS (
               SELECT 1
               FROM json_each(preset.record_json, '$.versions') AS version
               WHERE version.key IS preset.record_json -> '$.current_version'
           )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM presets AS preset
        JOIN json_each(preset.record_json, '$.versions') AS version
        WHERE json_type(version.value) IS NOT 'object'
           OR json_type(version.value, '$.version') IS NOT 'integer'
           OR json_extract(version.value, '$.version') < 0
           OR length(version.value -> '$.version') > 20
           OR (
               length(version.value -> '$.version') = 20
               AND version.value -> '$.version' > '18446744073709551615'
           )
           OR version.key IS NOT version.value -> '$.version'
           OR json_type(version.value, '$.created_at') IS NOT 'text'
           OR json_type(version.value, '$.role') IS NOT 'text'
           OR json_extract(version.value, '$.role') NOT IN ('orchestrator', 'runner')
           OR json_type(version.value, '$.provider_profile') IS NOT 'text'
           OR json_type(version.value, '$.model') IS NOT 'text'
           OR (
               json_type(version.value, '$.tools') IS NOT NULL
               AND json_type(version.value, '$.tools') <> 'array'
           )
           OR json_type(version.value, '$.system_prompt') IS NOT 'text'
           OR (
               json_type(version.value, '$.prompt') IS NOT NULL
               AND json_type(version.value, '$.prompt') <> 'text'
           )
           OR (
               json_type(version.value, '$.temperature') IS NOT NULL
               AND json_type(version.value, '$.temperature') NOT IN ('null', 'integer', 'real')
           )
           OR (
               json_type(version.value, '$.max_tokens') IS NOT NULL
               AND json_type(version.value, '$.max_tokens') NOT IN ('null', 'integer')
           )
           OR (
               json_type(version.value, '$.max_tokens') = 'integer'
               AND (
                   json_extract(version.value, '$.max_tokens') < 0
                   OR length(version.value -> '$.max_tokens') > 20
                   OR (
                       length(version.value -> '$.max_tokens') = 20
                       AND version.value -> '$.max_tokens' > '18446744073709551615'
                   )
               )
           )
           OR (
               json_type(version.value, '$.enable_coco_shim') IS NOT NULL
               AND json_type(version.value, '$.enable_coco_shim') NOT IN ('true', 'false')
           )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM presets AS preset
        JOIN json_each(preset.record_json, '$.versions') AS version
        JOIN json_each(version.value, '$.tools') AS tool
        WHERE json_type(tool.value) IS NOT 'object'
           OR json_type(tool.value, '$.name') IS NOT 'text'
           OR json_type(tool.value, '$.description') IS NOT 'text'
           OR json_type(tool.value, '$.input_schema') IS NULL
    )
    THEN 1
    ELSE 0
END;

DROP TABLE preset_json_migration_guard;

ALTER TABLE presets RENAME TO preset_records;

CREATE TABLE presets (
    name TEXT PRIMARY KEY NOT NULL,
    current_version TEXT NOT NULL,
    FOREIGN KEY (name, current_version)
        REFERENCES preset_versions(preset_name, version)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE preset_versions (
    preset_name TEXT NOT NULL,
    version TEXT NOT NULL,
    created_at TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('orchestrator', 'runner')),
    provider_profile TEXT NOT NULL,
    model TEXT NOT NULL,
    system_prompt TEXT NOT NULL,
    prompt TEXT NOT NULL,
    temperature DOUBLE,
    max_tokens TEXT,
    additional_params_json TEXT,
    enable_coco_shim BOOLEAN NOT NULL,
    PRIMARY KEY (preset_name, version),
    FOREIGN KEY (preset_name) REFERENCES presets(name) ON DELETE CASCADE
);

CREATE TABLE preset_version_tools (
    preset_name TEXT NOT NULL,
    version TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    input_schema_json TEXT NOT NULL,
    PRIMARY KEY (preset_name, version, ordinal),
    FOREIGN KEY (preset_name, version)
        REFERENCES preset_versions(preset_name, version)
        ON DELETE CASCADE
);

INSERT INTO presets (name, current_version)
SELECT
    name,
    record_json -> '$.current_version'
FROM preset_records;

INSERT INTO preset_versions (
    preset_name,
    version,
    created_at,
    role,
    provider_profile,
    model,
    system_prompt,
    prompt,
    temperature,
    max_tokens,
    additional_params_json,
    enable_coco_shim
)
SELECT
    preset.name,
    version.key,
    json_extract(version.value, '$.created_at'),
    json_extract(version.value, '$.role'),
    json_extract(version.value, '$.provider_profile'),
    json_extract(version.value, '$.model'),
    json_extract(version.value, '$.system_prompt'),
    COALESCE(json_extract(version.value, '$.prompt'), ''),
    json_extract(version.value, '$.temperature'),
    CASE
        WHEN json_type(version.value, '$.max_tokens') = 'null'
          OR json_type(version.value, '$.max_tokens') IS NULL
        THEN NULL
        ELSE version.value -> '$.max_tokens'
    END,
    CASE
        WHEN json_type(version.value, '$.additional_params') IS NULL THEN NULL
        ELSE version.value -> '$.additional_params'
    END,
    COALESCE(json_extract(version.value, '$.enable_coco_shim'), 0)
FROM preset_records AS preset
JOIN json_each(preset.record_json, '$.versions') AS version;

INSERT INTO preset_version_tools (
    preset_name,
    version,
    ordinal,
    name,
    description,
    input_schema_json
)
SELECT
    preset.name,
    version.key,
    CAST(tool.key AS INTEGER),
    json_extract(tool.value, '$.name'),
    json_extract(tool.value, '$.description'),
    tool.value -> '$.input_schema'
FROM preset_records AS preset
JOIN json_each(preset.record_json, '$.versions') AS version
JOIN json_each(version.value, '$.tools') AS tool;

DROP TABLE preset_records;
