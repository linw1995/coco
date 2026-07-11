CREATE TEMP TABLE skill_json_migration_guard (
    complete INTEGER NOT NULL CHECK (complete = 1)
);

INSERT INTO skill_json_migration_guard (complete)
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1
        FROM skills
        WHERE NOT json_valid(record_json)
    ) THEN 1
    ELSE 0
END;

DELETE FROM skill_json_migration_guard;

INSERT INTO skill_json_migration_guard (complete)
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1
        FROM skills AS skill
        WHERE skill.role NOT IN ('orchestrator', 'runner')
           OR json_type(skill.record_json) IS NOT 'object'
           OR json_type(skill.record_json, '$.name') IS NOT 'text'
           OR json_extract(skill.record_json, '$.name') IS NOT skill.name
           OR json_type(skill.record_json, '$.current_version') IS NOT 'integer'
           OR json_extract(skill.record_json, '$.current_version') < 0
           OR length(skill.record_json -> '$.current_version') > 20
           OR (
               length(skill.record_json -> '$.current_version') = 20
               AND skill.record_json -> '$.current_version' > '18446744073709551615'
           )
           OR json_type(skill.record_json, '$.versions') IS NOT 'object'
           OR NOT EXISTS (
               SELECT 1
               FROM json_each(skill.record_json, '$.versions') AS version
               WHERE version.key IS skill.record_json -> '$.current_version'
           )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM skills AS skill
        JOIN json_each(skill.record_json, '$.versions') AS version
        WHERE json_type(version.value) IS NOT 'object'
           OR json_type(version.value, '$.version') IS NOT 'integer'
           OR json_extract(version.value, '$.version') < 0
           OR length(version.value -> '$.version') > 20
           OR (
               length(version.value -> '$.version') = 20
               AND version.value -> '$.version' > '18446744073709551615'
           )
           OR version.key IS NOT version.value -> '$.version'
           OR (
               json_type(version.value, '$.id') IS NOT NULL
               AND json_type(version.value, '$.id') IS NOT 'text'
           )
           OR json_type(version.value, '$.created_at') IS NOT 'text'
           OR json_type(version.value, '$.description') IS NOT 'text'
           OR json_type(version.value, '$.body') IS NOT 'text'
           OR (
               json_type(version.value, '$.scripts') IS NOT NULL
               AND json_type(version.value, '$.scripts') IS NOT 'array'
           )
           OR (
               json_type(version.value, '$.enable_coco_shim') IS NOT NULL
               AND json_type(version.value, '$.enable_coco_shim') NOT IN ('true', 'false')
           )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM skills AS skill
        JOIN json_each(skill.record_json, '$.versions') AS version
        JOIN json_each(version.value, '$.scripts') AS script
        WHERE json_type(script.value) IS NOT 'object'
           OR json_type(script.value, '$.path') IS NOT 'text'
           OR json_type(script.value, '$.content') IS NOT 'text'
    )
    THEN 1
    ELSE 0
END;

DROP TABLE skill_json_migration_guard;

ALTER TABLE skills RENAME TO skill_records;

CREATE TABLE skills (
    role TEXT NOT NULL CHECK (role IN ('orchestrator', 'runner')),
    name TEXT NOT NULL,
    current_version TEXT NOT NULL,
    PRIMARY KEY (role, name),
    FOREIGN KEY (role, name, current_version)
        REFERENCES skill_versions(role, skill_name, version)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE skill_versions (
    role TEXT NOT NULL,
    skill_name TEXT NOT NULL,
    version TEXT NOT NULL,
    id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    description TEXT NOT NULL,
    body TEXT NOT NULL,
    enable_coco_shim BOOLEAN NOT NULL,
    PRIMARY KEY (role, skill_name, version),
    FOREIGN KEY (role, skill_name)
        REFERENCES skills(role, name)
        ON DELETE CASCADE
);

CREATE TABLE skill_version_scripts (
    role TEXT NOT NULL,
    skill_name TEXT NOT NULL,
    version TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    path TEXT NOT NULL,
    content TEXT NOT NULL,
    PRIMARY KEY (role, skill_name, version, ordinal),
    FOREIGN KEY (role, skill_name, version)
        REFERENCES skill_versions(role, skill_name, version)
        ON DELETE CASCADE
);

INSERT INTO skills (role, name, current_version)
SELECT
    role,
    name,
    record_json -> '$.current_version'
FROM skill_records;

INSERT INTO skill_versions (
    role,
    skill_name,
    version,
    id,
    created_at,
    description,
    body,
    enable_coco_shim
)
SELECT
    skill.role,
    skill.name,
    version.key,
    COALESCE(json_extract(version.value, '$.id'), ''),
    json_extract(version.value, '$.created_at'),
    json_extract(version.value, '$.description'),
    json_extract(version.value, '$.body'),
    COALESCE(json_extract(version.value, '$.enable_coco_shim'), 0)
FROM skill_records AS skill
JOIN json_each(skill.record_json, '$.versions') AS version;

INSERT INTO skill_version_scripts (
    role,
    skill_name,
    version,
    ordinal,
    path,
    content
)
SELECT
    skill.role,
    skill.name,
    version.key,
    CAST(script.key AS INTEGER),
    json_extract(script.value, '$.path'),
    json_extract(script.value, '$.content')
FROM skill_records AS skill
JOIN json_each(skill.record_json, '$.versions') AS version
JOIN json_each(version.value, '$.scripts') AS script;

DROP TABLE skill_records;
