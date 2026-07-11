CREATE TEMP TABLE skill_relational_migration_guard (
    complete INTEGER NOT NULL CHECK (complete = 1)
);

INSERT INTO skill_relational_migration_guard (complete)
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1
        FROM skills AS skill
        WHERE skill.role NOT IN ('orchestrator', 'runner')
           OR NOT EXISTS (
               SELECT 1
               FROM skill_versions AS version
               WHERE version.role = skill.role
                 AND version.skill_name = skill.name
                 AND version.version = skill.current_version
           )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM skill_versions AS version
        LEFT JOIN skills AS skill
          ON skill.role = version.role
         AND skill.name = version.skill_name
        WHERE skill.name IS NULL
           OR json_type(version.version) IS NOT 'integer'
           OR substr(version.version, 1, 1) = '-'
           OR length(version.version) > 20
           OR (
               length(version.version) = 20
               AND version.version > '18446744073709551615'
           )
           OR typeof(version.id) <> 'text'
           OR typeof(version.created_at) <> 'text'
           OR typeof(version.description) <> 'text'
           OR typeof(version.body) <> 'text'
           OR version.enable_coco_shim NOT IN (0, 1)
    )
    AND NOT EXISTS (
        SELECT 1
        FROM skill_version_scripts AS script
        LEFT JOIN skill_versions AS version
          ON version.role = script.role
         AND version.skill_name = script.skill_name
         AND version.version = script.version
        WHERE version.skill_name IS NULL
           OR script.ordinal < 0
           OR script.ordinal <> (
               SELECT COUNT(*)
               FROM skill_version_scripts AS preceding
               WHERE preceding.role = script.role
                 AND preceding.skill_name = script.skill_name
                 AND preceding.version = script.version
                 AND preceding.ordinal < script.ordinal
           )
           OR typeof(script.path) <> 'text'
           OR typeof(script.content) <> 'text'
    )
    THEN 1
    ELSE 0
END;

DROP TABLE skill_relational_migration_guard;

CREATE TEMP TABLE skill_version_script_json (
    role TEXT NOT NULL,
    skill_name TEXT NOT NULL,
    version TEXT NOT NULL,
    scripts_json TEXT NOT NULL,
    PRIMARY KEY (role, skill_name, version)
);

INSERT INTO skill_version_script_json (role, skill_name, version, scripts_json)
SELECT
    version.role,
    version.skill_name,
    version.version,
    COALESCE(
        (
            SELECT json_group_array(json(ordered_script.script_json))
            FROM (
                SELECT json_object(
                    'path', script.path,
                    'content', script.content
                ) AS script_json
                FROM skill_version_scripts AS script
                WHERE script.role = version.role
                  AND script.skill_name = version.skill_name
                  AND script.version = version.version
                ORDER BY script.ordinal
            ) AS ordered_script
        ),
        '[]'
    )
FROM skill_versions AS version;

CREATE TEMP TABLE skill_version_record_json (
    role TEXT NOT NULL,
    skill_name TEXT NOT NULL,
    version TEXT NOT NULL,
    record_json TEXT NOT NULL,
    PRIMARY KEY (role, skill_name, version)
);

INSERT INTO skill_version_record_json (role, skill_name, version, record_json)
SELECT
    version.role,
    version.skill_name,
    version.version,
    json_object(
        'id', version.id,
        'version', json(version.version),
        'created_at', version.created_at,
        'description', version.description,
        'body', version.body,
        'scripts', json(script.scripts_json),
        'enable_coco_shim', json(
            CASE WHEN version.enable_coco_shim THEN 'true' ELSE 'false' END
        )
    )
FROM skill_versions AS version
JOIN skill_version_script_json AS script
  ON script.role = version.role
 AND script.skill_name = version.skill_name
 AND script.version = version.version;

CREATE TEMP TABLE skill_record_json (
    role TEXT NOT NULL,
    name TEXT NOT NULL,
    record_json TEXT NOT NULL,
    PRIMARY KEY (role, name)
);

INSERT INTO skill_record_json (role, name, record_json)
SELECT
    skill.role,
    skill.name,
    json_object(
        'name', skill.name,
        'current_version', json(skill.current_version),
        'versions', json(
            COALESCE(
                (
                    SELECT json_group_object(version.version, json(version.record_json))
                    FROM skill_version_record_json AS version
                    WHERE version.role = skill.role
                      AND version.skill_name = skill.name
                ),
                '{}'
            )
        )
    )
FROM skills AS skill;

DROP TABLE skills;
DROP TABLE skill_version_scripts;
DROP TABLE skill_versions;

CREATE TABLE skills (
    role TEXT NOT NULL,
    name TEXT NOT NULL,
    record_json TEXT NOT NULL,
    PRIMARY KEY (role, name)
);

INSERT INTO skills (role, name, record_json)
SELECT role, name, record_json
FROM skill_record_json;

DROP TABLE skill_record_json;
DROP TABLE skill_version_record_json;
DROP TABLE skill_version_script_json;
