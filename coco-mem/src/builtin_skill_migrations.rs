use coco_types::{SessionRole, SkillRecord, SkillUpdatePatch, SkillVersion};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuiltinSkillMigration {
    pub role: SessionRole,
    pub name: &'static str,
    revision_ids: &'static [&'static str],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuiltinSkillMigrationAction {
    Updated,
    Unchanged,
    SkipRolledBack,
    SkipUserModified,
    TargetMismatch,
}

impl BuiltinSkillMigration {
    pub fn target_revision_id(self) -> &'static str {
        self.revision_ids
            .last()
            .copied()
            .expect("builtin skill revision history must not be empty")
    }

    pub fn source_revision_ids(self) -> &'static [&'static str] {
        let source_count = self.revision_ids.len().saturating_sub(1);
        &self.revision_ids[..source_count]
    }
}

pub const BUILTIN_SKILL_MIGRATIONS: &[BuiltinSkillMigration] = &[
    BuiltinSkillMigration {
        role: SessionRole::Orchestrator,
        name: "coco-orchestrator",
        revision_ids: &[
            // Before the orchestrator runner prompt included load_image.
            "cbc625296d083943949e2255e848aec2c439d4573a3386cd39a63e71726c2438",
            // Before the prompt command was renamed to job.
            "79a81ed8e48dc4bac77d8d87ad5566d3b25c1aa1c6fd63cf89aec1efbc0ea6b9",
            // Before skill run required an explicit handoff.
            "1df4b89775b27c799b4f6b80b32b75c0cccd837dd574048484b38c13a5aff146",
            "eafe15f4db18391cbc6abee65a874317f6b350bed013272dea152e6285c18952",
        ],
    },
    BuiltinSkillMigration {
        role: SessionRole::Orchestrator,
        name: "new-skill",
        revision_ids: &["f6ede23518a575c8d87472a189b71dedf4fbc92b26403db2af748a00d481dbad"],
    },
    BuiltinSkillMigration {
        role: SessionRole::Orchestrator,
        name: "cronjob",
        revision_ids: &[
            // Before stale prompt job state was ignored.
            "88035685e93fab0d2a1b297aaf3e34da83e7415415112cc2266f7135ed019b9e",
            // Before the prompt command was renamed to job.
            "f57de170e92e784a37b2debbcf6854c73857235a4bf0e699a1cd67035b24cd92",
            "872b8f90c21af69be61fe7d90085dbd4491ca6dedd0aeae08feeee65db3aae5a",
        ],
    },
    BuiltinSkillMigration {
        role: SessionRole::Orchestrator,
        name: "recovery",
        revision_ids: &[
            // Before the prompt command was renamed to job.
            "6bf4094ad2dd2f9932cfc8d13a0f4a6b7adc9fe293e1ea6bc9f995d9c880a3f8",
            // Before session handoff required an explicit prompt.
            "dfc5ea6b5ef4c46ffb4c0c7d1fde59f1ebfe782eeb673a0987353047b72c7e3b",
            "91adf3f8b4e2fb11008b58db4d0c62c21b1b76cbe13b53a58e81fdeca1548b3b",
        ],
    },
    BuiltinSkillMigration {
        role: SessionRole::Orchestrator,
        name: "compact",
        revision_ids: &[
            // Before the prompt command was renamed to job.
            "3abb36a6333215088666cb168fef445430d19e19e19232e9e703286e3be3b9c6",
            // Before session handoff required an explicit prompt.
            "d035938926144776ca4341aaa57eaa3ed28a76234222f1ff06fe06cf5d8ab9ff",
            // Before self-compaction delegated to a durable worker branch.
            "6a260a4377c10fe227c4957db8a63ebfb8b6b292a9e3862c21402a1c1b73d14e",
            "b6db92669aaa86c89d354469cfb395d1fba5f1e96b73f1e4228e1f0188b016df",
        ],
    },
    BuiltinSkillMigration {
        role: SessionRole::Runner,
        name: "coco-runner",
        revision_ids: &[
            // Before the prompt command was renamed to job.
            "faa2096bbf0847b8e91247c56caf688e02442bdebde1d6dabae0b830ab373f22",
            "dcf88bdb5caaa2c8e4702cd5dfaa3e20919e08ce367ab7965e1f0d62710a60f4",
        ],
    },
    BuiltinSkillMigration {
        role: SessionRole::Runner,
        name: "telegram",
        revision_ids: &[
            // Before the attachment download script was added.
            "8d8630a19107380d2ba0cc1bcd3bf904f888a68bf535364b12b30340a582265c",
            // Before downloads were directed into the workspace.
            "fe5361a23cc71e2253b9d7867604cf1994db8fb6273dcae2ba2088a48c827e3c",
            // Before the download script defaulted into the workspace.
            "a86a9cb4ec5d5b8f6284970aa7c9feb53ddfbe7d1b984e9210dda7d1801edfd1",
            // Before send supported local images and files.
            "1b3f4dcf9b56400edb41ba960e6743b2e938ee58800e5dbb7fc02b11a8d432a0",
            // Before voice messages were supported.
            "5430febd6787debefdd86ed2830c7665483f0a02416e714288595f1850b4a2ee",
            // Before nono credential proxy base URLs were supported.
            "00f872655a1ca169ac3a5d1f21cbfa757346d33c5bd2fa8b4e72d0114b97a0d3",
            "ccfcf2d47498eb67fb118873bfdf9ed69d76768e93947982a6c20d25f5cb8117",
        ],
    },
];

pub fn migrate_builtin_skill(
    migration: BuiltinSkillMigration,
    record: &mut SkillRecord,
    target: &SkillVersion,
) -> BuiltinSkillMigrationAction {
    if target.id != migration.target_revision_id() {
        return BuiltinSkillMigrationAction::TargetMismatch;
    }
    let Some(current) = record.current() else {
        return BuiltinSkillMigrationAction::Unchanged;
    };
    if current.id == target.id {
        return BuiltinSkillMigrationAction::Unchanged;
    }
    if !migration
        .source_revision_ids()
        .contains(&current.id.as_str())
    {
        return BuiltinSkillMigrationAction::SkipUserModified;
    }
    let target_was_applied = record
        .versions
        .values()
        .any(|version| version.id == target.id);
    let mut saw_current_revision = false;
    let mut saw_intervening_revision = false;
    let source_was_restored = record
        .versions
        .range(..record.current_version)
        .map(|(_, version)| version)
        .any(|version| {
            if version.id == current.id {
                if saw_intervening_revision {
                    return true;
                }
                saw_current_revision = true;
            } else if saw_current_revision {
                saw_intervening_revision = true;
            }
            false
        })
        || (saw_current_revision && saw_intervening_revision);
    if target_was_applied || source_was_restored {
        return BuiltinSkillMigrationAction::SkipRolledBack;
    }

    let patch = SkillUpdatePatch {
        description: Some(target.description.clone()),
        body: Some(target.body.clone()),
        scripts: Some(target.scripts.clone()),
        enable_coco_shim: Some(target.enable_coco_shim),
    };
    if record.update(&patch).is_some() {
        BuiltinSkillMigrationAction::Updated
    } else {
        BuiltinSkillMigrationAction::Unchanged
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::default_skill_groups;

    #[test]
    fn builtin_skill_revision_histories_match_current_defaults() {
        let defaults = default_skill_groups();
        let default_keys = [
            (SessionRole::Orchestrator, &defaults.orchestrator),
            (SessionRole::Runner, &defaults.runner),
        ]
        .into_iter()
        .flat_map(|(role, records)| {
            records
                .keys()
                .map(move |name| (role.as_str(), name.as_str()))
        })
        .collect::<HashSet<_>>();
        let migration_keys = BUILTIN_SKILL_MIGRATIONS
            .iter()
            .map(|migration| (migration.role.as_str(), migration.name))
            .collect::<HashSet<_>>();

        assert_eq!(migration_keys, default_keys);
        assert_eq!(migration_keys.len(), BUILTIN_SKILL_MIGRATIONS.len());

        for migration in BUILTIN_SKILL_MIGRATIONS {
            let target = defaults
                .for_role(migration.role)
                .get(migration.name)
                .and_then(SkillRecord::current)
                .expect("builtin migration must reference a current default skill");
            assert_eq!(
                migration.target_revision_id(),
                target.id,
                "builtin skill changes must append a revision for {}",
                migration.name
            );
            assert_eq!(
                migration.revision_ids.iter().collect::<HashSet<_>>().len(),
                migration.revision_ids.len(),
                "builtin skill revision history must not contain duplicates for {}",
                migration.name
            );
            assert!(
                migration.revision_ids.iter().all(|revision| {
                    revision.len() == 64
                        && revision
                            .bytes()
                            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
                }),
                "builtin skill revisions must be lower hex SHA-256 values for {}",
                migration.name
            );
        }
    }

    #[test]
    fn known_builtin_revision_updates_to_current_default() {
        let defaults = default_skill_groups();
        let migration = BUILTIN_SKILL_MIGRATIONS
            .iter()
            .find(|migration| migration.name == "telegram")
            .copied()
            .unwrap();
        let target = defaults
            .for_role(migration.role)
            .get(migration.name)
            .and_then(SkillRecord::current)
            .unwrap();
        let mut record = defaults
            .for_role(migration.role)
            .get(migration.name)
            .cloned()
            .unwrap();
        record.current_version = 1;
        record.versions.get_mut(&1).unwrap().id =
            migration.source_revision_ids().last().unwrap().to_string();

        assert_eq!(
            migrate_builtin_skill(migration, &mut record, target),
            BuiltinSkillMigrationAction::Updated
        );
        assert_eq!(record.current_version, 2);
        assert_eq!(record.current().unwrap().id, target.id);
    }

    #[test]
    fn unknown_builtin_revision_is_preserved() {
        let defaults = default_skill_groups();
        let migration = BUILTIN_SKILL_MIGRATIONS
            .iter()
            .find(|migration| migration.name == "telegram")
            .copied()
            .unwrap();
        let target = defaults
            .for_role(migration.role)
            .get(migration.name)
            .and_then(SkillRecord::current)
            .unwrap();
        let mut record = defaults
            .for_role(migration.role)
            .get(migration.name)
            .cloned()
            .unwrap();
        record.versions.get_mut(&1).unwrap().id = "user-modified".to_owned();

        assert_eq!(
            migrate_builtin_skill(migration, &mut record, target),
            BuiltinSkillMigrationAction::SkipUserModified
        );
        assert_eq!(record.current_version, 1);
        assert_eq!(record.current().unwrap().id, "user-modified");
    }

    #[test]
    fn rolled_back_builtin_revision_is_preserved() {
        let defaults = default_skill_groups();
        let migration = BUILTIN_SKILL_MIGRATIONS
            .iter()
            .find(|migration| migration.name == "telegram")
            .copied()
            .unwrap();
        let target = defaults
            .for_role(migration.role)
            .get(migration.name)
            .and_then(SkillRecord::current)
            .unwrap();
        let mut record = defaults
            .for_role(migration.role)
            .get(migration.name)
            .cloned()
            .unwrap();
        let source_revision = migration.source_revision_ids().last().unwrap();
        record.versions.get_mut(&1).unwrap().id = source_revision.to_string();
        assert_eq!(
            migrate_builtin_skill(migration, &mut record, target),
            BuiltinSkillMigrationAction::Updated
        );
        record.rollback(1).unwrap();

        assert_eq!(
            migrate_builtin_skill(migration, &mut record, target),
            BuiltinSkillMigrationAction::SkipRolledBack
        );
        assert_eq!(record.current_version, 3);
        assert_eq!(record.current().unwrap().id, *source_revision);
    }

    #[test]
    fn pre_upgrade_rollback_is_preserved() {
        let defaults = default_skill_groups();
        let migration = BUILTIN_SKILL_MIGRATIONS
            .iter()
            .find(|migration| migration.name == "telegram")
            .copied()
            .unwrap();
        let target = defaults
            .for_role(migration.role)
            .get(migration.name)
            .and_then(SkillRecord::current)
            .unwrap();
        let mut record = defaults
            .for_role(migration.role)
            .get(migration.name)
            .cloned()
            .unwrap();
        let source_revision = migration.source_revision_ids().last().unwrap();
        record.versions.get_mut(&1).unwrap().id = source_revision.to_string();
        record
            .update(&SkillUpdatePatch {
                body: Some("user-modified body".to_owned()),
                ..SkillUpdatePatch::default()
            })
            .unwrap();
        record.rollback(1).unwrap();

        assert_eq!(
            migrate_builtin_skill(migration, &mut record, target),
            BuiltinSkillMigrationAction::SkipRolledBack
        );
        assert_eq!(record.current_version, 3);
        assert_eq!(record.current().unwrap().id, *source_revision);
        assert!(
            record
                .versions
                .values()
                .all(|version| version.id != target.id)
        );
    }

    #[test]
    fn no_op_update_does_not_block_builtin_migration() {
        let defaults = default_skill_groups();
        let migration = BUILTIN_SKILL_MIGRATIONS
            .iter()
            .find(|migration| migration.name == "telegram")
            .copied()
            .unwrap();
        let target = defaults
            .for_role(migration.role)
            .get(migration.name)
            .and_then(SkillRecord::current)
            .unwrap();
        let mut record = defaults
            .for_role(migration.role)
            .get(migration.name)
            .cloned()
            .unwrap();
        let source_revision = migration.source_revision_ids().last().unwrap();
        record.versions.get_mut(&1).unwrap().id = source_revision.to_string();
        record
            .update(&SkillUpdatePatch {
                description: Some(record.current().unwrap().description.clone()),
                ..SkillUpdatePatch::default()
            })
            .unwrap();
        record.current_version = 2;
        record.versions.get_mut(&2).unwrap().id = source_revision.to_string();

        assert_eq!(
            migrate_builtin_skill(migration, &mut record, target),
            BuiltinSkillMigrationAction::Updated
        );
        assert_eq!(record.current_version, 3);
        assert_eq!(record.current().unwrap().id, target.id);
    }
}
