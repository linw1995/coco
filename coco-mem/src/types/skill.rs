use std::collections::BTreeMap;

use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{SessionRole, hash::hex_encode};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SkillScript {
    pub path: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SkillRuntimeContext {
    pub name: String,
    pub handoff: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SkillVersionSpec {
    pub description: String,
    pub body: String,
    #[serde(default)]
    pub scripts: Vec<SkillScript>,
    #[serde(default)]
    pub enable_coco_shim: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SkillUpdatePatch {
    pub description: Option<String>,
    pub body: Option<String>,
    pub scripts: Option<Vec<SkillScript>>,
    pub enable_coco_shim: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SkillVersion {
    #[serde(default)]
    pub id: String,
    pub version: u64,
    pub created_at: Timestamp,
    pub description: String,
    pub body: String,
    #[serde(default)]
    pub scripts: Vec<SkillScript>,
    #[serde(default)]
    pub enable_coco_shim: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SkillRecord {
    pub name: String,
    pub current_version: u64,
    #[serde(default)]
    pub versions: BTreeMap<u64, SkillVersion>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SkillGroups {
    #[serde(default)]
    pub orchestrator: BTreeMap<String, SkillRecord>,
    #[serde(default)]
    pub runner: BTreeMap<String, SkillRecord>,
}

impl SkillUpdatePatch {
    pub fn is_empty(&self) -> bool {
        self.description.is_none()
            && self.body.is_none()
            && self.scripts.is_none()
            && self.enable_coco_shim.is_none()
    }
}

impl SkillVersion {
    pub fn new(version: u64, spec: SkillVersionSpec) -> Self {
        Self {
            id: compute_skill_version_id(
                &spec.description,
                &spec.body,
                &spec.scripts,
                spec.enable_coco_shim,
            ),
            version,
            created_at: Timestamp::now(),
            description: spec.description,
            body: spec.body,
            scripts: spec.scripts,
            enable_coco_shim: spec.enable_coco_shim,
        }
    }

    pub fn expected_id(&self) -> String {
        compute_skill_version_id(
            &self.description,
            &self.body,
            &self.scripts,
            self.enable_coco_shim,
        )
    }

    pub fn normalize_id(&mut self) {
        if self.id.is_empty() {
            self.id = self.expected_id();
        }
    }

    pub fn id_matches_content(&self) -> bool {
        self.id == self.expected_id()
    }
}

impl SkillRecord {
    pub fn new(name: impl Into<String>, spec: SkillVersionSpec) -> Self {
        let version = SkillVersion::new(1, spec);
        let current_version = version.version;
        let mut versions = BTreeMap::new();
        versions.insert(current_version, version);

        Self {
            name: name.into(),
            current_version,
            versions,
        }
    }

    pub fn current(&self) -> Option<&SkillVersion> {
        self.versions.get(&self.current_version)
    }

    pub fn update(&mut self, patch: &SkillUpdatePatch) -> Option<&SkillVersion> {
        let current = self.current()?.clone();
        let next_version = self.versions.keys().next_back().copied().unwrap_or(0) + 1;
        let description = patch.description.clone().unwrap_or(current.description);
        let body = patch.body.clone().unwrap_or(current.body);
        let scripts = patch.scripts.clone().unwrap_or(current.scripts);
        let enable_coco_shim = patch.enable_coco_shim.unwrap_or(current.enable_coco_shim);
        let next = SkillVersion {
            id: compute_skill_version_id(&description, &body, &scripts, enable_coco_shim),
            version: next_version,
            created_at: Timestamp::now(),
            description,
            body,
            scripts,
            enable_coco_shim,
        };

        self.current_version = next_version;
        self.versions.insert(next_version, next);
        self.current()
    }

    pub fn rollback(&mut self, target_version: u64) -> Option<&SkillVersion> {
        let target = self.versions.get(&target_version)?.clone();
        let next_version = self.versions.keys().next_back().copied().unwrap_or(0) + 1;
        let next = SkillVersion {
            id: target.id,
            version: next_version,
            created_at: Timestamp::now(),
            description: target.description,
            body: target.body,
            scripts: target.scripts,
            enable_coco_shim: target.enable_coco_shim,
        };

        self.current_version = next_version;
        self.versions.insert(next_version, next);
        self.current()
    }
}

impl SkillGroups {
    pub fn is_empty(&self) -> bool {
        self.orchestrator.is_empty() && self.runner.is_empty()
    }

    pub fn for_role(&self, role: SessionRole) -> &BTreeMap<String, SkillRecord> {
        match role {
            SessionRole::Orchestrator => &self.orchestrator,
            SessionRole::Runner => &self.runner,
        }
    }

    pub fn for_role_mut(&mut self, role: SessionRole) -> &mut BTreeMap<String, SkillRecord> {
        match role {
            SessionRole::Orchestrator => &mut self.orchestrator,
            SessionRole::Runner => &mut self.runner,
        }
    }
}

pub fn default_skill_groups() -> SkillGroups {
    let mut groups = SkillGroups::default();
    groups.orchestrator.insert(
        "coco-orchestrator".to_owned(),
        SkillRecord::new(
            "coco-orchestrator",
            SkillVersionSpec {
                description:
                    "Guide an orchestrator session through CoCo branch and prompt workflows."
                        .to_owned(),
                body: include_str!("../default_skills/coco-orchestrator.md")
                    .trim()
                    .to_owned(),
                scripts: Vec::new(),
                enable_coco_shim: true,
            },
        ),
    );
    groups.orchestrator.insert(
        "new-skill".to_owned(),
        SkillRecord::new(
            "new-skill",
            SkillVersionSpec {
                description: "Create or update dynamic CoCo skills through the skill add workflow."
                    .to_owned(),
                body: include_str!("../default_skills/new-skill.md")
                    .trim()
                    .to_owned(),
                scripts: Vec::new(),
                enable_coco_shim: true,
            },
        ),
    );
    groups.orchestrator.insert(
        "cronjob".to_owned(),
        SkillRecord::new(
            "cronjob",
            SkillVersionSpec {
                description: "Manage host crontab entries that submit CoCo prompts.".to_owned(),
                body: include_str!("../default_skills/cronjob.md")
                    .trim()
                    .to_owned(),
                scripts: vec![
                    SkillScript {
                        path: "scripts/cronjob_add.py".to_owned(),
                        content: include_str!("../default_skills/cronjob/scripts/cronjob_add.py")
                            .to_owned(),
                    },
                    SkillScript {
                        path: "scripts/cronjob_run.py".to_owned(),
                        content: include_str!("../default_skills/cronjob/scripts/cronjob_run.py")
                            .to_owned(),
                    },
                    SkillScript {
                        path: "scripts/cronjob_crontab.py".to_owned(),
                        content: include_str!(
                            "../default_skills/cronjob/scripts/cronjob_crontab.py"
                        )
                        .to_owned(),
                    },
                ],
                enable_coco_shim: true,
            },
        ),
    );
    groups.orchestrator.insert(
        "recovery".to_owned(),
        SkillRecord::new(
            "recovery",
            SkillVersionSpec {
                description: "Recover an LLM backend failure from the built-in day branch."
                    .to_owned(),
                body: include_str!("../default_skills/recovery.md")
                    .trim()
                    .to_owned(),
                scripts: Vec::new(),
                enable_coco_shim: true,
            },
        ),
    );
    groups.orchestrator.insert(
        "compact".to_owned(),
        SkillRecord::new(
            "compact",
            SkillVersionSpec {
                description:
                    "Compact a branch by summarizing the latest provider context into a handoff."
                        .to_owned(),
                body: include_str!("../default_skills/compact.md")
                    .trim()
                    .to_owned(),
                scripts: Vec::new(),
                enable_coco_shim: true,
            },
        ),
    );
    groups.runner.insert(
        "coco-runner".to_owned(),
        SkillRecord::new(
            "coco-runner",
            SkillVersionSpec {
                description:
                    "Guide a runner session through the CoCo commands available in runner scope."
                        .to_owned(),
                body: include_str!("../default_skills/coco-runner.md")
                    .trim()
                    .to_owned(),
                scripts: Vec::new(),
                enable_coco_shim: true,
            },
        ),
    );
    groups.runner.insert(
        "telegram".to_owned(),
        SkillRecord::new(
            "telegram",
            SkillVersionSpec {
                description:
                    "Send, reply to, edit, download, and attach files, images, and voice messages through the Telegram Bot API."
                        .to_owned(),
                body: include_str!("../default_skills/telegram.md")
                    .trim()
                    .to_owned(),
                scripts: vec![
                    SkillScript {
                        path: "scripts/telegram_send.py".to_owned(),
                        content: include_str!("../default_skills/telegram/scripts/telegram_send.py")
                            .to_owned(),
                    },
                    SkillScript {
                        path: "scripts/telegram_edit.py".to_owned(),
                        content: include_str!("../default_skills/telegram/scripts/telegram_edit.py")
                            .to_owned(),
                    },
                    SkillScript {
                        path: "scripts/telegram_download.py".to_owned(),
                        content: include_str!(
                            "../default_skills/telegram/scripts/telegram_download.py"
                        )
                        .to_owned(),
                    },
                ],
                enable_coco_shim: true,
            },
        ),
    );
    groups
}

// Skill revision ids are content-addressed so builtin revisions remain stable across stores.
#[derive(Serialize)]
struct SkillVersionHashPayload<'a> {
    description: &'a str,
    body: &'a str,
    scripts: &'a [SkillScript],
    enable_coco_shim: bool,
}

fn compute_skill_version_id(
    description: &str,
    body: &str,
    scripts: &[SkillScript],
    enable_coco_shim: bool,
) -> String {
    let payload = serde_json::to_vec(&SkillVersionHashPayload {
        description,
        body,
        scripts,
        enable_coco_shim,
    })
    .expect("skill version hash payload should serialize");

    let mut hasher = Sha256::new();
    hasher.update(format!("skill_version {}\0", payload.len()).as_bytes());
    hasher.update(&payload);

    hex_encode(&hasher.finalize())
}
