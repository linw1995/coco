use coco_types::{SkillGroups, SkillRecord, SkillScript, SkillVersionSpec};

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
                body: include_str!("default_skills/coco-orchestrator.md")
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
                body: include_str!("default_skills/new-skill.md")
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
                body: include_str!("default_skills/cronjob.md").trim().to_owned(),
                scripts: vec![
                    SkillScript {
                        path: "scripts/cronjob_add.py".to_owned(),
                        content: include_str!("default_skills/cronjob/scripts/cronjob_add.py")
                            .to_owned(),
                    },
                    SkillScript {
                        path: "scripts/cronjob_run.py".to_owned(),
                        content: include_str!("default_skills/cronjob/scripts/cronjob_run.py")
                            .to_owned(),
                    },
                    SkillScript {
                        path: "scripts/cronjob_crontab.py".to_owned(),
                        content: include_str!("default_skills/cronjob/scripts/cronjob_crontab.py")
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
                body: include_str!("default_skills/recovery.md").trim().to_owned(),
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
                body: include_str!("default_skills/compact.md").trim().to_owned(),
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
                body: include_str!("default_skills/coco-runner.md")
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
                body: include_str!("default_skills/telegram.md")
                    .trim()
                    .to_owned(),
                scripts: vec![
                    SkillScript {
                        path: "scripts/telegram_send.py".to_owned(),
                        content: include_str!("default_skills/telegram/scripts/telegram_send.py")
                            .to_owned(),
                    },
                    SkillScript {
                        path: "scripts/telegram_edit.py".to_owned(),
                        content: include_str!("default_skills/telegram/scripts/telegram_edit.py")
                            .to_owned(),
                    },
                    SkillScript {
                        path: "scripts/telegram_download.py".to_owned(),
                        content: include_str!("default_skills/telegram/scripts/telegram_download.py")
                            .to_owned(),
                    },
                ],
                enable_coco_shim: true,
            },
        ),
    );
    groups
}
