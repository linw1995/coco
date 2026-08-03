use coco_types::{SessionRole, SkillGroups, SkillRecord, SkillScript, SkillVersion};
use leptos::{html::HtmlElement, prelude::*};

use crate::api::GraphViewportResponse;
use crate::graph_render::{GraphCanvas, GraphCanvasModel};
use crate::host::web_graph_view::ViewMode;
use crate::panels::{
    InitialProviderContext, NODE_DETAIL_PANEL_ID, NodeDetailPanel, ProviderContextPanel,
    render_markdown_nodes,
};

use super::CLIENT_ASSET_VERSION;
use super::markdown::markdown_document;

const HYDRATION_BOOTSTRAP: &str = "__RESOLVED_RESOURCES=[];\
__SERIALIZED_ERRORS=[];\
__PENDING_RESOURCES=[];\
__RESOURCE_RESOLVERS=[];\
__INCOMPLETE_CHUNKS=[];";

pub fn render_index_page(
    mode: ViewMode,
    viewport: &GraphViewportResponse,
    initial_provider_context: Option<InitialProviderContext>,
) -> String {
    render_document(render_root(mode, viewport, initial_provider_context), true)
}

pub fn render_skills_page(
    groups: &SkillGroups,
    requested_role: SessionRole,
    requested_name: Option<&str>,
    requested_version: Option<u64>,
) -> String {
    let role = requested_role;
    let skills = groups.for_role(role);
    let selected = requested_name
        .and_then(|name| skills.get(name))
        .or_else(|| skills.values().next());
    let version = selected.and_then(|skill| {
        requested_version
            .and_then(|version| skill.versions.get(&version))
            .or_else(|| skill.current())
    });
    render_document(render_skills_root(groups, role, selected, version), false)
}

fn render_document(root: AnyView, hydrate: bool) -> String {
    let hydration = hydrate.then(|| {
        let options = LeptosOptions::builder()
            .output_name("coco_console")
            .site_pkg_dir(format!("pkg/{CLIENT_ASSET_VERSION}"))
            .build();
        view! {
            <script>{HYDRATION_BOOTSTRAP}</script>
            <HydrationScripts options=options islands=true/>
        }
    });
    let rendered: View<HtmlElement<_, _, _>> = view! {
        <html lang="en">
            <head>
                <meta charset="utf-8" />
                <meta name="viewport" content="width=device-width, initial-scale=1" />
                <title>"CoCo Console"</title>
                <link rel="stylesheet" href="/style.css" />
                <link rel="license" href="/third-party-notices.html" />
                {hydration}
            </head>
            <body>{root}</body>
        </html>
    };
    format!("<!doctype html>{}", rendered.to_html())
}

fn render_root(
    mode: ViewMode,
    viewport: &GraphViewportResponse,
    initial_provider_context: Option<InitialProviderContext>,
) -> AnyView {
    let revision = viewport.version;
    let stats = format!("{} / revision {}", mode.label(), revision);
    let graph_mode = mode.as_query_value().to_owned();
    let graph = GraphCanvasModel::new(mode == ViewMode::Anchors, viewport);
    let provider_context_panel = view! {
        <ProviderContextPanel graph_mode=graph_mode initial=initial_provider_context/>
    }
    .into_any();
    let node_detail_panel = view! { <NodeDetailPanel/> }.into_any();
    view! {
        <main
            id="console-root"
            class="shell"
            data-version=revision.to_string()
            data-graph-mode=mode.as_query_value()
        >
            <header class="topbar">
                <section class="brand">
                    <h1>"CoCo Console"</h1>
                    <p>"Live node relationship graph from the daemon store."</p>
                </section>
                <section class="topbar-actions">
                    {render_app_nav(AppPage::Graph)}
                    {render_mode_switch(mode)}
                    <p class="stats">{stats}</p>
                </section>
            </header>
            <section class="content">
                <div class="graph-shell">
                    <div class="graph-surface"><GraphCanvas graph/></div>
                    {render_empty_time_scale()}
                </div>
                <section class="provider-context-panel">
                    <div class="provider-context-slot">
                        {provider_context_panel}
                    </div>
                </section>
                <aside class="side">
                    <div id=NODE_DETAIL_PANEL_ID class="node-detail-slot">{node_detail_panel}</div>
                </aside>
            </section>
        </main>
    }
    .into_any()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AppPage {
    Graph,
    Skills,
}

fn render_app_nav(active: AppPage) -> AnyView {
    view! {
        <nav class="app-nav" aria-label="Console section">
            <a class=app_nav_class(active == AppPage::Graph) href="/">"Graph"</a>
            <a class=app_nav_class(active == AppPage::Skills) href="/skills">"Skills"</a>
        </nav>
    }
    .into_any()
}

fn app_nav_class(active: bool) -> &'static str {
    if active {
        "app-nav-item active"
    } else {
        "app-nav-item"
    }
}

fn render_skills_root(
    groups: &SkillGroups,
    role: SessionRole,
    selected: Option<&SkillRecord>,
    version: Option<&SkillVersion>,
) -> AnyView {
    let skill_count = groups.orchestrator.len() + groups.runner.len();
    let stats = format!("{skill_count} skills");
    view! {
        <main class="shell skills-shell">
            <header class="topbar">
                <section class="brand">
                    <h1>"CoCo Console"</h1>
                    <p>"Inspect persisted skills, revisions, and bundled scripts."</p>
                </section>
                <section class="topbar-actions">
                    {render_app_nav(AppPage::Skills)}
                    <p class="stats">{stats}</p>
                </section>
            </header>
            <section class="skills-content">
                {render_skill_catalog(groups, role, selected.map(|skill| skill.name.as_str()))}
                {render_skill_detail(role, selected, version)}
            </section>
        </main>
    }
    .into_any()
}

fn render_skill_catalog(
    groups: &SkillGroups,
    role: SessionRole,
    selected_name: Option<&str>,
) -> AnyView {
    let skills = groups.for_role(role);
    view! {
        <aside class="skills-catalog">
            <nav class="skill-role-switch" aria-label="Skill role">
                {render_skill_role_link(
                    SessionRole::Orchestrator,
                    role,
                    groups.orchestrator.len(),
                )}
                {render_skill_role_link(SessionRole::Runner, role, groups.runner.len())}
            </nav>
            <nav class="skill-list" aria-label="Skills">
                {skills.values().map(|skill| {
                    let current = skill.current();
                    let href = skill_href(role, &skill.name, Some(skill.current_version));
                    let class = if selected_name == Some(skill.name.as_str()) {
                        "skill-list-item active"
                    } else {
                        "skill-list-item"
                    };
                    let summary = current
                        .map(|version| version.description.as_str())
                        .unwrap_or("Missing current version");
                    view! {
                        <a class=class href=href>
                            <span class="skill-list-heading">
                                <strong>{skill.name.clone()}</strong>
                                <span>{format!("v{}", skill.current_version)}</span>
                            </span>
                            <span class="skill-list-summary">{summary.to_owned()}</span>
                        </a>
                    }
                }).collect::<Vec<_>>()}
            </nav>
        </aside>
    }
    .into_any()
}

fn render_skill_role_link(
    link_role: SessionRole,
    active_role: SessionRole,
    count: usize,
) -> AnyView {
    let class = if link_role == active_role {
        "skill-role-item active"
    } else {
        "skill-role-item"
    };
    let label = match link_role {
        SessionRole::Orchestrator => "Orchestrator",
        SessionRole::Runner => "Runner",
    };
    view! {
        <a class=class href=skill_role_href(link_role)>
            <span>{label}</span><small>{count}</small>
        </a>
    }
    .into_any()
}

fn render_skill_detail(
    role: SessionRole,
    skill: Option<&SkillRecord>,
    version: Option<&SkillVersion>,
) -> AnyView {
    let (Some(skill), Some(version)) = (skill, version) else {
        return view! {
            <section class="skill-detail skill-detail-empty">
                <h2>"No skills"</h2>
                <p>"No persisted skills are available for this role."</p>
            </section>
        }
        .into_any();
    };
    let is_current = version.version == skill.current_version;
    let body = markdown_document(&version.body);
    view! {
        <article class="skill-detail">
            <header class="skill-detail-header">
                <div>
                    <p class="skill-eyebrow">{role.as_str()}</p>
                    <h2>{skill.name.clone()}</h2>
                    <p class="skill-description">{version.description.clone()}</p>
                </div>
                <span class:current=is_current class="skill-version-badge">
                    {format!("v{}{}", version.version, if is_current { " · current" } else { "" })}
                </span>
            </header>
            {render_version_switch(role, skill, version.version)}
            <dl class="skill-metadata">
                <div><dt>"Revision ID"</dt><dd><code>{version.id.clone()}</code></dd></div>
                <div><dt>"Created"</dt><dd>{version.created_at.to_string()}</dd></div>
                <div><dt>"Scripts"</dt><dd>{version.scripts.len().to_string()}</dd></div>
                <div>
                    <dt>"CoCo shim"</dt>
                    <dd>{if version.enable_coco_shim { "Enabled" } else { "Disabled" }}</dd>
                </div>
            </dl>
            <section class="skill-section">
                <h3>"Skill instructions"</h3>
                {if version.body.is_empty() {
                    view! { <p class="skill-empty">"Empty"</p> }.into_any()
                } else if let Some(document) = body {
                    view! {
                        <div class="markdown-body skill-markdown">
                            {render_markdown_nodes(document.blocks)}
                        </div>
                    }
                    .into_any()
                } else {
                    view! { <pre class="skill-source">{version.body.clone()}</pre> }.into_any()
                }}
            </section>
            {render_skill_scripts(&version.scripts)}
        </article>
    }
    .into_any()
}

fn render_version_switch(role: SessionRole, skill: &SkillRecord, selected: u64) -> AnyView {
    view! {
        <nav class="skill-version-switch" aria-label="Skill version">
            <span>"Version"</span>
            <div>
                {skill.versions.values().rev().map(|version| {
                    let class = if version.version == selected {
                        "skill-version-item active"
                    } else {
                        "skill-version-item"
                    };
                    let current = (version.version == skill.current_version).then_some("Current");
                    view! {
                        <a class=class href=skill_href(role, &skill.name, Some(version.version))>
                            {format!("v{}", version.version)}
                            {current.map(|label| view! { <small>{label}</small> })}
                        </a>
                    }
                }).collect::<Vec<_>>()}
            </div>
        </nav>
    }
    .into_any()
}

fn render_skill_scripts(scripts: &[SkillScript]) -> AnyView {
    view! {
        <section class="skill-section skill-scripts">
            <div class="skill-section-heading">
                <h3>"Scripts"</h3>
                <span>{scripts.len()}</span>
            </div>
            {if scripts.is_empty() {
                view! { <p class="skill-empty">"No scripts in this version."</p> }.into_any()
            } else {
                view! {
                    <div class="skill-script-list">
                        {scripts.iter().enumerate().map(|(index, script)| {
                            view! {
                                <details class="skill-script" open=index == 0>
                                    <summary>
                                        <code>{script.path.clone()}</code>
                                        <span>{format_script_size(script.content.len())}</span>
                                    </summary>
                                    <pre data-language=script_language(&script.path)>
                                        <code>{script.content.clone()}</code>
                                    </pre>
                                </details>
                            }
                        }).collect::<Vec<_>>()}
                    </div>
                }
                .into_any()
            }}
        </section>
    }
    .into_any()
}

fn skill_role_href(role: SessionRole) -> String {
    format!("/skills?role={}", role.as_str())
}

fn skill_href(role: SessionRole, name: &str, version: Option<u64>) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    serializer.append_pair("role", role.as_str());
    serializer.append_pair("name", name);
    if let Some(version) = version {
        serializer.append_pair("version", &version.to_string());
    }
    format!("/skills?{}", serializer.finish())
}

fn script_language(path: &str) -> Option<&str> {
    path.rsplit_once('.').map(|(_, extension)| extension)
}

fn format_script_size(bytes: usize) -> String {
    if bytes >= 1024 {
        format!("{:.1} KiB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

fn render_mode_switch(mode: ViewMode) -> AnyView {
    let anchors_class = mode_switch_class(mode == ViewMode::Anchors);
    let all_class = mode_switch_class(mode == ViewMode::All);
    view! {
        <nav class="mode-switch" aria-label="Graph mode">
            <a class=anchors_class href="/?mode=anchors">"Anchors"</a>
            <a class=all_class href="/?mode=all">"All"</a>
        </nav>
    }
    .into_any()
}

fn mode_switch_class(active: bool) -> &'static str {
    if active {
        "mode-switch-item active"
    } else {
        "mode-switch-item"
    }
}

fn render_empty_time_scale() -> AnyView {
    view! {
        <nav class="time-scale time-scale-empty" aria-label="Graph time navigator">
            <div class="time-scale-track">
                <div class="time-scale-cursor" style="left: 50%;">
                    <span class="time-scale-label">"Live graph"</span>
                </div>
            </div>
            <div class="time-scale-extents">
                <span>"-"</span>
                <span>"-"</span>
            </div>
        </nav>
    }
    .into_any()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{
        GraphBezierRoute, GraphCanvas, GraphViewport, GraphViewportEdge, GraphViewportEdgeKind,
        GraphViewportNode, Point,
    };
    use coco_types::{SkillScript, SkillUpdatePatch, SkillVersionSpec};

    #[test]
    fn index_contains_graph_bootstrap_contract() {
        let viewport = GraphViewportResponse {
            version: 7,
            canvas: GraphCanvas {
                width: 2400,
                height: 900,
            },
            viewport: GraphViewport {
                x: 1120,
                y: 0,
                width: 1280,
                height: 720,
                overscan: 180,
            },
            nodes: vec![GraphViewportNode {
                key: "node:latest".to_owned(),
                id: "latest".to_owned(),
                node_target: "node-latest".to_owned(),
                short_id: "latest".to_owned(),
                kind: "text".to_owned(),
                summary: "Latest node".to_owned(),
                labels: vec!["main".to_owned()],
                x: 2300,
                y: 120,
            }],
            edges: vec![GraphViewportEdge {
                key: "edge:latest".to_owned(),
                kind: GraphViewportEdgeKind::Primary,
                source_id: "previous".to_owned(),
                target_id: "latest".to_owned(),
                route: GraphBezierRoute {
                    source: Point { x: 2200, y: 120 },
                    control_1: Point { x: 2230, y: 120 },
                    control_2: Point { x: 2250, y: 120 },
                    target: Point { x: 2280, y: 120 },
                },
            }],
        };
        let page = render_index_page(ViewMode::All, &viewport, None);

        assert!(page.contains("data-version=\"7\""));
        assert!(page.contains("data-graph-mode=\"all\""));
        assert!(page.contains("virtual-graph"));
        assert!(page.contains("data-viewport-x=\"1120\""));
        assert!(page.contains("data-canvas-width=\"2400\""));
        assert!(page.contains("viewBox=\"1120 0 1280 720\""));
        assert!(page.contains("preserveAspectRatio=\"xMaxYMin meet\""));
        assert!(page.contains("width=\"2400\" height=\"900\""));
        assert!(page.contains("data-node-id=\"latest\""));
        assert!(page.contains("data-render-key=\"edge:latest\""));
        assert!(!page.contains("Loading graph..."));
        assert!(page.contains(&format!("/pkg/{CLIENT_ASSET_VERSION}/coco_console.js")));
        assert!(page.contains(&format!("/pkg/{CLIENT_ASSET_VERSION}/coco_console_bg.wasm")));
        assert!(!page.contains("\"/pkg/coco_console.js\""));
        let bootstrap = page
            .find("__INCOMPLETE_CHUNKS=[]")
            .expect("hydration globals should be initialized");
        let graph_loader = page
            .find("mod.hydrate();")
            .expect("graph loader should be rendered");
        let island_loader = page
            .find("hydrateIslands(document.body, mod)")
            .expect("island loader should be rendered");
        assert!(bootstrap < graph_loader);
        assert!(graph_loader < island_loader);
        assert!(!page.contains("<!--bo-"));
        assert!(!page.contains("<!--bc-"));
        assert_eq!(page.matches("<leptos-island").count(), 2);
        assert!(page.contains("graph-anchor-range"));
        assert_eq!(
            page.matches("Select a node to inspect its content.")
                .count(),
            1
        );
        assert_eq!(
            page.matches("Select a node to inspect its provider context.")
                .count(),
            1
        );
        assert!(page.contains("href=\"/skills\""));
    }

    #[test]
    fn skills_page_switches_versions_and_renders_scripts() {
        let mut skill = SkillRecord::new(
            "deploy & verify",
            SkillVersionSpec {
                description: "Initial revision".to_owned(),
                body: "# Initial\n\nRun the old workflow.".to_owned(),
                scripts: vec![SkillScript {
                    path: "scripts/old.py".to_owned(),
                    content: "print('<unsafe>')\n".to_owned(),
                }],
                enable_coco_shim: false,
            },
        );
        skill.update(&SkillUpdatePatch {
            description: Some("Current revision".to_owned()),
            body: Some("# Current\n\nRun the new workflow.".to_owned()),
            scripts: Some(vec![SkillScript {
                path: "scripts/new.sh".to_owned(),
                content: "#!/bin/sh\necho ready\n".to_owned(),
            }]),
            enable_coco_shim: Some(true),
        });
        let groups = SkillGroups {
            orchestrator: [(skill.name.clone(), skill)].into_iter().collect(),
            runner: Default::default(),
        };

        let historical = render_skills_page(
            &groups,
            SessionRole::Orchestrator,
            Some("deploy & verify"),
            Some(1),
        );

        assert!(historical.contains("Initial revision"));
        assert!(historical.contains("scripts/old.py"));
        assert!(historical.contains("&lt;unsafe&gt;"));
        assert!(!historical.contains("<unsafe>"));
        assert!(historical.contains("name=deploy+%26+verify"));
        assert!(historical.contains("version=1"));
        assert!(historical.contains("version=2"));
        assert!(historical.contains("v2"));
        assert!(historical.contains("Current"));
        assert!(!historical.contains("#!/bin/sh"));
        assert!(!historical.contains(HYDRATION_BOOTSTRAP));
        assert!(!historical.contains("coco_console.js"));
        assert!(!historical.contains("coco_console_bg.wasm"));

        let current = render_skills_page(
            &groups,
            SessionRole::Orchestrator,
            Some("deploy & verify"),
            Some(99),
        );
        assert!(current.contains("Current revision"));
        assert!(current.contains("scripts/new.sh"));
        assert!(current.contains("v2 · current"));
    }
}
