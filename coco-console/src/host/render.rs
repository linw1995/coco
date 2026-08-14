use coco_types::{SessionRole, SkillGroups, SkillRecord, SkillScript, SkillVersion};
use leptos::{html::HtmlElement, prelude::*};

use crate::api::GraphViewportResponse;
use crate::host::web_graph_view::ViewMode;
use crate::panels::{
    InitialProviderContext, NODE_DETAIL_PANEL_ID, NodeDetailPanel, ProviderContextPanel,
    highlighted_code_views, render_markdown_nodes,
};
use crate::{GraphCanvas, GraphCanvasModel};

use super::CLIENT_ASSET_VERSION;
use super::markdown::markdown_document;

const HYDRATION_BOOTSTRAP: &str = "__RESOLVED_RESOURCES=[];\
__SERIALIZED_ERRORS=[];\
__PENDING_RESOURCES=[];\
__RESOURCE_RESOLVERS=[];\
__INCOMPLETE_CHUNKS=[];";

pub fn render_index_page(
    mode: ViewMode,
    viewport: GraphViewportResponse,
    initial_provider_context: Option<InitialProviderContext>,
) -> String {
    render_document(
        view! { <GraphPage mode viewport initial_provider_context/> }.into_any(),
        true,
    )
}

pub fn render_skills_page(
    groups: SkillGroups,
    requested_role: SessionRole,
    requested_name: Option<&str>,
    requested_version: Option<u64>,
) -> String {
    render_document(
        view! {
            <SkillsPage
                groups
                role=requested_role
                requested_name=requested_name.map(str::to_owned)
                requested_version
            />
        }
        .into_any(),
        false,
    )
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

#[component]
fn GraphPage(
    mode: ViewMode,
    viewport: GraphViewportResponse,
    initial_provider_context: Option<InitialProviderContext>,
) -> impl IntoView {
    let revision = viewport.version;
    let stats = format!("{} / revision {}", mode.label(), revision);
    let graph_mode = mode.as_query_value().to_owned();
    let graph = GraphCanvasModel::new(mode == ViewMode::Anchors, mode == ViewMode::All, &viewport);
    let node_detail_drawer_open = initial_provider_context.is_some();
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
                    <AppNav active=AppPage::Graph/>
                    <ModeSwitch mode/>
                    <p class="stats">{stats}</p>
                </section>
            </header>
            <section
                class="content"
                data-open-drawer=node_detail_drawer_open.then_some("node-detail")
            >
                <div class="graph-shell">
                    <div class="graph-surface"><GraphCanvas graph/></div>
                    <EmptyTimeScale/>
                </div>
                <nav class="side-drawer-toolbar" aria-label="Inspector panels">
                    <button
                        type="button"
                        data-drawer-open="provider-context"
                        aria-controls="provider-context-drawer"
                        aria-expanded="false"
                    >
                        "Context"
                    </button>
                    <button
                        type="button"
                        data-drawer-open="node-detail"
                        aria-controls="node-detail-drawer"
                        aria-expanded=node_detail_drawer_open.to_string()
                    >
                        "Detail"
                    </button>
                </nav>
                <aside
                    id="provider-context-drawer"
                    class="side-drawer provider-context-panel"
                    data-drawer="provider-context"
                    aria-label="Provider Context"
                    aria-hidden="true"
                    inert=""
                >
                    <button
                        type="button"
                        class="side-drawer-close"
                        data-drawer-close=""
                        aria-label="Close Provider Context"
                    >
                        <span aria-hidden="true">"×"</span>
                    </button>
                    <div class="provider-context-slot">
                        <ProviderContextPanel
                            graph_mode=graph_mode
                            initial=initial_provider_context
                        />
                    </div>
                </aside>
                <aside
                    id="node-detail-drawer"
                    class="side-drawer node-detail-panel"
                    data-drawer="node-detail"
                    aria-label="Node Detail"
                    aria-hidden=(!node_detail_drawer_open).to_string()
                    inert=(!node_detail_drawer_open).then_some("")
                >
                    <button
                        type="button"
                        class="side-drawer-close"
                        data-drawer-close=""
                        aria-label="Close Node Detail"
                    >
                        <span aria-hidden="true">"×"</span>
                    </button>
                    <div id=NODE_DETAIL_PANEL_ID class="node-detail-slot">
                        <NodeDetailPanel graph_mode=mode.as_query_value().to_owned()/>
                    </div>
                </aside>
            </section>
        </main>
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AppPage {
    Graph,
    Skills,
}

#[component]
fn AppNav(active: AppPage) -> impl IntoView {
    view! {
        <nav class="app-nav" aria-label="Console section">
            <a class=app_nav_class(active == AppPage::Graph) href="/">"Graph"</a>
            <a class=app_nav_class(active == AppPage::Skills) href="/skills">"Skills"</a>
        </nav>
    }
}

fn app_nav_class(active: bool) -> &'static str {
    if active {
        "app-nav-item active"
    } else {
        "app-nav-item"
    }
}

#[derive(Clone)]
struct SkillCatalogItem {
    name: String,
    current_version: u64,
    summary: String,
}

#[component]
fn SkillsPage(
    mut groups: SkillGroups,
    role: SessionRole,
    requested_name: Option<String>,
    requested_version: Option<u64>,
) -> impl IntoView {
    let orchestrator_count = groups.orchestrator.len();
    let runner_count = groups.runner.len();
    let skill_count = orchestrator_count + runner_count;
    let stats = format!("{skill_count} skills");
    let skills = groups.for_role(role);
    let selected_name = requested_name
        .filter(|name| skills.contains_key(name))
        .or_else(|| skills.keys().next().cloned());
    let catalog = skills
        .values()
        .map(|skill| SkillCatalogItem {
            name: skill.name.clone(),
            current_version: skill.current_version,
            summary: skill
                .current()
                .map(|version| version.description.clone())
                .unwrap_or_else(|| "Missing current version".to_owned()),
        })
        .collect();
    let selected = selected_name
        .as_deref()
        .and_then(|name| groups.for_role_mut(role).remove(name));
    view! {
        <main class="shell skills-shell">
            <header class="topbar">
                <section class="brand">
                    <h1>"CoCo Console"</h1>
                    <p>"Inspect persisted skills, revisions, and bundled scripts."</p>
                </section>
                <section class="topbar-actions">
                    <AppNav active=AppPage::Skills/>
                    <p class="stats">{stats}</p>
                </section>
            </header>
            <section class="skills-content">
                <SkillCatalog
                    role
                    selected_name
                    orchestrator_count
                    runner_count
                    skills=catalog
                />
                <SkillDetail role skill=selected requested_version/>
            </section>
        </main>
    }
}

#[component]
fn SkillCatalog(
    role: SessionRole,
    selected_name: Option<String>,
    orchestrator_count: usize,
    runner_count: usize,
    skills: Vec<SkillCatalogItem>,
) -> impl IntoView {
    view! {
        <aside class="skills-catalog">
            <nav class="skill-role-switch" aria-label="Skill role">
                <SkillRoleLink
                    role=SessionRole::Orchestrator
                    active_role=role
                    count=orchestrator_count
                />
                <SkillRoleLink role=SessionRole::Runner active_role=role count=runner_count/>
            </nav>
            <nav class="skill-list" aria-label="Skills">
                {skills.into_iter().map(|skill| {
                    let href = skill_href(role, &skill.name, Some(skill.current_version));
                    let class = if selected_name.as_deref() == Some(skill.name.as_str()) {
                        "skill-list-item active"
                    } else {
                        "skill-list-item"
                    };
                    view! {
                        <a class=class href=href>
                            <span class="skill-list-heading">
                                <strong>{skill.name}</strong>
                                <span>{format!("v{}", skill.current_version)}</span>
                            </span>
                            <span class="skill-list-summary">{skill.summary}</span>
                        </a>
                    }
                }).collect::<Vec<_>>()}
            </nav>
        </aside>
    }
}

#[component]
fn SkillRoleLink(role: SessionRole, active_role: SessionRole, count: usize) -> impl IntoView {
    let class = if role == active_role {
        "skill-role-item active"
    } else {
        "skill-role-item"
    };
    let label = match role {
        SessionRole::Orchestrator => "Orchestrator",
        SessionRole::Runner => "Runner",
    };
    view! {
        <a class=class href=skill_role_href(role)>
            <span>{label}</span><small>{count}</small>
        </a>
    }
}

#[component]
fn SkillDetail(
    role: SessionRole,
    skill: Option<SkillRecord>,
    requested_version: Option<u64>,
) -> AnyView {
    let Some(mut skill) = skill else {
        return view! {
            <section class="skill-detail skill-detail-empty">
                <h2>"No skills"</h2>
                <p>"No persisted skills are available for this role."</p>
            </section>
        }
        .into_any();
    };
    let selected_version = requested_version
        .filter(|version| skill.versions.contains_key(version))
        .unwrap_or(skill.current_version);
    let versions = skill.versions.keys().rev().copied().collect();
    let Some(version) = skill.versions.remove(&selected_version) else {
        return view! {
            <section class="skill-detail skill-detail-empty">
                <h2>"No skills"</h2>
                <p>"No persisted skills are available for this role."</p>
            </section>
        }
        .into_any();
    };
    let SkillVersion {
        version,
        id,
        created_at,
        description,
        body,
        scripts,
        enable_coco_shim,
    } = version;
    let is_current = version == skill.current_version;
    let script_count = scripts.len();
    view! {
        <article class="skill-detail">
            <header class="skill-detail-header">
                <div>
                    <p class="skill-eyebrow">{role.as_str()}</p>
                    <h2>{skill.name.clone()}</h2>
                    <p class="skill-description">{description}</p>
                </div>
                <span class:current=is_current class="skill-version-badge">
                    {format!("v{version}{}", if is_current { " · current" } else { "" })}
                </span>
            </header>
            <VersionSwitch
                role
                skill_name=skill.name
                versions
                current_version=skill.current_version
                selected_version=version
            />
            <dl class="skill-metadata">
                <div><dt>"Revision ID"</dt><dd><code>{id}</code></dd></div>
                <div><dt>"Created"</dt><dd>{created_at.to_string()}</dd></div>
                <div><dt>"Scripts"</dt><dd>{script_count.to_string()}</dd></div>
                <div>
                    <dt>"CoCo shim"</dt>
                    <dd>{if enable_coco_shim { "Enabled" } else { "Disabled" }}</dd>
                </div>
            </dl>
            <SkillInstructions body/>
            <SkillScripts scripts/>
        </article>
    }
    .into_any()
}

#[component]
fn VersionSwitch(
    role: SessionRole,
    skill_name: String,
    versions: Vec<u64>,
    current_version: u64,
    selected_version: u64,
) -> impl IntoView {
    view! {
        <nav class="skill-version-switch" aria-label="Skill version">
            <span>"Version"</span>
            <div>
                {versions.into_iter().map(|version| {
                    let class = if version == selected_version {
                        "skill-version-item active"
                    } else {
                        "skill-version-item"
                    };
                    let current = (version == current_version).then_some("Current");
                    view! {
                        <a class=class href=skill_href(role, &skill_name, Some(version))>
                            {format!("v{version}")}
                            {current.map(|label| view! { <small>{label}</small> })}
                        </a>
                    }
                }).collect::<Vec<_>>()}
            </div>
        </nav>
    }
}

#[component]
fn SkillInstructions(body: String) -> AnyView {
    let document = markdown_document(&body);
    view! {
        <section class="skill-section">
            <h3>"Skill instructions"</h3>
            {if body.is_empty() {
                view! { <p class="skill-empty">"Empty"</p> }.into_any()
            } else if let Some(document) = document {
                view! {
                    <div class="markdown-body skill-markdown">
                        {render_markdown_nodes(document.blocks)}
                    </div>
                }
                .into_any()
            } else {
                view! { <pre class="skill-source">{body}</pre> }.into_any()
            }}
        </section>
    }
    .into_any()
}

#[component]
fn SkillScripts(scripts: Vec<SkillScript>) -> impl IntoView {
    let count = scripts.len();
    view! {
        <section class="skill-section skill-scripts">
            <div class="skill-section-heading">
                <h3>"Scripts"</h3>
                <span>{count}</span>
            </div>
            {if scripts.is_empty() {
                view! { <p class="skill-empty">"No scripts in this version."</p> }.into_any()
            } else {
                view! {
                    <div class="skill-script-list">
                        {scripts.into_iter().enumerate().map(|(index, script)| {
                            let language = script_language(&script.path).map(str::to_owned);
                            let highlights = language
                                .as_deref()
                                .map(|language| {
                                    super::syntax_highlight::code_highlight_ranges(
                                        language,
                                        &script.content,
                                    )
                                })
                                .unwrap_or_default();
                            let size = format_script_size(script.content.len());
                            let code = highlighted_code_views(script.content, &highlights);
                            view! {
                                <details class="skill-script" open=index == 0>
                                    <summary>
                                        <code>{script.path}</code>
                                        <span>{size}</span>
                                    </summary>
                                    <pre data-language=language>
                                        <code>{code}</code>
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

#[component]
fn ModeSwitch(mode: ViewMode) -> impl IntoView {
    let anchors_class = mode_switch_class(mode == ViewMode::Anchors);
    let all_class = mode_switch_class(mode == ViewMode::All);
    view! {
        <nav class="mode-switch" aria-label="Graph mode">
            <a class=anchors_class href="/?mode=anchors">"Anchors"</a>
            <a class=all_class href="/?mode=all">"All"</a>
        </nav>
    }
}

fn mode_switch_class(active: bool) -> &'static str {
    if active {
        "mode-switch-item active"
    } else {
        "mode-switch-item"
    }
}

#[component]
fn EmptyTimeScale() -> impl IntoView {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{
        GraphBezierRoute, GraphCanvas, GraphErrorNode, GraphJob, GraphViewport, GraphViewportEdge,
        GraphViewportEdgeKind, GraphViewportNode, Point,
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
            error_nodes: vec![GraphErrorNode {
                id: "failed".to_owned(),
                node_target: "detail-failed".to_owned(),
                short_id: "failed".to_owned(),
                created_at: "2026-08-06T09:00:00Z".to_owned(),
                summary: "backend unavailable".to_owned(),
                point: Point { x: 2100, y: 240 },
            }],
            active_job_count: 1,
            jobs: vec![GraphJob {
                id: "job-current".to_owned(),
                short_id: "job-curr".to_owned(),
                created_at: "2026-08-06T09:05:00Z".to_owned(),
                status: "running".to_owned(),
                branch: "main".to_owned(),
                work_branch: "recovery".to_owned(),
                head_id: "latest".to_owned(),
                head_target: "detail-latest".to_owned(),
                head_short_id: "latest".to_owned(),
                point: Point { x: 2300, y: 120 },
                head_anchor_id: "anchor".to_owned(),
                head_anchor_target: "detail-anchor".to_owned(),
                head_anchor_point: Point { x: 2100, y: 240 },
            }],
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
        let anchors_href = crate::error_node_href(&viewport.error_nodes[0], false);
        assert!(anchors_href.contains("mode=all"));
        assert!(anchors_href.contains("graph_focus_target=detail-failed"));
        assert!(anchors_href.contains("graph_focus_x=2100"));
        assert!(anchors_href.contains("graph_focus_y=240"));
        let anchors_job_href = crate::job_href(&viewport.jobs[0], false);
        assert_eq!(anchors_job_href, "#detail-anchor");
        let anchors_head_href =
            crate::job_destination_href(&viewport.jobs[0], false, crate::JobDestination::Head);
        assert!(anchors_head_href.contains("mode=all"));
        assert!(anchors_head_href.contains("graph_focus_target=detail-latest"));
        let all_anchor_href =
            crate::job_destination_href(&viewport.jobs[0], true, crate::JobDestination::HeadAnchor);
        assert!(all_anchor_href.contains("mode=anchors"));
        assert!(all_anchor_href.contains("graph_focus_target=detail-anchor"));

        let page = render_index_page(ViewMode::All, viewport, None);

        assert!(page.contains("data-version=\"7\""));
        assert!(page.contains("data-graph-mode=\"all\""));
        assert!(page.contains("virtual-graph"));
        assert!(page.contains("data-viewport-x=\"1120\""));
        assert!(page.contains("data-canvas-width=\"2400\""));
        assert!(page.contains("viewBox=\"1120 0 1280 720\""));
        assert!(page.contains("preserveAspectRatio=\"xMaxYMin meet\""));
        assert!(page.contains("width=\"2400\" height=\"900\""));
        assert!(page.contains("data-node-id=\"latest\""));
        assert!(page.contains("Error Nodes"));
        assert!(page.contains("Jobs"));
        assert!(page.contains("aria-label=\"Jobs (1 active)\""));
        assert!(page.contains("job-curr"));
        assert!(page.contains("main → recovery"));
        assert!(page.contains("head latest"));
        assert!(page.contains("job-destination-head"));
        assert!(page.contains("job-destination-anchor"));
        assert!(page.contains(">Head</a>"));
        assert!(page.contains(">Head anchor</a>"));
        assert!(page.contains("href=\"#detail-failed\""));
        assert!(page.contains("backend unavailable"));
        assert!(page.contains("data-node-x=\"2100\""));
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
        assert!(page.contains("class=\"side-drawer-toolbar\""));
        assert!(page.contains("data-drawer=\"provider-context\""));
        assert!(page.contains("data-drawer=\"node-detail\""));
        assert_eq!(page.matches("data-drawer-close").count(), 2);
        assert_eq!(page.matches("aria-expanded=\"false\"").count(), 2);
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
            groups.clone(),
            SessionRole::Orchestrator,
            Some("deploy & verify"),
            Some(1),
        );

        assert!(historical.contains("Initial revision"));
        assert!(historical.contains("scripts/old.py"));
        assert!(historical.contains("&lt;unsafe&gt;"));
        assert!(historical.contains("syntax-function"));
        assert!(historical.contains("syntax-string"));
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
            groups,
            SessionRole::Orchestrator,
            Some("deploy & verify"),
            Some(99),
        );
        assert!(current.contains("Current revision"));
        assert!(current.contains("scripts/new.sh"));
        assert!(current.contains("v2 · current"));
    }
}
