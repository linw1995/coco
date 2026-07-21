use std::net::SocketAddr;

use coco_console::{ConsoleConfig, ConsolePublisher, start_console_server_with_graph_store_path};
use coco_mem::{
    Anchor, Kind, MergeParent, NewNode, NodeStore, PromptAnchor, Role, SqliteStore, StoreError,
};

#[tokio::main]
async fn main() {
    let demo_root =
        std::env::temp_dir().join(format!("coco-anchor-edge-lanes-{}", std::process::id()));
    let store = SqliteStore::open(&demo_root)
        .await
        .expect("failed to open demo store");
    let root = store.root_id();

    let alpha_0 = demo_anchor(&store, &root, "Alpha / 0", Vec::new()).await;
    let beta_0 = demo_anchor(&store, &root, "Beta / 0", Vec::new()).await;
    let gamma_0 = demo_anchor(&store, &root, "Gamma / 0", Vec::new()).await;
    let delta_0 = demo_anchor(&store, &root, "Delta / 0", Vec::new()).await;
    let epsilon_0 = demo_anchor(&store, &root, "Epsilon / 0", Vec::new()).await;

    let epsilon_1 = demo_anchor(&store, &epsilon_0, "Epsilon / 1", Vec::new()).await;
    let delta_1 = demo_anchor(&store, &delta_0, "Delta / 1", Vec::new()).await;
    let gamma_1 = demo_anchor(&store, &gamma_0, "Gamma / 1", Vec::new()).await;
    let beta_1 = demo_anchor(&store, &beta_0, "Beta / 1", Vec::new()).await;
    let alpha_1 = demo_anchor(&store, &alpha_0, "Alpha / 1", Vec::new()).await;

    let alpha_2 = demo_anchor(
        &store,
        &alpha_1,
        "Alpha / 2",
        vec![MergeParent::shadow(delta_0.clone())],
    )
    .await;
    let gamma_2 = demo_anchor(
        &store,
        &gamma_1,
        "Gamma / 2",
        vec![MergeParent::merge(alpha_0.clone())],
    )
    .await;
    let epsilon_2 = demo_anchor(
        &store,
        &epsilon_1,
        "Epsilon / 2",
        vec![MergeParent::shadow(beta_0.clone())],
    )
    .await;
    let beta_2 = demo_anchor(&store, &beta_1, "Beta / 2", Vec::new()).await;

    let beta_3 = demo_anchor(
        &store,
        &beta_2,
        "Beta / 3",
        vec![MergeParent::shadow(epsilon_0.clone())],
    )
    .await;
    let epsilon_3 = demo_anchor(
        &store,
        &epsilon_2,
        "Epsilon / 3",
        vec![MergeParent::merge(gamma_0.clone())],
    )
    .await;
    let alpha_3 = demo_anchor(
        &store,
        &alpha_2,
        "Alpha / 3",
        vec![
            MergeParent::merge(beta_0.clone()),
            MergeParent::shadow(epsilon_0.clone()),
        ],
    )
    .await;
    let delta_3 = demo_anchor(
        &store,
        &delta_1,
        "Delta / 3",
        vec![MergeParent::merge(gamma_2.clone())],
    )
    .await;

    let hub_4 = demo_anchor(
        &store,
        &alpha_3,
        "Hub / 4",
        vec![
            MergeParent::merge(delta_3.clone()),
            MergeParent::shadow(gamma_0.clone()),
        ],
    )
    .await;
    let beta_4 = demo_anchor(
        &store,
        &beta_3,
        "Beta / 4",
        vec![MergeParent::shadow(alpha_0.clone())],
    )
    .await;
    let epsilon_4 = demo_anchor(
        &store,
        &epsilon_3,
        "Epsilon / 4",
        vec![
            MergeParent::merge(beta_2.clone()),
            MergeParent::shadow(alpha_0.clone()),
        ],
    )
    .await;

    demo_anchor(
        &store,
        &hub_4,
        "Final convergence / 5",
        vec![
            MergeParent::merge(beta_4),
            MergeParent::merge(epsilon_4),
            MergeParent::shadow(delta_0),
        ],
    )
    .await;

    let server = start_console_server_with_graph_store_path(
        ConsoleConfig {
            addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        },
        store,
        ConsolePublisher::new(),
        demo_root.clone(),
    )
    .await
    .expect("failed to start demo server");
    println!("Demo store: {}", demo_root.display());
    println!("Anchors view: http://{}/?view=anchors", server.addr());
    server.wait().await.expect("demo server failed");
}

async fn demo_anchor(
    store: &impl NodeStore,
    parent: &str,
    prompt: &str,
    merge_parents: Vec<MergeParent>,
) -> String {
    append_anchor(store, parent, prompt, merge_parents)
        .await
        .expect("failed to append demo anchor")
}

async fn append_anchor(
    store: &impl NodeStore,
    parent: &str,
    prompt: &str,
    merge_parents: Vec<MergeParent>,
) -> Result<String, StoreError> {
    store
        .append(NewNode {
            parent: parent.to_owned(),
            role: Role::User,
            metadata: None,
            kind: Kind::Anchor(Anchor::prompt(
                merge_parents,
                PromptAnchor {
                    prompt: prompt.to_owned(),
                    attachments: Vec::new(),
                },
            )),
        })
        .await
}
