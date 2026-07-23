use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use snafu::prelude::*;

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NonoCredentialInjectMode {
    Header,
    UrlPath,
    QueryParam,
    BasicAuth,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NonoCredentialEndpoint {
    pub method: String,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NonoCredentialRoute {
    pub service: String,
    pub upstream: String,
    pub secret_env: String,
    pub inject_mode: NonoCredentialInjectMode,
    pub inject_header: Option<String>,
    pub credential_format: Option<String>,
    pub path_pattern: Option<String>,
    pub path_replacement: Option<String>,
    pub query_param_name: Option<String>,
    pub endpoint_rules: Vec<NonoCredentialEndpoint>,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("credential environment variable {name:?} is not set"))]
    MissingSecret { name: String },

    #[snafu(display("duplicate credential service {service:?}"))]
    DuplicateService { service: String },

    #[snafu(display("could not serialize nono credential profile: {source}"))]
    SerializeProfile { source: serde_json::Error },

    #[snafu(display("could not write nono credential profile {path:?}: {source}"))]
    WriteProfile { path: PathBuf, source: io::Error },

    #[snafu(display("could not prepare nono credential secret {path:?}: {source}"))]
    WriteSecret { path: PathBuf, source: io::Error },
}

#[derive(Debug, Serialize)]
struct NonoCredentialProfile {
    network: NonoCredentialNetworkProfile,
}

#[derive(Debug, Serialize)]
struct NonoCredentialNetworkProfile {
    credentials: Vec<String>,
    custom_credentials: BTreeMap<String, NonoCustomCredential>,
}

#[derive(Debug, Serialize)]
struct NonoCustomCredential {
    upstream: String,
    credential_key: String,
    env_var: String,
    inject_mode: NonoCredentialInjectMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    inject_header: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    credential_format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    path_pattern: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    path_replacement: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    query_param_name: Option<String>,
    endpoint_rules: Vec<NonoCredentialEndpointProfile>,
}

#[derive(Debug, Serialize)]
struct NonoCredentialEndpointProfile {
    method: String,
    path: String,
}

pub fn prepare_profile(
    runtime_root: &Path,
    routes: &[NonoCredentialRoute],
) -> Result<Option<PathBuf>, Error> {
    if routes.is_empty() {
        return Ok(None);
    }

    let secret_dir = runtime_root.join("credential-secrets");
    std::fs::create_dir_all(&secret_dir).context(WriteSecretSnafu {
        path: secret_dir.clone(),
    })?;
    #[cfg(unix)]
    std::fs::set_permissions(&secret_dir, std::fs::Permissions::from_mode(0o700)).context(
        WriteSecretSnafu {
            path: secret_dir.clone(),
        },
    )?;

    let mut custom_credentials = BTreeMap::new();
    for route in routes {
        if custom_credentials.contains_key(&route.service) {
            return DuplicateServiceSnafu {
                service: route.service.clone(),
            }
            .fail();
        }

        let secret = std::env::var_os(&route.secret_env)
            .filter(|secret| !secret.is_empty())
            .context(MissingSecretSnafu {
                name: route.secret_env.clone(),
            })?;
        let secret_path = secret_dir.join(&route.service);
        write_secret(&secret_path, &secret)?;

        custom_credentials.insert(
            route.service.clone(),
            NonoCustomCredential {
                upstream: route.upstream.clone(),
                credential_key: format!("file://{}", secret_path.display()),
                env_var: route.secret_env.clone(),
                inject_mode: route.inject_mode,
                inject_header: route.inject_header.clone(),
                credential_format: route.credential_format.clone(),
                path_pattern: route.path_pattern.clone(),
                path_replacement: route.path_replacement.clone(),
                query_param_name: route.query_param_name.clone(),
                endpoint_rules: route
                    .endpoint_rules
                    .iter()
                    .map(|endpoint| NonoCredentialEndpointProfile {
                        method: endpoint.method.clone(),
                        path: endpoint.path.clone(),
                    })
                    .collect(),
            },
        );
    }

    let profile = NonoCredentialProfile {
        network: NonoCredentialNetworkProfile {
            credentials: custom_credentials.keys().cloned().collect(),
            custom_credentials,
        },
    };
    let data = serde_json::to_vec_pretty(&profile).context(SerializeProfileSnafu)?;
    let path = runtime_root.join("nono-credential-profile.json");
    std::fs::write(&path, data).context(WriteProfileSnafu { path: path.clone() })?;
    Ok(Some(path))
}

fn write_secret(path: &Path, secret: &OsStr) -> Result<(), Error> {
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    options.mode(0o600);

    let mut file = options.open(path).context(WriteSecretSnafu {
        path: path.to_path_buf(),
    })?;
    file.write_all(secret.as_encoded_bytes())
        .context(WriteSecretSnafu {
            path: path.to_path_buf(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn telegram_route() -> NonoCredentialRoute {
        NonoCredentialRoute {
            service: "telegram".to_owned(),
            upstream: "https://api.telegram.org".to_owned(),
            secret_env: "COCO_TELEGRAM_BOT_TOKEN".to_owned(),
            inject_mode: NonoCredentialInjectMode::UrlPath,
            inject_header: None,
            credential_format: None,
            path_pattern: Some("/bot{}/".to_owned()),
            path_replacement: Some("/bot{}/".to_owned()),
            query_param_name: None,
            endpoint_rules: vec![NonoCredentialEndpoint {
                method: "POST".to_owned(),
                path: "/bot*/sendMessage".to_owned(),
            }],
        }
    }

    #[tokio::test]
    async fn profile_references_an_isolated_secret_file() {
        let runtime_root = tempfile::tempdir().unwrap();
        let path = crate::with_process_env_async(
            &[("COCO_TELEGRAM_BOT_TOKEN", Some(OsStr::new("real-secret")))],
            || async {
                prepare_profile(runtime_root.path(), &[telegram_route()])
                    .unwrap()
                    .unwrap()
            },
        )
        .await;
        let profile: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();

        assert_eq!(
            profile["network"]["credentials"],
            serde_json::json!(["telegram"])
        );
        let credential = &profile["network"]["custom_credentials"]["telegram"];
        let credential_key = credential["credential_key"].as_str().unwrap();
        assert!(credential_key.starts_with("file:///"));
        assert_eq!(credential["env_var"], "COCO_TELEGRAM_BOT_TOKEN");
        assert_eq!(credential["inject_mode"], "url_path");
        assert_eq!(
            credential["endpoint_rules"][0],
            serde_json::json!({
                "method": "POST",
                "path": "/bot*/sendMessage"
            })
        );
        assert!(
            !serde_json::to_string(&profile)
                .unwrap()
                .contains("real-secret")
        );

        let secret_path = Path::new(credential_key.strip_prefix("file://").unwrap());
        assert_eq!(std::fs::read_to_string(secret_path).unwrap(), "real-secret");
        #[cfg(unix)]
        assert_eq!(
            std::fs::metadata(secret_path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[tokio::test]
    async fn empty_secret_is_reported_as_missing() {
        let runtime_root = tempfile::tempdir().unwrap();
        let error = crate::with_process_env_async(
            &[("COCO_TELEGRAM_BOT_TOKEN", Some(OsStr::new("")))],
            || async { prepare_profile(runtime_root.path(), &[telegram_route()]).unwrap_err() },
        )
        .await;

        assert!(matches!(
            error,
            Error::MissingSecret { name } if name == "COCO_TELEGRAM_BOT_TOKEN"
        ));
    }
}
