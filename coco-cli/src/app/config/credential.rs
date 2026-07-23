use std::collections::HashMap;

use coco_llm::{NonoCredentialEndpoint, NonoCredentialInjectMode, NonoCredentialRoute};
use serde::Deserialize;

use super::parse_env_placeholder;
use crate::{Result, error::InvalidCredentialRouteSnafu};

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
pub struct ExecConfig {
    #[serde(default)]
    credentials: HashMap<String, CredentialRouteConfig>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
struct CredentialRouteConfig {
    upstream: String,
    secret: String,
    inject_mode: NonoCredentialInjectMode,
    inject_header: Option<String>,
    credential_format: Option<String>,
    path_pattern: Option<String>,
    path_replacement: Option<String>,
    query_param_name: Option<String>,
    #[serde(default)]
    endpoints: Vec<CredentialEndpointConfig>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
struct CredentialEndpointConfig {
    method: String,
    path: String,
}

pub fn resolve_routes(config: ExecConfig) -> Result<Vec<NonoCredentialRoute>> {
    let mut services = config.credentials.into_iter().collect::<Vec<_>>();
    services.sort_by(|left, right| left.0.cmp(&right.0));
    services
        .into_iter()
        .map(|(service, route)| resolve_route(service, route))
        .collect()
}

fn resolve_route(service: String, route: CredentialRouteConfig) -> Result<NonoCredentialRoute> {
    validate_route(&service, &route)?;
    let secret_env = parse_env_placeholder(&route.secret)
        .filter(|name| is_valid_env_name(name))
        .ok_or_else(|| {
            invalid_route(
                &service,
                "secret must be an environment reference such as ${API_TOKEN}",
            )
        })?
        .to_owned();

    Ok(NonoCredentialRoute {
        service,
        upstream: route.upstream,
        secret_env,
        inject_mode: route.inject_mode,
        inject_header: route.inject_header,
        credential_format: route.credential_format,
        path_pattern: route.path_pattern,
        path_replacement: route.path_replacement,
        query_param_name: route.query_param_name,
        endpoint_rules: route
            .endpoints
            .into_iter()
            .map(|endpoint| NonoCredentialEndpoint {
                method: endpoint.method.to_ascii_uppercase(),
                path: endpoint.path,
            })
            .collect(),
    })
}

fn validate_route(service: &str, route: &CredentialRouteConfig) -> Result<()> {
    if service.is_empty()
        || !service
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(invalid_route(
            service,
            "service name must contain only ASCII letters, digits, '-' or '_'",
        ));
    }

    let upstream = url::Url::parse(&route.upstream)
        .map_err(|_| invalid_route(service, "upstream must be a valid HTTPS URL"))?;
    if upstream.scheme() != "https"
        || upstream.host_str().is_none()
        || !upstream.username().is_empty()
        || upstream.password().is_some()
        || upstream.fragment().is_some()
    {
        return Err(invalid_route(
            service,
            "upstream must be an HTTPS URL without credentials or a fragment",
        ));
    }

    if route.endpoints.is_empty() {
        return Err(invalid_route(
            service,
            "at least one endpoint rule is required",
        ));
    }
    for endpoint in &route.endpoints {
        if endpoint.method.is_empty()
            || (endpoint.method != "*"
                && !endpoint
                    .method
                    .bytes()
                    .all(|byte| byte.is_ascii_alphabetic()))
            || !endpoint.path.starts_with('/')
        {
            return Err(invalid_route(
                service,
                "endpoint method must be an HTTP method or '*' and path must start with '/'",
            ));
        }
    }

    match route.inject_mode {
        NonoCredentialInjectMode::Header => {
            if route.inject_header.as_deref().is_none_or(str::is_empty) {
                return Err(invalid_route(
                    service,
                    "header injection requires inject_header",
                ));
            }
            if route
                .credential_format
                .as_deref()
                .is_some_and(|format| !format.contains("{}"))
            {
                return Err(invalid_route(
                    service,
                    "credential_format must contain a '{}' placeholder",
                ));
            }
        }
        NonoCredentialInjectMode::UrlPath => {
            let (Some(path_pattern), Some(path_replacement)) = (
                route
                    .path_pattern
                    .as_deref()
                    .filter(|value| !value.is_empty()),
                route
                    .path_replacement
                    .as_deref()
                    .filter(|value| !value.is_empty()),
            ) else {
                return Err(invalid_route(
                    service,
                    "URL path injection requires path_pattern and path_replacement",
                ));
            };
            if !path_pattern.contains("{}") || !path_replacement.contains("{}") {
                return Err(invalid_route(
                    service,
                    "path_pattern and path_replacement must contain a '{}' placeholder",
                ));
            }
        }
        NonoCredentialInjectMode::QueryParam => {
            if route.query_param_name.as_deref().is_none_or(str::is_empty) {
                return Err(invalid_route(
                    service,
                    "query parameter injection requires query_param_name",
                ));
            }
        }
        NonoCredentialInjectMode::BasicAuth => {}
    }

    Ok(())
}

fn invalid_route(service: &str, message: &str) -> crate::Error {
    InvalidCredentialRouteSnafu {
        service: service.to_owned(),
        message: message.to_owned(),
    }
    .build()
}

fn is_valid_env_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}
