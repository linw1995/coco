# Credential Proxy

CoCo can expose API credentials to an installed skill through nono's credential
proxy. The sandbox receives a phantom token and a service-specific base URL.
CoCo hands the real credential to the nono supervisor through a mode-`0600`
short-lived file outside the sandbox. nono injects it only into requests that
match the configured endpoint rules. The file is removed with the exec session.

Configured credential routes are available to unified exec sessions. They
expose only phantom credentials; no skill-specific authorization is required.

## Configuration

Store the real secret in the CoCo process environment, then add a route to
`config.toml`:

```toml
[exec.credentials.github]
upstream = "https://api.github.com"
secret = "${GITHUB_TOKEN}"
inject_mode = "header"
inject_header = "Authorization"
credential_format = "Bearer {}"

[[exec.credentials.github.endpoints]]
method = "GET"
path = "/repos/*/issues"
```

The following injection modes are supported:

- `header`: requires `inject_header`; `credential_format` is optional.
- `url_path`: requires `path_pattern` and `path_replacement`.
- `query_param`: requires `query_param_name`.
- `basic_auth`: uses HTTP Basic authentication.

Every route must use an HTTPS upstream and define at least one endpoint rule.
Methods are case-insensitive in the TOML file and normalized to uppercase.
Endpoint paths use nono's path patterns.

The client must honor the base URL created by nono. A service named `github`
receives `GITHUB_BASE_URL`; `telegram` receives `TELEGRAM_BASE_URL`. The
credential environment variable contains only a phantom value inside the
sandbox.

## Sandbox Modes

The credential proxy is active only when the command is actually launched
through nono:

- `COCO_EXEC_SANDBOX=nono` requires nono. Missing nono or a configured secret
  stops the command before the child process starts.
- `COCO_EXEC_SANDBOX=auto` uses nono and the credential proxy when nono is
  available. Otherwise, it falls back to the original unsandboxed shell
  execution.
- `COCO_EXEC_SANDBOX=off` ignores credential proxy routes and uses the original
  unsandboxed shell execution.

The nono execution path inherits only CoCo's environment allowlist. The
fallback paths preserve the original environment inheritance behavior, so a
child process may read plaintext credentials from the CoCo process
environment. Use `COCO_EXEC_SANDBOX=nono` when preventing plaintext credential
access is a security requirement.

The generated nono profile contains only a path to the isolated secret file,
never the real credential value. The secret file is not included in the
sandbox's filesystem grants.
