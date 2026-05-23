use std::path::{Path, PathBuf};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use coco_mem::Tool;
use serde::Deserialize;
use snafu::prelude::*;

use crate::ToolInvocationContext;

#[derive(Debug, Clone)]
pub(crate) struct ImageToolRuntime {
    definition: Tool,
    workspace_root: PathBuf,
}

#[derive(Debug, Snafu)]
pub enum ImageToolError {
    #[snafu(display("load_image arguments must be valid JSON: {source}"))]
    ParseArgs { source: serde_json::Error },

    #[snafu(display("load_image source {source_kind:?} requires {field}"))]
    MissingField {
        source_kind: ImageSourceKind,
        field: &'static str,
    },

    #[snafu(display("local image path {path:?} is outside workspace {workspace:?}"))]
    PathOutsideWorkspace { path: PathBuf, workspace: PathBuf },

    #[snafu(display("failed to read local image {path:?}: {source}"))]
    ReadLocalImage {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("unable to resolve local image path {path:?}: {source}"))]
    ResolveLocalImage {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("image media type {media_type:?} is not supported"))]
    UnsupportedMediaType { media_type: String },
}

impl From<ImageToolError> for rig::tool::ToolError {
    fn from(error: ImageToolError) -> Self {
        Self::ToolCallError(Box::new(error))
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ImageSourceKind {
    LocalPath,
    Url,
}

#[derive(Debug, Deserialize)]
struct LoadImageArgs {
    source: ImageSourceKind,
    path: Option<String>,
    url: Option<String>,
    media_type: Option<String>,
}

pub(crate) fn load_runtime(definition: Tool, workspace_root: PathBuf) -> ImageToolRuntime {
    ImageToolRuntime {
        definition,
        workspace_root,
    }
}

fn require_image_media_type(media_type: String) -> Result<String, ImageToolError> {
    if matches!(
        media_type.as_str(),
        "image/jpeg"
            | "image/png"
            | "image/gif"
            | "image/webp"
            | "image/heic"
            | "image/heif"
            | "image/svg+xml"
    ) {
        Ok(media_type)
    } else {
        UnsupportedMediaTypeSnafu { media_type }.fail()
    }
}

fn guess_media_type(path: &Path, explicit: Option<String>) -> Result<String, ImageToolError> {
    match explicit {
        Some(media_type) => require_image_media_type(media_type),
        None => require_image_media_type(
            mime_guess::from_path(path)
                .first_raw()
                .unwrap_or("application/octet-stream")
                .to_owned(),
        ),
    }
}

fn resolve_local_path(workspace_root: &Path, path: &str) -> Result<PathBuf, ImageToolError> {
    let requested = PathBuf::from(path);
    let joined = if requested.is_absolute() {
        requested
    } else {
        workspace_root.join(requested)
    };
    let resolved = joined.canonicalize().context(ResolveLocalImageSnafu {
        path: joined.clone(),
    })?;
    let workspace = workspace_root
        .canonicalize()
        .context(ResolveLocalImageSnafu {
            path: workspace_root.to_path_buf(),
        })?;
    ensure!(
        resolved.starts_with(&workspace),
        PathOutsideWorkspaceSnafu {
            path: resolved,
            workspace,
        }
    );
    Ok(resolved)
}

fn image_json(data: String, media_type: String) -> Result<String, ImageToolError> {
    Ok(serde_json::json!({
        "type": "image",
        "data": data,
        "mimeType": require_image_media_type(media_type)?,
    })
    .to_string())
}

async fn load_local_image(
    args: LoadImageArgs,
    workspace_root: PathBuf,
) -> Result<String, ImageToolError> {
    let path = args.path.context(MissingFieldSnafu {
        source_kind: args.source,
        field: "path",
    })?;
    let resolved = resolve_local_path(&workspace_root, &path)?;
    let media_type = guess_media_type(&resolved, args.media_type)?;
    let bytes = tokio::fs::read(&resolved)
        .await
        .context(ReadLocalImageSnafu { path: resolved })?;
    image_json(BASE64_STANDARD.encode(bytes), media_type)
}

async fn load_url_image(args: LoadImageArgs) -> Result<String, ImageToolError> {
    let url = args.url.context(MissingFieldSnafu {
        source_kind: args.source,
        field: "url",
    })?;
    let media_type = args.media_type.context(MissingFieldSnafu {
        source_kind: args.source,
        field: "media_type",
    })?;
    image_json(url, media_type)
}

impl ImageToolRuntime {
    pub fn tool_definition(&self) -> rig::completion::ToolDefinition {
        rig::completion::ToolDefinition {
            name: self.definition.name.clone(),
            description: self.definition.description.clone(),
            parameters: self.definition.input_schema.clone(),
        }
    }

    pub async fn execute(
        &self,
        args: String,
        _invocation: ToolInvocationContext,
    ) -> std::result::Result<String, rig::tool::ToolError> {
        let args: LoadImageArgs = serde_json::from_str(&args).context(ParseArgsSnafu)?;
        let workspace_root = self.workspace_root.clone();
        Ok(match args.source {
            ImageSourceKind::LocalPath => load_local_image(args, workspace_root).await?,
            ImageSourceKind::Url => load_url_image(args).await?,
        })
    }
}

impl rig::tool::ToolDyn for ImageToolRuntime {
    fn name(&self) -> String {
        self.definition.name.clone()
    }

    fn definition(
        &self,
        _prompt: String,
    ) -> rig::wasm_compat::WasmBoxedFuture<'_, rig::completion::ToolDefinition> {
        let definition = self.tool_definition();
        Box::pin(async move { definition })
    }

    fn call(
        &self,
        args: String,
    ) -> rig::wasm_compat::WasmBoxedFuture<'_, std::result::Result<String, rig::tool::ToolError>>
    {
        Box::pin(async move { self.execute(args, ToolInvocationContext::default()).await })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;
    use serde_json::json;

    use super::*;

    fn load_image_tool_definition() -> Tool {
        crate::builtin_tool_definition("load_image").expect("builtin tool should exist")
    }

    #[tokio::test]
    async fn local_image_returns_rig_image_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tiny.png");
        tokio::fs::write(&path, b"not-a-real-png").await.unwrap();
        let runtime = load_runtime(load_image_tool_definition(), dir.path().to_path_buf());

        let output = runtime
            .execute(
                json!({
                    "source": "local_path",
                    "path": "tiny.png"
                })
                .to_string(),
                ToolInvocationContext::default(),
            )
            .await
            .unwrap();
        let value: Value = serde_json::from_str(&output).unwrap();

        assert_eq!(value["type"], "image");
        assert_eq!(value["mimeType"], "image/png");
        assert_eq!(value["data"], BASE64_STANDARD.encode(b"not-a-real-png"));
    }

    #[tokio::test]
    async fn local_image_rejects_paths_outside_workspace() {
        let workspace = tempfile::tempdir().unwrap();
        let outside = tempfile::NamedTempFile::new().unwrap();
        let runtime = load_runtime(load_image_tool_definition(), workspace.path().to_path_buf());

        let error = runtime
            .execute(
                json!({
                    "source": "local_path",
                    "path": outside.path()
                })
                .to_string(),
                ToolInvocationContext::default(),
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("outside workspace"));
    }

    #[tokio::test]
    async fn url_image_requires_media_type() {
        let runtime = load_runtime(load_image_tool_definition(), PathBuf::from("."));

        let error = runtime
            .execute(
                json!({
                    "source": "url",
                    "url": "https://example.com/image.png"
                })
                .to_string(),
                ToolInvocationContext::default(),
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("media_type"));
    }
}
