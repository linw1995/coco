use serde::{Deserialize, Serialize};

pub const DEFAULT_VIEWPORT_WIDTH: i32 = 1280;
pub const DEFAULT_VIEWPORT_HEIGHT: i32 = 720;
pub const DEFAULT_VIEWPORT_OVERSCAN: i32 = 180;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub struct GraphViewportRequest {
    pub x: i32,
    pub y: i32,
    pub width: i32,
    pub height: i32,
    pub overscan: i32,
}

impl Default for GraphViewportRequest {
    fn default() -> Self {
        Self {
            x: 0,
            y: 0,
            width: DEFAULT_VIEWPORT_WIDTH,
            height: DEFAULT_VIEWPORT_HEIGHT,
            overscan: DEFAULT_VIEWPORT_OVERSCAN,
        }
    }
}

impl GraphViewportRequest {
    pub fn normalized(self) -> Self {
        Self {
            x: self.x.max(0),
            y: self.y.max(0),
            width: self.width.max(1),
            height: self.height.max(1),
            overscan: self.overscan.max(0),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GraphViewportDiffRequest {
    pub previous: GraphViewportRequest,
    pub current: GraphViewportRequest,
    pub known: Option<GraphViewportKnownItems>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct GraphViewportKnownItems {
    pub lanes: Vec<String>,
    pub nodes: Vec<String>,
    pub edges: Vec<String>,
}
