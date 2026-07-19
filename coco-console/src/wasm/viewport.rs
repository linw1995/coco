pub const MIN_OVERSCAN: i32 = 180;
const VERTICAL_PREFETCH_MULTIPLIER: f64 = 3.0;
const SHORT_CANVAS_VISIBLE_WIDTH_FRACTION: f64 = 0.74;
const SHORT_CANVAS_VISIBLE_HEIGHT_FRACTION: f64 = 0.82;
const DRAG_THRESHOLD_PX: f64 = 3.0;

#[derive(Debug, Clone, Copy)]
pub struct ViewportState {
    pub x: f64,
    pub y: f64,
    pub width: f64,
    pub height: f64,
    pub overscan: i32,
}

#[derive(Debug, Clone, Copy)]
pub struct ViewportDrag {
    start_client_x: f64,
    start_client_y: f64,
    start_viewport_x: f64,
    start_viewport_y: f64,
    zoom: f64,
}

impl ViewportDrag {
    pub fn new(
        viewport: ViewportState,
        zoom: f64,
        start_client_x: f64,
        start_client_y: f64,
    ) -> Self {
        Self {
            start_client_x,
            start_client_y,
            start_viewport_x: viewport.x,
            start_viewport_y: viewport.y,
            zoom,
        }
    }

    pub fn viewport_origin_at(self, client_x: f64, client_y: f64) -> Option<(f64, f64)> {
        let delta_x = client_x - self.start_client_x;
        let delta_y = client_y - self.start_client_y;
        if delta_x.mul_add(delta_x, delta_y * delta_y) < DRAG_THRESHOLD_PX.powi(2) {
            return None;
        }
        Some((
            self.start_viewport_x - delta_x / self.zoom,
            self.start_viewport_y - delta_y / self.zoom,
        ))
    }
}

impl ViewportState {
    pub fn request_query(self) -> String {
        format!(
            "x={}&y={}&width={}&height={}&overscan={}",
            rounded_i32(self.x),
            rounded_i32(self.y),
            wire_dimension(self.width),
            wire_dimension(self.height),
            self.render_overscan()
        )
    }

    pub fn refresh_render_overscan(&mut self) {
        self.overscan = self.render_overscan();
    }

    pub fn with_render_overscan(mut self) -> Self {
        self.refresh_render_overscan();
        self
    }

    pub fn render_overscan(self) -> i32 {
        let width = f64::from(wire_dimension(self.width));
        let height = f64::from(wire_dimension(self.height));
        ((width.max(height) / 2.0)
            .max(height * VERTICAL_PREFETCH_MULTIPLIER)
            .ceil() as i32)
            .max(MIN_OVERSCAN)
    }
}

#[derive(Debug, Clone, Copy)]
struct ViewportBounds {
    left: f64,
    top: f64,
    right: f64,
    bottom: f64,
}

impl ViewportBounds {
    fn strict(viewport: ViewportState) -> Self {
        Self {
            left: viewport.x,
            top: viewport.y,
            right: viewport.x + viewport.width,
            bottom: viewport.y + viewport.height,
        }
    }

    fn rendered(viewport: ViewportState) -> Self {
        let overscan = f64::from(viewport.overscan);
        Self {
            left: viewport.x - overscan,
            top: viewport.y - overscan,
            right: viewport.x + viewport.width + overscan,
            bottom: viewport.y + viewport.height + overscan,
        }
    }

    fn intersects(self, other: Self) -> bool {
        self.left < other.right
            && other.left < self.right
            && self.top < other.bottom
            && other.top < self.bottom
    }
}

pub fn same_viewport(left: ViewportState, right: ViewportState) -> bool {
    rounded_i32(left.x) == rounded_i32(right.x)
        && rounded_i32(left.y) == rounded_i32(right.y)
        && wire_dimension(left.width) == wire_dimension(right.width)
        && wire_dimension(left.height) == wire_dimension(right.height)
        && left.overscan == right.overscan
}

pub fn needs_full_viewport_fetch(rendered: ViewportState, current: ViewportState) -> bool {
    needs_full_fetch(ViewportBounds::rendered(rendered), current)
}

pub fn needs_full_viewport_jump_fetch(rendered: ViewportState, current: ViewportState) -> bool {
    needs_full_fetch(ViewportBounds::strict(rendered), current)
}

fn needs_full_fetch(rendered: ViewportBounds, current: ViewportState) -> bool {
    !rendered.intersects(ViewportBounds::strict(current))
}

pub fn rounded_i32(value: f64) -> i32 {
    value.round().clamp(0.0, f64::from(i32::MAX)) as i32
}

fn wire_dimension(value: f64) -> i32 {
    rounded_i32(value).max(1)
}

pub fn short_canvas_auto_zoom(
    client_width: f64,
    client_height: f64,
    canvas_width: f64,
    canvas_height: f64,
    current_zoom: f64,
) -> f64 {
    if [
        client_width,
        client_height,
        canvas_width,
        canvas_height,
        current_zoom,
    ]
    .into_iter()
    .any(|value| value <= 0.0)
    {
        return current_zoom;
    }

    let width_zoom = short_axis_zoom(
        client_width,
        canvas_width,
        current_zoom,
        SHORT_CANVAS_VISIBLE_WIDTH_FRACTION,
    );
    let height_zoom = short_axis_zoom(
        client_height,
        canvas_height,
        current_zoom,
        SHORT_CANVAS_VISIBLE_HEIGHT_FRACTION,
    );
    width_zoom.max(height_zoom)
}

fn short_axis_zoom(
    client_size: f64,
    canvas_size: f64,
    current_zoom: f64,
    visible_fraction: f64,
) -> f64 {
    if canvas_size >= client_size {
        return current_zoom;
    }
    (client_size / (canvas_size * visible_fraction)).max(current_zoom)
}

#[cfg(test)]
mod tests {
    use super::{
        ViewportDrag, ViewportState, needs_full_viewport_fetch, needs_full_viewport_jump_fetch,
        same_viewport, short_canvas_auto_zoom,
    };

    fn viewport(x: f64, y: f64) -> ViewportState {
        ViewportState {
            x,
            y,
            width: 400.0,
            height: 240.0,
            overscan: 200,
        }
    }

    #[test]
    fn nearby_viewport_can_use_diff_patch() {
        assert!(!needs_full_viewport_fetch(
            viewport(0.0, 0.0),
            viewport(300.0, 0.0)
        ));
    }

    #[test]
    fn distant_horizontal_viewport_needs_full_fetch() {
        assert!(needs_full_viewport_fetch(
            viewport(0.0, 0.0),
            viewport(600.0, 0.0)
        ));
    }

    #[test]
    fn distant_vertical_viewport_needs_full_fetch() {
        assert!(needs_full_viewport_fetch(
            viewport(0.0, 0.0),
            viewport(0.0, 440.0)
        ));
    }

    #[test]
    fn overlapping_jump_can_use_diff_patch() {
        assert!(!needs_full_viewport_jump_fetch(
            viewport(0.0, 0.0),
            viewport(300.0, 0.0)
        ));
    }

    #[test]
    fn non_overlapping_jump_needs_full_fetch() {
        assert!(needs_full_viewport_jump_fetch(
            viewport(0.0, 0.0),
            viewport(400.0, 0.0)
        ));
    }

    #[test]
    fn request_query_rounds_dimensions_and_uses_render_overscan() {
        let mut viewport = ViewportState {
            x: 1.4,
            y: 2.6,
            width: 401.1,
            height: 239.9,
            overscan: 0,
        };
        viewport.refresh_render_overscan();

        assert_eq!(
            viewport.request_query(),
            "x=1&y=3&width=401&height=240&overscan=720"
        );
    }

    #[test]
    fn render_overscan_uses_wire_rounded_dimensions() {
        let mut viewport = ViewportState {
            x: 0.0,
            y: 0.0,
            width: 800.0,
            height: 594.1,
            overscan: 0,
        };
        viewport.refresh_render_overscan();

        assert_eq!(viewport.overscan, 1782);
        assert_eq!(
            viewport.request_query(),
            "x=0&y=0&width=800&height=594&overscan=1782"
        );
    }

    #[test]
    fn wire_dimensions_match_server_minimum() {
        let current = ViewportState {
            x: 0.0,
            y: 0.0,
            width: 0.4,
            height: 0.49,
            overscan: 0,
        }
        .with_render_overscan();
        let rendered = ViewportState {
            width: 1.0,
            height: 1.0,
            ..current
        }
        .with_render_overscan();

        assert_eq!(
            current.request_query(),
            "x=0&y=0&width=1&height=1&overscan=180"
        );
        assert!(same_viewport(current, rendered));
    }

    #[test]
    fn render_overscan_prefetches_distant_graph_items() {
        let viewport = ViewportState {
            x: 0.0,
            y: 0.0,
            width: 1000.0,
            height: 600.0,
            overscan: 0,
        };

        assert_eq!(viewport.render_overscan(), 1800);
    }

    #[test]
    fn with_render_overscan_normalizes_stale_values() {
        let viewport = ViewportState {
            x: 0.0,
            y: 0.0,
            width: 1000.0,
            height: 600.0,
            overscan: 200,
        }
        .with_render_overscan();

        assert_eq!(viewport.overscan, 1800);
    }

    #[test]
    fn same_viewport_compares_rounded_geometry_and_exact_overscan() {
        assert!(same_viewport(
            ViewportState {
                x: 1.4,
                y: 2.4,
                width: 399.6,
                height: 239.6,
                overscan: 200,
            },
            ViewportState {
                x: 1.49,
                y: 2.49,
                width: 399.51,
                height: 239.51,
                overscan: 200,
            }
        ));
    }

    #[test]
    fn short_canvas_auto_zoom_shows_a_partial_enlarged_canvas() {
        let zoom = short_canvas_auto_zoom(1000.0, 600.0, 720.0, 420.0, 1.0);

        assert!(zoom > 1.0);
        assert!(1000.0 / zoom < 720.0);
        assert!(600.0 / zoom < 420.0);
    }

    #[test]
    fn short_canvas_auto_zoom_keeps_large_canvas_zoom() {
        assert_eq!(
            short_canvas_auto_zoom(1000.0, 600.0, 1600.0, 900.0, 1.25),
            1.25
        );
    }

    #[test]
    fn viewport_drag_moves_against_scaled_pointer_delta_after_threshold() {
        let drag = ViewportDrag::new(viewport(100.0, 80.0), 2.0, 40.0, 30.0);

        assert_eq!(drag.viewport_origin_at(42.0, 32.0), None);
        assert_eq!(drag.viewport_origin_at(60.0, 40.0), Some((90.0, 75.0)));
    }
}
