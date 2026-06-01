pub const MIN_OVERSCAN: i32 = 180;

#[derive(Debug, Clone, Copy)]
pub struct ViewportState {
    pub x: f64,
    pub y: f64,
    pub width: f64,
    pub height: f64,
    pub overscan: i32,
}

impl ViewportState {
    pub fn request_query(self) -> String {
        format!(
            "x={}&y={}&width={}&height={}&overscan={}",
            rounded_i32(self.x),
            rounded_i32(self.y),
            rounded_i32(self.width),
            rounded_i32(self.height),
            self.render_overscan()
        )
    }

    pub fn refresh_render_overscan(&mut self) {
        self.overscan = self.render_overscan();
    }

    pub fn render_overscan(self) -> i32 {
        ((self.width.max(self.height) / 2.0).ceil() as i32).max(MIN_OVERSCAN)
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
        && rounded_i32(left.width) == rounded_i32(right.width)
        && rounded_i32(left.height) == rounded_i32(right.height)
        && left.overscan == right.overscan
}

pub fn needs_full_viewport_fetch(rendered: ViewportState, current: ViewportState) -> bool {
    needs_full_fetch(ViewportBounds::rendered(rendered), current)
}

pub fn needs_full_viewport_jump_fetch(rendered: ViewportState, current: ViewportState) -> bool {
    needs_full_fetch(ViewportBounds::strict(rendered), current)
}

pub fn bounds_visible_in_viewport(
    viewport: ViewportState,
    left: f64,
    top: f64,
    right: f64,
    bottom: f64,
) -> bool {
    left < viewport.x + viewport.width
        && right > viewport.x
        && top < viewport.y + viewport.height
        && bottom > viewport.y
}

fn needs_full_fetch(rendered: ViewportBounds, current: ViewportState) -> bool {
    !rendered.intersects(ViewportBounds::strict(current))
}

pub fn rounded_i32(value: f64) -> i32 {
    value.round().clamp(0.0, f64::from(i32::MAX)) as i32
}

#[cfg(test)]
mod tests {
    use super::{
        ViewportState, bounds_visible_in_viewport, needs_full_viewport_fetch,
        needs_full_viewport_jump_fetch, same_viewport,
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
    fn graph_item_bounds_visibility_uses_strict_viewport() {
        let current = viewport(200.0, 140.0);

        assert!(bounds_visible_in_viewport(
            current, 180.0, 130.0, 230.0, 190.0
        ));
        assert!(!bounds_visible_in_viewport(
            current, 10.0, 130.0, 180.0, 190.0
        ));
        assert!(!bounds_visible_in_viewport(
            current, 180.0, 10.0, 230.0, 120.0
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
            "x=1&y=3&width=401&height=240&overscan=201"
        );
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
}
