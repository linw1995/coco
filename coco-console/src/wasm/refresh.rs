use super::viewport::{
    ViewportState, needs_full_viewport_fetch, needs_full_viewport_jump_fetch, same_viewport,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PendingViewportUpdate {
    None,
    Patch,
    FullFetch,
}

impl PendingViewportUpdate {
    pub fn merge(self, update: Self) -> Self {
        match (self, update) {
            (Self::FullFetch, _) | (_, Self::FullFetch) => Self::FullFetch,
            (Self::Patch, _) | (_, Self::Patch) => Self::Patch,
            (Self::None, Self::None) => Self::None,
        }
    }

    pub fn is_pending(self) -> bool {
        self != Self::None
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ViewportFetch {
    None,
    Patch,
    Full,
}

pub fn pending_update_for_viewport_change(
    previous: ViewportState,
    current: ViewportState,
) -> PendingViewportUpdate {
    if needs_full_viewport_jump_fetch(previous, current) {
        PendingViewportUpdate::FullFetch
    } else {
        PendingViewportUpdate::Patch
    }
}

pub fn viewport_update_active(
    patch_in_flight: bool,
    pending_update: PendingViewportUpdate,
) -> bool {
    patch_in_flight || pending_update.is_pending()
}

pub fn next_viewport_fetch(
    rendered: ViewportState,
    current: ViewportState,
    pending_update: PendingViewportUpdate,
) -> ViewportFetch {
    if pending_update == PendingViewportUpdate::FullFetch
        || needs_full_viewport_fetch(rendered, current)
    {
        return ViewportFetch::Full;
    }
    if same_viewport(rendered, current) {
        ViewportFetch::None
    } else {
        ViewportFetch::Patch
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VersionRefresh {
    Apply,
    Defer,
    Drop,
}

pub fn version_refresh_action(
    captured_viewport: ViewportState,
    current_viewport: ViewportState,
    patch_in_flight: bool,
    pending_update: PendingViewportUpdate,
) -> VersionRefresh {
    if !same_viewport(captured_viewport, current_viewport) {
        return VersionRefresh::Drop;
    }
    if viewport_update_active(patch_in_flight, pending_update) {
        VersionRefresh::Defer
    } else {
        VersionRefresh::Apply
    }
}

#[cfg(test)]
mod tests {
    use super::{
        PendingViewportUpdate, VersionRefresh, ViewportFetch, next_viewport_fetch,
        pending_update_for_viewport_change, version_refresh_action, viewport_update_active,
    };
    use crate::wasm::viewport::ViewportState;

    fn viewport(x: f64) -> ViewportState {
        ViewportState {
            x,
            y: 0.0,
            width: 400.0,
            height: 240.0,
            overscan: 200,
        }
    }

    #[test]
    fn full_fetch_pending_selects_full_even_when_viewports_match() {
        assert_eq!(
            next_viewport_fetch(
                viewport(600.0),
                viewport(600.0),
                PendingViewportUpdate::FullFetch
            ),
            ViewportFetch::Full
        );
    }

    #[test]
    fn nearby_viewport_update_stays_patchable() {
        assert_eq!(
            pending_update_for_viewport_change(viewport(0.0), viewport(300.0)),
            PendingViewportUpdate::Patch
        );
    }

    #[test]
    fn non_overlapping_viewport_update_requests_full_fetch() {
        assert_eq!(
            pending_update_for_viewport_change(viewport(0.0), viewport(400.0)),
            PendingViewportUpdate::FullFetch
        );
    }

    #[test]
    fn full_fetch_update_is_not_downgraded_by_patch_update() {
        assert_eq!(
            PendingViewportUpdate::FullFetch.merge(PendingViewportUpdate::Patch),
            PendingViewportUpdate::FullFetch
        );
    }

    #[test]
    fn viewport_update_stays_active_after_pending_update_is_consumed() {
        assert!(viewport_update_active(true, PendingViewportUpdate::None));
    }

    #[test]
    fn version_refresh_defers_while_pending_viewport_update_waits() {
        assert_eq!(
            version_refresh_action(
                viewport(600.0),
                viewport(600.0),
                false,
                PendingViewportUpdate::FullFetch
            ),
            VersionRefresh::Defer
        );
    }

    #[test]
    fn version_refresh_defers_while_viewport_request_is_in_flight() {
        assert_eq!(
            version_refresh_action(
                viewport(600.0),
                viewport(600.0),
                true,
                PendingViewportUpdate::None
            ),
            VersionRefresh::Defer
        );
    }

    #[test]
    fn version_refresh_drops_stale_viewport_response() {
        assert_eq!(
            version_refresh_action(
                viewport(0.0),
                viewport(600.0),
                false,
                PendingViewportUpdate::None
            ),
            VersionRefresh::Drop
        );
    }

    #[test]
    fn version_refresh_applies_when_viewport_is_idle_and_current() {
        assert_eq!(
            version_refresh_action(
                viewport(600.0),
                viewport(600.0),
                false,
                PendingViewportUpdate::None
            ),
            VersionRefresh::Apply
        );
    }
}
