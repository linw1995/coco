use std::collections::BTreeSet;
use std::error::Error as StdError;
use std::fmt::{self, Display};

use async_trait::async_trait;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrontierMode {
    HotAll,
    Spilled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrontierConfig {
    pub low_watermark: usize,
    pub high_watermark: usize,
}

impl FrontierConfig {
    pub fn new(low_watermark: usize, high_watermark: usize) -> Self {
        Self {
            low_watermark,
            high_watermark,
        }
    }

    fn validate(self) -> Result<Self, FrontierStoreInvariant> {
        if self.low_watermark == 0 || self.low_watermark >= self.high_watermark {
            return Err(FrontierStoreInvariant::InvalidWatermarks {
                low: self.low_watermark,
                high: self.high_watermark,
            });
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FrontierMetrics {
    pub push_batches: u64,
    pub requested_pushes: u64,
    pub distinct_pushes: u64,
    pub inserted_pushes: u64,
    pub repeated_within_batch_pushes: u64,
    pub already_seen_pushes: u64,
    pub pops: u64,
    pub hot_to_spilled: u64,
    pub spilled_to_hot: u64,
    pub prefix_loads: u64,
    pub full_loads: u64,
    pub stale_minimums: u64,
    pub max_hot_len: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PushBatchStats {
    pub requested: usize,
    pub distinct: usize,
    pub inserted: usize,
    pub repeated_within_batch: usize,
    pub already_seen: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplaceMinOutcome<T> {
    Applied { popped: T, pushed: PushBatchStats },
    StaleMinimum { current: Option<T> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreMutation<T> {
    pub inserted: Vec<T>,
    pub pending_len: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreReplace<T> {
    Applied(StoreMutation<T>),
    StaleMinimum,
}

/// The store is authoritative for both pending membership and lifetime deduplication.
///
/// `replace` must be atomic. When `expected_min` is present, the store must apply no
/// changes unless it is the current minimum pending item. Successfully removed items
/// remain known to the store, so later attempts to insert them are reported as
/// duplicates rather than making them pending again.
#[async_trait]
pub trait ExternalFrontierStore<T>: Send + Sync
where
    T: Clone + Ord + Send + Sync + 'static,
{
    type Error: Send + Sync + 'static;

    async fn pending_len(&self) -> Result<usize, Self::Error>;

    async fn ordered_prefix(&self, limit: usize) -> Result<Vec<T>, Self::Error>;

    async fn load_all(&self) -> Result<Vec<T>, Self::Error>;

    async fn replace(
        &mut self,
        expected_min: Option<&T>,
        additions: &[T],
    ) -> Result<StoreReplace<T>, Self::Error>;

    /// Completes the current minimum together with any work-store checkpoint
    /// owned by the concrete store.
    ///
    /// Stores without an associated work checkpoint can use the frontier-only
    /// replacement semantics.
    async fn complete_minimum(&mut self, expected_min: &T) -> Result<StoreReplace<T>, Self::Error> {
        self.replace(Some(expected_min), &[]).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrontierStoreInvariant {
    InvalidWatermarks { low: usize, high: usize },
    SnapshotNotStrictlyOrdered,
    UnexpectedSnapshotLength { expected: usize, actual: usize },
    UnexpectedMutationLength { expected: usize, actual: usize },
    MutationReturnedUnknownItem,
    MutationReturnedDuplicateItem,
    UnexpectedStaleMinimum,
    ConcurrentMutation,
}

impl Display for FrontierStoreInvariant {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidWatermarks { low, high } => {
                write!(
                    formatter,
                    "frontier watermarks must satisfy 0 < low < high, got low={low}, high={high}"
                )
            }
            Self::SnapshotNotStrictlyOrdered => {
                formatter.write_str("frontier store snapshot is not strictly ordered")
            }
            Self::UnexpectedSnapshotLength { expected, actual } => write!(
                formatter,
                "frontier store snapshot contained {actual} items, expected {expected}"
            ),
            Self::UnexpectedMutationLength { expected, actual } => write!(
                formatter,
                "frontier store mutation reported {actual} pending items, expected {expected}"
            ),
            Self::MutationReturnedUnknownItem => {
                formatter.write_str("frontier store reported an insertion that was not requested")
            }
            Self::MutationReturnedDuplicateItem => {
                formatter.write_str("frontier store reported the same inserted item more than once")
            }
            Self::UnexpectedStaleMinimum => {
                formatter.write_str("frontier store rejected an insertion-only mutation as stale")
            }
            Self::ConcurrentMutation => formatter.write_str(
                "frontier store minimum changed repeatedly while applying an atomic replacement",
            ),
        }
    }
}

impl StdError for FrontierStoreInvariant {}

#[derive(Debug)]
pub enum AdaptiveFrontierError<E> {
    Store(E),
    Invariant(FrontierStoreInvariant),
}

impl<E> From<FrontierStoreInvariant> for AdaptiveFrontierError<E> {
    fn from(value: FrontierStoreInvariant) -> Self {
        Self::Invariant(value)
    }
}

impl<E: Display> Display for AdaptiveFrontierError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(source) => write!(formatter, "frontier store operation failed: {source}"),
            Self::Invariant(source) => Display::fmt(source, formatter),
        }
    }
}

impl<E: StdError + 'static> StdError for AdaptiveFrontierError<E> {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Store(source) => Some(source),
            Self::Invariant(source) => Some(source),
        }
    }
}

pub struct AdaptiveFrontier<T, S>
where
    T: Clone + Ord + Send + Sync + 'static,
    S: ExternalFrontierStore<T>,
{
    store: S,
    config: FrontierConfig,
    mode: FrontierMode,
    pending_len: usize,
    hot: BTreeSet<T>,
    metrics: FrontierMetrics,
}

impl<T, S> AdaptiveFrontier<T, S>
where
    T: Clone + Ord + Send + Sync + 'static,
    S: ExternalFrontierStore<T>,
{
    pub async fn open(
        store: S,
        config: FrontierConfig,
    ) -> Result<Self, AdaptiveFrontierError<S::Error>> {
        let config = config.validate()?;
        let pending_len = store
            .pending_len()
            .await
            .map_err(AdaptiveFrontierError::Store)?;
        let mode = if pending_len <= config.high_watermark {
            FrontierMode::HotAll
        } else {
            FrontierMode::Spilled
        };
        let mut frontier = Self {
            store,
            config,
            mode,
            pending_len,
            hot: BTreeSet::new(),
            metrics: FrontierMetrics::default(),
        };
        frontier.reload_for_mode().await?;
        Ok(frontier)
    }

    pub fn mode(&self) -> FrontierMode {
        self.mode
    }

    pub fn len(&self) -> usize {
        self.pending_len
    }

    pub fn is_empty(&self) -> bool {
        self.pending_len == 0
    }

    pub fn hot_len(&self) -> usize {
        self.hot.len()
    }

    pub fn metrics(&self) -> FrontierMetrics {
        self.metrics
    }

    pub fn into_store(self) -> S {
        self.store
    }

    pub async fn peek_min(&mut self) -> Result<Option<T>, AdaptiveFrontierError<S::Error>> {
        self.ensure_hot().await?;
        Ok(self.hot.first().cloned())
    }

    pub async fn push_batch<I>(
        &mut self,
        items: I,
    ) -> Result<PushBatchStats, AdaptiveFrontierError<S::Error>>
    where
        I: IntoIterator<Item = T>,
    {
        let batch = Batch::new(items);
        if batch.requested == 0 {
            return Ok(PushBatchStats::default());
        }
        let additions = batch.items.iter().cloned().collect::<Vec<_>>();
        let replacement = self
            .store
            .replace(None, &additions)
            .await
            .map_err(AdaptiveFrontierError::Store)?;
        let StoreReplace::Applied(mutation) = replacement else {
            return Err(FrontierStoreInvariant::UnexpectedStaleMinimum.into());
        };
        let pushed = self.validate_mutation(&batch, &mutation, false)?;
        self.apply_insertions(&mutation.inserted);
        self.pending_len = mutation.pending_len;
        self.record_push(pushed);
        self.rebalance().await?;
        Ok(pushed)
    }

    pub async fn pop_min(&mut self) -> Result<Option<T>, AdaptiveFrontierError<S::Error>> {
        for attempt in 0..2 {
            let Some(minimum) = self.peek_min().await? else {
                return Ok(None);
            };
            match self.replace_min(&minimum, std::iter::empty::<T>()).await? {
                ReplaceMinOutcome::Applied { popped, .. } => return Ok(Some(popped)),
                ReplaceMinOutcome::StaleMinimum { .. } if attempt == 1 => {
                    return Err(FrontierStoreInvariant::ConcurrentMutation.into());
                }
                ReplaceMinOutcome::StaleMinimum { .. } => {}
            }
        }
        unreachable!("pop attempts either return or continue")
    }

    /// Atomically removes the current minimum and inserts all replacements.
    ///
    /// Callers that need crash-safe expansion can inspect the minimum, compute its
    /// children, and commit the cursor and children with this operation.
    pub async fn replace_min<I>(
        &mut self,
        expected_min: &T,
        additions: I,
    ) -> Result<ReplaceMinOutcome<T>, AdaptiveFrontierError<S::Error>>
    where
        I: IntoIterator<Item = T>,
    {
        let batch = Batch::new(additions);
        self.ensure_hot().await?;
        let current = self.hot.first().cloned();
        if current.as_ref() != Some(expected_min) {
            self.metrics.stale_minimums = self.metrics.stale_minimums.saturating_add(1);
            return Ok(ReplaceMinOutcome::StaleMinimum { current });
        }

        let values = batch.items.iter().cloned().collect::<Vec<_>>();
        let replacement = self
            .store
            .replace(Some(expected_min), &values)
            .await
            .map_err(AdaptiveFrontierError::Store)?;
        match replacement {
            StoreReplace::Applied(mutation) => {
                let pushed = self.validate_mutation(&batch, &mutation, true)?;
                let removed = self.hot.remove(expected_min);
                debug_assert!(removed, "expected minimum must exist in the hot prefix");
                self.apply_insertions(&mutation.inserted);
                self.pending_len = mutation.pending_len;
                self.metrics.pops = self.metrics.pops.saturating_add(1);
                if pushed.requested > 0 {
                    self.record_push(pushed);
                }
                self.rebalance().await?;
                Ok(ReplaceMinOutcome::Applied {
                    popped: expected_min.clone(),
                    pushed,
                })
            }
            StoreReplace::StaleMinimum => {
                self.metrics.stale_minimums = self.metrics.stale_minimums.saturating_add(1);
                self.refresh_authoritative_state().await?;
                Ok(ReplaceMinOutcome::StaleMinimum {
                    current: self.hot.first().cloned(),
                })
            }
        }
    }

    /// Atomically completes the current minimum in the authoritative store.
    ///
    /// Unlike `replace_min`, a concrete external store may couple this with a
    /// durable work checkpoint. This is the operation used by resumable DAG
    /// traversal after the cursor's projection and child expansion are durable.
    pub async fn complete_min(
        &mut self,
        expected_min: &T,
    ) -> Result<ReplaceMinOutcome<T>, AdaptiveFrontierError<S::Error>> {
        self.ensure_hot().await?;
        let current = self.hot.first().cloned();
        if current.as_ref() != Some(expected_min) {
            self.metrics.stale_minimums = self.metrics.stale_minimums.saturating_add(1);
            return Ok(ReplaceMinOutcome::StaleMinimum { current });
        }

        match self
            .store
            .complete_minimum(expected_min)
            .await
            .map_err(AdaptiveFrontierError::Store)?
        {
            StoreReplace::Applied(mutation) => {
                let batch = Batch::new(std::iter::empty::<T>());
                let pushed = self.validate_mutation(&batch, &mutation, true)?;
                let removed = self.hot.remove(expected_min);
                debug_assert!(removed, "expected minimum must exist in the hot prefix");
                self.pending_len = mutation.pending_len;
                self.metrics.pops = self.metrics.pops.saturating_add(1);
                self.rebalance().await?;
                Ok(ReplaceMinOutcome::Applied {
                    popped: expected_min.clone(),
                    pushed,
                })
            }
            StoreReplace::StaleMinimum => {
                self.metrics.stale_minimums = self.metrics.stale_minimums.saturating_add(1);
                self.refresh_authoritative_state().await?;
                Ok(ReplaceMinOutcome::StaleMinimum {
                    current: self.hot.first().cloned(),
                })
            }
        }
    }

    fn validate_mutation(
        &self,
        batch: &Batch<T>,
        mutation: &StoreMutation<T>,
        removed_minimum: bool,
    ) -> Result<PushBatchStats, FrontierStoreInvariant> {
        let inserted = mutation.inserted.iter().cloned().collect::<BTreeSet<_>>();
        if inserted.len() != mutation.inserted.len() {
            return Err(FrontierStoreInvariant::MutationReturnedDuplicateItem);
        }
        if !inserted.is_subset(&batch.items) {
            return Err(FrontierStoreInvariant::MutationReturnedUnknownItem);
        }
        let expected_len = self
            .pending_len
            .saturating_sub(usize::from(removed_minimum))
            .saturating_add(inserted.len());
        if mutation.pending_len != expected_len {
            return Err(FrontierStoreInvariant::UnexpectedMutationLength {
                expected: expected_len,
                actual: mutation.pending_len,
            });
        }
        Ok(PushBatchStats {
            requested: batch.requested,
            distinct: batch.items.len(),
            inserted: inserted.len(),
            repeated_within_batch: batch.requested.saturating_sub(batch.items.len()),
            already_seen: batch.items.len().saturating_sub(inserted.len()),
        })
    }

    fn apply_insertions(&mut self, inserted: &[T]) {
        match self.mode {
            FrontierMode::HotAll => {
                for item in inserted {
                    self.hot.insert(item.clone());
                    self.trim_hot();
                }
            }
            FrontierMode::Spilled => self.extend_spilled_prefix(inserted),
        }
        self.record_hot_len();
    }

    fn extend_spilled_prefix(&mut self, inserted: &[T]) {
        let Some(boundary) = self.hot.last().cloned() else {
            return;
        };
        for item in inserted.iter().filter(|item| *item <= &boundary) {
            self.hot.insert(item.clone());
            self.trim_hot();
        }
    }

    async fn rebalance(&mut self) -> Result<(), AdaptiveFrontierError<S::Error>> {
        match self.mode {
            FrontierMode::HotAll if self.pending_len > self.config.high_watermark => {
                self.mode = FrontierMode::Spilled;
                self.metrics.hot_to_spilled = self.metrics.hot_to_spilled.saturating_add(1);
                self.trim_hot();
            }
            FrontierMode::Spilled if self.pending_len <= self.config.low_watermark => {
                let hot = self.load_all().await?;
                self.hot = hot;
                self.mode = FrontierMode::HotAll;
                self.metrics.spilled_to_hot = self.metrics.spilled_to_hot.saturating_add(1);
            }
            FrontierMode::Spilled
                if self.hot.len() <= self.config.low_watermark
                    && self.hot.len() < self.pending_len =>
            {
                self.hot = self.load_prefix().await?;
            }
            FrontierMode::HotAll | FrontierMode::Spilled => {}
        }
        self.record_hot_len();
        Ok(())
    }

    async fn ensure_hot(&mut self) -> Result<(), AdaptiveFrontierError<S::Error>> {
        if self.pending_len == 0 {
            self.hot.clear();
            return Ok(());
        }
        if self.hot.is_empty() {
            self.reload_for_mode().await?;
        }
        Ok(())
    }

    async fn refresh_authoritative_state(&mut self) -> Result<(), AdaptiveFrontierError<S::Error>> {
        self.pending_len = self
            .store
            .pending_len()
            .await
            .map_err(AdaptiveFrontierError::Store)?;
        match self.mode {
            FrontierMode::HotAll if self.pending_len > self.config.high_watermark => {
                self.mode = FrontierMode::Spilled;
                self.metrics.hot_to_spilled = self.metrics.hot_to_spilled.saturating_add(1);
            }
            FrontierMode::Spilled if self.pending_len <= self.config.low_watermark => {
                self.mode = FrontierMode::HotAll;
                self.metrics.spilled_to_hot = self.metrics.spilled_to_hot.saturating_add(1);
            }
            FrontierMode::HotAll | FrontierMode::Spilled => {}
        }
        self.reload_for_mode().await
    }

    async fn reload_for_mode(&mut self) -> Result<(), AdaptiveFrontierError<S::Error>> {
        let hot = match self.mode {
            FrontierMode::HotAll => self.load_all().await?,
            FrontierMode::Spilled => self.load_prefix().await?,
        };
        self.hot = hot;
        self.record_hot_len();
        Ok(())
    }

    async fn load_prefix(&mut self) -> Result<BTreeSet<T>, AdaptiveFrontierError<S::Error>> {
        let expected = self.pending_len.min(self.config.high_watermark);
        let values = self
            .store
            .ordered_prefix(self.config.high_watermark)
            .await
            .map_err(AdaptiveFrontierError::Store)?;
        self.metrics.prefix_loads = self.metrics.prefix_loads.saturating_add(1);
        ordered_set(values, expected).map_err(AdaptiveFrontierError::Invariant)
    }

    async fn load_all(&mut self) -> Result<BTreeSet<T>, AdaptiveFrontierError<S::Error>> {
        let values = self
            .store
            .load_all()
            .await
            .map_err(AdaptiveFrontierError::Store)?;
        self.metrics.full_loads = self.metrics.full_loads.saturating_add(1);
        ordered_set(values, self.pending_len).map_err(AdaptiveFrontierError::Invariant)
    }

    fn trim_hot(&mut self) {
        while self.hot.len() > self.config.high_watermark {
            self.hot.pop_last();
        }
    }

    fn record_push(&mut self, pushed: PushBatchStats) {
        self.metrics.push_batches = self.metrics.push_batches.saturating_add(1);
        self.metrics.requested_pushes = self
            .metrics
            .requested_pushes
            .saturating_add(saturating_u64(pushed.requested));
        self.metrics.distinct_pushes = self
            .metrics
            .distinct_pushes
            .saturating_add(saturating_u64(pushed.distinct));
        self.metrics.inserted_pushes = self
            .metrics
            .inserted_pushes
            .saturating_add(saturating_u64(pushed.inserted));
        self.metrics.repeated_within_batch_pushes = self
            .metrics
            .repeated_within_batch_pushes
            .saturating_add(saturating_u64(pushed.repeated_within_batch));
        self.metrics.already_seen_pushes = self
            .metrics
            .already_seen_pushes
            .saturating_add(saturating_u64(pushed.already_seen));
    }

    fn record_hot_len(&mut self) {
        self.metrics.max_hot_len = self.metrics.max_hot_len.max(self.hot.len());
    }
}

struct Batch<T> {
    requested: usize,
    items: BTreeSet<T>,
}

impl<T: Ord> Batch<T> {
    fn new(items: impl IntoIterator<Item = T>) -> Self {
        let mut requested = 0usize;
        let mut unique = BTreeSet::new();
        for item in items {
            requested = requested.saturating_add(1);
            unique.insert(item);
        }
        Self {
            requested,
            items: unique,
        }
    }
}

fn ordered_set<T: Ord>(
    values: Vec<T>,
    expected_len: usize,
) -> Result<BTreeSet<T>, FrontierStoreInvariant> {
    if values.len() != expected_len {
        return Err(FrontierStoreInvariant::UnexpectedSnapshotLength {
            expected: expected_len,
            actual: values.len(),
        });
    }
    if values.windows(2).any(|items| items[0] >= items[1]) {
        return Err(FrontierStoreInvariant::SnapshotNotStrictlyOrdered);
    }
    Ok(values.into_iter().collect())
}

fn saturating_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use super::*;

    #[derive(Debug)]
    struct MemoryFrontierStore<T> {
        pending: BTreeSet<T>,
        seen: BTreeSet<T>,
        reject_next_minimum: bool,
        replacements: usize,
    }

    impl<T: Clone + Ord> MemoryFrontierStore<T> {
        fn new(items: impl IntoIterator<Item = T>) -> Self {
            let pending = items.into_iter().collect::<BTreeSet<_>>();
            Self {
                seen: pending.clone(),
                pending,
                reject_next_minimum: false,
                replacements: 0,
            }
        }

        fn rejecting_next_minimum(mut self) -> Self {
            self.reject_next_minimum = true;
            self
        }
    }

    #[async_trait]
    impl<T> ExternalFrontierStore<T> for MemoryFrontierStore<T>
    where
        T: Clone + Ord + Send + Sync + 'static,
    {
        type Error = Infallible;

        async fn pending_len(&self) -> Result<usize, Self::Error> {
            Ok(self.pending.len())
        }

        async fn ordered_prefix(&self, limit: usize) -> Result<Vec<T>, Self::Error> {
            Ok(self.pending.iter().take(limit).cloned().collect())
        }

        async fn load_all(&self) -> Result<Vec<T>, Self::Error> {
            Ok(self.pending.iter().cloned().collect())
        }

        async fn replace(
            &mut self,
            expected_min: Option<&T>,
            additions: &[T],
        ) -> Result<StoreReplace<T>, Self::Error> {
            self.replacements += 1;
            if expected_min.is_some() && self.reject_next_minimum {
                self.reject_next_minimum = false;
                return Ok(StoreReplace::StaleMinimum);
            }
            if expected_min.is_some_and(|expected| self.pending.first() != Some(expected)) {
                return Ok(StoreReplace::StaleMinimum);
            }
            if let Some(expected) = expected_min {
                self.pending.remove(expected);
            }
            let mut inserted = Vec::new();
            for item in additions {
                if self.seen.insert(item.clone()) {
                    self.pending.insert(item.clone());
                    inserted.push(item.clone());
                }
            }
            Ok(StoreReplace::Applied(StoreMutation {
                inserted,
                pending_len: self.pending.len(),
            }))
        }
    }

    fn config() -> FrontierConfig {
        FrontierConfig::new(2, 4)
    }

    #[test]
    fn frontier_store_invariant_messages_are_stable() {
        let cases = [
            (
                FrontierStoreInvariant::InvalidWatermarks { low: 4, high: 4 },
                "frontier watermarks must satisfy 0 < low < high, got low=4, high=4",
            ),
            (
                FrontierStoreInvariant::SnapshotNotStrictlyOrdered,
                "frontier store snapshot is not strictly ordered",
            ),
            (
                FrontierStoreInvariant::UnexpectedSnapshotLength {
                    expected: 3,
                    actual: 2,
                },
                "frontier store snapshot contained 2 items, expected 3",
            ),
            (
                FrontierStoreInvariant::UnexpectedMutationLength {
                    expected: 5,
                    actual: 7,
                },
                "frontier store mutation reported 7 pending items, expected 5",
            ),
            (
                FrontierStoreInvariant::MutationReturnedUnknownItem,
                "frontier store reported an insertion that was not requested",
            ),
            (
                FrontierStoreInvariant::MutationReturnedDuplicateItem,
                "frontier store reported the same inserted item more than once",
            ),
            (
                FrontierStoreInvariant::UnexpectedStaleMinimum,
                "frontier store rejected an insertion-only mutation as stale",
            ),
            (
                FrontierStoreInvariant::ConcurrentMutation,
                "frontier store minimum changed repeatedly while applying an atomic replacement",
            ),
        ];

        for (invariant, expected) in cases {
            assert_eq!(invariant.to_string(), expected);
        }
    }

    #[tokio::test]
    async fn rejects_invalid_watermarks() {
        let error = AdaptiveFrontier::open(
            MemoryFrontierStore::<i32>::new([]),
            FrontierConfig::new(4, 4),
        )
        .await
        .err()
        .expect("invalid watermarks should fail");

        assert!(matches!(
            error,
            AdaptiveFrontierError::Invariant(FrontierStoreInvariant::InvalidWatermarks {
                low: 4,
                high: 4
            })
        ));
    }

    #[tokio::test]
    async fn hot_frontier_orders_and_deduplicates_items_for_the_run_lifetime() {
        let mut frontier = AdaptiveFrontier::open(MemoryFrontierStore::new([]), config())
            .await
            .unwrap();

        let pushed = frontier.push_batch([3, 1, 2, 1]).await.unwrap();

        assert_eq!(
            pushed,
            PushBatchStats {
                requested: 4,
                distinct: 3,
                inserted: 3,
                repeated_within_batch: 1,
                already_seen: 0,
            }
        );
        assert_eq!(frontier.mode(), FrontierMode::HotAll);
        assert_eq!(frontier.pop_min().await.unwrap(), Some(1));
        assert_eq!(frontier.pop_min().await.unwrap(), Some(2));
        assert_eq!(frontier.pop_min().await.unwrap(), Some(3));
        assert_eq!(frontier.pop_min().await.unwrap(), None);

        let pushed_again = frontier.push_batch([1, 1]).await.unwrap();
        assert_eq!(pushed_again.inserted, 0);
        assert_eq!(pushed_again.repeated_within_batch, 1);
        assert_eq!(pushed_again.already_seen, 1);
        assert!(frontier.is_empty());
        assert_eq!(frontier.metrics().pops, 3);
    }

    #[tokio::test]
    async fn spill_mode_preserves_the_global_minimum_across_batch_pushes() {
        let mut frontier = AdaptiveFrontier::open(MemoryFrontierStore::new(10..=20), config())
            .await
            .unwrap();

        assert_eq!(frontier.mode(), FrontierMode::Spilled);
        assert_eq!(frontier.hot_len(), 4);
        let pushed = frontier.push_batch([30, 0, 15, 30]).await.unwrap();
        assert_eq!(pushed.inserted, 2);
        assert_eq!(pushed.repeated_within_batch, 1);
        assert_eq!(pushed.already_seen, 1);
        assert!(frontier.hot_len() <= 4);

        let mut popped = Vec::new();
        while let Some(item) = frontier.pop_min().await.unwrap() {
            popped.push(item);
            assert!(frontier.hot_len() <= 4);
        }

        assert_eq!(
            popped,
            std::iter::once(0)
                .chain(10..=20)
                .chain(std::iter::once(30))
                .collect::<Vec<_>>()
        );
        assert_eq!(frontier.metrics().spilled_to_hot, 1);
    }

    #[tokio::test]
    async fn high_and_low_watermarks_apply_hysteresis() {
        let mut frontier = AdaptiveFrontier::open(MemoryFrontierStore::new([]), config())
            .await
            .unwrap();

        frontier.push_batch(1..=5).await.unwrap();
        assert_eq!(frontier.mode(), FrontierMode::Spilled);
        assert_eq!(frontier.metrics().hot_to_spilled, 1);

        assert_eq!(frontier.pop_min().await.unwrap(), Some(1));
        assert_eq!(frontier.pop_min().await.unwrap(), Some(2));
        assert_eq!(frontier.pop_min().await.unwrap(), Some(3));
        assert_eq!(frontier.len(), 2);
        assert_eq!(frontier.mode(), FrontierMode::HotAll);
        assert_eq!(frontier.metrics().spilled_to_hot, 1);

        frontier.push_batch([6, 7]).await.unwrap();
        assert_eq!(frontier.len(), 4);
        assert_eq!(frontier.mode(), FrontierMode::HotAll);
        frontier.push_batch([8]).await.unwrap();
        assert_eq!(frontier.mode(), FrontierMode::Spilled);
        assert_eq!(frontier.metrics().hot_to_spilled, 2);
    }

    #[tokio::test]
    async fn replacing_the_minimum_makes_smaller_children_immediately_visible() {
        let mut frontier = AdaptiveFrontier::open(MemoryFrontierStore::new([10, 20]), config())
            .await
            .unwrap();

        let outcome = frontier.replace_min(&10, [20, 15, 5]).await.unwrap();

        let ReplaceMinOutcome::Applied { popped, pushed } = outcome else {
            panic!("the expected minimum should be replaced");
        };
        assert_eq!(popped, 10);
        assert_eq!(pushed.inserted, 2);
        assert_eq!(pushed.repeated_within_batch, 0);
        assert_eq!(pushed.already_seen, 1);
        assert_eq!(frontier.pop_min().await.unwrap(), Some(5));
        assert_eq!(frontier.pop_min().await.unwrap(), Some(15));
        assert_eq!(frontier.pop_min().await.unwrap(), Some(20));
    }

    #[tokio::test]
    async fn stale_replacement_never_attaches_children_to_another_minimum() {
        let store = MemoryFrontierStore::new([10, 20]).rejecting_next_minimum();
        let mut frontier = AdaptiveFrontier::open(store, config()).await.unwrap();

        let outcome = frontier.replace_min(&10, [5, 15]).await.unwrap();

        assert_eq!(
            outcome,
            ReplaceMinOutcome::StaleMinimum { current: Some(10) }
        );
        assert_eq!(frontier.len(), 2);
        assert_eq!(frontier.metrics().stale_minimums, 1);

        let outcome = frontier.replace_min(&10, [5, 15]).await.unwrap();
        assert!(matches!(
            outcome,
            ReplaceMinOutcome::Applied { popped: 10, .. }
        ));
        assert_eq!(frontier.pop_min().await.unwrap(), Some(5));
    }

    #[tokio::test]
    async fn caller_expected_minimum_must_match_the_cached_global_minimum() {
        let mut frontier = AdaptiveFrontier::open(MemoryFrontierStore::new([10, 20]), config())
            .await
            .unwrap();

        let outcome = frontier.replace_min(&20, [15]).await.unwrap();

        assert_eq!(
            outcome,
            ReplaceMinOutcome::StaleMinimum { current: Some(10) }
        );
        assert_eq!(frontier.len(), 2);
        assert_eq!(frontier.metrics().stale_minimums, 1);
        let store = frontier.into_store();
        assert_eq!(store.replacements, 0);
    }

    #[tokio::test]
    async fn reopening_reconstructs_the_hot_prefix_from_the_authoritative_store() {
        let mut frontier = AdaptiveFrontier::open(MemoryFrontierStore::new(1..=10), config())
            .await
            .unwrap();
        assert_eq!(frontier.pop_min().await.unwrap(), Some(1));
        assert_eq!(frontier.pop_min().await.unwrap(), Some(2));
        let store = frontier.into_store();

        let mut reopened = AdaptiveFrontier::open(store, config()).await.unwrap();
        let pushed = reopened.push_batch([1, 11]).await.unwrap();

        assert_eq!(pushed.inserted, 1);
        assert_eq!(pushed.repeated_within_batch, 0);
        assert_eq!(pushed.already_seen, 1);
        let mut popped = Vec::new();
        while let Some(item) = reopened.pop_min().await.unwrap() {
            popped.push(item);
        }
        assert_eq!(popped, (3..=11).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn refills_keep_memory_bounded_during_a_large_drain() {
        let mut frontier = AdaptiveFrontier::open(
            MemoryFrontierStore::new(0..1_000),
            FrontierConfig::new(8, 16),
        )
        .await
        .unwrap();

        for expected in 0..1_000 {
            assert_eq!(frontier.pop_min().await.unwrap(), Some(expected));
            assert!(frontier.hot_len() <= 16);
        }

        assert!(frontier.is_empty());
        assert!(frontier.metrics().prefix_loads > 1);
        assert_eq!(frontier.metrics().max_hot_len, 16);
    }
}
