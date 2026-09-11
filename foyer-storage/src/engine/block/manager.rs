// Copyright 2026 foyer Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    collections::{HashSet, VecDeque},
    fmt::Debug,
    ops::{Deref, DerefMut},
    sync::{
        Arc, RwLock, RwLockWriteGuard,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use asyncband::oneshot;
use foyer_common::{
    error::{ErrorKind, Result},
    metrics::Metrics,
    spawn::Spawner,
};
use futures_core::future::BoxFuture;
use futures_util::{
    FutureExt,
    future::{Shared, ready},
};
use itertools::Itertools;
use rand::seq::IteratorRandom;

use crate::{
    Device,
    engine::block::{
        eviction::{EvictionInfo, EvictionPicker},
        reclaimer::ReclaimerTrait,
    },
    io::{
        bytes::{IoB, IoBuf, IoBufMut},
        device::Partition,
        engine::IoEngine,
    },
};

pub type BlockId = u32;

/// Block statistics.
#[derive(Debug, Default)]
pub struct BlockStatistics {
    /// Estimated invalid bytes in the block.
    /// FIXME(MrCroxx): This value is way too coarse. Need fix.
    pub invalid: AtomicUsize,
    /// Access count of the block.
    pub access: AtomicUsize,
    /// Marked as `true` if the block is about to be evicted by some eviction picker.
    pub probation: AtomicBool,
}

impl BlockStatistics {
    pub(crate) fn reset(&self) {
        self.invalid.store(0, Ordering::Relaxed);
        self.access.store(0, Ordering::Relaxed);
        self.probation.store(false, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct BlockInner {
    id: BlockId,
    partition: Arc<dyn Partition>,
    io_engine: Arc<dyn IoEngine>,
    statistics: Arc<BlockStatistics>,
}

/// A block is a logical partition of a device. It is used to manage the device's storage space.
#[derive(Debug, Clone)]
pub struct Block {
    inner: Arc<BlockInner>,
}

impl Block {
    /// Get block id.
    pub fn id(&self) -> BlockId {
        self.inner.id
    }

    /// Get block Statistics.
    pub fn statistics(&self) -> &Arc<BlockStatistics> {
        &self.inner.statistics
    }

    /// Get block size.
    pub fn size(&self) -> usize {
        self.inner.partition.size()
    }

    pub(crate) async fn write(&self, buf: Box<dyn IoBuf>, offset: u64) -> (Box<dyn IoB>, Result<()>) {
        let (buf, res) = self
            .inner
            .io_engine
            .write(buf, self.inner.partition.as_ref(), offset)
            .await;
        (buf, res)
    }

    pub(crate) async fn read(&self, buf: Box<dyn IoBufMut>, offset: u64) -> (Box<dyn IoB>, Result<()>) {
        let (buf, res) = self
            .inner
            .io_engine
            .read(buf, self.inner.partition.as_ref(), offset)
            .await;
        (buf, res)
    }

    pub(crate) fn partition(&self) -> &Arc<dyn Partition> {
        &self.inner.partition
    }
}

#[cfg(test)]
impl Block {
    pub(crate) fn new_for_test(id: BlockId, partition: Arc<dyn Partition>, io_engine: Arc<dyn IoEngine>) -> Self {
        let inner = BlockInner {
            id,
            partition,
            io_engine,
            statistics: Arc::<BlockStatistics>::default(),
        };
        let inner = Arc::new(inner);
        Self { inner }
    }
}

pub type GetCleanBlockHandle = Shared<BoxFuture<'static, Block>>;

#[derive(Debug)]
struct State {
    clean_blocks: VecDeque<BlockId>,
    evictable_blocks: HashSet<BlockId>,
    writing_blocks: HashSet<BlockId>,
    reclaiming_blocks: HashSet<BlockId>,

    clean_block_waiters: Vec<oneshot::Sender<Block>>,

    eviction_pickers: Vec<Box<dyn EvictionPicker>>,

    reclaim_waiters: Vec<oneshot::Sender<()>>,
}

#[derive(Debug)]
struct Inner {
    blocks: Vec<Block>,
    state: RwLock<State>,
    reclaimer: Arc<dyn ReclaimerTrait>,
    reclaim_concurrency: usize,
    clean_block_threshold: usize,
    metrics: Arc<Metrics>,
    spawner: Spawner,
}

#[derive(Debug, Clone)]
pub struct BlockManager {
    inner: Arc<Inner>,
}

impl BlockManager {
    #[expect(clippy::too_many_arguments)]
    pub fn open(
        device: Arc<dyn Device>,
        io_engine: Arc<dyn IoEngine>,
        block_size: usize,
        mut eviction_pickers: Vec<Box<dyn EvictionPicker>>,
        reclaimer: Arc<dyn ReclaimerTrait>,
        reclaim_concurrency: usize,
        clean_block_threshold: usize,
        metrics: Arc<Metrics>,
        spawner: Spawner,
    ) -> Result<Self> {
        let mut blocks = vec![];

        while device.free() >= block_size {
            let partition = match device.create_partition(block_size) {
                Ok(partition) => partition,
                Err(e) if e.kind() == ErrorKind::NoSpace => break,
                Err(e) => return Err(e),
            };
            let id = blocks.len() as BlockId;
            let block = Block {
                inner: Arc::new(BlockInner {
                    id,
                    partition,
                    io_engine: io_engine.clone(),
                    statistics: Arc::<BlockStatistics>::default(),
                }),
            };
            blocks.push(block);
        }

        let rs = blocks.iter().map(|r| r.id()).collect_vec();
        for pickers in eviction_pickers.iter_mut() {
            pickers.init(&rs, block_size);
        }

        metrics.storage_block_engine_block_size_bytes.absolute(block_size as _);

        let state = State {
            clean_blocks: VecDeque::new(),
            evictable_blocks: HashSet::new(),
            writing_blocks: HashSet::new(),
            reclaiming_blocks: HashSet::new(),
            clean_block_waiters: Vec::new(),
            eviction_pickers,
            reclaim_waiters: Vec::new(),
        };
        let inner = Inner {
            blocks,
            state: RwLock::new(state),
            reclaimer,
            reclaim_concurrency,
            clean_block_threshold,
            metrics,
            spawner,
        };
        let inner = Arc::new(inner);
        let this = Self { inner };
        Ok(this)
    }

    pub fn init(&self, clean_blocks: &[BlockId]) {
        let mut state = self.inner.state.write().unwrap();
        let mut evictable_blocks: HashSet<BlockId> = self.inner.blocks.iter().map(|r| r.id()).collect();
        state.clean_blocks = clean_blocks
            .iter()
            .inspect(|id| {
                evictable_blocks.remove(id);
            })
            .copied()
            .collect();

        // Temporarily take pickers to make borrow checker happy.
        let mut pickers = std::mem::take(&mut state.eviction_pickers);

        // Notify pickers.
        for block in evictable_blocks {
            state.evictable_blocks.insert(block);
            for picker in pickers.iter_mut() {
                picker.on_block_evictable(
                    EvictionInfo {
                        blocks: &self.inner.blocks,
                        evictable: &state.evictable_blocks,
                        clean: state.clean_blocks.len(),
                    },
                    block,
                );
            }
        }

        // Restore taken pickers after operations.

        std::mem::swap(&mut state.eviction_pickers, &mut pickers);
        assert!(pickers.is_empty());

        let metrics = &self.inner.metrics;
        metrics
            .storage_block_engine_block_clean
            .absolute(state.clean_blocks.len() as _);
        metrics
            .storage_block_engine_block_evictable
            .absolute(state.evictable_blocks.len() as _);
        metrics
            .storage_block_engine_block_writing
            .absolute(state.writing_blocks.len() as _);
        metrics
            .storage_block_engine_block_reclaiming
            .absolute(state.reclaiming_blocks.len() as _);
    }

    pub fn blocks(&self) -> usize {
        self.inner.blocks.len()
    }

    pub fn block(&self, id: BlockId) -> &Block {
        &self.inner.blocks[id as usize]
    }

    pub fn get_clean_block(&self) -> GetCleanBlockHandle {
        let this = self.clone();
        async move {
            // Wrap state lock guard to make borrow checker happy.
            let rx = {
                let mut state = this.inner.state.write().unwrap();
                if let Some(id) = state.clean_blocks.pop_front() {
                    let block = this.inner.blocks[id as usize].clone();
                    state.writing_blocks.insert(id);
                    this.inner.metrics.storage_block_engine_block_clean.decrease(1);
                    this.inner.metrics.storage_block_engine_block_writing.increase(1);
                    this.reclaim_if_needed(&mut state);
                    return block;
                } else {
                    let (tx, rx) = oneshot::channel();
                    state.clean_block_waiters.push(tx);
                    drop(state);
                    rx
                }
            };
            rx.await.unwrap()
        }
        .boxed()
        .shared()
    }

    pub fn on_writing_finish(&self, block: Block) {
        let mut state = self.inner.state.write().unwrap();
        state.writing_blocks.remove(&block.id());
        self.inner.metrics.storage_block_engine_block_writing.decrease(1);
        let inserted = state.evictable_blocks.insert(block.id());
        self.inner.metrics.storage_block_engine_block_evictable.increase(1);

        assert!(inserted);

        // Temporarily take pickers to make borrow checker happy.
        let mut pickers = std::mem::take(&mut state.eviction_pickers);

        // Notify pickers.
        for picker in pickers.iter_mut() {
            picker.on_block_evictable(
                EvictionInfo {
                    blocks: &self.inner.blocks,
                    evictable: &state.evictable_blocks,
                    clean: state.clean_blocks.len(),
                },
                block.id(),
            );
        }

        // Restore taken pickers after operations.

        std::mem::swap(&mut state.eviction_pickers, &mut pickers);
        assert!(pickers.is_empty());

        tracing::debug!(
            id = block.id(),
            "[block manager]: Block state transfers from writing to evictable."
        );

        self.reclaim_if_needed(&mut state);
    }

    fn on_reclaim_finish(&self, block: Block) {
        let mut state = self.inner.state.write().unwrap();
        state.reclaiming_blocks.remove(&block.id());
        self.inner.metrics.storage_block_engine_block_reclaiming.decrease(1);
        if let Some(waiter) = state.clean_block_waiters.pop() {
            self.inner.metrics.storage_block_engine_block_writing.increase(1);
            if waiter.send(block.clone()).is_err() {
                // Waiter was cancelled; return the reclaimed block to the clean pool instead of
                // leaking it out of the state machine.
                self.inner.metrics.storage_block_engine_block_writing.decrease(1);
                self.inner.metrics.storage_block_engine_block_clean.increase(1);
                state.clean_blocks.push_back(block.id());
            } else {
                state.writing_blocks.insert(block.id());
            }
        } else {
            self.inner.metrics.storage_block_engine_block_clean.increase(1);
            state.clean_blocks.push_back(block.id());
        }
        self.reclaim_if_needed(&mut state);
        if state.reclaiming_blocks.is_empty() {
            for tx in std::mem::take(&mut state.reclaim_waiters) {
                let _ = tx.send(());
            }
        }
    }

    fn reclaim_if_needed(&self, state: &mut RwLockWriteGuard<'_, State>) {
        if state.clean_blocks.len() < self.inner.clean_block_threshold
            && state.reclaiming_blocks.len() < self.inner.reclaim_concurrency
            && let Some(block) = self.evict(state)
        {
            state.reclaiming_blocks.insert(block.id());
            self.inner.metrics.storage_block_engine_block_reclaiming.increase(1);
            let block = ReclaimingBlock {
                block_manager: self.clone(),
                block,
            };
            let future = self.inner.reclaimer.reclaim(block);
            self.inner.spawner.spawn(future);
        }
    }

    fn evict(&self, state: &mut RwLockWriteGuard<'_, State>) -> Option<Block> {
        let mut picked = None;

        if state.evictable_blocks.is_empty() {
            return None;
        }

        // Temporarily take pickers to make borrow checker happy.
        let mut pickers = std::mem::take(&mut state.eviction_pickers);

        // Pick a block to evict with pickers.
        for picker in pickers.iter_mut() {
            if let Some(block) = picker.pick(EvictionInfo {
                blocks: &self.inner.blocks,
                evictable: &state.evictable_blocks,
                clean: state.clean_blocks.len(),
            }) {
                picked = Some(block);
                break;
            }
        }

        // If no block is selected, just randomly pick one.
        let picked = picked.unwrap_or_else(|| state.evictable_blocks.iter().choose(&mut rand::rng()).copied().unwrap());

        // Update evictable map.
        let removed = state.evictable_blocks.remove(&picked);
        self.inner.metrics.storage_block_engine_block_evictable.decrease(1);
        assert!(removed);

        // Notify pickers.
        for picker in pickers.iter_mut() {
            picker.on_block_evict(
                EvictionInfo {
                    blocks: &self.inner.blocks,
                    evictable: &state.evictable_blocks,
                    clean: state.clean_blocks.len(),
                },
                picked,
            );
        }

        // Restore taken pickers after operations.
        std::mem::swap(&mut state.eviction_pickers, &mut pickers);
        assert!(pickers.is_empty());

        let block = self.inner.blocks[picked as usize].clone();
        tracing::debug!("[block manager]: Block {picked} is evicted.");

        Some(block)
    }

    pub fn wait_reclaim(&self) -> BoxFuture<'static, ()> {
        let mut state = self.inner.state.write().unwrap();
        if state.reclaiming_blocks.is_empty() {
            return ready(()).boxed();
        }
        let (tx, rx) = oneshot::channel();
        state.reclaim_waiters.push(tx);
        async move {
            let _ = rx.await;
        }
        .boxed()
    }
}

pub struct ReclaimingBlock {
    block_manager: BlockManager,
    block: Block,
}

impl Deref for ReclaimingBlock {
    type Target = Block;

    fn deref(&self) -> &Self::Target {
        &self.block
    }
}

impl DerefMut for ReclaimingBlock {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.block
    }
}

impl Drop for ReclaimingBlock {
    fn drop(&mut self) {
        self.block_manager.on_reclaim_finish(self.block.clone());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use asyncband::oneshot;
    use foyer_common::{metrics::Metrics, spawn::Spawner};
    use futures_core::future::BoxFuture;
    use futures_util::FutureExt;

    use super::{Block, BlockManager, ReclaimingBlock};
    use crate::{
        DeviceBuilder, IoEngineConfig, NoopDeviceBuilder, NoopIoEngineConfig,
        engine::block::{eviction::FifoPicker, reclaimer::ReclaimerTrait},
        io::engine::IoEngineBuildContext,
        test_utils::Holder,
    };

    const BLOCK_SIZE: usize = 4096;

    /// A reclaimer that parks the reclaim task on a [`Holder`] until it is released, then drops the
    /// [`ReclaimingBlock`] to fire `on_reclaim_finish`. This lets tests deterministically control
    /// when `on_reclaim_finish` runs relative to setting up a cancelled `get_clean_block` waiter.
    #[derive(Debug)]
    struct HolderReclaimer {
        holder: Holder,
    }

    impl HolderReclaimer {
        fn new(holder: Holder) -> Self {
            Self { holder }
        }
    }

    impl ReclaimerTrait for HolderReclaimer {
        fn reclaim(&self, block: ReclaimingBlock) -> BoxFuture<'static, ()> {
            let holder = self.holder.clone();
            async move {
                holder.wait().await;
                drop(block);
            }
            .boxed()
        }
    }

    /// A reclaimer that must never run: it is only used with `reclaim_concurrency == 0`, so
    /// `reclaim_if_needed` never triggers a reclaim and the tests can drive `on_reclaim_finish`
    /// (or `evict`) deterministically without involving the runtime spawner.
    #[derive(Debug)]
    struct DummyReclaimer;

    impl ReclaimerTrait for DummyReclaimer {
        fn reclaim(&self, _block: ReclaimingBlock) -> BoxFuture<'static, ()> {
            unreachable!("DummyReclaimer::reclaim must not be invoked when reclaim_concurrency == 0")
        }
    }

    async fn block_manager_for_test(
        nblocks: usize,
        reclaim_concurrency: usize,
        clean_block_threshold: usize,
        reclaimer: Arc<dyn ReclaimerTrait>,
    ) -> BlockManager {
        let spawner = Spawner::current();
        let io_engine = NoopIoEngineConfig
            .boxed()
            .build(IoEngineBuildContext {
                spawner: spawner.clone(),
            })
            .await
            .unwrap();
        let device = NoopDeviceBuilder::new(BLOCK_SIZE * nblocks).build().unwrap();
        BlockManager::open(
            device,
            io_engine,
            BLOCK_SIZE,
            vec![Box::<FifoPicker>::default()],
            reclaimer,
            reclaim_concurrency,
            clean_block_threshold,
            Arc::new(Metrics::noop()),
            spawner,
        )
        .unwrap()
    }

    /// Number of block ids currently tracked across the four state sets.
    fn tracked_block_count(bm: &BlockManager) -> usize {
        let s = bm.inner.state.read().unwrap();
        s.clean_blocks.len() + s.writing_blocks.len() + s.evictable_blocks.len() + s.reclaiming_blocks.len()
    }

    fn state_counts(bm: &BlockManager) -> [usize; 4] {
        let s = bm.inner.state.read().unwrap();
        [
            s.clean_blocks.len(),
            s.writing_blocks.len(),
            s.evictable_blocks.len(),
            s.reclaiming_blocks.len(),
        ]
    }

    /// Evict a block from the evictable pool and move it straight into `reclaiming_blocks`, as
    /// `reclaim_if_needed` does right before spawning the reclaimer. Returns the reclaiming block.
    fn evict_to_reclaiming(bm: &BlockManager) -> Block {
        let mut state = bm.inner.state.write().unwrap();
        let block = bm.evict(&mut state).expect("evictable must be non-empty");
        state.reclaiming_blocks.insert(block.id());
        block
    }

    /// Push a cancelled `get_clean_block` waiter: a sender whose receiver is dropped immediately,
    /// leaving an orphaned `oneshot::Sender` in `clean_block_waiters`.
    fn push_cancelled_waiter(bm: &BlockManager) {
        let (tx, rx) = oneshot::channel();
        let mut state = bm.inner.state.write().unwrap();
        state.clean_block_waiters.push(tx);
        drop(rx);
        drop(state);
    }

    #[test_log::test(tokio::test)]
    async fn test_on_reclaim_finish_dead_waiter_returns_block_to_clean_pool() {
        let reclaimer = Arc::new(DummyReclaimer) as Arc<dyn ReclaimerTrait>;
        let bm = block_manager_for_test(1, 0, 1, reclaimer).await;
        bm.init(&[]);
        assert_eq!(bm.blocks(), 1);

        let block = evict_to_reclaiming(&bm);
        push_cancelled_waiter(&bm);

        // Before the fix, the dead waiter's `send` returned `Err` and the block was dropped out of
        // every state set, permanently leaking it from the block rotation.
        bm.on_reclaim_finish(block);

        let [clean, writing, evictable, reclaiming] = state_counts(&bm);
        assert_eq!(clean, 1, "reclaimed block must return to the clean pool, not be leaked");
        assert_eq!(writing, 0);
        assert_eq!(evictable, 0);
        assert_eq!(reclaiming, 0);
        assert_eq!(tracked_block_count(&bm), 1, "block leaked out of all state sets");
    }

    #[test_log::test(tokio::test)]
    async fn test_on_reclaim_finish_live_waiter_tracks_writing_block() {
        let reclaimer = Arc::new(DummyReclaimer) as Arc<dyn ReclaimerTrait>;
        let bm = block_manager_for_test(1, 0, 1, reclaimer).await;
        bm.init(&[]);

        let block = evict_to_reclaiming(&bm);

        // Register a *live* waiter (receiver kept alive).
        let (tx, rx) = oneshot::channel();
        {
            let mut state = bm.inner.state.write().unwrap();
            state.clean_block_waiters.push(tx);
        }

        bm.on_reclaim_finish(block);

        // A block handed to a live waiter must be tracked in `writing_blocks`, matching the fast
        // path of `get_clean_block`. The buggy waiter path incremented the writing metric but never
        // inserted the id into `writing_blocks`.
        {
            let state = bm.inner.state.read().unwrap();
            assert!(
                state.writing_blocks.contains(&0),
                "block handed to a live waiter must be tracked in writing_blocks, got writing={:?}",
                state.writing_blocks.iter().collect::<Vec<_>>(),
            );
            assert!(state.clean_blocks.is_empty());
            assert!(state.evictable_blocks.is_empty());
            assert!(state.reclaiming_blocks.is_empty());
        }

        // The waiter must receive exactly the reclaimed block.
        let received = rx.await.unwrap();
        assert_eq!(received.id(), 0);

        // With the block properly tracked, the writing -> evictable transition must succeed.
        bm.on_writing_finish(bm.block(0).clone());
        let [clean, writing, evictable, reclaiming] = state_counts(&bm);
        assert_eq!(evictable, 1, "block must transition writing -> evictable");
        assert_eq!(writing, 0);
        assert_eq!(clean, 0);
        assert_eq!(reclaiming, 0);
    }

    /// Faithfully reproduces the production trigger: a real runtime task parked on
    /// `get_clean_block` is cancelled (as `try_join_all` cancels sibling per-block futures on an
    /// I/O error), orphaning its sender. The subsequent reclaim must recover the block, not leak it.
    #[test_log::test(tokio::test)]
    async fn test_cancelled_runtime_get_clean_block_waiter_is_recovered_by_reclaim() {
        let holder = Holder::default();
        holder.hold();
        let reclaimer = Arc::new(HolderReclaimer::new(holder.clone())) as Arc<dyn ReclaimerTrait>;
        let bm = block_manager_for_test(2, 1, 1, reclaimer).await;
        // clean = {0}, evictable = {1}.
        bm.init(&[0]);

        // Fast-path get_clean_block for block 0 triggers a real reclaim of block 1 (parks on holder).
        let block0 = bm.get_clean_block().clone().now_or_never().expect("block 0 fast path");
        assert_eq!(block0.id(), 0);

        // Spawn a real task that awaits get_clean_block for the (now empty) clean pool, mirroring a
        // flusher per-block future parked on its block handle.
        let wait = bm.get_clean_block();
        let jh = tokio::spawn(wait);

        // Let the task poll get_clean_block once and park, registering a sender with a real runtime
        // waker on clean_block_waiters.
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            bm.inner.state.read().unwrap().clean_block_waiters.len(),
            1,
            "the parked get_clean_block must have registered a waiter",
        );

        // Cancel the awaiting task, exactly as a sibling I/O error cancels it via try_join_all.
        let abort = jh.abort_handle();
        abort.abort();
        // Awaiting a cancelled JoinHandle resolves once the runtime has dropped the task future,
        // which drops the Shared get_clean_block handle and its oneshot receiver.
        let _ = jh.await;
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        // The sender is now orphaned but still queued in clean_block_waiters.
        assert_eq!(
            bm.inner.state.read().unwrap().clean_block_waiters.len(),
            1,
            "cancelled waiter must leave its orphaned sender queued",
        );

        // Release the parked reclaimer; on_reclaim_finish must consume the orphan and recover block 1.
        holder.unhold();
        bm.wait_reclaim().await;

        assert_eq!(
            tracked_block_count(&bm),
            bm.blocks(),
            "reclaimed block leaked out of all state sets"
        );
        let state = bm.inner.state.read().unwrap();
        assert!(
            state.clean_blocks.contains(&1),
            "reclaimed block 1 must return to the clean pool, not be leaked",
        );
        assert!(state.clean_block_waiters.is_empty());
    }
}
