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
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, RwLock, RwLockWriteGuard,
    },
};

use foyer_common::{
    error::{ErrorKind, Result},
    metrics::Metrics,
    spawn::Spawner,
};
use futures_core::future::BoxFuture;
use futures_util::{
    future::{ready, Shared},
    FutureExt,
};
use itertools::Itertools;
use mea::oneshot;
use rand::seq::IteratorRandom;

use crate::{
    engine::block::{
        eviction::{EvictionInfo, EvictionPicker},
        reclaimer::ReclaimerTrait,
    },
    io::{
        bytes::{IoB, IoBuf, IoBufMut},
        device::Partition,
        engine::IoEngine,
    },
    Device,
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
    // VERGLAS PATCH: live disk-resize watermark state. A block in `retired` holds no data and is in no other
    // set: `get_clean_block`, `evict`, and the eviction pickers only ever see `clean_blocks`/`evictable_blocks`,
    // so retired blocks are automatically invisible to normal operation. Partition ids/offsets and the length of
    // `Inner::blocks` never change; retirement only moves membership between these sets.
    retired: HashSet<BlockId>,
    // VERGLAS PATCH: blocks that `retire_tail` has committed to retiring but that are still writing or
    // reclaiming. The in-flight phase's completion handler (`on_writing_finish`/`on_reclaim_finish`) checks this
    // set and routes the block to `retired` instead of its normal destination (`evictable`/`clean`) once that
    // phase finishes.
    pending_retire: HashSet<BlockId>,

    clean_block_waiters: Vec<oneshot::Sender<Block>>,

    eviction_pickers: Vec<Box<dyn EvictionPicker>>,

    reclaim_waiters: Vec<oneshot::Sender<()>>,
    // VERGLAS PATCH: woken every time a pending-retire block finishes moving into `retired`, so `retire_tail`
    // can re-check whether all of the blocks it is waiting on have landed.
    retire_waiters: Vec<oneshot::Sender<()>>,
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

        // VERGLAS PATCH: the store always opens `blocks` at its ceiling capacity (partition ids/offsets and the
        // count of blocks never change), but a prior live shrink may have physically truncated the backing
        // device. Blocks whose byte extent falls beyond the device's current physical length are the tail a
        // previous `resize_disk` shrink retired; treat them as already retired so `init()` (recovery) never
        // scans them, and so callers see the same active capacity they left the store in.
        let physical_len = device.physical_len()?;
        let block_size_u64 = block_size as u64;
        let retired: HashSet<BlockId> = blocks
            .iter()
            .map(|b| b.id())
            .filter(|id| (*id as u64 + 1) * block_size_u64 > physical_len)
            .collect();
        metrics.storage_block_engine_block_retired.absolute(retired.len() as _);

        let state = State {
            clean_blocks: VecDeque::new(),
            evictable_blocks: HashSet::new(),
            writing_blocks: HashSet::new(),
            reclaiming_blocks: HashSet::new(),
            retired,
            pending_retire: HashSet::new(),
            clean_block_waiters: Vec::new(),
            eviction_pickers,
            reclaim_waiters: Vec::new(),
            retire_waiters: Vec::new(),
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
        // VERGLAS PATCH: blocks `open()` already classified as retired (the truncated tail of a prior shrink)
        // were never scanned by the recovery runner and must stay retired, not fall into `evictable` by default.
        let mut evictable_blocks: HashSet<BlockId> = self
            .inner
            .blocks
            .iter()
            .map(|r| r.id())
            .filter(|id| !state.retired.contains(id))
            .collect();
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

    /// Get the number of blocks currently retired by a live disk-resize shrink.
    ///
    /// `blocks() - retired_count()` is the store's current active block count.
    // VERGLAS PATCH: lets the engine compute how many of the ceiling blocks are actually backed by physical
    // storage right now, both for `resize_disk` bookkeeping and for excluding retired blocks from recovery scans.
    pub fn retired_count(&self) -> usize {
        self.inner.state.read().unwrap().retired.len()
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
                    tracing::debug!(id, "[block manager]: get_clean_block resolved immediately.");
                    return block;
                } else {
                    let (tx, rx) = oneshot::channel();
                    state.clean_block_waiters.push(tx);
                    tracing::debug!(
                        waiters = state.clean_block_waiters.len(),
                        "[block manager]: get_clean_block found no clean block, registered a waiter."
                    );
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

        // VERGLAS PATCH: a block queued for retirement (via `retire_tail`) while it was still writing now holds
        // data, so it cannot skip straight to `retired` the way an untouched clean block can. Route it through
        // the same reclaim path an evicted block takes so its index entries are dropped first;
        // `on_reclaim_finish` sees it is still in `pending_retire` and finishes the retirement from there.
        if state.pending_retire.contains(&block.id()) {
            tracing::debug!(
                id = block.id(),
                "[block manager]: writing block queued for retirement, reclaiming before retire."
            );
            self.spawn_reclaim(&mut state, block);
        } else {
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
        }

        self.reclaim_if_needed(&mut state);
    }

    fn on_reclaim_finish(&self, block: Block) {
        let mut state = self.inner.state.write().unwrap();
        state.reclaiming_blocks.remove(&block.id());
        self.inner.metrics.storage_block_engine_block_reclaiming.decrease(1);

        // VERGLAS PATCH: a block that was queued for retirement while writing or reclaiming finishes its
        // retirement here, once whatever phase it was in when `retire_tail` caught it has completed.
        if state.pending_retire.remove(&block.id()) {
            state.retired.insert(block.id());
            self.inner.metrics.storage_block_engine_block_retired.increase(1);
            tracing::debug!(id = block.id(), "[block manager]: Block state transfers to retired.");
            for tx in std::mem::take(&mut state.retire_waiters) {
                let _ = tx.send(());
            }
        } else if let Some(waiter) = state.clean_block_waiters.pop() {
            self.inner.metrics.storage_block_engine_block_writing.increase(1);
            let _ = waiter.send(block);
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

    fn reclaim_if_needed<'a>(&self, state: &mut RwLockWriteGuard<'a, State>) {
        if state.clean_blocks.len() < self.inner.clean_block_threshold
            && state.reclaiming_blocks.len() < self.inner.reclaim_concurrency
        {
            if let Some(block) = self.evict(state) {
                self.spawn_reclaim(state, block);
            }
        }
    }

    /// Move `block` into `reclaiming_blocks` and spawn its reclaim task.
    ///
    /// Shared by opportunistic reclaim (`reclaim_if_needed`) and mandatory retirement of an evictable or
    /// in-flight block (`retire_tail`, `on_writing_finish`): both need a block's index entries dropped before it
    /// can become clean (or, for retirement, retired).
    // VERGLAS PATCH: factored out so `retire_tail` can drive an evictable block through exactly the same reclaim
    // path `evict()`/`reclaim_if_needed` already use, instead of duplicating the spawn bookkeeping.
    fn spawn_reclaim<'a>(&self, state: &mut RwLockWriteGuard<'a, State>, block: Block) {
        state.reclaiming_blocks.insert(block.id());
        self.inner.metrics.storage_block_engine_block_reclaiming.increase(1);
        let reclaiming = ReclaimingBlock {
            block_manager: self.clone(),
            block,
        };
        let future = self.inner.reclaimer.reclaim(reclaiming);
        self.inner.spawner.spawn(future);
    }

    fn evict<'a>(&self, state: &mut RwLockWriteGuard<'a, State>) -> Option<Block> {
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

    /// Retire the `count` highest-id non-retired blocks.
    ///
    /// A clean block holds no data and moves straight to `retired`. An evictable block is pulled out of the
    /// eviction pickers' view and sent through the same reclaim path `evict()` uses (its index entries must be
    /// dropped before the block is safe to treat as empty). A block that is currently writing or reclaiming
    /// joins `pending_retire`; `on_writing_finish`/`on_reclaim_finish` finish its retirement once that phase
    /// completes. The returned future resolves with the ids that were retired once all of them have landed in
    /// `retired`.
    // VERGLAS PATCH: the retire half of live disk-resize. Blocks are always chosen from the tail (highest id
    // first) so the store's active region stays a contiguous prefix — required for `set_physical_len` to
    // truncate exactly the bytes no live block needs, and for a reopened store to recover the retired tail
    // without scanning it (see `open()`).
    pub fn retire_tail(&self, count: usize) -> BoxFuture<'static, Vec<BlockId>> {
        let this = self.clone();
        async move {
            let ids = {
                let mut state = this.inner.state.write().unwrap();
                this.begin_retire_tail(&mut state, count)
            };

            // VERGLAS PATCH: the "still pending?" check and "register a waiter" must happen under the same lock
            // acquisition. Checking, releasing the lock, and only then registering leaves a window where a
            // concurrent `on_reclaim_finish` can retire the last pending block and drain `retire_waiters` before
            // this waiter is in it — a lost wakeup that hangs `retire_tail` forever. An initial buggy version of
            // this loop split the check and the registration into two lock acquisitions and deadlocked under
            // real (non-mocked) reclaim timing; see `foyer-storage/tests` in this fork's `resize` module.
            loop {
                let rx = {
                    let mut state = this.inner.state.write().unwrap();
                    if ids.iter().all(|id| state.retired.contains(id)) {
                        None
                    } else {
                        let (tx, rx) = oneshot::channel();
                        state.retire_waiters.push(tx);
                        Some(rx)
                    }
                };
                match rx {
                    None => break,
                    Some(rx) => {
                        let _ = rx.await;
                    }
                }
            }

            ids
        }
        .boxed()
    }

    /// Select up to `count` highest-id blocks that are neither retired nor already queued for retirement, and
    /// begin retiring each of them synchronously (see `retire_tail`). Returns the selected ids.
    fn begin_retire_tail<'a>(&self, state: &mut RwLockWriteGuard<'a, State>, count: usize) -> Vec<BlockId> {
        let mut candidates: Vec<BlockId> = self
            .inner
            .blocks
            .iter()
            .map(|b| b.id())
            .filter(|id| !state.retired.contains(id) && !state.pending_retire.contains(id))
            .collect();
        candidates.sort_unstable_by(|a, b| b.cmp(a));
        candidates.truncate(count);

        for &id in &candidates {
            self.begin_retire_one(state, id);
        }

        candidates
    }

    /// Begin retiring a single block, dispatching on its current state (see `retire_tail`).
    fn begin_retire_one<'a>(&self, state: &mut RwLockWriteGuard<'a, State>, id: BlockId) {
        if let Some(pos) = state.clean_blocks.iter().position(|&b| b == id) {
            state.clean_blocks.remove(pos);
            self.inner.metrics.storage_block_engine_block_clean.decrease(1);
            state.retired.insert(id);
            self.inner.metrics.storage_block_engine_block_retired.increase(1);
            tracing::debug!(id, "[block manager]: clean block retired directly.");
            // VERGLAS PATCH: every other path that removes a block from `clean_blocks` (`get_clean_block`,
            // `on_reclaim_finish`) is followed by a `reclaim_if_needed` check so the clean-block count is
            // replenished under `clean_block_threshold` pressure. Retiring a clean block shrinks `clean_blocks`
            // the same way and must preserve that invariant, or a concurrent `get_clean_block` waiter can be left
            // waiting forever with no in-flight write or reclaim left to eventually wake it.
            self.reclaim_if_needed(state);
            return;
        }

        if state.evictable_blocks.remove(&id) {
            self.inner.metrics.storage_block_engine_block_evictable.decrease(1);

            // Temporarily take pickers to make borrow checker happy.
            let mut pickers = std::mem::take(&mut state.eviction_pickers);
            for picker in pickers.iter_mut() {
                picker.on_block_evict(
                    EvictionInfo {
                        blocks: &self.inner.blocks,
                        evictable: &state.evictable_blocks,
                        clean: state.clean_blocks.len(),
                    },
                    id,
                );
            }
            std::mem::swap(&mut state.eviction_pickers, &mut pickers);
            assert!(pickers.is_empty());

            state.pending_retire.insert(id);
            let block = self.inner.blocks[id as usize].clone();
            self.spawn_reclaim(state, block);
            tracing::debug!(
                id,
                "[block manager]: evictable block queued for retirement, reclaiming."
            );
            return;
        }

        // The block is currently writing or reclaiming. `on_writing_finish`/`on_reclaim_finish` will notice it
        // is pending retirement and route it to `retired` once that phase finishes.
        debug_assert!(state.writing_blocks.contains(&id) || state.reclaiming_blocks.contains(&id));
        state.pending_retire.insert(id);
        tracing::debug!(id, "[block manager]: in-flight block queued for retirement.");
    }

    /// Restore up to `count` retired blocks back to clean, preferring the lowest ids (the blocks closest to the
    /// current active watermark, restored first as the device grows back toward them). Waiters blocked in
    /// `get_clean_block` are woken directly instead of just being left to notice the new clean blocks, since
    /// nothing else drains `clean_block_waiters` outside of `on_reclaim_finish`. Returns the restored ids.
    // VERGLAS PATCH: the grow half of live disk-resize. Restored blocks are clean by construction: retirement
    // never leaves data behind (a clean block retires directly; an evictable/writing/reclaiming block is always
    // reclaimed first), so no recovery scan is needed to bring them back.
    pub fn restore_retired(&self, count: usize) -> Vec<BlockId> {
        let mut state = self.inner.state.write().unwrap();

        let mut candidates: Vec<BlockId> = state.retired.iter().copied().collect();
        candidates.sort_unstable();
        candidates.truncate(count);

        for &id in &candidates {
            state.retired.remove(&id);
            self.inner.metrics.storage_block_engine_block_retired.decrease(1);

            if let Some(waiter) = state.clean_block_waiters.pop() {
                state.writing_blocks.insert(id);
                self.inner.metrics.storage_block_engine_block_writing.increase(1);
                let block = self.inner.blocks[id as usize].clone();
                let _ = waiter.send(block);
            } else {
                state.clean_blocks.push_back(id);
                self.inner.metrics.storage_block_engine_block_clean.increase(1);
            }
        }

        tracing::debug!(ids = ?candidates, "[block manager]: blocks restored from retired.");

        candidates
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

// VERGLAS PATCH: unit tests for the live disk-resize state machine (`retire_tail`/`restore_retired`), added ahead
// of the implementation per this fork's TDD policy. They exercise `BlockManager` directly against a `NoopDevice`
// so each block-state transition (clean, evictable, writing, reclaiming) can be driven precisely, independent of
// the full block engine (covered separately by the `resize_disk` integration tests in `engine.rs`).
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use itertools::Itertools;
    use tokio::time::timeout;

    use super::*;
    use crate::{
        engine::block::eviction::FifoPicker,
        io::{device::noop::NoopDeviceBuilder, engine::IoEngineBuildContext},
        DeviceBuilder, IoEngineConfig, NoopIoEngineConfig,
    };

    const BLOCK_SIZE: usize = 4 * 1024;

    /// A reclaimer that finishes reclaiming a block as soon as it is spawned.
    #[derive(Debug, Default)]
    struct ImmediateReclaimer;

    impl ReclaimerTrait for ImmediateReclaimer {
        fn reclaim(&self, block: ReclaimingBlock) -> BoxFuture<'static, ()> {
            async move {
                drop(block);
            }
            .boxed()
        }
    }

    async fn manager_for_test(blocks: usize, clean: &[BlockId], reclaimer: Arc<dyn ReclaimerTrait>) -> BlockManager {
        let device = NoopDeviceBuilder::new(blocks * BLOCK_SIZE).build().unwrap();
        let spawner = Spawner::current();
        let io_engine = NoopIoEngineConfig
            .boxed()
            .build(IoEngineBuildContext {
                spawner: spawner.clone(),
            })
            .await
            .unwrap();
        let metrics = Arc::new(Metrics::noop());
        let manager = BlockManager::open(
            device,
            io_engine,
            BLOCK_SIZE,
            vec![Box::<FifoPicker>::default()],
            reclaimer,
            blocks,
            // `clean_block_threshold: 0` disables opportunistic background reclaim, so tests only observe the
            // state transitions they explicitly trigger.
            0,
            metrics,
            spawner,
        )
        .unwrap();
        manager.init(clean);
        manager
    }

    #[test_log::test(tokio::test)]
    async fn test_retire_tail_clean_block_retires_synchronously() {
        let manager = manager_for_test(4, &[0, 1, 2, 3], Arc::new(ImmediateReclaimer)).await;

        // All 4 blocks are clean: retiring the tail 2 needs no reclaim and must not suspend the future.
        let ids = manager
            .retire_tail(2)
            .now_or_never()
            .expect("retiring clean blocks must resolve without yielding")
            .into_iter()
            .sorted()
            .collect_vec();

        assert_eq!(ids, vec![2, 3]);
        assert_eq!(manager.retired_count(), 2);
    }

    #[test_log::test(tokio::test)]
    async fn test_retire_tail_evictable_block_goes_through_reclaim() {
        // `init(&[])` leaves every block evictable.
        let manager = manager_for_test(2, &[], Arc::new(ImmediateReclaimer)).await;

        let ids = timeout(Duration::from_secs(5), manager.retire_tail(1))
            .await
            .expect("retire_tail must not deadlock reclaiming an evictable block");

        assert_eq!(ids, vec![1]);
        assert_eq!(manager.retired_count(), 1);
    }

    #[test_log::test(tokio::test)]
    async fn test_retire_tail_writing_block_waits_for_writing_finish() {
        let manager = manager_for_test(2, &[0, 1], Arc::new(ImmediateReclaimer)).await;

        // Take block 0 out of `clean` and into `writing`, simulating a flusher mid-write. Block 1 stays clean.
        let writing = manager.get_clean_block().await;
        assert_eq!(writing.id(), 0);

        let manager2 = manager.clone();
        let retire = tokio::spawn(async move { manager2.retire_tail(2).await });

        // Block 1 (clean) retires immediately; block 0 is still writing, so the whole call must not finish yet.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !retire.is_finished(),
            "retire_tail resolved before the in-flight write finished"
        );
        assert_eq!(manager.retired_count(), 1);

        // Finishing the write must let retirement of block 0 proceed (through reclaim) and the call complete.
        manager.on_writing_finish(writing);
        let ids = timeout(Duration::from_secs(5), retire)
            .await
            .expect("retire_tail must not deadlock waiting on a block that was writing")
            .unwrap()
            .into_iter()
            .sorted()
            .collect_vec();

        assert_eq!(ids, vec![0, 1]);
        assert_eq!(manager.retired_count(), 2);
    }

    #[test_log::test(tokio::test)]
    async fn test_restore_retired_wakes_pending_get_clean_block() {
        let manager = manager_for_test(2, &[], Arc::new(ImmediateReclaimer)).await;
        timeout(Duration::from_secs(5), manager.retire_tail(2)).await.unwrap();
        assert_eq!(manager.retired_count(), 2);

        // No clean blocks are left: a `get_clean_block` call must wait.
        let waiter = manager.get_clean_block();
        assert!(
            waiter.clone().now_or_never().is_none(),
            "get_clean_block must have nothing to hand out before restore"
        );

        let restored = manager.restore_retired(1);
        assert_eq!(restored, vec![0]);
        assert_eq!(manager.retired_count(), 1);

        // Restoring a block must directly satisfy the waiter instead of leaving it stuck.
        let block = timeout(Duration::from_secs(5), waiter)
            .await
            .expect("restore_retired must wake a waiter blocked in get_clean_block");
        assert_eq!(block.id(), 0);
    }
}
