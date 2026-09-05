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
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};

#[cfg(feature = "tracing")]
use fastrace::prelude::*;
use foyer_common::{
    bits,
    code::{StorageKey, StorageValue},
    error::{Error, ErrorKind, Result},
    metrics::Metrics,
    properties::{Age, Properties},
    spawn::Spawner,
};
use futures_core::future::BoxFuture;
use futures_util::{
    future::{join_all, try_join_all},
    FutureExt,
};
use itertools::Itertools;
use mea::mpsc::UnboundedReceiver;

use super::{
    flusher::{Flusher, InvalidStats, Submission},
    indexer::Indexer,
    recover::RecoverRunner,
};
#[cfg(any(test, feature = "test_utils"))]
use crate::test_utils::*;
use crate::{
    compress::Compression,
    engine::{
        block::{
            eviction::{EvictionPicker, FifoPicker, InvalidRatioPicker},
            manager::{BlockId, BlockManager},
            reclaimer::{BlockCleaner, Reclaimer, ReclaimerTrait},
            serde::{AtomicSequence, EntryHeader},
            tombstone::{Tombstone, TombstoneLog},
        },
        Engine, EngineBuildContext, EngineConfig, Populated,
    },
    filter::conditions::IoThrottle,
    io::{bytes::IoSliceMut, PAGE},
    keeper::PieceRef,
    serde::EntryDeserializer,
    Device, Load, RejectAll, StorageFilter, StorageFilterResult,
};

/// Config for the block-based disk cache engine.
///
/// The block-based disk cache engine is suitable for general cache entries with size from 2K to hundreds of MiBs.
///
/// Each cache entry will be aligned to a multiplier of 4K on disk, hence too small cache entries will lead to heavy
/// internal fragmentation.
///
/// The disk cache evicts cache entries in block unit.
pub struct BlockEngineConfig<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    device: Arc<dyn Device>,
    block_size: usize,
    compression: Compression,
    indexer_shards: usize,
    recover_concurrency: usize,
    flushers: usize,
    reclaimers: usize,
    buffer_pool_size: usize,
    blob_index_size: usize,
    // VERGLAS PATCH: pre-existing bug fix (unrelated to live disk-resize). This was a bare `usize` hardcoded to
    // 16 MiB in `new()`, contradicting its own doc comment ("Default: `buffer_pool_size` * 2") the moment a
    // caller raised `buffer_pool_size` without also raising this: entries submitted past the stale 16 MiB
    // threshold are silently dropped as ordinary backpressure (`BlockEngine::enqueue`'s
    // `submit_queue_size > submit_queue_size_threshold` check), which reads exactly like "writes vanish after
    // resize" if the caller happens to resize around the same time. `None` here means "derive from
    // `buffer_pool_size` at build time," resolved in `build()`.
    submit_queue_size_threshold: Option<usize>,
    clean_block_threshold: usize,
    eviction_pickers: Vec<Box<dyn EvictionPicker>>,
    admission_filter: StorageFilter,
    reinsertion_filter: StorageFilter,
    enable_tombstone_log: bool,
    #[cfg(any(test, feature = "test_utils"))]
    flush_switch: Switch,
    #[cfg(any(test, feature = "test_utils"))]
    load_holder: Holder,
    marker: PhantomData<(K, V, P)>,
}

impl<K, V, P> Debug for BlockEngineConfig<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockEngineConfig")
            .field("device", &self.device)
            .field("block_size", &self.block_size)
            .field("compression", &self.compression)
            .field("indexer_shards", &self.indexer_shards)
            .field("recover_concurrency", &self.recover_concurrency)
            .field("flushers", &self.flushers)
            .field("reclaimers", &self.reclaimers)
            .field("buffer_pool_size", &self.buffer_pool_size)
            .field("blob_index_size", &self.blob_index_size)
            .field("submit_queue_size_threshold", &self.submit_queue_size_threshold)
            .field("clean_block_threshold", &self.clean_block_threshold)
            .field("eviction_pickers", &self.eviction_pickers)
            .field("admission_filter", &self.admission_filter)
            .field("reinsertion_filter", &self.reinsertion_filter)
            .field("enable_tombstone_log", &self.enable_tombstone_log)
            .finish()
    }
}

impl<K, V, P> BlockEngineConfig<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    /// Create a new block-based disk cache engine builder with default configurations.
    pub fn new(device: Arc<dyn Device>) -> Self {
        Self {
            device,
            block_size: 16 * 1024 * 1024, // 16 MiB
            compression: Compression::default(),
            indexer_shards: 64,
            recover_concurrency: 8,
            flushers: 1,
            reclaimers: 1,
            buffer_pool_size: 16 * 1024 * 1024, // 16 MiB
            blob_index_size: 4 * 1024,          // 4 KiB
            submit_queue_size_threshold: None,  // derived from `buffer_pool_size * 2` at build time
            clean_block_threshold: 1,
            eviction_pickers: vec![Box::new(InvalidRatioPicker::new(0.8)), Box::<FifoPicker>::default()],
            admission_filter: StorageFilter::new(),
            reinsertion_filter: StorageFilter::new().with_condition(RejectAll),
            enable_tombstone_log: false,
            #[cfg(any(test, feature = "test_utils"))]
            flush_switch: Switch::default(),
            #[cfg(any(test, feature = "test_utils"))]
            load_holder: Holder::default(),
            marker: PhantomData,
        }
    }

    /// Set the block size for the block-based disk cache engine.
    ///
    /// Block is the minimal cache eviction unit for the block-based disk cache,
    /// its size also limits the max cacheable entry size.
    ///
    /// The block size must be 4K-aligned. the given value is not 4K-aligned, it will be automatically aligned up.
    ///
    /// Default: `16 MiB`.
    pub fn with_block_size(mut self, block_size: usize) -> Self {
        self.block_size = bits::align_up(PAGE, block_size);
        self
    }

    /// Set the shard num of the indexer. Each shard has its own lock.
    ///
    /// Default: `64`.
    pub fn with_indexer_shards(mut self, indexer_shards: usize) -> Self {
        self.indexer_shards = indexer_shards;
        self
    }

    /// Set the recover concurrency for the disk cache store.
    ///
    /// Default: `8`.
    pub fn with_recover_concurrency(mut self, recover_concurrency: usize) -> Self {
        self.recover_concurrency = recover_concurrency;
        self
    }

    /// Set the flusher count for the disk cache store.
    ///
    /// The flusher count limits how many blocks can be concurrently written.
    ///
    /// Default: `1`.
    pub fn with_flushers(mut self, flushers: usize) -> Self {
        self.flushers = flushers;
        self
    }

    /// Set the admission filter for th disk cache store.
    ///
    /// The admission filter is used to pick the entries that can be inserted into the disk cache store.
    ///
    /// Default: Admit all.
    pub fn with_admission_filter(mut self, filter: StorageFilter) -> Self {
        self.admission_filter = filter;
        self
    }

    /// Set the reclaimer count for the disk cache store.
    ///
    /// The reclaimer count limits how many blocks can be concurrently reclaimed.
    ///
    /// Default: `1`.
    pub fn with_reclaimers(mut self, reclaimers: usize) -> Self {
        self.reclaimers = reclaimers;
        self
    }

    /// Set the total flush buffer pool size.
    ///
    /// Each flusher shares a volume at `threshold / flushers`.
    ///
    /// If the buffer of the flush queue exceeds the threshold, the further entries will be ignored.
    ///
    /// Default: 16 MiB.
    pub fn with_buffer_pool_size(mut self, buffer_pool_size: usize) -> Self {
        self.buffer_pool_size = buffer_pool_size;
        self
    }

    /// Set the blob index size for each blob.
    ///
    /// A larger blob index size can hold more blob entries, but it will also increase the io size of each blob part
    /// write.
    ///
    /// NOTE: The size will be aligned up to a multiplier of 4K.
    ///
    /// Default: 4 KiB
    pub fn with_blob_index_size(mut self, blob_index_size: usize) -> Self {
        let blob_index_size = bits::align_up(PAGE, blob_index_size);
        self.blob_index_size = blob_index_size;
        self
    }

    /// Set the submit queue size threshold.
    ///
    /// If the total entry estimated size in the submit queue exceeds the threshold, the further entries will be
    /// ignored.
    ///
    /// Default: `buffer_pool_size` * 2.
    pub fn with_submit_queue_size_threshold(mut self, submit_queue_size_threshold: usize) -> Self {
        self.submit_queue_size_threshold = Some(submit_queue_size_threshold);
        self
    }

    /// Set the clean block threshold for the disk cache store.
    ///
    /// The reclaimers only work when the clean block count is equal to or lower than the clean block threshold.
    ///
    /// Default: the same value as the `reclaimers`.
    pub fn with_clean_block_threshold(mut self, clean_block_threshold: usize) -> Self {
        self.clean_block_threshold = clean_block_threshold;
        self
    }

    /// Set the eviction pickers for th disk cache store.
    ///
    /// The eviction picker is used to pick the block to reclaim.
    ///
    /// The eviction pickers are applied in order. If the previous eviction picker doesn't pick any block, the next one
    /// will be applied.
    ///
    /// If no eviction picker picks a block, a block will be picked randomly.
    ///
    /// Default: [ invalid ratio picker { threshold = 0.8 }, fifo picker ]
    pub fn with_eviction_pickers(mut self, eviction_pickers: Vec<Box<dyn EvictionPicker>>) -> Self {
        self.eviction_pickers = eviction_pickers;
        self
    }

    /// Set the reinsertion filter for th disk cache store.
    ///
    /// The reinsertion filter is used to pick the entries that can be reinsertion into the disk cache store while
    /// reclaiming.
    ///
    /// Note: Only extremely important entries should be picked. If too many entries are picked, both insertion and
    /// reinsertion will be stuck.
    ///
    /// Default: Reject all.
    pub fn with_reinsertion_filter(mut self, filter: StorageFilter) -> Self {
        self.reinsertion_filter = filter;
        self
    }

    /// Enable the tombstone log.
    ///
    /// For updatable cache, either the tombstone log or [`crate::engine::RecoverMode::None`] must be enabled to prevent
    /// from the phantom entries after reopen.
    pub fn with_tombstone_log(mut self, enable: bool) -> Self {
        self.enable_tombstone_log = enable;
        self
    }

    /// Pass the flush holder for test.
    #[cfg(any(test, feature = "test_utils"))]
    pub fn with_flush_switch(mut self, flush_switch: Switch) -> Self {
        self.flush_switch = flush_switch;
        self
    }

    /// Pass the load holder for test.
    #[cfg(any(test, feature = "test_utils"))]
    pub fn with_load_holder(mut self, load_holder: Holder) -> Self {
        self.load_holder = load_holder;
        self
    }

    /// Build the block-based disk cache engine with the given configurations.
    pub async fn build(
        self: Box<Self>,
        EngineBuildContext {
            io_engine,
            metrics,
            spawner: runtime,
            recover_mode,
        }: EngineBuildContext,
    ) -> Result<Arc<BlockEngine<K, V, P>>> {
        let device = self.device;
        let block_size = self.block_size;
        // VERGLAS PATCH: pre-existing bug fix. Resolve the documented default here, at build time, against
        // whatever `buffer_pool_size` the caller actually configured — not a value frozen in `new()` before the
        // caller had a chance to override `buffer_pool_size`. See the field's doc comment on `BlockEngineConfig`.
        let submit_queue_size_threshold = self.submit_queue_size_threshold.unwrap_or(self.buffer_pool_size * 2);

        let mut tombstones = vec![];

        let tombstone_log = if self.enable_tombstone_log {
            // TODO(MrCroxx): The tombstone log support multiples partitions for multiple device support.
            let mut partitions = vec![];

            let max_entries = device.capacity() / PAGE;
            let pages = max_entries / TombstoneLog::SLOTS_PER_PAGE
                + if max_entries % TombstoneLog::SLOTS_PER_PAGE > 0 {
                    1
                } else {
                    0
                };
            let partition = device.create_partition(pages * PAGE)?;
            partitions.push(partition);

            let tombstone_log = TombstoneLog::open(partitions, io_engine.clone(), &mut tombstones).await?;
            Some(tombstone_log)
        } else {
            None
        };

        let indexer = Indexer::new(self.indexer_shards);
        let submit_queue_size = Arc::<AtomicUsize>::default();

        #[expect(clippy::type_complexity)]
        let (flushers, rxs): (Vec<Flusher<K, V, P>>, Vec<UnboundedReceiver<Submission<K, V, P>>>) = (0..self.flushers)
            .map(|id| Flusher::<K, V, P>::new(id, submit_queue_size.clone(), metrics.clone()))
            .unzip();

        let reclaimer = Reclaimer::new(
            indexer.clone(),
            flushers.clone(),
            Arc::new(self.reinsertion_filter),
            self.blob_index_size,
            device.statistics().clone(),
            runtime.clone(),
        );
        let reclaimer: Arc<dyn ReclaimerTrait> = Arc::new(reclaimer);

        let block_manager = BlockManager::open(
            device.clone(),
            io_engine,
            block_size,
            self.eviction_pickers,
            reclaimer,
            self.reclaimers,
            self.clean_block_threshold,
            metrics.clone(),
            runtime.clone(),
        )?;
        let blocks = block_manager.blocks();

        if self.flushers + self.clean_block_threshold > blocks / 2 {
            tracing::warn!("[block engine]: block-based object disk cache stable blocks count is too small, flusher [{flushers}] + clean block threshold [{clean_block_threshold}] (default = reclaimers) is supposed to be much larger than the block count [{blocks}]",
                    flushers = self.flushers,
                    clean_block_threshold = self.clean_block_threshold,
                );
        }

        let sequence = AtomicSequence::default();

        // VERGLAS PATCH: a store reopened after a live shrink has fewer physically-backed blocks than its
        // ceiling (`BlockManager::open` already classified the truncated tail as retired). Retired blocks are
        // always the highest-id contiguous tail, so `0..active_blocks` is still the exact contiguous range the
        // recover runner assumes; scanning the retired tail would read past the physically truncated file.
        let active_blocks = blocks - block_manager.retired_count();

        RecoverRunner::run(
            self.recover_concurrency,
            recover_mode,
            self.blob_index_size,
            (0..active_blocks as BlockId).collect_vec(),
            &sequence,
            &indexer,
            &block_manager,
            &tombstones,
            runtime.clone(),
            metrics.clone(),
        )
        .await?;

        let io_buffer_size = self.buffer_pool_size / self.flushers;
        // Pre-existing clippy fix (unrelated to live disk-resize): `.into_iter()` here is a no-op `zip` already
        // accepts `IntoIterator`, and a newer clippy on this toolchain flags it as `useless_conversion`.
        for (flusher, rx) in flushers.iter().zip(rxs) {
            flusher.run(
                rx,
                block_size,
                io_buffer_size,
                self.blob_index_size,
                self.compression,
                indexer.clone(),
                block_manager.clone(),
                tombstone_log.clone(),
                metrics.clone(),
                &runtime,
                #[cfg(any(test, feature = "test_utils"))]
                self.flush_switch.clone(),
            )?;
        }

        let admission_filter = self.admission_filter.with_condition(IoThrottle);

        let inner = BlockEngineInner {
            admission_filter,
            device,
            // VERGLAS PATCH: `resize_disk` needs both to convert a target byte count into a target block count
            // and to clamp it to the minimum functional footprint.
            block_size,
            clean_block_threshold: self.clean_block_threshold,
            indexer,
            block_manager,
            flushers,
            submit_queue_size,
            submit_queue_size_threshold,
            sequence,
            _spawner: runtime,
            active: AtomicBool::new(true),
            metrics,
            #[cfg(any(test, feature = "test_utils"))]
            flush_switch: self.flush_switch,
            #[cfg(any(test, feature = "test_utils"))]
            load_holder: self.load_holder,
        };
        let inner = Arc::new(inner);
        let engine = BlockEngine { inner };
        let engine = Arc::new(engine);
        Ok(engine)
    }
}

impl<K, V, P> EngineConfig<K, V, P> for BlockEngineConfig<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn build(self: Box<Self>, ctx: EngineBuildContext) -> BoxFuture<'static, Result<Arc<dyn Engine<K, V, P>>>> {
        async move { self.build(ctx).await.map(|e| e as Arc<dyn Engine<K, V, P>>) }.boxed()
    }
}

impl<K, V, P> From<BlockEngineConfig<K, V, P>> for Box<dyn EngineConfig<K, V, P>>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn from(builder: BlockEngineConfig<K, V, P>) -> Self {
        builder.boxed()
    }
}

/// Block-based disk cache engine.
pub struct BlockEngine<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    inner: Arc<BlockEngineInner<K, V, P>>,
}

impl<K, V, P> Debug for BlockEngine<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericStore").finish()
    }
}

struct BlockEngineInner<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    admission_filter: StorageFilter,

    device: Arc<dyn Device>,
    // VERGLAS PATCH: live disk-resize bookkeeping (see `resize_disk`).
    block_size: usize,
    clean_block_threshold: usize,

    indexer: Indexer,
    block_manager: BlockManager,

    flushers: Vec<Flusher<K, V, P>>,

    submit_queue_size: Arc<AtomicUsize>,
    submit_queue_size_threshold: usize,

    sequence: AtomicSequence,

    _spawner: Spawner,

    active: AtomicBool,

    metrics: Arc<Metrics>,

    #[cfg(any(test, feature = "test_utils"))]
    flush_switch: Switch,

    #[cfg(any(test, feature = "test_utils"))]
    load_holder: Holder,
}

impl<K, V, P> Clone for BlockEngine<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<K, V, P> BlockEngine<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn wait(&self) -> impl Future<Output = ()> + Send + 'static {
        let flushers = self.inner.flushers.clone();
        let block_manager = self.inner.block_manager.clone();
        async move {
            join_all(flushers.iter().map(|flusher| flusher.wait())).await;
            block_manager.wait_reclaim().await;
        }
    }

    fn close(&self) -> BoxFuture<'static, Result<()>> {
        let this = self.clone();
        async move {
            this.inner.active.store(false, Ordering::Relaxed);
            this.wait().await;
            Ok(())
        }
        .boxed()
    }

    /// Resize the disk cache's active on-disk footprint.
    ///
    /// The store always opens at its ceiling capacity (`blocks * block_size`, from the configured device) and
    /// never exceeds it: `target_bytes` is rounded down to a whole number of blocks and clamped between the
    /// minimum functional footprint (`clean_block_threshold + 1` active blocks, so the engine can always make
    /// forward progress) and the ceiling.
    ///
    /// Shrinking retires the tail blocks beyond the target (see [`BlockManager::retire_tail`]) and then
    /// physically truncates the device. Growing physically extends the device first, then restores previously
    /// retired blocks (see [`BlockManager::restore_retired`]) — extension must happen before the blocks are
    /// handed back out, or a write could land past the (still-truncated) end of the file.
    ///
    /// Returns the resulting active capacity in bytes.
    // VERGLAS PATCH: live disk-resize entry point. Block ids and their byte offsets never change; only how many
    // of the ceiling's blocks are currently active does. See `BlockManager`'s `retired`/`pending_retire` state
    // and `Device::set_physical_len` for the mechanics.
    fn resize_disk(&self, target_bytes: u64) -> BoxFuture<'static, Result<u64>> {
        let this = self.clone();
        async move {
            let block_size = this.inner.block_size as u64;
            let total_blocks = this.inner.block_manager.blocks();
            let min_active_blocks = (this.inner.clean_block_threshold + 1).min(total_blocks);

            let target_blocks = ((target_bytes / block_size) as usize).clamp(min_active_blocks, total_blocks);
            let active_blocks = total_blocks - this.inner.block_manager.retired_count();

            match target_blocks.cmp(&active_blocks) {
                std::cmp::Ordering::Less => {
                    let shrink_by = active_blocks - target_blocks;
                    // VERGLAS PATCH: each flusher may be holding a block open as its "current" block, writing to
                    // it lazily as entries arrive; such a block never becomes evictable on its own (see
                    // `Submission::Rotate`). Ask every flusher to release its current block before selecting
                    // retirement candidates, or `retire_tail` could wait forever on a write that was never
                    // actually going to happen absent new traffic.
                    for flusher in &this.inner.flushers {
                        flusher.rotate();
                    }
                    this.inner.block_manager.retire_tail(shrink_by).await;
                    let new_len = target_blocks as u64 * block_size;
                    this.inner.device.set_physical_len(new_len)?;
                    Ok(new_len)
                }
                std::cmp::Ordering::Greater => {
                    let grow_by = target_blocks - active_blocks;
                    let new_len = target_blocks as u64 * block_size;
                    this.inner.device.set_physical_len(new_len)?;
                    this.inner.block_manager.restore_retired(grow_by);
                    Ok(new_len)
                }
                std::cmp::Ordering::Equal => Ok(active_blocks as u64 * block_size),
            }
        }
        .boxed()
    }

    #[cfg_attr(feature = "tracing", trace(name = "foyer::storage::engine::block::generic::enqueue"))]
    fn enqueue(&self, piece: PieceRef<K, V, P>, estimated_size: usize) {
        if !self.inner.active.load(Ordering::Relaxed) {
            tracing::warn!("cannot enqueue new entry after closed");
            return;
        }

        tracing::trace!(
            hash = piece.hash(),
            age = ?piece.properties().age().unwrap_or_default(),
            "[block engine]: enqueue"
        );
        match piece.properties().age().unwrap_or_default() {
            Age::Fresh | Age::Old => {}
            Age::Young => {
                // skip write block engine if the entry is still young
                tracing::debug!(hash = piece.hash(), "[block engine]: enqueue skipped, entry is young");
                self.inner.metrics.storage_block_engine_enqueue_skip.increase(1);
                return;
            }
        }

        let submit_queue_size = self.inner.submit_queue_size.load(Ordering::Relaxed);
        if submit_queue_size > self.inner.submit_queue_size_threshold {
            tracing::debug!(
                hash = piece.hash(),
                submit_queue_size,
                threshold = self.inner.submit_queue_size_threshold,
                "[block engine]: enqueue skipped, submit queue overflow"
            );
            self.inner.metrics.storage_queue_channel_overflow.increase(1);
            return;
        }

        let sequence = self.inner.sequence.fetch_add(1, Ordering::Relaxed);

        self.inner.flushers[piece.hash() as usize % self.inner.flushers.len()].submit(Submission::CacheEntry {
            piece,
            estimated_size,
            sequence,
        });
    }

    fn load(&self, hash: u64) -> impl Future<Output = Result<Load<K, V, P>>> + Send + 'static {
        tracing::trace!(hash, "[block engine]: load");

        #[cfg(any(test, feature = "test_utils"))]
        let load_holer = self.inner.load_holder.wait();

        let indexer = self.inner.indexer.clone();
        let metrics = self.inner.metrics.clone();
        let block_manager = self.inner.block_manager.clone();

        let load = async move {
            #[cfg(any(test, feature = "test_utils"))]
            load_holer.await;

            let addr = match indexer.get(hash) {
                Some(addr) => addr,
                None => {
                    return Ok(Load::Miss);
                }
            };

            tracing::trace!(hash, ?addr, "[block engine]: load");

            let block = block_manager.block(addr.block);
            if block.partition().statistics().is_read_throttled() {
                return Ok(Load::Throttled);
            }

            let buf = IoSliceMut::new(bits::align_up(PAGE, addr.len as _));
            let (buf, res) = block.read(Box::new(buf), addr.offset as _).await;
            match res {
                Ok(_) => {}
                Err(e) => {
                    tracing::error!(hash, ?addr, ?e, "[block engine load]: load error");
                    return Err(e);
                }
            }

            let header = match EntryHeader::read(&buf[..EntryHeader::serialized_len()]) {
                Ok(header) => header,
                Err(e) => {
                    return match e.kind() {
                        ErrorKind::Parse
                        | ErrorKind::MagicMismatch
                        | ErrorKind::ChecksumMismatch
                        | ErrorKind::OutOfRange => {
                            tracing::warn!(
                                hash,
                                ?addr,
                                ?e,
                                "[block engine load]: deserialize read buffer raise error, remove this entry and skip"
                            );
                            indexer.remove(hash);
                            Ok(Load::Miss)
                        }
                        _ => {
                            tracing::error!(hash, ?addr, ?e, "[block engine load]: load error");
                            Err(e)
                        }
                    }
                }
            };

            let (key, value) = {
                let now = Instant::now();
                let res = match EntryDeserializer::deserialize::<K, V>(
                    &buf[EntryHeader::serialized_len()..],
                    header.key_len as _,
                    header.value_len as _,
                    header.compression,
                    Some(header.checksum),
                ) {
                    Ok(res) => res,
                    Err(e) => {
                        return match e.kind() {
                            ErrorKind::MagicMismatch | ErrorKind::ChecksumMismatch | ErrorKind::OutOfRange => {
                                tracing::warn!(
                                hash,
                                ?addr,
                                ?header,
                                ?e,
                                "[block engine load]: deserialize read buffer raise error, remove this entry and skip"
                            );
                                indexer.remove(hash);
                                Ok(Load::Miss)
                            }
                            _ => {
                                tracing::error!(hash, ?addr, ?header, ?e, "[block engine load]: load error");
                                Err(e)
                            }
                        }
                    }
                };
                metrics
                    .storage_entry_deserialize_duration
                    .record(now.elapsed().as_secs_f64());
                res
            };

            let age = match block.statistics().probation.load(Ordering::Relaxed) {
                true => Age::Old,
                false => Age::Young,
            };

            Ok(Load::Entry {
                key,
                value,
                populated: Populated { age },
            })
        };
        #[cfg(feature = "tracing")]
        let load = load.in_span(Span::enter_with_local_parent(
            "foyer::storage::engine::block::generic::load",
        ));
        load
    }

    fn delete(&self, hash: u64) {
        if !self.inner.active.load(Ordering::Relaxed) {
            tracing::warn!("cannot delete entry after closed");
            return;
        }

        let sequence = self.inner.sequence.fetch_add(1, Ordering::Relaxed);
        let stats = self
            .inner
            .indexer
            .insert_tombstone(hash, sequence)
            .map(|addr| InvalidStats {
                block: addr.block,
                size: bits::align_up(PAGE, addr.len as usize),
            });

        let this = self.clone();

        this.inner.flushers[hash as usize % this.inner.flushers.len()].submit(Submission::Tombstone {
            tombstone: Tombstone { hash, sequence },
            stats,
        });
    }

    fn may_contains(&self, hash: u64) -> bool {
        self.inner.indexer.get(hash).is_some()
    }

    fn destroy(&self) -> BoxFuture<'static, Result<()>> {
        let this = self.clone();
        async move {
            if !this.inner.active.load(Ordering::Relaxed) {
                return Err(Error::new(ErrorKind::Closed, "cannot delete entry after closed"));
            }

            // Write a tombstone to clear tombstone log by increase the max sequence.
            let sequence = this.inner.sequence.fetch_add(1, Ordering::Relaxed);

            this.inner.flushers[0].submit(Submission::Tombstone {
                tombstone: Tombstone { hash: 0, sequence },
                stats: None,
            });
            this.wait().await;

            // Clear indices.
            //
            // This step must perform after the latest writer finished,
            // otherwise the indices of the latest batch cannot be cleared.
            this.inner.indexer.clear();

            // Clean blocks.
            try_join_all((0..this.inner.block_manager.blocks() as BlockId).map(|id| {
                let block = this.inner.block_manager.block(id).clone();
                async move {
                    let res = BlockCleaner::clean(&block).await;
                    block.statistics().reset();
                    res
                }
            }))
            .await?;

            Ok(())
        }
        .boxed()
    }

    #[cfg(any(test, feature = "test_utils"))]
    pub fn hold_flush(&self) {
        self.inner.flush_switch.on();
    }

    #[cfg(any(test, feature = "test_utils"))]
    pub fn unhold_flush(&self) {
        self.inner.flush_switch.off();
    }
}

impl<K, V, P> Engine<K, V, P> for BlockEngine<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn device(&self) -> &Arc<dyn Device> {
        &self.inner.device
    }

    fn filter(&self, hash: u64, estimated_size: usize) -> StorageFilterResult {
        self.inner
            .admission_filter
            .filter(self.inner.device.statistics(), hash, estimated_size)
    }

    fn enqueue(&self, piece: PieceRef<K, V, P>, estimated_size: usize) {
        self.enqueue(piece, estimated_size);
    }

    fn load(&self, hash: u64) -> BoxFuture<'static, Result<Load<K, V, P>>> {
        // TODO(MrCroxx): refactor this.
        self.load(hash).boxed()
    }

    fn delete(&self, hash: u64) {
        self.delete(hash);
    }

    fn may_contains(&self, hash: u64) -> bool {
        self.may_contains(hash)
    }

    fn destroy(&self) -> BoxFuture<'static, Result<()>> {
        self.destroy()
    }

    fn wait(&self) -> BoxFuture<'static, ()> {
        // TODO(MrCroxx): refactor this.
        self.wait().boxed()
    }

    fn close(&self) -> BoxFuture<'static, Result<()>> {
        self.close()
    }

    fn resize_disk(&self, target_bytes: u64) -> BoxFuture<'static, Result<u64>> {
        self.resize_disk(target_bytes)
    }
}

#[cfg(test)]
mod tests {

    use std::{fs::File, path::Path};

    use bytesize::ByteSize;
    use foyer_common::hasher::ModHasher;
    use foyer_memory::{Cache, CacheBuilder, CacheEntry, FifoConfig, TestProperties};
    use itertools::Itertools;

    use super::*;
    use crate::{
        engine::RecoverMode,
        io::{
            device::{combined::CombinedDeviceBuilder, fs::FsDeviceBuilder, DeviceBuilder},
            engine::{IoEngine, IoEngineBuildContext, IoEngineConfig},
        },
        serde::EntrySerializer,
        test_utils::Biased,
        PsyncIoEngineConfig, RejectAll,
    };

    const KB: usize = 1024;

    fn cache_for_test() -> Cache<u64, Vec<u8>, ModHasher, TestProperties> {
        CacheBuilder::new(10)
            .with_shards(1)
            .with_eviction_config(FifoConfig::default())
            .with_hash_builder(ModHasher::default())
            .build()
    }

    async fn io_engine_for_test(spawner: Spawner) -> Arc<dyn IoEngine> {
        // TODO(MrCroxx): Test with other io engines.
        PsyncIoEngineConfig::new()
            .boxed()
            .build(IoEngineBuildContext { spawner })
            .await
            .unwrap()
    }

    /// 4 files, fifo eviction, 16 KiB block, 64 KiB capacity.
    async fn engine_for_test(dir: impl AsRef<Path>) -> Arc<BlockEngine<u64, Vec<u8>, TestProperties>> {
        store_for_test_with_reinsertion_filter(dir, StorageFilter::new().with_condition(RejectAll)).await
    }

    async fn store_for_test_with_reinsertion_filter(
        dir: impl AsRef<Path>,
        reinsertion_filter: StorageFilter,
    ) -> Arc<BlockEngine<u64, Vec<u8>, TestProperties>> {
        let device = FsDeviceBuilder::new(dir)
            .with_capacity(ByteSize::kib(64).as_u64() as _)
            .build()
            .unwrap();
        let spawner = Spawner::current();
        let io_engine = io_engine_for_test(spawner.clone()).await;
        let metrics = Arc::new(Metrics::noop());
        let builder = BlockEngineConfig {
            device,
            block_size: 16 * 1024,
            compression: Compression::None,
            indexer_shards: 4,
            recover_concurrency: 2,
            flushers: 1,
            reclaimers: 1,
            clean_block_threshold: 1,
            admission_filter: StorageFilter::new(),
            eviction_pickers: vec![Box::<FifoPicker>::default()],
            reinsertion_filter,
            enable_tombstone_log: false,
            buffer_pool_size: 16 * 1024 * 1024,
            blob_index_size: 4 * 1024,
            submit_queue_size_threshold: Some(16 * 1024 * 1024 * 2),
            flush_switch: Switch::default(),
            load_holder: Holder::default(),
            marker: PhantomData,
        };

        let builder = Box::new(builder);
        builder
            .build(EngineBuildContext {
                io_engine,
                metrics,
                spawner,
                recover_mode: RecoverMode::Strict,
            })
            .await
            .unwrap()
    }

    async fn store_for_test_with_tombstone_log(
        dir: impl AsRef<Path>,
    ) -> Arc<BlockEngine<u64, Vec<u8>, TestProperties>> {
        let device = FsDeviceBuilder::new(dir)
            .with_capacity(ByteSize::kib(64).as_u64() as usize + ByteSize::kib(4).as_u64() as usize)
            .build()
            .unwrap();
        let spawner = Spawner::current();
        let io_engine = io_engine_for_test(spawner.clone()).await;
        let metrics = Arc::new(Metrics::noop());
        let builder = BlockEngineConfig {
            device,
            block_size: 16 * 1024,
            compression: Compression::None,
            indexer_shards: 4,
            recover_concurrency: 2,
            flushers: 1,
            reclaimers: 1,
            clean_block_threshold: 1,
            eviction_pickers: vec![Box::<FifoPicker>::default()],
            admission_filter: StorageFilter::new(),
            reinsertion_filter: StorageFilter::new().with_condition(RejectAll),
            enable_tombstone_log: true,
            buffer_pool_size: 16 * 1024 * 1024,
            blob_index_size: 4 * 1024,
            submit_queue_size_threshold: Some(16 * 1024 * 1024 * 2),
            flush_switch: Switch::default(),
            load_holder: Holder::default(),
            marker: PhantomData,
        };
        let builder = Box::new(builder);
        builder
            .build(EngineBuildContext {
                io_engine,
                metrics,
                spawner,
                recover_mode: RecoverMode::Strict,
            })
            .await
            .unwrap()
    }

    fn enqueue(
        store: &BlockEngine<u64, Vec<u8>, TestProperties>,
        entry: CacheEntry<u64, Vec<u8>, ModHasher, TestProperties>,
    ) {
        let estimated_size = EntrySerializer::estimated_size(entry.key(), entry.value());
        store.enqueue(entry.piece().into(), estimated_size);
    }

    #[test_log::test(tokio::test)]
    async fn test_store_enqueue_lookup_recovery() {
        let dir = tempfile::tempdir().unwrap();

        let memory = cache_for_test();
        let store = engine_for_test(dir.path()).await;

        // [ [e1, e2], [], [], [] ]
        store.hold_flush();
        let e1 = memory.insert(1, vec![1; 7 * KB]);
        let e2 = memory.insert(2, vec![2; 3 * KB]);
        enqueue(&store, e1.clone());
        enqueue(&store, e2);
        store.unhold_flush();
        store.wait().await;

        let r1 = store.load(memory.hash(&1)).await.unwrap().kv().unwrap();
        assert_eq!(r1, (1, vec![1; 7 * KB]));
        let r2 = store.load(memory.hash(&2)).await.unwrap().kv().unwrap();
        assert_eq!(r2, (2, vec![2; 3 * KB]));

        // [ [e1, e2], [e3, e4], [], [] ]
        store.hold_flush();
        let e3 = memory.insert(3, vec![3; 7 * KB]);
        let e4 = memory.insert(4, vec![4; 2 * KB]);
        enqueue(&store, e3);
        enqueue(&store, e4);
        store.unhold_flush();
        store.wait().await;

        let r1 = store.load(memory.hash(&1)).await.unwrap().kv().unwrap();
        assert_eq!(r1, (1, vec![1; 7 * KB]));
        let r2 = store.load(memory.hash(&2)).await.unwrap().kv().unwrap();
        assert_eq!(r2, (2, vec![2; 3 * KB]));
        let r3 = store.load(memory.hash(&3)).await.unwrap().kv().unwrap();
        assert_eq!(r3, (3, vec![3; 7 * KB]));
        let r4 = store.load(memory.hash(&4)).await.unwrap().kv().unwrap();
        assert_eq!(r4, (4, vec![4; 2 * KB]));

        // [ [e1, e2], [e3, e4], [e5], [] ]
        let e5 = memory.insert(5, vec![5; 11 * KB]);
        enqueue(&store, e5);
        store.wait().await;

        let r1 = store.load(memory.hash(&1)).await.unwrap().kv().unwrap();
        assert_eq!(r1, (1, vec![1; 7 * KB]));
        let r2 = store.load(memory.hash(&2)).await.unwrap().kv().unwrap();
        assert_eq!(r2, (2, vec![2; 3 * KB]));
        let r3 = store.load(memory.hash(&3)).await.unwrap().kv().unwrap();
        assert_eq!(r3, (3, vec![3; 7 * KB]));
        let r4 = store.load(memory.hash(&4)).await.unwrap().kv().unwrap();
        assert_eq!(r4, (4, vec![4; 2 * KB]));
        let r5 = store.load(memory.hash(&5)).await.unwrap().kv().unwrap();
        assert_eq!(r5, (5, vec![5; 11 * KB]));

        // [ [], [e3, e4], [e5], [e6, e4*] ]
        store.hold_flush();
        let e6 = memory.insert(6, vec![6; 7 * KB]);
        let e4v2 = memory.insert(4, vec![!4; 3 * KB]);
        enqueue(&store, e6);
        enqueue(&store, e4v2);
        store.unhold_flush();
        store.wait().await;

        assert!(store.load(memory.hash(&1)).await.unwrap().kv().is_none());
        assert!(store.load(memory.hash(&2)).await.unwrap().kv().is_none());
        let r3 = store.load(memory.hash(&3)).await.unwrap().kv().unwrap();
        assert_eq!(r3, (3, vec![3; 7 * KB]));
        let r4v2 = store.load(memory.hash(&4)).await.unwrap().kv().unwrap();
        assert_eq!(r4v2, (4, vec![!4; 3 * KB]));
        let r5 = store.load(memory.hash(&5)).await.unwrap().kv().unwrap();
        assert_eq!(r5, (5, vec![5; 11 * KB]));
        let r6 = store.load(memory.hash(&6)).await.unwrap().kv().unwrap();
        assert_eq!(r6, (6, vec![6; 7 * KB]));

        store.close().await.unwrap();
        enqueue(&store, e1);
        store.wait().await;

        drop(store);

        let store = engine_for_test(dir.path()).await;

        assert!(store.load(memory.hash(&1)).await.unwrap().kv().is_none());
        assert!(store.load(memory.hash(&2)).await.unwrap().kv().is_none());
        let r3 = store.load(memory.hash(&3)).await.unwrap().kv().unwrap();
        assert_eq!(r3, (3, vec![3; 7 * KB]));
        let r4v2 = store.load(memory.hash(&4)).await.unwrap().kv().unwrap();
        assert_eq!(r4v2, (4, vec![!4; 3 * KB]));
        let r5 = store.load(memory.hash(&5)).await.unwrap().kv().unwrap();
        assert_eq!(r5, (5, vec![5; 11 * KB]));
        let r6 = store.load(memory.hash(&6)).await.unwrap().kv().unwrap();
        assert_eq!(r6, (6, vec![6; 7 * KB]));
    }

    #[test_log::test(tokio::test)]
    async fn test_store_delete_recovery() {
        let dir = tempfile::tempdir().unwrap();

        let memory = cache_for_test();
        let store = store_for_test_with_tombstone_log(dir.path()).await;

        let es = (0..10).map(|i| memory.insert(i, vec![i as u8; 3 * KB])).collect_vec();

        // [[0, 1, 2], [3, 4, 5], [6, 7, 8], []]
        for e in es.iter().take(9) {
            enqueue(&store, e.clone());
        }
        store.wait().await;

        for i in 0..9 {
            assert_eq!(
                store.load(memory.hash(&i)).await.unwrap().kv(),
                Some((i, vec![i as u8; 3 * KB]))
            );
        }

        store.delete(memory.hash(&3));
        store.wait().await;
        assert_eq!(store.load(memory.hash(&3)).await.unwrap().kv(), None);

        store.close().await.unwrap();
        drop(store);

        let store = store_for_test_with_tombstone_log(dir.path()).await;
        for i in 0..9 {
            if i != 3 {
                assert_eq!(
                    store.load(memory.hash(&i)).await.unwrap().kv(),
                    Some((i, vec![i as u8; 3 * KB]))
                );
            } else {
                assert_eq!(store.load(memory.hash(&3)).await.unwrap().kv(), None);
            }
        }

        enqueue(&store, es[3].clone());
        store.wait().await;
        assert_eq!(
            store.load(memory.hash(&3)).await.unwrap().kv(),
            Some((3, vec![3; 3 * KB]))
        );

        store.close().await.unwrap();
        drop(store);

        let store = store_for_test_with_tombstone_log(dir.path()).await;

        assert_eq!(
            store.load(memory.hash(&3)).await.unwrap().kv(),
            Some((3, vec![3; 3 * KB]))
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_store_destroy_recovery() {
        let dir = tempfile::tempdir().unwrap();

        let memory = cache_for_test();
        let store = store_for_test_with_tombstone_log(dir.path()).await;

        let es = (0..10).map(|i| memory.insert(i, vec![i as u8; 3 * KB])).collect_vec();

        // [[0, 1, 2], [3, 4, 5], [6, 7, 8], []]
        store.hold_flush();
        for e in es.iter().take(9) {
            enqueue(&store, e.clone());
        }
        store.unhold_flush();
        store.wait().await;

        for i in 0..9 {
            assert_eq!(
                store.load(memory.hash(&i)).await.unwrap().kv(),
                Some((i, vec![i as u8; 3 * KB]))
            );
        }

        store.delete(memory.hash(&3));
        store.wait().await;
        assert_eq!(store.load(memory.hash(&3)).await.unwrap().kv(), None);

        store.destroy().await.unwrap();

        store.close().await.unwrap();
        drop(store);

        let store = store_for_test_with_tombstone_log(dir.path()).await;
        for i in 0..9 {
            assert_eq!(store.load(memory.hash(&i)).await.unwrap().kv(), None);
        }

        enqueue(&store, es[3].clone());
        store.wait().await;
        assert_eq!(
            store.load(memory.hash(&3)).await.unwrap().kv(),
            Some((3, vec![3; 3 * KB]))
        );

        store.close().await.unwrap();
        drop(store);

        let store = store_for_test_with_tombstone_log(dir.path()).await;

        assert_eq!(
            store.load(memory.hash(&3)).await.unwrap().kv(),
            Some((3, vec![3; 3 * KB]))
        );
    }

    // FIXME(MrCroxx): Move the admission test to store level.
    // #[test_log::test(tokio::test)]
    // async fn test_store_admission() {
    //     let dir = tempfile::tempdir().unwrap();

    //     let memory = cache_for_test();
    //     let store = store_for_test_with_admission_picker(&memory, dir.path(),
    // Arc::new(BiasedPicker::new([1]))).await;

    //     let e1 = memory.insert(1, vec![1; 7 * KB]);
    //     let e2 = memory.insert(2, vec![2; 7 * KB]);

    //     assert!(enqueue(&store, e1.clone(),).await.unwrap());
    //     assert!(!enqueue(&store, e2,).await.unwrap());

    //     let r1 = store.load(&1).await.unwrap().unwrap();
    //     assert_eq!(r1, (1, vec![1; 7 * KB]));
    //     assert!(store.load(&2).await.unwrap().is_none());
    // }

    #[test_log::test(tokio::test)]
    async fn test_store_reinsertion() {
        let dir = tempfile::tempdir().unwrap();

        let memory = cache_for_test();
        let store = store_for_test_with_reinsertion_filter(
            dir.path(),
            StorageFilter::new().with_condition(Biased::new(vec![1, 3, 5, 7, 9, 11, 13, 15, 17, 19])),
        )
        .await;

        let es = (0..15).map(|i| memory.insert(i, vec![i as u8; 3 * KB])).collect_vec();

        // [[(0), (1), (2)], [(3), (4), (5)], [(6), (7), (8)], []]
        for e in es.iter().take(9).cloned() {
            enqueue(&store, e);
            store.wait().await;
        }

        for i in 0..9 {
            let r = store.load(memory.hash(&i)).await.unwrap().kv().unwrap();
            assert_eq!(r, (i, vec![i as u8; 3 * KB]));
        }

        // [[], [(3), (4), (5)], [(6), (7), (8)], [(9), (10), (1)]]
        enqueue(&store, es[9].clone());
        enqueue(&store, es[10].clone());
        store.wait().await;
        let mut res = vec![];
        for i in 0..11 {
            res.push(store.load(memory.hash(&i)).await.unwrap().kv());
        }
        assert_eq!(
            res,
            vec![
                None,
                Some((1, vec![1; 3 * KB])),
                None,
                Some((3, vec![3; 3 * KB])),
                Some((4, vec![4; 3 * KB])),
                Some((5, vec![5; 3 * KB])),
                Some((6, vec![6; 3 * KB])),
                Some((7, vec![7; 3 * KB])),
                Some((8, vec![8; 3 * KB])),
                Some((9, vec![9; 3 * KB])),
                Some((10, vec![10; 3 * KB])),
            ]
        );

        // [[(11), (3), (5)], [], [(6), (7), (8)], [(9), (10), (1)]]
        enqueue(&store, es[11].clone());
        store.wait().await;
        let mut res = vec![];
        for i in 0..12 {
            res.push(store.load(memory.hash(&i)).await.unwrap().kv());
        }
        assert_eq!(
            res,
            vec![
                None,
                Some((1, vec![1; 3 * KB])),
                None,
                Some((3, vec![3; 3 * KB])),
                None,
                Some((5, vec![5; 3 * KB])),
                Some((6, vec![6; 3 * KB])),
                Some((7, vec![7; 3 * KB])),
                Some((8, vec![8; 3 * KB])),
                Some((9, vec![9; 3 * KB])),
                Some((10, vec![10; 3 * KB])),
                Some((11, vec![11; 3 * KB])),
            ]
        );

        // [[(11), (3), (5)], [(12), (13), (14)], [], [(9), (10), (1)]]
        store.delete(memory.hash(&7));
        store.wait().await;
        enqueue(&store, es[12].clone());
        store.wait().await;
        enqueue(&store, es[13].clone());
        store.wait().await;
        enqueue(&store, es[14].clone());
        store.wait().await;
        let mut res = vec![];
        for i in 0..15 {
            res.push(store.load(memory.hash(&i)).await.unwrap().kv());
        }
        assert_eq!(
            res,
            vec![
                None,
                Some((1, vec![1; 3 * KB])),
                None,
                Some((3, vec![3; 3 * KB])),
                None,
                Some((5, vec![5; 3 * KB])),
                None,
                None,
                None,
                Some((9, vec![9; 3 * KB])),
                Some((10, vec![10; 3 * KB])),
                Some((11, vec![11; 3 * KB])),
                Some((12, vec![12; 3 * KB])),
                Some((13, vec![13; 3 * KB])),
                Some((14, vec![14; 3 * KB])),
            ]
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_store_magic_checksum_mismatch() {
        let dir = tempfile::tempdir().unwrap();

        let memory = cache_for_test();
        let store = engine_for_test(dir.path()).await;

        // write entry 1
        let e1 = memory.insert(1, vec![1; 7 * KB]);
        enqueue(&store, e1);
        store.wait().await;

        // check entry 1
        let r1 = store.load(memory.hash(&1)).await.unwrap().kv().unwrap();
        assert_eq!(r1, (1, vec![1; 7 * KB]));

        // corrupt entry and header
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let entry = entry.unwrap();
            if !entry.metadata().unwrap().is_file() {
                continue;
            }

            let file = File::options().write(true).open(entry.path()).unwrap();
            #[cfg(target_family = "unix")]
            {
                use std::os::unix::fs::FileExt;
                file.write_all_at(&[b'x'; 42], 5 * 1024).unwrap();
            }
            #[cfg(target_family = "windows")]
            {
                use std::os::windows::fs::FileExt;
                file.seek_write(&[b'x'; 42], 5 * 1024).unwrap();
            }
        }

        assert!(store.load(memory.hash(&1)).await.unwrap().kv().is_none());
    }

    #[test_log::test(tokio::test)]
    async fn test_aggregated_device() {
        let dir = tempfile::tempdir().unwrap();

        const KB: usize = 1024;
        const MB: usize = 1024 * 1024;

        let spawner = Spawner::current();
        let io_engine = io_engine_for_test(spawner.clone()).await;

        let d1 = FsDeviceBuilder::new(dir.path().join("dev1"))
            .with_capacity(MB)
            .build()
            .unwrap();
        let d2 = FsDeviceBuilder::new(dir.path().join("dev2"))
            .with_capacity(2 * MB)
            .build()
            .unwrap();
        let d3 = FsDeviceBuilder::new(dir.path().join("dev3"))
            .with_capacity(4 * MB)
            .build()
            .unwrap();
        let device = CombinedDeviceBuilder::new()
            .with_device(d1)
            .with_device(d2)
            .with_device(d3)
            .build()
            .unwrap();
        let engine = BlockEngineConfig::<u64, Vec<u8>, TestProperties>::new(device)
            .with_block_size(64 * KB)
            .boxed()
            .build(EngineBuildContext {
                io_engine,
                metrics: Arc::new(Metrics::noop()),
                spawner,
                recover_mode: RecoverMode::None,
            })
            .await
            .unwrap();
        assert_eq!(engine.inner.block_manager.blocks(), (1 + 2 + 4) * MB / (64 * KB));
    }

    // VERGLAS PATCH: `resize_disk` integration tests. These need a real single-file, sparse-file-backed device
    // (`FileDeviceBuilder`) since `FsDeviceBuilder`, used by the other tests in this module, spreads partitions
    // across one file per block and does not support `set_physical_len`.
    mod resize {
        use std::time::Duration;

        use super::*;
        use crate::io::device::file::FileDeviceBuilder;

        /// 4 blocks, 16 KiB each, 64 KiB ceiling capacity.
        const BLOCK_SIZE: usize = 16 * KB;
        const BLOCKS: usize = 4;
        const CAPACITY: usize = BLOCK_SIZE * BLOCKS;
        /// Aligned to exactly one page-rounded blob part per block (16 KiB block - 4 KiB default blob index = 12
        /// KiB usable; a 10 KiB value plus header aligns up to 12 KiB), so each of 4 entries submitted in a
        /// single flush round lands in its own block, in id order.
        const ENTRY_VALUE_SIZE: usize = 10 * KB;

        async fn engine_for_resize_test(path: impl AsRef<Path>) -> Arc<BlockEngine<u64, Vec<u8>, TestProperties>> {
            let device = FileDeviceBuilder::new(path).with_capacity(CAPACITY).build().unwrap();
            let spawner = Spawner::current();
            let io_engine = io_engine_for_test(spawner.clone()).await;
            BlockEngineConfig::<u64, Vec<u8>, TestProperties>::new(device)
                .with_block_size(BLOCK_SIZE)
                // `clean_block_threshold: 0` disables the engine's default "always keep at least one clean
                // block ready" background reclaim. With the default threshold of 1, that background reclaim
                // fires the instant all 4 blocks hold data (picking the oldest, block 0) regardless of
                // `resize_disk` — these tests need reclaim to happen only when they explicitly shrink.
                .with_clean_block_threshold(0)
                .boxed()
                .build(EngineBuildContext {
                    io_engine,
                    metrics: Arc::new(Metrics::noop()),
                    spawner,
                    recover_mode: RecoverMode::Strict,
                })
                .await
                .unwrap()
        }

        /// Insert `keys.len()` entries in a single flush round, so they land one per block, in ascending id order
        /// starting from block 0 (see `ENTRY_VALUE_SIZE`).
        async fn insert_one_per_block(
            engine: &BlockEngine<u64, Vec<u8>, TestProperties>,
            memory: &Cache<u64, Vec<u8>, ModHasher, TestProperties>,
            keys: &[u64],
        ) {
            engine.hold_flush();
            for &k in keys {
                let entry = memory.insert(k, vec![k as u8; ENTRY_VALUE_SIZE]);
                enqueue(engine, entry);
            }
            engine.unhold_flush();
            engine.wait().await;
        }

        #[test_log::test(tokio::test)]
        async fn test_resize_disk_shrink_truncates_file_and_evicts_tail() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("data");

            let memory = cache_for_test();
            let engine = engine_for_resize_test(&path).await;

            insert_one_per_block(&engine, &memory, &[1, 2, 3, 4]).await;
            for k in 1..=4u64 {
                assert_eq!(
                    engine.load(memory.hash(&k)).await.unwrap().kv().unwrap(),
                    (k, vec![k as u8; ENTRY_VALUE_SIZE])
                );
            }
            assert_eq!(engine.inner.block_manager.retired_count(), 0);
            assert_eq!(std::fs::metadata(&path).unwrap().len(), CAPACITY as u64);

            // Shrink to 2 blocks: retires the tail (blocks 2 and 3, holding entries 3 and 4).
            let active = engine.resize_disk((2 * BLOCK_SIZE) as u64).await.unwrap();
            assert_eq!(active, (2 * BLOCK_SIZE) as u64);
            assert_eq!(engine.inner.block_manager.retired_count(), 2);

            // The backing file is physically truncated, not just logically shrunk.
            assert_eq!(std::fs::metadata(&path).unwrap().len(), (2 * BLOCK_SIZE) as u64);

            // Entries that lived in the retired tail now miss.
            assert!(engine.load(memory.hash(&3)).await.unwrap().is_miss());
            assert!(engine.load(memory.hash(&4)).await.unwrap().is_miss());

            // Entries that survived in the still-active blocks still hit.
            assert_eq!(
                engine.load(memory.hash(&1)).await.unwrap().kv().unwrap(),
                (1, vec![1u8; ENTRY_VALUE_SIZE])
            );
            assert_eq!(
                engine.load(memory.hash(&2)).await.unwrap().kv().unwrap(),
                (2, vec![2u8; ENTRY_VALUE_SIZE])
            );
        }

        #[test_log::test(tokio::test)]
        async fn test_resize_disk_grow_back_restores_write_capacity() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("data");

            let memory = cache_for_test();
            let engine = engine_for_resize_test(&path).await;

            insert_one_per_block(&engine, &memory, &[1, 2, 3, 4]).await;
            engine.resize_disk((2 * BLOCK_SIZE) as u64).await.unwrap();
            assert_eq!(engine.inner.block_manager.retired_count(), 2);

            // Grow back to the full ceiling: restores the 2 retired blocks to clean.
            let active = engine.resize_disk(CAPACITY as u64).await.unwrap();
            assert_eq!(active, CAPACITY as u64);
            assert_eq!(engine.inner.block_manager.retired_count(), 0);
            assert_eq!(std::fs::metadata(&path).unwrap().len(), CAPACITY as u64);

            // A new entry lands in a restored block: it is retrievable, and doing so did not require evicting the
            // entries that survived the shrink (proving the grow added real capacity, not just triggered reclaim).
            insert_one_per_block(&engine, &memory, &[5]).await;
            assert_eq!(
                engine.load(memory.hash(&5)).await.unwrap().kv().unwrap(),
                (5, vec![5u8; ENTRY_VALUE_SIZE])
            );
            assert_eq!(
                engine.load(memory.hash(&1)).await.unwrap().kv().unwrap(),
                (1, vec![1u8; ENTRY_VALUE_SIZE])
            );
            assert_eq!(
                engine.load(memory.hash(&2)).await.unwrap().kv().unwrap(),
                (2, vec![2u8; ENTRY_VALUE_SIZE])
            );
        }

        #[test_log::test(tokio::test)]
        async fn test_resize_disk_shrink_with_writes_in_flight_does_not_deadlock() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("data");

            let memory = cache_for_test();
            // Unlike `engine_for_resize_test`, this test keeps the engine's default `clean_block_threshold: 1`:
            // the shrink below is going to retire blocks a queued write still needs, so this test specifically
            // needs the engine's normal self-healing background reclaim (of the surviving evictable blocks) to
            // free up a clean block again — with `clean_block_threshold: 0` that reclaim never fires and the
            // queued write would wait forever, which is a pre-existing gap in this store's design (reclaim is
            // only ever triggered by consuming a clean block or finishing a write/reclaim), not a live
            // disk-resize deadlock.
            let device = FileDeviceBuilder::new(&path).with_capacity(CAPACITY).build().unwrap();
            let spawner = Spawner::current();
            let io_engine = io_engine_for_test(spawner.clone()).await;
            let engine = BlockEngineConfig::<u64, Vec<u8>, TestProperties>::new(device)
                .with_block_size(BLOCK_SIZE)
                .boxed()
                .build(EngineBuildContext {
                    io_engine,
                    metrics: Arc::new(Metrics::noop()),
                    spawner,
                    recover_mode: RecoverMode::Strict,
                })
                .await
                .unwrap();

            insert_one_per_block(&engine, &memory, &[1, 2]).await;

            // Hold the flusher so entries 3..6 stay queued (write pressure exceeding the 2 remaining clean
            // blocks) while a concurrent shrink retires the tail those queued writes would otherwise use.
            engine.hold_flush();
            for k in 3..=6u64 {
                let entry = memory.insert(k, vec![k as u8; ENTRY_VALUE_SIZE]);
                enqueue(&engine, entry);
            }

            let resize_engine = engine.clone();
            let resize = tokio::spawn(async move { resize_engine.resize_disk((2 * BLOCK_SIZE) as u64).await });

            tokio::time::sleep(Duration::from_millis(50)).await;
            engine.unhold_flush();

            let active = tokio::time::timeout(Duration::from_secs(5), resize)
                .await
                .expect("resize_disk must not deadlock while writes are queued against the retired tail")
                .unwrap()
                .unwrap();
            assert_eq!(active, (2 * BLOCK_SIZE) as u64);

            // The queued writes must also eventually drain (via the engine's own reclaim of the surviving
            // blocks), proving the shrink did not leave the flusher permanently stuck either.
            tokio::time::timeout(Duration::from_secs(5), engine.wait())
                .await
                .expect("queued writes must not deadlock after a concurrent shrink");

            // The engine is still fully functional after the contention: a fresh insert lands and is retrievable.
            insert_one_per_block(&engine, &memory, &[99]).await;
            assert_eq!(
                engine.load(memory.hash(&99)).await.unwrap().kv().unwrap(),
                (99, vec![99u8; ENTRY_VALUE_SIZE])
            );
        }

        #[test_log::test(tokio::test)]
        async fn test_resize_disk_reopen_after_shrink_recovers() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("data");

            let memory = cache_for_test();
            let engine = engine_for_resize_test(&path).await;
            insert_one_per_block(&engine, &memory, &[1, 2, 3, 4]).await;
            engine.resize_disk((2 * BLOCK_SIZE) as u64).await.unwrap();
            engine.close().await.unwrap();
            drop(engine);

            // Reopen at the same ceiling capacity. `RecoverMode::Strict` panics on any recovery error, so a
            // successful build already proves the retired tail was not scanned (the file is physically shorter
            // than the ceiling; scanning past its end would fail).
            let reopened = engine_for_resize_test(&path).await;

            assert_eq!(reopened.inner.block_manager.blocks(), BLOCKS);
            assert_eq!(reopened.inner.block_manager.retired_count(), 2);
            assert_eq!(std::fs::metadata(&path).unwrap().len(), (2 * BLOCK_SIZE) as u64);

            assert_eq!(
                reopened.load(memory.hash(&1)).await.unwrap().kv().unwrap(),
                (1, vec![1u8; ENTRY_VALUE_SIZE])
            );
            assert_eq!(
                reopened.load(memory.hash(&2)).await.unwrap().kv().unwrap(),
                (2, vec![2u8; ENTRY_VALUE_SIZE])
            );
        }

        /// Same engine shape as `engine_for_resize_test`, but with the engine's *default* `clean_block_threshold`
        /// (1, not 0), so `resize_disk`'s functional floor is 2 active blocks — matching the downstream repro
        /// ("resize_disk down until ~2 blocks active") instead of the artificially deterministic 1-block floor
        /// the other tests in this module use.
        async fn engine_for_floor_test(
            path: impl AsRef<Path>,
            blocks: usize,
        ) -> Arc<BlockEngine<u64, Vec<u8>, TestProperties>> {
            let capacity = BLOCK_SIZE * blocks;
            let device = FileDeviceBuilder::new(path).with_capacity(capacity).build().unwrap();
            let spawner = Spawner::current();
            let io_engine = io_engine_for_test(spawner.clone()).await;
            BlockEngineConfig::<u64, Vec<u8>, TestProperties>::new(device)
                .with_block_size(BLOCK_SIZE)
                .boxed()
                .build(EngineBuildContext {
                    io_engine,
                    metrics: Arc::new(Metrics::noop()),
                    spawner,
                    recover_mode: RecoverMode::Strict,
                })
                .await
                .unwrap()
        }

        // VERGLAS PATCH: regression test for a defect the downstream acceptance test caught — after a shrink to
        // the functional floor (which forces the flusher's dangling current block closed via
        // `Submission::Rotate`) followed by a grow back to the ceiling, further inserts silently never reached
        // disk. `engine.load()` would still miss for them even after `engine.wait()` returned successfully,
        // because the write never actually landed: nothing had reported it as failed, it had simply been dropped
        // by the flusher's post-rotate handle re-acquisition (see `Runner::current_block_handle` in flusher.rs).
        #[test_log::test(tokio::test)]
        async fn test_resize_disk_insert_after_floor_shrink_and_grow_back_lands_on_disk() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("data");

            const FLOOR_BLOCKS: usize = 8;
            const FLOOR_CAPACITY: usize = BLOCK_SIZE * FLOOR_BLOCKS;

            let engine = engine_for_floor_test(&path, FLOOR_BLOCKS).await;
            let memory = cache_for_test();

            // Fill all 8 blocks (one entry per block; the last stays the flusher's dangling "current"). With the
            // default `clean_block_threshold` (1) this can also trigger opportunistic background reclaim mid-fill
            // — which entries end up surviving the fill isn't the point here, so nothing is asserted about it.
            insert_one_per_block(&engine, &memory, &[1, 2, 3, 4, 5, 6, 7, 8]).await;

            // Shrink to the functional floor: `target_bytes = 0` clamps up to the minimum active footprint
            // (`clean_block_threshold + 1` = 2 blocks here, matching the downstream repro's "~2 blocks active").
            // Whichever block the flusher is currently holding open as "current" is necessarily retired by this,
            // forcing a `Submission::Rotate`.
            let floor = engine.resize_disk(0).await.unwrap();
            assert_eq!(floor, (2 * BLOCK_SIZE) as u64);

            // Grow back to the full ceiling: restores the retired blocks to clean.
            let active = engine.resize_disk(FLOOR_CAPACITY as u64).await.unwrap();
            assert_eq!(active, FLOOR_CAPACITY as u64);
            assert_eq!(engine.inner.block_manager.retired_count(), 0);
            assert_eq!(std::fs::metadata(&path).unwrap().len(), FLOOR_CAPACITY as u64);

            // Insert 4 more entries and flush. This is the regression: these writes must actually reach disk,
            // not just appear to succeed.
            insert_one_per_block(&engine, &memory, &[9, 10, 11, 12]).await;

            for k in 9..=12u64 {
                let loaded = tokio::time::timeout(Duration::from_secs(5), engine.load(memory.hash(&k)))
                    .await
                    .expect("load must not hang")
                    .unwrap();
                assert_eq!(
                    loaded.kv(),
                    Some((k, vec![k as u8; ENTRY_VALUE_SIZE])),
                    "entry {k} inserted after floor shrink + grow-back did not land on disk"
                );
            }

            // Confirm disk residency independent of the indexer too: close and reopen, and the new entries must
            // still be there (a purely in-memory or dropped write would not survive this).
            engine.close().await.unwrap();
            drop(engine);
            let reopened = engine_for_floor_test(&path, FLOOR_BLOCKS).await;
            for k in 9..=12u64 {
                assert_eq!(
                    reopened.load(memory.hash(&k)).await.unwrap().kv(),
                    Some((k, vec![k as u8; ENTRY_VALUE_SIZE])),
                    "entry {k} did not survive reopen: it was never durably written"
                );
            }
        }
    }
}
