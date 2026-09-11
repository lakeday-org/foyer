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
    collections::{HashMap, hash_map::Entry},
    fmt::Debug,
    sync::{Arc, atomic::Ordering},
    time::Instant,
};

use foyer_common::{
    error::{Error, ErrorKind, Result},
    metrics::Metrics,
    spawn::Spawner,
};
use futures_util::{StreamExt, TryStreamExt, stream};
use itertools::Itertools;

use super::indexer::{EntryAddress, Indexer};
use crate::engine::{
    RecoverMode,
    block::{
        indexer::HashedEntryAddress,
        manager::{Block, BlockId, BlockManager},
        scanner::{BlockScanner, EntryInfo},
        serde::{AtomicSequence, Sequence},
        tombstone::Tombstone,
    },
};

#[derive(Debug)]
pub struct RecoverRunner;

/// Order recovered evictable `(block_id, latest_entry_sequence)` pairs oldest-written
/// (lowest sequence) first.
///
/// [`FifoPicker`](super::eviction::FifoPicker) and other order-sensitive eviction pickers
/// reconstruct their eviction queue from the call order of
/// [`EvictionPicker::on_block_evictable`](super::eviction::EvictionPicker::on_block_evictable).
/// After a restart the picker queue must be rebuilt in true write-recency order, otherwise
/// a newer-written recovered block may be evicted before an older-written one.
///
/// Block ids are **not** a reliable recency proxy: they are recycled after reclamation
/// (a reclaimed id is returned to the back of the clean list and later reused), so a low
/// id may hold data written more recently than a high id. The persisted per-entry write
/// `sequence` is monotonic across the cache lifetime and is therefore the correct key.
fn order_evictable_blocks_by_sequence(mut evictable_blocks: Vec<(BlockId, Sequence)>) -> Vec<BlockId> {
    evictable_blocks.sort_by_key(|(_, sequence)| *sequence);
    evictable_blocks.into_iter().map(|(block, _)| block).collect()
}

impl RecoverRunner {
    #[expect(clippy::too_many_arguments)]
    pub async fn run(
        recover_concurrency: usize,
        recover_mode: RecoverMode,
        blob_index_size: usize,
        blocks: Vec<BlockId>,
        sequence: &AtomicSequence,
        indexer: &Indexer,
        block_manager: &BlockManager,
        tombstones: &[Tombstone],
        spawner: Spawner,
        metrics: Arc<Metrics>,
    ) -> Result<()> {
        let now = Instant::now();

        // Recover blocks concurrently.
        let mode = recover_mode;
        let total = stream::iter(blocks.into_iter().map(|id| {
            let block = block_manager.block(id).clone();
            spawner.spawn(async move { BlockRecoverRunner::run(mode, block, blob_index_size).await })
        }))
        .buffered(recover_concurrency)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        // Return error is there is.
        let (total, errs): (Vec<_>, Vec<_>) = total.into_iter().partition(|res| res.is_ok());
        if !errs.is_empty() {
            let mut e = Error::new(ErrorKind::Recover, "failed to recover blocks");
            for err in errs.into_iter().map(|r| r.unwrap_err()) {
                e = e.with_context("reason", err.to_string());
            }
            return Err(e);
        }

        #[derive(Debug)]
        enum EntryAddressOrTombstone {
            EntryAddress(EntryAddress),
            Tombstone,
        }

        // Dedup entries.
        let mut latest_sequence = 0;
        let mut indices: HashMap<u64, (Sequence, EntryAddressOrTombstone)> = HashMap::new();
        let mut clean_blocks = vec![];
        let mut evictable_blocks: Vec<(BlockId, Sequence)> = vec![];

        let mut insert_or_update =
            |hash: u64, sequence: Sequence, addr: EntryAddressOrTombstone| match indices.entry(hash) {
                Entry::Occupied(mut entry) => {
                    let (latest, latest_addr) = entry.get_mut();
                    if sequence >= *latest {
                        *latest = sequence;
                        *latest_addr = addr;
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert((sequence, addr));
                }
            };
        for (block, infos) in total.into_iter().map(|r| r.unwrap()).enumerate() {
            let block = block as BlockId;

            if infos.is_empty() {
                clean_blocks.push(block);
                continue;
            }

            // A block's recency (FIFO age) is the sequence of its latest write, i.e. the
            // maximum `addr.sequence` among its entries. Recovery has this per-entry data
            // available and only needs to aggregate it per block so that the evictable
            // blocks can be fed to the pickers in true oldest-written-first order below.
            let mut max_sequence = 0;
            for EntryInfo { hash, addr } in infos {
                latest_sequence = latest_sequence.max(addr.sequence);
                max_sequence = max_sequence.max(addr.sequence);
                insert_or_update(hash, addr.sequence, EntryAddressOrTombstone::EntryAddress(addr));
            }
            evictable_blocks.push((block, max_sequence));
        }
        tombstones.iter().for_each(|tombstone| {
            latest_sequence = latest_sequence.max(tombstone.sequence);
            insert_or_update(tombstone.hash, tombstone.sequence, EntryAddressOrTombstone::Tombstone);
        });
        let indices = indices
            .into_iter()
            .filter_map(|(hash, (sequence, addr))| {
                tracing::trace!("[recover runner]: hash {hash} has version: {sequence:?} {addr:?}");
                match addr {
                    EntryAddressOrTombstone::Tombstone => None,
                    EntryAddressOrTombstone::EntryAddress(address) => Some(HashedEntryAddress { hash, address }),
                }
            })
            .collect_vec();

        // Log recovery.
        tracing::info!(
            "Recovers {e} blocks with data, {c} clean blocks, {t} total entries with max sequence as {s}..",
            e = evictable_blocks.len(),
            c = clean_blocks.len(),
            t = indices.len(),
            s = latest_sequence,
        );

        // Update components.
        indexer.insert_batch(indices);
        sequence.store(latest_sequence + 1, Ordering::Release);

        // Feed the recovered evictable blocks to the block manager in oldest-written-first
        // order. `FifoPicker` and other order-sensitive pickers rely on this call order to
        // reconstruct their eviction queue across restarts; an unordered iteration here
        // would corrupt FIFO eviction order for the recovered cohort.
        let evictable_blocks = order_evictable_blocks_by_sequence(evictable_blocks);
        block_manager.init(&clean_blocks, &evictable_blocks);

        let elapsed = now.elapsed();
        tracing::info!("[recover] finish in {:?}", elapsed);

        metrics
            .storage_block_engine_recover_duration
            .record(elapsed.as_secs_f64());

        Ok(())
    }
}

#[derive(Debug)]
struct BlockRecoverRunner;

impl BlockRecoverRunner {
    async fn run(mode: RecoverMode, block: Block, blob_index_size: usize) -> Result<Vec<EntryInfo>> {
        if mode == RecoverMode::None {
            return Ok(vec![]);
        }

        let mut recovered = vec![];

        let id = block.id();
        let mut iter = BlockScanner::new(block, blob_index_size);
        'recover: loop {
            let r = iter.next().await;
            let infos = match r {
                Ok(Some(infos)) => infos,
                Ok(None) => break,
                Err(e) => {
                    if mode == RecoverMode::Strict {
                        return Err(e);
                    } else {
                        tracing::warn!("error raised when recovering block {id}, skip further recovery for {id}.");
                        break;
                    }
                }
            };

            for info in infos {
                if info.addr.sequence < recovered.last().map(|last: &EntryInfo| last.addr.sequence).unwrap_or(0) {
                    break 'recover;
                }
                recovered.push(info);
            }
        }

        Ok(recovered)
    }
}

#[cfg(test)]
mod tests {
    use super::order_evictable_blocks_by_sequence;
    use crate::engine::block::{manager::BlockId, serde::Sequence};

    #[test]
    fn test_order_evictable_blocks_by_sequence_uses_sequence_not_block_id() {
        // (block_id, latest entry sequence). Block id 0 holds the NEWEST write (its id was
        // recycled after a reclamation cycle), so it must be ordered last despite being the
        // lowest id. Ordering by block id would wrongly evict it first.
        let input: Vec<(BlockId, Sequence)> = vec![(0, 80), (2, 20), (3, 30), (5, 50), (7, 70)];
        let ordered = order_evictable_blocks_by_sequence(input);
        assert_eq!(ordered, vec![2, 3, 5, 7, 0]);
    }

    #[test]
    fn test_order_evictable_blocks_by_sequence_empty() {
        assert!(order_evictable_blocks_by_sequence(vec![]).is_empty());
    }

    #[test]
    fn test_order_evictable_blocks_by_sequence_ties_preserve_block_id_order() {
        // Recovery feeds blocks in ascending block-id order; `sort_by_key` is stable, so
        // blocks sharing a sequence (e.g. written in the same flush batch) keep that order.
        let input: Vec<(BlockId, Sequence)> = vec![(1, 10), (2, 10), (5, 10), (9, 10)];
        let ordered = order_evictable_blocks_by_sequence(input);
        assert_eq!(ordered, vec![1, 2, 5, 9]);
    }
}
