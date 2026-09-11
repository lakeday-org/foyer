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
    sync::Arc,
};

use itertools::Itertools;
use parking_lot::RwLock;

use crate::engine::block::{manager::BlockId, serde::Sequence};

#[derive(Debug, Clone)]
pub enum Index {
    Address(EntryAddress),
    Tombstone(Sequence),
}

impl Index {
    fn sequence(&self) -> Sequence {
        match self {
            Index::Address(addr) => addr.sequence,
            Index::Tombstone(seq) => *seq,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HashedEntryAddress {
    pub hash: u64,
    pub address: EntryAddress,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryAddress {
    pub block: BlockId,
    pub offset: u32,
    pub len: u32,

    pub sequence: Sequence,
}

type IndexerShard = HashMap<u64, Index>;

/// [`Indexer`] records key hash to entry address on fs.
#[derive(Debug, Clone)]
pub struct Indexer {
    shards: Arc<Vec<RwLock<IndexerShard>>>,
}

impl Indexer {
    pub fn new(shards: usize) -> Self {
        let shards = (0..shards).map(|_| RwLock::new(HashMap::new())).collect_vec();
        Self {
            shards: Arc::new(shards),
        }
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::block::indexer::insert_tombstone")
    )]
    pub fn insert_tombstone(&self, hash: u64, sequence: Sequence) -> Option<EntryAddress> {
        let shard = self.shard(hash);
        let mut shard = self.shards[shard].write();
        self.insert_inner(&mut shard, hash, Index::Tombstone(sequence))
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::block::indexer::insert_batch")
    )]
    pub fn insert_batch(&self, batch: Vec<HashedEntryAddress>) -> Vec<HashedEntryAddress> {
        let shards: HashMap<usize, Vec<HashedEntryAddress>> =
            batch.into_iter().into_group_map_by(|haddr| self.shard(haddr.hash));

        let mut olds = vec![];
        for (s, batch) in shards {
            let mut shard = self.shards[s].write();
            for haddr in batch {
                if let Some(old) = self.insert_inner(&mut shard, haddr.hash, Index::Address(haddr.address)) {
                    olds.push(HashedEntryAddress {
                        hash: haddr.hash,
                        address: old,
                    });
                }
            }
        }
        olds
    }

    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::storage::block::indexer::get"))]
    pub fn get(&self, hash: u64) -> Option<EntryAddress> {
        let shard = self.shard(hash);
        match self.shards[shard].read().get(&hash) {
            Some(index) => match index {
                Index::Address(addr) => Some(addr.clone()),
                Index::Tombstone(_) => None,
            },
            None => None,
        }
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::block::indexer::remove")
    )]
    pub fn remove(&self, hash: u64) -> Option<EntryAddress> {
        let shard = self.shard(hash);
        match self.shards[shard].write().entry(hash) {
            Entry::Occupied(o) => match o.get() {
                Index::Address(_) => self.extract_address(o.remove()),
                Index::Tombstone(_) => None,
            },
            Entry::Vacant(_) => None,
        }
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::block::indexer::remove_batch")
    )]
    pub fn remove_batch<I>(&self, batch: I) -> Vec<EntryAddress>
    where
        I: IntoIterator<Item = (u64, Sequence)>,
    {
        let shards = batch.into_iter().into_group_map_by(|(hash, _)| self.shard(*hash));

        let mut olds = vec![];
        for (s, hashes) in shards {
            let mut shard = self.shards[s].write();
            for (hash, sequence) in hashes {
                match shard.entry(hash) {
                    Entry::Occupied(o) => {
                        // Only evict live `Address` entries; always retain `Tombstone`
                        // markers as a per-hash deletion watermark. The flusher calls
                        // `remove_batch` after persisting a tombstone; if the tombstone
                        // were dropped here the slot would become `Vacant`, and a stale
                        // in-flight reinsertion (older than the delete) would later be
                        // installed unconditionally by the `Vacant` branch of
                        // `insert_inner`, resurrecting the deleted entry. Keeping the
                        // tombstone lets the `Occupied`-branch sequence guard reject
                        // such stale reinsertions (`old_seq >= tombstone_seq` is false).
                        // A newer reinsertion/insert still supersedes the tombstone via
                        // the same sequence guard.
                        if sequence >= o.get().sequence()
                            && let Index::Address(_) = o.get()
                            && let Some(addr) = self.extract_address(o.remove())
                        {
                            olds.push(addr);
                        }
                    }
                    Entry::Vacant(_) => {}
                }
            }
        }
        olds
    }

    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::storage::block::indexer::clear"))]
    pub fn clear(&self) {
        self.shards.iter().for_each(|shard| shard.write().clear());
    }

    #[inline(always)]
    fn shard(&self, hash: u64) -> usize {
        hash as usize % self.shards.len()
    }

    fn insert_inner(&self, shard: &mut IndexerShard, hash: u64, index: Index) -> Option<EntryAddress> {
        match shard.entry(hash) {
            Entry::Occupied(mut o) => {
                // `>` for updates.
                // '=' for reinsertions.
                if index.sequence() >= o.get().sequence() {
                    self.extract_address(o.insert(index))
                } else {
                    self.extract_address(index)
                }
            }
            Entry::Vacant(v) => {
                v.insert(index);
                None
            }
        }
    }

    fn extract_address(&self, index: Index) -> Option<EntryAddress> {
        match index {
            Index::Address(addr) => Some(addr),
            Index::Tombstone(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(sequence: Sequence) -> EntryAddress {
        EntryAddress {
            block: 1,
            offset: 0,
            len: 64,
            sequence,
        }
    }

    fn hadrr(hash: u64, sequence: Sequence) -> HashedEntryAddress {
        HashedEntryAddress {
            hash,
            address: addr(sequence),
        }
    }

    /// After the flusher persists a tombstone and calls `remove_batch`, the
    /// tombstone must NOT be erased from the indexer. It stays as a deletion
    /// watermark so that a stale in-flight reinsertion (older than the delete)
    /// landing in `insert_batch` is rejected by the `Occupied`-branch sequence
    /// guard instead of being installed into a `Vacant` slot.
    #[test]
    fn stale_reinsertion_after_tombstone_persist_is_rejected() {
        let indexer = Indexer::new(4);
        let hash = 42u64;

        // Live entry @ S10.
        indexer.insert_batch(vec![hadrr(hash, 10)]);
        assert_eq!(indexer.get(hash), Some(addr(10)));

        // Delete @ S20.
        indexer.insert_tombstone(hash, 20);
        assert_eq!(indexer.get(hash), None);

        // Flusher persists the tombstone and removes it; the watermark must persist.
        indexer.remove_batch(vec![(hash, 20)]);
        assert_eq!(indexer.get(hash), None);

        // Stale reinsertion @ S10 arrives after the tombstone was persisted.
        indexer.insert_batch(vec![hadrr(hash, 10)]);
        assert!(
            indexer.get(hash).is_none(),
            "deleted entry must not reappear, but got {:?}",
            indexer.get(hash)
        );
    }

    /// A newer reinsertion/insert must supersede a retained tombstone and become
    /// live again (the delete is "undone" by a fresh write with a higher sequence).
    #[test]
    fn newer_insert_supersedes_retained_tombstone() {
        let indexer = Indexer::new(4);
        let hash = 42u64;
        indexer.insert_batch(vec![hadrr(hash, 10)]);
        indexer.insert_tombstone(hash, 20);
        indexer.remove_batch(vec![(hash, 20)]);
        // Re-write at S30 (> S20) replaces the retained tombstone with a live address.
        // A superseding insert produces no "old" evicted address (the displaced
        // value was a tombstone, which carries no address).
        assert!(indexer.insert_batch(vec![hadrr(hash, 30)]).is_empty());
        assert_eq!(indexer.get(hash), Some(addr(30)));
    }

    /// `remove_batch` must still evict live `Address` entries: the reclaimer relies
    /// on it to drop addresses of unpicked entries while reclaiming a block.
    #[test]
    fn remove_batch_still_evicts_live_addresses() {
        let indexer = Indexer::new(4);
        let hash = 42u64;
        indexer.insert_batch(vec![hadrr(hash, 10)]);
        let olds = indexer.remove_batch(vec![(hash, 10)]);
        assert_eq!(olds, vec![addr(10)]);
        assert_eq!(indexer.get(hash), None);
    }
}
