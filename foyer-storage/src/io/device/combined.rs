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

use std::sync::{Arc, RwLock};

use foyer_common::error::{Error, ErrorKind, Result};

use crate::{
    RawFile, Statistics, Throttle,
    io::device::{Device, DeviceBuilder, Partition, PartitionId},
};

/// Builder for a combined device that wraps multiple devices and allows access to their blocks.
///
/// The throttle and statistics of the combined device will override the inner devices' throttles and statistics.
///
/// NOTE: The kind of device is a preview, the throttle and statistics strategy is likely to be modified later.
#[derive(Debug)]
pub struct CombinedDeviceBuilder {
    devices: Vec<Arc<dyn Device>>,
    throttle: Throttle,
}

impl Default for CombinedDeviceBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl CombinedDeviceBuilder {
    /// Create a new combined device builder with empty devices.
    pub fn new() -> Self {
        Self {
            devices: vec![],
            throttle: Throttle::default(),
        }
    }

    /// Add a device to the combined device builder.
    pub fn with_device(mut self, device: Arc<dyn Device>) -> Self {
        self.devices.push(device);
        self
    }

    /// Set the throttle for the combined device to override the inner devices' throttles.
    ///
    /// The throttles of the combined devices are disabled by default.
    pub fn with_throttle(mut self, throttle: Throttle) -> Self {
        self.throttle = throttle;
        self
    }
}

impl DeviceBuilder for CombinedDeviceBuilder {
    fn build(self) -> Result<Arc<dyn Device>> {
        let device = CombinedDevice {
            devices: self.devices,
            statistics: Arc::new(Statistics::new(self.throttle)),
            inner: RwLock::new(Inner {
                partitions: vec![],
                next: 0,
            }),
        };
        let device = Arc::new(device);
        Ok(device)
    }
}

#[derive(Debug)]
struct Inner {
    partitions: Vec<Arc<CombinedPartition>>,
    next: usize,
}

/// [`CombinedDevice`] is a wrapper for other device to use only a part of it.
#[derive(Debug)]
pub struct CombinedDevice {
    devices: Vec<Arc<dyn Device>>,
    inner: RwLock<Inner>,
    statistics: Arc<Statistics>,
}

impl Device for CombinedDevice {
    fn capacity(&self) -> usize {
        self.devices.iter().map(|d| d.capacity()).sum()
    }

    fn allocated(&self) -> usize {
        self.devices.iter().map(|d| d.allocated()).sum()
    }

    fn create_partition(&self, size: usize) -> Result<Arc<dyn Partition>> {
        let mut inner = self.inner.write().unwrap();
        loop {
            if inner.next >= self.devices.len() {
                let capacity = self.devices.iter().map(|d| d.capacity()).sum::<usize>();
                return Err(Error::no_space(capacity, capacity, size));
            }
            let device = &self.devices[inner.next];
            match device.create_partition(size) {
                Ok(p) => {
                    let partition = CombinedPartition {
                        inner: p,
                        id: inner.partitions.len() as PartitionId,
                        statistics: self.statistics.clone(),
                    };
                    let partition = Arc::new(partition);
                    inner.partitions.push(partition.clone());
                    return Ok(partition);
                }
                Err(e) => {
                    if e.kind() == ErrorKind::NoSpace {
                        inner.next += 1;
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    fn partitions(&self) -> usize {
        self.inner.read().unwrap().partitions.len()
    }

    fn partition(&self, id: PartitionId) -> Arc<dyn Partition> {
        self.inner.read().unwrap().partitions[id as usize].clone()
    }

    fn statistics(&self) -> &Arc<Statistics> {
        &self.statistics
    }
}

#[derive(Debug)]
pub struct CombinedPartition {
    inner: Arc<dyn Partition>,
    id: PartitionId,
    statistics: Arc<Statistics>,
}

impl Partition for CombinedPartition {
    fn id(&self) -> PartitionId {
        self.id
    }

    fn size(&self) -> usize {
        self.inner.size()
    }

    fn translate(&self, address: u64) -> (RawFile, u64) {
        self.inner.translate(address)
    }

    fn statistics(&self) -> &Arc<Statistics> {
        &self.statistics
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    /// A minimal in-memory device that mirrors the `FileDevice`/`FsDevice`
    /// semantics: `allocated()` is the sum of partition sizes and
    /// `create_partition` returns `NoSpace` iff `allocated + size > capacity`.
    ///
    /// This avoids filesystem access while faithfully exercising
    /// `CombinedDevice` allocation accounting.
    #[derive(Debug)]
    struct MockDevice {
        capacity: usize,
        partitions: Mutex<Vec<Arc<MockPartition>>>,
        statistics: Arc<Statistics>,
    }

    #[derive(Debug)]
    struct MockPartition {
        id: PartitionId,
        size: usize,
        statistics: Arc<Statistics>,
    }

    impl Partition for MockPartition {
        fn id(&self) -> PartitionId {
            self.id
        }
        fn size(&self) -> usize {
            self.size
        }
        fn translate(&self, _address: u64) -> (RawFile, u64) {
            (RawFile(0 as _), 0)
        }
        fn statistics(&self) -> &Arc<Statistics> {
            &self.statistics
        }
    }

    impl MockDevice {
        fn new(capacity: usize) -> Arc<Self> {
            Arc::new(MockDevice {
                capacity,
                partitions: Mutex::new(vec![]),
                statistics: Arc::new(Statistics::new(Throttle::default())),
            })
        }
    }

    impl Device for MockDevice {
        fn capacity(&self) -> usize {
            self.capacity
        }
        fn allocated(&self) -> usize {
            self.partitions.lock().unwrap().iter().map(|p| p.size).sum()
        }
        fn create_partition(&self, size: usize) -> Result<Arc<dyn Partition>> {
            let mut partitions = self.partitions.lock().unwrap();
            let allocated = partitions.iter().map(|p| p.size).sum::<usize>();
            if allocated + size > self.capacity {
                return Err(Error::no_space(self.capacity, allocated, allocated + size));
            }
            let id = partitions.len() as PartitionId;
            let partition = Arc::new(MockPartition {
                id,
                size,
                statistics: self.statistics.clone(),
            });
            partitions.push(partition.clone());
            Ok(partition as Arc<dyn Partition>)
        }
        fn partitions(&self) -> usize {
            self.partitions.lock().unwrap().len()
        }
        fn partition(&self, id: PartitionId) -> Arc<dyn Partition> {
            self.partitions.lock().unwrap()[id as usize].clone() as Arc<dyn Partition>
        }
        fn statistics(&self) -> &Arc<Statistics> {
            &self.statistics
        }
    }

    // Direction 1: over-count → under-free → silent loss.
    // One leading device with capacity < block_size (4 MiB < 16 MiB default).
    #[test]
    fn test_combined_device_one_leading_small_silent_loss() {
        let block_size: usize = 16 * 1024 * 1024; // default per engine.rs

        let device_a: Arc<dyn Device> = MockDevice::new(4 * 1024 * 1024); // 4 MiB, 4K-aligned, < block_size
        let device_b: Arc<dyn Device> = MockDevice::new(48 * 1024 * 1024); // 48 MiB = 3 blocks
        let device = CombinedDeviceBuilder::new()
            .with_device(device_a)
            .with_device(device_b)
            .build()
            .unwrap();

        let mut count = 0;
        loop {
            if device.free() < block_size {
                break;
            }
            match device.create_partition(block_size) {
                Ok(_p) => count += 1,
                Err(e) if e.kind() == ErrorKind::NoSpace => break,
                Err(e) => panic!("unexpected error: {e:?}"),
            }
        }
        assert_eq!(count, 3, "should fit 3 blocks, only got {}", count);
    }

    // Direction 2: over-count, n ≥ 2 leading small devices → allocated() > capacity() → underflow.
    #[test]
    fn test_combined_device_two_leading_allocated_exceeds_capacity() {
        let block_size: usize = 16 * 1024 * 1024;

        let device_a: Arc<dyn Device> = MockDevice::new(4 * 1024 * 1024);
        let device_b: Arc<dyn Device> = MockDevice::new(4 * 1024 * 1024);
        let device_c: Arc<dyn Device> = MockDevice::new(48 * 1024 * 1024);
        let device = CombinedDeviceBuilder::new()
            .with_device(device_a)
            .with_device(device_b)
            .with_device(device_c)
            .build()
            .unwrap();

        for _ in 0..3 {
            device.create_partition(block_size).unwrap();
        }

        assert!(
            device.allocated() <= device.capacity(),
            "allocated ({}) exceeds capacity ({}) — free() would underflow",
            device.allocated(),
            device.capacity()
        );
    }

    // Direction 3: under-count → over-free → masked by NoSpace.
    // A device creates multiple partitions before NoSpace; take(inner.next) omits most of them.
    #[test]
    fn test_combined_device_multi_partition_under_count() {
        let block_size: usize = 16 * 1024 * 1024;

        let device_a: Arc<dyn Device> = MockDevice::new(3 * block_size); // 3 partitions before NoSpace
        let device_b: Arc<dyn Device> = MockDevice::new(block_size); // 1 partition
        let device = CombinedDeviceBuilder::new()
            .with_device(device_a)
            .with_device(device_b)
            .build()
            .unwrap();

        for _ in 0..4 {
            device.create_partition(block_size).unwrap();
        }

        assert_eq!(
            device.allocated(),
            4 * block_size,
            "allocated() should be {} but reported {}",
            4 * block_size,
            device.allocated()
        );
    }
}
