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
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

/// Configurations for tracing.
#[derive(Debug, Default)]
pub struct TracingConfig {
    /// Threshold for recording the hybrid cache `insert` and `insert_with_context` operation in us.
    record_hybrid_insert_threshold_us: AtomicU64,
    /// Threshold for recording the hybrid cache `get` operation in us.
    record_hybrid_get_threshold_us: AtomicU64,
    /// Threshold for recording the hybrid cache `remove` operation in us.
    record_hybrid_remove_threshold_us: AtomicU64,
    /// Threshold for recording the hybrid cache `get_or_fetch` operation in us.
    record_hybrid_get_or_fetch_threshold_us: AtomicU64,
}

impl TracingConfig {
    /// Update tracing config with options.
    pub fn update(&self, options: TracingOptions) {
        if let Some(threshold) = options.record_hybrid_insert_threshold {
            self.record_hybrid_insert_threshold_us
                .store(threshold.as_micros() as _, Ordering::Relaxed);
        }

        if let Some(threshold) = options.record_hybrid_get_threshold {
            self.record_hybrid_get_threshold_us
                .store(threshold.as_micros() as _, Ordering::Relaxed);
        }

        if let Some(threshold) = options.record_hybrid_remove_threshold {
            self.record_hybrid_remove_threshold_us
                .store(threshold.as_micros() as _, Ordering::Relaxed);
        }

        if let Some(threshold) = options.record_hybrid_get_or_fetch_threshold {
            self.record_hybrid_get_or_fetch_threshold_us
                .store(threshold.as_micros() as _, Ordering::Relaxed);
        }
    }

    /// Threshold for recording the hybrid cache `insert` and `insert_with_context` operation.
    pub fn record_hybrid_insert_threshold(&self) -> Duration {
        Duration::from_micros(self.record_hybrid_insert_threshold_us.load(Ordering::Relaxed))
    }

    /// Threshold for recording the hybrid cache `get` operation.
    pub fn record_hybrid_get_threshold(&self) -> Duration {
        Duration::from_micros(self.record_hybrid_get_threshold_us.load(Ordering::Relaxed))
    }

    /// Threshold for recording the hybrid cache `remove` operation.
    pub fn record_hybrid_remove_threshold(&self) -> Duration {
        Duration::from_micros(self.record_hybrid_remove_threshold_us.load(Ordering::Relaxed))
    }

    /// Threshold for recording the hybrid cache `get_or_fetch` operation.
    pub fn record_hybrid_get_or_fetch_threshold(&self) -> Duration {
        Duration::from_micros(self.record_hybrid_get_or_fetch_threshold_us.load(Ordering::Relaxed))
    }
}

/// Options for tracing.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TracingOptions {
    /// Threshold for recording the hybrid cache `insert` and `insert_with_context` operation.
    record_hybrid_insert_threshold: Option<Duration>,
    /// Threshold for recording the hybrid cache `get` operation.
    record_hybrid_get_threshold: Option<Duration>,
    /// Threshold for recording the hybrid cache `remove` operation.
    record_hybrid_remove_threshold: Option<Duration>,
    /// Threshold for recording the hybrid cache `get_or_fetch` operation.
    record_hybrid_get_or_fetch_threshold: Option<Duration>,
}

impl Default for TracingOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl TracingOptions {
    /// Create an empty tracing options.
    pub fn new() -> Self {
        Self {
            record_hybrid_insert_threshold: None,
            record_hybrid_get_threshold: None,
            record_hybrid_remove_threshold: None,
            record_hybrid_get_or_fetch_threshold: None,
        }
    }

    /// Set the threshold for recording the hybrid cache `insert` and `insert_with_context` operation.
    pub fn with_record_hybrid_insert_threshold(mut self, threshold: Duration) -> Self {
        self.record_hybrid_insert_threshold = Some(threshold);
        self
    }

    /// Set the threshold for recording the hybrid cache `get` operation.
    pub fn with_record_hybrid_get_threshold(mut self, threshold: Duration) -> Self {
        self.record_hybrid_get_threshold = Some(threshold);
        self
    }

    /// Set the threshold for recording the hybrid cache `remove` operation.
    pub fn with_record_hybrid_remove_threshold(mut self, threshold: Duration) -> Self {
        self.record_hybrid_remove_threshold = Some(threshold);
        self
    }

    /// Set the threshold for recording the hybrid cache `get_or_fetch` operation.
    pub fn with_record_hybrid_get_or_fetch_threshold(mut self, threshold: Duration) -> Self {
        self.record_hybrid_get_or_fetch_threshold = Some(threshold);
        self
    }
}
