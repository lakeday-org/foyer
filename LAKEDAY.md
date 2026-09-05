# Lakeday's Foyer patches

This is a public GitHub fork of [foyer-rs/foyer](https://github.com/foyer-rs/foyer).
The `main` branch follows upstream. The `release/v0.22.3` branch is the unchanged
upstream `v0.22.3` tag (`ff6b01512e580665a217c2bd892e0a884ae749e6`).
Lakeday's patches are proposed together on `lakeday/cache-reclamation`, against
that release branch. Consumers must pin the reviewed patch commit by SHA.

## Carried patch set

The source and regression tests are preserved from commits `a802626` and
`58a7f001d8cd894423106713904daf948db20e65` in the former
`verglas-org/verglas-foyer` repository. This migration does not change their
behavior. Existing `VERGLAS PATCH` comments record their provenance. Linux CI
also required removing one redundant `.into_iter()` in the upstream io_uring
implementation for Rust 1.96.1 Clippy; that correction does not change behavior.

- Live disk reclamation through `HybridCache::resize_disk`, `Store`, and the
  block engine. Shrinking retires tail blocks before truncating the backing
  file; growing extends the file before restoring those blocks. Active
  capacity stays between the engine's minimum working size and its opening
  ceiling. Flusher rotation and deferred block reservation allow shrinking
  while writes are in flight or the cache is idle.
- A submit-queue default derived from the configured buffer-pool size, avoiding
  unexpected dropped cache writes when the buffer pool grows.
- Supporting device methods, diagnostics, a test-import correction, and a
  Clippy correction carried with those changes.

Physical resizing is supported by `FileDevice`. `FsDevice`, `CombinedDevice`,
and `PartialDevice` reject physical resizing. The runtime must choose a supported
device and coordinate query reservations with completed cache reclamation;
this fork does not itself implement query memory or spill management.

The regression tests cover block retirement, physical truncation, growth,
concurrent writes, reopening after shrink, and writes after shrinking to the
minimum and growing again. The Lakeday workflow runs formatting, Clippy, and
library tests for the affected packages.

## Updating upstream

Keep `main` free of Lakeday patches so GitHub's fork synchronization can follow
upstream directly. For a library upgrade, create a new release branch from the
chosen upstream tag, apply the remaining patch set in one PR, and run its tests
plus the consumer's cache tests. Drop any patch that upstream has incorporated.
Update consumer SHAs only after verifying that release and patch combination.
