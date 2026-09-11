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

use std::sync::atomic::AtomicU64;

use bytes::{Buf, BufMut};
use foyer_common::error::{Error, ErrorKind, Result};

use crate::{compress::Compression, serde::Checksummer};

const ENTRY_MAGIC: u32 = 0x97_03_27_00;
const ENTRY_MAGIC_MASK: u32 = 0xFF_FF_FF_00;

pub type Sequence = u64;
pub type AtomicSequence = AtomicU64;

#[derive(Debug, PartialEq, Eq)]
pub struct EntryHeader {
    pub key_len: u32,
    pub value_len: u32,
    pub hash: u64,
    pub sequence: Sequence,
    pub checksum: u64,
    pub compression: Compression,
}

impl EntryHeader {
    pub const CHECKSUM_FIELD_OFFSET: usize = 4 + 4 + 8 + 8;
    pub const CHECKSUM_FIELD_LEN: usize = 8;
    pub const MAGIC_FIELD_LEN: usize = 4;
    pub const MAGIC_FIELD_OFFSET: usize = Self::CHECKSUM_FIELD_OFFSET + Self::CHECKSUM_FIELD_LEN;

    pub const fn serialized_len() -> usize {
        Self::CHECKSUM_FIELD_OFFSET + Self::CHECKSUM_FIELD_LEN + Self::MAGIC_FIELD_LEN
    }

    // Cover every entry byte except the embedded `checksum` field, which by
    // construction cannot cover itself. This protects the header fields —
    // including the `compression` discriminant carried by the trailing magic
    // word — as well as the key+value payload. The caller must bounds-check.
    pub fn checksum(buf: &[u8], key_len: usize, value_len: usize) -> u64 {
        let end = Self::serialized_len() + key_len + value_len;
        Checksummer::checksum64_chunks(&[&buf[..Self::CHECKSUM_FIELD_OFFSET], &buf[Self::MAGIC_FIELD_OFFSET..end]])
    }

    pub fn write(&self, mut buf: impl BufMut) {
        buf.put_u32(self.key_len);
        buf.put_u32(self.value_len);
        buf.put_u64(self.hash);
        buf.put_u64(self.sequence);
        buf.put_u64(self.checksum);

        let v = ENTRY_MAGIC | self.compression.to_u8() as u32;
        buf.put_u32(v);
    }

    pub fn read(mut buf: impl Buf) -> Result<Self> {
        let key_len = buf.get_u32();
        let value_len = buf.get_u32();
        let hash = buf.get_u64();
        let sequence = buf.get_u64();
        let checksum = buf.get_u64();

        let v = buf.get_u32();

        tracing::trace!(
            "read entry header, key len: {key_len}, value_len: {value_len}, hash: {hash}, sequence: {sequence}, checksum: {checksum}, extra: {v}"
        );

        let magic = v & ENTRY_MAGIC_MASK;
        if magic != ENTRY_MAGIC {
            return Err(Error::new(ErrorKind::MagicMismatch, "entry header magic mismatch")
                .with_context("expected", ENTRY_MAGIC)
                .with_context("get", magic));
        }
        let compression = Compression::try_from(v as u8)?;

        Ok(Self {
            key_len,
            value_len,
            hash,
            sequence,
            checksum,
            compression,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_entry_buf(key_len: usize, value_len: usize, compression: Compression) -> Vec<u8> {
        let total = EntryHeader::serialized_len() + key_len + value_len;
        let mut buf = vec![0u8; total];
        (&mut buf[0..4]).put_u32(key_len as u32);
        (&mut buf[4..8]).put_u32(value_len as u32);
        (&mut buf[8..16]).put_u64(0xdead_beef_cafe_babe);
        (&mut buf[16..24]).put_u64(0x1234_5678_9abc_def0);
        let v = ENTRY_MAGIC | compression.to_u8() as u32;
        (&mut buf[EntryHeader::MAGIC_FIELD_OFFSET..EntryHeader::MAGIC_FIELD_OFFSET + 4]).put_u32(v);
        // A deterministic, non-magic-bearing payload pattern.
        for (i, b) in buf[EntryHeader::serialized_len()..].iter_mut().enumerate() {
            *b = (i as u64).wrapping_mul(0x9e3779b9_7f4a7c15).wrapping_add(0x42) as u8;
        }
        let checksum = EntryHeader::checksum(&buf, key_len, value_len);
        (&mut buf
            [EntryHeader::CHECKSUM_FIELD_OFFSET..EntryHeader::CHECKSUM_FIELD_OFFSET + EntryHeader::CHECKSUM_FIELD_LEN])
            .put_u64(checksum);
        buf
    }

    #[test]
    fn entry_header_checksum_roundtrips() {
        let key_len = 8usize;
        let value_len = 64usize;
        for compression in [Compression::None, Compression::Zstd, Compression::Lz4] {
            let buf = build_entry_buf(key_len, value_len, compression);
            let header = EntryHeader::read(&buf[..EntryHeader::serialized_len()]).unwrap();
            assert_eq!(header.key_len as usize, key_len);
            assert_eq!(header.value_len as usize, value_len);
            assert_eq!(header.compression, compression);
            // The block engine's verify path recomputes the checksum and it must match.
            assert_eq!(EntryHeader::checksum(&buf, key_len, value_len), header.checksum);
        }
    }

    #[test]
    fn entry_header_checksum_detects_compression_flip_to_valid() {
        let key_len = 8usize;
        let value_len = 64usize;
        for compression in [Compression::None, Compression::Zstd, Compression::Lz4] {
            let buf = build_entry_buf(key_len, value_len, compression);
            let stored = EntryHeader::checksum(&buf, key_len, value_len);
            for other in [Compression::None, Compression::Zstd, Compression::Lz4] {
                if other == compression {
                    continue;
                }
                let mut corrupted = buf.clone();
                corrupted[EntryHeader::MAGIC_FIELD_OFFSET + 3] = other.to_u8();
                // The flip evades the magic mask check (the bug's premise) ...
                let hdr = EntryHeader::read(&corrupted[..EntryHeader::serialized_len()]).unwrap();
                assert_eq!(hdr.compression, other);
                assert_eq!(hdr.checksum, stored);
                // ... but the checksum now detects it.
                assert_ne!(EntryHeader::checksum(&corrupted, key_len, value_len), stored);
            }
        }
    }

    #[test]
    fn entry_header_checksum_elides_checksum_field() {
        let key_len = 8usize;
        let value_len = 64usize;
        let buf = build_entry_buf(key_len, value_len, Compression::None);
        let c0 = EntryHeader::checksum(&buf, key_len, value_len);
        let mut modified = buf.clone();
        for i in 0..EntryHeader::CHECKSUM_FIELD_LEN {
            let off = EntryHeader::CHECKSUM_FIELD_OFFSET + i;
            modified[off] = !modified[off];
        }
        let c1 = EntryHeader::checksum(&modified, key_len, value_len);
        assert_eq!(c0, c1, "the checksum field must be elided from its own hash");
    }

    #[test]
    fn entry_header_checksum_detects_covered_corruption() {
        let key_len = 8usize;
        let value_len = 64usize;
        let buf = build_entry_buf(key_len, value_len, Compression::None);
        let stored = EntryHeader::checksum(&buf, key_len, value_len);
        let covered = (0..EntryHeader::CHECKSUM_FIELD_OFFSET)
            .chain(EntryHeader::MAGIC_FIELD_OFFSET..EntryHeader::serialized_len())
            .chain(EntryHeader::serialized_len()..EntryHeader::serialized_len() + key_len + value_len);
        for off in covered {
            let mut corrupted = buf.clone();
            corrupted[off] ^= 0xff;
            assert_ne!(
                EntryHeader::checksum(&corrupted, key_len, value_len),
                stored,
                "corruption at covered byte {off} must be detected",
            );
        }
    }
}
