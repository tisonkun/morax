// Copyright 2024 tison <wander4096@gmail.com>
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

use std::cell::Ref;
use std::cell::RefCell;
use std::cell::RefMut;
use std::fmt::Debug;
use std::fmt::Formatter;

use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;

use crate::codec::err_codec_message;
use crate::codec::Decoder;
use crate::codec::RecordList;
use crate::records::*;
use crate::IoResult;

fn check_and_fetch_batch_size(bytes: &[u8], remaining: usize) -> IoResult<usize> {
    if remaining < RECORD_BATCH_OVERHEAD {
        return Err(err_codec_message(format!(
            "no enough bytes when decode records (remaining: {}, required header: {})",
            remaining, RECORD_BATCH_OVERHEAD
        )));
    }

    let record_size = (&bytes[LENGTH_OFFSET..])
        .read_i32::<BigEndian>()
        .map_err(|err| err_codec_message(format!("failed to read record size: {err}")))?;
    let batch_size = record_size as usize + LOG_OVERHEAD;
    if remaining < batch_size {
        return Err(err_codec_message(format!(
            "no enough bytes when decode records (remaining: {}, required batch: {})",
            remaining, batch_size
        )));
    }

    let magic = (&bytes[MAGIC_OFFSET..])
        .read_i8()
        .map_err(|err| err_codec_message(format!("failed to read version: {err}")))?;
    if magic != 2 {
        return Err(err_codec_message(format!(
            "unsupported record batch version: {}",
            magic
        )));
    }

    Ok(batch_size)
}

#[derive(Default)]
pub struct RecordBatches {
    bytes: RefCell<Vec<u8>>,
}

impl Debug for RecordBatches {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("RecordBatches");
        de.field("batches", &self.batches());
        de.finish()
    }
}

impl RecordBatches {
    pub fn new(bytes: Vec<u8>) -> Self {
        RecordBatches {
            bytes: RefCell::new(bytes),
        }
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes.into_inner()
    }

    pub fn mut_batches(&mut self) -> IoResult<Vec<MutableRecordBatch>> {
        let mut batches = vec![];
        let mut bytes = RefMut::map(self.bytes.borrow_mut(), |bs| bs.as_mut_slice());
        let mut remaining = bytes.len();
        while remaining > 0 {
            let batch_size = check_and_fetch_batch_size(&bytes, remaining)?;
            let (left, right) = RefMut::map_split(bytes, |b| b.split_at_mut(batch_size));
            batches.push(MutableRecordBatch { bytes: left });
            bytes = right;
            remaining -= batch_size;
        }
        Ok(batches)
    }

    pub fn batches(&self) -> IoResult<Vec<RecordBatch>> {
        let mut batches = vec![];
        let mut bytes = Ref::map(self.bytes.borrow(), |bs| bs.as_slice());
        let mut remaining = bytes.len();
        while remaining > 0 {
            let batch_size = check_and_fetch_batch_size(&bytes, remaining)?;
            let (left, right) = Ref::map_split(bytes, |b| b.split_at(batch_size));
            batches.push(RecordBatch { bytes: left });
            bytes = right;
            remaining -= batch_size;
        }
        Ok(batches)
    }
}

pub struct MutableRecordBatch<'a> {
    bytes: RefMut<'a, [u8]>,
}

// SAFETY: record's length are validated on construction; so all slices are valid.
impl MutableRecordBatch<'_> {
    pub fn view(&self) -> RecordBatchView {
        RecordBatchView { bytes: &self.bytes }
    }

    pub fn set_last_offset(&mut self, offset: i64) {
        let base_offset = offset - self.view().last_offset_delta() as i64;
        (&mut self.bytes[BASE_OFFSET_OFFSET..])
            .write_i64::<BigEndian>(base_offset)
            .expect("write base offset");
    }

    pub fn set_partition_leader_epoch(&mut self, epoch: i32) {
        (&mut self.bytes[PARTITION_LEADER_EPOCH_OFFSET..])
            .write_i32::<BigEndian>(epoch)
            .expect("write partition leader epoch");
    }
}

impl Debug for MutableRecordBatch<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        RecordBatchView::fmt(&self.view(), f)
    }
}

pub struct RecordBatch<'a> {
    bytes: Ref<'a, [u8]>,
}

impl RecordBatch<'_> {
    pub fn view(&self) -> RecordBatchView {
        RecordBatchView { bytes: &self.bytes }
    }
}

impl Debug for RecordBatch<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        RecordBatchView::fmt(&self.view(), f)
    }
}

pub struct RecordBatchView<'a> {
    bytes: &'a [u8],
}

impl Debug for RecordBatchView<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("RecordBatch");
        de.field("magic", &self.magic());
        de.field("offset", &(self.base_offset()..=self.last_offset()));
        de.field("sequence", &(self.base_sequence()..=self.last_sequence()));
        de.field("is_transactional", &self.is_transactional());
        de.field("is_control_batch", &self.is_control_batch());
        de.field("compression_type", &self.compression_type());
        de.field("timestamp_type", &self.timestamp_type());
        de.field("crc", &self.checksum());
        de.field("records_count", &self.records_count());
        de.field("records", &self.records());
        de.finish()
    }
}

/// Similar to [i32::wrapping_add], but wrap to `0` instead of [i32::MIN].
pub fn increment_sequence(sequence: i32, increment: i32) -> i32 {
    if sequence > i32::MAX - increment {
        increment - (i32::MAX - sequence) - 1
    } else {
        sequence + increment
    }
}

/// Similar to [i32::wrapping_add], but wrap at `0` instead of [i32::MIN].
pub fn decrement_sequence(sequence: i32, decrement: i32) -> i32 {
    if sequence < decrement {
        i32::MAX - (decrement - sequence) + 1
    } else {
        sequence - decrement
    }
}

// SAFETY: record's length are validated on construction; so all slices are valid.
impl RecordBatchView<'_> {
    pub fn magic(&self) -> i8 {
        (&self.bytes[MAGIC_OFFSET..]).read_i8().expect("read magic")
    }

    pub fn base_offset(&self) -> i64 {
        (&self.bytes[BASE_OFFSET_OFFSET..])
            .read_i64::<BigEndian>()
            .expect("read base offset")
    }

    pub fn last_offset(&self) -> i64 {
        self.base_offset() + self.last_offset_delta() as i64
    }

    pub fn base_sequence(&self) -> i32 {
        (&self.bytes[BASE_SEQUENCE_OFFSET..])
            .read_i32::<BigEndian>()
            .expect("read base sequence")
    }

    pub fn last_sequence(&self) -> i32 {
        match self.base_sequence() {
            NO_SEQUENCE => NO_SEQUENCE,
            seq => increment_sequence(seq, self.last_offset_delta()),
        }
    }

    fn last_offset_delta(&self) -> i32 {
        (&self.bytes[LAST_OFFSET_DELTA_OFFSET..])
            .read_i32::<BigEndian>()
            .expect("read last offset delta")
    }

    pub fn max_timestamp(&self) -> i64 {
        (&self.bytes[MAX_TIMESTAMP_OFFSET..])
            .read_i64::<BigEndian>()
            .expect("read max timestamp")
    }

    pub fn records_count(&self) -> i32 {
        (&self.bytes[RECORDS_COUNT_OFFSET..])
            .read_i32::<BigEndian>()
            .expect("read records count")
    }

    pub fn records(&self) -> Vec<Record> {
        let mut records = &self.bytes[RECORDS_COUNT_OFFSET..];
        RecordList.decode(&mut records).expect("malformed records")
    }

    pub fn checksum(&self) -> u32 {
        (&self.bytes[CRC_OFFSET..])
            .read_u32::<BigEndian>()
            .expect("read checksum")
    }

    pub fn is_transactional(&self) -> bool {
        self.attributes() & TRANSACTIONAL_FLAG_MASK > 0
    }

    pub fn is_control_batch(&self) -> bool {
        self.attributes() & CONTROL_FLAG_MASK > 0
    }

    pub fn timestamp_type(&self) -> TimestampType {
        if self.attributes() & TIMESTAMP_TYPE_MASK != 0 {
            TimestampType::LogAppendTime
        } else {
            TimestampType::CreateTime
        }
    }

    pub fn compression_type(&self) -> CompressionType {
        (self.attributes() & COMPRESSION_CODEC_MASK).into()
    }

    pub fn delete_horizon_ms(&self) -> Option<i64> {
        if self.has_delete_horizon_ms() {
            Some(
                (&self.bytes[BASE_TIMESTAMP_OFFSET..])
                    .read_i64::<BigEndian>()
                    .expect("read base timestamp offset"),
            )
        } else {
            None
        }
    }

    fn has_delete_horizon_ms(&self) -> bool {
        self.attributes() & DELETE_HORIZON_FLAG_MASK > 0
    }

    // note we're not using the second byte of attributes
    fn attributes(&self) -> u8 {
        (&self.bytes[ATTRIBUTES_OFFSET..])
            .read_u16::<BigEndian>()
            .expect("read attributes") as u8
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use crate::records::record_batch::RecordBatches;

    const RECORD: &[u8] = &[
        // batch 1
        0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // first offset
        0x0, 0x0, 0x0, 0x52, // record batch size
        0xFF, 0xFF, 0xFF, 0xFF, // partition leader epoch
        0x2,  // magic byte
        0xE2, 0x3F, 0xC9, 0x74, // crc
        0x0, 0x0, // attributes
        0x0, 0x0, 0x0, 0x0, // last offset delta
        0x0, 0x0, 0x1, 0x89, 0xAF, 0x78, 0x40, 0x72, // base timestamp
        0x0, 0x0, 0x1, 0x89, 0xAF, 0x78, 0x40, 0x72, // max timestamp
        0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, // producer ID
        0x0, 0x0, // producer epoch
        0x0, 0x0, 0x0, 0x0, // base sequence
        0x0, 0x0, 0x0, 0x1,  // record counts
        0x40, // first record size
        0x0,  // attribute
        0x0,  // timestamp delta
        0x0,  // offset delta
        0x1,  // key length (zigzag : -1)
        // empty key payload
        0x34, // value length (zigzag : 26)
        0x54, 0x68, 0x69, 0x73, 0x20, 0x69, 0x73, 0x20, 0x74, 0x68, 0x65, 0x20, 0x66, 0x69, 0x72,
        0x73, 0x74, 0x20, 0x6D, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2E, // value payload
        0x0,  // header counts
        // batch 2
        0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // first offset
        0x0, 0x0, 0x0, 0x52, // record batch size
        0xFF, 0xFF, 0xFF, 0xFF, // partition leader epoch
        0x2,  // magic byte
        0xE2, 0x3F, 0xC9, 0x74, // crc
        0x0, 0x0, // attributes
        0x0, 0x0, 0x0, 0x0, // last offset delta
        0x0, 0x0, 0x1, 0x89, 0xAF, 0x78, 0x40, 0x72, // base timestamp
        0x0, 0x0, 0x1, 0x89, 0xAF, 0x78, 0x40, 0x72, // max timestamp
        0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, // producer ID
        0x0, 0x0, // producer epoch
        0x0, 0x0, 0x0, 0x0, // base sequence
        0x0, 0x0, 0x0, 0x1,  // record counts
        0x40, // first record size
        0x0,  // attribute
        0x0,  // timestamp delta
        0x0,  // offset delta
        0x1,  // key length (zigzag : -1)
        // empty key payload
        0x34, // value length (zigzag : 26)
        0x54, 0x68, 0x69, 0x73, 0x20, 0x69, 0x73, 0x20, 0x74, 0x68, 0x65, 0x20, 0x66, 0x69, 0x72,
        0x73, 0x74, 0x20, 0x6D, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2E, // value payload
        0x0,  // header counts
    ];

    #[test]
    fn test_codec_records() -> io::Result<()> {
        let records = RecordBatches::new(RECORD.to_vec());
        let record_batches = records.batches().unwrap();
        assert_eq!(record_batches.len(), 2);
        let record_batch = record_batches[0].view();
        assert_eq!(record_batch.records_count(), 1);
        let record_vec = record_batch.records();
        assert_eq!(record_vec.len(), 1);
        let record = &record_vec[0];
        assert_eq!(record.key_len, -1);
        assert_eq!(record.key, None);
        assert_eq!(record.value_len, 26);
        assert_eq!(
            record.value.as_deref().map(String::from_utf8_lossy),
            Some("This is the first message.".into())
        );
        Ok(())
    }
}
