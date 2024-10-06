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

pub const NO_SEQUENCE: i32 = -1;

// The current attributes are given below:
// ---------------------------------------------------------------------------------------------------------------------------
// | Unused (7-15) | Delete Horizon Flag (6) | Control (5) | Transactional (4) | Timestamp Type (3) | Compression Type (0-2) |
// ---------------------------------------------------------------------------------------------------------------------------
pub const COMPRESSION_CODEC_MASK: u8 = 0x07;
pub const TIMESTAMP_TYPE_MASK: u8 = 0x08;
pub const TRANSACTIONAL_FLAG_MASK: u8 = 0x10;
pub const CONTROL_FLAG_MASK: u8 = 0x20;
pub const DELETE_HORIZON_FLAG_MASK: u8 = 0x40;

// offset table
pub const BASE_OFFSET_OFFSET: usize = 0;
pub const BASE_OFFSET_LENGTH: usize = 8;
pub const LENGTH_OFFSET: usize = BASE_OFFSET_OFFSET + BASE_OFFSET_LENGTH;
pub const LENGTH_LENGTH: usize = 4;
pub const PARTITION_LEADER_EPOCH_OFFSET: usize = LENGTH_OFFSET + LENGTH_LENGTH;
pub const PARTITION_LEADER_EPOCH_LENGTH: usize = 4;
pub const MAGIC_OFFSET: usize = PARTITION_LEADER_EPOCH_OFFSET + PARTITION_LEADER_EPOCH_LENGTH;
pub const MAGIC_LENGTH: usize = 1;
pub const CRC_OFFSET: usize = MAGIC_OFFSET + MAGIC_LENGTH;
pub const CRC_LENGTH: usize = 4;
pub const ATTRIBUTES_OFFSET: usize = CRC_OFFSET + CRC_LENGTH;
pub const ATTRIBUTE_LENGTH: usize = 2;
pub const LAST_OFFSET_DELTA_OFFSET: usize = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
pub const LAST_OFFSET_DELTA_LENGTH: usize = 4;
pub const BASE_TIMESTAMP_OFFSET: usize = LAST_OFFSET_DELTA_OFFSET + LAST_OFFSET_DELTA_LENGTH;
pub const BASE_TIMESTAMP_LENGTH: usize = 8;
pub const MAX_TIMESTAMP_OFFSET: usize = BASE_TIMESTAMP_OFFSET + BASE_TIMESTAMP_LENGTH;
pub const MAX_TIMESTAMP_LENGTH: usize = 8;
pub const PRODUCER_ID_OFFSET: usize = MAX_TIMESTAMP_OFFSET + MAX_TIMESTAMP_LENGTH;
pub const PRODUCER_ID_LENGTH: usize = 8;
pub const PRODUCER_EPOCH_OFFSET: usize = PRODUCER_ID_OFFSET + PRODUCER_ID_LENGTH;
pub const PRODUCER_EPOCH_LENGTH: usize = 2;
pub const BASE_SEQUENCE_OFFSET: usize = PRODUCER_EPOCH_OFFSET + PRODUCER_EPOCH_LENGTH;
pub const BASE_SEQUENCE_LENGTH: usize = 4;
pub const RECORDS_COUNT_OFFSET: usize = BASE_SEQUENCE_OFFSET + BASE_SEQUENCE_LENGTH;
pub const RECORDS_COUNT_LENGTH: usize = 4;
pub const RECORDS_OFFSET: usize = RECORDS_COUNT_OFFSET + RECORDS_COUNT_LENGTH;
pub const RECORD_BATCH_OVERHEAD: usize = RECORDS_OFFSET;

pub const HEADER_SIZE_UP_TO_MAGIC: usize = MAGIC_OFFSET + MAGIC_LENGTH;
pub const LOG_OVERHEAD: usize = LENGTH_OFFSET + LENGTH_LENGTH;
