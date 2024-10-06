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

use byteorder::WriteBytesExt;

use crate::codec::*;
use crate::IoResult;

// Version 1 adds throttle time to the response.
//
// Starting in version 2, on quota violation, brokers send out responses before throttling.
//
// Version 3 is the first flexible version. Tagged fields are only supported in the body but
// not in the header. The length of the header must not change in order to guarantee the
// backward compatibility.
//
// Starting from Apache Kafka 2.4 (KIP-511), ApiKeys field is populated with the supported
// versions of the ApiVersionsRequest when an UNSUPPORTED_VERSION error is returned.

#[derive(Debug, Default, Clone)]
pub struct ApiVersionsResponse {
    /// The top-level error code.
    pub error_code: i16,
    /// The APIs supported by the broker.
    pub api_keys: Vec<ApiVersion>,
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// Features supported by the broker.
    pub supported_features: Vec<SupportedFeatureKey>,
    /// The monotonically increasing epoch for the finalized features information. Valid values are
    /// >= 0. A value of -1 is special and represents unknown epoch.
    pub finalized_features_epoch: i64,
    /// List of cluster-wide finalized features. The information is valid only if
    /// FinalizedFeaturesEpoch >= 0.
    pub finalized_features: Vec<FinalizedFeatureKey>,
    /// Set by a KRaft controller if the required configurations for ZK migration are present.
    pub zk_migration_ready: bool,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for ApiVersionsResponse {
    fn write<B: WriteBytesExt>(&self, buf: &mut B, version: i16) -> IoResult<()> {
        Int16.encode(buf, self.error_code)?;
        NullableArray(Struct(version), version >= 3).encode(buf, self.api_keys.as_slice())?;
        if version >= 1 {
            Int32.encode(buf, self.throttle_time_ms)?;
        }
        if version >= 3 {
            RawTaggedFieldList.encode_with(buf, 3, &self.unknown_tagged_fields, |buf| {
                RawTaggedFieldWriter.write_field(
                    buf,
                    0,
                    NullableArray(Struct(version), version >= 3),
                    self.supported_features.as_slice(),
                )?;
                RawTaggedFieldWriter.write_field(buf, 1, Int64, self.finalized_features_epoch)?;
                RawTaggedFieldWriter.write_field(
                    buf,
                    2,
                    NullableArray(Struct(version), version >= 3),
                    self.finalized_features.as_slice(),
                )?;
                Ok(())
            })?;
        }
        Ok(())
    }

    fn calculate_size(&self, version: i16) -> usize {
        let mut res = 0;
        res += Int16::SIZE; //self.error_code
        res +=
            NullableArray(Struct(version), version >= 3).calculate_size(self.api_keys.as_slice());
        if version >= 1 {
            res += Int32::SIZE; // self.throttle_time_ms
        }
        if version >= 3 {
            res += RawTaggedFieldList.calculate_size_with(
                3,
                RawTaggedFieldWriter.calculate_field_size(
                    0,
                    NullableArray(Struct(version), version >= 3),
                    self.supported_features.as_slice(),
                ) + RawTaggedFieldWriter.calculate_field_size(
                    1,
                    Int64,
                    &self.finalized_features_epoch,
                ) + RawTaggedFieldWriter.calculate_field_size(
                    2,
                    NullableArray(Struct(version), version >= 3),
                    self.finalized_features.as_slice(),
                ),
                &self.unknown_tagged_fields,
            );
        }
        res
    }
}

#[derive(Debug, Default, Clone)]
pub struct ApiVersion {
    /// The API index.
    pub api_key: i16,
    /// The minimum supported version, inclusive.
    pub min_version: i16,
    /// The maximum supported version, inclusive.
    pub max_version: i16,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for ApiVersion {
    fn write<B: WriteBytesExt>(&self, buf: &mut B, version: i16) -> IoResult<()> {
        Int16.encode(buf, self.api_key)?;
        Int16.encode(buf, self.min_version)?;
        Int16.encode(buf, self.max_version)?;
        if version >= 3 {
            RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        }
        Ok(())
    }

    fn calculate_size(&self, version: i16) -> usize {
        let mut res = 0;
        res += Int16::SIZE; // self.api_key
        res += Int16::SIZE; // self.min_version
        res += Int16::SIZE; // self.max_version
        if version >= 3 {
            res += RawTaggedFieldList.calculate_size(&self.unknown_tagged_fields);
        }
        res
    }
}

#[derive(Debug, Default, Clone)]
pub struct SupportedFeatureKey {
    /// The name of the feature.
    pub name: String,
    /// The minimum supported version for the feature.
    pub min_version: i16,
    /// The maximum supported version for the feature.
    pub max_version: i16,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for SupportedFeatureKey {
    fn write<B: WriteBytesExt>(&self, buf: &mut B, version: i16) -> IoResult<()> {
        if version > 3 {
            Err(err_encode_message_unsupported(
                version,
                "SupportedFeatureKey",
            ))?
        }
        NullableString(true).encode(buf, self.name.as_ref())?;
        Int16.encode(buf, self.min_version)?;
        Int16.encode(buf, self.max_version)?;
        RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        Ok(())
    }

    fn calculate_size(&self, _version: i16) -> usize {
        let mut res = 0;
        res += NullableString(true).calculate_size(self.name.as_ref());
        res += Int16::SIZE; // self.min_version
        res += Int16::SIZE; // self.max_version
        res += RawTaggedFieldList.calculate_size(&self.unknown_tagged_fields);
        res
    }
}

#[derive(Debug, Default, Clone)]
pub struct FinalizedFeatureKey {
    /// The name of the feature.
    pub name: String,
    /// The cluster-wide finalized max version level for the feature.
    pub max_version_level: i16,
    /// The cluster-wide finalized min version level for the feature.
    pub min_version_level: i16,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for FinalizedFeatureKey {
    fn write<B: WriteBytesExt>(&self, buf: &mut B, version: i16) -> IoResult<()> {
        if version > 3 {
            Err(err_encode_message_unsupported(
                version,
                "FinalizedFeatureKey",
            ))?
        }
        NullableString(true).encode(buf, self.name.as_ref())?;
        Int16.encode(buf, self.max_version_level)?;
        Int16.encode(buf, self.min_version_level)?;
        RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        Ok(())
    }

    fn calculate_size(&self, _version: i16) -> usize {
        let mut res = 0;
        res += NullableString(true).calculate_size(self.name.as_ref());
        res += Int16::SIZE; // self.max_version_level
        res += Int16::SIZE; // self.min_version_level
        res += RawTaggedFieldList.calculate_size(&self.unknown_tagged_fields);
        res
    }
}
