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

use std::io::Cursor;

use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;

use crate::codec::Decodable;
use crate::codec::Encodable;
use crate::codec::Encoder;
use crate::codec::Int32;
use crate::schemata::apikey::ApiMessageType;
use crate::schemata::request_header::RequestHeader;
use crate::schemata::response_header::ResponseHeader;
use crate::IoResult;

pub mod apikey;
#[allow(dead_code)]
pub mod error;

pub mod api_versions_request;
pub mod api_versions_response;
pub mod create_topic_request;
pub mod create_topic_response;
pub mod fetch_request;
pub mod fetch_response;
pub mod find_coordinator_request;
pub mod find_coordinator_response;
pub mod heartbeat_request;
pub mod heartbeat_response;
pub mod init_producer_id_request;
pub mod init_producer_id_response;
pub mod join_group_request;
pub mod join_group_response;
pub mod metadata_request;
pub mod metadata_response;
pub mod offset_fetch_request;
pub mod offset_fetch_response;
pub mod produce_request;
pub mod produce_response;
pub mod request_header;
pub mod response_header;
pub mod sync_group_request;
pub mod sync_group_response;

#[derive(Debug)]
pub enum Request {
    ApiVersionsRequest(api_versions_request::ApiVersionsRequest),
    CreateTopicRequest(create_topic_request::CreateTopicsRequest),
    FetchRequest(fetch_request::FetchRequest),
    FindCoordinatorRequest(find_coordinator_request::FindCoordinatorRequest),
    HeartbeatRequest(heartbeat_request::HeartbeatRequest),
    InitProducerIdRequest(init_producer_id_request::InitProducerIdRequest),
    JoinGroupRequest(join_group_request::JoinGroupRequest),
    MetadataRequest(metadata_request::MetadataRequest),
    OffsetFetchRequest(offset_fetch_request::OffsetFetchRequest),
    ProduceRequest(produce_request::ProduceRequest),
    SyncGroupRequest(sync_group_request::SyncGroupRequest),
}

impl Request {
    pub fn decode<T: AsRef<[u8]>>(buf: &mut Cursor<T>) -> IoResult<(RequestHeader, Request)> {
        let header_version = {
            let pos = buf.position();
            let api_key = buf.read_i16::<BigEndian>()?;
            let api_version = buf.read_i16::<BigEndian>()?;
            buf.set_position(pos);
            ApiMessageType::try_from(api_key)?.request_header_version(api_version)
        };

        let header = RequestHeader::read(buf, header_version)?;
        let api_type = ApiMessageType::try_from(header.request_api_key)?;
        let api_version = header.request_api_version;

        let request = match api_type {
            ApiMessageType::API_VERSIONS => {
                api_versions_request::ApiVersionsRequest::read(buf, api_version)
                    .map(Request::ApiVersionsRequest)
            }
            ApiMessageType::CREATE_TOPICS => {
                create_topic_request::CreateTopicsRequest::read(buf, api_version)
                    .map(Request::CreateTopicRequest)
            }
            ApiMessageType::FETCH => {
                fetch_request::FetchRequest::read(buf, api_version).map(Request::FetchRequest)
            }
            ApiMessageType::FIND_COORDINATOR => {
                find_coordinator_request::FindCoordinatorRequest::read(buf, api_version)
                    .map(Request::FindCoordinatorRequest)
            }
            ApiMessageType::INIT_PRODUCER_ID => {
                init_producer_id_request::InitProducerIdRequest::read(buf, api_version)
                    .map(Request::InitProducerIdRequest)
            }
            ApiMessageType::JOIN_GROUP => {
                join_group_request::JoinGroupRequest::read(buf, api_version)
                    .map(Request::JoinGroupRequest)
            }
            ApiMessageType::HEARTBEAT => {
                heartbeat_request::HeartbeatRequest::read(buf, api_version)
                    .map(Request::HeartbeatRequest)
            }
            ApiMessageType::METADATA => metadata_request::MetadataRequest::read(buf, api_version)
                .map(Request::MetadataRequest),
            ApiMessageType::OFFSET_FETCH => {
                offset_fetch_request::OffsetFetchRequest::read(buf, api_version)
                    .map(Request::OffsetFetchRequest)
            }
            ApiMessageType::PRODUCE => {
                produce_request::ProduceRequest::read(buf, api_version).map(Request::ProduceRequest)
            }
            ApiMessageType::SYNC_GROUP => {
                sync_group_request::SyncGroupRequest::read(buf, api_version)
                    .map(Request::SyncGroupRequest)
            }
            api_type => unreachable!("unknown api type {}", api_type),
        }?;

        Ok((header, request))
    }
}

#[derive(Debug)]
pub enum Response {
    ApiVersionsResponse(api_versions_response::ApiVersionsResponse),
    CreateTopicsResponse(create_topic_response::CreateTopicsResponse),
    FindCoordinatorResponse(find_coordinator_response::FindCoordinatorResponse),
    FetchResponse(fetch_response::FetchResponse),
    HeartbeatResponse(heartbeat_response::HeartbeatResponse),
    InitProducerIdResponse(init_producer_id_response::InitProducerIdResponse),
    JoinGroupResponse(join_group_response::JoinGroupResponse),
    MetadataResponse(metadata_response::MetadataResponse),
    OffsetFetchResponse(offset_fetch_response::OffsetFetchResponse),
    ProduceResponse(produce_response::ProduceResponse),
    SyncGroupResponse(sync_group_response::SyncGroupResponse),
}

impl Response {
    pub fn encode<B: WriteBytesExt>(&self, header: RequestHeader, buf: &mut B) -> IoResult<()> {
        let api_type = ApiMessageType::try_from(header.request_api_key)?;
        let api_version = header.request_api_version;
        let correlation_id = header.correlation_id;

        let response_header_version = api_type.response_header_version(api_version);
        let response_header = ResponseHeader {
            correlation_id,
            unknown_tagged_fields: vec![],
        };

        // 1. total size
        let size = self.calculate_size(api_version)
            + response_header.calculate_size(response_header_version);
        Int32.encode(buf, size as i32)?;

        // 2. response header
        response_header.write(buf, response_header_version)?;

        // 3. response body
        self.do_encode(buf, api_version)
    }

    fn calculate_size(&self, version: i16) -> usize {
        match self {
            Response::ApiVersionsResponse(resp) => resp.calculate_size(version),
            Response::CreateTopicsResponse(resp) => resp.calculate_size(version),
            Response::FindCoordinatorResponse(resp) => resp.calculate_size(version),
            Response::FetchResponse(resp) => resp.calculate_size(version),
            Response::HeartbeatResponse(resp) => resp.calculate_size(version),
            Response::InitProducerIdResponse(resp) => resp.calculate_size(version),
            Response::JoinGroupResponse(resp) => resp.calculate_size(version),
            Response::MetadataResponse(resp) => resp.calculate_size(version),
            Response::OffsetFetchResponse(resp) => resp.calculate_size(version),
            Response::ProduceResponse(resp) => resp.calculate_size(version),
            Response::SyncGroupResponse(resp) => resp.calculate_size(version),
        }
    }

    fn do_encode<B: WriteBytesExt>(&self, buf: &mut B, version: i16) -> IoResult<()> {
        match self {
            Response::ApiVersionsResponse(resp) => resp.write(buf, version),
            Response::CreateTopicsResponse(resp) => resp.write(buf, version),
            Response::FindCoordinatorResponse(resp) => resp.write(buf, version),
            Response::FetchResponse(resp) => resp.write(buf, version),
            Response::HeartbeatResponse(resp) => resp.write(buf, version),
            Response::InitProducerIdResponse(resp) => resp.write(buf, version),
            Response::JoinGroupResponse(resp) => resp.write(buf, version),
            Response::MetadataResponse(resp) => resp.write(buf, version),
            Response::OffsetFetchResponse(resp) => resp.write(buf, version),
            Response::ProduceResponse(resp) => resp.write(buf, version),
            Response::SyncGroupResponse(resp) => resp.write(buf, version),
        }
    }
}
