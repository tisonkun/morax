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

use std::sync::Arc;

use kafka_api::schemata::request_header::RequestHeader;
use kafka_api::schemata::Request;
use kafka_api::schemata::Response;
use morax_meta::PostgresMetaService;
use morax_protos::property::StorageProps;

mod admin;
mod fetch;
mod group;
mod produce;

#[derive(Debug, Clone)]
pub struct ClientInfo {
    pub client_id: String,
    pub client_host: String,
}

#[derive(Debug, Clone)]
pub struct ClusterMeta {
    pub cluster_id: String,
    pub controller_id: i32,
    // always return this broker
    pub broker: BrokerMeta,
}

#[derive(Debug, Clone)]
pub struct BrokerMeta {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Debug)]
pub struct Broker {
    meta: Arc<PostgresMetaService>,
    fallback_storage: StorageProps,
    cluster_meta: ClusterMeta,
}

impl Broker {
    pub fn new(
        meta_service: Arc<PostgresMetaService>,
        fallback_storage: StorageProps,
        cluster_meta: ClusterMeta,
    ) -> Self {
        Broker {
            meta: meta_service,
            fallback_storage,
            cluster_meta,
        }
    }
}

impl Broker {
    pub async fn reply(
        &self,
        client_info: ClientInfo,
        header: RequestHeader,
        request: Request,
    ) -> Response {
        match request {
            Request::ApiVersionsRequest(request) => {
                Response::ApiVersionsResponse(self.receive_api_versions(request))
            }
            Request::CreateTopicRequest(request) => {
                Response::CreateTopicsResponse(self.receive_create_topic(request).await)
            }
            Request::FetchRequest(request) => {
                Response::FetchResponse(self.receive_fetch(request).await)
            }
            Request::FindCoordinatorRequest(request) => {
                Response::FindCoordinatorResponse(self.receive_find_coordinator(request))
            }
            Request::InitProducerIdRequest(request) => {
                Response::InitProducerIdResponse(self.receive_init_producer_id(request).await)
            }
            Request::JoinGroupRequest(request) => {
                Response::JoinGroupResponse(self.receive_join_group(client_info, request).await)
            }
            Request::MetadataRequest(request) => {
                Response::MetadataResponse(self.receive_metadata(request).await)
            }
            Request::OffsetFetchRequest(request) => {
                Response::OffsetFetchResponse(self.receive_offset_fetch(header, request).await)
            }
            Request::ProduceRequest(request) => {
                Response::ProduceResponse(self.receive_produce(request).await)
            }
            Request::SyncGroupRequest(request) => {
                Response::SyncGroupResponse(self.receive_sync_group(request).await)
            }
            Request::HeartbeatRequest(request) => {
                Response::HeartbeatResponse(self.receive_heartbeat(request))
            }
        }
    }
}
