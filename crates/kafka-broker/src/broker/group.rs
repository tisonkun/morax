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

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use kafka_api::schemata::error::ErrorCode;
use kafka_api::schemata::join_group_request::JoinGroupRequest;
use kafka_api::schemata::join_group_response::JoinGroupResponse;
use kafka_api::schemata::join_group_response::JoinGroupResponseMember;
use kafka_api::schemata::sync_group_request::SyncGroupRequest;
use kafka_api::schemata::sync_group_response::SyncGroupResponse;
use morax_meta::GroupMeta;
use morax_meta::MemberMeta;

use crate::broker::Broker;
use crate::broker::ClientInfo;

impl Broker {
    pub(super) async fn receive_join_group(
        &self,
        client_info: ClientInfo,
        request: JoinGroupRequest,
    ) -> JoinGroupResponse {
        let JoinGroupRequest {
            group_id,
            member_id,
            rebalance_timeout_ms,
            session_timeout_ms,
            protocol_type,
            protocols,
            ..
        } = request;

        let empty_member_id = member_id.is_empty();
        let member_id = if empty_member_id {
            let random_suffix = uuid::Uuid::new_v4().to_string();
            format!("{}-{}", client_info.client_id, random_suffix)
        } else {
            member_id
        };

        let member_id_clone = member_id.clone();
        let meta_result = self
            .meta
            .upsert_group_meta(group_id.clone(), empty_member_id, move |group_meta| {
                let group_meta = match group_meta {
                    None => {
                        log::error!("group not found: {group_id}");
                        return Err(JoinGroupResponse {
                            error_code: ErrorCode::INVALID_GROUP_ID.code(),
                            ..Default::default()
                        });
                    }
                    Some(group_meta) => group_meta,
                };

                let mut group_meta = GroupMetaBiz(group_meta);
                group_meta.add(MemberMeta {
                    group_id,
                    member_id: member_id_clone,
                    client_id: client_info.client_id.clone(),
                    client_host: client_info.client_host,
                    protocol_type,
                    protocols: protocols
                        .into_iter()
                        .map(|p| (p.name, p.metadata))
                        .collect(),
                    assignment: Default::default(),
                    rebalance_timeout_ms,
                    session_timeout_ms,
                });
                Ok(group_meta.0)
            })
            .await;

        let group_meta = match meta_result {
            Ok(Ok(group_meta)) => group_meta,
            Ok(Err(resp)) => {
                return resp;
            }
            Err(err) => {
                log::error!("failed to upsert group meta: {err:?}");
                return JoinGroupResponse {
                    error_code: ErrorCode::UNKNOWN_SERVER_ERROR.code(),
                    member_id,
                    ..Default::default()
                };
            }
        };

        let leader = match group_meta.leader_id {
            Some(ref leader) => leader.clone(),
            None => unreachable!("non-empty group must have a leader"),
        };

        let mut members = vec![];
        for (member_id, mut member_meta) in group_meta.members {
            let metadata = if let Some(ref protocol) = group_meta.protocol {
                member_meta.protocols.remove(protocol).unwrap_or_default()
            } else {
                Default::default()
            };

            members.push(JoinGroupResponseMember {
                member_id,
                metadata,
                ..Default::default()
            });
        }

        JoinGroupResponse {
            generation_id: group_meta.generation_id,
            protocol_type: group_meta.protocol_type,
            protocol_name: group_meta.protocol,
            leader,
            member_id,
            members,
            ..Default::default()
        }
    }

    pub(super) async fn receive_sync_group(&self, request: SyncGroupRequest) -> SyncGroupResponse {
        let SyncGroupRequest {
            group_id,
            member_id,
            protocol_type,
            protocol_name,
            assignments,
            ..
        } = request;

        let meta_result = self
            .meta
            .upsert_group_meta(group_id.clone(), member_id.is_empty(), move |group_meta| {
                let group_meta = match group_meta {
                    None => {
                        log::error!("group not found: {group_id}");
                        return Err(SyncGroupResponse {
                            error_code: ErrorCode::INVALID_GROUP_ID.code(),
                            ..Default::default()
                        });
                    }
                    Some(group_meta) => group_meta,
                };

                let mut group_meta = GroupMetaBiz(group_meta);
                group_meta.sync(
                    assignments
                        .into_iter()
                        .map(|assign| (assign.member_id, assign.assignment))
                        .collect::<BTreeMap<String, Vec<u8>>>(),
                );
                Ok(group_meta.0)
            })
            .await;

        let group_meta = match meta_result {
            Ok(Ok(group_meta)) => group_meta,
            Ok(Err(resp)) => {
                return resp;
            }
            Err(err) => {
                log::error!("failed to upsert group meta: {err:?}");
                return SyncGroupResponse {
                    error_code: ErrorCode::UNKNOWN_SERVER_ERROR.code(),
                    ..Default::default()
                };
            }
        };

        let assignment = match group_meta.members.get(&member_id) {
            Some(member) => member.assignment.clone(),
            None => {
                log::error!("failed to find member: {member_id}");
                return SyncGroupResponse {
                    error_code: ErrorCode::UNKNOWN_SERVER_ERROR.code(),
                    ..Default::default()
                };
            }
        };

        SyncGroupResponse {
            protocol_type,
            protocol_name,
            assignment,
            ..Default::default()
        }
    }
}

#[derive(Debug)]
struct GroupMetaBiz(GroupMeta);

impl GroupMetaBiz {
    fn add(&mut self, member: MemberMeta) {
        if self.0.members.is_empty() {
            self.0.protocol_type = Some(member.protocol_type.clone());
        }
        debug_assert_eq!(self.0.group_id, member.group_id);
        debug_assert_eq!(
            self.0.protocol_type.as_deref(),
            Some(member.protocol_type.as_str())
        );
        if self.0.leader_id.is_none() {
            self.0.leader_id = Some(member.member_id.clone());
        }
        self.0.members.insert(member.member_id.clone(), member);
        self.make_next_generation();
    }

    fn sync(&mut self, assignments: BTreeMap<String, Vec<u8>>) {
        for member in self.0.members.values_mut() {
            let assignment = assignments
                .get(&member.member_id)
                .cloned()
                .unwrap_or_default();
            member.assignment = assignment;
        }
    }

    fn make_next_generation(&mut self) {
        self.0.generation_id += 1;
        if self.0.members.is_empty() {
            self.0.protocol = None;
        } else {
            self.0.protocol = self.select_protocol();
        }
    }

    fn select_protocol(&self) -> Option<String> {
        fn vote(member_meta: &MemberMeta, candidates: &BTreeSet<String>) -> String {
            log::trace!(
                "{} supports protocols {:?}, vote among {:?}",
                member_meta.member_id,
                member_meta.protocols,
                candidates
            );

            candidates
                .iter()
                .find(|p| member_meta.protocols.contains_key(*p))
                // SAFETY: at least contains this member's own protocol
                .expect("member does not support any of the candidate protocols")
                .clone()
        }

        let candidates = self.candidate_protocols();
        let mut votes = BTreeMap::new();
        for member in self.0.members.values() {
            let vote_for = vote(member, &candidates);
            *(votes.entry(vote_for).or_insert(0)) += 1;
        }
        votes
            .iter()
            .map(|(k, v)| (v, k))
            .next()
            .map(|(_, selected)| selected)
            .cloned()
    }

    fn candidate_protocols(&self) -> BTreeSet<String> {
        self.0
            .members
            .values()
            .flat_map(|m| m.protocols.keys())
            .cloned()
            .collect()
    }
}
