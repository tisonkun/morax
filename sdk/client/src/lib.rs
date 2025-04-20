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

use error_stack::ResultExt;
use morax_api::request::AcknowledgeRequest;
use morax_api::request::AcknowledgeResponse;
use morax_api::request::CreateSubscriptionRequest;
use morax_api::request::CreateSubscriptionResponse;
use morax_api::request::CreateTopicRequest;
use morax_api::request::CreateTopicResponse;
use morax_api::request::PublishMessageRequest;
use morax_api::request::PublishMessageResponse;
use morax_api::request::PullMessageRequest;
use morax_api::request::PullMessageResponse;
use reqwest::Client;
use reqwest::ClientBuilder;
use reqwest::Response;
use reqwest::StatusCode;
use serde::de::DeserializeOwned;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ClientError(String);

#[derive(Debug, Clone)]
pub enum HTTPResponse<T> {
    Success(T),
    Error(ErrorStatus),
}

impl<T> HTTPResponse<T> {
    pub fn into_success(self) -> Option<T> {
        match self {
            HTTPResponse::Success(t) => Some(t),
            HTTPResponse::Error(_) => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ErrorStatus {
    code: StatusCode,
    payload: Vec<u8>,
}

impl std::fmt::Display for ErrorStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{:?} ({}): {}",
            self.code.canonical_reason(),
            self.code.as_u16(),
            if self.payload.is_empty() {
                "(no payload)".into()
            } else {
                String::from_utf8_lossy(&self.payload)
            }
        )
    }
}

#[derive(Debug)]
pub struct HTTPClient {
    endpoint: String,
    client: Client,
}

impl HTTPClient {
    pub fn new(
        endpoint: impl Into<String>,
        builder: ClientBuilder,
    ) -> error_stack::Result<Self, ClientError> {
        let endpoint = endpoint.into();
        let make_error = || ClientError(format!("failed to create client: {endpoint:?}"));

        Ok(Self {
            endpoint: endpoint.clone(),
            client: builder.build().change_context_lazy(make_error)?,
        })
    }

    pub async fn create_topic(
        &self,
        topic_name: String,
        request: CreateTopicRequest,
    ) -> error_stack::Result<HTTPResponse<CreateTopicResponse>, ClientError> {
        let make_error = || ClientError(format!("failed to create topic: {request:?}"));

        let response = self
            .client
            .post(format!("{}/v1/topics/{topic_name}", self.endpoint))
            .json(&request)
            .send()
            .await
            .change_context_lazy(make_error)?;

        make_response(response).await
    }

    pub async fn publish(
        &self,
        topic_name: String,
        request: PublishMessageRequest,
    ) -> error_stack::Result<HTTPResponse<PublishMessageResponse>, ClientError> {
        let make_error = || ClientError(format!("failed to publish messages: {request:?}"));

        let response = self
            .client
            .post(format!("{}/v1/topics/{topic_name}/publish", self.endpoint))
            .json(&request)
            .send()
            .await
            .change_context_lazy(make_error)?;

        make_response(response).await
    }

    pub async fn create_subscription(
        &self,
        subscription_name: String,
        request: CreateSubscriptionRequest,
    ) -> error_stack::Result<HTTPResponse<CreateSubscriptionResponse>, ClientError> {
        let make_error = || ClientError(format!("failed to create subscription: {request:?}"));

        let response = self
            .client
            .post(format!(
                "{}/v1/subscriptions/{subscription_name}",
                self.endpoint
            ))
            .json(&request)
            .send()
            .await
            .change_context_lazy(make_error)?;

        make_response(response).await
    }

    pub async fn pull(
        &self,
        subscription_name: String,
        request: PullMessageRequest,
    ) -> error_stack::Result<HTTPResponse<PullMessageResponse>, ClientError> {
        let make_error = || ClientError(format!("failed to pull messages: {request:?}"));

        let response = self
            .client
            .post(format!(
                "{}/v1/subscriptions/{subscription_name}/pull",
                self.endpoint
            ))
            .json(&request)
            .send()
            .await
            .change_context_lazy(make_error)?;

        make_response(response).await
    }

    pub async fn acknowledge(
        &self,
        subscription_name: String,
        request: AcknowledgeRequest,
    ) -> error_stack::Result<HTTPResponse<AcknowledgeResponse>, ClientError> {
        let make_error = || ClientError(format!("failed to acknowledge: {request:?}"));

        let response = self
            .client
            .post(format!(
                "{}/v1/subscriptions/{subscription_name}/acknowledge",
                self.endpoint
            ))
            .json(&request)
            .send()
            .await
            .change_context_lazy(make_error)?;

        make_response(response).await
    }
}

async fn make_response<T: DeserializeOwned>(
    r: Response,
) -> error_stack::Result<HTTPResponse<T>, ClientError> {
    let make_error = || ClientError("failed to make response".to_string());

    let status = r.status();

    if status.is_success() {
        let result = r.json().await.change_context_lazy(make_error)?;
        return Ok(HTTPResponse::Success(result));
    }

    let payload = r.bytes().await.change_context_lazy(make_error)?;
    Ok(HTTPResponse::Error(ErrorStatus {
        code: status,
        payload: payload.to_vec(),
    }))
}
