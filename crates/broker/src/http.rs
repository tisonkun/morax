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

use morax_api::property::StorageProperty;
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
use morax_meta::PostgresMetaService;
use poem::http::StatusCode;
use poem::middleware::AddData;
use poem::middleware::Compression;
use poem::web::Data;
use poem::web::Json;
use poem::web::Path;
use poem::EndpointExt;
use poem::Route;

use crate::broker::Broker;

#[poem::handler]
pub async fn health_check() -> poem::Result<String> {
    Ok("OK".to_string())
}

#[poem::handler]
pub async fn create_topic(
    Data(broker): Data<&Broker>,
    Path(topic_name): Path<String>,
    Json(request): Json<CreateTopicRequest>,
) -> poem::Result<Json<CreateTopicResponse>> {
    let response = broker
        .create_topic(topic_name, request)
        .await
        .inspect_err(|err| log::error!(err:?; "failed to create topic"))
        .map_err(|err| poem::Error::new(err.into_error(), StatusCode::UNPROCESSABLE_ENTITY))?;
    Ok(Json(response))
}

#[poem::handler]
pub async fn publish(
    Data(broker): Data<&Broker>,
    Path(topic_name): Path<String>,
    Json(request): Json<PublishMessageRequest>,
) -> poem::Result<Json<PublishMessageResponse>> {
    let response = broker
        .publish(topic_name, request)
        .await
        .inspect_err(|err| log::error!(err:?; "failed to publish messages"))
        .map_err(|err| poem::Error::new(err.into_error(), StatusCode::UNPROCESSABLE_ENTITY))?;
    Ok(Json(response))
}

#[poem::handler]
pub async fn create_subscription(
    Data(broker): Data<&Broker>,
    Path(subscription_name): Path<String>,
    Json(request): Json<CreateSubscriptionRequest>,
) -> poem::Result<Json<CreateSubscriptionResponse>> {
    let response = broker
        .create_subscription(subscription_name, request)
        .await
        .inspect_err(|err| log::error!(err:?; "failed to create subscription"))
        .map_err(|err| poem::Error::new(err.into_error(), StatusCode::UNPROCESSABLE_ENTITY))?;
    Ok(Json(response))
}

#[poem::handler]
pub async fn pull(
    Data(broker): Data<&Broker>,
    Path(subscription_name): Path<String>,
    Json(request): Json<PullMessageRequest>,
) -> poem::Result<Json<PullMessageResponse>> {
    let response = broker
        .pull(subscription_name, request)
        .await
        .inspect_err(|err| log::error!(err:?; "failed to pull messages"))
        .map_err(|err| poem::Error::new(err.into_error(), StatusCode::UNPROCESSABLE_ENTITY))?;
    Ok(Json(response))
}

#[poem::handler]
pub async fn acknowledge(
    Data(broker): Data<&Broker>,
    Path(subscription_name): Path<String>,
    Json(request): Json<AcknowledgeRequest>,
) -> poem::Result<Json<AcknowledgeResponse>> {
    let response = broker
        .acknowledge(subscription_name, request)
        .await
        .inspect_err(|err| log::error!(err:?; "failed to acknowledge"))
        .map_err(|err| poem::Error::new(err.into_error(), StatusCode::UNPROCESSABLE_ENTITY))?;
    Ok(Json(response))
}

pub fn make_broker_router(
    meta: Arc<PostgresMetaService>,
    default_storage: StorageProperty,
) -> Route {
    let broker = Broker::new(meta, default_storage);

    let v1_route = Route::new()
        .at("/health", poem::get(health_check))
        .at("/topics/:topic_name", poem::post(create_topic))
        .at("/topics/:topic_name/publish", poem::post(publish))
        .at(
            "/subscriptions/:subscription_name",
            poem::post(create_subscription),
        )
        .at("/subscriptions/:subscription_name/pull", poem::post(pull))
        .at(
            "/subscriptions/:subscription_name/acknowledge",
            poem::post(acknowledge),
        )
        .with(Compression::new())
        .with(AddData::new(broker));

    Route::new().nest("v1", v1_route)
}
