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

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use error_stack::Result;
use error_stack::ResultExt;
use mea::latch::Latch;
use mea::waitgroup::WaitGroup;
use morax_meta::PostgresMetaService;
use morax_protos::config::BrokerConfig;
use poem::listener::Acceptor;
use poem::listener::Listener;

use crate::server::resolve_advertise_addr;
use crate::server::ServerFuture;
use crate::ServerError;

#[derive(Debug)]
pub(crate) struct BrokerBootstrapContext {
    pub(crate) config: BrokerConfig,
    pub(crate) meta_service: Arc<PostgresMetaService>,
    pub(crate) wg: WaitGroup,
    pub(crate) shutdown: Arc<Latch>,
}

pub(crate) async fn bootstrap_broker(
    context: BrokerBootstrapContext,
) -> Result<(SocketAddr, ServerFuture<()>), ServerError> {
    let BrokerBootstrapContext {
        config,
        meta_service,
        wg,
        shutdown,
    } = context;

    let broker_addr = config.listen_addr.as_str();
    let broker_acceptor = poem::listener::TcpListener::bind(broker_addr)
        .into_acceptor()
        .await
        .change_context_lazy(|| {
            ServerError(format!("failed to listen to broker: {broker_addr}"))
        })?;
    let broker_listen_addr = broker_acceptor.local_addr()[0]
        .as_socket_addr()
        .cloned()
        .ok_or_else(|| ServerError("failed to get local address of broker".to_string()))?;
    let broker_advertise_addr =
        resolve_advertise_addr(broker_listen_addr, config.advertise_addr.as_deref())?;

    let broker_fut = {
        let shutdown_clone = shutdown;
        let wg_clone = wg;

        let route = morax_broker::make_api_router(meta_service);
        let signal = async move {
            log::info!("Broker has started on [{broker_listen_addr}]");
            drop(wg_clone);

            shutdown_clone.wait().await;
            log::info!("Broker is closing");
        };

        morax_runtime::server_runtime().spawn(async move {
            poem::Server::new_with_acceptor(broker_acceptor)
                .run_with_graceful_shutdown(route, signal, Some(Duration::from_secs(30)))
                .await
                .change_context_lazy(|| ServerError("failed to run the broker".to_string()))
        })
    };

    Ok((broker_advertise_addr, broker_fut))
}
