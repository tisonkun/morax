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
use std::net::SocketAddr;
use std::sync::Arc;

use error_stack::bail;
use error_stack::ResultExt;
use kafka_api::schemata::apikey::ApiMessageType;
use kafka_api::schemata::Request;
use latches::task::Latch;
use morax_meta::PostgresMetaService;
use morax_protos::config::KafkaBrokerConfig;
use morax_runtime::wait_group::WaitGroup;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

use crate::broker::Broker;
use crate::broker::BrokerMeta;
use crate::broker::ClientInfo;
use crate::broker::ClusterMeta;
use crate::BrokerError;

pub async fn start_broker(
    meta_service: Arc<PostgresMetaService>,
    broker_listener: TcpListener,
    config: KafkaBrokerConfig,
    wg: WaitGroup,
    shutdown: Arc<Latch>,
) -> error_stack::Result<(), BrokerError> {
    let addr = broker_listener.local_addr().change_context_lazy(|| {
        BrokerError("failed to get local address of broker listener".to_string())
    })?;
    let cluster_meta = ClusterMeta {
        cluster_id: "Morax Kafka Broker".to_string(),
        controller_id: 1,
        broker: BrokerMeta {
            node_id: 1,
            host: addr.ip().to_string(),
            port: addr.port() as i32,
        },
    };
    let broker = Arc::new(Broker::new(
        meta_service,
        config.fallback_storage,
        cluster_meta,
    ));

    log::info!("Starting Kafka Broker at {addr}");
    drop(wg);

    loop {
        let socket = tokio::select! {
            _ = shutdown.wait() => {
                log::info!("Morax Server is closing");
                return Ok(());
            }
            socket = broker_listener.accept() => socket,
        };

        let (socket, remote_addr) = socket
            .change_context_lazy(|| BrokerError("failed to accept new connections".to_string()))?;
        let shutdown_ref = shutdown.clone();
        let broker_ref = broker.clone();
        let _forget = morax_runtime::api_runtime().spawn(async move {
            if let Err(err) = process_packet(socket, remote_addr, broker_ref, shutdown_ref).await {
                log::error!("failed to process packet: {err:?}");
            }
        });
    }
}

async fn process_packet(
    mut socket: TcpStream,
    remote_addr: SocketAddr,
    broker: Arc<Broker>,
    shutdown: Arc<Latch>,
) -> error_stack::Result<(), BrokerError> {
    loop {
        tokio::select! {
             _ = shutdown.wait() => {
                log::info!("Morax Server is closing");
                return Ok(());
            }
            closed = process_packet_one(&mut socket, &remote_addr, &broker) => {
                if closed? {
                    return Ok(());
                }
            }
        }
    }
}

// Process one packet from the client. Return true if the connection is closed.
async fn process_packet_one(
    socket: &mut TcpStream,
    client_host: &SocketAddr,
    broker: &Broker,
) -> error_stack::Result<bool, BrokerError> {
    let n = {
        let mut buf = [0; size_of::<i32>()];
        if let Err(err) = socket.read_exact(&mut buf).await {
            match err.kind() {
                std::io::ErrorKind::UnexpectedEof => {
                    log::info!("connection closed by client");
                    return Ok(true);
                }
                _ => bail!(BrokerError(format!("failed to read packet length: {err}"))),
            }
        }
        i32::from_be_bytes(buf) as usize
    };

    let mut bytes = {
        let mut bytes = vec![0u8; n];
        socket
            .read_exact(&mut bytes)
            .await
            .change_context_lazy(|| BrokerError("failed to read packet bytes".to_string()))?;
        Cursor::new(bytes)
    };

    let (header, request) = Request::decode(&mut bytes)
        .change_context_lazy(|| BrokerError("failed to decode Kafka request".to_string()))?;

    // SAFETY: verified above
    let api_type =
        ApiMessageType::try_from(header.request_api_key).expect("must be a valid api key");
    log::debug!(
        "Receive header {} (version: {}) with correlation_id {} and client_id {}",
        api_type,
        header.request_api_version,
        header.correlation_id,
        header.client_id
    );
    log::debug!("Receive request {request:?}");

    let client_info = ClientInfo {
        client_id: header.client_id.clone(),
        client_host: client_host.to_string(),
    };
    let response = broker.reply(client_info, header.clone(), request).await;

    log::debug!("Send response {response:?}");
    let mut bytes = vec![];
    response
        .encode(header, &mut bytes)
        .change_context_lazy(|| BrokerError("failed to encode Kafka response".to_string()))?;
    socket
        .write_all(&bytes)
        .await
        .change_context_lazy(|| BrokerError("failed to send response bytes".to_string()))?;
    Ok(false)
}
