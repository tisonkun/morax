use std::net::SocketAddr;
use std::sync::Arc;

use error_stack::Result;
use error_stack::ResultExt;
use mea::latch::Latch;
use mea::waitgroup::WaitGroup;
use morax_meta::PostgresMetaService;
use morax_protos::config::KafkaBrokerConfig;

use crate::server::resolve_advertise_addr;
use crate::server::ServerFuture;
use crate::ServerError;

#[derive(Debug)]
pub(crate) struct KafkaBootstrapContext {
    pub(crate) config: KafkaBrokerConfig,
    pub(crate) meta_service: Arc<PostgresMetaService>,
    pub(crate) wg: WaitGroup,
    pub(crate) shutdown: Arc<Latch>,
}

pub(crate) async fn bootstrap_kafka_broker(
    context: KafkaBootstrapContext,
) -> Result<(SocketAddr, ServerFuture<()>), ServerError> {
    let KafkaBootstrapContext {
        config,
        meta_service,
        wg,
        shutdown,
    } = context;

    let broker_addr = config.listen_addr.as_str();
    let broker_listener = tokio::net::TcpListener::bind(broker_addr)
        .await
        .change_context_lazy(|| {
            ServerError(format!("failed to listen to kafka broker: {broker_addr}"))
        })?;
    let broker_listen_addr = broker_listener.local_addr().change_context_lazy(|| {
        ServerError("failed to get local address of kafka broker".to_string())
    })?;
    let broker_advertise_addr =
        resolve_advertise_addr(broker_listen_addr, config.advertise_addr.as_deref())?;

    let broker_fut = morax_runtime::api_runtime().spawn(async move {
        morax_kafka_broker::start_broker(meta_service, broker_listener, config, wg, shutdown)
            .await
            .change_context_lazy(|| ServerError("failed to run the Kafka broker".to_string()))
    });

    Ok((broker_advertise_addr, broker_fut))
}
