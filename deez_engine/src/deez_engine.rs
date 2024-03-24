use std::{
    collections::HashSet,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::{Builder, JoinHandle},
    time::{Duration, Instant, SystemTime},
    io::{self, Write}
};

use cached::{Cached, TimedCache};
use dashmap::DashMap;
use jito_core::ofac::is_tx_ofac_related;
use jito_protos::{
    auth::{
        auth_service_client::AuthServiceClient, GenerateAuthChallengeRequest,
        GenerateAuthTokensRequest, GenerateAuthTokensResponse, RefreshAccessTokenRequest, Role,
        Token,
    },
    block_engine::{
        block_engine_relayer_client::BlockEngineRelayerClient, packet_batch_update::Msg,
        AccountsOfInterestRequest, AccountsOfInterestUpdate, ExpiringPacketBatch,
        PacketBatchUpdate, ProgramsOfInterestRequest, ProgramsOfInterestUpdate,
    },
    convert::packet_to_proto_packet,
    packet::PacketBatch as ProtoPacketBatch,
    shared::{Header, Heartbeat},
};
use log::{error, *};
use prost_types::Timestamp;
use solana_core::banking_trace::BankingPacketBatch;
use solana_metrics::{datapoint_error, datapoint_info};
use solana_sdk::{
    address_lookup_table::AddressLookupTableAccount, pubkey::Pubkey, signature::Signer,
    signer::keypair::Keypair, transaction::VersionedTransaction,
};
use thiserror::Error;
use tokio::{
    runtime::Runtime,
    select,
    sync::mpsc::{channel, Receiver, Sender},
    time::{interval, sleep},
    net::TcpStream,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    codegen::InterceptedService,
    service::Interceptor,
    transport::{Channel, Endpoint},
    Response, Status, Streaming,
};

pub struct DeezEnginePackets {
    pub banking_packet_batch: BankingPacketBatch,
}

#[derive(Error, Debug)]
pub enum DeezEngineError {
    #[error("auth service failed: {0}")]
    AuthServiceFailure(String),

    #[error("deez engine failed: {0}")]
    DeezEngineFailure(String),
}

pub type DeezEngineResult<T> = Result<T, DeezEngineError>;


pub struct DeezEngineRelayerHandler {
    deez_engine_forwarder: JoinHandle<()>,
}

impl DeezEngineRelayerHandler {
    pub fn new(
        mut deez_engine_receiver: Receiver<DeezEnginePackets>,
        deez_engine_url: String,
    ) -> DeezEngineRelayerHandler {
        let deez_engine_forwarder = Builder::new()
            .name("deez_engine_relayer_handler_thread".into())
            .spawn(move || {
                let rt = Runtime::new().unwrap();
                rt.block_on(async move {
                    let result = Self::connect(
                        &mut deez_engine_receiver,
                        deez_engine_url,
                    ).await;
                })
            })
            .unwrap();

        DeezEngineRelayerHandler{
            deez_engine_forwarder,
        }
    }

    async fn connect(
        deez_engine_receiver: &mut Receiver<DeezEnginePackets>,
        deez_engine_url: String,
    ) -> io::Result<()> {
        let mut stream = TcpStream::connect(deez_engine_url).await?;

        Self::start_event_loop(deez_engine_receiver, deez_engine_url).await
    }

    async fn start_event_loop(
        deez_engine_receiver: &mut Receiver<DeezEnginePackets>,
        deez_stream: TcpStream,
    ) -> io::Result<()> {
        loop {
            let deez_engine_batches = deez_engine_receiver.recv().await.ok_or_else(|| 
                io::Error::new(io::ErrorKind::Other, "deez engine packet receiver disconnected")
            )?;

            trace!("received deez engine batches");

            for packet_batch in deez_engine_batches.banking_packet_batch.0.iter() {
                for packet in packet_batch.iter() {
                    if packet.meta().discard() || packet.meta().is_simple_vote_tx() {
                        continue;
                    }

                    if let Ok(tx) = packet.deserialize_slice::<VersionedTransaction, _>(..) {
                        let tx_data = match bincode::serialize(&tx) {
                            Ok(data) => data,
                            Err(_) => continue, // Handle serialization error or log it as needed
                        };

                        stream.write_all(&tx_data).await?;
                    }
                }
            }
        }
    
        Ok(())
    }

    pub fn join(self) {
        self.deez_engine_forwarder.join().unwrap();
    }

}