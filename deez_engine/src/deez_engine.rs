use std::{
    io,
    sync::Arc,
    thread::{Builder, JoinHandle},
    time::{Duration, Instant},
};
use dashmap::DashSet;
use jito_block_engine::block_engine::BlockEnginePackets;
use jito_core::tx_cache::should_forward_tx;
use log::*;
use solana_sdk::transaction::VersionedTransaction;
use thiserror::Error;
use tokio::{io::AsyncWriteExt, net::TcpStream, runtime::Runtime, select, sync::{broadcast::Receiver, mpsc, Mutex}, task, time::{interval, sleep, timeout}};
use tokio::io::{AsyncReadExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use crate::heartbeat_sender::OnChainHeartbeatSender;

const HEARTBEAT_LEN: u16 = 4;
const PONG_MESSAGE: &[u8; 4] = b"pong";
const PONG_MSG_WITH_LENGTH: &[u8; 6] = &[
    (HEARTBEAT_LEN & 0xFF) as u8,
    ((HEARTBEAT_LEN >> 8) & 0xFF) as u8,
    PONG_MESSAGE[0],
    PONG_MESSAGE[1],
    PONG_MESSAGE[2],
    PONG_MESSAGE[3],
];

const HEARTBEAT_MSG: &[u8; 4] = b"ping";
const HEARTBEAT_MSG_WITH_LENGTH: &[u8; 6] = &[
    (HEARTBEAT_LEN & 0xFF) as u8,
    ((HEARTBEAT_LEN >> 8) & 0xFF) as u8,
    HEARTBEAT_MSG[0],
    HEARTBEAT_MSG[1],
    HEARTBEAT_MSG[2],
    HEARTBEAT_MSG[3],
];

const V2_LEN: u16 = 2;
const V2_MSG: &[u8; 2] = b"v2";
const V2_MSG_WITH_LENGTH: &[u8; 4] = &[
    (V2_LEN & 0xFF) as u8,
    ((V2_LEN >> 8) & 0xFF) as u8,
    V2_MSG[0],
    V2_MSG[1],
];

const V3_LEN: u16 = 2;
const V3_MSG: &[u8; 2] = b"v3";
const V3_MSG_WITH_LENGTH: &[u8; 4] = &[
    (V3_LEN & 0xFF) as u8,
    ((V3_LEN >> 8) & 0xFF) as u8,
    V3_MSG[0],
    V3_MSG[1],
];


const RPC_HEARTBEAT_MSG: [u8; 2] = [0, 0];
const STATS_MSG: [u8; 2] = [0, 1];

const STATS_EPOCH_CONNECTIVITY: u16 = 0;

const DEEZ_REGIONS: [&str; 4] = [
    "ny",
    "de",
    "slc",
    "tokyo"
];
const DEEZ_ENGINE_URL: &str = ".engine.v2.deez.wtf:8374";

#[derive(Error, Debug)]
pub enum DeezEngineError {
    #[error("deez engine failed: {0}")]
    Engine(String),

    #[error("deez tcp stream failure: {0}")]
    TcpStream(#[from] io::Error),

    #[error("deez tcp connection timed out")]
    TcpConnectionTimeout(#[from] tokio::time::error::Elapsed),

    #[error("cannot find closest engine")]
    CannotFindEngine(String),

    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
}

pub type DeezEngineResult<T> = Result<T, DeezEngineError>;

pub struct DeezEngineRelayerHandler {
    deez_engine_forwarder: JoinHandle<()>,
}

pub struct ParsedMessage {
    pub header: [u8; 2],
    pub body: Vec<u8>,
}

pub struct TcpReaderCodec {
    reader: BufReader<OwnedReadHalf>,
}

impl TcpReaderCodec {
    /// Encapsulate a TcpStream with reader functionality
    pub fn new(stream: OwnedReadHalf) -> io::Result<Self> {
        let reader = BufReader::new(stream);
        Ok(Self { reader })
    }

    async fn read_u16_from_bufreader(reader: &mut BufReader<OwnedReadHalf>) -> io::Result<u16> {
        let mut buf = [0; 2];
        reader.read_exact(&mut buf).await?;
        Ok(u16::from_le_bytes(buf))
    }

    async fn read_from_bufreader(reader: &mut BufReader<OwnedReadHalf>) -> io::Result<Vec<u8>> {
        let length = Self::read_u16_from_bufreader(reader).await?;
        let mut buffer = vec![0; length as usize];
        reader.read_exact(&mut buffer).await?;
        Ok(buffer)
    }

    pub async fn read_message(&mut self) -> io::Result<ParsedMessage> {
        let b = Self::read_from_bufreader(&mut self.reader).await?;
        if b.len() < 2 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Message too short"));
        }
        let header = [b[0], b[1]];
        let message = ParsedMessage {
            header,
            body: b[2..].to_vec(),
        };
        Ok(message)
    }
}

impl DeezEngineRelayerHandler {
    pub fn new(mut deez_engine_receiver: Receiver<BlockEnginePackets>, rpc_servers: Vec<String>, restart_interval: Duration,) -> DeezEngineRelayerHandler {
        let deez_engine_forwarder = Builder::new()
            .name("deez_engine_relayer_handler_thread".into())
            .spawn(move || {
                let rt = Runtime::new().unwrap();
                rt.block_on(async move {
                    loop {
                        let result = Self::connect_and_run(
                            &mut deez_engine_receiver,
                            rpc_servers.clone(),
                            restart_interval,
                        )
                            .await;

                        if let Err(e) = result {
                            match e {
                                DeezEngineError::Engine(_) => {
                                    deez_engine_receiver = deez_engine_receiver.resubscribe();
                                    error!("error with deez engine broadcast receiver, resubscribing to event stream: {:?}", e)
                                },
                                DeezEngineError::TcpStream(_) | DeezEngineError::TcpConnectionTimeout(_) => {
                                    error!("error with deez engine connection, attempting to re-establish connection: {:?}", e);
                                },
                                DeezEngineError::CannotFindEngine(_) => {
                                    error!("failed to find eligible mempool engine to connect to, retrying: {:?}", e);
                                },
                                DeezEngineError::Http(e) => {
                                    error!("failed to connect to mempool engine: {:?}, retrying", e);
                                }
                            }

                            sleep(Duration::from_secs(2)).await;
                        }

                        info!("Restarting DeezEngineRelayer");
                        deez_engine_receiver = deez_engine_receiver.resubscribe();
                    }
                })
            })
            .unwrap();

        DeezEngineRelayerHandler {
            deez_engine_forwarder,
        }
    }

    async fn connect_and_run(
        deez_engine_receiver: &mut Receiver<BlockEnginePackets>,
        rpc_servers: Vec<String>,
        restart_interval: Duration,
    ) -> DeezEngineResult<()> {
        let deez_engine_url = Self::find_closest_engine().await?;
        info!("determined closest engine in connect and run as {}", deez_engine_url);
        let engine_stream = Self::connect_to_engine(&deez_engine_url).await?;

        let retry_future = sleep(restart_interval);
        tokio::pin!(retry_future);

        select! {
            result = Self::start_event_loop(deez_engine_receiver, engine_stream, rpc_servers) => result,
            _ = &mut retry_future => Ok(()),
        }
    }

    async fn start_event_loop(
        deez_engine_receiver: &mut Receiver<BlockEnginePackets>,
        deez_engine_stream: TcpStream,
        rpc_servers: Vec<String>
    ) -> DeezEngineResult<()> {
        let (reader, writer) = deez_engine_stream.into_split();
        let mut line_reader = TcpReaderCodec::new(reader)?;
        let forwarder = Arc::new(Mutex::new(writer));
        let mut heartbeat_interval = interval(Duration::from_secs(5));
        let mut flush_interval = interval(Duration::from_secs(60));
        let tx_cache = Arc::new(DashSet::new());
        let (forward_error_sender, mut forward_error_receiver) = mpsc::unbounded_channel();
        let onchain_heartbeat_sender = Arc::new(Mutex::new(OnChainHeartbeatSender::new(rpc_servers)));

        // SEND V2 HEADER
        let _ = Self::forward_packets(forwarder.clone(), V2_MSG_WITH_LENGTH).await;

        // SEND V3 HEADER
        let _ = Self::forward_packets(forwarder.clone(), V3_MSG_WITH_LENGTH).await;

        let mut last_activity = Instant::now();
        let activity_timeout = Duration::from_secs(300);

        loop {
            let cloned_forwarder = forwarder.clone();
            let cloned_error_sender = forward_error_sender.clone();
            let cloned_tx_cache = tx_cache.clone();
            let heartbeat_sender = onchain_heartbeat_sender.clone();

            select! {
                recv_result = deez_engine_receiver.recv() => {
                    match recv_result {
                        Ok(deez_engine_batches) => {
                            trace!("received deez engine batches");
                            last_activity = Instant::now();
                            // Proceed with handling the batches as before
                            tokio::spawn(async move {
                                for packet_batch in deez_engine_batches.banking_packet_batch.0.iter() {
                                    for packet in packet_batch {
                                        if packet.meta().discard() || packet.meta().is_simple_vote_tx() {
                                            continue;
                                        }

                                        if let Ok(tx) = packet.deserialize_slice::<VersionedTransaction, _>(..) {
                                            let mut tx_data = match bincode::serialize(&tx) {
                                                Ok(data) => data,
                                                Err(_) => continue,
                                            };
                                            let tx_signature = tx.signatures[0].to_string();
                                            if !should_forward_tx(&cloned_tx_cache, &tx_signature) {
                                                continue;
                                            }

                                            let meta_bytes = match bincode::serialize(&packet.meta()) {
                                                Ok(data) => data,
                                                Err(_) => continue,
                                            };
                                            tx_data.reserve(meta_bytes.len());
                                            tx_data.splice(0..0, meta_bytes.clone());

                                            let length_bytes = (tx_data.len() as u16).to_le_bytes().to_vec();
                                            tx_data.reserve(2);
                                            tx_data.splice(0..0, length_bytes);

                                            if let Err(e) = Self::forward_packets(cloned_forwarder.clone(), tx_data.as_slice()).await {
                                                if let Err(send_err) = cloned_error_sender.send(e) {
                                                    error!("failed to transmit packet forward error to management channel: {send_err}");
                                                }
                                            } else {
                                                // if send successful, add signature to cache
                                                cloned_tx_cache.insert(tx_signature);
                                                trace!("successfully relayed packets to deez_engine");
                                            }
                                        }
                                    }
                                };
                            });

                        }
                        Err(e) => match e {
                            tokio::sync::broadcast::error::RecvError::Lagged(n) => {
                                warn!("Receiver lagged by {n} messages, continuing to receive future messages.");
                            }
                            tokio::sync::broadcast::error::RecvError::Closed => {
                                return Err(DeezEngineError::Engine("broadcast channel closed".to_string()));
                            }
                        },
                    }
                }
                _ = sleep(Duration::from_secs(1)) => {
                    if last_activity.elapsed() > activity_timeout {
                        warn!("No activity detected for {:?}, restarting flow", activity_timeout);
                        return Ok(());
                    }
                }
                on_chain_heartbeat = line_reader.read_message() => {
                    match on_chain_heartbeat {
                        Ok(message) => {
                            if message.header == RPC_HEARTBEAT_MSG {
                                 tokio::spawn(async move {
                                        let _ = heartbeat_sender.lock().await.broadcast(message.body).await;
                                    });
                            } else if message.header == STATS_MSG {
                                if message.body.len() >= 4 {
                                    let stats_type = u16::from_le_bytes([message.body[0], message.body[1]]);
                                    if stats_type == STATS_EPOCH_CONNECTIVITY {
                                        let epoch_connectivity_bps = u16::from_le_bytes([message.body[2], message.body[3]]);
                                        let epoch_connectivity_pct = (epoch_connectivity_bps as f32 / 10000.0) * 100.0;
                                        warn!("Current epoch connectivity: {:.2}%", epoch_connectivity_pct);
                                    } else {
                                        warn!("Unexpected format");
                                    }
                                } else {
                                    warn!("Too short");
                                }
                            }
                        },
                        _ => {}
                    }
                }
                forward_error = forward_error_receiver.recv() => {
                    match forward_error {
                        Some(e) => {
                            return Err(DeezEngineError::TcpStream(e))
                        },
                        None => continue,
                    }
                }
                _ = heartbeat_interval.tick() => {
                    info!("sending heartbeat (deez)");
                    Self::forward_packets(cloned_forwarder.clone(), HEARTBEAT_MSG_WITH_LENGTH).await?;
                }
                _ = flush_interval.tick() => {
                    info!("flushing signature cache");
                    tx_cache.clear();
                }
            }
        }
    }

    pub async fn find_closest_engine() -> DeezEngineResult<String> {
        let attempts = 5;
        let mut handles = vec![];

        for &region in DEEZ_REGIONS.iter() {
            let region = region.to_string();

            let handle = task::spawn(async move {
                let mut total_latency = Duration::ZERO;
                let mut success_count = 0;

                for _ in 0..attempts {
                    match TcpStream::connect(format!("{}{}", region, DEEZ_ENGINE_URL)).await {
                        Ok(mut stream) => {
                            let start = Instant::now();

                            let _ = stream.write_all(V2_MSG_WITH_LENGTH).await;

                            if stream.write_all(HEARTBEAT_MSG_WITH_LENGTH).await.is_ok() {
                                let mut buffer = vec![0; PONG_MSG_WITH_LENGTH.len()];
                                if stream.read_exact(&mut buffer).await.is_ok() {
                                    if buffer == PONG_MSG_WITH_LENGTH {
                                        let elapsed = start.elapsed();
                                        total_latency += elapsed;
                                        success_count += 1;
                                    }
                                }
                            }
                        }
                        Err(_e) => {
                            error!("error connecting to {}", region);
                        }
                    }
                }

                if success_count > 0 {
                    let average_latency = total_latency / success_count as u32;
                    Some((region, average_latency))
                } else {
                    None
                }
            });

            handles.push(handle);
        }

        let mut shortest_time = Duration::from_secs(u64::MAX);
        let mut closest_region  = DEEZ_REGIONS[1].to_string();

        for handle in handles {
            if let Ok(Some((region, average_latency))) = handle.await {
                if average_latency < shortest_time {
                    shortest_time = average_latency;
                    closest_region = region;
                }
            }
        }
        info!("determined closest region: {}", closest_region);
        Ok(format!("{}{}", closest_region, DEEZ_ENGINE_URL))
    }

    pub async fn connect_to_engine(engine_url: &str) -> DeezEngineResult<TcpStream> {
        let stream_future = TcpStream::connect(engine_url);

        let stream = timeout(Duration::from_secs(10), stream_future).await??;

        if let Err(e) = stream.set_nodelay(true) {
            warn!(
                "TcpStream NAGLE disable failed ({e:?}) - packet delivery will be slightly delayed"
            )
        }

        info!("successfully connected to deez tcp engine!");

        Ok(stream)
    }

    pub async fn forward_packets(
        stream: Arc<Mutex<OwnedWriteHalf>>,
        data: &[u8],
    ) -> Result<(), std::io::Error> {
        stream.lock().await.write_all(data).await
    }

    pub fn join(self) {
        self.deez_engine_forwarder.join().unwrap();
    }
}
