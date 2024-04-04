use std::{
    io,
    sync::Arc,
    thread::{Builder, JoinHandle},
    time::{Duration, Instant},
};

use base64::{engine::general_purpose, Engine};
use jito_block_engine::block_engine::BlockEnginePackets;
use log::*;
use solana_sdk::transaction::VersionedTransaction;
use thiserror::Error;
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    runtime::Runtime,
    select,
    sync::{broadcast::Receiver, mpsc, Mutex},
    time::{interval, sleep, timeout},
};

const HEARTBEAT_MSG: &[u8; 5] = b"ping\n";
const DEEZ_ENGINE_URLS: [&str; 5] = [
    "ny.engine.deez.wtf",
    "utah.engine.deez.wtf",
    "amsterdam.engine.deez.wtf",
    "london.engine.deez.wtf",
    "tokyo.engine.deez.wtf",
];

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

impl DeezEngineRelayerHandler {
    pub fn new(mut deez_engine_receiver: Receiver<BlockEnginePackets>) -> DeezEngineRelayerHandler {
        let deez_engine_forwarder = Builder::new()
            .name("deez_engine_relayer_handler_thread".into())
            .spawn(move || {
                let rt = Runtime::new().unwrap();
                rt.block_on(async move {
                    loop {
                        let result = Self::connect(
                            &mut deez_engine_receiver,
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
                    }
                })
            })
            .unwrap();

        DeezEngineRelayerHandler {
            deez_engine_forwarder,
        }
    }

    async fn connect(
        deez_engine_receiver: &mut Receiver<BlockEnginePackets>,
    ) -> DeezEngineResult<()> {
        let deez_engine_url = Self::find_closest_engine().await?;
        info!("determined closest engine as {}", deez_engine_url);
        let engine_stream = Self::connect_to_engine(&deez_engine_url).await?;
        Self::start_event_loop(deez_engine_receiver, engine_stream).await
    }

    async fn start_event_loop(
        deez_engine_receiver: &mut Receiver<BlockEnginePackets>,
        deez_engine_stream: TcpStream,
    ) -> DeezEngineResult<()> {
        let forwarder = Arc::new(Mutex::new(deez_engine_stream));
        let mut heartbeat_interval = interval(Duration::from_secs(5));
        let (forward_error_sender, mut forward_error_receiver) = mpsc::unbounded_channel();
         
        loop {
            let cloned_forwarder = forwarder.clone();
            let cloned_error_sender = forward_error_sender.clone();

            select! {
                recv_result = deez_engine_receiver.recv() => {
                    match recv_result {
                        Ok(deez_engine_batches) => {
                            trace!("received deez engine batches");
                            // Proceed with handling the batches as before
                            tokio::spawn(async move {
                                for packet_batch in deez_engine_batches.banking_packet_batch.0.iter() {
                                    for packet in packet_batch {
                                        if packet.meta().discard() || packet.meta().is_simple_vote_tx() {
                                            continue;
                                        }

                                        if let Ok(tx) = packet.deserialize_slice::<VersionedTransaction, _>(..) {
                                            let tx_data = match bincode::serialize(&tx) {
                                                Ok(data) => data,
                                                Err(_) => continue, // Handle serialization error or log it as needed
                                            };
                                        
                                            let base64_encoded_tx = general_purpose::STANDARD.encode(tx_data);
                                            let delimited_tx_data = format!("{}\n", base64_encoded_tx);

                                            if let Err(e) = Self::forward_packets(cloned_forwarder.clone(), delimited_tx_data.as_bytes()).await {
                                                if let Err(send_err) = cloned_error_sender.send(e) {
                                                    error!("failed to transmit packet forward error to management channel: {send_err}");
                                                }
                                            } else {
                                                trace!("succesfully relayed packets");
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
                    Self::forward_packets(cloned_forwarder.clone(), HEARTBEAT_MSG).await?;
                }
            }
        }
    }

    pub async fn find_closest_engine() -> DeezEngineResult<String> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(2))
            .build()?;

        let mut closest_engine = String::new();
        let mut shortest_time = Duration::from_secs(u64::MAX);

        for &url in DEEZ_ENGINE_URLS.iter() {
            let start = Instant::now();
            let result = client
                .get(format!("http://{}:8372/mempool/health", url))
                .send()
                .await;

            match result {
                Ok(_response) => {
                    let elapsed = start.elapsed();
                    if elapsed < shortest_time {
                        shortest_time = elapsed;
                        closest_engine = url.to_string();
                    }
                }
                Err(_e) => {
                    error!("error connecting to {}", url)
                    // ignore for now
                }
            }
        }

        if closest_engine.is_empty() {
            Err(DeezEngineError::CannotFindEngine(
                "could not connect to any engine.".to_string(),
            ))
        } else {
            Ok(format!("{}:8373", closest_engine))
        }
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
        stream: Arc<Mutex<TcpStream>>,
        data: &[u8],
    ) -> Result<(), std::io::Error> {
        stream.lock().await.write_all(data).await
    }

    pub fn join(self) {
        self.deez_engine_forwarder.join().unwrap();
    }
}
