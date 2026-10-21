use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::Duration;

use crate::protocol::frame::{self, FrameParser};

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

const BIND_RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct ClientId(u64);

impl ClientId {
    pub fn new() -> Self {
        Self(NEXT_CLIENT_ID.fetch_add(1, Ordering::SeqCst))
    }
}

impl std::fmt::Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug)]
pub struct ClientCommand {
    pub client_id: ClientId,
    pub payload: Vec<u8>,
}

pub type ClientDirectMap = Arc<Mutex<HashMap<ClientId, mpsc::UnboundedSender<Vec<u8>>>>>;

pub struct TcpFrontend {
    bind_addrs: Vec<String>,
    command_tx: mpsc::UnboundedSender<ClientCommand>,
    broadcast_tx: broadcast::Sender<Vec<u8>>,
    direct_map: ClientDirectMap,
    // Whether the physical radio is attached and initialized. MeshCore has
    // no frame to say "device unavailable", so while it is false the
    // frontend behaves like an absent node: it does not listen and every
    // client connection is closed.
    radio_up_rx: watch::Receiver<bool>,
    shutdown_rx: watch::Receiver<bool>,
}

impl TcpFrontend {
    pub fn new(
        bind_addrs: Vec<String>,
        command_tx: mpsc::UnboundedSender<ClientCommand>,
        broadcast_tx: broadcast::Sender<Vec<u8>>,
        direct_map: ClientDirectMap,
        radio_up_rx: watch::Receiver<bool>,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            bind_addrs,
            command_tx,
            broadcast_tx,
            direct_map,
            radio_up_rx,
            shutdown_rx,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        loop {
            // Stay closed until the radio is up.
            tokio::select! {
                result = self.radio_up_rx.wait_for(|up| *up) => {
                    if result.is_err() {
                        return Ok(());
                    }
                }
                _ = self.shutdown_rx.wait_for(|stop| *stop) => {
                    tracing::info!("TCP frontend shutting down");
                    return Ok(());
                }
            }

            let Some(listeners) = self.bind_all().await else {
                tokio::time::sleep(BIND_RETRY_DELAY).await;
                continue;
            };

            // Separate receiver: the select arm below borrows it mutably
            // while the accept arm clones self.radio_up_rx for each client.
            let mut down_rx = self.radio_up_rx.clone();
            loop {
                // Accept on whichever listener gets a connection first.
                let accept_any =
                    futures::future::select_all(listeners.iter().map(|l| Box::pin(l.accept())));
                tokio::select! {
                    (result, _, _) = accept_any => {
                        let (stream, addr) = match result {
                            Ok(conn) => conn,
                            Err(e) => {
                                tracing::warn!(error = %e, "TCP frontend accept failed");
                                continue;
                            }
                        };
                        let command_tx = self.command_tx.clone();
                        let broadcast_rx = self.broadcast_tx.subscribe();
                        let direct_map = self.direct_map.clone();
                        let radio_up_rx = self.radio_up_rx.clone();
                        tracing::info!(addr = %addr, "spawning client handler");
                        tokio::spawn(handle_client(
                            stream,
                            addr,
                            command_tx,
                            broadcast_rx,
                            direct_map,
                            radio_up_rx,
                        ));
                    }
                    _ = down_rx.wait_for(|up| !*up) => {
                        tracing::warn!("radio unavailable, TCP frontend closed");
                        break;
                    }
                    _ = self.shutdown_rx.wait_for(|stop| *stop) => {
                        tracing::info!("TCP frontend shutting down");
                        return Ok(());
                    }
                }
            }
        }
    }

    /// Bind every configured address, or none: a partial frontend would
    /// silently leave the clients of the failed port without service.
    async fn bind_all(&self) -> Option<Vec<TcpListener>> {
        let mut listeners = Vec::with_capacity(self.bind_addrs.len());
        for addr in &self.bind_addrs {
            match TcpListener::bind(addr).await {
                Ok(l) => {
                    tracing::info!(addr = %addr, "TCP frontend listening");
                    listeners.push(l);
                }
                Err(e) => {
                    tracing::error!(addr = %addr, error = %e, "TCP frontend bind failed, retrying");
                    return None;
                }
            }
        }
        Some(listeners)
    }
}

async fn handle_client(
    stream: TcpStream,
    addr: SocketAddr,
    command_tx: mpsc::UnboundedSender<ClientCommand>,
    mut broadcast_rx: broadcast::Receiver<Vec<u8>>,
    direct_map: ClientDirectMap,
    mut radio_up_rx: watch::Receiver<bool>,
) {
    let client_id = ClientId::new();
    tracing::info!(client = %client_id, addr = %addr, "client connected");

    let _ = stream.set_nodelay(true);

    let (mut reader, mut writer) = stream.into_split();
    let mut parser = FrameParser::new();

    let (direct_tx, mut direct_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    direct_map.lock().unwrap().insert(client_id, direct_tx);

    let mut read_handle = tokio::spawn(async move {
        let mut buf = vec![0u8; 4096];

        loop {
            match tokio::io::AsyncReadExt::read(&mut reader, &mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    let frames = parser.feed(&buf[..n]);
                    for payload in frames {
                        tracing::trace!(
                            client = %client_id,
                            len = payload.len(),
                            "received command from client"
                        );
                        let cmd = ClientCommand { client_id, payload };
                        if command_tx.send(cmd).is_err() {
                            return;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(client = %client_id, error = %e, "client read error");
                    break;
                }
            }
        }
    });

    let mut write_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                Some(payload) = direct_rx.recv() => {
                    let framed = frame::encode_response(&payload);
                    if tokio::io::AsyncWriteExt::write_all(&mut writer, &framed).await.is_err() {
                        break;
                    }
                }
                result = broadcast_rx.recv() => {
                    match result {
                        Ok(payload) => {
                            let framed = frame::encode_response(&payload);
                            if tokio::io::AsyncWriteExt::write_all(&mut writer, &framed).await.is_err() {
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(_) => break,
                    }
                }
            }
        }
    });

    tokio::select! {
        _ = &mut read_handle => {},
        _ = &mut write_handle => {},
        _ = radio_up_rx.wait_for(|up| !*up) => {
            tracing::info!(client = %client_id, "radio unavailable, closing client");
        }
    }

    // Dropping a JoinHandle does not cancel its task. Abort both halves so
    // the socket is actually closed and the peer sees the disconnect.
    read_handle.abort();
    write_handle.abort();

    direct_map.lock().unwrap().remove(&client_id);
    tracing::info!(client = %client_id, addr = %addr, "client disconnected");
}
