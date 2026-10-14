use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, watch};

use crate::protocol::frame::{self, FrameParser};

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

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
    /// Listener that accepted this client. Only history replay reads it;
    /// routing, caching and the in-flight slot are shared across listeners.
    pub listener: Arc<ListenerProfile>,
}

/// One TCP accept loop's configuration.
///
/// The anonymous listener (the historical `--tcp-frontend-port`, `name: None`)
/// keeps serving any number of clients and never replays history: replay is a
/// per-client guarantee only a single-client slot can make. A named listener
/// replays `history_hours` of channel messages to whoever connects, resuming
/// from a cursor remembered by *name* -- which is why renaming a listener is
/// how an operator hands a port to a new device or resets one that lost its
/// local store.
#[derive(Debug, Clone)]
pub struct ListenerProfile {
    pub name: Option<String>,
    pub bind_addr: String,
    /// Replay depth. 0 disables replay even for a named listener.
    pub history_hours: u64,
}

impl ListenerProfile {
    pub fn replays_history(&self) -> bool {
        self.name.is_some() && self.history_hours > 0
    }

    /// Name for logs and the history cursor key.
    pub fn label(&self) -> &str {
        self.name.as_deref().unwrap_or("<anonymous>")
    }
}

pub type ClientDirectMap = Arc<Mutex<HashMap<ClientId, mpsc::UnboundedSender<Vec<u8>>>>>;

pub struct TcpFrontend {
    listeners: Vec<ListenerProfile>,
    command_tx: mpsc::UnboundedSender<ClientCommand>,
    broadcast_tx: broadcast::Sender<Vec<u8>>,
    direct_map: ClientDirectMap,
    shutdown_rx: watch::Receiver<bool>,
}

impl TcpFrontend {
    pub fn new(
        listeners: Vec<ListenerProfile>,
        command_tx: mpsc::UnboundedSender<ClientCommand>,
        broadcast_tx: broadcast::Sender<Vec<u8>>,
        direct_map: ClientDirectMap,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            listeners,
            command_tx,
            broadcast_tx,
            direct_map,
            shutdown_rx,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        for profile in &self.listeners {
            let listener = TcpListener::bind(&profile.bind_addr)
                .await
                .with_context(|| format!("failed to bind TCP frontend on {}", profile.bind_addr))?;
            tracing::info!(
                addr = %profile.bind_addr,
                listener = %profile.label(),
                history_hours = profile.history_hours,
                "TCP frontend listening"
            );

            let command_tx = self.command_tx.clone();
            let broadcast_tx = self.broadcast_tx.clone();
            let direct_map = self.direct_map.clone();
            let shutdown_rx = self.shutdown_rx.clone();
            let profile = Arc::new(profile.clone());
            tokio::spawn(async move {
                if let Err(e) = accept_loop(
                    listener,
                    profile.clone(),
                    command_tx,
                    broadcast_tx,
                    direct_map,
                    shutdown_rx,
                )
                .await
                {
                    tracing::error!(error = %e, listener = %profile.label(), "TCP frontend error");
                }
            });
        }

        // Hold the caller's contract of returning at shutdown; the accept
        // loops notice the same watch channel on their own.
        let mut shutdown_rx = self.shutdown_rx.clone();
        loop {
            if *shutdown_rx.borrow() || shutdown_rx.changed().await.is_err() {
                break;
            }
        }

        tracing::info!("TCP frontend shutting down");
        Ok(())
    }
}

async fn accept_loop(
    listener: TcpListener,
    profile: Arc<ListenerProfile>,
    command_tx: mpsc::UnboundedSender<ClientCommand>,
    broadcast_tx: broadcast::Sender<Vec<u8>>,
    direct_map: ClientDirectMap,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, addr) = result?;
                let command_tx = command_tx.clone();
                let broadcast_rx = broadcast_tx.subscribe();
                let direct_map = direct_map.clone();
                let profile = profile.clone();
                tracing::info!(addr = %addr, listener = %profile.label(), "spawning client handler");
                tokio::spawn(handle_client(stream, addr, command_tx, broadcast_rx, direct_map, profile));
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    return Ok(());
                }
            }
        }
    }
}

async fn handle_client(
    stream: TcpStream,
    addr: SocketAddr,
    command_tx: mpsc::UnboundedSender<ClientCommand>,
    mut broadcast_rx: broadcast::Receiver<Vec<u8>>,
    direct_map: ClientDirectMap,
    listener: Arc<ListenerProfile>,
) {
    let client_id = ClientId::new();
    tracing::info!(client = %client_id, addr = %addr, listener = %listener.label(), "client connected");

    let _ = stream.set_nodelay(true);

    let (mut reader, mut writer) = stream.into_split();
    let mut parser = FrameParser::new();

    let (direct_tx, mut direct_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    direct_map.lock().unwrap().insert(client_id, direct_tx);

    // The read task needs the listener profile to tag each command with; the
    // write task and the trailing log keep their own handle.
    let read_listener = listener.clone();
    let read_handle = tokio::spawn(async move {
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
                        let cmd = ClientCommand {
                            client_id,
                            payload,
                            listener: read_listener.clone(),
                        };
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

    let write_handle = tokio::spawn(async move {
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
        _ = read_handle => {},
        _ = write_handle => {},
    }

    direct_map.lock().unwrap().remove(&client_id);
    tracing::info!(client = %client_id, addr = %addr, listener = %listener.label(), "client disconnected");
}
