use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::{self, Duration};

use crate::backend;
use crate::frontend::tcp::{
    ClientCommand, ClientDirectMap, ClientId, ListenerProfile, TcpFrontend,
};
use crate::node::state::{CachedChannel, NewMessage, NodeState};
use crate::protocol::decode::{
    DecodedValue, decode_command_payload, decode_response_payload, format_decoded,
};

const TIMEOUT_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// How long a command whose client already got ERROR is kept as a "ghost",
/// waiting to swallow the reply the radio still owes it. Long enough to cover
/// any plausible radio loop stall; short enough that a reply genuinely lost on
/// the link does not keep eating later commands' answers.
const GHOST_TTL: Duration = Duration::from_secs(60);

/// Upper bound on outstanding ghosts, so a radio that has stopped answering
/// altogether cannot make them pile up for a whole TTL.
const MAX_GHOSTS: usize = 8;

/// A reply slower than this is the radio stalling on something else, not a
/// round trip. Worth saying out loud: it is the early warning for the stall
/// that eventually trips the in-flight timeout.
const SLOW_REPLY_WARN: Duration = Duration::from_secs(1);

/// How often the log is swept for expired messages. Deliberately far apart:
/// the sweep is a write on the SQLite pool, which is pinned to one connection
/// and shared with the radio RX path.
const PRUNE_INTERVAL: Duration = Duration::from_secs(60);

const SECS_PER_HOUR: i64 = 3600;

/// Protocol version the proxy advertises to the radio as `app_target_ver` in
/// CMD_DEVICE_QUERY. The firmware only branches on `app_target_ver >= 3`
/// (`MyMesh.cpp`: `queueMessage` and `onChannelMessageRecv`), which selects the
/// V3 message frames (RESP_CODE_CONTACT_MSG_RECV_V3 0x10 /
/// RESP_CODE_CHANNEL_MSG_RECV_V3 0x11) over the legacy forms (0x07/0x08).
///
/// It mirrors FIRMWARE_VER_CODE (14 on MeshCore 1.18.0), which is the version
/// this proxy reports verbatim to clients inside the cached DEVICE_INFO blob;
/// pinning it keeps the two consistent. Before this was pinned the radio's
/// `app_target_ver` was whatever the first client to slip past the cache sent,
/// so the frame type a client saw depended on which other client connected
/// first.
const APP_TARGET_VER: u8 = 14;

/// Maximum age of a pending_acks entry before it is considered abandoned.
/// LoRa retransmissions inside the radio top out around ~30 s; five minutes
/// is comfortably above any realistic real ACK and short enough to bound
/// memory growth from channel sends (which never produce an ACK).
const PENDING_ACK_TTL: Duration = Duration::from_secs(300);

/// Response codes that are spontaneous pushes from the radio and never complete
/// an in-flight request. They are always broadcast to every client.
///
/// Because `try_route_to_in_flight` completes the in-flight request on any code
/// that is not listed here and that `is_reply_for` does not rule out, this list
/// is load-bearing: a missing unsolicited code gets delivered to whichever
/// client happens to have a request in flight and terminates it early, robbing
/// every other client of the frame.
///
/// Only list codes the radio emits *without* a corresponding command. The
/// borderline cases, checked against MeshCore 1.18.0 `MyMesh.cpp`:
///
/// * `0x84` RAW_DATA and `0x8E` CONTROL_DATA are inbound-only. The outbound
///   commands (`CMD_SEND_RAW_DATA` 0x19, `CMD_SEND_CONTROL_DATA` 0x37) are
///   answered with a plain OK, so these codes never belong to a request.
/// * `0x85`/`0x86` LOGIN_*, `0x87` STATUS_RESPONSE, `0x8B` TELEMETRY_RESPONSE
///   and `0x89` TRACE_DATA look like pushes but are, or can be, replies the app
///   correlates with a command it sent (usually by matching a tag). Listing any
///   of them would hang that command forever. 0x8B is the decisive case: the
///   `len == 4` "self" form of CMD_SEND_TELEMETRY_REQ is answered with it
///   directly, and nothing else closes that request.
/// * `0x8C` BINARY_RESPONSE and `0x8D` PATH_DISCOVERY_RESPONSE *are* pushes,
///   despite being tag-correlated. Their commands (CMD_SEND_BINARY_REQ 0x32,
///   CMD_SEND_ANON_REQ 0x39, CMD_SEND_PATH_DISCOVERY_REQ 0x34 and remote
///   CMD_SEND_TELEMETRY_REQ 0x27) are all answered immediately with
///   RESP_CODE_SENT (0x06) carrying the tag; the 0x8C/0x8D arrives seconds
///   later, once the in-flight slot is long free, and the app finds it by tag.
///   Listing them cannot hang anything -- no command's *direct* reply is 0x8C
///   or 0x8D -- and leaving them out means a late frame is eaten by whatever
///   unrelated request happens to be in flight. MeshCore 1.18.0 made that
///   routine by emitting an unsolicited 0x8C from `onContactResponse` for every
///   contact response that matches no pending request (e.g. push telemetry).
/// * `0x82` ACK is listed so `try_route_to_in_flight` declines it and control
///   falls through to `try_route_ack`, which routes it to the client that is
///   waiting on that `ack_code`. Without this the ACK is eaten by whatever
///   unrelated request happens to be in flight.
const PUSH_EVENT_CODES: &[u8] = &[
    0x80, // ADVERTISEMENT           (push)
    0x81, // PATH_UPDATE             (push)
    0x82, // ACK / SEND_CONFIRMED    (routed via pending_acks)
    0x83, // MESSAGES_WAITING        (push 'tickle')
    0x84, // RAW_DATA                (push, onRawDataRecv)
    0x88, // LOG_DATA                (push)
    0x8A, // NEW_ADVERT              (push)
    0x8C, // BINARY_RESPONSE         (tag-correlated push, see above)
    0x8D, // PATH_DISCOVERY_RESPONSE (tag-correlated push, see above)
    0x8E, // CONTROL_DATA            (push, onControlDataRecv)
    0x8F, // CONTACT_DELETED         (push, onContactOverwrite)
    0x90, // CONTACTS_FULL           (push, onContactsFull)
];

fn is_push_event(code: u8) -> bool {
    PUSH_EVENT_CODES.contains(&code)
}

/// Response codes that only ever belong to a `GET_CONTACTS` reply in progress.
///
/// The radio streams that reply one frame per main-loop iteration, so it spans
/// minutes on a repeater with hundreds of contacts and its frames arrive
/// interleaved with -- and after -- the replies to commands sent while it ran
/// (`MyMesh::checkSerialInterface` advances the iterator only on an iteration
/// where no command arrived; only a second `GET_CONTACTS` is refused, with
/// ERR_CODE_BAD_STATE). They must therefore never satisfy a *different*
/// request: a stray CONTACT frame would be eaten by whatever is waiting and its
/// real answer would then be broadcast to clients that never asked. Same shape
/// as a missing `PUSH_EVENT_CODES` entry, from the other direction.
fn is_contacts_frame(code: u8) -> bool {
    matches!(code, 0x02..=0x04)
}

/// The `since` a GET_CONTACTS payload carries; absent or zero means the whole
/// book. A request with a `since` streams only what changed after it, so its
/// reply cannot be used to conclude the cached rows are complete.
fn contacts_since(payload: &[u8]) -> u32 {
    if payload.len() < 5 {
        return 0;
    }
    u32::from_le_bytes(payload[1..5].try_into().unwrap_or([0; 4]))
}

/// Whether `code` can complete a request whose command was `cmd_code`, or be
/// the reply a ghost filed for that command is waiting to swallow.
///
/// Push events never can, and neither can the frames of an open contacts
/// stream -- with one exception that the command disambiguates.
/// `RESP_CODE_CONTACT` (0x03) is both an iterator frame of a `GET_CONTACTS`
/// reply (`MyMesh::checkSerialInterface`) and the answer `CMD_GET_CONTACT_BY_KEY`
/// (0x1e) gets from `writeContactRespFrame`, so for a 0x1e request it is a reply
/// and for every other command it is still a stream frame. 0x02/0x04 belong to
/// the stream alone: every lookup is answered with 0x03.
///
/// Matching a 0x03 to the wrong command is why this is command-relative rather
/// than a flat "is a reply" test. Without the exception the lookup's answer was
/// never claimed by its own request -- it fell through to the push broadcast --
/// and the ghost filed for the failed lookup could never claim a 0x03 of its
/// own, so it sat for `GHOST_TTL` swallowing whatever unrelated reply arrived
/// next, taking the whole command path down with it.
///
/// `try_route_contacts_stream` runs before either matcher, so while a
/// `GET_CONTACTS` reply is open a 0x03 still goes to its subscribers and a
/// lookup answered mid-dump keeps the old, ambiguous behaviour.
fn is_reply_for(cmd_code: u8, code: u8) -> bool {
    if is_push_event(code) {
        return false;
    }
    if is_contacts_frame(code) {
        return cmd_code == 0x1E && code == 0x03;
    }
    true
}

/// How much of an open contacts reply is buffered so a client attaching
/// mid-stream can be caught up. A companion radio's whole book is
/// `MAX_CONTACTS` entries of ~150 B (`DEVICE_INFO` reports 350 on the boards
/// seen so far), so this is headroom over any real reply and only a radio
/// streaming nonsense can reach it.
const MAX_CONTACTS_REPLAY_BYTES: usize = 256 * 1024;

#[derive(Debug)]
struct InFlight {
    /// `None` for internal sync commands (sync_from_radio); `Some` for client requests.
    client_id: Option<ClientId>,
    cmd_code: u8,
    sent_at: Instant,
}

#[derive(Debug)]
struct Queued {
    /// `None` for internal sync commands; `Some` for client requests.
    client_id: Option<ClientId>,
    payload: Vec<u8>,
}

/// A `GET_CONTACTS` reply the radio is streaming, if one is open.
///
/// This runs *alongside* `in_flight`: the firmware advances the contact
/// iterator between companion commands rather than blocking on it, so a
/// client's GET_CHANNEL or GET_MSG is answered while the stream is still
/// going. At most one may be open -- the radio refuses a second one with
/// ERR_CODE_BAD_STATE -- so a client that asks while one runs is attached to
/// it (see `attach_to_contacts`) instead of being forwarded.
#[derive(Debug)]
struct ContactsStream {
    /// Every frame since CONTACT_START, in order. A client that attaches
    /// mid-stream is replayed this prefix so it receives the whole reply rather
    /// than the tail of the one already in progress.
    frames: Vec<Vec<u8>>,
    /// Clients receiving this reply: the one that asked, plus anyone attached.
    subscribers: Vec<ClientId>,
    /// False once `frames` passes the cap; a later subscriber can then no
    /// longer be caught up and is queued instead.
    replayable: bool,
    /// The `since` this reply was asked for. A client may only attach to a
    /// reply that covers what it asked for: with `since = 0` (the whole book) it
    /// covers every ask, and a delta covers an ask for the same watermark or
    /// older. Attaching a client that asked for everything to someone else's
    /// delta would hand it a truncated list it could not detect as one.
    since: u32,
    last_frame_at: Instant,
}

/// A command the proxy gave up on, whose reply has not arrived yet.
///
/// The radio answers in order, so an owed reply lands before the reply to any
/// command sent after it — which is exactly why it must be accounted for: it
/// would otherwise be matched against whatever is in flight and handed to a
/// client that never asked for it.
#[derive(Debug)]
struct Ghost {
    cmd_code: u8,
    since: Instant,
}

/// Whether `try_serve_from_cache` can answer this command without the radio.
///
/// These are the reads the startup sync warms (`initialize_radio` fetches
/// SELF_INFO and DEVICE_INFO, `sync_from_radio` the channel list), which is what
/// makes them worth leaving *behind* the sync in `enqueue_or_dispatch` rather
/// than jumping it: a queued one is normally answered from the freshly warmed
/// cache without touching the radio. Keep in sync with the arms of
/// `try_serve_from_cache` -- `GET_CONTACTS` is forwarded, never cached, so it
/// jumps the sync like any other client request.
fn cache_served_command(cmd_code: u8) -> bool {
    matches!(
        cmd_code,
        0x01 // APP_START        → SELF_INFO
            | 0x14 // GET_BATTERY
            | 0x16 // DEVICE_QUERY    → DEVICE_INFO
            | 0x1F // GET_CHANNEL
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendType {
    Serial,
    Ble,
    Tcp,
}

pub struct CoreConfig {
    pub backend_type: BackendType,
    pub serial_port: Option<String>,
    pub serial_baud: u32,
    pub ble_address: Option<String>,
    #[cfg_attr(not(feature = "ble"), allow(dead_code))]
    pub ble_pin: String,
    pub tcp_backend_host: Option<String>,
    pub tcp_backend_port: u16,
    pub tcp_frontend_host: String,
    pub tcp_frontend_port: u16,
    /// Named listeners, each with its own message-history cursor. The
    /// anonymous listener built from the two fields above is prepended to
    /// these at startup.
    pub listeners: Vec<ListenerProfile>,
    /// Retention window: how far back a named listener may replay, and the age
    /// at which logged messages are pruned.
    pub message_history_hours: u64,
    /// Whether any listener replays history at all. When none does, every row
    /// the log would hold is unreadable: no cursor exists to replay it, so the
    /// inserts are pure latency on the path that delivers each frame.
    pub log_messages: bool,
    /// How long a client request may occupy the in-flight slot before it is
    /// failed with ERROR. See `--in-flight-timeout`.
    pub in_flight_timeout: Duration,
    /// How long a client connection may go without sending a command before
    /// the frontend closes it. Zero disables the sweep. See
    /// `--client-idle-timeout`.
    pub client_idle_timeout: Duration,
    pub data_dir: PathBuf,
    pub event_log_level: LogLevel,
    pub event_log_json: bool,
    pub record_radio_rx: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Off,
    Summary,
    Verbose,
}

pub struct Core {
    config: CoreConfig,
    state: Arc<NodeState>,
    self_info_raw: Option<Vec<u8>>,
    device_info_raw: Option<Vec<u8>>,
    battery_info_raw: Option<Vec<u8>>,
    radio_pubkey: Option<[u8; 32]>,
    radio_name: Option<String>,
    max_channels: u8,
    radio_send_tx: Option<mpsc::UnboundedSender<Vec<u8>>>,
    radio_recv_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    shutdown_rx: watch::Receiver<bool>,
    command_tx: mpsc::UnboundedSender<ClientCommand>,
    command_rx: mpsc::UnboundedReceiver<ClientCommand>,
    broadcast_tx: broadcast::Sender<Vec<u8>>,
    client_channels: ClientDirectMap,
    // The radio handles one command at a time. We serialize: a single
    // request is in flight; further client commands wait in pending_queue.
    in_flight: Option<InFlight>,
    pending_queue: VecDeque<Queued>,
    // The one GET_CONTACTS reply that may be streaming alongside `in_flight`.
    // Contacts are not cached: the request carries the client's own `since`
    // watermark and the radio answers it, so the proxy's job here is to keep one
    // dump from being paid for twice -- and to keep a minutes-long one from
    // blocking every other command.
    contacts: Option<ContactsStream>,
    // Commands whose client already got ERROR, but whose reply the radio still
    // owes us: it answers in order, so the reply will arrive before that of any
    // command sent afterwards. Holding them here keeps a late reply from being
    // handed to the next client as *its* answer (see `try_claim_ghost`).
    ghosts: VecDeque<Ghost>,
    // Clients waiting for the (possibly much later) LoRa-delivery ACK
    // (0x82), keyed by the 4-byte expected_ack the radio handed back in
    // the corresponding MSG_SENT. The Instant is the insertion time,
    // used to evict stale entries when a new ack is recorded. Channel
    // sends (0x03) never produce an ACK, so without eviction those
    // entries would accumulate forever.
    pending_acks: HashMap<[u8; 4], (ClientId, Instant)>,
    // Message-history replay state, one entry per client that has polled on a
    // history listener. The entry outlives a client's catch-up (so a second
    // client on the same single-client listener can be spotted) and is
    // removed when the client goes away. Bounded by connected clients, and
    // the *persisted* cursors are bounded by the configured listeners.
    history_sync: HashMap<ClientId, HistorySync>,
    last_prune: Instant,
}

/// A client catching up on a listener's message history.
#[derive(Debug)]
struct HistorySync {
    listener: String,
    /// Last row id handed to this session.
    cursor: i64,
    /// Newest row id when the client first spoke on this connection. Frozen
    /// on purpose: frames newer than this reach the client as live
    /// broadcasts, so replaying them too would deliver them twice.
    head: i64,
    /// Highest row id this client was handed live. Only frames that actually
    /// reached it count, which is why this is credited at the delivery sites
    /// rather than by re-reading `latest_message_id()` when the session ends:
    /// a client that has gone away must not be credited with frames logged
    /// after it left.
    live_high: i64,
    /// Oldest `recv_ts` this listener wants.
    since_ts: i64,
}

impl Core {
    pub async fn new(config: CoreConfig, shutdown_rx: watch::Receiver<bool>) -> Result<Self> {
        let state = NodeState::open(config.data_dir.join("state.db")).await?;

        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (broadcast_tx, _) = broadcast::channel(256);

        let (_, dummy_rx) = mpsc::unbounded_channel::<Vec<u8>>();

        Ok(Self {
            config,
            state: Arc::new(state),
            self_info_raw: None,
            device_info_raw: None,
            battery_info_raw: None,
            radio_pubkey: None,
            radio_name: None,
            max_channels: 40,
            radio_send_tx: None,
            radio_recv_rx: dummy_rx,
            shutdown_rx,
            command_tx,
            command_rx,
            broadcast_tx,
            client_channels: Arc::new(Mutex::new(HashMap::new())),
            in_flight: None,
            pending_queue: VecDeque::new(),
            contacts: None,
            ghosts: VecDeque::new(),
            pending_acks: HashMap::new(),
            history_sync: HashMap::new(),
            last_prune: Instant::now(),
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        tracing::info!("starting coresplitter (MeshCore client multiplexer + cache)");

        if let Err(e) = self.connect_backend().await {
            tracing::warn!(error = %e, "initial radio connection failed, will retry");
            self.reconnect_radio().await;
        } else if let Err(e) = self.initialize_radio().await {
            tracing::warn!(error = %e, "initial radio initialization failed, will retry");
            self.radio_send_tx = None;
            self.reconnect_radio().await;
        }

        // Queue sync commands so they serialize with any early client requests.
        self.sync_from_radio().await;

        // Load persisted blobs from kv_store if not already set (radio offline fallback)
        if self.self_info_raw.is_none()
            && let Ok(Some(blob)) = self.state.kv_get("self_info_raw").await
        {
            tracing::info!("restored SELF_INFO blob from kv_store");
            self.extract_radio_identity(&blob);
            self.self_info_raw = Some(blob);
        }
        if self.device_info_raw.is_none()
            && let Ok(Some(blob)) = self.state.kv_get("device_info_raw").await
        {
            tracing::info!("restored DEVICE_INFO blob from kv_store");
            self.extract_max_channels(&blob);
            self.device_info_raw = Some(blob);
        }
        if self.battery_info_raw.is_none()
            && let Ok(Some(blob)) = self.state.kv_get("battery_info_raw").await
        {
            tracing::info!("restored BATTERY blob from kv_store");
            self.battery_info_raw = Some(blob);
        }
        self.prune_history().await;

        // The anonymous listener keeps the historical single-port behaviour and
        // never replays: it serves any number of clients, and a cursor shared
        // between them would hand history to whichever client polled first and
        // silently withhold it from the rest.
        let mut listeners = vec![ListenerProfile {
            name: None,
            bind_addr: format!(
                "{}:{}",
                self.config.tcp_frontend_host, self.config.tcp_frontend_port
            ),
            history_hours: 0,
        }];
        listeners.extend(self.config.listeners.iter().cloned());

        let shutdown_rx = self.shutdown_rx.clone();
        let mut frontend = TcpFrontend::new(
            listeners,
            self.command_tx.clone(),
            self.broadcast_tx.clone(),
            self.client_channels.clone(),
            shutdown_rx,
            self.config.client_idle_timeout,
        );
        tokio::spawn(async move {
            if let Err(e) = frontend.run().await {
                tracing::error!(error = %e, "TCP frontend error");
            }
        });

        tracing::info!("multiplexer ready, accepting clients");

        let mut timeout_tick = tokio::time::interval(TIMEOUT_CHECK_INTERVAL);
        timeout_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;

                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        tracing::info!("shutdown signal received, stopping");
                        break;
                    }
                }
                Some(cmd) = self.command_rx.recv() => {
                    self.handle_client_command(cmd).await;
                }
                data = self.radio_recv_rx.recv() => {
                    match data {
                        Some(payload) => self.handle_radio_rx(payload).await,
                        None => {
                            tracing::warn!("radio connection lost");
                            self.reconnect_radio().await;
                        }
                    }
                }
                _ = timeout_tick.tick() => {
                    self.check_in_flight_timeout().await;
                    self.housekeeping().await;
                }
            }
        }

        tracing::info!("performing graceful shutdown");
        self.shutdown().await;

        Ok(())
    }

    async fn shutdown(&mut self) {
        tracing::info!("shutting down multiplexer");

        self.radio_send_tx = None;
        let (_, dummy_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        self.radio_recv_rx = dummy_rx;

        // Persist replay positions for clients still connected, so a restart
        // does not hand them their own recent history a second time.
        let clients: Vec<ClientId> = self.history_sync.keys().copied().collect();
        for client in clients {
            self.flush_history_cursor(&client, true).await;
        }

        tracing::info!("shutdown complete");
    }

    async fn connect_backend(&mut self) -> Result<()> {
        let radio = match self.config.backend_type {
            BackendType::Serial => {
                let port = self
                    .config
                    .serial_port
                    .as_ref()
                    .context("serial port not specified")?;
                tracing::info!(port = %port, baud = self.config.serial_baud, "connecting to radio via serial");
                backend::serial::connect(port, self.config.serial_baud).await?
            }
            BackendType::Ble => {
                let addr = self
                    .config
                    .ble_address
                    .as_ref()
                    .context("BLE address not specified")?;
                tracing::info!(addr = %addr, "connecting to radio via BLE");
                #[cfg(not(feature = "ble"))]
                anyhow::bail!("BLE support not compiled (rebuild with --features ble)");
                #[cfg(feature = "ble")]
                backend::ble::connect(addr, &self.config.ble_pin).await?
            }
            BackendType::Tcp => {
                let host = self
                    .config
                    .tcp_backend_host
                    .as_ref()
                    .context("TCP backend host not specified")?;
                let port = self.config.tcp_backend_port;
                tracing::info!(host = %host, port = %port, "connecting to radio via TCP");
                backend::tcp::connect(host, port).await?
            }
        };

        let backend::RadioIo {
            send_tx,
            mut recv_rx,
            disconnect_rx,
            ..
        } = radio;

        let (publish_tx, new_recv_rx) = mpsc::unbounded_channel();
        self.radio_recv_rx = new_recv_rx;
        self.radio_send_tx = Some(send_tx);

        tokio::spawn(async move {
            let mut disconnect_rx = disconnect_rx;

            loop {
                tokio::select! {
                    data = recv_rx.recv() => {
                        match data {
                            Some(bytes) => {
                                if publish_tx.send(bytes).is_err() {
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                    _ = &mut disconnect_rx => {
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    async fn reconnect_radio(&mut self) {
        self.radio_send_tx = None;

        // Fail any request that was in flight when the radio dropped.
        if let Some(f) = self.in_flight.take()
            && let Some(cid) = f.client_id
        {
            self.send_to_client(&cid, vec![0x01, 0x01]);
        }
        // A contacts reply in progress died with the link. Drop it rather than
        // leave it open: the re-sync below queues a fresh GET_CONTACTS, and an
        // open stream would make that one wait for a CONTACT_END that can
        // never arrive.
        if let Some(stream) = self.contacts.take() {
            for cid in &stream.subscribers {
                self.send_to_client(cid, vec![0x01, 0x01]);
            }
        }
        // Fail all queued client requests; discard internal sync entries.
        while let Some(q) = self.pending_queue.pop_front() {
            if let Some(cid) = q.client_id {
                self.send_to_client(&cid, vec![0x01, 0x01]);
            }
        }

        let mut delay = Duration::from_secs(1);
        let max_delay = Duration::from_secs(60);

        tracing::warn!("starting radio reconnection loop");

        loop {
            tokio::select! {
                biased;
                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        tracing::info!("shutdown during reconnection, aborting");
                        let (_, dummy_rx) = mpsc::unbounded_channel::<Vec<u8>>();
                        self.radio_recv_rx = dummy_rx;
                        return;
                    }
                }
                _ = time::sleep(delay) => {}
            }

            tracing::info!(
                delay_secs = delay.as_secs(),
                "attempting radio reconnection"
            );

            match self.connect_backend().await {
                Ok(()) => {
                    tracing::info!("radio reconnected");
                    match self.initialize_radio().await {
                        Ok(()) => {
                            tracing::info!("radio re-initialized successfully");
                            self.sync_from_radio().await;
                            return;
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "re-initialization failed, will retry");
                            self.radio_send_tx = None;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "reconnection failed");
                }
            }

            delay = (delay * 2).min(max_delay);
        }
    }

    async fn initialize_radio(&mut self) -> Result<()> {
        let appstart = build_appstart();
        self.send_to_radio(&appstart).await?;

        // Wait for SELF_INFO (0x05) from the radio
        self.wait_for_self_info().await?;

        // Query device info to learn radio capabilities (max_channels, etc.).
        //
        // The second byte is `app_target_ver`. The firmware ignores this
        // command entirely unless `len >= 2` (MyMesh.cpp: `CMD_DEVICE_QUERY &&
        // len >= 2`), replying ERR_CODE_UNSUPPORTED_CMD instead -- which would
        // leave `device_info_raw` empty. That matters for more than caching:
        // `app_target_ver` is a single radio-global, sticky variable that
        // selects RESP_CODE_CONTACT_MSG_RECV (0x07/0x08) versus the V3 forms
        // (0x10/0x11) for *every* client. Claiming it here means the proxy owns
        // that negotiation, so a client's own DEVICE_QUERY is served from cache
        // (see `try_serve_from_cache`) and can never flip the frame format out
        // from under the other clients.
        let device_query = vec![0x16, APP_TARGET_VER];
        self.send_to_radio(&device_query).await?;

        self.wait_for_device_info().await?;

        Ok(())
    }

    async fn wait_for_self_info(&mut self) -> Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                anyhow::bail!("timeout waiting for radio SELF_INFO");
            }
            tokio::select! {
                biased;
                _ = self.shutdown_rx.changed() => {
                    anyhow::bail!("shutdown during radio initialization");
                }
                data = self.radio_recv_rx.recv() => {
                    match data {
                        Some(payload) if !payload.is_empty() && payload[0] == 0x05 => {
                            self.cache_response(0x05, &payload).await;
                            if self.self_info_raw.is_some() {
                                tracing::info!(
                                    name = self.radio_name.as_deref().unwrap_or("?"),
                                    pubkey = %self.radio_pubkey
                                        .as_ref()
                                        .map(|pk| hex::encode(&pk[..8]))
                                        .unwrap_or_default(),
                                    "radio initialized, received SELF_INFO"
                                );
                                return Ok(());
                            } else {
                                anyhow::bail!("failed to cache SELF_INFO");
                            }
                        }
                        Some(payload) => {
                            tracing::warn!(code = payload[0], "discarding unexpected response while waiting for SELF_INFO");
                            continue;
                        }
                        None => {
                            anyhow::bail!("radio disconnected during initialization");
                        }
                    }
                }
            }
        }
    }

    async fn wait_for_device_info(&mut self) -> Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                tracing::warn!(
                    "timeout waiting for DEVICE_INFO, using max_channels={}",
                    self.max_channels
                );
                return Ok(());
            }
            tokio::select! {
                biased;
                _ = self.shutdown_rx.changed() => {
                    anyhow::bail!("shutdown during device query");
                }
                data = self.radio_recv_rx.recv() => {
                    match data {
                        Some(payload) if !payload.is_empty() && payload[0] == 0x0D => {
                            self.cache_response(0x0D, &payload).await;
                            return Ok(());
                        }
                        Some(payload) if !payload.is_empty() && payload[0] == 0x01 => {
                            tracing::warn!("radio returned error for DEVICE_QUERY, using default max_channels={}", self.max_channels);
                            return Ok(());
                        }
                        Some(payload) => {
                            tracing::warn!(code = payload[0], "discarding unexpected response while waiting for DEVICE_INFO");
                            continue;
                        }
                        None => {
                            anyhow::bail!("radio disconnected during device query");
                        }
                    }
                }
            }
        }
    }

    async fn sync_from_radio(&mut self) {
        tracing::info!("queuing channel sync from physical radio");

        // Contacts are deliberately NOT prefetched here. The request is the
        // client's to make: it carries its own `since` watermark, and the radio
        // filters the reply by it (`contact.lastmod > since`), so a client that
        // has synced before asks for a delta and gets a handful of frames. The
        // proxy owning a second watermark only to re-ask the same question
        // would duplicate state both ends already keep -- and asking for the
        // whole book at startup is what made a repeater with hundreds of
        // contacts pause for minutes on every restart.
        //
        // So `GET_CONTACTS` is forwarded rather than originated (see
        // `handle_client_command`): a client that has synced before asks for a
        // delta, and one that has not pays for the whole book once.
        for idx in 0..self.max_channels {
            self.pending_queue.push_back(Queued {
                client_id: None,
                payload: vec![0x1F, idx],
            });
        }
        self.advance_queue().await;
    }

    async fn send_to_radio(&self, data: &[u8]) -> Result<()> {
        let tx = self.radio_send_tx.as_ref().context("radio not connected")?;
        tx.send(data.to_vec()).context("failed to send to radio")?;
        Ok(())
    }

    async fn handle_client_command(&mut self, cmd: ClientCommand) {
        if cmd.payload.is_empty() {
            return;
        }

        let cmd_code = cmd.payload[0];
        self.log_event("->", cmd_code, &cmd.payload);

        // 1. History replay: a client on a named listener starts a replay
        //    session the first time it speaks, and its polls are answered from
        //    the message log until it catches up. The radio is never touched,
        //    and neither is the single in-flight slot.
        self.begin_history_sync(&cmd).await;
        if self.try_serve_history(&cmd).await {
            return;
        }

        // 2. Cache-only paths: serve from RAM / SQLite, never touch the radio.
        if self
            .try_serve_from_cache(&cmd.client_id, &cmd.payload)
            .await
        {
            return;
        }

        // 3. A GET_CONTACTS that the cache cannot answer while the radio is
        //    already streaming one is attached to that stream. Forwarding it
        //    would only earn ERR_CODE_BAD_STATE (the firmware allows one
        //    iterator at a time), and waiting for the next stream would cost
        //    the client the whole book again.
        if cmd_code == 0x04 && self.attach_to_contacts(&cmd.client_id, contacts_since(&cmd.payload))
        {
            return;
        }

        // 4. Anything else goes through the serialized radio queue.
        self.enqueue_or_dispatch(Some(cmd.client_id), cmd.payload)
            .await;
    }

    /// Returns true if the command was fully served from cache without
    /// forwarding to the radio. The set of codes handled here is mirrored by
    /// `cache_served_command`, which decides queue precedence; a new arm needs
    /// an entry there too.
    async fn try_serve_from_cache(&self, client_id: &ClientId, payload: &[u8]) -> bool {
        let Some(&cmd_code) = payload.first() else {
            return false;
        };
        match cmd_code {
            0x01 => {
                if let Some(ref blob) = self.self_info_raw {
                    self.send_to_client(client_id, blob.clone());
                    return true;
                }
            }
            0x16 => {
                if let Some(ref blob) = self.device_info_raw {
                    self.send_to_client(client_id, blob.clone());
                    return true;
                }
            }
            0x14 => {
                if let Some(ref blob) = self.battery_info_raw {
                    self.send_to_client(client_id, blob.clone());
                    return true;
                }
            }
            0x1F => {
                let requested_idx = if payload.len() > 1 {
                    payload[1] as i64
                } else {
                    -1
                };
                // `BaseChatMesh::getChannel` refuses every idx >= MAX_GROUP_CHANNELS,
                // so the radio can only answer ERR_CODE_NOT_FOUND for these. Reply
                // it here and keep a doomed request off the single in-flight slot.
                // (An in-range but empty slot is a different case: the radio may
                // still hold a channel the cache has not seen, so that forwards.)
                if requested_idx >= i64::from(self.max_channels) {
                    tracing::debug!(
                        idx = requested_idx,
                        max_channels = self.max_channels,
                        "GET_CHANNEL out of range, replying locally"
                    );
                    self.send_to_client(client_id, vec![0x01, 0x02]);
                    return true;
                }
                if let Ok(channels) = self.state.get_channels().await
                    && let Some(ch) = channels.iter().find(|c| c.idx == requested_idx)
                {
                    self.respond_channel_info(client_id, ch).await;
                    return true;
                }
            }
            _ => {}
        }
        false
    }

    /// Start a replay session for a client on a history listener, the first
    /// time it speaks.
    ///
    /// This runs for every command, not just 0x0A, because `head` marks the
    /// point from which frames already reach this client as live broadcasts.
    /// Snapshotted at the first poll instead, a client that connected and only
    /// synced later would have everything it had already been shown replayed
    /// back to it.
    async fn begin_history_sync(&mut self, cmd: &ClientCommand) {
        if !cmd.listener.replays_history() || self.history_sync.contains_key(&cmd.client_id) {
            return;
        }
        let Some(listener) = cmd.listener.name.as_deref() else {
            return;
        };
        // Nothing logged yet: no history to hand out, so no state to keep.
        // `try_serve_history` picks it up if the log fills in later.
        let Some(head) = self.state.latest_message_id().await.ok().flatten() else {
            return;
        };

        // A named listener is a single-client slot. Two clients on one name
        // share a cursor, so the newcomer silently gets whatever the other one
        // has not consumed. Say so instead of leaving it mysterious. Only live
        // clients count: the reaper runs on the 1 s tick, so a just-departed
        // client may still have an entry.
        //
        // The address matters: when the same device shows up with several
        // client ids it is one app reconnecting in a burst (an auto-reconnect
        // racing a manual Connect tap, typically), not two devices. Naming the
        // existing clients and the peer is what makes that legible.
        let others: Vec<ClientId> = self
            .history_sync
            .iter()
            .filter(|(client, sync)| sync.listener == listener && self.client_is_alive(client))
            .map(|(client, _)| *client)
            .collect();
        if !others.is_empty() {
            tracing::warn!(
                listener = %listener,
                client = %cmd.client_id,
                addr = %cmd.addr,
                existing = ?others,
                "second client on a single-client listener: it shares that \
                 listener's history cursor"
            );
        }

        let cursor = self
            .state
            .history_cursor(listener)
            .await
            .ok()
            .flatten()
            .unwrap_or(0);
        tracing::info!(
            listener = %listener,
            cursor,
            head,
            hours = cmd.listener.history_hours,
            "client syncing message history"
        );
        self.history_sync.insert(
            cmd.client_id,
            HistorySync {
                listener: listener.to_string(),
                cursor,
                head,
                live_high: 0,
                since_ts: history_window_start(cmd.listener.history_hours),
            },
        );
    }

    /// Record that a logged frame reached clients live, so it is not handed to
    /// them a second time by a later replay.
    ///
    /// Called from the delivery sites, not from the log: the proxy's own
    /// synthetic echo skips the sender, and a frame the radio pushes while a
    /// non-0x0A request is in flight goes to one client as its accidental
    /// reply and nobody else. Crediting on the strength of "it was logged"
    /// would quietly drop those frames from the other clients' history.
    fn credit_history_receipts(&mut self, id: i64, exclude: Option<&ClientId>) {
        // Only live clients: the reaper runs on the 1 s tick, so a client that
        // has already gone away still has an entry, and crediting it would
        // drop the frame from the listener's shared cursor for everyone after.
        let alive: Vec<ClientId> = self
            .history_sync
            .keys()
            .filter(|client| self.client_is_alive(client))
            .copied()
            .collect();
        for client in alive {
            if exclude == Some(&client) {
                continue;
            }
            if let Some(sync) = self.history_sync.get_mut(&client) {
                sync.live_high = sync.live_high.max(id);
            }
        }
    }

    /// Write a message frame to the log that history replay reads back.
    ///
    /// Skipped outright when no listener replays history (`log_messages`): the
    /// row could never be served to anybody, and an insert on this path is an
    /// fsync that every client waits for. It is not free either way -- a
    /// one-message-per-poll drain pays it once per poll -- so the frontend-only
    /// deployment, which is the common one, should not pay it at all.
    async fn log_message(&self, msg: &NewMessage<'_>, what: &str) -> Option<i64> {
        if !self.config.log_messages {
            return None;
        }
        match self.state.insert_message(msg).await {
            Ok(id) => Some(id),
            Err(e) => {
                tracing::warn!(error = %e, "failed to cache {what}");
                None
            }
        }
    }

    /// Answer CMD_SYNC_NEXT_MESSAGE (0x0A) from the message log while a client
    /// on a named listener is still catching up.
    ///
    /// Returns true when the command was answered here, in which case it must
    /// not be forwarded: the client gets exactly the one frame per poll the
    /// radio would have replied with, but the single in-flight slot stays free
    /// for real traffic.
    async fn try_serve_history(&mut self, cmd: &ClientCommand) -> bool {
        if !cmd.listener.replays_history() || cmd.payload.first() != Some(&0x0A) {
            return false;
        }

        let Some(sync) = self.history_sync.get(&cmd.client_id) else {
            return false;
        };
        let (cursor, head, since_ts) = (sync.cursor, sync.head, sync.since_ts);
        if cursor >= head {
            return false;
        }

        let frame = match self.state.next_channel_frame(cursor, head, since_ts).await {
            Ok(frame) => frame,
            Err(e) => {
                tracing::warn!(error = %e, "failed to read message history");
                return false;
            }
        };
        let Some(frame) = frame else {
            // Nothing inside the window is left: pruned, or logged before this
            // listener existed. Credit the client and let the radio take over.
            if let Some(sync) = self.history_sync.get_mut(&cmd.client_id) {
                sync.cursor = head;
            }
            self.flush_history_cursor(&cmd.client_id, false).await;
            return false;
        };

        let reached_head = frame.id >= head;
        self.send_to_client(&cmd.client_id, frame.raw);
        if let Some(sync) = self.history_sync.get_mut(&cmd.client_id) {
            sync.cursor = frame.id;
        }
        if reached_head {
            self.flush_history_cursor(&cmd.client_id, false).await;
        }
        true
    }

    /// Write a client's replay cursor back to its listener's kv_store entry.
    ///
    /// A client that finished its replay is credited with everything that
    /// reached it live from then on; replaying those frames after a reconnect
    /// would only duplicate them. A client that died mid-replay keeps its exact
    /// position, so a few frames it already saw may repeat, but none it never
    /// received are skipped.
    async fn flush_history_cursor(&mut self, client_id: &ClientId, forget: bool) {
        let Some(sync) = self.history_sync.get(client_id) else {
            return;
        };
        let listener = sync.listener.clone();
        let value = if sync.cursor >= sync.head {
            sync.cursor.max(sync.live_high)
        } else {
            sync.cursor
        };
        if forget {
            self.history_sync.remove(client_id);
        }
        if let Err(e) = self.state.set_history_cursor(&listener, value).await {
            tracing::warn!(error = %e, listener = %listener, "failed to persist history cursor");
            return;
        }
        tracing::debug!(listener = %listener, cursor = value, "persisted history cursor");
    }

    /// Periodic maintenance on the 1 s tick: retire clients that have gone
    /// away, and sweep expired messages.
    async fn housekeeping(&mut self) {
        // A ghost exists only to swallow a reply the radio still owed. If that
        // reply never comes, holding the ghost would eat a later command's
        // answer instead, so let it go.
        while self
            .ghosts
            .front()
            .is_some_and(|g| g.since.elapsed() >= GHOST_TTL)
        {
            let ghost = self.ghosts.pop_front().expect("checked Some above");
            tracing::warn!(
                cmd = format_args!("0x{:02x}", ghost.cmd_code),
                "no reply ever came for a timed-out request; dropping its ghost"
            );
        }

        let dead: Vec<ClientId> = self
            .history_sync
            .keys()
            .filter(|client| !self.client_is_alive(client))
            .copied()
            .collect();
        for client in dead {
            // The frontend drops disconnected clients from ClientDirectMap
            // without telling the actor, so this is the only disconnect signal
            // a history cursor gets.
            self.flush_history_cursor(&client, true).await;
        }

        if self.last_prune.elapsed() >= PRUNE_INTERVAL {
            self.prune_history().await;
        }
    }

    /// Delete logged messages older than the retention window. A window of
    /// zero hours therefore keeps nothing at all.
    async fn prune_history(&mut self) {
        let before = history_window_start(self.config.message_history_hours);
        match self.state.prune_messages(before).await {
            Ok(0) => {}
            Ok(rows) => tracing::debug!(rows, "pruned message history"),
            Err(e) => tracing::warn!(error = %e, "failed to prune message history"),
        }
        self.last_prune = Instant::now();
    }

    /// Enqueue the command if the radio is busy with a previous request,
    /// otherwise dispatch it immediately.
    ///
    /// A client's request jumps ahead of the entries `sync_from_radio` left in
    /// the queue. That sync is `max_channels` requests long (every channel slot)
    /// and the radio answers one command per loop, so on a slow link it occupies
    /// the queue for seconds; a command left behind all of it is delayed for
    /// every one of them, even though none of it is the work the client asked
    /// for.
    ///
    /// The reads `try_serve_from_cache` can answer are the exception, and they
    /// keep their place. The sync is filling those very cache entries, so a
    /// queued one is normally answered from the freshly warmed cache without
    /// touching the radio at all. Jumping ahead instead would re-read from the
    /// radio what the sync is already reading — which is what a client walking
    /// the channel list on connect would then do, doubling the work.
    async fn enqueue_or_dispatch(&mut self, client_id: Option<ClientId>, payload: Vec<u8>) {
        // A second GET_CONTACTS must never go out while one is streaming. The
        // firmware refuses it (ERR_CODE_BAD_STATE), and that ERROR frame would
        // be ambiguous with the frames still arriving. It waits its turn
        // instead; `advance_queue` keeps it parked until the stream ends.
        let behind_contacts_stream = payload[0] == 0x04 && self.contacts.is_some();
        if self.in_flight.is_none() && !behind_contacts_stream {
            self.dispatch_to_radio(client_id, payload).await;
            return;
        }

        let entry = Queued { client_id, payload };
        if client_id.is_some()
            && !cache_served_command(entry.payload[0])
            && let Some(pos) = self
                .pending_queue
                .iter()
                .position(|queued| queued.client_id.is_none())
        {
            // Before the first internal entry, but after any client already
            // queued: clients keep their own arrival order.
            self.pending_queue.insert(pos, entry);
            return;
        }
        self.pending_queue.push_back(entry);
    }

    /// Forward a command to the radio and set up the slot its reply needs.
    /// Returns false if the forward failed and ERROR was sent to the
    /// originating client. On success performs post-forward side effects
    /// (echo synthesis for SEND_*, cache invalidation for setters).
    async fn dispatch_to_radio(&mut self, client_id: Option<ClientId>, payload: Vec<u8>) -> bool {
        let cmd_code = payload[0];

        if let Err(e) = self.send_to_radio(&payload).await {
            tracing::warn!(error = %e, cmd = format_args!("0x{cmd_code:02x}"), "forward failed");
            if let Some(cid) = client_id {
                self.send_to_client(&cid, vec![0x01, 0x01]);
            }
            return false;
        }

        if let Some(cid) = client_id {
            match cmd_code {
                0x02 if payload.len() >= 14 => {
                    self.synthesize_contact_echo(&cid, &payload).await;
                }
                0x03 if payload.len() >= 8 => {
                    self.synthesize_channel_echo(&cid, &payload).await;
                }
                _ => {}
            }
        }

        match cmd_code {
            0x20 if payload.len() > 1 => {
                let idx = payload[1] as i64;
                let _ = self.state.delete_channel(idx).await;
            }
            // Setters that mutate a cached blob. Clearing it means the next
            // client to ask re-reads from the radio, which is how the cache
            // converges after any write.
            //
            //   SELF_INFO:   0x08 name, 0x0B radio+tuning, 0x0C tx power,
            //                0x0E coords, 0x15 tuning, 0x26 other params,
            //                0x33 factory reset
            //   DEVICE_INFO: 0x25 BLE pin, 0x0B repeat flag, 0x3D path hash
            //                mode, 0x33 factory reset
            //   both:        0x42 run CLI command
            //
            // 0x3D is easy to miss: CMD_SET_PATH_HASH_MODE writes byte 81 of
            // DEVICE_INFO (verified against MeshCore 1.18.0 MyMesh.cpp:1504).
            //
            // 0x42 is the same trap from the other side. MeshCore 1.18.0 grew
            // CMD_RUN_CLI_COMMAND, whose CLI is a second, parallel setter path:
            // `set name`, `set radio`, `set tx`, `set lat`/`set lon`,
            // `set multi.acks`, `set path.hash.mode` and `set pin` now reach the
            // same fields through CommonRadioPrefs::handleCommand instead of the
            // CMD_SET_* handlers, so none of the arms above fire for them.
            // Clearing unconditionally is deliberate: a CLI `get ...` costs one
            // re-read, a CLI `set ...` left cached is served stale forever.
            0x08 | 0x0B | 0x0C | 0x0E | 0x15 | 0x25 | 0x26 | 0x33 | 0x3D | 0x42 => {
                self.self_info_raw = None;
                self.device_info_raw = None;
                self.battery_info_raw = None;
            }
            _ => {}
        }

        if cmd_code == 0x04 {
            // The reply is a stream, not one frame: it gets its own slot so it
            // does not hold the radio hostage for the minutes it can run. The
            // firmware advances the iterator between commands, so normal
            // requests keep flowing beside it.
            debug_assert!(
                self.contacts.is_none(),
                "a second GET_CONTACTS reached the radio while one was streaming"
            );
            self.contacts = Some(ContactsStream {
                frames: Vec::new(),
                subscribers: client_id.into_iter().collect(),
                replayable: true,
                since: contacts_since(&payload),
                last_frame_at: Instant::now(),
            });
            return true;
        }

        self.in_flight = Some(InFlight {
            client_id,
            cmd_code,
            sent_at: Instant::now(),
        });
        true
    }

    /// Pop and dispatch the next queued command for which the originating
    /// client is still connected. Internal sync entries (client_id=None)
    /// are always eligible. Stops at the first one successfully dispatched,
    /// or when the queue empties.
    ///
    /// An open contacts stream does not block this. It runs beside the
    /// in-flight slot, and the only thing it holds back is another
    /// `GET_CONTACTS` (which the radio would refuse anyway).
    async fn advance_queue(&mut self) {
        while let Some(next) = self.pending_queue.pop_front() {
            if let Some(cid) = next.client_id
                && !self.client_is_alive(&cid)
            {
                tracing::debug!(client = %cid, "dropping queued request: client gone");
                continue;
            }
            // A second GET_CONTACTS while one is streaming is refused by the
            // firmware (ERR_CODE_BAD_STATE), and its reply would be ambiguous
            // with the frames still arriving. Park the asker instead; it is
            // served from the completed cache when the stream ends.
            if next.payload[0] == 0x04 && self.contacts.is_some() {
                self.pending_queue.push_front(next);
                return;
            }
            // The cache may have been filled while this waited (a sync reply
            // answered in the meantime), in which case the radio does not need
            // to be touched at all.
            if let Some(cid) = next.client_id
                && self.try_serve_from_cache(&cid, &next.payload).await
            {
                continue;
            }
            if self.in_flight.is_some() {
                // The one normal slot is taken; this must wait its turn.
                self.pending_queue.push_front(next);
                return;
            }
            if !self.dispatch_to_radio(next.client_id, next.payload).await {
                continue; // ERROR already sent; try the next entry
            }
            // Keep draining: the contacts stream lives in its own slot and
            // does not stop the channel sync queued behind it.
        }
    }

    fn client_is_alive(&self, client_id: &ClientId) -> bool {
        self.client_channels.lock().unwrap().contains_key(client_id)
    }

    /// Synthesize a CONTACT_MSG_RECV (0x07) for the rest of the clients so
    /// they see this message as if it had arrived over LoRa from the radio.
    ///
    /// Unlike the channel echo this stays on the legacy frame even though the
    /// proxy pins `app_target_ver` to the V3 generation: the V3 contact form
    /// carries a variable-length sender prefix for signed messages, which this
    /// echo does not model. Contact frames are never replayed, so a mixed
    /// generation in the log costs nothing.
    async fn synthesize_contact_echo(&mut self, sender: &ClientId, payload: &[u8]) {
        let Some(pk) = self.radio_pubkey else {
            tracing::warn!("no radio pubkey known, skipping synthetic broadcast for SEND_MSG");
            return;
        };
        let from_key = &pk[..6];
        let msg_type = payload[1];
        let ts = u32::from_le_bytes(payload[3..7].try_into().unwrap_or([0; 4])) as i64;
        let text = String::from_utf8_lossy(&payload[13..]).to_string();

        let mut fake = Vec::with_capacity(13 + text.len());
        fake.push(0x07);
        fake.extend_from_slice(from_key);
        fake.push(0); // path_len
        fake.push(msg_type); // txt_type
        fake.extend_from_slice(&payload[3..7]); // timestamp
        fake.extend_from_slice(text.as_bytes());
        self.broadcast_to_others(Some(sender), &fake);

        // The sender is excluded from its own echo, so it is credited for
        // every other client but not for this frame.
        if let Some(id) = self
            .log_message(
                &NewMessage {
                    msg_type: "contact",
                    from_key: Some(from_key),
                    channel_idx: None,
                    text: &text,
                    timestamp: ts,
                    raw: &fake,
                    code: 0x07,
                    recv_ts: chrono::Utc::now().timestamp(),
                },
                "contact echo",
            )
            .await
        {
            self.credit_history_receipts(id, Some(sender));
        }
    }

    /// Synthesize a CHANNEL_MSG_RECV_V3 (0x11) for the rest of the clients.
    ///
    /// The V3 form rather than the legacy 0x08, because `app_target_ver` is
    /// pinned to APP_TARGET_VER: the radio only ever emits V3 and clients are
    /// told firmware 13, so an echo in any other generation would be a frame
    /// the radio itself would no longer produce.
    async fn synthesize_channel_echo(&mut self, sender: &ClientId, payload: &[u8]) {
        let Some(pk) = self.radio_pubkey else {
            tracing::warn!("no radio pubkey known, skipping synthetic broadcast for SEND_CHAN_MSG");
            return;
        };
        let from_key = &pk[..6];
        let channel = payload[2] as i64;
        let ts = u32::from_le_bytes(payload[3..7].try_into().unwrap_or([0; 4])) as i64;
        // The radio writes `"<node_name>: "` in front of every channel text it
        // puts on air (BaseChatMesh::sendGroupMessage), and apps recover the
        // sender from that prefix. Echoing the text bare makes a client read the
        // whole message as a sender name with an empty body.
        let text = format!(
            "{}: {}",
            self.radio_name.as_deref().unwrap_or(""),
            String::from_utf8_lossy(&payload[7..])
        );

        let mut fake = Vec::with_capacity(11 + text.len());
        fake.push(0x11);
        fake.push(0); // snr: there was no radio reception behind this message
        fake.push(0); // reserved1
        fake.push(0); // reserved2
        fake.push(payload[2]); // channel
        fake.push(0); // path_len
        fake.push(0); // txt_type = TXT_TYPE_PLAIN
        fake.extend_from_slice(&payload[3..7]); // timestamp
        fake.extend_from_slice(text.as_bytes());
        self.broadcast_to_others(Some(sender), &fake);

        // The sender is excluded from its own echo, so it is credited for
        // every other client but not for this frame: its own message is still
        // replayed to it if it ever syncs the history back.
        if let Some(id) = self
            .log_message(
                &NewMessage {
                    msg_type: "channel",
                    from_key: Some(from_key),
                    channel_idx: Some(channel),
                    text: &text,
                    timestamp: ts,
                    raw: &fake,
                    code: 0x11,
                    recv_ts: chrono::Utc::now().timestamp(),
                },
                "channel echo",
            )
            .await
        {
            self.credit_history_receipts(id, Some(sender));
        }
    }

    /// Called on every timeout tick. Fails the request in flight once it has
    /// gone unanswered for longer than the configured timeout, and an open
    /// contacts reply once its radio has gone quiet for that long.
    ///
    /// They are measured differently because they are shaped differently: the
    /// in-flight request is a single frame, so its age is the only signal,
    /// while a streamed reply is judged by the gap between its frames -- one
    /// frame per radio loop legitimately takes minutes on a repeater with
    /// hundreds of contacts, and a radio that has genuinely stalled says
    /// nothing at all.
    async fn check_in_flight_timeout(&mut self) {
        self.check_contacts_timeout().await;

        let timed_out = self
            .in_flight
            .as_ref()
            .is_some_and(|f| f.sent_at.elapsed() >= self.config.in_flight_timeout);
        if !timed_out {
            return;
        }
        let f = self.in_flight.take().expect("checked Some above");
        let waited_ms = f.sent_at.elapsed().as_millis() as u64;
        if let Some(cid) = f.client_id {
            tracing::warn!(
                client = %cid,
                cmd = format_args!("0x{:02x}", f.cmd_code),
                waited_ms,
                "in_flight timeout, replying ERROR"
            );
            self.send_to_client(&cid, vec![0x01, 0x01]);
        } else {
            tracing::warn!(
                cmd = format_args!("0x{:02x}", f.cmd_code),
                waited_ms,
                "in_flight sync timeout, discarding"
            );
        }
        self.remember_ghost(f);
        self.advance_queue().await;
    }

    /// Fail an open contacts reply whose radio has gone quiet.
    ///
    /// The stream is dropped rather than ghosted: its frames are excluded from
    /// every request's matching (see `is_contacts_frame`), so whatever the
    /// radio still owes arrives as an unclaimed broadcast instead of being
    /// eaten by the next client's request. What must not happen is a second
    /// `GET_CONTACTS` while the radio thinks its iterator is still running, so
    /// the next one waits for `contacts` to clear, which this does.
    async fn check_contacts_timeout(&mut self) {
        let stalled = self
            .contacts
            .as_ref()
            .is_some_and(|s| s.last_frame_at.elapsed() >= self.config.in_flight_timeout);
        if !stalled {
            return;
        }
        let stream = self.contacts.take().expect("checked Some above");
        tracing::warn!(
            frames = stream.frames.len(),
            idle_ms = stream.last_frame_at.elapsed().as_millis() as u64,
            "contacts stream stalled, abandoning it"
        );
        for cid in &stream.subscribers {
            self.send_to_client(cid, vec![0x01, 0x01]);
        }
        self.advance_queue().await;
    }

    /// Keep track of the reply the radio still owes a command we just failed.
    fn remember_ghost(&mut self, f: InFlight) {
        if self.ghosts.len() >= MAX_GHOSTS {
            let oldest = self.ghosts.pop_front().expect("non-empty above");
            tracing::warn!(
                cmd = format_args!("0x{:02x}", oldest.cmd_code),
                "too many unanswered timed-out requests; giving up on the oldest"
            );
        }
        self.ghosts.push_back(Ghost {
            cmd_code: f.cmd_code,
            since: Instant::now(),
        });
    }

    /// Consume a frame that answers a command the proxy already failed.
    ///
    /// After an in-flight timeout the client has had its ERROR, but the radio
    /// still owes exactly one reply for that command and answers in order — so
    /// the next frame to arrive is that reply, not the answer to whatever
    /// request is in flight now. Offering it to the live request would hand one
    /// client another client's answer and then broadcast the displaced reply to
    /// everybody: `is_reply_for` accepts any code but the pushes, and the
    /// stream frames only a lookup can own. The owed reply is dropped instead,
    /// loudly, and the live request keeps waiting for its own.
    ///
    /// The front ghost decides what counts as its reply, because that is not a
    /// property of the code alone: 0x03 answers a 0x1e lookup and belongs to a
    /// stream for anybody else (see `is_reply_for`). A frame the front ghost
    /// cannot own leaves it in place for the command it actually answers.
    ///
    /// Returns true when the frame was accounted for and must not be routed.
    fn try_claim_ghost(&mut self, code: u8, payload: &[u8], logged: Option<i64>) -> bool {
        let cmd_code = match self.ghosts.front() {
            Some(owed) => owed.cmd_code,
            None => return false,
        };
        if !is_reply_for(cmd_code, code) {
            return false;
        }
        let ghost = self.ghosts.pop_front().expect("checked non-empty above");
        let late_ms = ghost.since.elapsed().as_millis() as u64;
        tracing::warn!(
            cmd = format_args!("0x{cmd_code:02x}"),
            reply = format_args!("0x{code:02x}"),
            len = payload.len(),
            late_ms,
            "dropping a reply for a request that already timed out"
        );

        // A poll that timed out can still have popped an inbound message off
        // the radio's queue. The radio hands those out once, so dropping it
        // would lose it for the other clients too: send it down the ordinary
        // broadcast path instead.
        if matches!(code, 0x07 | 0x08 | 0x10 | 0x11 | 0x1B) {
            let _ = self.broadcast_tx.send(payload.to_vec());
            if let Some(id) = logged {
                self.credit_history_receipts(id, None);
            }
        }
        true
    }

    fn send_to_client(&self, client_id: &ClientId, payload: Vec<u8>) {
        let client_tx = self.client_channels.lock().unwrap().get(client_id).cloned();
        if let Some(tx) = client_tx {
            let _ = tx.send(payload);
        }
    }

    fn broadcast_to_others(&self, exclude: Option<&ClientId>, payload: &[u8]) {
        let channels = self.client_channels.lock().unwrap();
        for (id, tx) in channels.iter() {
            if let Some(exclude_id) = exclude
                && id == exclude_id
            {
                continue;
            }
            let _ = tx.send(payload.to_vec());
        }
    }

    fn extract_radio_identity(&mut self, blob: &[u8]) {
        // SELF_INFO layout: [0]=0x05, [1]=adv_type, [2]=tx_power, [3]=max_tx_power,
        // [4..36]=pubkey, ... name at the tail (offset depends on whether sf/cr
        // bytes are present; see decode_response_payload for the spec).
        if blob.len() >= 36 {
            let mut pk = [0u8; 32];
            pk.copy_from_slice(&blob[4..36]);
            self.radio_pubkey = Some(pk);
        }
        if let Some(decoded) = decode_response_payload(0x05, blob)
            && let Some(DecodedValue::String(name)) = decoded.get("name")
        {
            self.radio_name = Some(name.clone());
        }
    }

    fn extract_max_channels(&mut self, blob: &[u8]) {
        if let Some(decoded) = decode_response_payload(0x0D, blob)
            && let Some(DecodedValue::Integer(n)) = decoded.get("max_channels")
        {
            let max = (*n).min(255) as u8;
            if max > 0 {
                self.max_channels = max;
                tracing::info!(
                    max_channels = self.max_channels,
                    "radio reports max channels"
                );
            }
        }
    }

    async fn respond_channel_info(&self, client_id: &ClientId, channel: &CachedChannel) {
        tracing::info!(idx = channel.idx, name = %channel.name, "responding with cached CHANNEL_INFO");
        let mut payload = vec![0x12];
        payload.push(channel.idx as u8);
        let mut name_b = [0u8; 32];
        let nlen = channel.name.len().min(32);
        name_b[..nlen].copy_from_slice(&channel.name.as_bytes()[..nlen]);
        payload.extend_from_slice(&name_b);
        let mut secret_b = [0u8; 16];
        if let Some(ref secret) = channel.config {
            let slen = secret.len().min(16);
            secret_b[..slen].copy_from_slice(&secret[..slen]);
        }
        payload.extend_from_slice(&secret_b);
        self.send_to_client(client_id, payload);
    }

    async fn handle_radio_rx(&mut self, payload: Vec<u8>) {
        if payload.is_empty() {
            return;
        }

        let resp_code = payload[0];
        self.log_event("<-", resp_code, &payload);

        let logged = self.cache_response(resp_code, &payload).await;

        if self.config.record_radio_rx {
            let _ = self.state.insert_raw_rx(resp_code, &payload).await;
        }

        // 1. A frame of the contacts reply that is streaming beside the slot
        //    belongs to that stream, never to the request in flight.
        if self.try_route_contacts_stream(resp_code, &payload).await {
            return;
        }
        // 2. A reply the radio still owed a request the proxy already failed
        //    must not be offered to the live in-flight request below.
        if self.try_claim_ghost(resp_code, &payload, logged) {
            return;
        }
        // 3. If the frame completes the in_flight request, route it to that
        //    client.
        if self
            .try_route_to_in_flight(resp_code, &payload, logged)
            .await
        {
            return;
        }
        // 4. If it's a late ACK matching a previous MSG_SENT, route it.
        if self.try_route_ack(resp_code, &payload) {
            return;
        }
        // 5. Otherwise it's a spontaneous push event from the radio
        //    (MESSAGES_WAITING, ADVERTISEMENT, NEW_ADVERT, ...), which
        //    everybody received.
        let _ = self.broadcast_tx.send(payload);
        if let Some(id) = logged {
            self.credit_history_receipts(id, None);
        }
    }

    /// Match the response against the current in_flight request. Returns
    /// true if it was routed. `logged` is the row id `cache_response` just
    /// assigned, when the frame carried a message at all.
    async fn try_route_to_in_flight(
        &mut self,
        code: u8,
        payload: &[u8],
        logged: Option<i64>,
    ) -> bool {
        let Some(in_flight) = self.in_flight.as_ref() else {
            return false;
        };

        let client_id = in_flight.client_id;
        let cmd_code = in_flight.cmd_code;

        // Push events never complete a request, even with a slot open, and
        // neither does a frame of the contacts stream running beside it --
        // except the 0x03 that answers a 0x1e lookup.
        if !is_reply_for(cmd_code, code) {
            return false;
        }

        let elapsed = in_flight.sent_at.elapsed();
        if elapsed >= SLOW_REPLY_WARN {
            tracing::warn!(
                cmd = format_args!("0x{cmd_code:02x}"),
                reply = format_args!("0x{code:02x}"),
                elapsed_ms = elapsed.as_millis() as u64,
                "slow reply from the radio"
            );
        } else {
            tracing::debug!(
                cmd = format_args!("0x{cmd_code:02x}"),
                reply = format_args!("0x{code:02x}"),
                elapsed_ms = elapsed.as_millis() as u64,
                "radio replied"
            );
        }

        if let Some(cid) = client_id {
            // If this is the MSG_SENT closing a SEND_*, stash the expected_ack
            // so the later 0x82 finds its way back. Lazy GC: purge stale
            // entries first so the map stays bounded (channel sends never
            // produce an ACK, so without this they'd accumulate forever).
            if code == 0x06
                && payload.len() >= 6
                && let Ok(ack) = <[u8; 4]>::try_from(&payload[2..6])
            {
                let now = Instant::now();
                self.pending_acks.retain(|_, (_, inserted_at)| {
                    now.duration_since(*inserted_at) < PENDING_ACK_TTL
                });
                self.pending_acks.insert(ack, (cid, now));
            }

            self.send_to_client(&cid, payload.to_vec());
        }

        // An incoming LoRa message delivered as the reply to a GET_MSG is
        // a global event from the rest of the network's point of view: the
        // radio only hands it out once (it is popped from the offline queue),
        // so every other connected client must be given a copy or it would
        // never see the message at all. The originator already got it through
        // send_to_client above. 0x1B (CHANNEL_DATA_RECV) belongs in this set
        // because it is likewise a CMD_SYNC_NEXT_MESSAGE (0x0A) reply carrying
        // an inbound group datagram.
        if cmd_code == 0x0A && matches!(code, 0x07 | 0x08 | 0x10 | 0x11 | 0x1B) {
            self.broadcast_to_others(client_id.as_ref(), payload);
            // Everybody saw it: the originator as its poll reply, the rest as
            // this broadcast.
            if let Some(id) = logged {
                self.credit_history_receipts(id, None);
            }
        }

        // Every request that reaches this point is a single-frame one; the
        // only multi-frame reply (GET_CONTACTS) is tracked as a stream, not as
        // the in-flight request. So the reply frees the slot.
        self.in_flight = None;
        self.advance_queue().await;
        true
    }

    /// Route a frame of the contacts reply currently streaming, if one is open.
    ///
    /// These frames arrive interleaved with other commands' replies, so they
    /// are claimed here before anything else can look at them. They are also
    /// excluded from the request matcher (`is_contacts_frame`), which is what
    /// keeps the frames of a stream the proxy has given up on from being eaten
    /// by the next client's request.
    async fn try_route_contacts_stream(&mut self, code: u8, payload: &[u8]) -> bool {
        if !is_contacts_frame(code) || self.contacts.is_none() {
            return false;
        }

        let end = code == 0x04;
        let subscribers = {
            let stream = self.contacts.as_mut().expect("checked Some above");
            stream.last_frame_at = Instant::now();
            if !end && stream.replayable {
                let buffered: usize = stream.frames.iter().map(Vec::len).sum();
                if buffered + payload.len() > MAX_CONTACTS_REPLAY_BYTES {
                    // Past the cap a late subscriber can no longer be caught up,
                    // and half a catch-up is worse than none: stop keeping
                    // frames and let later asks wait for the cached list.
                    stream.replayable = false;
                    stream.frames = Vec::new();
                    tracing::warn!(
                        limit = MAX_CONTACTS_REPLAY_BYTES,
                        "contacts reply too large to replay to a late subscriber"
                    );
                } else {
                    stream.frames.push(payload.to_vec());
                }
            }
            stream.subscribers.clone()
        };

        for cid in &subscribers {
            self.send_to_client(cid, payload.to_vec());
        }

        if !end {
            return true;
        }

        // CONTACT_END: this reply is finished. Whether it covered the whole
        // book or a `since` delta is the client's business, not ours -- nothing
        // is stored, so there is no completeness to record.
        let stream = self.contacts.take().expect("checked Some above");
        tracing::info!(
            frames = stream.frames.len(),
            subscribers = subscribers.len(),
            since = stream.since,
            "contacts reply complete"
        );
        // A request parked while this reply was open can go to the radio now.
        self.advance_queue().await;
        true
    }

    /// Attach a client to the contacts reply already streaming, replaying the
    /// frames it missed. Returns false when nothing is streaming, the reply
    /// grew past the replay cap, or the reply does not cover what this client
    /// asked for -- in which case the caller queues the request instead.
    fn attach_to_contacts(&mut self, client_id: &ClientId, client_since: u32) -> bool {
        let (frames, replayable, already_attached, covers) = match self.contacts.as_ref() {
            Some(stream) => (
                stream.frames.clone(),
                stream.replayable,
                stream.subscribers.contains(client_id),
                // A full book (`since == 0`) covers any ask, so the test
                // collapses to a straight watermark comparison.
                stream.since <= client_since,
            ),
            None => return false,
        };
        if !replayable || already_attached || !covers {
            return false;
        }
        if let Some(stream) = self.contacts.as_mut() {
            stream.subscribers.push(*client_id);
        }
        tracing::info!(
            frames = frames.len(),
            "attaching a client to the contacts reply already in progress"
        );
        for frame in frames {
            self.send_to_client(client_id, frame);
        }
        true
    }

    fn try_route_ack(&mut self, code: u8, payload: &[u8]) -> bool {
        if code != 0x82 || payload.len() < 5 {
            return false;
        }
        let Ok(ack) = <[u8; 4]>::try_from(&payload[1..5]) else {
            return false;
        };
        let Some((client_id, _)) = self.pending_acks.remove(&ack) else {
            return false;
        };
        self.send_to_client(&client_id, payload.to_vec());
        true
    }

    async fn cache_response(&mut self, code: u8, payload: &[u8]) -> Option<i64> {
        let recv_ts = chrono::Utc::now().timestamp();
        let mut logged = None;
        match code {
            // RESP_CODE_CONTACT (0x03) and RESP_CODE_ADVERT_PATH etc. are not
            // cached: contacts are multiplexed, not stored (see `Core::contacts`).
            0x05 => {
                let blob = payload.to_vec();
                let _ = self.state.kv_set("self_info_raw", &blob).await;
                self.extract_radio_identity(&blob);
                self.self_info_raw = Some(blob);
            }
            0x0C => {
                let blob = payload.to_vec();
                let _ = self.state.kv_set("battery_info_raw", &blob).await;
                self.battery_info_raw = Some(blob);
            }
            0x0D => {
                let blob = payload.to_vec();
                let _ = self.state.kv_set("device_info_raw", &blob).await;
                self.extract_max_channels(&blob);
                self.device_info_raw = Some(blob);
            }
            0x07 => {
                if let Some(decoded) = decode_response_payload(code, payload) {
                    let text = decoded
                        .get("text")
                        .map(|v| match v {
                            DecodedValue::String(s) => s.clone(),
                            _ => String::new(),
                        })
                        .unwrap_or_default();
                    let from_key = if payload.len() >= 7 {
                        Some(payload[1..7].to_vec())
                    } else {
                        None
                    };
                    let ts = decoded
                        .get("timestamp")
                        .map(|v| match v {
                            DecodedValue::Integer(i) => *i,
                            _ => chrono::Utc::now().timestamp(),
                        })
                        .unwrap_or_else(|| chrono::Utc::now().timestamp());

                    logged = self
                        .log_message(
                            &NewMessage {
                                msg_type: "contact",
                                from_key: from_key.as_deref(),
                                channel_idx: None,
                                text: &text,
                                timestamp: ts,
                                raw: payload,
                                code,
                                recv_ts,
                            },
                            "message",
                        )
                        .await;
                }
            }
            0x08 => {
                if let Some(decoded) = decode_response_payload(code, payload) {
                    let text = decoded
                        .get("text")
                        .map(|v| match v {
                            DecodedValue::String(s) => s.clone(),
                            _ => String::new(),
                        })
                        .unwrap_or_default();
                    let channel_idx = decoded.get("channel").map(|v| match v {
                        DecodedValue::Integer(i) => *i,
                        _ => 0,
                    });
                    let ts = decoded
                        .get("timestamp")
                        .map(|v| match v {
                            DecodedValue::Integer(i) => *i,
                            _ => chrono::Utc::now().timestamp(),
                        })
                        .unwrap_or_else(|| chrono::Utc::now().timestamp());

                    logged = self
                        .log_message(
                            &NewMessage {
                                msg_type: "channel",
                                from_key: None,
                                channel_idx,
                                text: &text,
                                timestamp: ts,
                                raw: payload,
                                code,
                                recv_ts,
                            },
                            "channel message",
                        )
                        .await;
                }
            }
            0x10 if payload.len() >= 13 => {
                let txt_type = payload[11];
                let from_key = Some(payload[4..10].to_vec());
                let ts = u32::from_le_bytes(payload[12..16].try_into().unwrap_or([0; 4])) as i64;
                let text_offset = if txt_type == 2 { 20 } else { 16 };
                let text = if payload.len() > text_offset {
                    String::from_utf8_lossy(&payload[text_offset..]).to_string()
                } else {
                    String::new()
                };
                logged = self
                    .log_message(
                        &NewMessage {
                            msg_type: "contact",
                            from_key: from_key.as_deref(),
                            channel_idx: None,
                            text: &text,
                            timestamp: ts,
                            raw: payload,
                            code,
                            recv_ts,
                        },
                        "V3 contact message",
                    )
                    .await;
            }
            0x11 if payload.len() >= 11 => {
                let channel_idx = payload[4] as i64;
                let ts = u32::from_le_bytes(payload[7..11].try_into().unwrap_or([0; 4])) as i64;
                let text = if payload.len() > 11 {
                    String::from_utf8_lossy(&payload[11..]).to_string()
                } else {
                    String::new()
                };
                logged = self
                    .log_message(
                        &NewMessage {
                            msg_type: "channel",
                            from_key: None,
                            channel_idx: Some(channel_idx),
                            text: &text,
                            timestamp: ts,
                            raw: payload,
                            code,
                            recv_ts,
                        },
                        "V3 channel message",
                    )
                    .await;
            }
            0x12 if payload.len() >= 50 => {
                let idx = payload[1] as i64;
                let name_end = payload[2..34].iter().position(|&b| b == 0).unwrap_or(32);
                let name = String::from_utf8_lossy(&payload[2..2 + name_end]).to_string();
                let secret = payload[34..50].to_vec();
                if let Err(e) = self
                    .state
                    .upsert_channel(&CachedChannel {
                        idx,
                        name,
                        config: Some(secret),
                    })
                    .await
                {
                    tracing::warn!(error = %e, "failed to cache channel info");
                }
            }
            _ => {}
        }
        logged
    }

    fn log_event(&self, arrow: &str, code: u8, payload: &[u8]) {
        if self.config.event_log_level == LogLevel::Off {
            return;
        }

        let (type_name, decoded) = if arrow == "->" {
            let cmd = crate::protocol::types::CommandCode::from_byte(code)
                .map(|c| c.to_string())
                .unwrap_or_else(|| format!("CMD_UNKNOWN(0x{code:02x})"));
            let decoded = decode_command_payload(code, payload);
            (cmd, decoded)
        } else {
            let resp = crate::protocol::types::ResponseCode::from_byte(code)
                .map(|r| r.to_string())
                .unwrap_or_else(|| format!("RESP_UNKNOWN(0x{code:02x})"));
            let decoded = decode_response_payload(code, payload);
            (resp, decoded)
        };

        if self.config.event_log_json {
            use serde::Serialize;
            #[derive(Serialize)]
            struct Event {
                direction: String,
                packet_type: String,
                #[serde(skip_serializing_if = "Option::is_none")]
                decoded: Option<HashMap<String, String>>,
                #[serde(skip_serializing_if = "Option::is_none")]
                payload_hex: Option<String>,
                #[serde(skip_serializing_if = "Option::is_none")]
                payload_len: Option<usize>,
            }

            let decoded_str = decoded.as_ref().map(|d| {
                d.iter()
                    .map(|(k, v)| {
                        let val = match v {
                            DecodedValue::String(s) => s.clone(),
                            DecodedValue::Integer(i) => i.to_string(),
                            DecodedValue::Float(f) => format!("{f:.2}"),
                            DecodedValue::Bool(b) => b.to_string(),
                        };
                        (k.clone(), val)
                    })
                    .collect::<HashMap<_, _>>()
            });

            let event = Event {
                direction: if arrow == "->" {
                    "TO_RADIO".into()
                } else {
                    "FROM_RADIO".into()
                },
                packet_type: type_name,
                decoded: decoded_str,
                payload_hex: if self.config.event_log_level == LogLevel::Verbose {
                    Some(hex::encode(payload))
                } else {
                    None
                },
                payload_len: if self.config.event_log_level == LogLevel::Verbose {
                    Some(payload.len())
                } else {
                    None
                },
            };

            if let Ok(json) = serde_json::to_string(&event) {
                tracing::info!("{json}");
            }
        } else {
            let msg = build_event_msg(
                arrow,
                &type_name,
                decoded.as_ref(),
                payload,
                self.config.event_log_level,
            );
            tracing::info!("{msg}");
        }
    }
}

/// Oldest `recv_ts` a window of `hours` covers, measured on the proxy's own
/// clock. Never on the sender's RTC stamp: those arrive skewed or zeroed.
fn history_window_start(hours: u64) -> i64 {
    chrono::Utc::now().timestamp() - hours as i64 * SECS_PER_HOUR
}

fn sanitize(msg: &str) -> String {
    msg.chars()
        .filter(|&c| c.is_ascii_graphic() || c == ' ')
        .collect()
}

fn build_event_msg(
    arrow: &str,
    type_name: &str,
    decoded: Option<&HashMap<String, DecodedValue>>,
    payload: &[u8],
    level: LogLevel,
) -> String {
    let mut msg = String::with_capacity(64 + payload.len() * 2);
    msg.push_str(arrow);
    msg.push(' ');
    msg.push_str(type_name);
    if let Some(d) = decoded {
        let formatted = format_decoded(d);
        if !formatted.is_empty() {
            msg.push_str(": ");
            msg.push_str(&formatted);
        }
    }
    if level == LogLevel::Verbose {
        use std::fmt::Write;
        let _ = write!(msg, " [{} bytes]: {}", payload.len(), hex::encode(payload));
    }
    sanitize(&msg)
}

fn build_appstart() -> Vec<u8> {
    let app_name = b"coresplitter";
    let mut payload = Vec::with_capacity(3 + app_name.len());
    payload.push(0x01);
    payload.push(1);
    payload.extend_from_slice(app_name);
    payload
}
