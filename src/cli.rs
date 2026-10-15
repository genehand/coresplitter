use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Parser;

use crate::core::{BackendType, CoreConfig, LogLevel};
use crate::frontend::tcp::ListenerProfile;

#[derive(Parser, Debug)]
#[command(
    name = "coresplitter",
    version,
    about = "MeshCore client multiplexer with cache"
)]
pub struct Cli {
    /// Serial port path (e.g., /dev/ttyUSB0)
    #[arg(long, short = 's', env = "CORESPLITTER_SERIAL_PORT")]
    pub serial: Option<String>,

    /// Serial baud rate
    #[arg(long, default_value = "115200", env = "CORESPLITTER_BAUD_RATE")]
    pub baud: u32,

    /// BLE device address (MAC or UUID)
    #[arg(long, short = 'b', env = "CORESPLITTER_BLE_ADDRESS")]
    pub ble: Option<String>,

    /// BLE pairing PIN
    #[arg(long, default_value = "123456", env = "CORESPLITTER_BLE_PIN")]
    pub ble_pin: String,

    /// TCP backend host (connect to remote radio/proxy)
    #[arg(long, short = 't', env = "CORESPLITTER_TCP_BACKEND_HOST")]
    pub tcp_backend_host: Option<String>,

    /// TCP backend port
    #[arg(
        long,
        short = 'p',
        default_value_t = 5000,
        env = "CORESPLITTER_TCP_BACKEND_PORT"
    )]
    pub tcp_backend_port: u16,

    /// TCP frontend bind address
    #[arg(
        long,
        default_value = "0.0.0.0",
        env = "CORESPLITTER_TCP_FRONTEND_HOST"
    )]
    pub tcp_frontend_host: String,

    /// TCP frontend port
    #[arg(long, default_value_t = 5000, env = "CORESPLITTER_TCP_FRONTEND_PORT")]
    pub tcp_frontend_port: u16,

    /// Data directory for the state database
    #[arg(long, default_value = "./data", env = "CORESPLITTER_DATA_DIR")]
    pub data_dir: PathBuf,

    /// Total message-history retention in hours. Named listeners replay at most
    /// this far back, the default history depth of a listener that omits its
    /// own, and the age at which logged messages are pruned. 0 keeps none.
    #[arg(long, default_value_t = 24, env = "CORESPLITTER_MESSAGE_HISTORY_HOURS")]
    pub message_history_hours: u64,

    /// How long to wait for the radio to answer one command before failing it
    /// with ERROR. This has to cover the radio's own loop stalls, not just the
    /// round trip: a radio services the companion interface only between its
    /// radio tasks, so a slow preset or a busy repeater can hold a reply back
    /// for seconds after the command itself has already run.
    ///
    /// A streaming reply is measured between its frames instead, because a
    /// `GET_CONTACTS` reply is one frame per radio loop and legitimately runs
    /// for minutes on a repeater with hundreds of contacts -- see
    /// `check_contacts_timeout`.
    #[arg(long, default_value_t = 15, env = "CORESPLITTER_IN_FLIGHT_TIMEOUT")]
    pub in_flight_timeout: u64,

    /// Close a client connection that has sent nothing for this many seconds.
    ///
    /// A companion client talks just to keep the link honest: it polls
    /// battery every 120 s and message sync whenever the UI asks. Silence for
    /// far longer than that means the client is gone, not quiet -- most often
    /// a suspended phone whose OS never sent a FIN, so the frontend would
    /// otherwise keep serving a half-open socket and every reconnect would
    /// stack another "live" client on the same listener. 0 disables the sweep.
    #[arg(long, default_value_t = 600, env = "CORESPLITTER_CLIENT_IDLE_TIMEOUT")]
    pub client_idle_timeout: u64,

    /// Extra named TCP listener with a resumable message history, as
    /// NAME:PORT[:HOURS]. Repeatable, or comma-separated in the environment.
    ///
    /// Each named listener is a single-client slot: clients on the same name
    /// share one history cursor, so give every device its own name. The
    /// unnamed --tcp-frontend-port listener never replays history.
    #[arg(
        long = "listen",
        value_name = "NAME:PORT[:HOURS]",
        env = "CORESPLITTER_LISTEN",
        value_delimiter = ','
    )]
    pub listen: Vec<String>,

    /// Log level: off, error, warn, info, debug, verbose
    #[arg(long, default_value = "info", env = "CORESPLITTER_LOG_LEVEL")]
    pub log_level: String,

    /// JSON event logging
    #[arg(long, env = "CORESPLITTER_LOG_JSON")]
    pub json: bool,

    /// Record all raw radio RX payloads into the raw_rx database table
    #[arg(long, env = "CORESPLITTER_RECORD_RADIO_RX")]
    pub record_radio_rx: bool,
}

impl Cli {
    pub fn into_config(self) -> Result<CoreConfig> {
        let backend_type = if self.serial.is_some() {
            BackendType::Serial
        } else if self.ble.is_some() {
            BackendType::Ble
        } else if self.tcp_backend_host.is_some() {
            BackendType::Tcp
        } else {
            BackendType::Serial
        };

        let event_log_level = match self.log_level.as_str() {
            "off" => LogLevel::Off,
            "verbose" => LogLevel::Verbose,
            _ => LogLevel::Summary,
        };

        let parsed: Vec<(ListenerProfile, u16)> = self
            .listen
            .iter()
            .map(|spec| parse_listener(spec, &self.tcp_frontend_host, self.message_history_hours))
            .collect::<Result<_>>()?;

        // History is keyed by listener name, so a duplicate name would have two
        // listeners fighting over one cursor; a duplicate port means one bind
        // fails at startup anyway, but say so clearly.
        let mut names = HashSet::new();
        let mut ports = HashSet::from([self.tcp_frontend_port]);
        for (profile, port) in &parsed {
            let name = profile.name.clone().unwrap_or_default();
            if !names.insert(name.clone()) {
                bail!(
                    "duplicate listener name '{name}': history is keyed by name, \
                     so two listeners must not share one"
                );
            }
            if !ports.insert(*port) {
                bail!("listener '{name}' reuses TCP port {port}");
            }
        }

        Ok(CoreConfig {
            backend_type,
            serial_port: self.serial,
            serial_baud: self.baud,
            ble_address: self.ble,
            ble_pin: self.ble_pin,
            tcp_backend_host: self.tcp_backend_host,
            tcp_backend_port: self.tcp_backend_port,
            tcp_frontend_host: self.tcp_frontend_host,
            tcp_frontend_port: self.tcp_frontend_port,
            listeners: parsed.into_iter().map(|(profile, _)| profile).collect(),
            message_history_hours: self.message_history_hours,
            in_flight_timeout: Duration::from_secs(self.in_flight_timeout.max(1)),
            client_idle_timeout: Duration::from_secs(self.client_idle_timeout),
            data_dir: self.data_dir,
            event_log_level,
            event_log_json: self.json,
            record_radio_rx: self.record_radio_rx,
        })
    }
}

/// Parse `NAME:PORT[:HOURS]`. `HOURS` defaults to the global retention, which
/// is also its ceiling: a listener may narrow the window, never widen it.
fn parse_listener(spec: &str, host: &str, retention_hours: u64) -> Result<(ListenerProfile, u16)> {
    let mut fields = spec.split(':');
    let name = fields.next().unwrap_or_default().trim();
    let port = fields.next();
    let hours = fields.next();
    if fields.next().is_some() {
        bail!("listener '{spec}': expected NAME:PORT[:HOURS]");
    }
    if name.is_empty() {
        bail!("listener '{spec}': a name is required, it is the history identity");
    }

    let port: u16 = port
        .context("a port is required")?
        .trim()
        .parse()
        .with_context(|| format!("listener '{name}': invalid port"))?;
    let hours = match hours {
        Some(hours) => hours
            .trim()
            .parse()
            .with_context(|| format!("listener '{name}': invalid hour count"))?,
        None => retention_hours,
    };
    if hours > retention_hours {
        bail!(
            "listener '{name}' asks for {hours}h of history but \
             --message-history-hours is {retention_hours}"
        );
    }

    Ok((
        ListenerProfile {
            name: Some(name.to_string()),
            bind_addr: format!("{host}:{port}"),
            history_hours: hours,
        },
        port,
    ))
}
