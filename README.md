# Core Splitter

[MeshCore](https://meshcore.io) **client multiplexer with cache**. Speaks the
companion protocol on both sides: it connects to a physical radio (serial,
BLE, or TCP) and serves a TCP interface to multiple concurrent companion
clients. Contacts, channels, SELF_INFO, DEVICE_INFO and BATTERY are cached in
SQLite so cold reads don't hit the radio. Every client sees the same state as
the physical radio — the proxy acts as a **faithful mirror**.

## How it works

```
┌────────┐             ┌────────────────┐                    ┌──────────┐
│ Client │◄───────────►│  Core Splitter │◄──────────────────►│ Physical │
│ A      │     TCP     │  (proxy)       │   TCP/serial/BLE   │ Radio    │
├────────┤   0x3c/3e   │                │      meshcore      │ Node     │
│ Client │             │  ┌──────────┐  │                    │          │
│ B      │             │  │  SQLite  │  │                    │          │
└────────┘             │  │  cache   │  │                    │          │
                       │  └──────────┘  │                    │          │
                       └────────────────┘                    └──────────┘
```

1. Proxy connects to the physical radio and syncs contacts and channels
   into SQLite at startup (and again after each reconnect). The sync
   commands are queued through the same FIFO as client commands so their
   responses never interleave with a real client request.
2. Opens a TCP server for companion clients to connect to.
3. Client commands are served from cache when possible (SELF_INFO,
   DEVICE_INFO, BATTERY, GET_CONTACTS, GET_CHANNEL); the rest are
   serialized through a FIFO queue and forwarded one at a time to the
   radio. The radio's reply is routed back to the originating client.
   If the radio disconnects, any in-flight or queued client request is
   failed immediately with ERROR.
4. Outgoing messages (SEND_MSG, SEND_CHAN_MSG): after the radio
   accepts the forward, a synthetic CONTACT_MSG_RECV / CHANNEL_MSG_RECV
   is echoed to the other clients so they see the message as if it
   had arrived over LoRa, and logged for later history replay. If the
   radio is unreachable the originator gets an ERROR and nothing is
   echoed or persisted.
5. Incoming LoRa traffic: the radio pushes MESSAGES_WAITING (0x83) to
   every client; the first one to poll with GET_MSG retrieves the
   frame and the proxy fans it out to the rest, so every client sees
   inbound messages even though only one poll consumed it. This covers
   text messages (0x07/0x08 and the V3 forms 0x10/0x11) and group
   datagrams (CHANNEL_DATA_RECV 0x1B).
6. Push events from the radio (ADVERTISEMENT, NEW_ADVERT, LOG_DATA,
   RAW_DATA, CONTROL_DATA, CONTACT_DELETED, CONTACTS_FULL, ...) go to
   everybody. Late LoRa-delivery ACKs (0x82) are routed back to the
   original sender by `ack_code`.

### Message history for named listeners

The unnamed `--tcp-frontend-port` listener behaves exactly as it always
has: any number of clients, no history. `--listen NAME:PORT[:HOURS]`
adds a **named listener**, and a named listener is a single-client
slot — whoever connects is replayed up to `HOURS` of channel messages
(default `--message-history-hours`, 24) before its normal sync
continues.

```bash
coresplitter \
  --tcp-frontend-port 5000 \
  --listen phone:5001 \
  --listen tablet:5002:6
```

- Replay rides the client's own `CMD_SYNC_NEXT_MESSAGE` (0x0A) polling:
  while a client is catching up, each poll is answered from the message
  log with the exact frame the radio handed out, so replay never
  occupies the single in-flight slot the radio needs for real traffic.
- Once a client has caught up, its polls go to the radio again. A frame
  is only credited to the listener's cursor when it was actually
  delivered to that client, so nothing it saw live is replayed back to
  it, and nothing it missed is skipped.
- The cursor is persisted per listener **name** in `kv_store`, so a
  reconnect — even across a proxy restart — resumes instead of
  replaying. Renaming a listener therefore resets its history, which is
  how you hand a port to a new device or recover a client that lost its
  local store.
- Give every device its own name. Two clients on one name share a
  cursor: the newcomer gets whatever the first has not consumed, and
  the proxy logs a warning when it sees it happen.
- Only channel messages are replayed. Contact frames are logged but not
  replayed, because the V3 contact form carries a variable-length
  sender prefix for signed messages.
- Logged messages older than `--message-history-hours` are pruned about
  once a minute, measured on the proxy's clock rather than the sender's
  (LoRa timestamps come from the sender's RTC and can be skewed or
  zero). `0` keeps no history at all.

### Protocol version negotiation

The radio's `app_target_ver` (set by `CMD_DEVICE_QUERY`) is radio-global
and sticky: it selects whether messages are delivered as the legacy
frames (0x07/0x08) or the V3 frames (0x10/0x11), for every client at
once. Since the proxy multiplexes many clients onto one radio
connection, it claims that negotiation itself — it sends
`CMD_DEVICE_QUERY` with an explicit version at startup (see
`APP_TARGET_VER` in `src/core.rs`) and serves every client's own
`CMD_DEVICE_QUERY` from the cached DEVICE_INFO blob. That way no client
can flip the frame format out from under the others.

## Requirements

- **Rust edition 2024** (requires Rust ≥ 1.85)
- **Linux** (single target)

## Build

The default build excludes BLE support to avoid pulling in system
dependencies (`dbus-1` headers):

```bash
cargo build --release
```

The binary is at `target/release/coresplitter`.

### BLE support

BLE is behind the optional `ble` feature and requires `dbus-1`
development headers:

```bash
# Debian / Ubuntu
sudo apt install libdbus-1-dev pkg-config
cargo build --release --features ble

# Alpine
apk add dbus-dev
cargo build --release --features ble

# Fedora
sudo dnf install dbus-devel pkgconf-pkg-config
cargo build --release --features ble
```

## Usage

### TCP backend (development / production)

```bash
cargo run --release -- \
  --tcp-backend-host 192.168.1.100 \
  --tcp-backend-port 5050 \
  --tcp-frontend-port 5001 \
  --data-dir /var/lib/coresplitter
```

### Serial backend

```bash
cargo run --release -- \
  --serial /dev/ttyUSB0 \
  --baud 115200 \
  --tcp-frontend-port 5001
```

### BLE backend (requires Bluetooth hardware)

Requires building with `--features ble` (see [BLE support](#ble-support)):

```bash
cargo run --release --features ble -- \
  --ble 00:11:22:33:44:55 \
  --ble-pin 123456
```

### Named listeners with message history

```bash
cargo run --release -- \
  --tcp-backend-host 192.168.1.100 \
  --tcp-backend-port 5050 \
  --tcp-frontend-port 5000 \
  --listen phone:5001 \
  --listen tablet:5002:6 \
  --data-dir /var/lib/coresplitter
```

See [Message history for named listeners](#message-history-for-named-listeners).

### Environment variables

All the flags above have `CORESPLITTER_*` equivalents:

| Variable | Description |
|---|---|
| `CORESPLITTER_SERIAL_PORT` | Serial port path |
| `CORESPLITTER_BAUD_RATE` | Baud rate (default: 115200) |
| `CORESPLITTER_BLE_ADDRESS` | BLE device address |
| `CORESPLITTER_BLE_PIN` | BLE pairing PIN (default: 123456) |
| `CORESPLITTER_TCP_BACKEND_HOST` | Radio TCP host |
| `CORESPLITTER_TCP_BACKEND_PORT` | Backend TCP port (default: 5000) |
| `CORESPLITTER_TCP_FRONTEND_HOST` | Frontend TCP bind (default: 0.0.0.0) |
| `CORESPLITTER_TCP_FRONTEND_PORT` | Frontend TCP port (default: 5000) |
| `CORESPLITTER_DATA_DIR` | Data directory (default: ./data) |
| `CORESPLITTER_MESSAGE_HISTORY_HOURS` | Message-history retention in hours (default: 24; 0 keeps none) |
| `CORESPLITTER_LISTEN` | Named listeners, comma-separated `NAME:PORT[:HOURS]` |
| `CORESPLITTER_LOG_LEVEL` | Log level: off, error, warn, info, debug, verbose |
| `CORESPLITTER_LOG_JSON` | Enable JSON event logging |
| `CORESPLITTER_RECORD_RADIO_RX` | Record all raw radio RX to database |

## Test suite

### Integration tests

Uses a fake radio (`tools/fake_radio.py`) and a real proxy deployed on
ephemeral ports. Requires Python 3.12+ with `uv`.

```bash
uv run tools/test_proxy.py
```

All tests run sequentially against one proxy on ephemeral ports, which is
restarted once by the history tests (same data directory, to prove the
replayed cursor survives it).

### Current tests (21 cases, 47 assertions, all PASS)

| # | Test | Description |
|---|------|-------------|
| 1 | Multi-client concurrent | Two clients both receive SELF_INFO; one sends a message, the other receives the broadcast |
| 2 | SET_CHANNEL + persistence | Creates a channel, verifies SQLite persistence after reconnection |
| 3 | Injected channel msg via GET_MSG | Client waits for 0x83, polls with GET_MSG, gets CHANNEL_MSG_RECV (0x08) |
| 4 | Injected contact msg via GET_MSG | Same as #3 for CONTACT_MSG_RECV (0x07) |
| 5 | GET_MESSAGE polling | Client waits for 0x83, sends GET_MESSAGE, receives queued messages |
| 6 | Multiple messages in order | FIFO polling: two consecutive messages have different content |
| 7 | Long message (>133 chars) | Two clients: one sends long text, the other receives the intact broadcast |
| 8 | Contact send (0x02) | Two clients: one sends SEND_MSG, the other receives the raw payload broadcast |
| 9 | GET_MSG reply fans out | Two clients; one polls via GET_MSG, the other sees the same frame via broadcast |
| 10 | TCP reconnection | Kills fake radio, waits for reconnect, verifies GET_CHANNEL works |
| 11 | DEVICE_QUERY negotiation | The proxy pins `app_target_ver` and serves clients' DEVICE_QUERY from cache, so no client re-negotiates the frame format for the others |
| 12 | Unsolicited pushes | 0x84/0x8E/0x8F/0x90 reach an idle client and do not terminate another client's in-flight request |
| 13 | CHANNEL_DATA_RECV fan-out | A 0x1B group datagram polled by one client is fanned out to the rest |
| 14 | Setter cache invalidation | SET_PATH_HASH_MODE (0x3D) invalidates the cached DEVICE_INFO instead of leaving it stale |
| 15 | ACK routing | A late 0x82 ACK reaches its original sender even while another client's request is in flight |
| 16 | Message history replay | A named listener replays the logged channel history, byte-identical to what the radio emitted |
| 17 | History resume | A reconnect on the same listener name starts at the cursor instead of the window start |
| 18 | History across restart | The per-listener cursor survives a proxy restart, so replay resumes rather than repeats |
| 19 | Local sends in history | A channel message sent by one client is replayed to the next client on that listener |
| 20 | Live frames not replayed | A frame logged before the client connected is replayed; one it already saw live is not |
| 21 | Anonymous listener | The unnamed `--tcp-frontend-port` listener never replays history |

### Fake radio

```bash
# TCP mode
uv run tools/fake_radio.py --port 5050

# TCP mode with simulated incoming messages
uv run tools/fake_radio.py --port 5050 --inject

# Serial PTY mode
uv run tools/fake_radio.py --serial
```

`--control-port` exposes a newline-delimited control channel so tests can
drive the radio deterministically instead of racing the periodic
injector (`delay <ms>`, `hold <hexcode>`, `release`, `push <hex>`,
`queue <hex>`, `appver`, `stats`). The suite uses it to pin a request
in-flight while it fires unsolicited pushes, and to assert the
negotiated protocol version.

## Container

A multi-stage `Containerfile` is included:

```bash
docker build -t coresplitter -f Containerfile .
docker run --rm coresplitter --help
```

## Project status

- **TCP backend**: tested against a real radio (Heltec V3, firmware v1.15.0)
- **Serial backend**: implemented, pending hardware test
- **BLE backend**: implemented, pending hardware test
- **Tests**: 47/47 PASS (21 cases) on TCP backend against fake radio
