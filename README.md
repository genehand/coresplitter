# Core Splitter

[MeshCore](https://meshcore.io) **client multiplexer with cache**. Speaks the
companion protocol on both sides: it connects to a physical radio (serial,
BLE, or TCP) and serves a TCP interface to multiple concurrent companion
clients. Channels, SELF_INFO, DEVICE_INFO and BATTERY are cached in SQLite
so cold reads don't hit the radio. Contacts are deliberately not cached: the
radio filters `GET_CONTACTS` on the client's own watermark, so a stored copy
would go stale the moment it could answer a request itself. Every client sees the same state as
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

1. Proxy connects to the physical radio and syncs the channel list into
   SQLite at startup (and again after each reconnect). The sync commands
   are queued through the same FIFO as client commands so their responses
   never interleave with a real client request. A client's request still
   jumps ahead of the entries the sync left queued: the radio answers one
   command per loop, so a request behind the `max_channels`-long sync
   would wait seconds for work it did not ask for. The reads the proxy can
   serve from cache are the exception — they keep their place, because the
   sync is filling those very entries.
2. Opens a TCP server for companion clients to connect to.
3. Client commands are served from cache when possible (SELF_INFO,
   DEVICE_INFO, BATTERY, GET_CHANNEL); the rest are serialized through a
   FIFO queue and forwarded one at a time to the radio. The radio's reply
   is routed back to the originating client. `GET_CHANNEL` past the
   radio's `max_channels` is never forwarded: the firmware's `getChannel`
   refuses every such idx, so the locally-known NOT_FOUND answer keeps an
   app's walk to the end of the channel list off the radio's single
   in-flight slot.
   Contacts are forwarded, never cached: a client's `GET_CONTACTS` (with
   its own `since` watermark) goes to the radio every time, so the radio's
   `lastmod` filter keeps the reply a delta and nothing in the proxy can
   serve a frozen copy. A `GET_CONTACTS` reply is also the one thing that
   runs *beyond* the single slot: the firmware streams it one contact per
   radio loop (minutes on a repeater with hundreds of contacts) while
   still answering the rest of the interface, and refuses only a second
   `GET_CONTACTS`. The proxy keeps that reply beside the in-flight request
   rather than in it, and a client that asks while one is streaming is
   attached to it — replayed the frames it missed — provided the
   streaming reply covers what it asked for: a whole-book stream covers
   any ask, a delta only an ask for the same watermark or older. A client
   that asked for more is queued for its own dump rather than handed a
   reply that silently lacks contacts.
   If the radio disconnects, any in-flight or queued client request is
   failed immediately with ERROR. A request the radio answers too late to
   beat `--in-flight-timeout` is failed the same way, and the reply that
   still arrives for it is discarded rather than handed to whichever
   client is next in line.
4. Outgoing messages (SEND_MSG, SEND_CHAN_MSG): after the radio
   accepts the forward, a synthetic CONTACT_MSG_RECV / CHANNEL_MSG_RECV
   is echoed to the other clients so they see the message as if it
   had arrived over LoRa (channel text is echoed with the radio's
   `"<name>: "` sender prefix, exactly as `BaseChatMesh::sendGroupMessage`
   writes it on air), and logged for later history replay. If the
   radio is unreachable the originator gets an ERROR and nothing is
   echoed or persisted. The echo is a faithful radio reception and
   nothing more: attribution is up to the client, see [Message history
   for named listeners](#message-history-for-named-listeners).
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
  the proxy logs a warning (with the peer address and the client ids
  already on that name) when it sees it happen. The same address
  appearing under several ids is one app reconnecting in a burst — an
  auto-reconnect racing a manual Connect tap — not several devices.
- Only channel messages are replayed. Contact frames are logged but not
  replayed, because the V3 contact form carries a variable-length
  sender prefix for signed messages.
- Nothing is written to the message log unless a listener replays it. The
  insert is a commit on the path that hands the frame to every client
  (~0.35 ms of each poll), and with no named listener no cursor could ever
  read the row back. `--tcp-frontend-port`-only deployments therefore pay
  nothing for history, and the log starts when you add a `--listen`.
- Logged messages older than `--message-history-hours` are pruned about
  once a minute, measured on the proxy's clock rather than the sender's
  (LoRa timestamps come from the sender's RTC and can be skewed or
  zero). `0` keeps no history at all.

#### Limitation for sent messages

The replay carries no "sent by me" marker, and a channel frame has no field
for one. The standard MeshCore client only treats a message as its own when
it was the one that sent it. The proxy is unable to modify this, so a
message sent through one listener will appear as incoming on another.

### Client liveness

A companion talks just to keep the link honest — the reference client
polls battery every 120 s and syncs messages whenever the UI asks. When
a phone suspends (screen lock) its OS may never send a FIN, so the
proxy would otherwise keep serving a half-open socket forever and count
it as a live client on that listener. `--client-idle-timeout` (default
600 s, `0` disables) closes a connection that has sent nothing for that
long, so a reconnecting client takes over a slot instead of stacking on
top of a socket nobody is reading. The sweep is idle-based, not a
connection age cap: a client that keeps polling outlives it.

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
| `CORESPLITTER_IN_FLIGHT_TIMEOUT` | Seconds to wait for the radio's reply to one command before failing it with ERROR (default: 15) |
| `CORESPLITTER_CLIENT_IDLE_TIMEOUT` | Seconds of client silence before the frontend closes the connection (default: 600; 0 disables) |
| `CORESPLITTER_LISTEN` | Named listeners, comma-separated `NAME:PORT[:HOURS]` |
| `CORESPLITTER_LOG_LEVEL` | Log level: off, error, warn, info, debug, verbose |
| `CORESPLITTER_LOG_JSON` | Enable JSON event logging |
| `CORESPLITTER_RECORD_RADIO_RX` | Record all raw radio RX to database |

## Test suite

### Integration tests

Uses a fake radio (`tools/fake_radio.py`) and a real proxy deployed on
ephemeral ports. Requires Python 3.12+ with `uv`; the first run installs
pytest and pytest-asyncio from `pyproject.toml`.

```bash
uv run pytest tools/test_proxy.py                 # whole suite
uv run pytest tools/test_proxy.py -k delta        # one test by keyword
uv run pytest tools/test_proxy.py -x              # stop at the first failure
```

The suite is one asyncio session: all tests share a single fake radio and
proxy on ephemeral ports, started once by a session-scoped fixture. Tests
therefore run in file order and share the radio's state (a channel created in
test 2 is still there in test 20), and the history tests restart the proxy
against the same data directory to prove the replayed cursor survives it.
Selecting a single test with `-k` still starts the full environment, so a
targeted run behaves like a fresh deployment.

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
injector (`delay <ms>`, `hold <hexcode>`, `release`, `stream <ms>`,
`contacts reset`, `push <hex>`, `queue <hex>`, `appver`, `stats`). The suite
uses it to pin a request in-flight while it fires unsolicited pushes, to slow a
contacts reply so a second client's attach can be observed, and to assert the
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
- **Tests**: 30 integration tests (pytest) on TCP backend against fake radio, 9 Rust unit tests
