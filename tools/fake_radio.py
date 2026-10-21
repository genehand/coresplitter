#!/usr/bin/env python3
"""Fake MeshCore radio emulator for development and testing.

Usage:
    uv run tools/fake_radio.py --port 5050           # TCP mode
    uv run tools/fake_radio.py --serial              # Serial PTY mode
    uv run tools/fake_radio.py --port 5050 --inject  # + inject incoming msgs
    uv run tools/fake_radio.py --port 5050 --control-port 5051

Speaks the MeshCore companion protocol (0x3c/0x3e framing). Responds to
commands with realistic canned data. Optionally injects fake incoming
messages on a timer.

The optional --control-port exposes a newline-delimited control channel so
integration tests can drive the radio deterministically instead of racing
the periodic injector:

    delay <ms>       hold every subsequent command response for <ms>
    push <hex>       send a raw payload to every connected backend, unframed
    queue <hex>      enqueue a payload for delivery by the next GET_MSG (0x0A)
    appver           print the negotiated app_target_ver
    stats            print JSON with app_target_ver and per-command counters
"""

import argparse
import asyncio
import collections
import copy
import fcntl
import json
import os
import random
import struct
import sys
import termios
import time
import tty

HOST = "0.0.0.0"
PORT = 5050

# Mirrors MeshCore companion-v1.18.0 (see examples/companion_radio/MyMesh.h).
FIRMWARE_VER_CODE = 14
MAX_GROUP_CHANNELS = 40

# What the proxy is expected to advertise as `app_target_ver`.
EXPECTED_APP_TARGET_VER = 14

# Fixed LoRa-delivery ACK the fake radio hands back in MSG_SENT (RESP_CODE_SENT),
# so tests can later push the matching PUSH_CODE_SEND_CONFIRMED (0x82) frame.
SENT_ACK = bytes.fromhex("deadbeef")

# ---------------------------------------------------------------------------
# Default test data
# ---------------------------------------------------------------------------

DEFAULT_CONTACTS = [
    {
        "key": bytes.fromhex("aabbccdd0011aabbccdd0011aabbccdd0011aabbccdd0011aabbccdd0011aabb"),
        "type": 0,
        "name": "Alpha Test",
        "lat": 43.3623,
        "lon": -8.4115,
        "last_advert": 0,
        "flags": 0x01,
        "path": bytes([0x2A, 0x7F]),
        "lastmod": 1700000100,
    },
    {
        "key": bytes.fromhex("1122334455661122334455661122334455661122334455661122334455661122"),
        "type": 0,
        "name": "Bravo Test",
        "lat": 43.3700,
        "lon": -8.4200,
        "last_advert": 0,
        "flags": 0,
        "path": b"",
        "lastmod": 1700000200,
    },
]

# `contacts reset` restores this, so a test that removes or renames a contact
# does not change what the tests after it see.
CONTACTS_BASELINE = copy.deepcopy(DEFAULT_CONTACTS)

DEFAULT_CHANNELS = [
    {"idx": 0, "name": "Public", "psk": b"\x00" * 16},
    {"idx": 1, "name": "Pruebas", "psk": b"\x00" * 16},
    {"idx": 2, "name": "Dev", "psk": random.randbytes(16)},
]

# ---------------------------------------------------------------------------
# Protocol helpers
# ---------------------------------------------------------------------------

CMD_FRAME = 0x3C
RESP_FRAME = 0x3E


def encode_response(payload: bytes) -> bytes:
    return bytes([RESP_FRAME]) + struct.pack("<H", len(payload)) + payload


def decode_frame(data: bytes):
    """Return (header, payload) or None."""
    if len(data) < 3:
        return None
    hdr = data[0]
    (plen,) = struct.unpack("<H", data[1:3])
    if len(data) < 3 + plen:
        return None
    return hdr, data[3 : 3 + plen]


# ---------------------------------------------------------------------------
# Response builders
# ---------------------------------------------------------------------------

def respond_ok(extra: bytes = b"") -> bytes:
    return encode_response(b"\x00" + extra)


def respond_error(code: int = 1) -> bytes:
    return encode_response(bytes([0x01, code]))


def respond_self_info(node_name: str = "FakeRadio", tx_power: int = 20) -> bytes:
    name = node_name.encode("utf-8")[:32] + b"\x00"
    pk = bytes.fromhex("deadbeefcafebabedeadbeefcafebabedeadbeefcafebabedeadbeefcafebabe")
    lat = int(43.3623 * 1_000_000)
    lon = int(-8.4115 * 1_000_000)
    freq = int(868_175_000 / 1000)
    bw = int(125_000 / 1000)

    payload = bytearray()
    payload.append(0x05)
    payload.append(1)
    payload.append(tx_power)
    payload.append(1)
    payload.extend(pk)
    payload.extend(struct.pack("<i", lat))
    payload.extend(struct.pack("<i", lon))
    payload.extend(b"\x00" * 4)
    payload.extend(struct.pack("<I", freq))
    payload.extend(struct.pack("<I", bw))
    payload.append(12)  # sf
    payload.append(5)   # cr
    payload.extend(name)
    return encode_response(bytes(payload))


def contact_frame(c: dict) -> bytes:
    """A single RESP_CODE_CONTACT (0x03) frame, as `writeContactRespFrame`
    writes it.

    The same frame answers a GET_CONTACTS iterator step and
    CMD_GET_CONTACT_BY_KEY, so both paths must build it identically.
    """
    entry = bytearray()
    entry.append(0x03)
    entry.extend(c["key"])
    entry.append(c["type"])
    entry.append(c.get("flags", 0))
    path = c.get("path", b"")[:64]
    entry.append(len(path))
    entry.extend(path.ljust(64, b"\x00"))
    name_b = c["name"].encode("utf-8")[:32].ljust(32, b"\x00")
    entry.extend(name_b)
    entry.extend(struct.pack("<I", c["last_advert"]))
    entry.extend(struct.pack("<i", int(c["lat"] * 1_000_000)))
    entry.extend(struct.pack("<i", int(c["lon"] * 1_000_000)))
    entry.extend(struct.pack("<I", c["lastmod"]))  # [144..148], the `since` field
    return encode_response(bytes(entry))


def respond_contacts(since: int = 0, contacts: list | None = None) -> list[bytes]:
    """A CONTACT reply, mirroring MyMesh.cpp's ContactsIterator.

    `since` filters on `lastmod`, and the count in CONTACT_START is the total
    number of contacts, NOT the filtered count -- the firmware writes
    `getNumContacts()` there, which is what a client's `since` bookkeeping
    relies on for its own store.
    """
    source = DEFAULT_CONTACTS if contacts is None else contacts
    frames: list[bytes] = []
    n = len(source)
    start = bytearray([0x02])
    start.extend(struct.pack("<I", n))
    frames.append(encode_response(bytes(start)))

    for c in source:
        if c["lastmod"] <= since:
            continue  # the iterator still advances, it just does not send
        frames.append(contact_frame(c))

    end = bytearray([0x04])
    end.extend(struct.pack("<I", max((c["lastmod"] for c in source if c["lastmod"] > since), default=0)))
    frames.append(encode_response(bytes(end)))
    return frames


def respond_device_info(len_ok: bool = True, repeat: int = 0,
                       path_hash_mode: int = 0) -> bytes:
    """DEVICE_INFO (0x0D), exactly 82 bytes.

    Mirrors MeshCore 1.18.0 MyMesh.cpp handleCmdFrame():
      [0]=0x0D  [1]=firmware_ver  [2]=MAX_CONTACTS/2  [3]=MAX_GROUP_CHANNELS
      [4..8]=ble_pin  [8..20]=build_date  [20..60]=manufacturer
      [60..80]=firmware_version  [80]=repeat  [81]=path_hash_mode
    """
    if not len_ok:
        # Real firmware requires `CMD_DEVICE_QUERY && len >= 2` and otherwise
        # falls through to ERR_CODE_UNSUPPORTED_CMD (1).
        return respond_error(1)
    payload = bytearray()
    payload.append(0x0D)
    payload.append(FIRMWARE_VER_CODE)
    payload.append(175)
    payload.append(MAX_GROUP_CHANNELS)
    payload.extend(struct.pack("<I", 0))                 # ble_pin, 4
    payload.extend(b"15 Oct 2026".ljust(12, b"\x00"))    # build_date, 12
    payload.extend(b"Heltec V3".ljust(40, b"\x00"))      # manufacturer, 40
    payload.extend(b"v1.18.0".ljust(20, b"\x00"))        # fw version, 20
    payload.append(repeat & 0xFF)                          # [80]
    payload.append(path_hash_mode & 0xFF)                  # [81]
    assert len(payload) == 82, f"DEVICE_INFO must be 82 bytes, got {len(payload)}"
    return encode_response(bytes(payload))


def respond_channel_info(idx: int, name: str, psk: bytes = b"\x00" * 16) -> bytes:
    payload = bytearray()
    payload.append(0x12)
    payload.append(idx)
    name_b = name.encode("utf-8")[:32].ljust(32, b"\x00")
    payload.extend(name_b)
    secret = psk[:16].ljust(16, b"\x00")
    payload.extend(secret)
    return encode_response(bytes(payload))


def respond_battery() -> bytes:
    payload = bytearray()
    payload.append(0x0C)
    payload.extend(struct.pack("<H", 4100))
    return encode_response(bytes(payload))


def respond_current_time() -> bytes:
    payload = bytearray()
    payload.append(0x09)
    payload.extend(struct.pack("<I", int(time.time())))
    return encode_response(bytes(payload))


def respond_msg_sent() -> bytes:
    """RESP_CODE_SENT (0x06).

    [0]=0x06 [1]=msg_type [2..6]=expected_ack [6..10]=est_timeout
    The proxy keys `pending_acks` on [2..6] so the later 0x82 finds its client.
    """
    payload = bytearray()
    payload.append(0x06)
    payload.append(0)
    payload.extend(SENT_ACK)
    payload.extend(struct.pack("<I", 30000))
    return encode_response(bytes(payload))


def make_send_confirmed(ack: bytes = SENT_ACK, trip_time_ms: int = 123) -> bytes:
    """PUSH_CODE_SEND_CONFIRMED (0x82).

    [0]=0x82 [1..5]=ack [5..9]=trip_time. Only 4 ack bytes: the companion
    protocol docs claim 6, which is the on-air value, not this frame.
    """
    payload = bytearray()
    payload.append(0x82)
    payload.extend(ack[:4].ljust(4, b"\x00"))
    payload.extend(struct.pack("<I", trip_time_ms))
    return encode_response(bytes(payload))


def make_raw_data(data: bytes) -> bytes:
    """PUSH_CODE_RAW_DATA (0x84): inbound raw packet pushed to the app.

    [0]=0x84 [1]=snr [2]=rssi [3]=reserved [4..]=payload
    """
    return encode_response(bytes([0x84, 0x00, 0x00, 0xFF]) + data)


def make_control_data(data: bytes, path_len: int = 0xFF) -> bytes:
    """PUSH_CODE_CONTROL_DATA (0x8E): inbound control packet pushed to the app.

    [0]=0x8E [1]=snr [2]=rssi [3]=path_len [4..]=payload
    """
    return encode_response(bytes([0x8E, 0x00, 0x00, path_len & 0xFF]) + data)


def respond_login() -> bytes:
    return encode_response(b"\x85")


def make_chan_msg(channel: int, text: str, app_ver: int = 0) -> bytes:
    """CHANNEL_MSG_RECV. V3 form when the app negotiated app_target_ver >= 3.

    Legacy 0x08: [0]=code [1]=chan [2]=path_len [3]=txt_type [4..8]=ts [8..]=text
    V3     0x11: [0]=code [1]=snr [2..4]=reserved [4]=chan [5]=path_len
                 [6]=txt_type [7..11]=ts [11..]=text
    """
    payload = bytearray()
    if app_ver >= 3:
        payload.append(0x11)
        payload.append(0)  # snr * 4
        payload.append(0)  # reserved1
        payload.append(0)  # reserved2
        payload.append(channel)
    else:
        payload.append(0x08)
        payload.append(channel)
    payload.append(0)  # path_len
    payload.append(0)  # txt_type
    payload.extend(struct.pack("<I", int(time.time())))
    payload.extend(text.encode("utf-8"))
    return encode_response(bytes(payload))


def make_channel_data(channel: int, data: bytes, data_type: int = 1) -> bytes:
    """CHANNEL_DATA_RECV (0x1B) -- inbound group datagram.

    [0]=0x1B [1]=snr [2..4]=reserved [4]=chan [5]=path_len
    [6..8]=data_type LE [8]=data_len [9..]=data
    """
    payload = bytearray()
    payload.append(0x1B)
    payload.append(0)
    payload.append(0)
    payload.append(0)
    payload.append(channel)
    payload.append(0xFF)  # path_len = 0xFF => not flood-routed
    payload.extend(struct.pack("<H", data_type))
    payload.append(len(data))
    payload.extend(data)
    return encode_response(bytes(payload))


def make_contact_deleted(pub_key: bytes) -> bytes:
    """PUSH_CODE_CONTACT_DELETED (0x8F): 32-byte pubkey follows."""
    return encode_response(b"\x8f" + pub_key[:32].ljust(32, b"\x00"))


def make_contacts_full() -> bytes:
    """PUSH_CODE_CONTACTS_FULL (0x90): payload is just the code."""
    return encode_response(b"\x90")


# ---------------------------------------------------------------------------
# Writer abstraction (TCP stream or serial fd)
# ---------------------------------------------------------------------------

class TcpWriter:
    def __init__(self, writer: asyncio.StreamWriter):
        self._w = writer

    def write(self, data: bytes) -> None:
        self._w.write(data)

    async def drain(self) -> None:
        await self._w.drain()

    def close(self) -> None:
        self._w.close()


class SerialWriter:
    def __init__(self, fd: int):
        self.fd = fd

    def write(self, data: bytes) -> None:
        os.write(self.fd, data)

    async def drain(self) -> None:
        pass

    def close(self) -> None:
        os.close(self.fd)


# ---------------------------------------------------------------------------
# Fake Radio
# ---------------------------------------------------------------------------

def make_contact_msg(from_key: bytes, text: str, app_ver: int = 0) -> bytes:
    """CONTACT_MSG_RECV. V3 form when the app negotiated app_target_ver >= 3.

    Legacy 0x07: [0]=code [1..7]=key [7]=path_len [8]=txt_type
                 [9..13]=ts [13..]=text
    V3     0x10: [0]=code [1]=snr [2..4]=reserved [4..10]=key [10]=path_len
                 [11]=txt_type [12..16]=ts [16..]=text
    """
    payload = bytearray()
    if app_ver >= 3:
        payload.append(0x10)
        payload.append(0)  # snr * 4
        payload.append(0)  # reserved1
        payload.append(0)  # reserved2
        payload.extend(from_key[:6].ljust(6, b"\x00"))
    else:
        payload.append(0x07)
        payload.extend(from_key[:6].ljust(6, b"\x00"))
    payload.append(0)  # path_len
    payload.append(1)  # txt_type
    payload.extend(struct.pack("<I", int(time.time())))
    payload.extend(text.encode("utf-8"))
    return encode_response(bytes(payload))


def make_messages_waiting() -> bytes:
    return encode_response(b"\x83")


class RadioState:
    """State shared by every connection, mirroring the single MyMesh instance
    a real radio has. `app_target_ver` in particular is NOT per-client."""

    def __init__(self):
        self.app_target_ver = 0
        self.delay_ms = 0
        self.repeat = 0
        self.path_hash_mode = 0
        self.node_name = "FakeRadio"
        self.tx_power = 20
        self.held_codes: set[int] = set()
        self.held: list[tuple] = []
        self.cmd_counts: collections.Counter = collections.Counter()
        # The one ContactsIterator a real MeshCore instance has. A second
        # CMD_GET_CONTACTS while it runs is refused the way MyMesh.cpp does.
        self.iter_started = False
        self.iter_task = None
        # `since` of the last CONTACT request, and how many CONTACT frames have
        # gone out: what a test needs to show that a warm sync is a delta.
        self.contacts_since = None
        self.contacts_streamed = 0

    async def maybe_delay(self):
        if self.delay_ms:
            await asyncio.sleep(self.delay_ms / 1000.0)

    def summary(self) -> dict:
        return {
            "app_target_ver": self.app_target_ver,
            "repeat": self.repeat,
            "path_hash_mode": self.path_hash_mode,
            "node_name": self.node_name,
            "tx_power": self.tx_power,
            "held": len(self.held),
            "iter_started": self.iter_started,
            "contacts_since": self.contacts_since,
            "contacts_streamed": self.contacts_streamed,
            "contacts_lastmod_max": max((c["lastmod"] for c in DEFAULT_CONTACTS), default=0),
            "cmd_counts": {f"0x{k:02x}": v for k, v in sorted(self.cmd_counts.items())},
        }


class FakeRadio:
    def __init__(self, inject: bool = False, state: "RadioState | None" = None):
        self.channels: list[dict] = list(DEFAULT_CHANNELS)
        self.inject = inject
        # Shared across connections: the real firmware keeps app_target_ver on
        # the (single) MyMesh instance, not per client.
        self.state = state if state is not None else RadioState()
        # Per-writer message queues (writer fd/id → list of framed messages)
        self._msg_queues: dict[int, list[bytes]] = {}
        self._next_queue_id = 0
        # Every live backend writer, so the control channel can push to all.
        self._writers: list = []

    def _queue_for_writer(self, w) -> int:
        qid = id(w)
        if qid not in self._msg_queues:
            self._msg_queues[qid] = []
        return qid

    def _push_msg(self, w, framed: bytes):
        qid = self._queue_for_writer(w)
        self._msg_queues[qid].append(framed)

    def _pop_msg(self, w) -> bytes | None:
        qid = self._queue_for_writer(w)
        q = self._msg_queues[qid]
        return q.pop(0) if q else None

    # -- TCP handler -------------------------------------------------------

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info("peername")
        print(f"[+] TCP conexión desde {addr}")
        w = TcpWriter(writer)

        inject_task = None
        if self.inject:
            inject_task = asyncio.create_task(self._injector(w))
        self._writers.append(w)

        buf = b""
        try:
            while True:
                data = await reader.read(4096)
                if not data:
                    break

                buf += data
                while True:
                    result = decode_frame(buf)
                    if result is None:
                        break
                    hdr, payload = result
                    buf = buf[3 + len(payload):]
                    if hdr != CMD_FRAME:
                        print(f"  [!] Header inesperado: 0x{hdr:02x}")
                        continue
                    await self._handle_command(payload, w)
        except (ConnectionResetError, asyncio.IncompleteReadError):
            pass
        finally:
            if inject_task:
                inject_task.cancel()
            if w in self._writers:
                self._writers.remove(w)
            w.close()
        print(f"[-] TCP desconectado {addr}")

    async def _stream_contacts(self, w, since: int):
        """Stream a CONTACT reply, one frame per "main-loop iteration".

        MyMesh::checkSerialInterface() advances the contact iterator only on an
        iteration where no command arrived, so a real radio keeps answering the
        companion interface while this runs -- and refuses only a second
        GET_CONTACTS (ERR_CODE_BAD_STATE via `_iter_started`). Writing inside
        the reader loop would model the opposite, so this is its own task.
        """
        try:
            frames = respond_contacts(since)
            w.write(frames[0])  # CONTACT_START goes out as the command runs
            await w.drain()
            for frame in frames[1:]:
                await asyncio.sleep(0)  # yield like a main-loop iteration
                if not self.state.iter_started:
                    return  # APP_START aborted the iterator
                if frame[3] == 0x03:
                    self.state.contacts_streamed += 1
                w.write(frame)
                await w.drain()
        except (ConnectionResetError, asyncio.IncompleteReadError, OSError):
            pass
        finally:
            self.state.iter_started = False
            self.state.iter_task = None

    # -- Serial handler ----------------------------------------------------

    async def run_serial(self):
        master_fd, slave_fd = os.openpty()
        slave_path = os.ttyname(slave_fd)

        fl = fcntl.fcntl(master_fd, fcntl.F_GETFL)
        fcntl.fcntl(master_fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)

        tty.setraw(slave_fd)

        print(f"[+] Serial PTY creado: {slave_path}", flush=True)
        print(f"    Conecta el proxy con: --serial {slave_path}", flush=True)

        w = SerialWriter(master_fd)
        loop = asyncio.get_running_loop()

        inject_task = None
        if self.inject:
            inject_task = asyncio.create_task(self._injector(w))

        buf = bytearray()

        def on_read():
            nonlocal buf
            try:
                data = os.read(master_fd, 4096)
                if not data:
                    print("  [serial] EOF on master")
                    loop.remove_reader(master_fd)
                    return
            except BlockingIOError:
                return
            except OSError as e:
                print(f"  [serial] OSError: {e}")
                loop.remove_reader(master_fd)
                return

            print(f"  [serial] read {len(data)}B: {data.hex()[:60]}", flush=True)
            buf.extend(data)
            while True:
                result = decode_frame(bytes(buf))
                if result is None:
                    break
                hdr, payload = result
                buf = buf[3 + len(payload):]
                if hdr != CMD_FRAME:
                    print(f"  [!] Header inesperado: 0x{hdr:02x}")
                    continue
                asyncio.create_task(self._handle_command(payload, w))

        loop.add_reader(master_fd, on_read)

        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            pass
        finally:
            if inject_task:
                inject_task.cancel()
            loop.remove_reader(master_fd)
            os.close(master_fd)
            os.close(slave_fd)
            print("[-] Serial PTY cerrado")

    # -- Command dispatch --------------------------------------------------

    async def _handle_command(self, payload: bytes, w, count: bool = True):
        if not payload:
            return
        code = payload[0]
        rest = payload[1:]
        cmd_name = CMD_NAMES.get(code, f"0x{code:02x}")
        print(f"  -> {cmd_name} ({len(payload)}B)", flush=True)

        if count:
            self.state.cmd_counts[code] += 1

        # `hold <code>` parks matching commands so a test can keep a request
        # in-flight for as long as it needs, without slowing every other
        # command (which would starve the proxy's own sync queue).
        if code in self.state.held_codes:
            self.state.held.append((payload, w))
            print(f"     (held {cmd_name})", flush=True)
            return

        await self.state.maybe_delay()

        match code:
            case 0x01:
                # MyMesh.cpp stops any left-over ContactsIterator when an app
                # starts a session; a stream in progress has to notice.
                self.state.iter_started = False
                w.write(respond_self_info(self.state.node_name, self.state.tx_power))
                await w.drain()

            case 0x02:
                w.write(respond_msg_sent())
                await w.drain()

            case 0x03:
                # Channel text sends are answered with a plain OK, not MSG_SENT:
                # MyMesh.cpp's CMD_SEND_CHANNEL_TXT_MSG branch calls writeOKFrame()
                # and there is no expected_ack for a group message. A channel
                # that does not exist is ERR_CODE_NOT_FOUND instead, before any
                # packet is made.
                idx = rest[1] if len(rest) > 1 else 0
                if any(c["idx"] == idx for c in self.channels):
                    w.write(respond_ok())
                else:
                    w.write(respond_error(2))
                await w.drain()

            case 0x04:
                # One ContactsIterator at a time, as in MyMesh.cpp: a second
                # CMD_GET_CONTACTS while one is running is refused. `since` is
                # read from an optional trailing u32 and filters on `lastmod`.
                if self.state.iter_started:
                    w.write(respond_error(4))
                else:
                    since = int.from_bytes(rest[:4], "little") if len(rest) >= 4 else 0
                    self.state.contacts_since = since
                    self.state.iter_started = True
                    self.state.iter_task = asyncio.create_task(self._stream_contacts(w, since))
                await w.drain()

            case 0x0F:
                # CMD_REMOVE_CONTACT: a full pubkey, looked up the way
                # MyMesh.cpp's lookupContactByPubKey does.
                if any(c["key"] == bytes(rest[:32]) for c in DEFAULT_CONTACTS):
                    DEFAULT_CONTACTS[:] = [c for c in DEFAULT_CONTACTS if c["key"] != bytes(rest[:32])]
                    w.write(respond_ok())
                else:
                    w.write(respond_error(2))
                await w.drain()

            case 0x05:
                w.write(respond_current_time())
                await w.drain()

            case 0x06:
                w.write(respond_ok())
                await w.drain()

            case 0x07:
                w.write(respond_ok())
                await w.drain()

            case 0x08:
                w.write(respond_ok())
                await w.drain()

            case 0x09:
                # ADD_UPDATE_CONTACT: the CONTACT frame layout minus lastmod,
                # which the radio stamps itself. Mirrors MyMesh.cpp: an
                # existing key is updated in place, a new one appended.
                if len(rest) >= 143:
                    path_len = rest[34]
                    c = {
                        "key": bytes(rest[0:32]),
                        "type": rest[32],
                        "flags": rest[33],
                        "path": bytes(rest[35:35 + min(path_len, 64)]),
                        "name": rest[99:131].rstrip(b"\x00").decode("utf-8", errors="replace"),
                        "last_advert": struct.unpack("<I", rest[131:135])[0],
                        "lat": struct.unpack("<i", rest[135:139])[0] / 1_000_000,
                        "lon": struct.unpack("<i", rest[139:143])[0] / 1_000_000,
                        "lastmod": int(time.time()),
                    }
                    existing = next(
                        (x for x in DEFAULT_CONTACTS if x["key"] == c["key"]), None
                    )
                    if existing:
                        existing.update(c)
                    else:
                        DEFAULT_CONTACTS.append(c)
                    print(f"     -> Contacto actualizado: {c['name']!r}")
                    w.write(respond_ok())
                else:
                    w.write(respond_error(1))
                await w.drain()

            case 0x0D:
                # RESET_PATH: the radio clears the out_path of the contact.
                key = bytes(rest[:32])
                found = next(
                    (x for x in DEFAULT_CONTACTS if x["key"] == key), None
                )
                if found is None:
                    w.write(respond_error(2))
                else:
                    found["path"] = b""
                    found["lastmod"] = int(time.time())
                    w.write(respond_ok())
                await w.drain()

            case 0x0A:
                msg = self._pop_msg(w)
                if msg is not None:
                    w.write(msg)
                else:
                    w.write(encode_response(b"\x0A"))
                await w.drain()

            case 0x1E:
                # CMD_GET_CONTACT_BY_KEY: a full pubkey, looked up the way
                # MyMesh.cpp's lookupContactByPubKey does, and answered with a
                # single RESP_CODE_CONTACT frame -- the very same 0x03 an
                # iterator frame of a GET_CONTACTS reply carries.
                key = bytes(rest[:32])
                found = next((c for c in DEFAULT_CONTACTS if c["key"] == key), None)
                if found is None:
                    w.write(respond_error(2))
                else:
                    w.write(contact_frame(found))
                await w.drain()

            case 0x0B:
                w.write(respond_ok())
                await w.drain()

            case 0x0C:
                w.write(respond_ok())
                await w.drain()

            case 0x0E:
                w.write(respond_ok())
                await w.drain()

            case 0x14:
                w.write(respond_battery())
                await w.drain()

            case 0x16:
                # Real firmware: `cmd_frame[0] == CMD_DEVICE_QUERY && len >= 2`.
                # A 1-byte DEVICE_QUERY falls through to ERR_CODE_UNSUPPORTED_CMD.
                if len(rest) >= 1:
                    self.state.app_target_ver = rest[0]
                    w.write(respond_device_info(
                        len_ok=True,
                        repeat=self.state.repeat,
                        path_hash_mode=self.state.path_hash_mode,
                    ))
                else:
                    w.write(respond_device_info(len_ok=False))
                await w.drain()

            case 0x3D:
                # CMD_SET_PATH_HASH_MODE: [1]=0 (sub-command), [2]=mode.
                # Real firmware rejects modes >= 3 with ERR_CODE_ILLEGAL_ARG.
                if len(rest) >= 2 and rest[0] == 0:
                    if rest[1] >= 3:
                        w.write(respond_error(6))
                    else:
                        self.state.path_hash_mode = rest[1]
                        w.write(respond_ok())
                else:
                    w.write(respond_error(6))
                await w.drain()

            case 0x42:
                # CMD_RUN_CLI_COMMAND (v14+): the firmware runs the text through
                # MyMesh::handleCommand and replies RESP_CODE_CLI_REPLY (0x1D).
                # Mirrors only the subset the tests exercise; it exists so a test
                # can mutate a cached SELF_INFO/DEVICE_INFO field through the CLI
                # path rather than the CMD_SET_* handlers.
                command = rest.decode("utf-8", "replace").strip()
                if command.startswith("set name "):
                    self.state.node_name = command[len("set name "):]
                    reply = b"OK"
                elif command.startswith("set tx "):
                    self.state.tx_power = int(command[len("set tx "):])
                    reply = b"OK"
                elif command == "get name":
                    reply = b"> " + self.state.node_name.encode("utf-8")
                else:
                    reply = b"Unknown command"
                w.write(encode_response(b"\x1d" + reply))
                await w.drain()

            case 0x1A:
                w.write(respond_login())
                await w.drain()

            case 0x1F:
                # Real firmware requires `CMD_GET_CHANNEL && len >= 2` and
                # otherwise falls through to ERR_CODE_UNSUPPORTED_CMD, and
                # BaseChatMesh::getChannel refuses every idx outside
                # [0, MAX_GROUP_CHANNELS) with ERR_CODE_NOT_FOUND. An in-range
                # slot that was never set still answers CHANNEL_INFO, with an
                # empty name.
                if not rest:
                    w.write(respond_error(1))
                elif rest[0] >= MAX_GROUP_CHANNELS:
                    w.write(respond_error(2))
                else:
                    idx = rest[0]
                    ch = next((c for c in self.channels if c["idx"] == idx), None)
                    if ch:
                        w.write(respond_channel_info(idx, ch["name"], ch["psk"]))
                    else:
                        w.write(respond_channel_info(idx, ""))
                await w.drain()

            case 0x20:
                if len(rest) >= 49:
                    idx = rest[0]
                    name = rest[1:33].rstrip(b"\x00").decode("utf-8", errors="replace")
                    psk = rest[33:49]
                    existing = next((c for c in self.channels if c["idx"] == idx), None)
                    if existing:
                        existing["name"] = name
                        existing["psk"] = psk
                    else:
                        self.channels.append({"idx": idx, "name": name, "psk": psk})
                    print(f"     -> Canal {idx}: name={name!r}")
                w.write(respond_ok())
                await w.drain()

            case 0x25:
                w.write(respond_ok())
                await w.drain()

            case 0x33:
                # CMD_FACTORY_RESET requires the literal argument "reset"
                # (`memcmp(&cmd_frame[1], "reset", 5)`); anything else falls
                # through to ERR_CODE_UNSUPPORTED_CMD. Real firmware then wipes
                # the filesystem and reboots -- modelled here as resetting the
                # settings fields, which is all test 30 needs to observe.
                if rest[:5] != b"reset":
                    w.write(respond_error(1))
                else:
                    self.state.node_name = "FakeRadio"
                    self.state.tx_power = 20
                    self.state.repeat = 0
                    self.state.path_hash_mode = 0
                    w.write(respond_ok())
                await w.drain()

            case 0x38:
                w.write(encode_response(b"\x18" + b"\x00" * 20))
                await w.drain()

            case _:
                print(f"     -> comando desconocido, respondiendo ERROR")
                w.write(respond_error(1))
                await w.drain()

    # -- Injection helpers -------------------------------------------------

    async def _injector(self, w):
        # Mirrors what a real MeshCore node does on inbound LoRa traffic:
        # queue the message internally and notify with MESSAGES_WAITING
        # (0x83). The client is expected to retrieve it via GET_MSG (0x0A).
        # No direct push of the 0x07/0x08 frame, which a real radio does
        # not do either.
        items = [
            lambda v: make_chan_msg(0, "Keep alive from fake radio", v),
            lambda v: make_contact_msg(b"\xAA\xBB\xCC\xDD\xEE\xFF", "Hello from Alpha", v),
            lambda v: make_chan_msg(0, "Test message on Public", v),
            lambda v: make_chan_msg(0, "Alerta: temperatura alta", v),
            lambda v: make_chan_msg(0, "A" * 200, v),
        ]
        try:
            n = 0
            while True:
                await asyncio.sleep(6)
                if n >= len(items):
                    n = 0
                framed = items[n](self.state.app_target_ver)
                self._push_msg(w, framed)
                w.write(make_messages_waiting())
                await w.drain()
                print(f"  <- INJECT item[{n}]: queued, sent 0x83")
                n += 1
        except asyncio.CancelledError:
            pass

    async def _delayed_inject(self, w, msg: bytes, delay: float):
        await asyncio.sleep(delay)
        try:
            # Push to GET_MESSAGE queue
            self._push_msg(w, msg)
            # Send as direct push for broadcast/caching
            w.write(msg)
            await w.drain()
            print(f"  <- INJECT (delayed): {msg[3:10].hex()}")
        except Exception:
            pass

    # -- Control channel ---------------------------------------------------

    async def handle_control(self, reader: asyncio.StreamReader,
                             writer: asyncio.StreamWriter):
        """Newline-delimited control channel used by the integration tests.

        Commands:
            delay <ms>   hold subsequent command responses for <ms>
            hold <hex>   park matching command codes instead of replying
            release      dispatch every parked command in arrival order
            contacts reset  restore the built-in contact list
            push <hex>   write a raw payload to every backend, unframed
            queue <hex>  enqueue a payload for the next GET_MSG (0x0A)
            appver       reply with the negotiated app_target_ver
            stats        reply with JSON state (app_target_ver, counters)
        """
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                parts = line.decode(errors="replace").strip().split()
                if not parts:
                    writer.write(b"ok\n")
                    await writer.drain()
                    continue
                verb = parts[0].lower()
                arg = parts[1] if len(parts) > 1 else ""

                if verb == "delay":
                    self.state.delay_ms = int(arg)
                    reply = f"delay={self.state.delay_ms}\n"
                elif verb == "hold":
                    self.state.held_codes.add(int(arg, 16))
                    reply = f"held={sorted(f'0x{c:02x}' for c in self.state.held_codes)}\n"
                elif verb == "release":
                    parked = self.state.held
                    self.state.held = []
                    self.state.held_codes.clear()
                    for hpayload, hw in parked:
                        await self._handle_command(hpayload, hw, count=False)
                    reply = f"released={len(parked)}\n"
                elif verb == "contacts":
                    # `contacts reset` puts the built-in list back: a test that
                    # removes a contact must not change what later tests see.
                    if arg == "reset":
                        DEFAULT_CONTACTS[:] = copy.deepcopy(CONTACTS_BASELINE)
                        reply = f"contacts={len(DEFAULT_CONTACTS)}\n"
                    else:
                        reply = "err usage: contacts reset\n"
                elif verb == "push":
                    payload = bytes.fromhex(arg)
                    n = 0
                    for w in list(self._writers):
                        try:
                            w.write(encode_response(payload))
                            await w.drain()
                            n += 1
                        except Exception:
                            pass
                    reply = f"pushed={n}\n"
                elif verb == "queue":
                    payload = bytes.fromhex(arg)
                    n = 0
                    for w in list(self._writers):
                        self._push_msg(w, encode_response(payload))
                        n += 1
                    reply = f"queued={n}\n"
                elif verb == "appver":
                    reply = f"{self.state.app_target_ver}\n"
                elif verb == "stats":
                    reply = json.dumps(self.state.summary()) + "\n"
                else:
                    reply = "err unknown command\n"

                writer.write(reply.encode())
                await writer.drain()
        except (ConnectionResetError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()


CMD_NAMES = {
    0x01: "AppStart",
    0x02: "SendMsg",
    0x03: "SendChanMsg",
    0x04: "GetContacts",
    0x05: "GetTime",
    0x06: "SetTime",
    0x07: "SendAdvert",
    0x08: "SetName",
    0x09: "UpdateContact",
    0x0A: "GetMsg",
    0x0B: "SetRadio",
    0x0C: "SetTxPower",
    0x0D: "ResetPath",
    0x0E: "SetCoords",
    0x0F: "RemoveContact",
    0x14: "GetBattery",
    0x16: "DeviceQuery",
    0x1A: "SendLogin",
    0x1F: "GetChannel",
    0x20: "SetChannel",
    0x25: "SetDevicePin",
    0x38: "GetStats",
    0x3D: "SetPathHashMode",
    0x3E: "SendChannelData",
    0x41: "SendRawPacket",
    0x42: "RunCliCommand",
}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    ap = argparse.ArgumentParser(description="Fake MeshCore radio for testing")
    ap.add_argument("--port", type=int, default=None, help="TCP port (default: 5050)")
    ap.add_argument("--serial", action="store_true", help="Serial PTY mode instead of TCP")
    ap.add_argument("--inject", action="store_true", help="Inject fake incoming messages")
    ap.add_argument("--control-port", type=int, default=None,
                    help="Expose the newline-delimited test control channel on this port")
    args = ap.parse_args()

    if args.serial:
        radio = FakeRadio(inject=args.inject)
        print("Fake radio en modo SERIAL (PTY)  (inject={})".format("on" if args.inject else "off"))
        print("  Canales:", [c["name"] for c in DEFAULT_CHANNELS])
        print("  Contactos:", [c["name"] for c in DEFAULT_CONTACTS])
        await radio.run_serial()
    else:
        port = args.port or PORT
        radio = FakeRadio(inject=args.inject)
        server = await asyncio.start_server(radio.handle_client, HOST, port)

        addr = ", ".join(str(s.getsockname()) for s in server.sockets)
        print(f"Fake radio escuchando en {addr}  (inject={'on' if args.inject else 'off'})")
        print(f"  Canales: {[c['name'] for c in DEFAULT_CHANNELS]}")
        print(f"  Contactos: {[c['name'] for c in DEFAULT_CONTACTS]}")

        ctl_server = None
        if args.control_port:
            ctl_server = await asyncio.start_server(
                radio.handle_control, HOST, args.control_port
            )
            print(f"  Control en {HOST}:{args.control_port}")

        async with server:
            if ctl_server is not None:
                async with ctl_server:
                    await server.serve_forever()
            else:
                await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
