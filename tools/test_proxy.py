#!/usr/bin/env python3
"""Integration tests for coresplitter proxy."""
import asyncio
import json
import logging
import os
import socket
import sqlite3
import struct
import subprocess
import sys
import time

import pytest
import pytest_asyncio

sys.path.insert(0, os.path.dirname(__file__))

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("test")

# --- Helpers ---

_client_seq = 0

class Client:
    def __init__(self, reader, writer):
        self.r = reader
        self.w = writer

    async def send(self, payload: bytes):
        frame = b"\x3c" + struct.pack("<H", len(payload)) + payload
        self.w.write(frame)
        await self.w.drain()

    async def recv(self, timeout=5.0):
        try:
            hdr = await asyncio.wait_for(self.r.readexactly(3), timeout)
        except asyncio.IncompleteReadError:
            return None
        except asyncio.TimeoutError:
            return b""
        except ConnectionResetError:
            return None
        length = struct.unpack("<H", hdr[1:3])[0]
        payload = await asyncio.wait_for(self.r.readexactly(length), timeout)
        return payload

    async def appstart(self, name: str | None = None):
        """APP_START as the firmware reads it: [0x01, app_ver, reserved(6), name].

        The proxy identifies a client by (local port, app name) to keep its
        message cursor, so every test gets a unique name unless it passes the
        same one on purpose -- otherwise the whole suite would share one
        history.
        """
        global _client_seq
        if name is None:
            _client_seq += 1
            name = f"test-{os.getpid()}-{_client_seq}"
        await self.send(b"\x01\x01" + b"\x00" * 6 + name.encode())
        t0 = time.monotonic()
        while time.monotonic() - t0 < 10:
            p = await self.recv(2)
            if p is None:
                return None
            if len(p) and p[0] == 0x05:
                return p
        return None

    async def get_channel(self, idx: int):
        await self.send(b"\x1f" + bytes([idx]))
        t0 = time.monotonic()
        while time.monotonic() - t0 < 5:
            p = await self.recv(2)
            if p is None:
                return None
            if len(p) and p[0] in (0x12, 0x01):
                return p
        return None

    async def set_channel(self, idx: int, name: str, psk: bytes | None = None):
        name_b = name.encode()[:32].ljust(32, b"\x00")
        if psk is None:
            from hashlib import sha256
            psk = sha256(name.encode()).digest()[:16]
        payload = b"\x20" + bytes([idx]) + name_b + psk
        await self.send(payload)
        t0 = time.monotonic()
        while time.monotonic() - t0 < 5:
            p = await self.recv(1)
            if p is None:
                return None
            if len(p) and p[0] == 0x00:
                return p
        return None

    async def get_message(self):
        await self.send(b"\x0A")
        t0 = time.monotonic()
        while time.monotonic() - t0 < 5:
            p = await self.recv(1)
            if p is None:
                return None
            if len(p) and p[0] in MSG_CODES + (0x0A, 0x1B):
                return p
        return None

    async def get_contact_by_key(self, key: bytes, timeout=5.0):
        """CMD_GET_CONTACT_BY_KEY, answered by RESP_CODE_CONTACT (0x03).

        `MyMesh::handleCmdFrame` answers with `writeContactRespFrame(
        RESP_CODE_CONTACT, *contact)`, or ERR_CODE_NOT_FOUND -- the lookup reply
        is the same frame an iterator sends step by step during a GET_CONTACTS
        dump, which is what makes it easy to mistake for a stream frame.
        """
        await self.send(b"\x1e" + key[:32])
        t0 = time.monotonic()
        while time.monotonic() - t0 < timeout:
            p = await self.recv(1)
            if p is None:
                return None
            if len(p) and p[0] == 0x03:
                return p
        return None

    async def send_msg(self, dst: bytes, text: str, ts: int | None = None):
        if ts is None:
            ts = int(time.time())
        payload = b"\x02\x00\x00" + struct.pack("<I", ts) + dst[:6].ljust(6, b"\x00") + text.encode()
        await self.send(payload)
        t0 = time.monotonic()
        while time.monotonic() - t0 < 5:
            p = await self.recv(1)
            if p is None:
                return None
            if len(p) and p[0] == 0x06:
                return p
        return None

    async def send_chan_msg(self, channel: int, text: str, ts: int | None = None):
        """SEND_CHAN_MSG with the firmware's layout and reply.

        `MyMesh::handleCmdFrame` reads [1]=txt_type, [2]=channel_idx, and
        answers OK (0x00) or ERROR (0x01), never MSG_SENT -- that is the reply
        for a *contact* send only.
        """
        if ts is None:
            ts = int(time.time())
        payload = b"\x03\x00" + bytes([channel]) + struct.pack("<I", ts) + text.encode()
        await self.send(payload)
        t0 = time.monotonic()
        while time.monotonic() - t0 < 5:
            p = await self.recv(1)
            if p is None:
                return None
            if len(p) and p[0] in (0x00, 0x01):
                return p
        return None

    async def get_contacts_raw(self, since=None, timeout=5.0):
        """GET_CONTACTS; returns (start, [contact frames], end) or None."""
        payload = b"\x04" if since is None else b"\x04" + struct.pack("<I", since)
        await self.send(payload)
        start, frames = None, []
        t0 = time.monotonic()
        while time.monotonic() - t0 < timeout:
            p = await self.recv(2)
            if p is None:
                return None
            if len(p) == 0:
                continue
            if p[0] == 0x02:
                start = p
            elif p[0] == 0x03:
                frames.append(p)
            elif p[0] == 0x04:
                return start, frames, p
        return None

    async def close(self):
        self.w.close()

async def connect_client(host="127.0.0.1", port=None) -> Client:
    r, w = await asyncio.wait_for(
        asyncio.open_connection(host, port), timeout=5
    )
    return Client(r, w)


class Control:
    """Client for the fake radio's deterministic test control channel."""

    def __init__(self, reader, writer):
        self.r = reader
        self.w = writer

    async def cmd(self, line: str, timeout=5.0) -> str:
        self.w.write((line + "\n").encode())
        await self.w.drain()
        resp = await asyncio.wait_for(self.r.readline(), timeout)
        return resp.decode("utf-8", "replace").strip()

    async def push(self, payload_hex: str) -> str:
        return await self.cmd(f"push {payload_hex}")

    async def queue(self, payload_hex: str) -> str:
        return await self.cmd(f"queue {payload_hex}")

    async def delay(self, ms: int) -> str:
        return await self.cmd(f"delay {ms}")

    async def hold(self, code: int) -> str:
        """Park a command code so the radio stops replying to it."""
        return await self.cmd(f"hold {code:02x}")

    async def release(self) -> str:
        """Dispatch every parked command in arrival order."""
        return await self.cmd("release")

    async def appver(self) -> int:
        return int(await self.cmd("appver"))

    async def stats(self) -> dict:
        return json.loads(await self.cmd("stats"))

    async def contacts_reset(self) -> str:
        """Restore the fake radio's built-in contact list."""
        return await self.cmd("contacts reset")

    async def close(self):
        self.w.close()


async def connect_control(port) -> Control:
    r, w = await asyncio.wait_for(
        asyncio.open_connection("127.0.0.1", port), timeout=5
    )
    return Control(r, w)


async def wait_for_port(port, timeout=15):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        try:
            r, w = await asyncio.wait_for(
                asyncio.open_connection("127.0.0.1", port), timeout=2
            )
            w.close()
            return True
        except (ConnectionRefusedError, OSError, asyncio.TimeoutError):
            await asyncio.sleep(0.5)
    return False


async def find_free_port():
    s = socket.socket()
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()
    return port


# The radio emits either the legacy message frames or the V3 ones depending on
# the `app_target_ver` negotiated via CMD_DEVICE_QUERY. The proxy pins that
# version (see APP_TARGET_VER in src/core.rs), so tests must accept both forms
# rather than hardcoding the legacy ones.
CONTACT_MSG_CODES = (0x07, 0x10)  # CONTACT_MSG_RECV, CONTACT_MSG_RECV_V3
CHANNEL_MSG_CODES = (0x08, 0x11)  # CHANNEL_MSG_RECV, CHANNEL_MSG_RECV_V3
MSG_CODES = CONTACT_MSG_CODES + CHANNEL_MSG_CODES

# Byte offset of the UTF-8 text in each message frame.
_TEXT_OFFSET = {0x07: 13, 0x10: 16, 0x08: 8, 0x11: 11}

# Mirrors MAX_GROUP_CHANNELS in tools/fake_radio.py, which the radio reports as
# byte 3 of DEVICE_INFO. `BaseChatMesh::getChannel` refuses idx >= this value.
MAX_GROUP_CHANNELS = 40


def msg_text(p: bytes) -> str:
    """Extract the message body from a message frame of either generation."""
    if not p or p[0] not in _TEXT_OFFSET:
        return ""
    return p[_TEXT_OFFSET[p[0]]:].decode("utf-8", errors="replace")


def v3_channel_frame(channel: int, text: bytes, ts: int | None = None) -> bytes:
    """A CHANNEL_MSG_RECV_V3 payload laid out the way the firmware emits it."""
    if ts is None:
        ts = int(time.time())
    return bytes([0x11, 0, 0, 0, channel, 0, 0]) + struct.pack("<I", ts) + text


def is_channel_frame(p: bytes) -> bool:
    return bool(p) and p[0] in CHANNEL_MSG_CODES


# --- Test cases ---

def ok(msg):
    log.info("  PASS: %s", msg)


def fail(msg):
    """Record a failed assertion. Raising is what fails the pytest test."""
    log.error("  FAIL: %s", msg)
    raise AssertionError(msg)


async def test_multi_client(proxy_port):
    """Test 1: two concurrent clients both receive SELF_INFO.

    A message one of them sends is broadcast to the other.
    """
    log.info("--- Test 1: Multi-cliente concurrente ---")
    c1 = await connect_client(port=proxy_port)
    c2 = await connect_client(port=proxy_port)

    r1 = await c1.appstart()
    if r1 is None:
        fail(f"c1 SELF_INFO failed (got: {await c1.recv(200)})")
    r2 = await c2.appstart()
    if r2 is None:
        fail(f"c2 SELF_INFO failed (got: {await c2.recv(200)})")
    if r1 and r2:
        ok("both clients receive SELF_INFO")
    else:
        await c1.close()
        await c2.close()
        return

    # c1 sends SEND_CHAN_MSG to channel 0
    await c1.send_chan_msg(0, "Multi-client test")

    # The OK is consumed by send_chan_msg's drain loop.
    ok("c1 sent message (OK consumed by drain)")

    # c2 should receive a synthesized channel message via broadcast_to_others.
    # Since the proxy pins app_target_ver, the echo is the V3 form (0x11);
    # accept either generation, as everywhere else in this suite.
    got_broadcast = None
    t0 = time.monotonic()
    while time.monotonic() - t0 < 5:
        p = await c2.recv(2)
        if p is None:
            break
        if len(p) == 0:
            continue
        if p[0] in CHANNEL_MSG_CODES and b"Multi-client test" in p:
            got_broadcast = p
            break

    if got_broadcast:
        ok(f"c2 receives the channel echo (0x{got_broadcast[0]:02x}) via broadcast_to_others")
        if got_broadcast[0] == 0x08:
            ok(f"the echo is the legacy frame c2 negotiated: {msg_text(got_broadcast)!r}")
        else:
            fail(
                f"c2 negotiated no version, so the echo must be 0x08, not "
                f"0x{got_broadcast[0]:02x}"
            )
    else:
        fail("c2 did not receive broadcast message")

    await c1.close()
    await c2.close()


async def test_set_channel(proxy_port):
    """Test 2: SET_CHANNEL persists across a reconnection.

    The channel is created once, then read back from SQLite after the client
    reconnects.
    """
    log.info("--- Test 2: SET_CHANNEL + persistence ---")
    c = await connect_client(port=proxy_port)
    r = await c.appstart()
    if not r:
        fail("SELF_INFO failed")
        await c.close()
        return

    r = await c.set_channel(5, "TestChannel")
    if r:
        ok("SET_CHANNEL → ACK (0x00)")
    else:
        fail("SET_CHANNEL did not receive ACK")
        await c.close()
        return

    r = await c.get_channel(5)
    if r and r[0] == 0x12:
        name = r[2:34].rstrip(b"\x00").decode("utf-8", "replace")
        if name == "TestChannel":
            ok("GET_CHANNEL(5) → 'TestChannel'")
        else:
            fail(f"GET_CHANNEL(5) expected 'TestChannel', got {name!r}")
    else:
            fail(f"GET_CHANNEL(5) failed: {r.hex() if r else 'None'}")

    # Reconnect and verify persistence
    log.info("  Reconnecting for persistence...")
    await c.close()
    c = await connect_client(port=proxy_port)
    await c.appstart()
    r = await c.get_channel(5)
    if r and r[0] == 0x12:
        name = r[2:34].rstrip(b"\x00").decode("utf-8", "replace")
        if name == "TestChannel":
            ok("SQLite persistence: GET_CHANNEL(5) → 'TestChannel' after reconnect")
        else:
            fail(f"persistence failed: {name!r}")
    else:
        fail("persistence: GET_CHANNEL failed")

    await c.close()


async def _wait_for_0x83(c, timeout):
    """Wait up to `timeout` seconds for a MESSAGES_WAITING (0x83) on `c`."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        p = await c.recv(2)
        if p is None:
            return False
        if len(p) and p[0] == 0x83:
            return True
    return False


async def test_injected_messages(proxy_port):
    """Test 3: an injected channel message arrives via GET_MSG polling.

    The client waits for MESSAGES_WAITING (0x83), then polls GET_MSG and gets
    CHANNEL_MSG_RECV (0x08).
    """
    log.info("--- Test 3: Injected channel msg via GET_MSG polling ---")
    c = await connect_client(port=proxy_port)
    r = await c.appstart()
    if not r:
        fail("SELF_INFO failed")
        await c.close()
        return

    if not await _wait_for_0x83(c, 10):
        fail("did not receive MESSAGES_WAITING (0x83) in 10s")
        await c.close()
        return

    msg = await c.get_message()
    if msg and msg[0] in CHANNEL_MSG_CODES:
        ok(f"GET_MSG after 0x83 returns CHANNEL_MSG_RECV (0x{msg[0]:02x})")
    else:
        fail(f"GET_MSG returned: {msg.hex() if msg else 'None'}")

    await c.close()


async def test_contact_msg_received(proxy_port):
    """Test 4: as test 3, for an injected contact message.

    Polling GET_MSG after a 0x83 yields CONTACT_MSG_RECV (0x07).
    """
    log.info("--- Test 4: Injected contact msg via GET_MSG polling ---")
    c = await connect_client(port=proxy_port)
    r = await c.appstart()
    if not r:
        fail("SELF_INFO failed")
        await c.close()
        return

    # The injector cycles through items; the contact msg is item 1.
    # Keep polling on every 0x83 until we hit a contact frame (or timeout).
    deadline = time.monotonic() + 30
    got_code = None
    while time.monotonic() < deadline:
        if not await _wait_for_0x83(c, 10):
            break
        msg = await c.get_message()
        if msg and msg[0] in CONTACT_MSG_CODES:
            got_code = msg[0]
            break

    if got_code is not None:
        ok(f"GET_MSG returns CONTACT_MSG_RECV (0x{got_code:02x})")
    else:
        fail("did not retrieve a contact msg via GET_MSG in 30s")

    await c.close()


async def test_get_message_polling(proxy_port):
    """Test 5: GET_MESSAGE (0x0A) polling.

    After a 0x83 tickle, the client polls GET_MESSAGE and receives the queued
    messages.
    """
    log.info("--- Test 5: GET_MESSAGE polling (0x0A) ---")
    c = await connect_client(port=proxy_port)
    r = await c.appstart()
    if not r:
        fail("SELF_INFO failed")
        await c.close()
        return

    # Wait for any 0x83 from the periodic injector (max 6s per cycle)
    t0 = time.monotonic()
    got_83 = False
    while time.monotonic() - t0 < 10:
        p = await c.recv(2)
        if p is None:
            break
        if len(p) and p[0] == 0x83:
            got_83 = True
            break

    if not got_83:
        fail("did not receive MESSAGES_WAITING (0x83) in 10s")
        await c.close()
        return
    ok("0x83 received")

    # Drain queue: GET_MESSAGE until stale injector items are gone, then
    # send one final GET_MESSAGE to verify the protocol works.
    polled = []
    t0 = time.monotonic()
    while time.monotonic() - t0 < 8:
        p = await c.get_message()
        if p is None:
            break
        if p[0] == 0x0A:
            break
        polled.append(p)
    # We should have polled at least one real message
    if polled:
        ok(f"GET_MESSAGE returned {len(polled)} messages (last 0x{polled[-1][0]:02x})")
    else:
        fail("GET_MESSAGE did not produce any real messages")

    await c.close()


async def test_multiple_messages_order(proxy_port):
    """Test 6: FIFO polling order.

    Two consecutive messages polled from the queue come back in order and have
    different content.
    """
    log.info("--- Test 6: Multiple messages in order ---")
    c = await connect_client(port=proxy_port)
    r = await c.appstart()
    if not r:
        fail("SELF_INFO failed")
        await c.close()
        return

    # Collect two consecutive messages via polling to verify ordering
    msgs = []
    t0 = time.monotonic()
    while len(msgs) < 2 and time.monotonic() - t0 < 20:
        p = await c.recv(3)
        if p is None or len(p) == 0:
            continue
        if p[0] == 0x83:
            m = await c.get_message()
            if m and m[0] in MSG_CODES:
                msgs.append(m)
    # After receiving two messages via push + poll, verify they differ
    if len(msgs) >= 2:
        txt0 = msg_text(msgs[0])
        txt1 = msg_text(msgs[1])
        if txt0 and txt1 and txt0 != txt1:
            ok(f"two distinct messages in order: {txt0[:20]!r} ≠ {txt1[:20]!r}")
        else:
            fail(f"messages equal or empty: {txt0[:20]!r} vs {txt1[:20]!r}")
    else:
        fail(f"only got {len(msgs)} messages in 20s")

    await c.close()


async def test_long_message(proxy_port):
    """Test 7: a long message keeps the layout the radio puts on air.

    One client sends the long text; the other receives it prefixed with the
    node name and cut at MAX_TEXT_LEN (160) bytes, exactly as the rest of
    the mesh would.
    """
    log.info("--- Test 7: Long message (>133 chars) ---")
    c1 = await connect_client(port=proxy_port)
    c2 = await connect_client(port=proxy_port)
    r1 = await c1.appstart()
    r2 = await c2.appstart()
    if not r1 or not r2:
        fail("SELF_INFO failed")
        await c1.close()
        await c2.close()
        return

    long_text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat."
    assert len(long_text) > 133

    await c1.send_chan_msg(0, long_text)

    t0 = time.monotonic()
    received = False
    while time.monotonic() - t0 < 6:
        p = await c2.recv(2)
        if p is None or len(p) == 0:
            continue
        if p[0] in CHANNEL_MSG_CODES:
            txt_rx = msg_text(p)
            if "Lorem" in txt_rx or len(txt_rx) > 100:
                received = True
                break

    # The rest of the mesh gets "<node name>: <text>", cut at 160 bytes.
    expected = ("FakeRadio: " + long_text)[:160]
    if not received:
        fail("c2 did not receive long message broadcast in 6s")
    elif txt_rx == expected:
        ok(f"c2 receives the channel echo as sent on air ({len(txt_rx)} chars)")
    else:
        fail(f"c2 got {txt_rx!r}, expected {expected!r}")

    await c1.close()
    await c2.close()


async def test_send_contact_msg(proxy_port):
    """Test 8: contact send (0x02).

    One client sends SEND_MSG and gets MSG_SENT; the other receives the raw
    payload by broadcast.
    """
    log.info("--- Test 8: Send contact message (0x02) ---")
    c1 = await connect_client(port=proxy_port)
    c2 = await connect_client(port=proxy_port)
    r1 = await c1.appstart()
    r2 = await c2.appstart()
    if not r1 or not r2:
        fail("SELF_INFO failed")
        await c1.close()
        await c2.close()
        return

    dst = bytes.fromhex("aabbccddeeff")
    r = await c1.send_msg(dst, "Hello contact")
    if not r:
        fail("SEND_MSG did not return MSG_SENT")
        await c1.close()
        await c2.close()
        return
    ok("c1: SEND_MSG → MSG_SENT (0x06)")

    t0 = time.monotonic()
    got_broadcast = False
    while time.monotonic() - t0 < 6:
        p = await c2.recv(2)
        if p is None or len(p) == 0:
            continue
        if p[0] in CONTACT_MSG_CODES and b"Hello contact" in p:
            got_broadcast = True
            break

    if got_broadcast:
        ok("c2 receives CONTACT_MSG_RECV via broadcast_to_others with correct text")
    else:
        fail("c2 did not receive 0x07 broadcast with expected text")

    await c1.close()
    await c2.close()


async def test_reconnect_tcp(proxy_port):
    """Test 9: the proxy drops its clients, then reconnects to the radio.

    MeshCore has no frame that says "device unavailable", so while the radio is
    gone the frontend behaves like an absent node: it stops listening and closes
    what it was serving. Once the radio is back on the same port it rebinds and
    serves again.
    """
    global _fake_proc, _fake_pid, _fake_port
    log.info("--- Test 10: TCP disconnection + reconnection ---")
    c = await connect_client(port=proxy_port)
    r = await c.appstart()
    if not r:
        await c.close()
        fail("SELF_INFO failed")
        return

    if _fake_proc and _fake_proc.returncode is None:
        log.info("  Killing fake radio (PID=%d)...", _fake_pid)
        _fake_proc.terminate()
        try:
            await asyncio.wait_for(_fake_proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            _fake_proc.kill()
    _fake_proc = None

    closed = False
    t0 = time.monotonic()
    while time.monotonic() - t0 < 5:
        p = await c.recv(1)
        if p is None:
            closed = True
            break
    await c.close()
    if closed:
        ok("the client connection is closed when the radio drops")
    else:
        fail("the client connection is still open after the radio dropped")

    try:
        probe = await connect_client(port=proxy_port)
        await probe.close()
        fail("the proxy accepted a connection with no radio attached")
        return
    except (ConnectionRefusedError, OSError):
        ok("the proxy refuses connections while the radio is down")

    log.info("  Restarting fake radio...")
    await start_fake(port=_fake_port)
    if not _fake_proc:
        fail("could not restart fake radio")
        return

    # Backoff is 1 s, 2 s, 4 s... give the proxy time to come back.
    if not await wait_for_port(proxy_port, timeout=20):
        fail("the proxy did not reopen the frontend after reconnecting")
        return

    c = await connect_client(port=proxy_port)
    r = await c.get_channel(0)
    await c.close()
    if r and r[0] == 0x12:
        ok("the proxy responds to GET_CHANNEL after reconnecting")
    else:
        fail(f"the proxy does not respond after reconnecting (r={r})")


async def test_get_msg_fans_out(proxy_port):
    """Test 10: a GET_MSG reply fans out to the other clients.

    GET_MSG pops from the radio's single offline queue, so a reply delivered to
    one client must be broadcast to everyone: one client polls, the other sees
    the same frame.
    """
    log.info("--- Test 9: GET_MSG reply fans out to other clients ---")
    c1 = await connect_client(port=proxy_port)
    c2 = await connect_client(port=proxy_port)

    r1 = await c1.appstart()
    r2 = await c2.appstart()
    if not r1 or not r2:
        fail("SELF_INFO failed for one or both clients")
        await c1.close()
        await c2.close()
        return

    # Wait for an injector 0x83 on c1 (c2 sees it too but we don't act on it).
    if not await _wait_for_0x83(c1, 10):
        fail("c1 did not receive MESSAGES_WAITING (0x83) in 10s")
        await c1.close()
        await c2.close()
        return

    # c1 polls. Expect a real message (not NO_MORE_MSGS).
    msg_c1 = await c1.get_message()
    if not msg_c1 or msg_c1[0] not in MSG_CODES:
        fail(f"c1 GET_MSG returned: {msg_c1.hex() if msg_c1 else 'None'}")
        await c1.close()
        await c2.close()
        return
    ok(f"c1 GET_MSG → 0x{msg_c1[0]:02x}")

    # c2 should see the same message via fan-out broadcast (no GET_MSG of
    # its own). Drain c2 until we find a matching 0x07/0x08. Need a
    # generous window because c2 will also see broadcast 0x83 from the
    # injector and we have to drain past those.
    got_fanout = None
    seen = []
    t0 = time.monotonic()
    while time.monotonic() - t0 < 10:
        p = await c2.recv(1)
        if p is None:
            break
        if len(p) == 0:
            continue
        seen.append(f"0x{p[0]:02x}")
        if p[0] in MSG_CODES:
            got_fanout = p
            break
    log.info("  c2 saw frames: %s", seen)

    if got_fanout is None:
        fail("c2 did not receive the message via fan-out")
    elif got_fanout == msg_c1:
        ok("c2 receives the same frame via fan-out broadcast")
    else:
        fail(
            f"c2 received a frame but bytes differ:\n"
            f"  c1: {msg_c1.hex()}\n  c2: {got_fanout.hex()}"
        )

    await c1.close()
    await c2.close()


async def test_unsolicited_pushes_do_not_terminate_requests(proxy_port, control_port):
    """Test 11: unsolicited pushes arriving during an in-flight request.

    Covers CONTACT_DELETED (0x8F), CONTACTS_FULL (0x90), RAW_DATA (0x84) and
    CONTROL_DATA (0x8E) -- all emitted by the firmware from inbound-traffic
    handlers with no corresponding command (the outbound CMD_SEND_RAW_DATA and
    CMD_SEND_CONTROL_DATA are answered with a plain OK) -- plus BINARY_RESPONSE
    (0x8C) and PATH_DISCOVERY_RESPONSE (0x8D).

    0x8C/0x8D are tag-correlated, but the request that triggers them
    (CMD_SEND_BINARY_REQ 0x32, CMD_SEND_ANON_REQ 0x39,
    CMD_SEND_PATH_DISCOVERY_REQ 0x34) is answered *immediately* with
    RESP_CODE_SENT (0x06) carrying the tag; the 0x8C/0x8D arrives later, once the
    in-flight slot is long free, and the app matches it by tag. So they belong in
    PUSH_EVENT_CODES too. 1.18.0 made a stray 0x8C routine by emitting one from
    `onContactResponse` for any contact response that matches no pending request.

    `Awaiting::Any` completes a request on any frame not listed in
    PUSH_EVENT_CODES, so a missing code here is delivered only to the one client
    that happens to be waiting: every other client silently loses the push, and
    the real reply later finds no in-flight slot and is broadcast to clients
    that never sent the request.

    The idle second client is what makes this observable: it must receive all
    six pushes, and must NOT receive the reply to c1's request.
    """
    log.info("--- Test 12: unsolicited pushes do not terminate an in-flight request ---")
    ctl = await connect_control(control_port)

    c1 = await connect_client(port=proxy_port)
    c2 = await connect_client(port=proxy_port)
    await c1.appstart()
    await c2.appstart()

    # Park the reply to CMD_GET_DEVICE_TIME so c1's request stays in flight for
    # exactly as long as this test needs, then fire the unsolicited pushes.
    await ctl.hold(0x05)
    await c1.send(b"\x05")
    await asyncio.sleep(0.4)  # let it reach the radio and go in-flight
    await ctl.push("8f" + "ab" * 32)                          # CONTACT_DELETED
    await ctl.push("90")                                       # CONTACTS_FULL
    await ctl.push("84" + "0000ff" + b"hi".hex())              # RAW_DATA
    await ctl.push("8e" + "0000ff" + b"hi".hex())              # CONTROL_DATA
    await ctl.push("8c" + "00" + "efbeadde" + b"hi".hex())     # BINARY_RESPONSE
    await ctl.push("8d" + "00" + "efbeadde" + b"hi".hex())     # PATH_DISCOVERY_RESPONSE

    expected = {
        0x8F: "0x8f",
        0x90: "0x90",
        0x84: "0x84",
        0x8E: "0x8e",
        0x8C: "0x8c",
        0x8D: "0x8d",
    }

    async def collect(client, want, window):
        out = []
        t0 = time.monotonic()
        while time.monotonic() - t0 < window:
            p = await client.recv(1)
            if p is None:
                break
            if not len(p):
                continue
            out.append(p[0])
            if p[0] in want:
                return out
        return out

    # c2 sent nothing, so it must still see every push a moment after they fire.
    seen2 = set()
    t0 = time.monotonic()
    while time.monotonic() - t0 < 5 and len(seen2) < len(expected):
        p = await c2.recv(1)
        if p is None:
            break
        if not len(p):
            continue
        seen2.add(p[0])
    log.info("  idle client c2 saw: %s", sorted(f"0x{b:02x}" for b in seen2))

    missing = [label for code, label in expected.items() if code not in seen2]
    if not missing:
        ok(f"idle c2 received all {len(expected)} unsolicited pushes as broadcasts")
    else:
        fail(f"idle c2 missed {missing}; a push was consumed as a reply")

    # c1 must still get the real reply once the radio is allowed to answer.
    await ctl.release()
    seen1 = await collect(c1, {0x09}, 8)
    log.info("  requesting client c1 saw: %s", [f"0x{b:02x}" for b in seen1])
    if 0x09 in seen1:
        ok("the in-flight GET_DEVICE_TIME still resolved with CURR_TIME (0x09)")
    else:
        fail("in-flight request never resolved with CURR_TIME (0x09)")

    # c2 must not receive a reply to a request it never made.
    stray = await collect(c2, {0x09}, 2.0)
    if 0x09 not in stray:
        ok("c2 did not receive c1's CURR_TIME reply")
    else:
        fail("c1's reply was broadcast to c2 because the push freed the in-flight slot")

    await c1.close()
    await c2.close()
    await ctl.close()


async def test_channel_data_fans_out(proxy_port, control_port):
    """Test 12: CHANNEL_DATA_RECV (0x1B) fans out to the other clients.

    A group datagram is delivered by CMD_SYNC_NEXT_MESSAGE (0x0A) exactly like a
    text message, so only the polling client used to see it -- every other client
    silently lost the datagram.
    """
    log.info("--- Test 13: CHANNEL_DATA_RECV (0x1B) fans out ---")
    ctl = await connect_control(control_port)
    c1 = await connect_client(port=proxy_port)
    c2 = await connect_client(port=proxy_port)
    if not await c1.appstart() or not await c2.appstart():
        fail("SELF_INFO failed")
        await c1.close()
        await c2.close()
        await ctl.close()
        return

    # Build a 0x1B frame: code, snr, 2 reserved, channel, path_len,
    # data_type (LE u16), data_len, data.
    body = b"\x1b\x00\x00\x00\x00\xff\x01\x00\x05hello"
    await ctl.queue(body.hex())
    await ctl.push("83")  # MESSAGES_WAITING tickle
    await asyncio.sleep(0.2)

    # The periodic injector may have queued messages ahead of ours, so drain
    # GET_MSG until the 0x1B datagram surfaces.
    got_c1 = None
    for _ in range(12):
        p = await c1.get_message()
        if p is None:
            break
        if p[0] == 0x1B:
            got_c1 = p
            break

    if got_c1 is not None:
        ok("c1 GET_MSG returned CHANNEL_DATA_RECV (0x1B)")
    else:
        fail("c1 never retrieved the queued 0x1B datagram")

    got_c2 = None
    seen = []
    t0 = time.monotonic()
    while time.monotonic() - t0 < 8:
        p = await c2.recv(1)
        if p is None:
            break
        if not len(p):
            continue
        seen.append(f"0x{p[0]:02x}")
        if p[0] == 0x1B:
            got_c2 = p
            break
    log.info("  c2 saw: %s", seen)

    if got_c2 is None:
        fail("c2 never received the 0x1B datagram via fan-out")
    elif got_c1 is not None and got_c2 == got_c1:
        ok("c2 receives the identical 0x1B frame via fan-out")
    else:
        fail("c2 received a 0x1B frame but the bytes differ from c1's")

    await c1.close()
    await c2.close()
    await ctl.close()


async def test_setter_invalidates_device_info(proxy_port, control_port):
    """Test 13: a setter that mutates DEVICE_INFO invalidates the cache.

    CMD_SET_PATH_HASH_MODE (0x3D) writes byte 81 of DEVICE_INFO. It is easy to
    overlook because it is not one of the obvious "settings" commands, and if it
    is missing from the invalidation list in `dispatch_to_radio` every client
    keeps reading the stale cached blob forever.
    """
    log.info("--- Test 14: SET_PATH_HASH_MODE invalidates cached DEVICE_INFO ---")
    ctl = await connect_control(control_port)
    c = await connect_client(port=proxy_port)
    await c.appstart()

    async def query_device_info():
        await c.send(b"\x16\x0d")
        t0 = time.monotonic()
        while time.monotonic() - t0 < 5:
            p = await c.recv(2)
            if p is None:
                return None
            if len(p) >= 2 and p[0] == 0x0D:
                return p
        return None

    di = await query_device_info()
    if di is None:
        fail("no DEVICE_INFO from cache")
        await c.close()
        await ctl.close()
        return
    if len(di) != 82:
        fail(f"DEVICE_INFO is {len(di)} bytes, the firmware emits 82")
    else:
        ok(f"DEVICE_INFO is 82 bytes (path_hash_mode byte present, ={di[81]})")

    if di[81] == 0:
        ok("initial path_hash_mode is 0")
    else:
        fail(f"expected initial path_hash_mode 0, got {di[81]}")

    # Change it to 2 through the proxy.
    await c.send(b"\x3d\x00\x02")
    ack = None
    t0 = time.monotonic()
    while time.monotonic() - t0 < 5:
        p = await c.recv(1)
        if p is None:
            break
        if len(p) and p[0] in (0x00, 0x01):
            ack = p
            break
    if ack and ack[0] == 0x00:
        ok("SET_PATH_HASH_MODE(2) → OK")
    else:
        fail(f"SET_PATH_HASH_MODE failed: {ack.hex() if ack else 'None'}")

    # The radio really did change it.
    stats = await ctl.stats()
    if stats.get("path_hash_mode") == 2:
        ok("radio applied path_hash_mode=2")
    else:
        fail(f"radio path_hash_mode={stats.get('path_hash_mode')}, expected 2")

    # And the proxy must not keep serving the old cached blob.
    di2 = await query_device_info()
    if di2 is None:
        fail("no DEVICE_INFO after the setter")
    elif len(di2) >= 82 and di2[81] == 2:
        ok("cached DEVICE_INFO was invalidated; clients see path_hash_mode=2")
    else:
        fail(
            "stale DEVICE_INFO served after SET_PATH_HASH_MODE "
            f"(byte 81 = {di2[81] if di2 and len(di2) > 81 else 'missing'}, expected 2)"
        )

    await c.close()
    await ctl.close()


async def test_cli_command_invalidates_self_info(proxy_port, control_port):
    """Test 14: CMD_RUN_CLI_COMMAND (0x42) invalidates the cached blobs.

    MeshCore 1.18.0 added a CLI reachable from the companion protocol. Its
    `set name` / `set radio` / `set tx` / `set lat` / `set multi.acks` /
    `set path.hash.mode` / `set pin` commands are a second, parallel setter path:
    they mutate the same SELF_INFO / DEVICE_INFO fields through
    CommonRadioPrefs::handleCommand, not through the CMD_SET_* handlers, so none
    of the arms that guard those fields fire. A 0x42 missing from the
    invalidation list serves the pre-CLI blob to every client forever -- the
    shape-B bug, with a new trigger.
    """
    log.info("--- Test 25: CMD_RUN_CLI_COMMAND invalidates cached SELF_INFO ---")
    ctl = await connect_control(control_port)
    c = await connect_client(port=proxy_port)
    if not await c.appstart():
        fail("SELF_INFO failed")
        await c.close()
        await ctl.close()
        return

    async def self_info():
        await c.send(b"\x01\x01" + b"testclient")
        t0 = time.monotonic()
        while time.monotonic() - t0 < 5:
            p = await c.recv(2)
            if p is None:
                return None
            if len(p) and p[0] == 0x05:
                return p
        return None

    si = await self_info()
    if si is not None and len(si) > 2 and si[2] == 20:
        ok("initial SELF_INFO reports tx_power=20")
    else:
        fail(f"unexpected initial SELF_INFO: {si!r}")

    # Change tx_power through the CLI, not through CMD_SET_RADIO_TX_POWER (0x0C).
    # SELF_INFO byte 2 is the field; no other test reads it, so this does not
    # leak state into the rest of the suite.
    await c.send(b"\x42" + b"set tx 17")
    reply = None
    t0 = time.monotonic()
    while time.monotonic() - t0 < 5:
        p = await c.recv(1)
        if p is None:
            break
        if len(p) and p[0] == 0x1D:
            reply = p
            break
    if reply is not None and reply[1:] == b"OK":
        ok("CMD_RUN_CLI_COMMAND 'set tx' → CLI_REPLY (0x1D) OK")
    else:
        fail(f"unexpected CLI reply: {reply!r}")

    stats = await ctl.stats()
    if stats.get("tx_power") == 17:
        ok("radio applied the CLI tx_power change")
    else:
        fail(f"radio tx_power={stats.get('tx_power')!r}, expected 17")

    si2 = await self_info()
    if si2 is not None and len(si2) > 2 and si2[2] == 17:
        ok("cached SELF_INFO was invalidated; clients see tx_power=17")
    else:
        fail(
            "stale SELF_INFO served after CMD_RUN_CLI_COMMAND "
            f"(byte 2 = {si2[2] if si2 is not None and len(si2) > 2 else 'missing'}, expected 17)"
        )

    await c.close()
    await ctl.close()


async def test_ack_routes_past_in_flight_request(proxy_port, control_port):
    """Test 15: a late ACK reaches its original sender, not the busy client.

    PUSH_CODE_SEND_CONFIRMED (0x82) arrives seconds after the MSG_SENT that
    registered it. `try_route_ack` only gets a look if `try_route_to_in_flight`
    declines the frame first, which is why 0x82 is listed in PUSH_EVENT_CODES.
    Otherwise whichever unrelated request happens to be in flight swallows the
    ACK and the sender never learns its message landed.
    """
    log.info("--- Test 15: ACK is not swallowed by another client's in-flight request ---")
    ctl = await connect_control(control_port)
    c1 = await connect_client(port=proxy_port)
    c2 = await connect_client(port=proxy_port)
    await c1.appstart()
    await c2.appstart()

    dst = bytes.fromhex("aabbccddeeff")
    sent = await c1.send_msg(dst, "needs an ack")
    if not sent or len(sent) < 6:
        fail(f"c1 SEND_MSG did not return MSG_SENT: {sent.hex() if sent else 'None'}")
        await c1.close()
        await c2.close()
        await ctl.close()
        return
    ack = sent[2:6]
    ok(f"c1 SEND_MSG → MSG_SENT, expected_ack={ack.hex()}")

    # Occupy the proxy's single in-flight slot with c2's unrelated request.
    await ctl.hold(0x05)
    await c2.send(b"\x05")
    await asyncio.sleep(0.4)

    await ctl.push((b"\x82" + ack + struct.pack("<I", 321)).hex())
    await asyncio.sleep(0.5)

    got_c1 = None
    t0 = time.monotonic()
    while time.monotonic() - t0 < 4:
        p = await c1.recv(1)
        if p is None:
            break
        if len(p) and p[0] == 0x82:
            got_c1 = p
            break

    if got_c1 is None:
        fail("c1 never received its ACK (0x82)")
    elif got_c1[1:5] == ack:
        ok("c1 received the ACK with the matching ack_code")
    else:
        fail(f"c1 received an ACK but for the wrong code: {got_c1.hex()}")

    # c2 must not have been handed c1's ACK as the answer to its own request.
    stray = await c2.recv(1)
    if stray is not None and len(stray) and stray[0] == 0x82:
        fail("c2 received c1's ACK as its reply (ACK swallowed by in-flight slot)")
    else:
        ok("c2's in-flight request was not answered with c1's ACK")

    await ctl.release()
    await c1.close()
    await c2.close()
    await ctl.close()


async def drain_history(client, needle: bytes | None = None, limit: int = 200):
    """Poll GET_MSG and return the channel frames it yields, in order.

    Stops early once `needle` shows up in a frame. Frames that are not channel
    messages (MESSAGES_WAITING pushes, NO_MORE_MSGS) are skipped by the client
    helper and never land here.
    """
    frames = []
    t0 = time.monotonic()
    while len(frames) < limit and time.monotonic() - t0 < 20:
        p = await client.get_message()
        if p is None:
            break
        if is_channel_frame(p):
            frames.append(p)
            if needle is not None and needle in p:
                break
    return frames


async def drain_history_until_no_more(client, limit: int = 60):
    """Drain the replay until the proxy hands the poll back to the radio.

    A NO_MORE_MSGS reply (0x0A) means the log is exhausted and the client is
    caught up, which is the signal that the replay is complete.
    """
    frames = []
    t0 = time.monotonic()
    while len(frames) < limit and time.monotonic() - t0 < 25:
        p = await client.get_message()
        if p is None or p[0] == 0x0A:
            break
        if is_channel_frame(p):
            frames.append(p)
    return frames


async def recv_until_channel(client, needle: bytes, timeout=5.0):
    """Read frames until a channel message containing `needle` shows up.

    The radio's periodic injector keeps emitting MESSAGES_WAITING (0x83) while
    a test waits, so read directly instead of through `get_message`, which
    would poll and pull those queued items out as well.
    """
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        p = await client.recv(1)
        if p is None:
            return None
        if is_channel_frame(p) and needle in p:
            return p
    return None


async def test_late_reply_is_not_misattributed(proxy_port, control_port):
    """Test 16: a reply for a timed-out request does not answer anyone else.

    The radio answers commands in order, so once the proxy has failed a request
    the next frame to arrive is *that* request's reply -- not the answer to
    whatever is in flight by then. `Awaiting::Any` would accept it (any non-push
    code does), hand one client another client's answer and broadcast the
    displaced reply to everybody.

    Checked twice: first with different expected codes (a single-frame matcher
    is enough there), then with two lookups that both expect 0x03 -- the case
    only the ghost can get right.
    """
    log.info("--- Test 22: a late reply is not handed to another client ---")
    ctl = await connect_control(control_port)

    # Park both replies, so the radio keeps the first command unanswered while
    # the second one reaches the in-flight slot.
    await ctl.hold(0x03)
    await ctl.hold(0x05)

    a = await connect_client(port=proxy_port)
    await a.appstart()
    await a.send(b"\x03\x00\x00" + struct.pack("<I", int(time.time())) + b"late reply probe")

    err = None
    t0 = time.monotonic()
    while time.monotonic() - t0 < 25:
        p = await a.recv(2)
        if p is None:
            break
        if len(p) and p[0] == 0x01:
            err = p
            break
    if err is None:
        fail("the parked channel send was never failed with ERROR")
        await ctl.release()
        await a.close()
        await ctl.close()
        return
    ok("the parked channel send was failed with ERROR after the in-flight timeout")

    # b takes the in-flight slot. The radio now owes two replies, in order: the
    # parked 0x03's OK first, then b's CURRENT_TIME.
    b = await connect_client(port=proxy_port)
    await b.appstart()
    await b.send(b"\x05")
    await asyncio.sleep(0.4)
    await ctl.release()

    first = None
    t0 = time.monotonic()
    while time.monotonic() - t0 < 5:
        p = await b.recv(1)
        if p is None:
            break
        # The injector fires MESSAGES_WAITING at every client; only the reply
        # to b's own request is of interest here.
        if len(p) and p[0] != 0x83:
            first = p
            break

    if first is None:
        fail("b never received a reply")
    elif first[0] == 0x00:
        fail("b was handed the timed-out send's OK as the answer to its own request")
    elif first[0] == 0x09:
        ok("b got its own CURRENT_TIME; the late OK was dropped")
    else:
        fail(f"b's first reply was 0x{first[0]:02x}, expected 0x09")

    await a.close()
    await b.close()

    # Second half: the harder case, where the expected code cannot tell the two
    # requests apart. Two GET_CONTACT_BY_KEY lookups are both answered with
    # 0x03, so only the ghost can stop the late answer to `a` from looking like
    # `b`'s. Without it, `b` is handed the contact `a` asked for.
    await ctl.hold(0x1E)
    a = await connect_client(port=proxy_port)
    await a.appstart()
    await a.send(b"\x1e" + CONTACTS_BASELINE_ALPHA)

    err = None
    t0 = time.monotonic()
    while time.monotonic() - t0 < 25:
        p = await a.recv(2)
        if p is None:
            break
        if len(p) and p[0] == 0x01:
            err = p
            break
    if err is None:
        await ctl.release()
        await a.close()
        await ctl.close()
        fail("the parked lookup was never failed with ERROR")
        return

    b = await connect_client(port=proxy_port)
    await b.appstart()
    await b.send(b"\x1e" + CONTACTS_BASELINE_BRAVO)
    await asyncio.sleep(0.4)
    await ctl.release()

    got = None
    t0 = time.monotonic()
    while time.monotonic() - t0 < 5:
        p = await b.recv(1)
        if p is None:
            break
        if len(p) and p[0] == 0x03:
            got = p
            break
    await ctl.release()
    await a.close()
    await b.close()
    await ctl.close()

    if got is None:
        fail("b never received a CONTACT reply")
    elif got[1:33] == CONTACTS_BASELINE_ALPHA:
        fail("b was handed the contact the timed-out lookup asked for")
    elif got[1:33] == CONTACTS_BASELINE_BRAVO:
        ok("b got its own lookup; the timed-out one's 0x03 was swallowed by its ghost")
    else:
        fail(f"b got a CONTACT reply for {got[1:33].hex()[:16]}...")


async def test_client_idle_timeout():
    """Test 17: a silent client is reaped, a polling one is not.

    A suspended phone never sends a FIN, so the frontend has to conclude a
    client is gone from silence alone. The rule has to be idle-based rather
    than a connection age cap: a client that keeps polling must outlive it.
    """
    log.info("--- Test 23: idle timeout reaps silent clients only ---")
    port = await find_free_port()
    data_dir = f"/tmp/cs_test_proxy/{os.getpid()}/idle"
    subprocess.run(["rm", "-rf", data_dir], check=False)
    proc = await asyncio.create_subprocess_exec(
        _proxy_args[0],
        "--tcp-backend-host", "127.0.0.1",
        "--tcp-backend-port", str(_fake_port),
        "--tcp-frontend-port", str(port),
        "--data-dir", data_dir,
        "--log-level", "info",
        "--client-idle-timeout", "2",
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        if not await wait_for_port(port):
            fail("idle-timeout proxy did not start")
            return

        idle = await connect_client(port=port)
        busy = await connect_client(port=port)
        await busy.appstart()

        idle_closed = False
        busy_alive = True
        t0 = time.monotonic()
        while time.monotonic() - t0 < 8:
            # CURRENT_TIME keeps `busy` off the idle list while `idle` is mute.
            await busy.send(b"\x05")
            if await busy.recv(1.0) is None:
                busy_alive = False
                break
            if await idle.recv(0.1) is None:
                idle_closed = True
                break

        await idle.close()
        await busy.close()

        if not idle_closed:
            fail("the silent client was never reaped")
        elif not busy_alive:
            fail("the polling client was reaped too: the sweep is not idle-based")
        else:
            ok("the silent client was closed while the polling one survived")
    finally:
        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()


async def test_get_channel_out_of_range_is_local(proxy_port, control_port):
    """Test 18: only *provably* doomed GET_CHANNELs are answered locally.

    `BaseChatMesh::getChannel` refuses every idx >= MAX_GROUP_CHANNELS, so the
    radio can only answer ERR_CODE_NOT_FOUND for those. Forwarding one burns
    the single in-flight slot on an outcome already known, which is what makes
    an app walk its way to "no more channels" expensive.

    The other half of the boundary matters just as much: idx below the bound
    must still reach the cache (or the radio, on a cache miss), because a
    refused idx there would hide a channel the app is supposed to see.
    """
    log.info("--- Test 24: out-of-range GET_CHANNEL is answered locally ---")
    ctl = await connect_control(control_port)
    before = (await ctl.stats())["cmd_counts"].get("0x1f", 0)

    c = await connect_client(port=proxy_port)
    await c.appstart()

    async def request(idx):
        await c.send(b"\x1f" + bytes([idx]))
        t0 = time.monotonic()
        while time.monotonic() - t0 < 3:
            p = await c.recv(1)
            if p is None:
                return None
            # Skip the injector's unsolicited MESSAGES_WAITING.
            if len(p) and p[0] in (0x01, 0x12):
                return p
        return None

    in_range = await request(MAX_GROUP_CHANNELS - 1)  # 39: last real slot
    boundary = await request(MAX_GROUP_CHANNELS)  # 40: the boundary itself
    past = await request(255)  # far past
    await c.close()

    after = (await ctl.stats())["cmd_counts"].get("0x1f", 0)
    await ctl.close()

    if in_range is None or in_range[0] != 0x12:
        # Answering this one locally as NOT_FOUND would hide a real channel.
        fail(f"in-range idx {MAX_GROUP_CHANNELS - 1} must still answer "
             f"CHANNEL_INFO (0x12), got {in_range!r}")
    elif boundary is None or len(boundary) < 2 or boundary[1] != 0x02:
        fail(f"idx {MAX_GROUP_CHANNELS} should be a local ERR_CODE_NOT_FOUND "
             f"(0x01 0x02), got {boundary!r}")
    elif past is None or len(past) < 2 or past[1] != 0x02:
        fail(f"idx 255 should be a local ERR_CODE_NOT_FOUND (0x01 0x02), got {past!r}")
    elif after != before:
        fail(f"GET_CHANNEL reached the radio for an idx the cache or the "
             f"boundary already settled ({before} -> {after})")
    else:
        ok("only idx >= max_channels is answered locally (0x01 0x02), radio untouched")


async def test_client_request_jumps_sync_queue(proxy_port, control_port):
    """Test 19: a client command does not queue behind the internal sync.

    `sync_from_radio` pushes one request per channel slot (plus the contact
    list) through the same FIFO as client requests, and the radio answers one
    command per loop. On a slow link that is seconds of queue, and a client
    command that falls in behind all of it waits for every entry before the
    radio even sees it -- even though none of those entries is the work the
    client asked for.

    `delay` is what makes that queue observable: it is the one control verb
    that slows the proxy's own sync, which is the condition under test, and it
    is cleared before returning. A client's non-cacheable request must answer
    inside a couple of command slots instead of the whole channel list.
    """
    log.info("--- Test 26: client requests jump the internal sync queue ---")
    ctl = await connect_control(control_port)
    # 400 ms per command: the channel sync alone is then >16 s, far
    # past the 3 s window this test gives the client's reply.
    await ctl.delay(400)

    await stop_proxy()
    if not await spawn_proxy():
        fail("proxy did not come back after a restart")
        await ctl.delay(0)
        await ctl.close()
        return

    # The frontend only starts accepting after sync_from_radio has queued the
    # whole channel list, so a client connecting now is guaranteed to be behind
    # it. (Jumping the queue is the point, so it must not need a warm cache.)
    c = await connect_client(port=proxy_port)
    t0 = time.monotonic()
    await c.send(b"\x0a")  # CMD_SYNC_NEXT_MESSAGE: never answered from cache
    reply = None
    while time.monotonic() - t0 < 3:
        p = await c.recv(0.5)
        if p is None:
            break
        # Skip the injector's unsolicited MESSAGES_WAITING, which any client
        # receives at any time.
        if len(p) and p[0] != 0x83:
            reply = p
            break
    elapsed = time.monotonic() - t0
    await ctl.delay(0)
    await c.close()
    await ctl.close()

    if reply is None:
        fail(f"client request not answered in 3.0s while the sync queued "
             f"(the radio was busy with the proxy's own reads)")
    elif reply[0] == 0x01:
        fail(f"client request was failed with ERROR instead of being answered: {reply!r}")
    else:
        ok(f"client request answered in {elapsed:.2f}s, ahead of the sync queue")


def contact_name(frame: bytes) -> str:
    """The name field of a RESP_CODE_CONTACT frame, [100..132]."""
    return frame[100:132].split(b"\x00")[0].decode("utf-8", "replace")


async def read_contacts_reply(client, timeout=8.0):
    """Collect one CONTACT reply; returns (declared total, frames received).

    The total in CONTACT_START is the radio's whole table, not the number of
    frames this reply carries: a `since` request announces the total and then
    sends only what changed, which is what the client's own watermark relies on.
    """
    frames = []
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        p = await client.recv(1.0)
        if p is None:
            break
        if len(p) and p[0] in (0x02, 0x03, 0x04):
            frames.append(p)
            if p[0] == 0x04:
                break
    declared = (
        int.from_bytes(frames[0][1:5], "little") if frames and len(frames[0]) >= 5 else -1
    )
    return declared, frames


async def spawn_scratch_proxy(tag, timeout="6", extra_args=()):
    """A proxy on its own port and its own (fresh) data directory.

    Needed wherever cache state is the thing under test: the suite's shared
    proxy has been warming its own caches for twenty tests by then. Returns the
    data directory too, because some of that state lives in SQLite rather than
    in RAM.
    """
    port = await find_free_port()
    data_dir = f"/tmp/cs_test_proxy/{os.getpid()}/{tag}"
    subprocess.run(["rm", "-rf", data_dir], check=False)
    proc = await asyncio.create_subprocess_exec(
        _proxy_args[0],
        "--tcp-backend-host", "127.0.0.1",
        "--tcp-backend-port", str(_fake_port),
        "--tcp-frontend-port", str(port),
        "--data-dir", data_dir,
        "--log-level", "info",
        "--in-flight-timeout", timeout,
        *extra_args,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    if not await wait_for_port(port):
        return None, port, data_dir
    return proc, port, data_dir


async def stop_proc(proc):
    if proc is None:
        return
    proc.terminate()
    try:
        await asyncio.wait_for(proc.wait(), timeout=5)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()


CONTACTS_BASELINE_ALPHA = bytes.fromhex(
    "aabbccdd0011aabbccdd0011aabbccdd0011aabbccdd0011aabbccdd0011aabb"
)
CONTACTS_BASELINE_BRAVO = bytes.fromhex(
    "1122334455661122334455661122334455661122334455661122334455661122"
)


def _contact_name(frame: bytes) -> str:
    return frame[100:132].rstrip(b"\x00").decode("utf-8", "replace")


def _contact_lastmod(frame: bytes) -> int:
    return struct.unpack("<I", frame[144:148])[0]


async def test_contacts_cached_verbatim(control_port):
    """Test 20: GET_CONTACTS is answered from the cache, frame for frame.

    The cached reply has to be what the radio sent: flags, out_path and
    lastmod included, and `since` applied the way the firmware applies it.
    Rebuilding the frame from decoded columns would zero all three, and a
    client that syncs incrementally would get the whole book every time.
    """
    log.info("--- Test 25: cached contacts are served verbatim ---")
    ctl = await connect_control(control_port)
    await ctl.contacts_reset()
    proc, port, _ = await spawn_scratch_proxy("contacts_verbatim")
    c = None
    try:
        if proc is None:
            fail("contacts-cache proxy did not start")
            return
        c = await connect_client(port=port)
        await c.appstart()

        full = None
        t0 = time.monotonic()
        while time.monotonic() - t0 < 5:
            full = await c.get_contacts_raw()
            if full and full[1]:
                break
            await asyncio.sleep(0.2)
        if not full or not full[1]:
            fail("GET_CONTACTS returned nothing")
            return
        start, frames, end = full

        alpha = next((f for f in frames if _contact_name(f) == "Alpha Test"), None)
        if (
            alpha
            and alpha[34] == 0x01                # flags
            and alpha[35] == 2                   # out_path_len
            and alpha[36:38] == bytes([0x2A, 0x7F])
            and _contact_lastmod(alpha) == 1_700_000_100
        ):
            ok("cached CONTACT keeps flags, path and lastmod from the radio")
        else:
            fail(f"cached CONTACT differs from the radio: {alpha.hex() if alpha else None}")
            return

        total = struct.unpack("<I", start[1:5])[0]
        if total == len(frames):
            ok("CONTACT_START carries the total count, not a filtered one")
        else:
            fail(f"CONTACT_START count {total} != {len(frames)} frames")

        part = await c.get_contacts_raw(since=1_700_000_100)
        if not part:
            fail("GET_CONTACTS(since) failed")
            return
        _, sent, since_end = part
        mods = [_contact_lastmod(f) for f in sent]
        if (
            all(m > 1_700_000_100 for m in mods)
            and len(sent) < len(frames)
            and struct.unpack("<I", since_end[1:5])[0] == max(mods, default=0)
        ):
            ok("GET_CONTACTS honours 'since' like the firmware")
        else:
            fail(f"GET_CONTACTS(since): lastmods={mods} end={since_end.hex()}")
    finally:
        if c is not None:
            await c.close()
        await stop_proc(proc)
        await ctl.close()


async def test_contacts_cache_answers_without_radio(control_port):
    """Test 21: a warm contact cache answers even while the radio is held.

    This is what the cache is for: the radio's answer is parked, so a client
    whose GET_CONTACTS is still answered cannot have reached it.
    """
    log.info("--- Test 26: the contacts cache is served without the radio ---")
    ctl = await connect_control(control_port)
    await ctl.contacts_reset()
    proc, port, _ = await spawn_scratch_proxy("contacts_cache_only")
    c = None
    try:
        if proc is None:
            fail("contacts-cache proxy did not start")
            return
        c = await connect_client(port=port)
        await c.appstart()
        # Warm the cache from the startup sync first.
        for _ in range(10):
            if await c.get_contacts_raw():
                break
            await asyncio.sleep(0.3)

        await ctl.hold(0x04)
        try:
            before = (await ctl.stats())["cmd_counts"].get("0x04", 0)
            got = await c.get_contacts_raw(timeout=3.0)
            after = (await ctl.stats())["cmd_counts"].get("0x04", 0)
        finally:
            await ctl.release()

        if not got or not got[1]:
            fail("GET_CONTACTS was not answered while the radio was held")
        elif after != before:
            fail(f"the ask reached the radio ({before} -> {after}) instead of the cache")
        else:
            ok("GET_CONTACTS answered from the cache, radio untouched")
    finally:
        if c is not None:
            await c.close()
        await stop_proc(proc)
        await ctl.close()


async def test_update_contact_refreshes_cache(control_port):
    """Test 22: ADD_UPDATE_CONTACT refreshes the cached frame."""
    log.info("--- Test 27: ADD_UPDATE_CONTACT refreshes the cache ---")
    ctl = await connect_control(control_port)
    await ctl.contacts_reset()
    proc, port, _ = await spawn_scratch_proxy("contacts_update")
    c = None
    try:
        if proc is None:
            fail("contacts-cache proxy did not start")
            return
        c = await connect_client(port=port)
        await c.appstart()
        full = None
        for _ in range(10):
            full = await c.get_contacts_raw()
            if full and full[1]:
                break
            await asyncio.sleep(0.3)
        alpha = next(
            (f for f in (full[1] if full else []) if _contact_name(f) == "Alpha Test"),
            None,
        )
        if not alpha:
            fail("Alpha Test not in the cached contacts")
            return

        # Rename it and give it a new path: the CONTACT frame minus code and
        # lastmod, which the radio stamps itself.
        upd = bytearray(alpha[1:144])
        upd[34] = 1  # out_path_len
        upd[35:99] = bytes([0x55]).ljust(64, b"\x00")
        upd[99:131] = b"Alpha Renamed".ljust(32, b"\x00")
        await c.send(b"\x09" + bytes(upd))
        reply = None
        t0 = time.monotonic()
        while time.monotonic() - t0 < 5:
            p = await c.recv(2)
            if p is None:
                break
            if len(p) and p[0] in (0x00, 0x01):
                reply = p
                break
        if not reply or reply[0] != 0x00:
            fail(f"ADD_UPDATE_CONTACT reply: {reply.hex() if reply else 'None'}")
            return
        ok("ADD_UPDATE_CONTACT → OK")

        # The refresh is a GET_CONTACT_BY_KEY queued behind the OK; wait for
        # the cache to carry the radio's new frame.
        await asyncio.sleep(0.7)
        full = await c.get_contacts_raw()
        renamed = next(
            (f for f in (full[1] if full else []) if _contact_name(f) == "Alpha Renamed"),
            None,
        )
        if (
            renamed
            and renamed[35] == 1
            and renamed[36] == 0x55
            and _contact_lastmod(renamed) != _contact_lastmod(alpha)
        ):
            ok("cached contact reflects ADD_UPDATE_CONTACT (name, path, lastmod)")
        else:
            names = [_contact_name(f) for f in (full[1] if full else [])]
            fail(f"cache not refreshed after ADD_UPDATE_CONTACT: {names}")
    finally:
        if c is not None:
            await c.close()
        await stop_proc(proc)
        await ctl.contacts_reset()
        await ctl.close()


async def test_remove_contact(control_port):
    """Test 23: REMOVE_CONTACT also purges the cached row."""
    log.info("--- Test 28: REMOVE_CONTACT purges the cache ---")
    ctl = await connect_control(control_port)
    await ctl.contacts_reset()
    proc, port, _ = await spawn_scratch_proxy("contacts_remove")
    c = None
    try:
        if proc is None:
            fail("contacts-cache proxy did not start")
            return
        c = await connect_client(port=port)
        await c.appstart()
        full = None
        for _ in range(10):
            full = await c.get_contacts_raw()
            if full and full[1]:
                break
            await asyncio.sleep(0.3)
        names = [_contact_name(f) for f in (full[1] if full else [])]
        if "Bravo Test" not in names:
            fail(f"GET_CONTACTS before remove: {names}")
            return

        await c.send(b"\x0f" + CONTACTS_BASELINE_BRAVO)
        reply = None
        t0 = time.monotonic()
        while time.monotonic() - t0 < 5:
            p = await c.recv(2)
            if p is None:
                break
            if len(p) and p[0] in (0x00, 0x01):
                reply = p
                break
        if not reply or reply[0] != 0x00:
            fail(f"REMOVE_CONTACT reply: {reply.hex() if reply else 'None'}")
            return
        ok("REMOVE_CONTACT → OK")

        full = await c.get_contacts_raw()
        names = [_contact_name(f) for f in (full[1] if full else [])]
        if "Bravo Test" in names:
            fail(f"removed contact still served from cache: {names}")
        elif "Alpha Test" not in names:
            fail(f"remaining contact missing after remove: {names}")
        else:
            ok("the cached list no longer holds the removed contact")
    finally:
        if c is not None:
            await c.close()
        await stop_proc(proc)
        await ctl.contacts_reset()
        await ctl.close()


async def test_message_layout_per_version(proxy_port):
    """Test 24: message frames follow each client's protocol version.

    The radio is negotiated once, at v3, because it picks the layout when it
    queues a received message. A v1 client must still see the legacy frame, so
    the proxy converts on the way out instead of letting the first client to
    connect decide the layout for everybody.
    """
    log.info("--- Test 29: message frames follow each client's version ---")
    old = await connect_client(port=proxy_port)
    new = await connect_client(port=proxy_port)
    if not await old.appstart() or not await new.appstart():
        await old.close()
        await new.close()
        fail("SELF_INFO failed for one or both clients")
        return

    # DEVICE_QUERY carries the version the app understands; the proxy answers
    # it and never hands it to the radio.
    for c, version in ((old, 1), (new, 3)):
        await c.send(bytes([0x16, version]))
        p = await c.recv(5)
        if not p or p[0] != 0x0D:
            await old.close()
            await new.close()
            fail(f"DEVICE_QUERY v{version} returned: {p.hex() if p else 'None'}")
            return

    if not await _wait_for_0x83(new, 10):
        await old.close()
        await new.close()
        fail("v3 client did not receive MESSAGES_WAITING (0x83) in 10s")
        return

    await new.send(b"\x0A")
    v3 = None
    t0 = time.monotonic()
    while time.monotonic() - t0 < 5:
        p = await new.recv(1)
        if p is None:
            break
        if len(p) and p[0] in MSG_CODES:
            v3 = p
            break
    if not v3 or v3[0] not in (0x10, 0x11):
        await old.close()
        await new.close()
        fail(f"v3 client GET_MSG returned: {v3.hex() if v3 else 'None'}")
        return
    ok(f"v3 client gets 0x{v3[0]:02x}")

    v1 = None
    t0 = time.monotonic()
    while time.monotonic() - t0 < 5:
        p = await old.recv(1)
        if p is None:
            break
        if len(p) and p[0] in MSG_CODES:
            v1 = p
            break
    await old.close()
    await new.close()

    if v1 == bytes([v3[0] - 0x09]) + v3[4:]:
        ok(f"v1 client gets the same message as 0x{v1[0]:02x}")
    else:
        fail(f"v1 client got {v1.hex() if v1 else 'None'} for {v3.hex()}")


async def test_rejected_send_not_echoed(proxy_port):
    """Test 25: a rejected send is not echoed to the other clients."""
    log.info("--- Test 30: a rejected send is not echoed ---")
    c1 = await connect_client(port=proxy_port)
    c2 = await connect_client(port=proxy_port)
    if not await c1.appstart() or not await c2.appstart():
        await c1.close()
        await c2.close()
        fail("SELF_INFO failed for one or both clients")
        return

    # Channel 30 does not exist on the radio: it answers ERROR, and nothing
    # may be handed to the other clients as if it had been sent.
    ts = int(time.time())
    await c1.send(b"\x03" + bytes([0, 30]) + struct.pack("<I", ts) + b"never sent")
    reply = None
    t0 = time.monotonic()
    while time.monotonic() - t0 < 5:
        p = await c1.recv(1)
        if p is None:
            break
        if len(p) and p[0] in (0x00, 0x01):
            reply = p
            break
    if not reply or reply[0] != 0x01:
        await c1.close()
        await c2.close()
        fail(f"SEND_CHAN_MSG to unknown channel: {reply.hex() if reply else 'None'}")
        return
    ok("SEND_CHAN_MSG to an unknown channel → ERROR")

    echoed = False
    t0 = time.monotonic()
    while time.monotonic() - t0 < 2:
        p = await c2.recv(0.5)
        if p is None:
            break
        if p and is_channel_frame(p) and b"never sent" in p:
            echoed = True
            break
    await c1.close()
    await c2.close()
    if echoed:
        fail("the other client received an echo of a rejected message")
    else:
        ok("the rejected message is not echoed")


async def _drain_for_text(c, text: bytes, polls: int = 5):
    """Poll GET_MSG until a frame containing `text` shows up, the radio says
    NO_MORE_MSGS, or `polls` runs out."""
    for _ in range(polls):
        p = await c.get_message()
        if not p:
            return None
        if text in p:
            return p
        if p[0] == 0x0A:
            return None
    return None


async def test_missed_messages_replayed(proxy_port):
    """Test 26: messages missed while offline are replayed on reconnect."""
    log.info("--- Test 31: missed messages are replayed ---")
    a = await connect_client(port=proxy_port)
    if not await a.appstart("replay-app"):
        await a.close()
        fail("SELF_INFO failed")
        return
    await a.close()
    await asyncio.sleep(0.5)  # let the proxy notice the disconnect

    b = await connect_client(port=proxy_port)
    await b.appstart()
    if not await b.send_chan_msg(0, "while you were away"):
        await b.close()
        fail("SEND_CHAN_MSG failed")
        return
    await b.close()

    a = await connect_client(port=proxy_port)
    await a.appstart("replay-app")
    if await _drain_for_text(a, b"while you were away"):
        ok("reconnecting client gets the message it missed via GET_MSG")
    else:
        fail("missed message was not replayed on reconnect")

    # Caught up now: reconnecting again must not replay it a second time.
    await a.close()
    await asyncio.sleep(0.5)
    a = await connect_client(port=proxy_port)
    await a.appstart("replay-app")
    if await _drain_for_text(a, b"while you were away", polls=3):
        fail("message replayed twice")
    else:
        ok("an already delivered message is not replayed again")
    await a.close()


async def test_own_messages_not_replayed(proxy_port):
    """Test 27: a client's own messages are not replayed back to it."""
    log.info("--- Test 32: own messages are not replayed ---")
    a = await connect_client(port=proxy_port)
    if not await a.appstart("echo-app"):
        await a.close()
        fail("SELF_INFO failed")
        return
    await a.send_chan_msg(0, "my own words")
    await a.close()
    await asyncio.sleep(0.5)

    a = await connect_client(port=proxy_port)
    await a.appstart("echo-app")
    replayed = await _drain_for_text(a, b"my own words", polls=3)
    await a.close()
    if replayed:
        fail("client got its own message back on reconnect")
    else:
        ok("a client's own message is not replayed to it")


async def test_same_app_two_ports(proxy_port):
    """Test 28: the same app on two ports keeps two separate histories.

    A client is (frontend port, app name), so two copies of one app behind
    different ports must not share a cursor: one of them would silently lose
    the messages the other had already consumed.
    """
    log.info("--- Test 33: same app on two ports keeps separate histories ---")
    port1 = await find_free_port()
    port2 = await find_free_port()
    data_dir = f"/tmp/cs_test_proxy/{os.getpid()}/two_ports"
    subprocess.run(["rm", "-rf", data_dir], check=False)
    proc = await asyncio.create_subprocess_exec(
        _proxy_args[0],
        "--tcp-backend-host", "127.0.0.1",
        "--tcp-backend-port", str(_fake_port),
        "--tcp-frontend-port", f"{port1},{port2}",
        "--data-dir", data_dir,
        "--log-level", "info",
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        if not await wait_for_port(port1) or not await wait_for_port(port2):
            fail("two-port proxy did not start")
            return
        for port in (port1, port2):
            c = await connect_client(port=port)
            if not await c.appstart("twin-app"):
                await c.close()
                fail(f"SELF_INFO failed on port {port}")
                return
            await c.close()
        await asyncio.sleep(0.5)

        sender = await connect_client(port=port1)
        await sender.appstart()
        await sender.send_chan_msg(0, "for both twins")
        await sender.close()

        got = []
        for port in (port1, port2):
            c = await connect_client(port=port)
            await c.appstart("twin-app")
            got.append(bool(await _drain_for_text(c, b"for both twins")))
            await c.close()
        if all(got):
            ok("each port's copy of the same app gets the missed message")
        else:
            fail(f"missed message per port (port1, port2): {got}")
    finally:
        await stop_proc(proc)


def _db_client_cursor(app_name: str):
    """Read a client's persisted replay cursor straight from SQLite."""
    db = os.path.join(_data_dir, "state.db")
    if not os.path.exists(db):
        return None
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        row = con.execute(
            "SELECT last_seen_id FROM clients WHERE app_name = ?", (app_name,)
        ).fetchone()
    finally:
        con.close()
    return row[0] if row else None


def _db_message_head():
    db = os.path.join(_data_dir, "state.db")
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        return con.execute("SELECT COALESCE(MAX(id), 0) FROM messages").fetchone()[0]
    finally:
        con.close()


async def test_cursor_only_moves_on_acknowledgement(proxy_port, control_port):
    """Test 29: a live cursor is persisted only up to what the client acked.

    A socket can die without a FIN -- a laptop switching from ethernet to wifi
    leaves the old connection half-open, writes still succeeding into the
    kernel buffer -- and the frontend keeps it for the whole idle timeout.
    Crediting every stored frame to such a client would skip them on the
    reconnect, which arrives as a *new* client id on the same identity.

    So the persisted cursor may only advance to the watermark the client proved
    it read by speaking (TCP delivers in order, so a command proves everything
    written before it arrived). A frame delivered after the last command stays
    replayable -- a duplicate at worst, never a loss.
    """
    log.info("--- Test 34: the cursor follows the client's acknowledgement ---")
    ctl = await connect_control(control_port)
    name = "cursor-app"

    # Park the identity's cursor on the current head.
    prime = await connect_client(port=proxy_port)
    await prime.appstart(name)
    await prime.close()
    await asyncio.sleep(0.5)
    parked = _db_client_cursor(name)
    if parked is None:
        await ctl.close()
        fail("the proxy never recorded the client identity")
        return

    a = await connect_client(port=proxy_port)
    await a.appstart(name)

    # Delivered live and acknowledged with a poll, the way the app answers a
    # MESSAGES_WAITING push.
    seen = b"Cursor confirmed live"
    await ctl.push(v3_channel_frame(0, seen).hex())
    if await recv_until_channel(a, seen) is None:
        await a.close()
        await ctl.close()
        fail("the client never saw the first pushed frame live")
        return
    await a.get_message()
    await asyncio.sleep(2.5)  # the housekeeping tick persists it
    after_ack = _db_client_cursor(name)
    head_after_ack = _db_message_head()
    if after_ack is not None and after_ack >= head_after_ack - 0:
        ok("the acknowledged frame is persisted")
    elif after_ack is not None and after_ack > parked:
        ok("the acknowledged frame is persisted")
    else:
        await a.close()
        await ctl.close()
        fail(f"acknowledged frame not persisted (cursor {after_ack}, parked {parked})")
        return

    # Delivered live afterwards, with no further command: unconfirmed, so it
    # must stay replayable.
    unseen = b"After the acknowledgement"
    await ctl.push(v3_channel_frame(0, unseen).hex())
    if await recv_until_channel(a, unseen) is None:
        await a.close()
        await ctl.close()
        fail("the client never saw the unacknowledged frame live")
        return
    # Let the housekeeping tick run: it is where a cursor is persisted, and the
    # point of the test is that this one does *not* reach `unseen`.
    await asyncio.sleep(2.5)

    # A reconnect on the same identity while the first session is still
    # counted alive -- the address change, without the idle sweep in between.
    b = await connect_client(port=proxy_port)
    await b.appstart(name)
    frames = await drain_history_until_no_more(b)
    await b.close()
    await a.close()
    await ctl.close()

    if any(seen in f for f in frames):
        fail(
            "a frame the first session acknowledged was replayed to the "
            f"reconnect ({len(frames)} frames replayed)"
        )
    ok("the acknowledged frame is not replayed after a reconnect")

    if any(unseen in f for f in frames):
        ok("the unacknowledged frame is still replayed, not lost")
    else:
        fail("a frame the client never acknowledged was skipped")


async def test_contacts_are_client_driven(control_port):
    """Test 30: with --no-contact-cache the proxy neither asks for nor stores contacts.

    A proxy-side watermark would be a third copy of state: the radio filters on
    `contact.lastmod` and the client keeps its own watermark from
    CONTACT_END. With the cache switched off, the client's request (with its
    `since`) goes to the radio every time, and nothing is stored behind it.
    `test_contacts_cached_verbatim` covers the default, cached behaviour.
    """
    log.info("--- Test 28: contacts are the client's request, not the proxy's ---")
    ctl = await connect_control(control_port)
    base = (await ctl.stats()).get("cmd_counts", {})
    max_lastmod = (await ctl.stats()).get("contacts_lastmod_max", 0)

    proc, port, _ = await spawn_scratch_proxy(
        "contacts_client_driven", extra_args=("--no-contact-cache",)
    )
    try:
        if proc is None:
            fail("contacts proxy did not start")
            return
        # The channel sync is the proxy's own; contacts must not be touched.
        await asyncio.sleep(1.0)
        streams = (await ctl.stats()).get("cmd_counts", {}).get("0x04", 0) - base.get("0x04", 0)
        if streams == 0:
            ok("the proxy asked the radio for no contacts at startup")
        else:
            fail(f"the proxy sent {streams} GET_CONTACTS at startup; contacts are the client's")

        c = await connect_client(port=port)

        # 1. A client's `since` reaches the radio unchanged.
        await c.send(b"\x04" + struct.pack("<I", max_lastmod))
        declared, frames = await read_contacts_reply(c)
        contacts = sum(1 for f in frames if f[0] == 0x03)
        seen_since = (await ctl.stats()).get("contacts_since")
        if seen_since == max_lastmod:
            ok(f"the client's since ({max_lastmod}) reached the radio unchanged")
        else:
            fail(f"the radio saw since={seen_since}, expected the client's {max_lastmod}")
        # Nothing in the fake radio's list is newer than that, so this is an
        # empty delta -- which still announces the whole table, as the protocol
        # requires (the count is the table, not the reply).
        if declared > 0 and contacts == 0 and frames and frames[-1][0] == 0x04:
            ok(f"the delta announced all {declared} contacts and sent none")
        else:
            fail(f"unexpected delta reply: declared={declared} contact_frames={contacts}")

        # 2. Nothing was stored: a full book and then a delta each go to the
        #    radio again, instead of being answered from a copy.
        before = (await ctl.stats()).get("cmd_counts", {}).get("0x04", 0)
        await c.send(b"\x04")
        declared, frames = await read_contacts_reply(c)
        full = sum(1 for f in frames if f[0] == 0x03)
        if full > 0 and declared == full:
            ok(f"a full request streamed {full} contacts")
        else:
            fail(f"unexpected full reply: declared={declared} contact_frames={full}")

        await c.send(b"\x04" + struct.pack("<I", max_lastmod))
        _declared, frames = await read_contacts_reply(c)
        again = sum(1 for f in frames if f[0] == 0x03)
        after = (await ctl.stats()).get("cmd_counts", {}).get("0x04", 0)
        if after - before == 2:
            ok("every request reached the radio: nothing is answered from a stored copy")
        else:
            fail(f"only {after - before}/2 requests reached the radio; contacts are being cached")
        if again == 0:
            ok("the repeat delta was empty, as a stored-then-forwarded client would see")
        else:
            fail(f"repeat delta returned {again} contacts, expected none")

        await c.close()
    finally:
        await stop_proc(proc)
        await ctl.close()


async def test_factory_reset_invalidates_cached_blobs(proxy_port, control_port):
    """Test 31: FACTORY_RESET (0x33) invalidates every cached blob.

    0x33 wipes the radio's identity and settings, so the proxy's copies of
    SELF_INFO / DEVICE_INFO / BATTERY must all be cleared. It is the broadest
    setter and the easiest to leave out of the invalidation list, because a
    "reset" reads like a channel/contact concern rather than a settings one.
    Counting radio reads distinguishes a fresh read from a served copy without
    depending on the reset changing any particular byte.
    """
    log.info("--- Test 30: FACTORY_RESET invalidates the cached blobs ---")
    ctl = await connect_control(control_port)
    c = await connect_client(port=proxy_port)
    await c.appstart()

    async def query_device_info():
        await c.send(b"\x16\x0d")
        t0 = time.monotonic()
        while time.monotonic() - t0 < 5:
            p = await c.recv(1)
            if p is None:
                return None
            if p[0] == 0x0D:
                return p
        return None

    async def query_battery():
        await c.send(b"\x14")
        t0 = time.monotonic()
        while time.monotonic() - t0 < 5:
            p = await c.recv(1)
            if p is None:
                return None
            if p[0] == 0x0C:
                return p
        return None

    async def factory_reset():
        # The literal "reset" argument is required by the firmware.
        await c.send(b"\x33reset")
        t0 = time.monotonic()
        while time.monotonic() - t0 < 5:
            p = await c.recv(1)
            if p is None:
                return None
            if p[0] in (0x00, 0x01):
                return p
        return None

    # A reset first guarantees both caches start cold, whatever earlier tests
    # left populated.
    if await factory_reset() is None:
        fail("FACTORY_RESET got no reply")
        await c.close()
        await ctl.close()
        return
    base = (await ctl.stats()).get("cmd_counts", {})

    # First read of each hits the radio; the second is served from cache.
    await query_device_info()
    await query_battery()
    await query_device_info()
    await query_battery()
    warm = (await ctl.stats()).get("cmd_counts", {})
    di_reads = warm.get("0x16", 0) - base.get("0x16", 0)
    bat_reads = warm.get("0x14", 0) - base.get("0x14", 0)
    if (di_reads, bat_reads) == (1, 1):
        ok("each blob was read from the radio once, then served from cache")
    else:
        fail(
            f"expected one radio read each while warming, saw "
            f"DEVICE_INFO={di_reads} BATTERY={bat_reads}"
        )

    # The reset clears both caches, so the next read goes to the radio again.
    if await factory_reset() is None:
        fail("the second FACTORY_RESET got no reply")
    await query_device_info()
    await query_battery()
    after = (await ctl.stats()).get("cmd_counts", {})
    di_reread = after.get("0x16", 0) - warm.get("0x16", 0)
    bat_reread = after.get("0x14", 0) - warm.get("0x14", 0)
    if (di_reread, bat_reread) == (1, 1):
        ok("FACTORY_RESET invalidated the cached DEVICE_INFO and BATTERY")
    else:
        fail(
            f"stale blobs after FACTORY_RESET: DEVICE_INFO rereads={di_reread} "
            f"BATTERY rereads={bat_reread}"
        )

    await c.close()
    await ctl.close()


async def test_get_contact_by_key(proxy_port):
    """Test 32: a GET_CONTACT_BY_KEY (0x1e) reply reaches only its own client.

    `MyMesh::handleCmdFrame` answers CMD_GET_CONTACT_BY_KEY with
    `writeContactRespFrame(RESP_CODE_CONTACT, *contact)` -- code 0x03, the very
    frame an iterator step of a GET_CONTACTS reply carries. Matching a reply to
    its request has to stay command-relative or a lookup's answer can be taken
    for a stream frame (or the other way round): the 0x03 then falls through to
    the broadcast, the requester is failed with ERROR after the in-flight
    timeout, and the ghost filed for the lookup -- a command whose reply is a
    0x03 -- can never claim its own answer, so it sits for GHOST_TTL eating the
    next client's reply. From the app that cascade reads as "everything is
    broken", including the OK to SEND_CHAN_MSG.

    Both halves are asserted, because they fail differently: the lookup is one
    client's answer and must not reach another, and the slot must be free right
    after it, not still held or haunted.
    """
    log.info("--- Test 31: GET_CONTACT_BY_KEY is a request, not a stream ---")

    # Must match DEFAULT_CONTACTS[0] in the fake radio.
    alpha_key = bytes.fromhex(
        "aabbccdd0011aabbccdd0011aabbccdd0011aabbccdd0011aabbccdd0011aabb"
    )

    requester = await connect_client(port=proxy_port)
    await requester.appstart()
    idle = await connect_client(port=proxy_port)
    await idle.appstart()
    other = await connect_client(port=proxy_port)
    await other.appstart()

    async def next_contact(client, span):
        """The next RESP_CODE_CONTACT frame this client sees, or None."""
        t0 = time.monotonic()
        while time.monotonic() - t0 < span:
            p = await client.recv(1.0)
            if p is None:
                return None
            if len(p) and p[0] == 0x03:
                return p
        return None

    problems = []

    frame = await requester.get_contact_by_key(alpha_key)
    if frame is None:
        problems.append("the lookup was never answered with RESP_CODE_CONTACT")
    elif contact_name(frame) != "Alpha Test":
        problems.append(
            f"the lookup answered {contact_name(frame)!r}, expected 'Alpha Test'"
        )

    # A lookup is one client's answer, not a push. A single-client test cannot
    # see the difference, which is exactly why the bug survived one.
    if await next_contact(idle, 2.0) is not None:
        problems.append("an idle client received another client's CONTACT lookup reply")

    # And the request really finished. Either it is still sitting in the slot
    # (so this poll queues behind it) or its ghost is, and then the ghost eats
    # the CURRENT_TIME: neither completes inside the window.
    await other.send(b"\x05")
    t0 = time.monotonic()
    answered = False
    while time.monotonic() - t0 < 3:
        p = await other.recv(1.0)
        if p is None:
            break
        if len(p) and p[0] == 0x09:
            answered = True
            break
    if not answered:
        problems.append(
            "a second client's CURRENT_TIME was not answered: the lookup still "
            "held the slot, or its ghost swallowed this reply"
        )

    for c in (requester, idle, other):
        await c.close()

    if problems:
        fail("; ".join(problems))
    ok("the lookup answered its own client only, and left the slot free")


async def test_resend_echoes_once(proxy_port):
    """Test 33: a resent channel message is echoed -- and stored -- once.

    A resend is not a new message: the client reuses the timestamp, which is
    what the firmware mixes into the payload hash on purpose, and the radio's
    `wasSeen` table drops the copy before any companion interface is reached.
    One echo per attempt therefore invents a frame no radio would have emitted,
    and one stored row per attempt hands the message to a reconnecting client
    twice.

    Asserted from both sides, because neither alone shows the bug: a second,
    idle client watching the broadcast sees the live duplicate, and a client
    with a cursor behind the log sees the stored one.
    """
    log.info("--- Test 38: a resent channel message is echoed once ---")
    text = "Resend dedupe probe"
    needle = text.encode()
    # Fixed rather than "now", so the two sends below are byte-identical, which
    # is exactly what a client retransmits (`channel_chat_screen.dart`'s
    # transmit() passes the message's own timestamp again).
    ts = int(time.time()) - 2

    # The observer's cursor is parked before the sends, so its reconnect can
    # only be replaying what this test stores.
    obs = await connect_client(port=proxy_port)
    await obs.appstart("resend-observer")
    await obs.close()
    await asyncio.sleep(0.5)

    watcher = await connect_client(port=proxy_port)
    await watcher.appstart()
    sender = await connect_client(port=proxy_port)
    await sender.appstart("resend-sender")

    first = await sender.send_chan_msg(0, text, ts=ts)
    resent = await sender.send_chan_msg(0, text, ts=ts)
    if first is None or resent is None:
        for c in (sender, watcher):
            await c.close()
        fail("SEND_CHAN_MSG did not produce OK")
        return
    ok("the message and its resend were both accepted by the radio")

    live = []
    t0 = time.monotonic()
    while time.monotonic() - t0 < 2.0:
        p = await watcher.recv(0.2)
        if p and is_channel_frame(p) and needle in p:
            live.append(p)
    await sender.close()
    await watcher.close()

    if len(live) == 1:
        ok("the idle client saw the resent message broadcast once")
    else:
        fail(f"the idle client saw the resent message {len(live)} times, expected 1")

    again = await connect_client(port=proxy_port)
    await again.appstart("resend-observer")
    frames = await drain_history_until_no_more(again)
    await again.close()

    replayed = [f for f in frames if needle in f]
    if len(replayed) == 1:
        ok("the log holds the resent message once, so replay is not duplicated")
    else:
        fail(f"replay handed the resent message over {len(replayed)} times, expected 1")

    # The surviving row must be the echoed frame itself: suppressing the second
    # echo is not allowed to change what a client receives.
    if replayed and msg_text(replayed[0]).startswith("FakeRadio: "):
        ok("the replayed frame is still the radio-prefixed echo")
    elif replayed:
        fail(f"replayed frame lost its sender prefix: {msg_text(replayed[0])[:40]!r}")


# --- Harness ---

_fake_proc = None
_fake_pid = None
_fake_port = None
_control_port = None
_proxy_proc = None
_proxy_port = None
_proxy_args = None
_data_dir = None


async def start_fake(port=None):
    global _fake_proc, _fake_pid, _fake_port, _control_port
    if port is None:
        _fake_port = await find_free_port()
    else:
        _fake_port = port
    _control_port = await find_free_port()
    _fake_proc = await asyncio.create_subprocess_exec(
        sys.executable, os.path.join(os.path.dirname(__file__), "fake_radio.py"),
        "--port", str(_fake_port), "--inject",
        "--control-port", str(_control_port),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    _fake_pid = _fake_proc.pid
    log.info("Fake radio started (PID=%d, port=%d, control=%d)", _fake_pid, _fake_port, _control_port)
    if not await wait_for_port(_fake_port):
        log.error("Fake radio did not come up")
        return False
    if not await wait_for_port(_control_port):
        log.error("Fake radio control channel did not come up")
        return False
    return True


async def stop_fake():
    global _fake_proc, _fake_pid
    if _fake_proc and _fake_proc.returncode is None:
        _fake_proc.terminate()
        try:
            await asyncio.wait_for(_fake_proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            _fake_proc.kill()
            await _fake_proc.wait()
    _fake_proc = None
    _fake_pid = None


async def _pipe_reader(stream, prefix):
    while True:
        line = await stream.readline()
        if not line:
            break
        log.info("%s %s", prefix, line.decode("utf-8", "replace").rstrip())


async def spawn_proxy():
    """Start the proxy with the suite's arguments, waiting until it listens.

    Used to restart it as well, always against the same data directory: the
    history cursor is expected to survive a restart, which is what makes the
    per-listener kv_store entry worth persisting.
    """
    global _proxy_proc
    _proxy_proc = await asyncio.create_subprocess_exec(
        *_proxy_args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    log.info("Proxy started (PID=%d)", _proxy_proc.pid)
    asyncio.create_task(_pipe_reader(_proxy_proc.stdout, "[proxy:out]"))
    asyncio.create_task(_pipe_reader(_proxy_proc.stderr, "[proxy:err]"))

    if not await wait_for_port(_proxy_port):
        log.error("Proxy did not come up on port %d", _proxy_port)
        return False
    log.info("Proxy ready on port %d", _proxy_port)
    return True


async def stop_proxy():
    global _proxy_proc
    if _proxy_proc and _proxy_proc.returncode is None:
        _proxy_proc.terminate()
        try:
            await asyncio.wait_for(_proxy_proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            _proxy_proc.kill()
            await _proxy_proc.wait()
    _proxy_proc = None

async def _start_session():
    """Start the shared fake radio and proxy once for the whole run.

    The suite is one long asyncio session: tests share the radio's state (a
    channel created in test 2 is still there in test 20) and the fixtures below
    simply hand each test the current ports. The data directory is wiped once,
    so a replay cursor has somewhere to resume from and every client identity
    starts unseen.
    """
    global _proxy_port, _proxy_args, _data_dir
    data_dir = f"/tmp/cs_test_proxy/{os.getpid()}"
    subprocess.run(["rm", "-rf", data_dir], check=False)
    _data_dir = data_dir

    if not await start_fake():
        await stop_fake()
        pytest.fail("fake radio did not start")

    _proxy_port = await find_free_port()
    proxy_bin = os.path.join(
        os.path.dirname(__file__), "..", "target", "debug", "coresplitter"
    )
    _proxy_args = [
        proxy_bin,
        "--tcp-backend-host", "127.0.0.1",
        "--tcp-backend-port", str(_fake_port),
        "--tcp-frontend-port", str(_proxy_port),
        "--data-dir", data_dir,
        "--log-level", "info",
        # The suite waits this out in the late-reply test; the shipped default
        # of 15 s would only make it slow, and the timeout path is the same
        # either way.
        "--in-flight-timeout", "6",
    ]
    if not await spawn_proxy():
        await stop_fake()
        pytest.fail("proxy did not start")


@pytest_asyncio.fixture(scope="session", autouse=True)
async def harness():
    """The shared fake radio + proxy, torn down after the last test.

    Autouse so even a single `-k`-selected test gets the full environment. Some
    tests restart the radio or the proxy mid-run, so teardown stops whatever is
    running at the end rather than a handle captured at setup.
    """
    await _start_session()
    yield
    await stop_proxy()
    await stop_fake()


# Function-scoped on purpose: test_reconnect_tcp restarts the fake radio (with a
# new control port) and test_client_request_jumps_sync_queue restarts the proxy,
# so each test must read the *current* port rather than a cached one.
@pytest.fixture
def proxy_port(harness):
    return _proxy_port


@pytest.fixture
def control_port(harness):
    return _control_port




if __name__ == "__main__":
    # Running the file directly would import 30 test functions and exit 0
    # without running any of them. Fail loudly instead.
    raise SystemExit(
        "This suite runs under pytest:\n"
        "    uv run pytest tools/test_proxy.py\n"
        "    uv run pytest tools/test_proxy.py -k <name>"
    )
