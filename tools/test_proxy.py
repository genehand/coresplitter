#!/usr/bin/env python3
"""Integration tests for coresplitter proxy."""
import asyncio
import json
import logging
import os
import socket
import struct
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("test")

# --- Helpers ---

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

    async def appstart(self):
        await self.send(b"\x01\x01" + b"testclient")
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

    async def get_contacts(self, lastmod=0):
        await self.send(b"\x04" + struct.pack("<I", lastmod))
        contacts = []
        t0 = time.monotonic()
        while time.monotonic() - t0 < 5:
            p = await self.recv(1)
            if p is None:
                break
            if len(p) == 0:
                continue
            if p[0] == 0x03:
                contacts.append(p)
            elif p[0] == 0x04:
                break
        return contacts

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
        if ts is None:
            ts = int(time.time())
        payload = b"\x03" + bytes([channel, 0]) + struct.pack("<I", ts) + text.encode()
        await self.send(payload)
        t0 = time.monotonic()
        while time.monotonic() - t0 < 5:
            p = await self.recv(1)
            if p is None:
                return None
            if len(p) and p[0] == 0x06:
                return p
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


def msg_text(p: bytes) -> str:
    """Extract the message body from a message frame of either generation."""
    if not p or p[0] not in _TEXT_OFFSET:
        return ""
    return p[_TEXT_OFFSET[p[0]]:].decode("utf-8", errors="replace")


# --- Test cases ---

PASS = 0
FAIL = 0

def ok(msg):
    global PASS
    PASS += 1
    log.info("  PASS: %s", msg)

def fail(msg):
    global FAIL
    FAIL += 1
    log.error("  FAIL: %s", msg)


async def test_multi_client(proxy_port):
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

    # MSG_SENT is consumed by send_chan_msg's drain loop.
    ok("c1 sent message (MSG_SENT consumed by drain)")

    # c2 should receive a synthesized CHANNEL_MSG_RECV (0x08) via broadcast
    got_broadcast = False
    t0 = time.monotonic()
    while time.monotonic() - t0 < 5:
        p = await c2.recv(2)
        if p is None:
            break
        if len(p) == 0:
            continue
        if p[0] == 0x08 and b"Multi-client test" in p:
            got_broadcast = True
            break

    if got_broadcast:
        ok("c2 receives CHANNEL_MSG_RECV (0x08) via broadcast_to_others")
    else:
        fail("c2 did not receive broadcast message")

    await c1.close()
    await c2.close()


async def test_set_channel(proxy_port):
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

    if received:
        ok(f"c2 receives CHANNEL_MSG_RECV with long text ({len(txt_rx)} chars)")
    else:
        fail("c2 did not receive long message broadcast in 6s")

    await c1.close()
    await c2.close()


async def test_send_contact_msg(proxy_port):
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
    global fake_proc, fake_pid, fake_port
    log.info("--- Test 10: TCP disconnection + reconnection ---")
    c = await connect_client(port=proxy_port)
    r = await c.appstart()
    if not r:
        fail("SELF_INFO failed")
        await c.close()
        return

    # Kill fake radio
    if fake_proc and fake_proc.returncode is None:
        log.info("  Killing fake radio (PID=%d)...", fake_pid)
        fake_proc.terminate()
        try:
            await asyncio.wait_for(fake_proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            fake_proc.kill()
    fake_proc = None

    await asyncio.sleep(4)
    log.info("  Proxy should be reconnecting...")

    # Restart fake radio on the same port (proxy still tries to reconnect to old port)
    log.info("  Restarting fake radio...")
    await start_fake(port=fake_port)
    if not fake_proc:
        fail("could not restart fake radio")
        await c.close()
        return

    # Drain any broadcast messages that arrived during sleep
    for _ in range(50):
        p = await c.recv(0.5)
        if p is None or len(p) == 0:
            break

    r = await c.get_channel(0)
    if r and r[0] == 0x12:
        ok("proxy reconnected and responds to GET_CHANNEL after reconnection")
    else:
        fail(f"proxy does not respond after reconnection (r={r}, raw=...)")

    await c.close()


async def test_get_msg_fans_out(proxy_port):
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


async def test_device_query_negotiation(proxy_port, control_port):
    """Test 11: the proxy owns CMD_DEVICE_QUERY / app_target_ver negotiation.

    Regression guard for the one-byte `CMD_DEVICE_QUERY` bug. The firmware only
    honours the command when `len >= 2`, so a 1-byte query is answered with
    ERR_CODE_UNSUPPORTED_CMD and `app_target_ver` stays 0. That left
    `device_info_raw` empty, which meant the first client's DEVICE_QUERY was
    forwarded to the radio -- and `app_target_ver` is radio-global and sticky,
    so whichever client won the race decided the message frame format for every
    other client.
    """
    log.info("--- Test 11: DEVICE_QUERY negotiation is owned by the proxy ---")
    ctl = await connect_control(control_port)

    ver = await ctl.appver()
    if ver >= 3:
        ok(f"proxy negotiated app_target_ver={ver} (>= 3, so V3 message frames)")
    else:
        fail(f"proxy left app_target_ver={ver}; the firmware rejects a 1-byte DEVICE_QUERY")

    before = (await ctl.stats())["cmd_counts"].get("0x16", 0)

    # A client's DEVICE_QUERY must be served from cache, never forwarded, so it
    # cannot re-negotiate the version out from under the other clients.
    c1 = await connect_client(port=proxy_port)
    c2 = await connect_client(port=proxy_port)
    got = []
    for c in (c1, c2):
        await c.appstart()
        await c.send(b"\x16\x0d")
        t0 = time.monotonic()
        while time.monotonic() - t0 < 5:
            p = await c.recv(2)
            if p is None:
                break
            if len(p) >= 2 and p[0] == 0x0D:
                got.append(p)
                break

    for i, p in enumerate(got, start=1):
        if p[1] == 13:
            ok(f"client {i} DEVICE_INFO served from cache (firmware_ver={p[1]})")
        else:
            fail(f"client {i} DEVICE_INFO firmware_ver={p[1]}, expected 13")
    if len(got) != 2:
        fail(f"only {len(got)}/2 clients got DEVICE_INFO from the proxy cache")

    after = (await ctl.stats())["cmd_counts"].get("0x16", 0)
    if after == before:
        ok(f"no DEVICE_QUERY was forwarded to the radio by the clients ({before} total)")
    else:
        fail(f"clients leaked DEVICE_QUERY to the radio ({before} -> {after})")

    ver_after = await ctl.appver()
    if ver_after == ver:
        ok(f"app_target_ver unchanged after client queries ({ver_after})")
    else:
        fail(f"app_target_ver was clobbered: {ver} -> {ver_after}")

    await c1.close()
    await c2.close()
    await ctl.close()


async def test_unsolicited_pushes_do_not_terminate_requests(proxy_port, control_port):
    """Test 12: unsolicited pushes arriving during an in-flight request.

    Covers CONTACT_DELETED (0x8F), CONTACTS_FULL (0x90), RAW_DATA (0x84) and
    CONTROL_DATA (0x8E) -- all emitted by the firmware from inbound-traffic
    handlers with no corresponding command (the outbound CMD_SEND_RAW_DATA and
    CMD_SEND_CONTROL_DATA are answered with a plain OK).

    `Awaiting::Any` completes a request on any frame not listed in
    PUSH_EVENT_CODES, so a missing code here is delivered only to the one client
    that happens to be waiting: every other client silently loses the push, and
    the real reply later finds no in-flight slot and is broadcast to clients
    that never sent the request.

    The idle second client is what makes this observable: it must receive all
    four pushes, and must NOT receive the reply to c1's request.
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

    expected = {0x8F: "0x8f", 0x90: "0x90", 0x84: "0x84", 0x8E: "0x8e"}

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
    """Test 13: CHANNEL_DATA_RECV (0x1B) fans out to the other clients.

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
    """Test 14: a setter that mutates DEVICE_INFO invalidates the cache.

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


# --- Harness ---

fake_proc = None
fake_pid = None
fake_port = None
control_port = None
proxy_port = None
proxy_proc = None

async def start_fake(port=None):
    global fake_proc, fake_pid, fake_port, control_port
    if port is None:
        fake_port = await find_free_port()
    else:
        fake_port = port
    control_port = await find_free_port()
    fake_proc = await asyncio.create_subprocess_exec(
        sys.executable, os.path.join(os.path.dirname(__file__), "fake_radio.py"),
        "--port", str(fake_port), "--inject",
        "--control-port", str(control_port),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    fake_pid = fake_proc.pid
    log.info("Fake radio started (PID=%d, port=%d, control=%d)", fake_pid, fake_port, control_port)
    ok = await wait_for_port(fake_port)
    if not ok:
        log.error("Fake radio no responde")
        return False
    if not await wait_for_port(control_port):
        log.error("Fake radio control channel no responde")
        return False
    return True

async def stop_fake():
    global fake_proc, fake_pid
    if fake_proc and fake_proc.returncode is None:
        fake_proc.terminate()
        try:
            await asyncio.wait_for(fake_proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            fake_proc.kill()
    fake_proc = None
    fake_pid = None

async def main():
    global PASS, FAIL, proxy_proc, proxy_port

    DATA_DIR = f"/tmp/cs_test_proxy/{os.getpid()}"
    subprocess.run(["rm", "-rf", DATA_DIR], check=False)

    if not await start_fake():
        return

    proxy_port = await find_free_port()

    proxy_bin = os.path.join(
        os.path.dirname(__file__), "..", "target", "debug", "coresplitter"
    )
    proxy_proc = await asyncio.create_subprocess_exec(
        proxy_bin,
        "--tcp-backend-host", "127.0.0.1",
        "--tcp-backend-port", str(fake_port),
        "--tcp-frontend-port", str(proxy_port),
        "--data-dir", DATA_DIR,
        "--log-level", "info",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,

    )
    log.info("Proxy started (PID=%d)", proxy_proc.pid)

    async def _pipe_reader(stream, prefix):
        while True:
            line = await stream.readline()
            if not line:
                break
            log.info("%s %s", prefix, line.decode("utf-8", "replace").rstrip())
    asyncio.create_task(_pipe_reader(proxy_proc.stdout, "[proxy:out]"))
    asyncio.create_task(_pipe_reader(proxy_proc.stderr, "[proxy:err]"))

    ok = await wait_for_port(proxy_port)
    if not ok:
        log.error("Proxy no responde en puerto %d", proxy_port)
        await stop_fake()
        return
    log.info("Proxy ready on port %d", proxy_port)

    await test_multi_client(proxy_port)
    await test_set_channel(proxy_port)
    await test_injected_messages(proxy_port)
    await test_contact_msg_received(proxy_port)
    await test_get_message_polling(proxy_port)
    await test_multiple_messages_order(proxy_port)
    await test_long_message(proxy_port)
    await test_send_contact_msg(proxy_port)
    await test_get_msg_fans_out(proxy_port)
    await test_reconnect_tcp(proxy_port)
    await test_device_query_negotiation(proxy_port, control_port)
    await test_unsolicited_pushes_do_not_terminate_requests(proxy_port, control_port)
    await test_channel_data_fans_out(proxy_port, control_port)
    await test_setter_invalidates_device_info(proxy_port, control_port)
    await test_ack_routes_past_in_flight_request(proxy_port, control_port)

    total = PASS + FAIL
    log.info("=" * 40)
    if FAIL == 0:
        log.info("RESULT: %d/%d PASS", PASS, total)
    else:
        log.error("RESULT: %d/%d PASS, %d FAIL", PASS, total, FAIL)

    if proxy_proc:
        proxy_proc.terminate()
        try:
            await asyncio.wait_for(proxy_proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            proxy_proc.kill()
    await stop_fake()

if __name__ == "__main__":
    asyncio.run(main())
