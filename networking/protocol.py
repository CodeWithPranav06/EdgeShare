"""Message framing over TCP.

TCP is a byte stream, so every message is sent as:
    [4 bytes: header length][JSON header][payload bytes]
The header always has "type" and "payload_size".

Message types: REGISTER, ACK, STATUS, TASK, RESULT, HEARTBEAT, ERROR, SUBMIT
TASK, RESULT and SUBMIT headers carry "sha256" of their payload; receivers check it
with verify_payload() so corrupted transfers are detected.
"""
import hashlib
import json
import struct

import config

MESSAGE_TYPES = {"REGISTER", "ACK", "STATUS", "TASK", "RESULT", "HEARTBEAT", "ERROR", "SUBMIT"}


def _recv_exact(sock, n):
    """Read exactly n bytes or raise ConnectionError."""
    data = bytearray(n)
    view = memoryview(data)
    received = 0
    while received < n:
        count = sock.recv_into(view[received:], n - received)
        if not count:
            raise ConnectionError("connection closed")
        received += count
    return bytes(data)


def sha256_hex(data):
    return hashlib.sha256(data).hexdigest()


def verify_payload(header, payload):
    """True when the header has no checksum or the checksum matches the payload."""
    expected = header.get("sha256")
    return expected is None or expected == sha256_hex(payload)


def send_msg(sock, header, payload=b""):
    header = dict(header)
    header["payload_size"] = len(payload)
    raw = json.dumps(header).encode("utf-8")
    sock.sendall(struct.pack("!I", len(raw)) + raw)
    if payload:
        sock.sendall(payload)


def recv_msg(sock):
    """Return (header_dict, payload_bytes). Raises ValueError on a malformed message."""
    (hlen,) = struct.unpack("!I", _recv_exact(sock, 4))
    if hlen > config.MAX_HEADER_BYTES:
        raise ValueError("header too large (%d bytes)" % hlen)
    header = json.loads(_recv_exact(sock, hlen).decode("utf-8"))
    if not isinstance(header, dict) or "type" not in header:
        raise ValueError("header must be a JSON object with a type")
    size = header.get("payload_size", 0)
    if not isinstance(size, int) or not 0 <= size <= config.MAX_PAYLOAD_BYTES:
        raise ValueError("bad payload_size: %r" % (size,))
    payload = _recv_exact(sock, size)
    return header, payload
