"""Message framing over TCP.

TCP is a byte stream, so every message is sent as:
    [4 bytes: header length][JSON header][payload bytes]
The header always has "type" and "payload_size".

Message types: REGISTER, ACK, STATUS, TASK, RESULT, HEARTBEAT, ERROR
"""
import json
import struct

MESSAGE_TYPES = {"REGISTER", "ACK", "STATUS", "TASK", "RESULT", "HEARTBEAT", "ERROR"}


def _recv_exact(sock, n):
    """Read exactly n bytes or raise ConnectionError."""
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("connection closed")
        data += chunk
    return data


def send_msg(sock, header, payload=b""):
    header = dict(header)
    header["payload_size"] = len(payload)
    raw = json.dumps(header).encode("utf-8")
    sock.sendall(struct.pack("!I", len(raw)) + raw + payload)


def recv_msg(sock):
    """Return (header_dict, payload_bytes)."""
    (hlen,) = struct.unpack("!I", _recv_exact(sock, 4))
    header = json.loads(_recv_exact(sock, hlen).decode("utf-8"))
    payload = _recv_exact(sock, header.get("payload_size", 0))
    return header, payload
