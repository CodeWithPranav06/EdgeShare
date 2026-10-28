"""Message framing over TCP.

TCP is a byte stream, so every message is sent as:
    [4 bytes: header length][JSON header][payload bytes]
The header always has "type" and "payload_size".

Message types: REGISTER, ACK, STATUS, TASK, RESULT, HEARTBEAT, ERROR
"""
import hashlib
import json
import os
import struct

MESSAGE_TYPES = {"REGISTER", "ACK", "STATUS", "TASK", "RESULT", "HEARTBEAT", "ERROR", "FILE"}


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


def sha256_hex(data):
    return hashlib.sha256(data).hexdigest()


def send_file(sock, path):
    """Send a file as one FILE message with its SHA-256 in the header."""
    with open(path, "rb") as f:
        data = f.read()
    send_msg(sock, {"type": "FILE", "name": os.path.basename(path),
                    "sha256": sha256_hex(data)}, data)


def recv_file(sock, dest_dir):
    """Receive a FILE message, verify its SHA-256, save it, return the path."""
    header, payload = recv_msg(sock)
    if header["type"] != "FILE":
        raise ValueError(f"expected FILE, got {header['type']}")
    if sha256_hex(payload) != header["sha256"]:
        raise ValueError("checksum mismatch: file corrupted in transfer")
    os.makedirs(dest_dir, exist_ok=True)
    path = os.path.join(dest_dir, os.path.basename(header["name"]))
    with open(path, "wb") as f:
        f.write(payload)
    return path
