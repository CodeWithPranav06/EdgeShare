"""UDP device discovery (member A).

The coordinator announces itself on UDP port config.DISCOVERY_PORT every
config.DISCOVERY_INTERVAL_S:
    {"service": "edgeshare", "tcp_port": 5000}
A worker started with --discover listens for one announcement and connects over TCP
to the sender's IP. (The original TODO had workers broadcasting, but a worker needs the
coordinator's address to open its TCP connection, so the coordinator announces instead;
workers still register over TCP as before.)

Fallback: some Wi-Fi networks (campus, guest, hotspot isolation) block broadcast. Then
start the worker with the coordinator IP: python main.py worker <coordinator-ip>
"""
import json
import socket
import time

import config

SERVICE = "edgeshare"


def local_ip():
    """Best guess at this computer's LAN IP (no packet is actually sent)."""
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.connect(("8.8.8.8", 80))
        return probe.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        probe.close()


def broadcast_loop(stop, tcp_port=config.COORDINATOR_PORT, port=config.DISCOVERY_PORT,
                   address="<broadcast>", interval=config.DISCOVERY_INTERVAL_S):
    """Coordinator side: announce until `stop` (a threading.Event) is set."""
    message = json.dumps({"service": SERVICE, "tcp_port": tcp_port}).encode("utf-8")
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    try:
        while True:
            try:
                sock.sendto(message, (address, port))
            except OSError:
                pass                              # no network right now; try again later
            if stop.wait(interval):
                break
    finally:
        sock.close()


def find_coordinator(timeout=10, port=config.DISCOVERY_PORT):
    """Worker side: wait for an announcement. Returns (ip, tcp_port) or None."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if hasattr(socket, "SO_REUSEPORT"):           # several workers on one Mac
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    deadline = time.monotonic() + timeout
    try:
        sock.bind(("", port))
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            sock.settimeout(remaining)
            try:
                data, (ip, _) = sock.recvfrom(1024)
            except socket.timeout:
                return None
            try:
                message = json.loads(data.decode("utf-8"))
            except ValueError:
                continue
            if isinstance(message, dict) and message.get("service") == SERVICE:
                return ip, int(message.get("tcp_port", config.COORDINATOR_PORT))
    finally:
        sock.close()
