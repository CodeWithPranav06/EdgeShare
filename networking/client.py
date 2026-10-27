"""Worker-side TCP client (member A). Starter version: connects and registers."""
import socket

import config
from networking.protocol import recv_msg, send_msg


def connect_and_register(host, port=config.COORDINATOR_PORT, worker_id="worker-1"):
    try:
        sock = socket.create_connection((host, port), timeout=5)
    except OSError as e:
        raise SystemExit(f"Connection failed ({e}). Is the server running first, "
                         f"and is the IP/port right?")
    send_msg(sock, {"type": "REGISTER", "worker_id": worker_id})
    header, _ = recv_msg(sock)
    print(f"[client] got {header['type']}")
    return sock


if __name__ == "__main__":
    import sys
    connect_and_register(sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1")
