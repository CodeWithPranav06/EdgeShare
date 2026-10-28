"""Worker-side TCP helper (member A)."""
import socket

import config


def connect(host, port=config.COORDINATOR_PORT):
    """Open a TCP connection to the coordinator with a clear error message."""
    try:
        return socket.create_connection((host, port), timeout=5)
    except OSError as e:
        raise SystemExit(f"Connection failed ({e}). Start the coordinator first, "
                         f"and check the IP, port and firewall.")
