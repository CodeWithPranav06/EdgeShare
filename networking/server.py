"""Coordinator-side TCP listener (member A). Starter version: accepts workers
and answers REGISTER with ACK. Extend with task sending, heartbeats, etc."""
import socket
import threading

import config
from networking.protocol import recv_msg, send_msg


def handle_worker(conn, addr, registry):
    try:
        while True:
            header, payload = recv_msg(conn)
            if header["type"] == "REGISTER":
                registry[header["worker_id"]] = addr
                send_msg(conn, {"type": "ACK", "worker_id": header["worker_id"]})
            else:
                send_msg(conn, {"type": "ERROR", "reason": "unknown type"})
    except ConnectionError:
        pass
    finally:
        conn.close()


def start_server(host=config.COORDINATOR_HOST, port=config.COORDINATOR_PORT,
                 ready=None, stop=None, registry=None):
    """Run until `stop` (threading.Event) is set. `ready` is set once listening."""
    registry = registry if registry is not None else {}
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port))
    srv.listen()
    srv.settimeout(0.5)
    print(f"[server] listening on {host}:{port}")
    if ready:
        ready.set()
    while not (stop and stop.is_set()):
        try:
            conn, addr = srv.accept()
        except socket.timeout:
            continue
        threading.Thread(target=handle_worker, args=(conn, addr, registry),
                         daemon=True).start()
    srv.close()


if __name__ == "__main__":
    start_server()
