
"""Coordinator-side TCP server for EdgeShare."""
import socket
import threading
import config
from networking.protocol import recv_msg, send_msg
from compute.coordinator import TaskCoordinator
from monitoring.resource_monitor import get_status


def handle_worker(conn, addr, registry, coordinator, lock):
    worker_id = None
    try:
        while True:
            header, payload = recv_msg(conn)
            msg_type = header.get("type")

            if msg_type == "REGISTER":
                worker_id = header["worker_id"]
                status = get_status()
                with lock:
                    registry[worker_id] = conn
                    coordinator.register_worker(worker_id, status)
                send_msg(conn, {"type": "ACK", "worker_id": worker_id})

            elif msg_type in ("HEARTBEAT", "STATUS"):
                if worker_id is None:
                    send_msg(conn, {"type": "ERROR", "reason": "Register first"})
                    continue
                status = header.get("status")
                if status is None:
                    status = get_status()
                coordinator.heartbeat(worker_id, status)
                send_msg(conn, {"type": "ACK", "worker_id": worker_id})

            elif msg_type == "RESULT":
                if worker_id is None:
                    send_msg(conn, {"type": "ERROR", "reason": "Register first"})
                    continue
                task_id = header.get("task_id")
                coordinator.complete_task(
                    worker_id, task_id, payload,
                    header.get("processing_s", 0)
                )
                send_msg(conn, {"type": "ACK", "task_id": task_id})

            else:
                send_msg(conn, {"type": "ERROR", "reason": "Unknown message type"})

            if worker_id is not None:
                task = coordinator.next_task(worker_id)
                if task is not None:
                    send_msg(conn, {
                        "type": "TASK",
                        "task_id": task["task_id"],
                        "task_type": task["task_type"],
                        "params": task.get("params", {}),
                    }, task.get("payload", b""))

    except (ConnectionError, OSError, KeyError, ValueError):
        pass
    finally:
        conn.close()


def start_server(host=config.COORDINATOR_HOST, port=config.COORDINATOR_PORT,
                 ready=None, stop=None, registry=None):
    """Run until stop is set; retain compatibility with the existing tests."""
    registry = registry if registry is not None else {}
    coordinator = TaskCoordinator()
    lock = threading.RLock()

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port))
    srv.listen()
    srv.settimeout(0.5)
    print(f"[server] listening on {host}:{port}")
    if ready:
        ready.set()

    try:
        while not (stop and stop.is_set()):
            try:
                conn, addr = srv.accept()
            except socket.timeout:
                continue
            threading.Thread(
                target=handle_worker,
                args=(conn, addr, registry, coordinator, lock),
                daemon=True,
            ).start()
    finally:
        srv.close()


if __name__ == "__main__":
    start_server()
