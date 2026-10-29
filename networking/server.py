import socket
import threading
import time

import config
from networking.protocol import recv_msg, send_msg
from compute.coordinator import TaskCoordinator
from monitoring.resource_monitor import get_status

def handle_worker(conn, registry, coordinator, lock, first=None):
    worker_id = None
    header, payload = first if first else (None, None)

    try:
        while True:
            if header is None:
                header, payload = recv_msg(conn)

            msg_type = header.get("type")

            if msg_type == "REGISTER":
                worker_id = header["worker_id"]
                with lock:
                    registry[worker_id] = conn
                    coordinator.register_worker(worker_id, get_status())
                send_msg(conn, {"type": "ACK", "worker_id": worker_id})

            elif msg_type in ("HEARTBEAT", "STATUS"):
                if worker_id is None:
                    send_msg(conn, {"type": "ERROR", "reason": "Register first"})
                else:
                    coordinator.heartbeat(worker_id, header.get("status") or get_status())
                    send_msg(conn, {"type": "ACK", "worker_id": worker_id})

            elif msg_type == "RESULT":
                if worker_id is None:
                    send_msg(conn, {"type": "ERROR", "reason": "Register first"})
                else:
                    coordinator.complete_task(
                        worker_id, header.get("task_id"), payload,
                        header.get("processing_s", 0)
                    )
                    send_msg(conn, {"type": "ACK", "task_id": header.get("task_id")})

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

            header, payload = recv_msg(conn)

    except (ConnectionError, OSError, KeyError, ValueError):
        pass
    finally:
        conn.close()
        if worker_id:
            with lock:
                registry.pop(worker_id, None)


def handle_submitter(conn, coordinator, lock, header, payload):
    try:
        task_id = header["task_id"]
        task = {
            "task_id": task_id,
            "task_type": header.get("task_type", "resize"),
            "params": header.get("params", {}),
            "payload": payload,
        }

        with lock:
            coordinator.submit_tasks([task])

        send_msg(conn, {"type": "ACK", "task_id": task_id})

        deadline = time.time() + 300
        while time.time() < deadline:
            with lock:
                result = coordinator.results.get(task_id)

            if result is not None:
                send_msg(conn, {
                    "type": "RESULT",
                    "task_id": task_id,
                    "processing_s": result.get("processing_s", 0),
                }, result["result"])
                return

            time.sleep(0.2)

        send_msg(conn, {"type": "ERROR", "reason": "Timed out waiting for worker"})

    except (ConnectionError, OSError, KeyError, ValueError) as exc:
        print(f"[server] submitter error: {exc}")
    finally:
        conn.close()


def start_server(host=config.COORDINATOR_HOST, port=config.COORDINATOR_PORT,
                 ready=None, stop=None, registry=None):
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

            def dispatch(c=conn):
                try:
                    first_header, first_payload = recv_msg(c)
                    if first_header.get("type") == "SUBMIT":
                        handle_submitter(c, coordinator, lock, first_header, first_payload)
                    else:
                        handle_worker(
                            c, registry, coordinator, lock,
                            first=(first_header, first_payload)
                        )
                except (ConnectionError, OSError, ValueError):
                    c.close()

            threading.Thread(target=dispatch, daemon=True).start()

    finally:
        srv.close()


if __name__ == "__main__":
    start_server()