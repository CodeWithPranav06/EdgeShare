```python
"""Coordinator-side TCP server for EdgeShare."""
import socket
import threading
import time

import config
from networking.protocol import recv_msg, send_msg
from compute.coordinator import TaskCoordinator
from monitoring.resource_monitor import get_status


def send_next_task(conn, worker_id, coordinator, lock):
    """Send the next queued task to a connected worker."""
    with lock:
        task = coordinator.next_task(worker_id)

    if task is not None:
        send_msg(conn, {
            "type": "TASK",
            "task_id": task["task_id"],
            "task_type": task["task_type"],
            "params": task.get("params", {}),
        }, task.get("payload", b""))


def handle_worker(conn, registry, coordinator, lock, first_message=None):
    worker_id = None

    try:
        if first_message is None:
            header, payload = recv_msg(conn)
        else:
            header, payload = first_message

        while True:
            msg_type = header.get("type")

            if msg_type == "REGISTER":
                worker_id = header["worker_id"]

                with lock:
                    registry[worker_id] = conn
                    coordinator.register_worker(worker_id, get_status())

                send_msg(conn, {
                    "type": "ACK",
                    "worker_id": worker_id
                })

            elif msg_type in ("HEARTBEAT", "STATUS"):
                if worker_id is None:
                    send_msg(conn, {
                        "type": "ERROR",
                        "reason": "Register first"
                    })
                else:
                    status = header.get("status") or get_status()
                    coordinator.heartbeat(worker_id, status)
                    send_msg(conn, {
                        "type": "ACK",
                        "worker_id": worker_id
                    })

            elif msg_type == "RESULT":
                if worker_id is None:
                    send_msg(conn, {
                        "type": "ERROR",
                        "reason": "Register first"
                    })
                else:
                    task_id = header.get("task_id")

                    with lock:
                        coordinator.complete_task(
                            worker_id,
                            task_id,
                            payload,
                            header.get("processing_s", 0)
                        )

                    send_msg(conn, {
                        "type": "ACK",
                        "task_id": task_id
                    })

            else:
                send_msg(conn, {
                    "type": "ERROR",
                    "reason": "Unknown message type"
                })

            if worker_id is not None:
                send_next_task(conn, worker_id, coordinator, lock)

            header, payload = recv_msg(conn)

    except (ConnectionError, OSError, KeyError, ValueError):
        pass

    finally:
        conn.close()

        if worker_id is not None:
            with lock:
                if registry.get(worker_id) is conn:
                    registry.pop(worker_id, None)


def handle_submitter(conn, coordinator, lock, first_header, first_payload):
    """Accept one image, queue it, and wait for its result."""
    task_id = first_header.get("task_id")

    task = {
        "task_id": task_id,
        "task_type": first_header.get("task_type", "resize"),
        "params": first_header.get("params", {}),
        "payload": first_payload,
    }

    try:
        with lock:
            coordinator.submit_tasks([task])

        send_msg(conn, {
            "type": "ACK",
            "task_id": task_id
        })

        deadline = time.monotonic() + 300

        while time.monotonic() < deadline:
            with lock:
                result = coordinator.results.get(task_id)

            if result is not None:
                send_msg(conn, {
                    "type": "RESULT",
                    "task_id": task_id,
                    "processing_s": result.get("processing_s", 0)
                }, result["result"])
                return

            time.sleep(0.2)

        send_msg(conn, {
            "type": "ERROR",
            "reason": "Timed out waiting for worker"
        })

    except (ConnectionError, OSError, KeyError, ValueError) as exc:
        print(f"[server] Submission failed: {exc}")

    finally:
        conn.close()


def start_server(host=config.COORDINATOR_HOST,
                 port=config.COORDINATOR_PORT,
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

            threading.Thread(
                target=dispatch_connection,
                args=(conn, registry, coordinator, lock),
                daemon=True
            ).start()

    finally:
        srv.close()


def dispatch_connection(conn, registry, coordinator, lock):
    """Distinguish Windows submissions from worker registrations."""
    try:
        header, payload = recv_msg(conn)
    except (ConnectionError, OSError, ValueError):
        conn.close()
        return

    if header.get("type") == "SUBMIT":
        handle_submitter(conn, coordinator, lock, header, payload)
    else:
        handle_worker(
            conn, registry, coordinator, lock,
            first_message=(header, payload)
        )


if __name__ == "__main__":
    start_server()
```
