"""Worker-side TCP client for EdgeShare.

    python main.py worker <coordinator-ip> [worker-id]

The worker registers with its resource status and runs tasks one at a time. A separate
heartbeat thread sends HEARTBEAT + fresh status every config.HEARTBEAT_INTERVAL_S, also
while a long task is running, so the coordinator can tell a slow worker from a dead one.
If the connection drops, the worker reconnects (with a new heartbeat thread).
"""
import socket
import threading
import time

import config
from compute.tasks import run_task
from monitoring.resource_monitor import get_status
from networking.protocol import recv_msg, send_msg, sha256_hex, verify_payload

RECONNECT_DELAY_S = 3


class RegistrationRefused(Exception):
    """The coordinator answered REGISTER with an ERROR (e.g. duplicate worker ID)."""


def default_worker_id():
    """This computer's name, so every laptop gets a different ID without typing one."""
    return socket.gethostname().split(".")[0] or "worker"


class WorkerSession:
    """One connection to the coordinator."""

    def __init__(self, sock, worker_id, status_fn=get_status, task_fn=run_task):
        self.sock = sock
        self.worker_id = worker_id
        self.status_fn = status_fn
        self.task_fn = task_fn
        self.send_lock = threading.Lock()     # heartbeats and results share the socket
        self.stopped = threading.Event()

    def send(self, header, payload=b""):
        with self.send_lock:
            send_msg(self.sock, header, payload)

    def register(self):
        self.send({"type": "REGISTER", "worker_id": self.worker_id,
                   "status": self.status_fn()})
        header, _ = recv_msg(self.sock)
        if header.get("type") != "ACK":
            raise RegistrationRefused(header.get("reason", header))

    def heartbeat_loop(self, interval):
        while not self.stopped.wait(interval):
            try:
                self.send({"type": "HEARTBEAT", "worker_id": self.worker_id,
                           "status": self.status_fn()})
            except OSError:
                break                             # the main loop sees the broken socket

    def handle_task(self, header, payload):
        task_id = header.get("task_id")
        if not verify_payload(header, payload):
            self.send({"type": "ERROR", "task_id": task_id,
                       "reason": "task data corrupted in transfer"})
            return False
        try:
            result, elapsed = self.task_fn(header["task_type"], header.get("params", {}), payload)
        except Exception as exc:                  # report the failure instead of dying
            self.send({"type": "ERROR", "task_id": task_id,
                       "reason": "%s: %s" % (type(exc).__name__, exc)})
            return False
        self.send({"type": "RESULT", "task_id": task_id, "processing_s": elapsed,
                   "sha256": sha256_hex(result)}, result)
        return True

    def run(self, interval=config.HEARTBEAT_INTERVAL_S):
        """Register, then process tasks until the connection closes."""
        self.register()
        print(f"[worker] registered as {self.worker_id}")
        threading.Thread(target=self.heartbeat_loop, args=(interval,), daemon=True).start()
        try:
            while True:
                header, payload = recv_msg(self.sock)
                msg_type = header.get("type")

                if msg_type == "TASK":
                    task_id = header.get("task_id")
                    print(f"[worker] processing task {task_id} ({header.get('task_type')}, "
                          f"{len(payload)} bytes)")
                    if self.handle_task(header, payload):
                        print(f"[worker] completed task {task_id}")
                    else:
                        print(f"[worker] task {task_id} failed")

                elif msg_type == "ERROR":
                    print(f"[worker] coordinator error: {header.get('reason')}")

                # ACKs need no reply: heartbeats keep the worker known and pulling tasks.
        finally:
            self.stopped.set()


def worker_loop(host, port=config.COORDINATOR_PORT, worker_id=None, reconnect=True):
    worker_id = worker_id or default_worker_id()
    while True:
        try:
            with socket.create_connection((host, port), timeout=10) as sock:
                sock.settimeout(None)
                print(f"[worker] connected to {host}:{port}")
                WorkerSession(sock, worker_id).run()
        except RegistrationRefused as exc:
            print(f"[worker] registration refused: {exc}")
        except (OSError, ConnectionError, ValueError) as exc:
            print(f"[worker] connection lost: {exc}")

        if not reconnect:
            return
        print(f"[worker] reconnecting in {RECONNECT_DELAY_S} seconds...")
        time.sleep(RECONNECT_DELAY_S)


if __name__ == "__main__":
    import sys

    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    worker_id = sys.argv[2] if len(sys.argv) > 2 else None
    worker_loop(host, worker_id=worker_id)
