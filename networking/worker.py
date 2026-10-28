"""Worker agent (member A): registers with the coordinator, sends heartbeats
with CPU/RAM, runs TASK messages through compute.tasks.run_task, returns RESULT.

    Worker("192.168.1.20", worker_id="laptop-2").run()
"""
import socket
import threading

import config
from compute.tasks import run_task
from monitoring.resource_monitor import get_status
from networking.client import connect
from networking.protocol import recv_msg, send_msg, sha256_hex


class Worker:
    def __init__(self, host, port=config.COORDINATOR_PORT, worker_id=None):
        self.host, self.port = host, port
        self.worker_id = worker_id or socket.gethostname()
        self.sock = None
        self._send_lock = threading.Lock()   # heartbeat and results share the socket
        self._stop = threading.Event()

    def _send(self, header, payload=b""):
        with self._send_lock:
            send_msg(self.sock, header, payload)

    def _heartbeat_loop(self):
        while not self._stop.wait(config.HEARTBEAT_INTERVAL_S):
            try:
                self._send({"type": "HEARTBEAT", "worker_id": self.worker_id,
                            "status": get_status()})
            except OSError:
                break

    def _handle_task(self, header, payload):
        base = {"type": "RESULT", "task_id": header["task_id"]}
        try:
            if header.get("sha256") and sha256_hex(payload) != header["sha256"]:
                raise ValueError("task data corrupted in transfer")
            result, secs = run_task(header["task_type"], header.get("params", {}), payload)
            self._send({**base, "status": "ok", "processing_s": secs,
                        "sha256": sha256_hex(result)}, result)
        except Exception as e:  # report the failure instead of dying
            self._send({**base, "status": "error", "reason": str(e)})

    def run(self):
        self.sock = connect(self.host, self.port)
        self._send({"type": "REGISTER", "worker_id": self.worker_id,
                    "status": get_status()})
        header, _ = recv_msg(self.sock)
        print(f"[worker {self.worker_id}] registered ({header['type']})")
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        try:
            while True:
                header, payload = recv_msg(self.sock)
                if header["type"] == "TASK":
                    self._handle_task(header, payload)
        except (ConnectionError, OSError):
            print(f"[worker {self.worker_id}] coordinator disconnected")
        finally:
            self._stop.set()
            self.sock.close()

    def stop(self):
        self._stop.set()
        if self.sock:
            try:
                self.sock.shutdown(socket.SHUT_RDWR)  # wakes the blocked recv
            except OSError:
                pass
            self.sock.close()
