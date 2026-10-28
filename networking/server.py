"""Coordinator-side networking (member A).

Coordinator keeps a registry of connected workers, sends them tasks, and puts
every result on `results` for the scheduler (member B) to consume.

    c = Coordinator(host, port); c.start()
    c.send_task(worker_id, task_id, "resize", {"width": 100, "height": 75}, image_bytes)
    res = c.results.get(timeout=10)   # {"task_id", "worker_id", "status", "processing_s", "payload"}
    c.on_worker_lost = lambda worker_id: ...   # called when a worker disconnects
"""
import queue
import socket
import threading
import time

import config
from networking.protocol import recv_msg, send_msg, sha256_hex


class Coordinator:
    def __init__(self, host=config.COORDINATOR_HOST, port=config.COORDINATOR_PORT):
        self.host, self.port = host, port
        self.workers = {}            # worker_id -> info dict (see _register)
        self.results = queue.Queue()
        self.on_worker_lost = None   # optional callback(worker_id)
        self.ready = threading.Event()
        self._stop = threading.Event()
        self._srv = None

    # ---- lifecycle ----
    def start(self):
        self._srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._srv.bind((self.host, self.port))
        self._srv.listen()
        self._srv.settimeout(0.5)
        print(f"[coordinator] listening on {self.host}:{self.port}")
        threading.Thread(target=self._accept_loop, daemon=True).start()
        self.ready.set()

    def stop(self):
        self._stop.set()

    def _accept_loop(self):
        while not self._stop.is_set():
            try:
                conn, addr = self._srv.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self._serve, args=(conn, addr), daemon=True).start()
        self._srv.close()

    # ---- sending ----
    def send_task(self, worker_id, task_id, task_type, params, payload=b""):
        w = self.workers[worker_id]
        header = {"type": "TASK", "task_id": task_id, "task_type": task_type,
                  "params": params, "sha256": sha256_hex(payload)}
        with w["send_lock"]:
            send_msg(w["conn"], header, payload)

    def alive_workers(self):
        """{worker_id: status} for workers still connected (input for schedulers)."""
        return {wid: w["status"] for wid, w in self.workers.items() if w["alive"]}

    # ---- per-connection thread ----
    def _serve(self, conn, addr):
        worker_id = None
        try:
            while True:
                header, payload = recv_msg(conn)
                kind = header["type"]
                if kind == "REGISTER":
                    worker_id = header["worker_id"]
                    self.workers[worker_id] = {
                        "conn": conn, "addr": addr, "alive": True,
                        "status": header.get("status", {}),
                        "last_seen": time.time(), "send_lock": threading.Lock()}
                    with self.workers[worker_id]["send_lock"]:
                        send_msg(conn, {"type": "ACK", "worker_id": worker_id})
                    print(f"[coordinator] worker {worker_id} registered from {addr[0]}")
                elif worker_id is None:
                    continue  # ignore anything before REGISTER
                elif kind == "HEARTBEAT":
                    self.workers[worker_id]["status"] = header.get("status", {})
                    self.workers[worker_id]["last_seen"] = time.time()
                elif kind == "RESULT":
                    ok = header.get("status") == "ok" and \
                        sha256_hex(payload) == header.get("sha256")
                    self.results.put({
                        "task_id": header["task_id"], "worker_id": worker_id,
                        "status": "ok" if ok else "error",
                        "processing_s": header.get("processing_s"),
                        "reason": header.get("reason", "" if ok else "bad checksum"),
                        "payload": payload})
        except (ConnectionError, OSError, ValueError):
            pass
        finally:
            conn.close()
            if worker_id in self.workers:
                self.workers[worker_id]["alive"] = False
                print(f"[coordinator] worker {worker_id} disconnected")
                if self.on_worker_lost:
                    self.on_worker_lost(worker_id)


def start_server(host=config.COORDINATOR_HOST, port=config.COORDINATOR_PORT):
    """Run a coordinator in the foreground until Ctrl+C (used by main.py)."""
    c = Coordinator(host, port)
    c.start()
    try:
        while True:
            time.sleep(5)
            print(f"[coordinator] workers: {c.alive_workers()}")
    except KeyboardInterrupt:
        c.stop()


if __name__ == "__main__":
    start_server()
