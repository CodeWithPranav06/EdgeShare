"""Coordinator failover (member A).

Every ACK from the coordinator carries the succession list: the connected workers in the
order they joined, e.g. [{"worker_id": "W1", "host": "192.168.1.21"}, ...]. Each worker
keeps the latest copy.

When a worker loses the coordinator and cannot reach it again for config.FAILOVER_AFTER_S,
it walks that list from the top:
  - the entry is itself     -> it starts a coordinator in this process and keeps working
                               as a worker of its own coordinator (connects to 127.0.0.1);
  - the entry is another one -> it waits up to config.FAILOVER_WAIT_S for a coordinator to
                               appear at that worker's IP, and joins it; if none appears,
                               that worker is probably dead too, so it tries the next one.
Because everyone walks the same list, all survivors end up at the same new coordinator.

No lost work: the coordinator keeps a copy of every unfinished task (with its data) on the
first worker in the list (server.Replicator). That worker queues them again when it takes
over, and saves the results in output/recovered/.

Limits (fine for the demo, worth stating in the report):
  - If the coordinator AND the first worker die at the same time, the queued tasks are lost.
  - Whoever submitted the tasks was talking to the dead coordinator, so recovered results
    are saved on the new coordinator (output/recovered/) instead of being sent back.
  - If only the link between one worker and the coordinator breaks, that worker first
    retries the old coordinator for FAILOVER_AFTER_S, so a short glitch does not start
    a second coordinator. A long, one-sided network split still can.
  - All laptops use the same TCP port (default 5000).
"""
import os
import socket
import threading
import time

import config


def _log(text):
    print(text + "\n", end="", flush=True)


class LocalCoordinator:
    """A coordinator started inside a worker process after a takeover."""

    def __init__(self, port, strategy=None, web=True, discovery=True, out_dir=None):
        from compute.coordinator import TaskCoordinator
        self.port = port
        self.coordinator = TaskCoordinator(strategy or config.SCHEDULER)
        self.stop = threading.Event()
        self.ready = threading.Event()
        self.error = None
        self.web = web
        self.discovery = discovery
        self.web_server = None
        self.out_dir = out_dir or os.path.join(config.OUTPUT_DIR, "recovered")
        self.recovered = []                   # task ids taken over from the old coordinator

    def _serve(self):
        from networking.server import start_server
        try:
            start_server(port=self.port, coordinator=self.coordinator, ready=self.ready,
                         stop=self.stop, discovery=self.discovery)
        except OSError as exc:
            self.error = exc
            self.ready.set()

    def start(self, timeout=5):
        threading.Thread(target=self._serve, daemon=True).start()
        self.ready.wait(timeout)
        if self.error is not None or not self.ready.is_set():
            return False
        if self.web:
            from gui.web_server import start_web_dashboard
            self.web_server, message = start_web_dashboard(self.coordinator, self.port)
            _log(f"[failover] web dashboard: {message}")
        return True

    def recover(self, tasks):
        """Queue the old coordinator's unfinished tasks and save their results."""
        tasks = [t for t in tasks if t.get("task_id") not in self.coordinator.tasks]
        if not tasks:
            return 0
        self.coordinator.submit_tasks(tasks)
        self.recovered = [t["task_id"] for t in tasks]
        threading.Thread(target=self._save_results, daemon=True).start()
        return len(tasks)

    def _save_results(self):
        pending = set(self.recovered)
        while pending and not self.stop.is_set():
            for task_id in list(pending):
                with self.coordinator.lock:
                    result = self.coordinator.results.get(task_id)
                    state = self.coordinator.task_state[task_id]["state"]
                    task = self.coordinator.tasks[task_id]
                if state == "failed":
                    pending.discard(task_id)
                elif result is not None:
                    os.makedirs(self.out_dir, exist_ok=True)
                    stem = os.path.splitext(os.path.basename(task.get("name") or task_id))[0]
                    ext = "_resized.png" if task.get("task_type") == "resize" else ".bin"
                    with open(os.path.join(self.out_dir, stem + ext), "wb") as f:
                        f.write(result["result"])
                    self.coordinator.release_task_data(task_id)
                    pending.discard(task_id)
            self.stop.wait(0.2)
        if not self.stop.is_set():
            _log(f"[failover] all recovered tasks finished; results in {self.out_dir}")

    def close(self):
        self.stop.set()
        if self.web_server is not None:
            self.web_server.shutdown()


def become_coordinator(port, tasks=(), **kwargs):
    """Start a coordinator in this process and queue the old coordinator's unfinished
    tasks. Returns the LocalCoordinator or None."""
    from networking.discovery import local_ip
    local = LocalCoordinator(port, **kwargs)
    if not local.start():
        _log(f"[failover] could not start a coordinator on port {port}: {local.error}")
        return None
    _log(f"[failover] this laptop is now the coordinator ({local_ip()}:{port})")
    count = local.recover(tasks)
    if count:
        _log(f"[failover] recovered {count} unfinished task(s) from the old coordinator")
    return local


def reachable(host, port, wait_s):
    """True if something accepts TCP connections at host:port within wait_s seconds."""
    deadline = time.monotonic() + wait_s
    while True:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.5)


def elect(worker_id, successors, port, become=None, tasks=()):
    """Walk the succession list. Returns (host to connect to, LocalCoordinator or None),
    or (None, None) if no successor could be found."""
    become = become or become_coordinator
    for entry in successors:
        if entry.get("worker_id") == worker_id:
            local = become(port, tasks)
            if local is not None:
                return "127.0.0.1", local
            continue                       # could not take over; let the next one do it
        host = entry.get("host")
        if not host:
            continue
        _log(f"[failover] waiting for {entry['worker_id']} ({host}) to take over...")
        if reachable(host, port, config.FAILOVER_WAIT_S):
            _log(f"[failover] new coordinator is {entry['worker_id']} at {host}")
            return host, None
    return None, None
