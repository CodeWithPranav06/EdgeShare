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

Limits (fine for the demo, worth stating in the report):
  - Tasks that were queued on the dead coordinator are lost; submit them again.
  - If only the link between one worker and the coordinator breaks, that worker first
    retries the old coordinator for FAILOVER_AFTER_S, so a short glitch does not start
    a second coordinator. A long, one-sided network split still can.
  - All laptops use the same TCP port (default 5000).
"""
import socket
import threading
import time

import config


def _log(text):
    print(text + "\n", end="", flush=True)


class LocalCoordinator:
    """A coordinator started inside a worker process after a takeover."""

    def __init__(self, port, strategy=None, web=True, discovery=True):
        from compute.coordinator import TaskCoordinator
        self.port = port
        self.coordinator = TaskCoordinator(strategy or config.SCHEDULER)
        self.stop = threading.Event()
        self.ready = threading.Event()
        self.error = None
        self.web = web
        self.discovery = discovery
        self.web_server = None

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

    def close(self):
        self.stop.set()
        if self.web_server is not None:
            self.web_server.shutdown()


def become_coordinator(port, **kwargs):
    """Start a coordinator in this process. Returns the LocalCoordinator or None."""
    from networking.discovery import local_ip
    local = LocalCoordinator(port, **kwargs)
    if not local.start():
        _log(f"[failover] could not start a coordinator on port {port}: {local.error}")
        return None
    _log(f"[failover] this laptop is now the coordinator ({local_ip()}:{port})")
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


def elect(worker_id, successors, port, become=None):
    """Walk the succession list. Returns (host to connect to, LocalCoordinator or None),
    or (None, None) if no successor could be found."""
    become = become or become_coordinator
    for entry in successors:
        if entry.get("worker_id") == worker_id:
            local = become(port)
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
