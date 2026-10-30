"""Coordinator-side TCP server for EdgeShare.

One thread per connection. A connection is either a worker (starts with REGISTER) or a
submitter (starts with SUBMIT: one image in, one result out).

Workers pull work: after every message from a worker (heartbeat, result, error) the
server sends it its next task, if the scheduler has one for it. One monitor thread
checks heartbeats every config.HEARTBEAT_INTERVAL_S, recovers the tasks of timed-out
workers, and offers queued tasks to idle workers.
"""
import socket
import threading
import time

import config
from compute.coordinator import TaskCoordinator
from networking.protocol import recv_msg, send_msg, sha256_hex, verify_payload

SUBMIT_TIMEOUT_S = 300


def log(text):
    """Print a whole line in one write (many threads print at the same time)."""
    print(text + "\n", end="", flush=True)


class Peer:
    """A registered worker connection. The lock makes sure only one thread writes
    to the socket at a time (handler thread, monitor thread, submit wake-ups)."""

    def __init__(self, conn, worker_id):
        self.conn = conn
        self.worker_id = worker_id
        self.lock = threading.Lock()

    def send(self, header, payload=b""):
        with self.lock:
            send_msg(self.conn, header, payload)


def _shutdown(sock):
    """Close a socket and wake any thread blocked in recv() on it."""
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    sock.close()


def send_next_task(peer, coordinator):
    """Send the next task for this worker, if any. Returns True if one was sent."""
    task = coordinator.next_task(peer.worker_id)
    if task is None:
        return False

    payload = task.get("payload", b"")
    try:
        peer.send({
            "type": "TASK",
            "task_id": task["task_id"],
            "task_type": task["task_type"],
            "params": task.get("params", {}),
            "sha256": sha256_hex(payload),
        }, payload)
    except OSError:
        coordinator.worker_disconnected(peer.worker_id, "could not send task")
        return False

    coordinator.mark_running(peer.worker_id, task["task_id"])
    return True


def wake_workers(registry, coordinator, lock):
    """Offer queued tasks to every connected worker (idle ones will take one)."""
    with lock:
        peers = list(registry.values())
    for peer in peers:
        send_next_task(peer, coordinator)


def handle_worker(conn, registry, coordinator, lock, first_message=None, addr=None):
    peer = None
    reason = "connection closed"

    try:
        if first_message is None:
            header, payload = recv_msg(conn)
        else:
            header, payload = first_message

        while True:
            msg_type = header.get("type")

            if msg_type == "REGISTER":
                if peer is not None:
                    peer.send({"type": "ERROR", "reason": "Already registered"})
                else:
                    peer = register_peer(conn, header, registry, coordinator, lock, addr)
                    if peer is None:
                        return

            elif peer is None:
                send_msg(conn, {"type": "ERROR", "reason": "Register first"})

            elif msg_type in ("HEARTBEAT", "STATUS"):
                if not coordinator.heartbeat(peer.worker_id, header.get("status")):
                    peer.send({"type": "ERROR", "reason": "Not registered (timed out)"})
                    reason = "heartbeat after timeout"
                    return
                peer.send({"type": "ACK", "worker_id": peer.worker_id})

            elif msg_type == "RESULT":
                task_id = header.get("task_id")
                if verify_payload(header, payload):
                    coordinator.complete_task(peer.worker_id, task_id, payload,
                                              header.get("processing_s", 0))
                else:
                    coordinator.fail_task(peer.worker_id, task_id, "result corrupted in transfer")
                peer.send({"type": "ACK", "task_id": task_id})

            elif msg_type == "ERROR":
                task_id = header.get("task_id")
                if task_id is not None:
                    coordinator.fail_task(peer.worker_id, task_id,
                                          header.get("reason", "worker error"))

            else:
                peer.send({"type": "ERROR", "reason": "Unknown message type"})

            if peer is not None:
                send_next_task(peer, coordinator)

            header, payload = recv_msg(conn)

    except (ConnectionError, OSError) as exc:
        reason = "connection lost (%s)" % (exc.__class__.__name__,)
    except (KeyError, ValueError) as exc:
        reason = "protocol error: %s" % exc

    finally:
        _shutdown(conn)

        if peer is not None:
            with lock:
                current = registry.get(peer.worker_id) is peer
                if current:
                    registry.pop(peer.worker_id, None)
            if current and coordinator.worker_disconnected(peer.worker_id, reason):
                log(f"[server] worker {peer.worker_id} disconnected: {reason}")


def register_peer(conn, header, registry, coordinator, lock, addr=None):
    """Register a worker; refuse an ID that is already connected. Returns the Peer or None."""
    worker_id = str(header.get("worker_id") or "").strip()
    if not worker_id:
        send_msg(conn, {"type": "ERROR", "reason": "worker_id is required"})
        return None

    peer = Peer(conn, worker_id)
    with peer.lock:                       # nobody may send a TASK before our ACK
        with lock:
            if worker_id in registry:
                send_msg(conn, {"type": "ERROR",
                                "reason": "worker_id %s is already connected; "
                                          "use a different ID" % worker_id})
                return None
            registry[worker_id] = peer
        try:
            coordinator.register_worker(worker_id, header.get("status"))
            send_msg(conn, {"type": "ACK", "worker_id": worker_id})
        except OSError:
            with lock:
                if registry.get(worker_id) is peer:
                    registry.pop(worker_id)
            coordinator.worker_disconnected(worker_id, "lost during registration")
            raise

    where = " from %s" % addr[0] if addr else ""
    log(f"[server] worker {worker_id} registered{where}")
    return peer


def handle_submitter(conn, coordinator, lock, first_header, first_payload,
                     wake=None, timeout=SUBMIT_TIMEOUT_S):
    """Accept one image, queue it, and wait for its result (or its failure)."""
    task_id = first_header.get("task_id")

    try:
        if not task_id:
            send_msg(conn, {"type": "ERROR", "reason": "task_id is required"})
            return
        if not verify_payload(first_header, first_payload):
            send_msg(conn, {"type": "ERROR", "task_id": task_id,
                            "reason": "data corrupted in transfer"})
            return

        task = {
            "task_id": task_id,
            "task_type": first_header.get("task_type", "resize"),
            "params": first_header.get("params", {}),
            "payload": first_payload,
            "name": first_header.get("name"),
        }
        try:
            coordinator.submit_tasks([task])
        except ValueError as exc:
            send_msg(conn, {"type": "ERROR", "task_id": task_id, "reason": str(exc)})
            return

        send_msg(conn, {"type": "ACK", "task_id": task_id})
        if wake:
            wake()

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with coordinator.lock:
                result = coordinator.results.get(task_id)
                state = coordinator.task_state[task_id]
                failed = state["state"] == "failed"
                error = state["error"]

            if result is not None:
                send_msg(conn, {
                    "type": "RESULT",
                    "task_id": task_id,
                    "worker_id": result.get("worker_id"),
                    "processing_s": result.get("processing_s", 0),
                    "sha256": sha256_hex(result["result"]),
                }, result["result"])
                coordinator.release_task_data(task_id)
                return

            if failed:
                send_msg(conn, {"type": "ERROR", "task_id": task_id,
                                "reason": error or "task failed"})
                return

            time.sleep(0.1)

        send_msg(conn, {
            "type": "ERROR",
            "task_id": task_id,
            "reason": "Timed out waiting for worker"
        })

    except (ConnectionError, OSError, KeyError, ValueError) as exc:
        log(f"[server] Submission failed: {exc}")

    finally:
        conn.close()


def monitor_workers(registry, coordinator, lock, stop, interval=config.HEARTBEAT_INTERVAL_S):
    """The only heartbeat checker: started once per server, never per worker."""
    while not stop.wait(interval):
        for worker_id in coordinator.check_workers():
            with lock:
                peer = registry.get(worker_id)
            if peer is not None:
                log(f"[server] worker {worker_id} timed out; closing its connection")
                _shutdown(peer.conn)
        wake_workers(registry, coordinator, lock)


def start_server(host=config.COORDINATOR_HOST,
                 port=config.COORDINATOR_PORT,
                 ready=None, stop=None, registry=None,
                 coordinator=None, discovery=False,
                 monitor_interval=config.HEARTBEAT_INTERVAL_S):
    """Run until stop is set. Pass `coordinator` to share it (e.g. with the dashboard)."""
    registry = registry if registry is not None else {}
    coordinator = coordinator if coordinator is not None else TaskCoordinator()
    lock = threading.RLock()
    stop = stop if stop is not None else threading.Event()
    stopped = threading.Event()           # also stops helpers if we exit on an error

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port))
    srv.listen()
    srv.settimeout(0.5)

    log(f"[server] listening on {host}:{port} (scheduler: {coordinator.strategy_name})")

    threading.Thread(target=monitor_workers,
                     args=(registry, coordinator, lock, stopped, monitor_interval),
                     daemon=True).start()
    if discovery:
        from networking.discovery import broadcast_loop
        threading.Thread(target=broadcast_loop, args=(stopped,), kwargs={"tcp_port": port},
                         daemon=True).start()

    if ready:
        ready.set()

    try:
        while not stop.is_set():
            try:
                conn, addr = srv.accept()
            except socket.timeout:
                continue

            threading.Thread(
                target=dispatch_connection,
                args=(conn, registry, coordinator, lock, addr),
                daemon=True
            ).start()

    finally:
        stopped.set()
        srv.close()
        with lock:
            peers = list(registry.values())
        for peer in peers:
            _shutdown(peer.conn)


def dispatch_connection(conn, registry, coordinator, lock, addr=None):
    """Distinguish image submissions from worker registrations."""
    conn.settimeout(None)
    try:
        header, payload = recv_msg(conn)
    except (ConnectionError, OSError, ValueError):
        conn.close()
        return

    if header.get("type") == "SUBMIT":
        handle_submitter(conn, coordinator, lock, header, payload,
                         wake=lambda: wake_workers(registry, coordinator, lock))
    else:
        handle_worker(
            conn, registry, coordinator, lock,
            first_message=(header, payload), addr=addr
        )


if __name__ == "__main__":
    start_server(discovery=True)
