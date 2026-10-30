"""Coordinator + workers over real TCP on localhost: registration, several workers,
image batches, heartbeats, worker failure mid-batch, and UDP discovery."""
import random
import socket
import threading
import time

import pytest
from PIL import Image

import config
from compute.coordinator import TaskCoordinator
from compute.tasks import run_task
from networking import discovery
from networking.client import RegistrationRefused, WorkerSession
from networking.protocol import recv_msg, send_msg, sha256_hex, verify_payload
from networking.server import start_server
from networking.submit import submit_batch, submit_image

FAST_STATUS = {"cpu_count": 4, "cpu_percent": 5, "free_ram_mb": 4096,
               "battery_percent": None, "power_plugged": None}


def free_port(kind=socket.SOCK_STREAM):
    with socket.socket(socket.AF_INET, kind) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for(condition, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.02)
    return False


def make_png(path, width=64, height=48, seed=0):
    rnd = random.Random(seed)
    image = Image.new("RGB", (width, height))
    image.putdata([(rnd.randrange(256), rnd.randrange(256), rnd.randrange(256))
                   for _ in range(width * height)])
    image.save(path)
    return path


class Server:
    def __init__(self, strategy="round_robin", heartbeat_timeout=None, monitor_interval=0.1):
        self.port = free_port()
        self.coordinator = TaskCoordinator(strategy)
        if heartbeat_timeout is not None:
            self.coordinator.recovery.heartbeat_timeout = heartbeat_timeout
        self.registry, self.ready, self.stop = {}, threading.Event(), threading.Event()
        self.thread = threading.Thread(target=start_server, kwargs=dict(
            host="127.0.0.1", port=self.port, ready=self.ready, stop=self.stop,
            registry=self.registry, coordinator=self.coordinator,
            monitor_interval=monitor_interval), daemon=True)

    def __enter__(self):
        self.thread.start()
        assert self.ready.wait(3)
        return self

    def __exit__(self, *exc):
        self.stop.set()
        self.thread.join(5)


class FakeWorker(threading.Thread):
    """A real WorkerSession in a thread, with controllable task code."""

    def __init__(self, port, worker_id, task_fn=run_task, interval=0.1):
        super().__init__(daemon=True)
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.sock.settimeout(None)
        self.session = WorkerSession(self.sock, worker_id, lambda: dict(FAST_STATUS), task_fn)
        self.interval = interval
        self.error = None

    def run(self):
        try:
            self.session.run(self.interval)
        except (OSError, ValueError, RegistrationRefused) as exc:
            self.error = exc

    def kill(self):
        """Like pulling the network cable: the connection just goes away."""
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()


# ---- protocol ---------------------------------------------------------------------------

def test_large_binary_payload_roundtrip():
    a, b = socket.socketpair()
    payload = random.Random(1).randbytes(5 * 1024 * 1024)
    t = threading.Thread(target=send_msg, args=(a, {"type": "TASK", "sha256": sha256_hex(payload)},
                                                payload))
    t.start()
    header, data = recv_msg(b)
    t.join()
    assert data == payload and verify_payload(header, data)
    assert not verify_payload(header, data[:-1] + b"\x00")
    a.close()
    b.close()


def test_malformed_header_is_rejected():
    a, b = socket.socketpair()
    a.sendall((config.MAX_HEADER_BYTES + 1).to_bytes(4, "big"))
    with pytest.raises(ValueError):
        recv_msg(b)
    a.close()
    b.close()


# ---- registration ---------------------------------------------------------------------------

def test_three_workers_register_at_once():
    with Server() as server:
        workers = [FakeWorker(server.port, "W%d" % n) for n in (1, 2, 3)]
        for worker in workers:
            worker.start()
        assert wait_for(lambda: set(server.registry) == {"W1", "W2", "W3"})
        snap = server.coordinator.snapshot()
        assert {w["worker_id"]: w["state"] for w in snap["workers"]} == \
            {"W1": "idle", "W2": "idle", "W3": "idle"}
        assert all(w["cpu_count"] == 4 for w in snap["workers"])     # the worker's own status
        for worker in workers:
            worker.kill()


def test_duplicate_worker_id_is_refused_and_others_unaffected():
    with Server() as server:
        first = FakeWorker(server.port, "W1")
        first.start()
        assert wait_for(lambda: "W1" in server.registry)
        second = FakeWorker(server.port, "W1")
        second.start()
        second.join(5)
        assert isinstance(second.error, RegistrationRefused)
        assert "already connected" in str(second.error)
        assert server.registry["W1"].conn is not None and first.is_alive()
        first.kill()


def test_disconnect_then_reconnect_with_same_id():
    with Server() as server:
        worker = FakeWorker(server.port, "W1")
        worker.start()
        assert wait_for(lambda: "W1" in server.registry)
        worker.kill()
        assert wait_for(lambda: "W1" not in server.registry)
        again = FakeWorker(server.port, "W1")
        again.start()
        assert wait_for(lambda: "W1" in server.registry and "W1" in server.coordinator.workers)
        again.kill()


# ---- images end to end --------------------------------------------------------------------------

def test_one_worker_resizes_one_image(tmp_path):
    image = make_png(tmp_path / "photo.png", 120, 80)
    with Server() as server:
        worker = FakeWorker(server.port, "mac")
        worker.start()
        assert wait_for(lambda: "mac" in server.registry)
        report = submit_image(image, "job-1", port=server.port, width=32, height=16,
                              out_dir=tmp_path / "out")
        worker.kill()
    assert report["ok"], report["error"]
    assert report["worker_id"] == "mac"
    with Image.open(report["output"]) as result:
        assert result.size == (32, 16)


def test_larger_image_transfers_intact(tmp_path):
    image = make_png(tmp_path / "big.png", 1200, 900, seed=3)
    assert image.stat().st_size > 2 * 1024 * 1024
    with Server() as server:
        worker = FakeWorker(server.port, "W1")
        worker.start()
        assert wait_for(lambda: "W1" in server.registry)
        report = submit_image(image, "big-1", port=server.port, width=512, height=512,
                              out_dir=tmp_path / "out")
        worker.kill()
    expected, _ = run_task("resize", {"width": 512, "height": 512}, image.read_bytes())
    assert report["ok"], report["error"]
    assert report["sha256"] == sha256_hex(expected)


def test_batch_is_shared_by_three_workers_in_order(tmp_path):
    images = [make_png(tmp_path / ("img%02d.png" % n), seed=n) for n in range(9)]

    def slow_resize(task_type, params, payload):
        time.sleep(0.15)                              # long enough for all three to be used
        return run_task(task_type, params, payload)

    with Server(strategy="round_robin") as server:
        workers = [FakeWorker(server.port, "W%d" % n, slow_resize) for n in (1, 2, 3)]
        for worker in workers:
            worker.start()
        assert wait_for(lambda: len(server.registry) == 3)
        reports = submit_batch(images, port=server.port, size=24, out_dir=tmp_path / "out",
                               parallel=9)
        for worker in workers:
            worker.kill()

    assert all(r["ok"] for r in reports), [r["error"] for r in reports]
    assert [r["image"] for r in reports] == [str(p) for p in images]
    used = [r["worker_id"] for r in reports]
    assert {used.count(w) for w in ("W1", "W2", "W3")} == {3}      # Round-Robin: 3 each


def test_worker_dies_mid_batch_and_nothing_is_lost(tmp_path):
    images = [make_png(tmp_path / ("img%02d.png" % n), seed=n) for n in range(8)]
    victim_started = threading.Event()

    def victim_task(task_type, params, payload):
        victim_started.set()
        time.sleep(30)                                # never finishes: we kill it first

    def normal(task_type, params, payload):
        time.sleep(0.05)
        return run_task(task_type, params, payload)

    with Server(strategy="round_robin") as server:
        good = [FakeWorker(server.port, w, normal) for w in ("W1", "W2")]
        victim = FakeWorker(server.port, "W3", victim_task)
        for worker in good + [victim]:
            worker.start()
        assert wait_for(lambda: len(server.registry) == 3)

        results = {}
        runner = threading.Thread(target=lambda: results.update(reports=submit_batch(
            images, port=server.port, size=24, out_dir=tmp_path / "out", parallel=8)))
        runner.start()
        assert victim_started.wait(5)
        victim.kill()                                  # dies holding a task
        runner.join(30)
        for worker in good:
            worker.kill()
        events = [text for _, text, _ in server.coordinator.events]

    reports = results["reports"]
    assert all(r["ok"] for r in reports), [r["error"] for r in reports]
    assert all(r["worker_id"] in ("W1", "W2") for r in reports)
    assert any("worker W3 failed" in e and "requeued" in e for e in events)


def test_silent_worker_times_out_and_its_task_moves(tmp_path):
    image = make_png(tmp_path / "a.png")
    with Server(strategy="round_robin", heartbeat_timeout=0.5) as server:
        silent = socket.create_connection(("127.0.0.1", server.port))
        send_msg(silent, {"type": "REGISTER", "worker_id": "frozen", "status": FAST_STATUS})
        assert recv_msg(silent)[0]["type"] == "ACK"

        results = {}
        runner = threading.Thread(target=lambda: results.update(report=submit_image(
            image, "t-frozen", port=server.port, width=10, height=10, out_dir=tmp_path / "out")))
        runner.start()
        header, _ = recv_msg(silent)                   # it gets the task, then hangs
        assert header["type"] == "TASK"

        good = FakeWorker(server.port, "alive")
        good.start()
        runner.join(15)
        good.kill()
        snap = {w["worker_id"]: w for w in server.coordinator.snapshot()["workers"]}
        silent.close()

    assert results["report"]["ok"] and results["report"]["worker_id"] == "alive"
    assert snap["frozen"]["state"] == "failed" and "heartbeat" in snap["frozen"]["reason"]


def test_slow_task_keeps_heartbeating_and_is_not_dead(tmp_path):
    image = make_png(tmp_path / "a.png")

    def slow(task_type, params, payload):
        time.sleep(1.5)                                # three heartbeat timeouts
        return run_task(task_type, params, payload)

    with Server(heartbeat_timeout=0.5) as server:
        worker = FakeWorker(server.port, "slowpoke", slow, interval=0.1)
        worker.start()
        assert wait_for(lambda: "slowpoke" in server.registry)
        report = submit_image(image, "slow-1", port=server.port, width=10, height=10,
                              out_dir=tmp_path / "out")
        dead = "slowpoke" in server.coordinator.recovery.dead_workers
        worker.kill()

    assert report["ok"] and report["worker_id"] == "slowpoke"
    assert not dead


def test_task_error_is_retried_then_reported(tmp_path):
    bad = tmp_path / "broken.png"
    bad.write_bytes(b"this is not an image")
    with Server() as server:
        workers = [FakeWorker(server.port, w) for w in ("W1", "W2")]
        for worker in workers:
            worker.start()
        assert wait_for(lambda: len(server.registry) == 2)
        report = submit_image(bad, "bad-1", port=server.port, out_dir=tmp_path / "out")
        state = server.coordinator.task_state["bad-1"]
        for worker in workers:
            worker.kill()

    assert not report["ok"] and "gave up after" in report["error"]
    assert state["state"] == "failed" and state["attempts"] == config.TASK_RETRY_LIMIT


def test_duplicate_submission_id_is_rejected(tmp_path):
    image = make_png(tmp_path / "a.png")
    with Server() as server:
        worker = FakeWorker(server.port, "W1")
        worker.start()
        assert wait_for(lambda: "W1" in server.registry)
        first = submit_image(image, "same", port=server.port, width=8, height=8,
                             out_dir=tmp_path / "out")
        second = submit_image(image, "same", port=server.port, width=8, height=8,
                              out_dir=tmp_path / "out")
        worker.kill()
    assert first["ok"] and not second["ok"] and "Duplicate" in second["error"]


# ---- discovery ----------------------------------------------------------------------------------

def test_worker_finds_coordinator_by_udp():
    port = free_port(socket.SOCK_DGRAM)
    found = {}
    listener = threading.Thread(target=lambda: found.update(
        result=discovery.find_coordinator(timeout=5, port=port)))
    listener.start()
    time.sleep(0.2)
    stop = threading.Event()
    beacon = threading.Thread(target=discovery.broadcast_loop, args=(stop,),
                              kwargs=dict(tcp_port=5123, port=port, address="127.0.0.1",
                                          interval=0.1))
    beacon.start()
    listener.join(6)
    stop.set()
    beacon.join(2)
    assert found["result"] == ("127.0.0.1", 5123)


def test_find_coordinator_gives_up():
    assert discovery.find_coordinator(timeout=0.3, port=free_port(socket.SOCK_DGRAM)) is None
