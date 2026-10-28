"""Smoke test: run before every merge to main.  python -m pytest"""
import io
import threading
import time

from PIL import Image

from compute.tasks import run_task
from monitoring.resource_monitor import get_status
from networking.protocol import sha256_hex
from networking.server import Coordinator
from networking.worker import Worker


def _start(port):
    c = Coordinator("127.0.0.1", port)
    c.start()
    w = Worker("127.0.0.1", port, worker_id="w1")
    threading.Thread(target=w.run, daemon=True).start()
    for _ in range(50):
        if "w1" in c.workers:
            break
        time.sleep(0.1)
    return c, w


def test_worker_registers_and_runs_hash_task():
    c, w = _start(5055)
    assert "w1" in c.alive_workers()
    c.send_task("w1", 1, "hash", {}, b"hello")
    res = c.results.get(timeout=5)
    assert res["status"] == "ok" and res["task_id"] == 1
    assert res["payload"] == sha256_hex(b"hello").encode()
    w.stop(); c.stop()


def test_resize_task_over_network():
    c, w = _start(5056)
    buf = io.BytesIO(); Image.new("RGB", (400, 300), "red").save(buf, "PNG")
    c.send_task("w1", 7, "resize", {"width": 100, "height": 75}, buf.getvalue())
    res = c.results.get(timeout=5)
    assert res["status"] == "ok"
    assert Image.open(io.BytesIO(res["payload"])).size == (100, 75)
    w.stop(); c.stop()


def test_worker_loss_is_reported():
    c, w = _start(5057)
    lost = []
    c.on_worker_lost = lost.append
    w.stop()
    time.sleep(1)
    assert lost == ["w1"] and "w1" not in c.alive_workers()
    c.stop()


def test_tasks_and_status():
    result, secs = run_task("hash", {}, b"hello")
    assert len(result) == 64 and secs >= 0
    assert set(get_status()) == {"cpu_count", "cpu_percent", "free_ram_mb"}
