"""Smoke test: run before every merge to main.  python -m pytest"""
import threading

from compute.tasks import run_task
from monitoring.resource_monitor import get_status
from networking.client import connect_and_register
from networking.server import start_server


def test_worker_registers_with_coordinator():
    ready, stop, registry = threading.Event(), threading.Event(), {}
    t = threading.Thread(target=start_server, kwargs=dict(
        host="127.0.0.1", port=5055, ready=ready, stop=stop, registry=registry))
    t.start()
    assert ready.wait(3)
    sock = connect_and_register("127.0.0.1", 5055, "w1")
    sock.close()
    stop.set()
    t.join()
    assert "w1" in registry


def test_tasks_and_status():
    result, secs = run_task("hash", {}, b"hello")
    assert len(result) == 64 and secs >= 0
    assert set(get_status()) == {"cpu_count", "cpu_percent", "free_ram_mb"}
