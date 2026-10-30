"""Smoke test: run before every merge to main.  python -m pytest"""
import socket
import threading

from compute.tasks import run_task
from monitoring.resource_monitor import get_status
from networking.protocol import recv_msg, send_msg
from networking.server import start_server


def test_worker_registers_with_coordinator():
    ready, stop, registry = threading.Event(), threading.Event(), {}
    t = threading.Thread(target=start_server, kwargs=dict(
        host="127.0.0.1", port=5055, ready=ready, stop=stop, registry=registry))
    t.start()
    assert ready.wait(3)
    try:
        with socket.create_connection(("127.0.0.1", 5055), timeout=5) as sock:
            send_msg(sock, {"type": "REGISTER", "worker_id": "w1"})
            header, _ = recv_msg(sock)
            assert header["type"] == "ACK"
            assert "w1" in registry
    finally:
        stop.set()
        t.join()


def test_tasks_and_status():
    result, secs = run_task("hash", {}, b"hello")
    assert len(result) == 64 and secs >= 0
    assert set(get_status()) == {"cpu_count", "cpu_percent", "free_ram_mb"}
