"""Dashboard: the model is tested from a real coordinator; the window test drives the real
Tk dashboard, server, and a worker (skipped when no display is available)."""
import time

import pytest

import config
from compute.coordinator import TaskCoordinator
from gui.dashboard import DashboardModel

from tests.test_networking import FakeWorker, free_port, make_png, wait_for


def coordinator_with_activity():
    coordinator = TaskCoordinator("round_robin")
    coordinator.register_worker("mac", {"cpu_count": 8, "cpu_percent": 12.0, "free_ram_mb": 5000,
                                        "battery_percent": 80, "power_plugged": False})
    coordinator.register_worker("desktop", {"cpu_count": 4, "cpu_percent": 30.0,
                                            "free_ram_mb": 9000, "battery_percent": None,
                                            "power_plugged": None})
    coordinator.register_worker("dying", None)
    coordinator.submit_tasks([{"task_id": t, "task_type": "resize", "name": t + ".png",
                               "payload": b""} for t in ("T1", "T2", "T3")])
    coordinator.next_task("mac")
    coordinator.complete_task("mac", "T1", b"r", 0.25)
    coordinator.next_task("desktop")
    coordinator.mark_running("desktop", "T2")
    coordinator.worker_disconnected("dying")
    return coordinator


def test_model_rows_show_real_backend_values():
    model = DashboardModel(coordinator_with_activity().snapshot())
    workers = {iid: values for iid, _, values in model.worker_rows()}
    assert workers["mac"][:7] == ("mac", "idle", "12%", "8", "5000 MB", "80%", "no")
    assert workers["desktop"][1] == "busy" and workers["desktop"][8] == "T2"
    assert workers["desktop"][5:7] == ("n/a", "n/a")             # no battery: shown, not hidden
    assert workers["dying"][1] == "failed" and "connection lost" in workers["dying"][10]

    tasks = {iid: values for iid, _, values in model.task_rows()}
    assert tasks["T1"][1:6] == ("T1.png", "completed", "mac", 1, "0.25s")
    assert tasks["T2"][2] == "running"
    assert "Tasks: 3 total, 1 pending, 1 running, 1 completed, 0 failed" in model.summary()
    assert "2 connected, 1 failed" in model.summary()
    assert model.progress() == pytest.approx(100 / 3)


def test_model_new_events_survives_log_overflow():
    coordinator = TaskCoordinator()
    for number in range(250):
        coordinator._event("event %d" % number)
    model = DashboardModel(coordinator.snapshot())
    assert [text for _, text, _ in model.new_events(248)] == ["event 248", "event 249"]
    assert len(model.new_events(0)) == 200                       # older ones were dropped
    assert model.new_events(250) == []


def test_window_submits_image_through_backend(tmp_path, monkeypatch):
    tk = pytest.importorskip("tkinter")
    from gui.dashboard import Dashboard

    try:
        root = tk.Tk()
    except tk.TclError:
        pytest.skip("no display for Tk")
    root.withdraw()

    monkeypatch.setattr(config, "OUTPUT_DIR", str(tmp_path / "out"))
    port = free_port()
    coordinator = TaskCoordinator("round_robin")
    dashboard = Dashboard(root, coordinator, port)
    worker = None
    try:
        assert dashboard.server_error is None
        worker = FakeWorker(port, "W1")
        worker.start()

        def pump(condition, timeout=15):
            def check():
                root.update()
                return condition()
            return wait_for(check, timeout)

        assert pump(lambda: dashboard.workers.exists("W1"))
        dashboard.size.set("40")
        dashboard.submit([make_png(tmp_path / "cat.png", 90, 60)])
        assert pump(lambda: "Batch finished: 1/1" in dashboard.log.get("1.0", "end"))

        output = tmp_path / "out" / "cat_resized.png"
        assert output.exists()
        assert str(output) in dashboard.log.get("1.0", "end")
        task_id = dashboard.tasks.get_children()[0]
        assert dashboard.tasks.item(task_id, "values")[2] == "completed"
        assert dashboard.workers.item("W1", "values")[9] == "1"           # done count

        worker.kill()
        assert pump(lambda: dashboard.workers.item("W1", "values")[1] == "failed")
        assert "worker W1 failed" in dashboard.log.get("1.0", "end")
    finally:
        if worker is not None:
            worker.kill()
        dashboard.close()
        time.sleep(0.6)                                # let the server thread exit
