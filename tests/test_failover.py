"""Coordinator failover: when the coordinator dies, the first worker takes over."""
import socket
import threading
import time

import pytest

import config
from compute import tasks as tasks_module
from compute.coordinator import TaskCoordinator
from networking import client, failover
from networking.server import start_server


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for(check, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            return True
        time.sleep(0.1)
    return False


@pytest.fixture
def fast(monkeypatch):
    monkeypatch.setattr(config, "FAILOVER_AFTER_S", 0.5)
    monkeypatch.setattr(config, "FAILOVER_WAIT_S", 4)
    monkeypatch.setattr(client, "RECONNECT_DELAY_S", 0.2)


def test_elect_picks_first_live_successor(fast):
    started = []
    host, local = failover.elect(
        "W2", [{"worker_id": "W1", "host": "127.0.0.1"}, {"worker_id": "W2", "host": "x"}],
        free_port(), become=lambda port, tasks: started.append(port) or "local")
    # W1 is not listening, so W2 waits for it, gives up, and takes over itself.
    assert host == "127.0.0.1" and local == "local" and len(started) == 1


def test_workers_elect_new_coordinator(fast, monkeypatch, tmp_path):
    port = free_port()
    old = TaskCoordinator()
    stop_old, ready = threading.Event(), threading.Event()
    threading.Thread(target=start_server,
                     kwargs=dict(host="0.0.0.0", port=port, coordinator=old,
                                 ready=ready, stop=stop_old),
                     daemon=True).start()
    assert ready.wait(5)

    taken_over = []

    def become(p, tasks=()):
        local = failover.LocalCoordinator(p, web=False, discovery=False,
                                          out_dir=str(tmp_path / "recovered"))
        if not local.start():
            return None
        local.recover(tasks)
        taken_over.append(local)
        return local

    monkeypatch.setattr(failover, "become_coordinator", become)

    stop_workers = threading.Event()
    for wid in ("W1", "W2"):
        threading.Thread(target=client.worker_loop,
                         args=("127.0.0.1", port, wid),
                         kwargs=dict(stop=stop_workers), daemon=True).start()
        assert wait_for(lambda: wid in old.snapshot()["workers"] or
                        any(w.get("worker_id") == wid for w in old.snapshot()["workers"]))

    # Slow tasks, so some are still unfinished when the coordinator crashes.
    monkeypatch.setitem(tasks_module.TASKS, "slow",
                        lambda params, payload: time.sleep(params["s"]) or payload)
    ids = ["t%d" % i for i in range(8)]
    old.submit_tasks([{"task_id": t, "task_type": "slow", "params": {"s": 0.8},
                       "payload": t.encode(), "name": t} for t in ids])
    time.sleep(2)                                    # replica reaches W1; some tasks finish
    done_before = {t for t in ids if old.task_state[t]["state"] == "completed"}
    assert len(done_before) < len(ids)

    stop_old.set()                                   # the coordinator "crashes"
    try:
        assert wait_for(lambda: len(taken_over) == 1), "nobody took over"
        new = taken_over[0].coordinator

        def both_joined():
            workers = new.snapshot()["workers"]
            ids = workers.keys() if isinstance(workers, dict) else \
                [w.get("worker_id") for w in workers]
            return {"W1", "W2"} <= set(ids)

        assert wait_for(both_joined), "workers did not join the new coordinator"
        time.sleep(1)
        assert len(taken_over) == 1                  # exactly one new coordinator

        # No lost work: every task that was not finished is redone by the new coordinator.
        missing = set(ids) - done_before
        assert missing <= set(new.tasks), "unfinished tasks were not handed over"
        assert wait_for(lambda: all(new.task_state[t]["state"] == "completed" for t in missing))
        assert wait_for(lambda: len(list((tmp_path / "recovered").iterdir())) == len(missing))
    finally:
        stop_workers.set()
        for local in taken_over:
            local.close()
