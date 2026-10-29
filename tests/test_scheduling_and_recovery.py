"""Round-Robin, resource/battery eligibility, heartbeat timeout, and recovery
through the real TaskCoordinator (no network)."""
import pytest

import config
from compute.coordinator import TaskCoordinator
from compute.task_scheduler import eligibility, resource_weighted, round_robin

HEALTHY = {"cpu_count": 4, "cpu_percent": 10, "free_ram_mb": 4096,
           "battery_percent": None, "power_plugged": None}


def status(**changes):
    return dict(HEALTHY, **changes)


def make(strategy="round_robin", workers=("W1", "W2", "W3")):
    coordinator = TaskCoordinator(strategy=strategy)
    for worker_id in workers:
        coordinator.register_worker(worker_id, status())
    return coordinator


def tasks(*ids):
    return [{"task_id": task_id, "task_type": "hash", "params": {}, "payload": b"x"}
            for task_id in ids]


def run_one(coordinator, worker_id):
    """Worker pulls one task and finishes it. Returns the task id (or None)."""
    task = coordinator.next_task(worker_id)
    if task is None:
        return None
    assert coordinator.complete_task(worker_id, task["task_id"], b"ok", 0.01)
    return task["task_id"]


# ---- Round-Robin ---------------------------------------------------------------------

def test_round_robin_six_tasks_over_three_workers():
    """The example for the report: T1..T6 over W1, W2, W3."""
    coordinator = make()
    coordinator.submit_tasks(tasks("T1", "T2", "T3", "T4", "T5", "T6"))
    order = []
    for _ in range(2):
        for worker_id in ("W1", "W2", "W3"):
            order.append((run_one(coordinator, worker_id), worker_id))
    assert order == [("T1", "W1"), ("T2", "W2"), ("T3", "W3"),
                     ("T4", "W1"), ("T5", "W2"), ("T6", "W3")]


def test_round_robin_rotates_when_tasks_arrive_one_at_a_time():
    """Old behaviour sent every single-task submission to the first worker."""
    coordinator = make()
    got = []
    for number in range(6):
        coordinator.submit_tasks(tasks("T%d" % number))
        for worker_id in ("W1", "W2", "W3"):
            if run_one(coordinator, worker_id):
                got.append(worker_id)
    assert got == ["W1", "W2", "W3", "W1", "W2", "W3"]


def test_round_robin_function_keeps_state_between_calls():
    workers = {"W1": {}, "W2": {}, "W3": {}}
    state = {}
    assert round_robin(tasks("a", "b"), workers, state) == {"a": "W1", "b": "W2"}
    assert round_robin(tasks("c", "d"), workers, state) == {"c": "W3", "d": "W1"}


def test_round_robin_skips_dead_worker():
    coordinator = make()
    assert coordinator.worker_disconnected("W2")
    coordinator.submit_tasks(tasks("T1", "T2", "T3", "T4"))
    assert coordinator.next_task("W2") is None
    done = [run_one(coordinator, w) for w in ("W1", "W3", "W1", "W3")]
    assert done == ["T1", "T2", "T3", "T4"]


def test_round_robin_skips_unavailable_worker():
    coordinator = make()
    coordinator.heartbeat("W2", status(cpu_percent=99))
    coordinator.submit_tasks(tasks("T1", "T2"))
    assert coordinator.next_task("W2") is None
    assert run_one(coordinator, "W1") == "T1"
    assert run_one(coordinator, "W3") == "T2"


def test_all_workers_unavailable_still_get_work():
    coordinator = make(workers=("W1",))
    coordinator.heartbeat("W1", status(cpu_percent=99))
    coordinator.submit_tasks(tasks("T1"))
    assert run_one(coordinator, "W1") == "T1"


def test_one_task_in_flight_per_worker():
    coordinator = make(workers=("W1",))
    coordinator.submit_tasks(tasks("T1", "T2"))
    assert coordinator.next_task("W1")["task_id"] == "T1"
    assert coordinator.next_task("W1") is None            # still busy with T1
    assert coordinator.complete_task("W1", "T1", b"ok")
    assert coordinator.next_task("W1")["task_id"] == "T2"


def test_new_worker_shares_queued_tasks():
    coordinator = make(workers=("W1",))
    coordinator.submit_tasks(tasks("T1", "T2", "T3", "T4"))
    assert run_one(coordinator, "W1") == "T1"
    coordinator.register_worker("W2", status())
    assert {run_one(coordinator, "W1"), run_one(coordinator, "W2")} == {"T2", "T3"}


# ---- resources and battery -----------------------------------------------------------

def test_missing_battery_is_eligible():
    assert eligibility(status(battery_percent=None, power_plugged=None)) == (True, "")


def test_low_battery_unplugged_is_unavailable():
    ok, reason = eligibility(status(battery_percent=config.LOW_BATTERY_PERCENT - 5,
                                    power_plugged=False))
    assert not ok and "battery" in reason


def test_low_battery_while_charging_is_eligible():
    assert eligibility(status(battery_percent=5, power_plugged=True))[0]


def test_overloaded_and_low_ram_are_unavailable():
    assert not eligibility(status(cpu_percent=config.MAX_CPU_PERCENT))[0]
    assert not eligibility(status(free_ram_mb=config.MIN_FREE_RAM_MB - 1))[0]


def test_low_battery_worker_is_skipped_when_others_exist():
    coordinator = make(strategy="resource_weighted", workers=("W1", "W2"))
    coordinator.heartbeat("W1", status(battery_percent=8, power_plugged=False))
    coordinator.submit_tasks(tasks("T1", "T2", "T3"))
    assert coordinator.next_task("W1") is None
    assert [run_one(coordinator, "W2") for _ in range(3)] == ["T1", "T2", "T3"]


def test_resource_weighted_prefers_stronger_worker():
    workers = {"small": status(cpu_count=2, cpu_percent=50, free_ram_mb=1024),
               "large": status(cpu_count=8, cpu_percent=10, free_ram_mb=8192)}
    result = resource_weighted(tasks(*range(10)), workers)
    assert list(result.values()).count("large") > list(result.values()).count("small")


def test_resource_weighted_counts_existing_load():
    workers = {"W1": status(load=3), "W2": status(load=0)}
    assert resource_weighted(tasks("T1"), workers) == {"T1": "W2"}


def test_heartbeat_status_updates_scheduling():
    coordinator = make(strategy="resource_weighted", workers=("W1", "W2"))
    coordinator.heartbeat("W1", status(cpu_percent=99))
    snap = {w["worker_id"]: w for w in coordinator.snapshot()["workers"]}
    assert snap["W1"]["state"] == "unavailable" and snap["W2"]["state"] == "idle"


# ---- heartbeats ------------------------------------------------------------------------

def test_heartbeat_timeout_marks_worker_failed():
    coordinator = make()
    last = coordinator.recovery.last_heartbeat["W1"]
    for other in ("W2", "W3"):
        coordinator.recovery.last_heartbeat[other] = last + config.HEARTBEAT_TIMEOUT_S + 1
    assert coordinator.check_workers(now=last + config.HEARTBEAT_TIMEOUT_S + 1) == ["W1"]
    snap = {w["worker_id"]: w for w in coordinator.snapshot()["workers"]}
    assert snap["W1"]["state"] == "failed" and "heartbeat" in snap["W1"]["reason"]


def test_slow_task_with_heartbeats_is_not_dead():
    coordinator = make(workers=("W1",))
    coordinator.submit_tasks(tasks("T1"))
    coordinator.next_task("W1")
    # The task takes 3 timeouts, but the worker keeps sending heartbeats.
    for _ in range(3):
        coordinator.heartbeat("W1", status(cpu_percent=90))
        now = coordinator.recovery.last_heartbeat["W1"] + config.HEARTBEAT_TIMEOUT_S - 1
        assert coordinator.check_workers(now=now) == []
    assert coordinator.complete_task("W1", "T1", b"ok")


def test_heartbeat_from_unknown_worker_is_rejected():
    assert make().heartbeat("ghost", status()) is False


# ---- failure and recovery --------------------------------------------------------------

def test_disconnect_mid_task_requeues_to_another_worker():
    coordinator = make()
    coordinator.submit_tasks(tasks("T1", "T2", "T3"))
    coordinator.next_task("W1")
    coordinator.next_task("W2")
    coordinator.next_task("W3")
    assert coordinator.worker_disconnected("W2")
    assert coordinator.task_state["T2"]["state"] == "pending"
    assert coordinator.complete_task("W1", "T1", b"1")
    # Round-Robin continues the rotation over the live workers (W1, W3): it is W3's turn.
    assert coordinator.next_task("W1") is None
    assert coordinator.complete_task("W3", "T3", b"3")
    assert coordinator.next_task("W3")["task_id"] == "T2"
    assert coordinator.complete_task("W3", "T2", b"2")
    assert [r["result"] for r in coordinator.results_in_task_order()] == [b"1", b"2", b"3"]
    assert coordinator.results["T2"]["worker_id"] == "W3"


def test_worker_error_retries_then_gives_up():
    coordinator = make()
    coordinator.submit_tasks(tasks("T1"))
    outcomes = []
    for worker_id in ("W1", "W2", "W3"):
        task = coordinator.next_task(worker_id)
        assert task["task_id"] == "T1"
        outcomes.append(coordinator.fail_task(worker_id, "T1", "bad image"))
    assert outcomes == ["requeued", "requeued", "failed"]
    assert coordinator.task_state["T1"]["state"] == "failed"
    assert "bad image" in coordinator.task_state["T1"]["error"]
    assert all(coordinator.next_task(w) is None for w in ("W1", "W2", "W3"))


def test_retry_limit_on_repeated_crashes_stops():
    coordinator = make(workers=())
    coordinator.submit_tasks(tasks("T1"))
    for number in range(config.TASK_RETRY_LIMIT):
        worker_id = "W%d" % number
        coordinator.register_worker(worker_id, status())
        assert coordinator.next_task(worker_id)["task_id"] == "T1"
        coordinator.worker_disconnected(worker_id)
    assert coordinator.task_state["T1"]["state"] == "failed"
    coordinator.register_worker("fresh", status())
    assert coordinator.next_task("fresh") is None


def test_result_from_wrong_worker_is_ignored():
    coordinator = make()
    coordinator.submit_tasks(tasks("T1"))
    coordinator.next_task("W1")
    assert coordinator.complete_task("W2", "T1", b"fake") is False
    assert coordinator.task_state["T1"]["state"] == "assigned"


def test_duplicate_result_is_ignored():
    coordinator = make()
    coordinator.submit_tasks(tasks("T1"))
    coordinator.next_task("W1")
    assert coordinator.complete_task("W1", "T1", b"first")
    assert coordinator.complete_task("W1", "T1", b"second") is False
    assert coordinator.results["T1"]["result"] == b"first"


def test_unknown_task_result_is_ignored():
    assert make().complete_task("W1", "nope", b"x") is False


def test_late_result_after_timeout_is_accepted_once():
    """W1 finishes just as it is declared dead: its result is used, the re-run is not."""
    coordinator = make(workers=("W1", "W2"))
    coordinator.submit_tasks(tasks("T1"))
    coordinator.next_task("W1")
    coordinator.worker_disconnected("W1")
    assert coordinator.next_task("W2")["task_id"] == "T1"
    assert coordinator.complete_task("W1", "T1", b"from W1")
    assert coordinator.complete_task("W2", "T1", b"from W2") is False
    assert coordinator.results["T1"]["result"] == b"from W1"
    assert coordinator.next_task("W2") is None          # W2 is free again


def test_result_for_failed_task_is_ignored():
    coordinator = make(workers=("W1",))
    coordinator.submit_tasks(tasks("T1"))
    for _ in range(config.TASK_RETRY_LIMIT):
        coordinator.next_task("W1")
        coordinator.fail_task("W1", "T1", "boom")
    assert coordinator.complete_task("W1", "T1", b"late") is False


def test_reregistering_worker_requeues_its_old_task():
    coordinator = make(workers=("W1",))
    coordinator.submit_tasks(tasks("T1"))
    coordinator.next_task("W1")
    coordinator.register_worker("W1", status())          # worker restarted
    assert coordinator.task_state["T1"]["state"] == "pending"
    assert coordinator.next_task("W1")["task_id"] == "T1"


def test_snapshot_counts_and_events():
    coordinator = make(workers=("W1", "W2"))
    coordinator.submit_tasks(tasks("T1", "T2", "T3"))
    run_one(coordinator, "W1")
    coordinator.next_task("W2")
    coordinator.mark_running("W2", "T2")
    snap = coordinator.snapshot()
    assert snap["counts"] == {"pending": 1, "assigned": 0, "running": 1,
                              "completed": 1, "failed": 0, "total": 3}
    workers = {w["worker_id"]: w for w in snap["workers"]}
    assert workers["W2"]["state"] == "busy" and workers["W2"]["tasks"] == ["T2"]
    assert workers["W1"]["completed"] == 1
    assert any("submitted" in text for _, text, _ in snap["events"])


@pytest.mark.parametrize("strategy", ["round_robin", "resource_weighted"])
def test_every_task_finishes_once_with_a_crash(strategy):
    coordinator = make(strategy=strategy)
    coordinator.submit_tasks(tasks(*["T%02d" % n for n in range(12)]))
    crashed = False
    rounds = 0
    while len(coordinator.results) < 12 and rounds < 100:
        rounds += 1
        for worker_id in ("W1", "W2", "W3"):
            task = coordinator.next_task(worker_id)
            if task is None:
                continue
            if worker_id == "W3" and not crashed:
                coordinator.worker_disconnected("W3")     # dies holding a task
                crashed = True
                continue
            coordinator.complete_task(worker_id, task["task_id"], task["task_id"].encode())
    assert crashed
    assert [r["result"] for r in coordinator.results_in_task_order()] == \
        [("T%02d" % n).encode() for n in range(12)]
