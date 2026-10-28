from compute.task_scheduler import round_robin, resource_weighted
from compute.recovery import RecoveryManager


def test_round_robin_assigns_all_tasks():
    tasks = [{"task_id": i} for i in range(4)]
    workers = {"w1": {}, "w2": {}}

    result = round_robin(tasks, workers)

    assert len(result) == 4
    assert result[0] == "w1"
    assert result[1] == "w2"
    assert result[2] == "w1"
    assert result[3] == "w2"


def test_scheduler_handles_no_workers():
    tasks = [{"task_id": 1}]

    assert round_robin(tasks, {}) == {}
    assert resource_weighted(tasks, {}) == {}


def test_resource_weighted_assigns_all_tasks():
    tasks = [{"task_id": i} for i in range(10)]
    workers = {
        "small": {
            "cpu_count": 1,
            "cpu_percent": 90,
            "free_ram_mb": 256,
        },
        "large": {
            "cpu_count": 8,
            "cpu_percent": 10,
            "free_ram_mb": 8192,
        },
    }

    result = resource_weighted(tasks, workers)

    assert len(result) == len(tasks)
    assert all(worker in workers for worker in result.values())
    assert list(result.values()).count("large") > list(
        result.values()
    ).count("small")


def test_dead_worker_task_is_requeued():
    manager = RecoveryManager(retry_limit=3, heartbeat_timeout=6)
    manager.register_worker("w1")
    manager.add_task({"task_id": "t1"})

    task = manager.get_next_task()
    manager.assign_task("w1", task)

    now = manager.last_heartbeat["w1"] + 7
    assert manager.check_workers(now=now) == ["w1"]

    recovered = manager.get_next_task()
    assert recovered["task_id"] == "t1"
    assert recovered["retries"] == 1


def test_task_stops_after_retry_limit():
    manager = RecoveryManager(retry_limit=3, heartbeat_timeout=6)

    for worker_id in ("w1", "w2", "w3"):
        manager.register_worker(worker_id)

        if worker_id == "w1":
            manager.add_task({"task_id": "t1"})

        task = manager.get_next_task()
        assert task is not None

        manager.assign_task(worker_id, task)
        now = manager.last_heartbeat[worker_id] + 7
        manager.check_workers(now=now)

    assert manager.get_next_task() is None
    assert len(manager.failed_tasks) == 1
    assert manager.failed_tasks[0]["task_id"] == "t1"
    assert manager.task_retries["t1"] == 3
