from compute.coordinator import TaskCoordinator


def test_register_worker_and_heartbeat():
    coordinator = TaskCoordinator()
    coordinator.register_worker("w1")

    assert "w1" in coordinator.workers
    assert coordinator.heartbeat("w1") is True
    assert coordinator.heartbeat("unknown") is False


def test_submit_assign_and_complete_task():
    coordinator = TaskCoordinator()
    coordinator.register_worker("w1")

    task = {
        "task_id": "t1",
        "task_type": "hash",
        "params": {},
        "payload": b"hello",
    }
    coordinator.submit_tasks([task])

    assigned = coordinator.next_task("w1")
    assert assigned["task_id"] == "t1"

    assert coordinator.complete_task("w1", "t1", b"result", 0.1) is True
    assert coordinator.results["t1"]["result"] == b"result"
    assert coordinator.results["t1"]["processing_s"] == 0.1


def test_duplicate_task_submission_is_rejected():
    coordinator = TaskCoordinator()
    coordinator.submit_tasks([{"task_id": "t1"}])

    try:
        coordinator.submit_tasks([{"task_id": "t1"}])
        assert False, "Expected duplicate task ID to be rejected"
    except ValueError:
        pass


def test_round_robin_strategy_assigns_tasks_to_workers():
    coordinator = TaskCoordinator(strategy="round_robin")
    coordinator.register_worker("w1")
    coordinator.register_worker("w2")

    coordinator.submit_tasks([
        {"task_id": "t1"},
        {"task_id": "t2"},
    ])

    first = coordinator.next_task("w1")
    second = coordinator.next_task("w2")

    assert first["task_id"] == "t1"
    assert second["task_id"] == "t2"


def test_wrong_worker_cannot_complete_task():
    coordinator = TaskCoordinator()
    coordinator.register_worker("w1")
    coordinator.register_worker("w2")
    coordinator.submit_tasks([{"task_id": "t1"}])

    task = coordinator.next_task("w1")
    assert task["task_id"] == "t1"

    assert coordinator.complete_task("w2", "t1", b"fake-result") is False
    assert "t1" not in coordinator.results


def test_duplicate_task_ids_in_same_batch_are_rejected():
    coordinator = TaskCoordinator()

    try:
        coordinator.submit_tasks([
            {"task_id": "t1"},
            {"task_id": "t1"},
        ])
        assert False, "Expected duplicate task IDs to be rejected"
    except ValueError:
        pass

    assert coordinator.tasks == {}
    assert coordinator.recovery.get_queued_tasks() == []


def test_task_is_reassigned_after_worker_failure():
    coordinator = TaskCoordinator(strategy="round_robin")
    coordinator.register_worker("w1")
    coordinator.register_worker("w2")

    coordinator.submit_tasks([
        {"task_id": "t1"},
    ])

    task = coordinator.next_task("w1")
    assert task is not None
    assert task["task_id"] == "t1"

    # Simulate w1 missing its heartbeat timeout.
    now = coordinator.recovery.last_heartbeat["w1"] + 10

    # Keep w2 alive at the simulated time when w1 times out.
    coordinator.recovery.last_heartbeat["w2"] = now

    dead = coordinator.check_workers(now=now)
    assert "w1" in dead
    assert "w2" not in dead

    # The recovered task should be available to the surviving worker.
    reassigned = coordinator.next_task("w2")
    assert reassigned is not None
    assert reassigned["task_id"] == "t1"
    assert coordinator.assignments["t1"] == "w2"
