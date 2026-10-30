
"""Schedulers (member B).

Interface: assign(tasks, workers, state=None) -> {task_id: worker_id}

`workers` maps worker_id -> status dict from monitoring.resource_monitor.get_status(),
in registration order. The coordinator may add "load" (tasks already queued for or
running on that worker). `state` is an optional dict the coordinator keeps between
calls so a strategy can remember where it stopped (Round-Robin uses it).
"""

import config


def eligibility(status):
    """Return (eligible, reason). Unknown values never make a worker ineligible."""
    status = status or {}

    cpu_percent = status.get("cpu_percent")
    if cpu_percent is not None and cpu_percent >= config.MAX_CPU_PERCENT:
        return False, "CPU overloaded (%s%%)" % cpu_percent

    free_ram = status.get("free_ram_mb")
    if free_ram is not None and free_ram < config.MIN_FREE_RAM_MB:
        return False, "low RAM (%s MB free)" % free_ram

    battery = status.get("battery_percent")
    if (battery is not None and status.get("power_plugged") is False
            and battery < config.LOW_BATTERY_PERCENT):
        return False, "low battery (%s%%, not charging)" % battery

    return True, ""


def round_robin(tasks, workers, state=None):
    """Distribute tasks among workers in rotation.

    With `state`, the rotation continues where the previous call stopped, so tasks
    that arrive one at a time still go W1, W2, W3, W1, ... instead of always W1.
    """
    ids = list(workers)

    if not ids:
        return {}

    turn = state.get("turn", 0) if state is not None else 0
    assignments = {
        task["task_id"]: ids[(turn + i) % len(ids)]
        for i, task in enumerate(tasks)
    }
    if state is not None:
        state["turn"] = turn + len(assignments)
    return assignments


def resource_weighted(tasks, workers, state=None):
    """Distribute tasks according to available CPU and RAM.

    weight = cpu_count * (100 - cpu_percent) / 100 * free_ram_mb
    Each task goes to the worker with the highest weight / (load + 1), where load is
    the number of tasks it already has, so a strong worker gets proportionally more
    tasks without starving the others.
    """
    ids = list(workers)

    if not ids or not tasks:
        return {}

    weights = {}

    for worker_id, status in workers.items():
        cpu_count = max(0, status.get("cpu_count") or 0)
        cpu_percent = min(100, max(0, status.get("cpu_percent") or 0))
        free_ram = status.get("free_ram_mb")
        free_ram = 1 if free_ram is None else max(0, free_ram)

        free_cpu = cpu_count * (100 - cpu_percent) / 100
        weights[worker_id] = free_cpu * free_ram

    if not any(weights.values()):
        return round_robin(tasks, workers, state)

    assignments = {}
    counts = {worker_id: workers[worker_id].get("load", 0) for worker_id in ids}

    for task in tasks:
        worker_id = max(
            ids,
            key=lambda wid: weights[wid] / (counts[wid] + 1)
        )
        assignments[task["task_id"]] = worker_id
        counts[worker_id] += 1

    return assignments


STRATEGIES = {
    "round_robin": round_robin,
    "resource_weighted": resource_weighted,
}
