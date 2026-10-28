
"""Schedulers (member B).

Interface: assign(tasks, workers) -> {task_id: worker_id}
"""


def round_robin(tasks, workers):
    """Distribute tasks among workers in rotation."""
    ids = list(workers)

    if not ids:
        return {}

    return {
        task["task_id"]: ids[i % len(ids)]
        for i, task in enumerate(tasks)
    }


def resource_weighted(tasks, workers):
    """Distribute tasks according to available CPU and RAM."""
    ids = list(workers)

    if not ids or not tasks:
        return {}

    weights = {}

    for worker_id, status in workers.items():
        cpu_count = max(0, status.get("cpu_count", 0))
        cpu_percent = min(100, max(0, status.get("cpu_percent", 0)))
        free_ram = max(0, status.get("free_ram_mb", 0))

        free_cpu = cpu_count * (100 - cpu_percent) / 100
        weights[worker_id] = free_cpu * free_ram

    if not any(weights.values()):
        return round_robin(tasks, workers)

    assignments = {}
    counts = {worker_id: 0 for worker_id in ids}

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
