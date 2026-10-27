"""Schedulers (member B).

Interface: assign(tasks, workers) -> {task_id: worker_id}
workers: {worker_id: {"cpu_count", "cpu_percent", "free_ram_mb"}}
"""


def round_robin(tasks, workers):
    ids = list(workers)
    return {t["task_id"]: ids[i % len(ids)] for i, t in enumerate(tasks)}


def resource_weighted(tasks, workers):
    """TODO: give each worker a share of tasks proportional to its free CPU
    (cpu_count * (100 - cpu_percent)) and free RAM."""
    raise NotImplementedError


# TODO: pull-based queue: workers request the next task when free.

STRATEGIES = {"round_robin": round_robin, "resource_weighted": resource_weighted}
