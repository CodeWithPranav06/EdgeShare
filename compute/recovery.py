
"""Failure handling for Member B."""

import time
from collections import deque

import config


class RecoveryManager:
    """Track workers and recover tasks after worker failures."""

    def __init__(self, retry_limit=None, heartbeat_timeout=None):
        self.retry_limit = (
            config.TASK_RETRY_LIMIT if retry_limit is None else retry_limit
        )
        self.heartbeat_timeout = (
            config.HEARTBEAT_TIMEOUT_S
            if heartbeat_timeout is None
            else heartbeat_timeout
        )

        self.last_heartbeat = {}
        self.dead_workers = set()
        self.worker_tasks = {}
        self.task_retries = {}
        self.task_queue = deque()
        self.failed_tasks = []
        self.recovery_log = []

    def register_worker(self, worker_id):
        """Register a worker or refresh its heartbeat."""
        self.last_heartbeat[worker_id] = time.monotonic()
        self.dead_workers.discard(worker_id)
        self.worker_tasks.setdefault(worker_id, [])

    def heartbeat(self, worker_id):
        """Record a heartbeat from a worker."""
        self.register_worker(worker_id)

    def add_task(self, task):
        """Add a task to the queue."""
        task_id = task["task_id"]
        self.task_retries.setdefault(task_id, task.get("retries", 0))
        self.task_queue.append(task)

    def assign_task(self, worker_id, task):
        """Record a task currently being processed by a worker."""
        if worker_id in self.dead_workers:
            raise ValueError("Cannot assign a task to a dead worker")

        self.worker_tasks.setdefault(worker_id, []).append(task)

    def complete_task(self, worker_id, task_id):
        """Remove a completed task from the worker's unfinished tasks."""
        tasks = self.worker_tasks.get(worker_id, [])

        self.worker_tasks[worker_id] = [
            task for task in tasks if task["task_id"] != task_id
        ]

    def check_workers(self, now=None):
        """Detect timed-out workers and recover their unfinished tasks.

        Returns a list of worker IDs newly marked dead.
        `now` is optional to make tests deterministic.
        """
        if now is None:
            now = time.monotonic()

        newly_dead = []

        for worker_id, last_seen in list(self.last_heartbeat.items()):
            if worker_id in self.dead_workers:
                continue

            if now - last_seen <= self.heartbeat_timeout:
                continue

            started = time.monotonic()
            self.dead_workers.add(worker_id)
            newly_dead.append(worker_id)

            unfinished = self.worker_tasks.pop(worker_id, [])

            for task in unfinished:
                task_id = task["task_id"]
                retries = self.task_retries.get(
                    task_id, task.get("retries", 0)
                ) + 1
                self.task_retries[task_id] = retries
                task["retries"] = retries

                if retries < self.retry_limit:
                    self.task_queue.append(task)
                else:
                    self.failed_tasks.append(task)

            self.recovery_log.append({
                "worker_id": worker_id,
                "recovered_at": time.monotonic(),
                "duration_s": time.monotonic() - started,
                "requeued": len(self.task_queue),
            })

        return newly_dead

    def get_queued_tasks(self):
        """Return a snapshot of queued tasks without removing them."""
        return list(self.task_queue)

    def remove_queued_task(self, task_id):
        """Remove and return a queued task by ID, or return None."""
        for task in self.task_queue:
            if task["task_id"] == task_id:
                self.task_queue.remove(task)
                return task
        return None

    def get_next_task(self):
        """Return the next queued task, or None if the queue is empty."""
        if not self.task_queue:
            return None

        return self.task_queue.popleft()
