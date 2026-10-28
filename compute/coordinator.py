"""Coordinator-side task management (Member B)."""

import threading

from compute.recovery import RecoveryManager
from compute.task_scheduler import STRATEGIES


class TaskCoordinator:
    """Manage task queues, worker resources, assignments, and results."""

    def __init__(self, strategy="resource_weighted"):
        if strategy not in STRATEGIES:
            raise ValueError("Unknown scheduling strategy: " + strategy)

        self.strategy = STRATEGIES[strategy]
        self.strategy_name = strategy
        self.recovery = RecoveryManager()
        self.workers = {}
        self.tasks = {}
        self.results = {}
        self.assignments = {}
        self.pending_assignments = {}
        self.lock = threading.RLock()

    def register_worker(self, worker_id, status=None):
        with self.lock:
            self.workers[worker_id] = status or {
                "cpu_count": 1,
                "cpu_percent": 0,
                "free_ram_mb": 1,
            }
            self.recovery.register_worker(worker_id)

    def heartbeat(self, worker_id, status=None):
        with self.lock:
            if worker_id not in self.workers:
                return False

            self.recovery.heartbeat(worker_id)
            if status is not None:
                self.workers[worker_id] = status
            return True

    def submit_tasks(self, tasks):
        """Queue tasks with unique IDs, validating the whole batch first."""
        with self.lock:
            batch = list(tasks)
            batch_ids = [task["task_id"] for task in batch]

            if len(batch_ids) != len(set(batch_ids)):
                raise ValueError("Duplicate task_id in submitted batch")

            for task_id in batch_ids:
                if task_id in self.tasks:
                    raise ValueError("Duplicate task_id: " + str(task_id))

            for task in batch:
                self.tasks[task["task_id"]] = task
                self.recovery.add_task(task)

    def next_task(self, worker_id):
        """Assign a queued task using the selected scheduling strategy."""
        with self.lock:
            if worker_id not in self.workers:
                return None
            if worker_id in self.recovery.dead_workers:
                return None

            queued = self.recovery.get_queued_tasks()
            if not queued:
                return None

            queued_ids = {task["task_id"] for task in queued}

            # Recalculate when new queued tasks appear or workers change.
            if not queued_ids.issubset(self.pending_assignments):
                self.pending_assignments.update(
                    self.strategy(queued, self.workers)
                )

            for task in queued:
                task_id = task["task_id"]
                if self.pending_assignments.get(task_id) != worker_id:
                    continue

                selected = self.recovery.remove_queued_task(task_id)
                if selected is None:
                    continue

                self.pending_assignments.pop(task_id, None)
                self.recovery.assign_task(worker_id, selected)
                self.assignments[task_id] = worker_id
                return selected

            return None

    def complete_task(self, worker_id, task_id, result, processing_s=0):
        """Store a result only when it comes from the assigned worker."""
        with self.lock:
            if task_id not in self.tasks:
                return False
            if self.assignments.get(task_id) != worker_id:
                return False
            if task_id in self.results:
                return False

            self.recovery.complete_task(worker_id, task_id)
            self.results[task_id] = {
                "result": result,
                "processing_s": processing_s,
            }
            self.assignments.pop(task_id, None)
            return True

    def check_workers(self, now=None):
        """Detect timed-out workers and recover unfinished tasks."""
        with self.lock:
            dead = self.recovery.check_workers(now=now)

            for worker_id in dead:
                self.workers.pop(worker_id, None)

                # Clear cached scheduler choices for this failed worker.
                for task_id, assigned_worker in list(self.assignments.items()):
                    if assigned_worker == worker_id:
                        self.assignments.pop(task_id, None)

                # Recovered tasks must be scheduled again for live workers.
                for task in self.recovery.get_queued_tasks():
                    self.pending_assignments.pop(task["task_id"], None)

            return dead

    def results_in_task_order(self):
        """Return completed results in submission order."""
        with self.lock:
            return [
                self.results[task_id]
                for task_id in self.tasks
                if task_id in self.results
            ]
