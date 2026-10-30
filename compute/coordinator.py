"""Coordinator-side task management (Member B).

Task life cycle (task_state[task_id]["state"]):
    pending   - queued, waiting for a worker
    assigned  - picked for a worker by next_task(), not sent yet
    running   - sent to the worker (mark_running)
    completed - a valid result was received
    failed    - given up after config.TASK_RETRY_LIMIT failures

Recovery: when a worker times out, disconnects, or reports an error, its unfinished
tasks go back to pending (retries + 1) or, at the retry limit, to failed.

Results: the first valid result wins. A result is accepted only from a worker that was
given that task, and only once; results for unknown, completed, or failed tasks are
ignored. A worker declared dead can still deliver a task it finished, which saves the
re-run. This is at-least-once execution (a task may run twice), not exactly-once.
"""

import threading
import time
from collections import deque

import config
from compute.recovery import RecoveryManager
from compute.task_scheduler import STRATEGIES, eligibility

UNKNOWN_STATUS = {
    "cpu_count": 1,
    "cpu_percent": 0,
    "free_ram_mb": None,
    "battery_percent": None,
    "power_plugged": None,
}


class TaskCoordinator:
    """Manage task queues, worker resources, assignments, and results."""

    def __init__(self, strategy=None):
        strategy = strategy or config.SCHEDULER
        if strategy not in STRATEGIES:
            raise ValueError("Unknown scheduling strategy: " + strategy)

        self.strategy = STRATEGIES[strategy]
        self.strategy_name = strategy
        self.scheduler_state = {}
        self.recovery = RecoveryManager()
        self.workers = {}
        self.worker_info = {}
        self.tasks = {}
        self.task_state = {}
        self.results = {}
        self.assignments = {}
        self.pending_assignments = {}
        self.events = deque(maxlen=200)       # (time, text, warn), newest last
        self.event_count = 0                  # events ever logged (the deque forgets old ones)
        self.started_at = time.time()
        self.lock = threading.RLock()

    # ------------------------------------------------------------------ workers

    def register_worker(self, worker_id, status=None):
        with self.lock:
            if (worker_id not in self.recovery.dead_workers
                    and self.recovery.worker_tasks.get(worker_id)):
                # Same ID registering again: its old session and tasks are gone.
                self.recovery.fail_worker(worker_id)
                self._after_worker_failed(worker_id, "re-registered")

            self.workers[worker_id] = status or dict(UNKNOWN_STATUS)
            info = self.worker_info.setdefault(worker_id, {"completed": 0})
            info.update(status=self.workers[worker_id], failed_reason=None)
            self.recovery.register_worker(worker_id)
            self.pending_assignments.clear()      # worker set changed: reschedule
            self._event("worker %s registered" % worker_id)

    def heartbeat(self, worker_id, status=None):
        with self.lock:
            if worker_id not in self.workers:
                return False

            self.recovery.heartbeat(worker_id)
            if status is not None:
                was_eligible = eligibility(self.workers[worker_id])[0]
                self.workers[worker_id] = status
                self.worker_info[worker_id]["status"] = status
                if eligibility(status)[0] != was_eligible:
                    self.pending_assignments.clear()
            return True

    def worker_disconnected(self, worker_id, reason="connection lost"):
        """Recover a worker's tasks immediately when its connection drops."""
        with self.lock:
            if worker_id not in self.workers:
                return False
            self.recovery.fail_worker(worker_id)
            self._after_worker_failed(worker_id, reason)
            return True

    def check_workers(self, now=None):
        """Detect timed-out workers and recover unfinished tasks."""
        with self.lock:
            dead = self.recovery.check_workers(now=now)
            for worker_id in dead:
                self._after_worker_failed(
                    worker_id,
                    "no heartbeat for %ss" % self.recovery.heartbeat_timeout,
                )
            return dead

    def _after_worker_failed(self, worker_id, reason):
        """Bookkeeping after RecoveryManager has requeued/failed the worker's tasks."""
        self.workers.pop(worker_id, None)
        info = self.worker_info.setdefault(worker_id, {"completed": 0})
        info["failed_reason"] = reason

        queued = {task["task_id"] for task in self.recovery.task_queue}
        requeued, failed = [], []
        for task_id, assigned_worker in list(self.assignments.items()):
            if assigned_worker != worker_id:
                continue
            self.assignments.pop(task_id, None)
            state = self.task_state[task_id]
            if task_id in queued:
                state.update(state="pending", worker=None)
                requeued.append(task_id)
            else:
                state.update(state="failed",
                             error="gave up after %d attempts (last: worker %s, %s)"
                                   % (state["attempts"], worker_id, reason))
                failed.append(task_id)

        # Recovered tasks must be scheduled again for live workers.
        self.pending_assignments.clear()

        message = "worker %s failed: %s" % (worker_id, reason)
        if requeued:
            message += "; requeued %s" % ", ".join(requeued)
        if failed:
            message += "; gave up on %s" % ", ".join(failed)
        self._event(message, warn=True)

    # -------------------------------------------------------------------- tasks

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
                self.task_state[task["task_id"]] = {
                    "state": "pending", "worker": None, "tried_by": [],
                    "attempts": 0, "error": None, "processing_s": None,
                    "task_type": task.get("task_type"), "name": task.get("name"),
                }
                self.recovery.add_task(task)
            if batch:
                self._event("%d task(s) submitted" % len(batch))

    def next_task(self, worker_id):
        """Assign a queued task using the selected scheduling strategy."""
        with self.lock:
            if worker_id not in self.workers:
                return None
            if worker_id in self.recovery.dead_workers:
                return None
            if len(self.recovery.worker_tasks.get(worker_id, [])) >= config.MAX_TASKS_PER_WORKER:
                return None

            queued = self.recovery.get_queued_tasks()
            if not queued:
                return None

            candidates = self._candidates()

            # Forget choices for workers that can no longer take work.
            for task_id, chosen in list(self.pending_assignments.items()):
                if chosen not in candidates:
                    self.pending_assignments.pop(task_id)

            unscheduled = [task for task in queued
                           if task["task_id"] not in self.pending_assignments]
            if unscheduled:
                view = {wid: dict(status, load=self._load(wid))
                        for wid, status in candidates.items()}
                self.pending_assignments.update(
                    self.strategy(unscheduled, view, self.scheduler_state)
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
                state = self.task_state[task_id]
                state["tried_by"].append(worker_id)
                state.update(state="assigned", worker=worker_id,
                             attempts=len(state["tried_by"]))
                return selected

            return None

    def mark_running(self, worker_id, task_id):
        """The task was sent to the worker."""
        with self.lock:
            if self.assignments.get(task_id) == worker_id:
                self.task_state[task_id]["state"] = "running"

    def complete_task(self, worker_id, task_id, result, processing_s=0):
        """Store a valid result; ignore unknown, duplicate, and stale ones."""
        with self.lock:
            if task_id not in self.tasks:
                return False
            state = self.task_state[task_id]
            if task_id in self.results or state["state"] in ("completed", "failed"):
                self._event("ignored duplicate result for %s from %s" % (task_id, worker_id))
                return False

            current = self.assignments.get(task_id)
            if current != worker_id and worker_id not in state["tried_by"]:
                self._event("ignored result for %s from %s (not its worker)"
                            % (task_id, worker_id), warn=True)
                return False

            if current is not None:
                self.recovery.complete_task(current, task_id)
            else:
                self.recovery.remove_queued_task(task_id)
                self.pending_assignments.pop(task_id, None)
            self.assignments.pop(task_id, None)
            if current != worker_id:
                self._event("accepted late result for %s from %s" % (task_id, worker_id), warn=True)

            self.results[task_id] = {
                "result": result,
                "processing_s": processing_s,
                "worker_id": worker_id,
            }
            state.update(state="completed", worker=worker_id, processing_s=processing_s)
            self.worker_info.setdefault(worker_id, {"completed": 0})["completed"] += 1
            return True

    def fail_task(self, worker_id, task_id, reason=""):
        """A worker reported an error for its task: retry it or give up.

        Returns "requeued", "failed", or None if the worker was not running it.
        """
        with self.lock:
            if self.assignments.get(task_id) != worker_id:
                return None
            outcome = self.recovery.fail_task(worker_id, task_id)
            if outcome is None:
                return None

            self.assignments.pop(task_id, None)
            state = self.task_state[task_id]
            if outcome == "requeued":
                state.update(state="pending", worker=None, error=reason)
            else:
                state.update(state="failed",
                             error="gave up after %d attempts: %s" % (state["attempts"], reason))
            self._event("task %s failed on %s: %s (%s)" % (task_id, worker_id, reason, outcome),
                        warn=True)
            return outcome

    def release_task_data(self, task_id):
        """Drop a finished task's payload and result bytes once they were delivered,
        so a long-running coordinator does not keep every image in memory."""
        with self.lock:
            if task_id in self.tasks:
                self.tasks[task_id]["payload"] = b""
            if task_id in self.results:
                self.results[task_id]["result"] = b""

    def results_in_task_order(self):
        """Return completed results in submission order."""
        with self.lock:
            return [
                self.results[task_id]
                for task_id in self.tasks
                if task_id in self.results
            ]

    # ------------------------------------------------------------------ helpers

    def _candidates(self):
        """Live workers that may get new tasks: the eligible ones, or every live
        worker when none is eligible (better slow than stuck)."""
        live = {wid: status for wid, status in self.workers.items()
                if wid not in self.recovery.dead_workers}
        eligible = {wid: status for wid, status in live.items() if eligibility(status)[0]}
        return eligible or live

    def _load(self, worker_id):
        """Tasks running on or already scheduled for a worker."""
        running = len(self.recovery.worker_tasks.get(worker_id, []))
        scheduled = sum(1 for chosen in self.pending_assignments.values() if chosen == worker_id)
        return running + scheduled

    def _event(self, text, warn=False):
        self.events.append((time.time(), text, warn))
        self.event_count += 1
        if warn:
            print("[coordinator] " + text + "\n", end="", flush=True)

    def snapshot(self):
        """A copy of the coordinator state for the dashboard (plain dicts and lists)."""
        with self.lock:
            now = time.monotonic()
            running = {wid: [task["task_id"] for task in tasks]
                       for wid, tasks in self.recovery.worker_tasks.items()}
            workers = []
            for worker_id, info in self.worker_info.items():
                status = info.get("status") or UNKNOWN_STATUS
                ok, reason = eligibility(status)
                if worker_id in self.recovery.dead_workers or worker_id not in self.workers:
                    state, reason = "failed", info.get("failed_reason") or "disconnected"
                elif running.get(worker_id):
                    state = "busy"
                elif not ok:
                    state = "unavailable"
                else:
                    state = "idle"
                last = self.recovery.last_heartbeat.get(worker_id)
                workers.append({
                    "worker_id": worker_id, "state": state, "reason": reason,
                    "cpu_count": status.get("cpu_count"),
                    "cpu_percent": status.get("cpu_percent"),
                    "free_ram_mb": status.get("free_ram_mb"),
                    "battery_percent": status.get("battery_percent"),
                    "power_plugged": status.get("power_plugged"),
                    "last_seen_s": None if last is None else round(now - last, 1),
                    "tasks": running.get(worker_id, []),
                    "completed": info.get("completed", 0),
                })

            tasks = [dict(self.task_state[task_id], task_id=task_id, tried_by=list(
                self.task_state[task_id]["tried_by"])) for task_id in self.tasks]
            counts = {name: 0 for name in ("pending", "assigned", "running", "completed", "failed")}
            for task in tasks:
                counts[task["state"]] += 1
            counts["total"] = len(tasks)

            return {
                "strategy": self.strategy_name,
                "uptime_s": round(time.time() - self.started_at, 1),
                "workers": workers,
                "tasks": tasks,
                "counts": counts,
                "events": list(self.events),
                "event_count": self.event_count,
            }
