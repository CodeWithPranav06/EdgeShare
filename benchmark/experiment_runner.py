"""
experiment_runner.py - run experiments E1-E6 (5 repeats each) and fill results.csv.

    python -m benchmark.experiment_runner --list                          # what each experiment does
    python -m benchmark.experiment_runner E2 --images-dir data/images     # one experiment, 5 repeats
    python -m benchmark.experiment_runner all --images-dir data/images    # every experiment
    python -m benchmark.experiment_runner E1 --quick --images-dir data/images   # tiny smoke test

Each run: build the job -> hand tasks to Member B's TaskCoordinator -> workers pull tasks
with next_task() and report with complete_task() -> merge the results in order -> compare
the SHA-256 with the single-laptop baseline (correctness_check.py) -> write one row to
results/results.csv.

WHERE THE WORKERS RUN
  The built-in backend (run_distributed_sim) runs workers as threads on THIS laptop, each
  with its own process, talking to the real TaskCoordinator. Use it to check that the whole
  pipeline works. For the real demo, replace run_distributed_sim with a function that has
  the same arguments and returns the same dict, but sends tasks to Member A's real workers.
  Everything else (baseline lookup, correctness check, CSV, repeats) stays the same.

EDIT THESE TWO THINGS TO MATCH THE TEAM
  1. EXPERIMENT_INFO / make_plan(): what E1-E6 really mean in your project plan.
  2. adapt_row_for_log_run(): maps our row onto the team's COLUMNS; by default every run is
     also logged through benchmark/run_experiments.py:log_run() to config.RESULTS_CSV.
"""

import argparse
import csv
import hashlib
import importlib
import multiprocessing
import os
import shutil
import statistics
import sys
import tempfile
import threading
import time
import uuid
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from pathlib import Path

from benchmark import correctness_check as cc
from benchmark.baseline_benchmark import HashTask, MatmulTask, ResizeTask, time_once
from benchmark.fault_injection import FaultInjector
from compute import image_processor as ip

COORDINATOR_MODULES = ["compute.coordinator", "compute.task_coordinator"]
CHUNK = {"resize": 5, "hash": 8, "matmul": 10}                 # items per task
SIZES = {"resize": [50, 100, 200, 500], "hash": [64, 128, 256, 512], "matmul": [100, 200, 300, 400]}
QUICK_SIZES = {"resize": [10, 20, 30], "hash": [16, 32, 64], "matmul": [50, 100, 150]}
UNITS = {"resize": "images/s", "hash": "MB/s", "matmul": "rows/s"}
HEARTBEAT_S = 0.25

EXPERIMENT_INFO = {
    "E1": "Overhead check: 1 worker vs the single laptop (speedup below 1 is expected)",
    "E2": "Scaling: 1..N workers, fixed job (speedup and efficiency vs number of workers)",
    "E3": "Job size: small to large jobs with N workers (find the break-even size)",
    "E4": "Fault tolerance: kill one worker mid-run, check no task is lost",
    "E5": "Schedulers: compare every strategy in STRATEGIES on equal workers",
    "E6": "Heterogeneity: one worker is slow and loaded, compare strategies",
}

CSV_FIELDS = ["Experiment_ID", "Run_ID", "Timestamp", "Task_Type", "Task_Size", "Strategy", "Workers",
              "Repeat", "Variant", "Baseline_Time", "Baseline_Source", "Total_Time", "Speedup",
              "Efficiency", "Throughput", "Throughput_Unit", "Transfer_Time", "Processing_Time",
              "Merge_Time", "Tasks", "Correct", "Completed", "Failed_Workers", "Detection_Delay",
              "Recovery_Time", "Overhead_vs_Control", "Notes"]


# --------------------------------------------------------------------------
# Loading the team's code
# --------------------------------------------------------------------------

def load_task_coordinator(module_name=None):
    """Import Member B's TaskCoordinator class."""
    names = [module_name] if module_name else COORDINATOR_MODULES
    for name in names:
        try:
            module = importlib.import_module(name)
        except ModuleNotFoundError as error:
            if error.name in (name, name.split(".")[0]):
                continue                                  # that module simply does not exist
            raise                                         # it exists but one of ITS imports is missing
        if hasattr(module, "TaskCoordinator"):
            return module.TaskCoordinator
    sys.exit("error: could not import TaskCoordinator. Run from the repo root, or pass "
             "--coordinator-module package.module (tried: %s)" % ", ".join(names))


def load_log_run(spec):
    """'package.module:function' -> the team's log_run function."""
    module_name, _, function_name = spec.partition(":")
    return getattr(importlib.import_module(module_name), function_name or "log_run")


def adapt_row_for_log_run(row):
    """Turn our detailed row into the team's COLUMNS (benchmark/run_experiments.py)."""
    failed = [w for w in str(row.get("Failed_Workers", "")).split(";") if w]
    return {
        "experiment": row["Experiment_ID"], "task_type": row["Task_Type"],
        "task_size": row["Task_Size"], "nodes": row["Workers"], "scheduler": row["Strategy"],
        "transfer_s": row["Transfer_Time"], "processing_s": row["Processing_Time"],
        "merge_s": row["Merge_Time"], "total_s": row["Total_Time"],
        "throughput": row["Throughput"], "speedup": row["Speedup"],
        "efficiency": row["Efficiency"], "failures": len(failed),
        "recovery_s": row["Recovery_Time"], "hash_ok": row["Correct"] == cc.PASS,
    }


# --------------------------------------------------------------------------
# Work done by a worker (module-level so it can run in another process)
# --------------------------------------------------------------------------

def compute_chunk(task_type, payload, shared=None):
    """Run one task. Returns (result list, seconds spent computing)."""
    start = time.perf_counter()
    if task_type == "resize":
        result = [ip.resize_bytes(data, ip.DEFAULT_SIZE) for data in payload]
    elif task_type == "hash":
        result = [hashlib.sha256(block).digest() for block in payload]
    elif task_type == "matmul":
        columns = list(zip(*shared))
        result = [[sum(x * y for x, y in zip(row, column)) for column in columns] for row in payload]
    else:
        raise ValueError("unknown task type: " + task_type)
    return result, time.perf_counter() - start


def load_payload(task):
    """What gets 'sent' to the worker. For resize, the image files are read here (that is
    part of the transfer cost, like reading a file before sending it over the network)."""
    if task["task_type"] == "resize":
        payload = []
        for path in task["payload"]:
            with open(path, "rb") as f:
                payload.append(f.read())
        return payload
    return task["payload"]


# --------------------------------------------------------------------------
# Building jobs and merging results
# --------------------------------------------------------------------------

def build_tasks(task_type, workload, chunk):
    """Split the baseline workload into tasks with unique IDs (in order)."""
    if task_type == "matmul":
        items, shared = workload
    else:
        items, shared = workload, None

    tasks = []
    for number, start in enumerate(range(0, len(items), chunk)):
        part = items[start:start + chunk]
        task = {"task_id": "T%04d" % number, "task_type": task_type, "payload": part,
                "cost": len(part), "size": len(part)}
        if shared is not None:
            task["shared"] = shared
        tasks.append(task)
    return tasks


def merge_results(task_type, tasks, results, output_dir):
    """Put task results back together in the original order and return the job's digest."""
    flat = [item for entry in results for item in entry["result"]]
    if task_type == "resize":
        paths = [path for task in tasks for path in task["payload"]]
        saved = [ip.save_output(output_dir, ip.output_name(path, ip.DEFAULT_SIZE), data)
                 for path, data in zip(paths, flat)]
        return cc.digest_resize_outputs(saved)
    if task_type == "hash":
        return cc.digest_hash_blocks(flat)
    return cc.digest_matmul_rows(flat)


# --------------------------------------------------------------------------
# Simulated workers (threads on this laptop, one process each)
# --------------------------------------------------------------------------

class SimWorker(threading.Thread):
    """Pulls tasks from the coordinator, computes them, reports results and heartbeats."""

    def __init__(self, index, coordinator, pool, stop, collect, slow=1.0, cpu_percent=5.0):
        super().__init__(daemon=True)
        self.index = index
        self.worker_id = "W%d" % (index + 1)
        self.coordinator = coordinator
        self.pool = pool
        self.stop = stop
        self.collect = collect                 # function(record) storing per-task timing
        self.slow = slow                       # 3.0 = three times slower than normal
        self.cpu_percent = cpu_percent         # what this worker reports as its load
        self.crashed = threading.Event()
        self.crash_time = None
        self.current_task = None
        self._last_beat = 0.0

    def status(self):
        return {"cpu_count": 1, "cpu_percent": self.cpu_percent, "free_ram_mb": 4096}

    def crash(self):
        """Simulate the laptop dying: no more heartbeats, the running task is lost."""
        self.crash_time = time.perf_counter()
        self.crashed.set()

    def _beat(self):
        now = time.perf_counter()
        if now - self._last_beat >= HEARTBEAT_S:
            self.coordinator.heartbeat(self.worker_id, self.status())
            self._last_beat = now

    def _pump(self, finished):
        """Keep sending heartbeats until finished() is true. False if we crashed meanwhile."""
        while not finished():
            if self.crashed.is_set() or self.stop.is_set():
                return False
            self._beat()
            time.sleep(0.005)
        return not self.crashed.is_set()

    def run(self):
        while not self.stop.is_set() and not self.crashed.is_set():
            self._beat()
            task = self.coordinator.next_task(self.worker_id)
            if task is None:
                time.sleep(0.01)
                continue

            self.current_task = task
            started = time.perf_counter()
            try:
                payload = load_payload(task)
                future = self.pool.submit(compute_chunk, task["task_type"], payload, task.get("shared"))
                if not self._pump(future.done):
                    return                                    # crashed mid-task: the result is lost
                result, computed = future.result()
                extra = computed * (self.slow - 1.0)          # simulate a slower laptop
                if extra > 0:
                    until = time.perf_counter() + extra
                    if not self._pump(lambda: time.perf_counter() >= until):
                        return
            except Exception as error:                        # a broken worker just goes silent
                print("  ! %s failed on %s: %s: %s" % (self.worker_id, task["task_id"], type(error).__name__, error))
                return

            processing = computed * self.slow
            round_trip = time.perf_counter() - started
            if self.coordinator.complete_task(self.worker_id, task["task_id"], result, processing):
                self.collect({"worker": self.worker_id, "processing": processing,
                              "transfer": max(0.0, round_trip - processing)})
            self.current_task = None


def run_distributed_sim(ctx, TaskCoordinator, tasks, cfg, fault_fraction=None, timeout=300.0):
    """Run `tasks` through the real TaskCoordinator with simulated workers.

    Returns a dict: coordinator, results (in task order), complete (bool), compute_seconds,
    stats (per-task timing), failed_workers, detection_delay, recovery_time.
    """
    coordinator = TaskCoordinator(cfg["strategy"])
    stop = threading.Event()
    stats, stats_lock = [], threading.Lock()

    def collect(record):
        with stats_lock:
            stats.append(record)

    count = cfg["workers"]
    workers = []
    for i in range(count):
        loaded = cfg.get("loaded") and i == count - 1
        worker = SimWorker(i, coordinator, ctx.get_pool(i), stop, collect,
                           slow=3.0 if loaded else 1.0, cpu_percent=90.0 if loaded else 5.0)
        workers.append(worker)
        coordinator.register_worker(worker.worker_id, worker.status())   # all registered before any task

    victim = workers[-1]
    injector = None
    if fault_fraction:
        injector = FaultInjector(fault_fraction, victim.crash, "worker %s dies" % victim.worker_id)

    start = time.perf_counter()
    coordinator.submit_tasks(tasks)
    for worker in workers:
        worker.start()

    detected = {}
    while True:
        with coordinator.lock:
            done = len(coordinator.results)
        if done >= len(tasks):
            break
        for worker_id in coordinator.check_workers():
            detected.setdefault(worker_id, time.perf_counter())
        if injector:
            injector.poll(done, len(tasks))
        if not any(w.is_alive() for w in workers) or time.perf_counter() - start > timeout:
            break
        time.sleep(0.02)
    end = time.perf_counter()

    stop.set()
    for worker in workers:
        worker.join(timeout=5)
    for i, worker in enumerate(workers):
        if worker.crashed.is_set():
            ctx.discard_pool(i)                  # its process may still be busy with the lost task

    with coordinator.lock:
        complete = len(coordinator.results) >= len(tasks)
        results = coordinator.results_in_task_order()

    failed = [w.worker_id for w in workers if w.crashed.is_set()]
    detection_delay = recovery_time = None
    if victim.crashed.is_set() and victim.crash_time:
        if victim.worker_id in detected:
            detection_delay = detected[victim.worker_id] - victim.crash_time
        recovery_time = end - victim.crash_time

    return {"coordinator": coordinator, "results": results, "complete": complete,
            "compute_seconds": end - start, "stats": stats, "failed_workers": failed,
            "detection_delay": detection_delay, "recovery_time": recovery_time}


# --------------------------------------------------------------------------
# Experiment context: baseline lookup, worker processes, logging
# --------------------------------------------------------------------------

class Context:
    def __init__(self, images_dir, baseline_csv, out_csv, log_run=None):
        self.images_dir = images_dir
        self.baseline_csv = baseline_csv
        self.out_csv = out_csv
        self.log_run = log_run
        self._task_objects = {}
        self._workloads = {}
        self._baselines = {}
        self._pools = {}
        self._temp_dirs = []

    # -- baseline task objects and workloads (same ones baseline_benchmark.py timed) --
    def task_object(self, task_type):
        if task_type not in self._task_objects:
            if task_type == "resize":
                if not self.images_dir:
                    sys.exit("error: --images-dir is required for the resize task")
                self._task_objects[task_type] = ResizeTask(self.images_dir)
            else:
                self._task_objects[task_type] = HashTask() if task_type == "hash" else MatmulTask()
        return self._task_objects[task_type]

    def available_images(self):
        return self.task_object("resize").available()

    def workload(self, task_type, size):
        key = (task_type, size)
        if key not in self._workloads:
            self._workloads[key] = self.task_object(task_type).prepare(size)
        return self._workloads[key]

    def baseline(self, task_type, size, measure_repeats=3):
        """(mean seconds, digest, source). From baseline_summary.csv if present, else measured now."""
        key = (task_type, size)
        if key in self._baselines:
            return self._baselines[key]

        found = None
        if os.path.isfile(self.baseline_csv):
            with open(self.baseline_csv, newline="") as f:
                for row in csv.DictReader(f):
                    if row["Task_Type"] == task_type and int(row["Task_Size"]) == int(size):
                        found = (float(row["Mean_Time"]), row["Result_Digest"], "baseline_summary.csv")
        if found is None:
            print("  (no baseline row for %s size %d - timing it now, %d runs)" % (task_type, size, measure_repeats))
            task, workload = self.task_object(task_type), self.workload(task_type, size)
            runs = [time_once(task, workload) for _ in range(measure_repeats)]
            found = (statistics.mean(t for t, _ in runs), runs[-1][1], "measured now")
        self._baselines[key] = found
        return found

    # -- one worker process per simulated laptop --
    def get_pool(self, index):
        if index not in self._pools:
            pool = ProcessPoolExecutor(max_workers=1, mp_context=multiprocessing.get_context("spawn"))
            pool.submit(os.getpid).result()                  # start the process now, not during timing
            self._pools[index] = pool
        return self._pools[index]

    def discard_pool(self, index):
        pool = self._pools.pop(index, None)
        if pool:
            pool.shutdown(wait=False, cancel_futures=True)

    def temp_dir(self):
        path = tempfile.mkdtemp(prefix="edgeshare_exp_")
        self._temp_dirs.append(path)
        return path

    def close(self):
        for pool in self._pools.values():
            pool.shutdown(wait=False, cancel_futures=True)
        for path in self._temp_dirs:
            shutil.rmtree(path, ignore_errors=True)

    # -- logging --
    def log(self, row):
        folder = os.path.dirname(self.out_csv)
        if folder:
            os.makedirs(folder, exist_ok=True)
        new_file = not os.path.exists(self.out_csv)
        with open(self.out_csv, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            if new_file:
                writer.writeheader()
            writer.writerow(row)
        if self.log_run:
            self.log_run(adapt_row_for_log_run(row))


# --------------------------------------------------------------------------
# One run
# --------------------------------------------------------------------------

def run_once(ctx, TaskCoordinator, cfg, repeat, variant="normal", fault_fraction=None,
             control_total=None, timeout=300.0):
    task_type, size = cfg["task"], cfg["size"]
    base_time, base_digest, base_source = ctx.baseline(task_type, size)
    tasks = build_tasks(task_type, ctx.workload(task_type, size), CHUNK[task_type])

    begin = time.perf_counter()
    outcome = run_distributed_sim(ctx, TaskCoordinator, tasks, cfg, fault_fraction, timeout)

    merge_start = time.perf_counter()
    digest, correct, notes = None, "INCOMPLETE", ""
    if outcome["complete"]:
        output_dir = ctx.temp_dir()
        digest = merge_results(task_type, tasks, outcome["results"], output_dir)
        correct = cc.check_digest(task_type, size, digest, baseline_digest=base_digest)["status"]
        shutil.rmtree(output_dir, ignore_errors=True)
    else:
        notes = "only %d of %d tasks finished" % (len(outcome["results"]), len(tasks))
    merge_time = time.perf_counter() - merge_start
    total = time.perf_counter() - begin

    stats = outcome["stats"]
    speedup = base_time / total if total > 0 else 0.0
    row = {
        "Experiment_ID": cfg["exp"], "Run_ID": uuid.uuid4().hex[:8],
        "Timestamp": datetime.now().isoformat(timespec="seconds"),
        "Task_Type": task_type, "Task_Size": size, "Strategy": cfg["strategy"],
        "Workers": cfg["workers"], "Repeat": repeat, "Variant": variant,
        "Baseline_Time": round(base_time, 4), "Baseline_Source": base_source,
        "Total_Time": round(total, 4), "Speedup": round(speedup, 3),
        "Efficiency": round(speedup / cfg["workers"], 3),
        "Throughput": round(size / total, 3), "Throughput_Unit": UNITS[task_type],
        "Transfer_Time": round(sum(s["transfer"] for s in stats), 4),
        "Processing_Time": round(sum(s["processing"] for s in stats), 4),
        "Merge_Time": round(merge_time, 4), "Tasks": len(tasks), "Correct": correct,
        "Completed": outcome["complete"], "Failed_Workers": ";".join(outcome["failed_workers"]),
        "Detection_Delay": "" if outcome["detection_delay"] is None else round(outcome["detection_delay"], 3),
        "Recovery_Time": "" if outcome["recovery_time"] is None else round(outcome["recovery_time"], 3),
        "Overhead_vs_Control": "" if control_total is None else round(total - control_total, 4),
        "Notes": notes + (" loaded-worker" if cfg.get("loaded") else ""),
    }
    ctx.log(row)
    print("  [%s %s w=%d %s rep %d] %.2fs  speedup %.2fx  eff %.0f%%  correct=%s%s" % (
        task_type, size, cfg["workers"], cfg["strategy"], repeat, total, speedup,
        100 * row["Efficiency"], correct, "  (%s)" % variant if variant != "normal" else ""))
    return row


# --------------------------------------------------------------------------
# Experiment plans
# --------------------------------------------------------------------------

def make_plan(exp_id, task_types, sizes_table, available_images, max_workers, strategies,
              default_strategy, size_override=None):
    """Turn an experiment id into a list of run configurations. EDIT to match your project plan."""
    plan = []
    for task in task_types:
        sizes = [s for s in sizes_table[task] if task != "resize" or s <= available_images]
        if not sizes:
            sizes = [available_images]
        pick = lambda i: size_override or sizes[min(i, len(sizes) - 1)]
        small, medium, large = pick(0), pick(1), pick(2)

        def cfg(size, workers, strategy=default_strategy, **extra):
            return dict(exp=exp_id, task=task, size=size, workers=workers, strategy=strategy, **extra)

        if exp_id == "E1":
            plan.append(cfg(medium, 1))
        elif exp_id == "E2":
            plan += [cfg(large, w) for w in range(1, max_workers + 1)]
        elif exp_id == "E3":
            plan += [cfg(s, max_workers) for s in ([size_override] if size_override else sizes)]
        elif exp_id == "E4":
            plan.append(cfg(large, max_workers))
        elif exp_id == "E5":
            plan += [cfg(large, max_workers, strategy=s) for s in strategies]
        elif exp_id == "E6":
            plan += [cfg(large, max_workers, strategy=s, loaded=True) for s in strategies]
        else:
            raise ValueError("unknown experiment: " + exp_id)
    return plan


def run_experiment(ctx, TaskCoordinator, exp_id, task_types, repeats, quick, max_workers,
                   size_override=None, kill_at=0.4, timeout=300.0):
    if exp_id == "E4" and max_workers < 2:
        sys.exit("error: E4 needs at least 2 workers (one dies, the others must finish the job)")

    module = sys.modules[TaskCoordinator.__module__]
    strategies = list(getattr(module, "STRATEGIES", {"resource_weighted": None}))
    default = "resource_weighted" if "resource_weighted" in strategies else strategies[0]
    available = ctx.available_images() if "resize" in task_types else 0
    plan = make_plan(exp_id, task_types, QUICK_SIZES if quick else SIZES, available,
                     max_workers, strategies, default, size_override)

    print("\n=== %s: %s ===" % (exp_id, EXPERIMENT_INFO[exp_id]))
    rows = []
    for cfg in plan:
        print("\n%s size=%s workers=%d strategy=%s%s" % (
            cfg["task"], cfg["size"], cfg["workers"], cfg["strategy"], " (one worker loaded)" if cfg.get("loaded") else ""))
        for repeat in range(1, repeats + 1):
            if exp_id == "E4":
                control = run_once(ctx, TaskCoordinator, cfg, repeat, "control", None, None, timeout)
                rows.append(control)
                rows.append(run_once(ctx, TaskCoordinator, cfg, repeat, "fault", kill_at,
                                     control["Total_Time"], timeout))
            else:
                rows.append(run_once(ctx, TaskCoordinator, cfg, repeat, "normal", None, None, timeout))
    return rows


def print_summary(rows):
    groups = {}
    for row in rows:
        key = (row["Experiment_ID"], row["Task_Type"], row["Task_Size"], row["Workers"], row["Strategy"], row["Variant"])
        groups.setdefault(key, []).append(row)

    print("\n%-4s %-7s %6s %4s %-18s %-8s %6s %18s %9s" % (
        "exp", "task", "size", "wrk", "strategy", "variant", "runs", "speedup (mean±std)", "correct"))
    for (exp, task, size, workers, strategy, variant), group in groups.items():
        speedups = [r["Speedup"] for r in group]
        spread = statistics.stdev(speedups) if len(speedups) > 1 else 0.0
        passed = sum(1 for r in group if r["Correct"] == "PASS")
        print("%-4s %-7s %6s %4d %-18s %-8s %6d %11.2f ± %-5.2f %6d/%d" % (
            exp, task, size, workers, strategy, variant, len(group), statistics.mean(speedups), spread,
            passed, len(group)))


# --------------------------------------------------------------------------
# Command line
# --------------------------------------------------------------------------

def main(argv=None):
    parser = argparse.ArgumentParser(description="EdgeShare experiment runner (E1-E6)")
    parser.add_argument("experiment", nargs="?", help="E1..E6 or 'all'")
    parser.add_argument("--list", action="store_true", help="describe the experiments and exit")
    parser.add_argument("--tasks", nargs="+", choices=["resize", "hash", "matmul"], default=["resize", "hash", "matmul"])
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--workers", type=int, default=3, help="max number of workers (default 3)")
    parser.add_argument("--size", type=int, default=None, help="use this one job size instead of the defaults")
    parser.add_argument("--kill-at", type=float, default=0.4, help="E4: fraction of progress when a worker dies")
    parser.add_argument("--images-dir", default=None, help="folder of test images (needed for resize)")
    parser.add_argument("--out", default="results/results.csv")
    parser.add_argument("--baseline-csv", default=cc.DEFAULT_BASELINE_CSV)
    parser.add_argument("--coordinator-module", default=None, help="e.g. compute.coordinator")
    parser.add_argument("--log-run", default="benchmark.run_experiments:log_run", metavar="MODULE:FUNCTION",
                        help="also call the team's log_run() (writes config.RESULTS_CSV); 'none' to skip")
    parser.add_argument("--timeout", type=float, default=300.0, help="give up on a run after this many seconds")
    parser.add_argument("--quick", action="store_true", help="tiny sizes and 2 repeats (smoke test)")
    args = parser.parse_args(argv)

    if args.list or not args.experiment:
        for exp_id, text in EXPERIMENT_INFO.items():
            print("%s  %s" % (exp_id, text))
        return []

    experiments = list(EXPERIMENT_INFO) if args.experiment.lower() == "all" else [args.experiment.upper()]
    for exp_id in experiments:
        if exp_id not in EXPERIMENT_INFO:
            sys.exit("error: unknown experiment %s (use --list)" % exp_id)

    if "resize" in args.tasks and not args.images_dir:
        sys.exit("error: --images-dir is required for the resize task (or use --tasks hash matmul)")

    TaskCoordinator = load_task_coordinator(args.coordinator_module)
    log_run = load_log_run(args.log_run) if args.log_run and args.log_run.lower() != "none" else None
    repeats = 2 if args.quick else args.repeats

    ctx = Context(args.images_dir, args.baseline_csv, args.out, log_run)
    all_rows = []
    try:
        for exp_id in experiments:
            all_rows += run_experiment(ctx, TaskCoordinator, exp_id, args.tasks, repeats, args.quick,
                                       args.workers, args.size, args.kill_at, args.timeout)
    finally:
        ctx.close()

    print_summary(all_rows)
    print("\nSaved %d rows to %s" % (len(all_rows), args.out))
    return all_rows


if __name__ == "__main__":
    main()
