"""Run log (member C). The experiments themselves are in benchmark/experiment_runner.py,
which calls log_run() for every run (python -m benchmark.experiment_runner --list)."""
import csv
import os

import config

COLUMNS = ["experiment", "task_type", "task_size", "nodes", "scheduler",
           "transfer_s", "processing_s", "merge_s", "total_s", "throughput",
           "speedup", "efficiency", "failures", "recovery_s", "hash_ok"]


def log_run(row):
    """Interface: append one run (dict with keys in COLUMNS) to results.csv."""
    new = not os.path.exists(config.RESULTS_CSV)
    os.makedirs(os.path.dirname(config.RESULTS_CSV), exist_ok=True)
    with open(config.RESULTS_CSV, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        if new:
            w.writeheader()
        w.writerow({c: row.get(c, "") for c in COLUMNS})
