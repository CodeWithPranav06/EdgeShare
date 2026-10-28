"""
baseline_benchmark.py - time the SINGLE-LAPTOP baseline for resize, hash and matmul.

This produces T_single for the speedup formula   Speedup = T_single / T_distributed.
Everything runs on this one machine: no network, no splitting.

Run (from the repo root, with image_processor.py next to this file):

    python baseline_benchmark.py --images-dir data/images

Options:
    --tasks resize hash matmul     which tasks to time (default: all three)
    --repeats 5                    timed runs per size (default 5)
    --out results/baseline.csv     one row per run; a *_summary.csv with mean/std is written too
    --quick                        tiny sizes and 2 repeats, just to check everything works
    --no-warmup                    skip the untimed warm-up run

What each task measures (sizes are in the units shown):
    resize  : N images -> 512x512 JPEG, saved to a temp folder (load + resize + save)
    hash    : N MB of data, SHA-256 of every 1 MB block, then one final hash
    matmul  : N x N integer matrices multiplied in plain Python (N rows of work)

IMPORTANT: the distributed runs must use the SAME task code and the SAME sizes,
otherwise the speedup is meaningless.
"""

import argparse
import csv
import hashlib
import os
import platform
import random
import shutil
import statistics
import sys
import tempfile
import time
import uuid
from datetime import datetime

import PIL
import psutil

import image_processor as ip

DEFAULT_SIZES = {"resize": [50, 100, 200, 500], "hash": [64, 128, 256, 512], "matmul": [100, 200, 300, 400]}
QUICK_SIZES = {"resize": [10, 20], "hash": [16, 32], "matmul": [50, 100]}
UNITS = {"resize": "images", "hash": "MB", "matmul": "rows"}
BLOCK_BYTES = 1024 * 1024

CSV_FIELDS = ["Experiment_ID", "Timestamp", "Task_Type", "Task_Size", "Unit", "Repeat",
              "Total_Time", "Throughput", "Result_Digest", "Machine", "CPU_Cores", "Python", "Pillow"]


# --------------------------------------------------------------------------
# The three tasks. Each has prepare(size) (not timed) and a run function (timed)
# that returns a digest of its result, so results can be compared later.
# --------------------------------------------------------------------------

class ResizeTask:
    def __init__(self, images_dir):
        self.images_dir = images_dir
        self.all_paths = ip.load_image_paths(images_dir)

    def available(self):
        return len(self.all_paths)

    def prepare(self, size):
        return self.all_paths[:size]

    def run(self, paths):
        output_dir = tempfile.mkdtemp(prefix="edgeshare_resize_")
        try:
            result = ip.process_batch(paths, output_dir, size=512)
            if result["failed"]:
                raise RuntimeError("%d image(s) failed, e.g. %s" % (len(result["failed"]), result["failed"][0]))
            return ip.hash_files(result["outputs"])
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)


class HashTask:
    def available(self):
        return None

    def prepare(self, size_mb):
        rng = random.Random(7)
        return [rng.randbytes(BLOCK_BYTES) for _ in range(size_mb)]

    def run(self, blocks):
        final = hashlib.sha256()
        for block in blocks:
            final.update(hashlib.sha256(block).digest())
        return final.hexdigest()


class MatmulTask:
    def available(self):
        return None

    def prepare(self, n):
        rng = random.Random(7)
        a = [[rng.randint(0, 9) for _ in range(n)] for _ in range(n)]
        b = [[rng.randint(0, 9) for _ in range(n)] for _ in range(n)]
        return a, b

    def run(self, matrices):
        a, b = matrices
        columns = list(zip(*b))
        digest = hashlib.sha256()
        for row in a:
            product = [sum(x * y for x, y in zip(row, column)) for column in columns]
            digest.update(repr(product).encode())
        return digest.hexdigest()[:32]


# --------------------------------------------------------------------------
# Benchmark loop
# --------------------------------------------------------------------------

def preflight_warnings():
    """Things that make timings unreliable."""
    warnings = []
    battery = psutil.sensors_battery() if hasattr(psutil, "sensors_battery") else None
    if battery is not None and not battery.power_plugged:
        warnings.append("laptop is on battery - plug in the charger (power saving slows the CPU)")
    load = psutil.cpu_percent(interval=1.0)
    if load > 20:
        warnings.append("CPU is %.0f%% busy - close browsers, videos and other apps first" % load)
    return warnings


def time_once(task, workload):
    start = time.perf_counter()
    digest = task.run(workload)
    return time.perf_counter() - start, digest


def run_benchmark(task_names, sizes, repeats, images_dir, warmup=True):
    tasks = {}
    if "resize" in task_names:
        tasks["resize"] = ResizeTask(images_dir)
    if "hash" in task_names:
        tasks["hash"] = HashTask()
    if "matmul" in task_names:
        tasks["matmul"] = MatmulTask()

    machine = "%s (%s)" % (platform.node(), platform.system())
    rows = []
    for name, task in tasks.items():
        task_sizes = list(sizes[name])
        if name == "resize":
            cap = task.available()
            usable = [s for s in task_sizes if s <= cap]
            if cap not in usable and cap > (usable[-1] if usable else 0):
                usable.append(cap)              # also time the whole folder
            if not usable:
                sys.exit("error: only %d image(s) in the folder, need at least %d" % (cap, task_sizes[0]))
            task_sizes = usable

        for size in task_sizes:
            workload = task.prepare(size)
            if warmup:
                time_once(task, workload)
            print("\n[%s] size=%d %s" % (name, size, UNITS[name]))
            for repeat in range(1, repeats + 1):
                seconds, digest = time_once(task, workload)
                print("  run %d/%d: %.3fs" % (repeat, repeats, seconds))
                rows.append({
                    "Experiment_ID": uuid.uuid4().hex[:8],
                    "Timestamp": datetime.now().isoformat(timespec="seconds"),
                    "Task_Type": name, "Task_Size": size, "Unit": UNITS[name], "Repeat": repeat,
                    "Total_Time": round(seconds, 4), "Throughput": round(size / seconds, 3),
                    "Result_Digest": digest, "Machine": machine, "CPU_Cores": psutil.cpu_count(),
                    "Python": platform.python_version(), "Pillow": PIL.__version__,
                })
    return rows


def summarize(rows):
    """One summary row per (task, size): mean, std, min, max, throughput, determinism."""
    groups = {}
    for row in rows:
        groups.setdefault((row["Task_Type"], row["Task_Size"]), []).append(row)

    summary = []
    for (task, size), group in groups.items():
        times = [r["Total_Time"] for r in group]
        summary.append({
            "Task_Type": task, "Task_Size": size, "Unit": group[0]["Unit"], "Runs": len(times),
            "Mean_Time": round(statistics.mean(times), 4),
            "Std_Time": round(statistics.stdev(times), 4) if len(times) > 1 else 0.0,
            "Min_Time": min(times), "Max_Time": max(times),
            "Mean_Throughput": round(statistics.mean(r["Throughput"] for r in group), 3),
            "Deterministic": len({r["Result_Digest"] for r in group}) == 1,
            "Result_Digest": group[0]["Result_Digest"],
        })
    return summary


def write_csv(path, fieldnames, rows):
    folder = os.path.dirname(path)
    if folder:
        os.makedirs(folder, exist_ok=True)
    new_file = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if new_file:
            writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description="Single-laptop baseline timing for EdgeShare")
    parser.add_argument("--images-dir", default=None, help="folder of test images (needed for resize)")
    parser.add_argument("--tasks", nargs="+", choices=["resize", "hash", "matmul"],
                        default=["resize", "hash", "matmul"])
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--out", default="results/baseline.csv")
    parser.add_argument("--quick", action="store_true", help="tiny sizes, 2 repeats (smoke test)")
    parser.add_argument("--no-warmup", action="store_true")
    args = parser.parse_args()

    if "resize" in args.tasks and not args.images_dir:
        sys.exit("error: --images-dir is required for the resize task")

    sizes = QUICK_SIZES if args.quick else DEFAULT_SIZES
    repeats = 2 if args.quick else args.repeats

    print("Machine: %s, %d cores, Python %s, Pillow %s" % (
        platform.node(), psutil.cpu_count(), platform.python_version(), PIL.__version__))
    for warning in preflight_warnings():
        print("WARNING:", warning)

    rows = run_benchmark(args.tasks, sizes, repeats, args.images_dir, warmup=not args.no_warmup)
    summary = summarize(rows)

    summary_path = args.out.rsplit(".", 1)[0] + "_summary.csv"
    write_csv(args.out, CSV_FIELDS, rows)
    write_csv(summary_path, list(summary[0].keys()), summary)

    print("\n%-8s %8s %6s %10s %9s %10s %s" % ("task", "size", "runs", "mean (s)", "std (s)", "per sec", "notes"))
    for s in summary:
        note = "" if s["Deterministic"] else "RESULT CHANGED BETWEEN RUNS!"
        print("%-8s %8d %6d %10.3f %9.3f %10.2f %s" % (
            s["Task_Type"], s["Task_Size"], s["Runs"], s["Mean_Time"], s["Std_Time"], s["Mean_Throughput"], note))
    print("\nSaved: %s (every run) and %s (mean/std per size)" % (args.out, summary_path))


if __name__ == "__main__":
    main()
