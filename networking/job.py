"""Run a whole job on the workers: split it, submit every task, merge the answers.

    python main.py job montecarlo                       # estimate pi with 20 M darts
    python main.py job primes --limit 50000000          # count primes below 50 M
    python main.py job matmul --n 300                   # C = A x B, rows shared out
    python main.py job image_filter --image photo.jpg --filter edges
    python main.py job wordcount --text book.txt
    python main.py job all                              # every job, one after another

Options: --host <coordinator-ip>  --port 5000  --chunks 40  --no-verify

With verification (the default) the same tasks are also run on this laptop alone. That
checks the distributed answer is identical, and gives the real single-laptop time, so the
speedup printed at the end is measured, not estimated.
"""
import argparse
import socket
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import config
from compute import jobs
from compute.tasks import run_task
from networking.protocol import recv_msg, send_msg, sha256_hex, verify_payload

PARALLEL = 16             # tasks in flight at once (the coordinator queues the rest)
TASK_TIMEOUT_S = 600


def submit_task(task, host="127.0.0.1", port=config.COORDINATOR_PORT, timeout=TASK_TIMEOUT_S):
    """Send one task and wait for its result. Returns a report dict; never raises."""
    report = {"task_id": task["task_id"], "ok": False, "result": None, "worker_id": None,
              "processing_s": None, "error": None}
    payload = task.get("payload", b"")
    try:
        with socket.create_connection((host, port), timeout=10) as sock:
            sock.settimeout(timeout)
            send_msg(sock, {"type": "SUBMIT", "task_id": task["task_id"],
                            "task_type": task["task_type"], "params": task.get("params", {}),
                            "name": task.get("name"), "sha256": sha256_hex(payload)}, payload)
            header, _ = recv_msg(sock)
            if header.get("type") != "ACK":
                raise RuntimeError(header.get("reason", "submission rejected"))
            header, result = recv_msg(sock)
            if header.get("type") != "RESULT":
                raise RuntimeError(header.get("reason", "task failed"))
            if not verify_payload(header, result):
                raise RuntimeError("result corrupted in transfer")
        report.update(ok=True, result=result, worker_id=header.get("worker_id"),
                      processing_s=header.get("processing_s"))
    except (OSError, ValueError, RuntimeError) as exc:
        report["error"] = "%s: %s" % (type(exc).__name__, exc)
    return report


def run_local(tasks):
    """Run every task on this laptop, one after another. Returns (results, seconds)."""
    start = time.perf_counter()
    results = {t["task_id"]: run_task(t["task_type"], t.get("params", {}), t.get("payload", b""))[0]
               for t in tasks}
    return results, time.perf_counter() - start


def run_job(job_type, host="127.0.0.1", port=config.COORDINATOR_PORT, verify=True,
            parallel=PARALLEL, on_progress=None, **options):
    """Split, run on the workers, merge, and (optionally) verify against one laptop.
    on_progress(done, total) is called after each task. Returns a summary dict."""
    tasks = jobs.split(job_type, **options)
    start = time.perf_counter()
    reports, done = {}, 0
    with ThreadPoolExecutor(max_workers=max(1, parallel)) as pool:
        futures = [pool.submit(submit_task, t, host, port) for t in tasks]
        for future in as_completed(futures):
            report = future.result()
            reports[report["task_id"]] = report
            done += 1
            if on_progress:
                on_progress(done, len(tasks))
    wall_s = time.perf_counter() - start

    failed = [r for r in reports.values() if not r["ok"]]
    by_worker = {}
    for r in reports.values():
        if r["ok"]:
            by_worker[r["worker_id"]] = by_worker.get(r["worker_id"], 0) + 1
    summary = {"job": job_type, "tasks": len(tasks), "failed": len(failed),
               "wall_s": round(wall_s, 3),
               "compute_s": round(sum(r["processing_s"] or 0 for r in reports.values()), 3),
               "by_worker": by_worker, "answer": None, "output": None,
               "errors": [r["error"] for r in failed][:5]}
    if failed:
        return summary

    answer, output = jobs.merge(job_type, tasks, {t: r["result"] for t, r in reports.items()})
    summary.update(answer=answer, output=output)
    if verify:
        local_results, local_s = run_local(tasks)
        local_answer, _ = jobs.merge(job_type, tasks, local_results)
        summary.update(single_laptop_s=round(local_s, 3), correct=local_answer == answer,
                       speedup=round(local_s / wall_s, 2) if wall_s else None)
    return summary


def print_summary(s):
    print("\n=== %s: %d tasks in %.2f s ===" % (s["job"], s["tasks"], s["wall_s"]))
    if s["failed"]:
        print("FAILED: %d task(s): %s" % (s["failed"], "; ".join(s["errors"])))
        return
    for key, value in s["answer"].items():
        print("  %-14s %s" % (key, value))
    print("  %-14s %s" % ("tasks/worker", ", ".join("%s=%d" % kv for kv in sorted(s["by_worker"].items()))))
    if "speedup" in s:
        print("  %-14s %.2f s   distributed: %.2f s   speedup: %.2fx   correct: %s"
              % ("one laptop", s["single_laptop_s"], s["wall_s"], s["speedup"],
                 "YES" if s["correct"] else "NO"))


def main(argv=None):
    parser = argparse.ArgumentParser(description="Run a distributed job on the EdgeShare workers.")
    parser.add_argument("job", choices=jobs.JOB_TYPES + ("all",))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=config.COORDINATOR_PORT)
    parser.add_argument("--chunks", type=int, help="number of tasks to split into")
    parser.add_argument("--samples", type=int, help="montecarlo: total darts")
    parser.add_argument("--limit", type=int, help="primes: count primes below this")
    parser.add_argument("--n", type=int, help="matmul: matrix size n x n")
    parser.add_argument("--image", help="image_filter: image file (default: generated)")
    parser.add_argument("--filter", choices=("blur", "edges", "sharpen", "emboss"))
    parser.add_argument("--text", help="wordcount: text file (default: generated)")
    parser.add_argument("--out", default=config.OUTPUT_DIR)
    parser.add_argument("--no-verify", action="store_true", help="skip the single-laptop check")
    args = parser.parse_args(argv)

    options = {"chunks": args.chunks, "samples": args.samples, "limit": args.limit, "n": args.n,
               "filter": args.filter,
               "image": Path(args.image).read_bytes() if args.image else None,
               "text": Path(args.text).read_bytes() if args.text else None}
    ok = True
    for job_type in (jobs.JOB_TYPES if args.job == "all" else (args.job,)):
        print("Running %s on %s:%d ..." % (job_type, args.host, args.port))
        s = run_job(job_type, args.host, args.port, verify=not args.no_verify,
                    on_progress=lambda d, t: print("\r  %d/%d tasks done" % (d, t), end="", flush=True),
                    **options)
        print_summary(s)
        if s["output"]:
            out = Path(args.out) / ("%s_%s.png" % (job_type, s["answer"]["filter"]))
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(s["output"])
            print("  %-14s %s" % ("saved", out.resolve()))
        ok = ok and not s["failed"] and s.get("correct", True)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
