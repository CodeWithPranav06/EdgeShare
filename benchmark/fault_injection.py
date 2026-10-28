"""
fault_injection.py - disconnect a worker in the middle of a run (experiment E4).

E4 asks: "if a worker dies mid-run, do we lose any task?"  The injector watches the job's
progress and, when it reaches a chosen fraction (default 40 %), triggers a fault action:

    FaultInjector(fraction=0.4, action=worker.crash)       # simulated worker (thread)
    FaultInjector(fraction=0.4, action=lambda: kill_process(proc))   # worker is a local process
    FaultInjector(fraction=0.4, action=lambda: manual_disconnect("W3"))   # real laptops

Real laptops: use manual_disconnect, then physically switch off that laptop's Wi-Fi
or press Ctrl+C in its worker window when the banner appears.

Run E4 from the command line (this just calls experiment_runner):

    python fault_injection.py --images-dir data/images --workers 3 --kill-at 0.4 --repeats 5
"""

import argparse
import sys
import time


class FaultInjector:
    """Fires `action()` exactly once, when done/total first reaches `fraction`."""

    def __init__(self, fraction, action, description="fault"):
        if not 0 < fraction < 1:
            raise ValueError("fraction must be between 0 and 1 (e.g. 0.4)")
        self.fraction = fraction
        self.action = action
        self.description = description
        self.fired = False
        self.fired_at = None            # time.perf_counter() when the fault was triggered
        self.progress_at_fire = None    # fraction of the job done at that moment

    def poll(self, done, total):
        """Call this regularly with the number of finished tasks. True when it fires."""
        if self.fired or total <= 0:
            return False
        if done / total >= self.fraction:
            self.fired = True
            self.fired_at = time.perf_counter()
            self.progress_at_fire = done / total
            print("  >>> FAULT INJECTED at %.0f%% progress: %s" % (100 * self.progress_at_fire, self.description))
            self.action()
            return True
        return False


def kill_process(process):
    """Stop a worker that runs as a local subprocess.Popen / multiprocessing.Process."""
    process.terminate()


def manual_disconnect(worker_id):
    """For real laptops: tell the person doing the demo to pull the plug on a worker."""
    banner = "=" * 60
    print("\n%s\n  DISCONNECT WORKER %s NOW\n  (turn off its Wi-Fi or press Ctrl+C in its window)\n%s\n"
          % (banner, worker_id, banner))


def main(argv=None):
    parser = argparse.ArgumentParser(description="Run experiment E4 (worker failure mid-run)")
    parser.add_argument("--images-dir", default=None, help="folder of test images (resize task)")
    parser.add_argument("--task", choices=["resize", "hash", "matmul"], default="resize")
    parser.add_argument("--size", type=int, default=None, help="job size (default: the 'large' size)")
    parser.add_argument("--workers", type=int, default=3, help="number of workers (at least 2)")
    parser.add_argument("--kill-at", type=float, default=0.4, help="fraction of progress when the worker dies")
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--quick", action="store_true")
    args, extra = parser.parse_known_args(argv)

    import experiment_runner                     # imported here to avoid a circular import
    runner_args = ["E4", "--tasks", args.task, "--workers", str(args.workers),
                   "--kill-at", str(args.kill_at), "--repeats", str(args.repeats)]
    if args.images_dir:
        runner_args += ["--images-dir", args.images_dir]
    if args.size:
        runner_args += ["--size", str(args.size)]
    if args.quick:
        runner_args += ["--quick"]
    experiment_runner.main(runner_args + extra)


if __name__ == "__main__":
    main(sys.argv[1:])
