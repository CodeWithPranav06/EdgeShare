"""
correctness_check.py - prove the distributed output equals the single-laptop baseline.

Idea: every task reduces its full result to ONE SHA-256 digest. The baseline run stores
its digest in results/baseline_summary.csv. After a distributed run, compute the digest
of the merged output the same way and compare. Equal digests = identical results.

Use it from the experiment runner:

    from correctness_check import check_output_folder, check_digest

    # resize: outputs were saved to a folder by the coordinator
    result = check_output_folder("resize", size=100, output_dir="out/run1")

    # hash: workers returned one SHA-256 per 1 MB block (in block order)
    result = check_digest("hash", 64, digest_hash_blocks(block_digests))

    # matmul: merged result rows (list of lists of ints, in row order)
    result = check_digest("matmul", 200, digest_matmul_rows(rows))

    print(format_result(result))          # PASS / FAIL / NO_BASELINE
    log_value = result["status"]          # put this in a "Correct" column of results.csv

From the command line:

    python correctness_check.py --task resize --size 100 --distributed-dir out/run1
    python correctness_check.py --task hash --size 64 --digest <hex digest>
    python correctness_check.py --compare-dirs out/baseline out/run1      # which files differ?

IMPORTANT for the digest to match:
  * resize : the distributed run must process the SAME first N images (sorted by name),
             and every laptop must have the SAME Pillow version.
  * hash   : blocks are 1 MB, generated from the same seed (see baseline_benchmark.py).
  * matmul : same matrices (seed 7) and the same row order.
"""

import argparse
import csv
import hashlib
import sys
from pathlib import Path

import image_processor as ip

DEFAULT_BASELINE_CSV = "results/baseline_summary.csv"

PASS = "PASS"
FAIL = "FAIL"
NO_BASELINE = "NO_BASELINE"


# --------------------------------------------------------------------------
# Digest functions (the baseline and the distributed run both use these)
# --------------------------------------------------------------------------

def digest_resize_outputs(paths):
    """Digest of a set of resized image files (names + contents, order-independent)."""
    return ip.hash_files(paths)


def digest_hash_blocks(block_digests):
    """Digest for the hash task, from the SHA-256 of each 1 MB block in block order.

    block_digests: list of 32-byte values or 64-character hex strings.
    """
    final = hashlib.sha256()
    for value in block_digests:
        final.update(bytes.fromhex(value) if isinstance(value, str) else bytes(value))
    return final.hexdigest()


def digest_matmul_rows(rows):
    """Digest for the matmul task, from the result rows (lists of ints) in row order."""
    digest = hashlib.sha256()
    for row in rows:
        digest.update(repr([int(x) for x in row]).encode())
    return digest.hexdigest()[:32]


# --------------------------------------------------------------------------
# Looking up the baseline and comparing
# --------------------------------------------------------------------------

def load_baseline_digest(task, size, baseline_csv=DEFAULT_BASELINE_CSV):
    """Find the baseline digest for (task, size). Returns (digest or None, note).

    If the CSV has several rows for the same task and size (you ran the baseline more
    than once) the most recent row is used, and the note says if they disagreed.
    """
    path = Path(baseline_csv)
    if not path.is_file():
        return None, "baseline file not found: %s (run baseline_benchmark.py first)" % baseline_csv

    found = []
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            if row["Task_Type"] == task and int(row["Task_Size"]) == int(size):
                found.append(row["Result_Digest"])
    if not found:
        return None, "no baseline row for task=%s size=%s in %s" % (task, size, baseline_csv)

    note = ""
    if len(set(found)) > 1:
        note = "baseline CSV has %d different digests for this size; using the latest" % len(set(found))
    return found[-1], note


def check_digest(task, size, distributed_digest, baseline_csv=DEFAULT_BASELINE_CSV, baseline_digest=None):
    """Compare a distributed digest with the baseline.

    Returns a dict: status (PASS / FAIL / NO_BASELINE), task, size, baseline_digest,
    distributed_digest, match (True / False / None) and message.
    """
    note = ""
    if baseline_digest is None:
        baseline_digest, note = load_baseline_digest(task, size, baseline_csv)

    result = {"task": task, "size": int(size), "baseline_digest": baseline_digest,
              "distributed_digest": distributed_digest}
    if baseline_digest is None:
        result.update(status=NO_BASELINE, match=None, message=note)
    elif baseline_digest == distributed_digest:
        result.update(status=PASS, match=True, message=note or "distributed output is identical to the baseline")
    else:
        result.update(status=FAIL, match=False,
                      message="distributed output DIFFERS from the baseline" + (" (%s)" % note if note else ""))
    return result


def check_output_folder(task, size, output_dir, baseline_csv=DEFAULT_BASELINE_CSV, baseline_digest=None):
    """Digest every file in a folder of distributed outputs (resize) and compare."""
    if task != "resize":
        raise ValueError("folder checks are only for the resize task")
    paths = [p for p in Path(output_dir).iterdir() if p.is_file() and not p.name.endswith(".tmp")]
    if not paths:
        return {"task": task, "size": int(size), "baseline_digest": baseline_digest,
                "distributed_digest": None, "status": FAIL, "match": False,
                "message": "no output files found in %s" % output_dir}
    result = check_digest(task, size, digest_resize_outputs(paths), baseline_csv, baseline_digest)
    result["files"] = len(paths)
    if result["status"] == FAIL and len(paths) != int(size):
        result["message"] += " - found %d output files, expected %d" % (len(paths), int(size))
    return result


def compare_folders(baseline_dir, distributed_dir):
    """Say exactly which files are missing, extra or different between two output folders."""
    def hashes(folder):
        out = {}
        for p in Path(folder).iterdir():
            if p.is_file():
                out[p.name] = hashlib.sha256(p.read_bytes()).hexdigest()
        return out

    base, dist = hashes(baseline_dir), hashes(distributed_dir)
    return {
        "missing": sorted(set(base) - set(dist)),        # in baseline, not in distributed
        "extra": sorted(set(dist) - set(base)),          # in distributed only
        "different": sorted(n for n in set(base) & set(dist) if base[n] != dist[n]),
        "identical": sum(1 for n in set(base) & set(dist) if base[n] == dist[n]),
    }


def format_result(result):
    """One readable line for logs and the demo."""
    return "[%s] %s size=%s: %s" % (result["status"], result["task"], result["size"], result["message"])


# --------------------------------------------------------------------------
# Command line
# --------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Check distributed output against the baseline")
    parser.add_argument("--task", choices=["resize", "hash", "matmul"])
    parser.add_argument("--size", type=int, help="the same size you ran in the baseline")
    parser.add_argument("--distributed-dir", help="folder of distributed resize outputs")
    parser.add_argument("--digest", help="distributed digest (hash / matmul tasks)")
    parser.add_argument("--baseline-csv", default=DEFAULT_BASELINE_CSV)
    parser.add_argument("--compare-dirs", nargs=2, metavar=("BASELINE_DIR", "DISTRIBUTED_DIR"),
                        help="list which files differ between two output folders")
    args = parser.parse_args()

    if args.compare_dirs:
        diff = compare_folders(*args.compare_dirs)
        print("identical files: %d" % diff["identical"])
        for key in ("missing", "extra", "different"):
            print("%s: %d %s" % (key, len(diff[key]), diff[key][:10]))
        sys.exit(0 if not (diff["missing"] or diff["extra"] or diff["different"]) else 1)

    if not (args.task and args.size):
        parser.error("--task and --size are required (or use --compare-dirs)")
    if args.task == "resize":
        if not args.distributed_dir:
            parser.error("--distributed-dir is required for resize")
        result = check_output_folder(args.task, args.size, args.distributed_dir, args.baseline_csv)
    else:
        if not args.digest:
            parser.error("--digest is required for hash and matmul")
        result = check_digest(args.task, args.size, args.digest, args.baseline_csv)

    print(format_result(result))
    print("  baseline   :", result["baseline_digest"])
    print("  distributed:", result["distributed_digest"])
    sys.exit(0 if result["status"] == PASS else 1)


if __name__ == "__main__":
    main()
