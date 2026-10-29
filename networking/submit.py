"""Submit images to the coordinator for resizing (from resize_remote.py in PR #6).

    python main.py submit <image-or-folder> [--host IP] [--size 512] [--out output]

Each image is one SUBMIT message on its own connection. Several are in flight at once
(--parallel) so every worker stays busy, while only that many images are held in
memory. Every result is checked against its SHA-256, saved into the output folder
(temp file + rename, so a crash never leaves half a file), and reported with the worker
that produced it. Results are returned in the same order as the input images.
"""
import argparse
import os
import socket
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import config
from networking.protocol import recv_msg, send_msg, sha256_hex, verify_payload
from networking.server import SUBMIT_TIMEOUT_S

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".bmp", ".webp", ".tif", ".tiff"}
DEFAULT_SIZE = 512
DEFAULT_PARALLEL = 6


def find_images(path):
    """One image file, or every image in a folder sorted by name."""
    path = Path(path)
    if path.is_file():
        return [path]
    if not path.is_dir():
        raise FileNotFoundError("not found: %s" % path)
    images = sorted(p for p in path.iterdir()
                    if p.is_file() and p.suffix.lower() in IMAGE_EXTENSIONS)
    stems = [p.stem for p in images]
    duplicates = sorted({s for s in stems if stems.count(s) > 1})
    if duplicates:
        raise ValueError("files share a name and would overwrite each other: %s"
                         % ", ".join(duplicates[:5]))
    return images


def output_path(out_dir, image_path):
    return Path(out_dir) / ("%s_resized.png" % Path(image_path).stem)


def save_atomically(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    with open(temp, "wb") as f:
        f.write(data)
    os.replace(temp, path)


def submit_image(image_path, task_id, host="127.0.0.1", port=config.COORDINATOR_PORT,
                 width=DEFAULT_SIZE, height=DEFAULT_SIZE, out_dir=config.OUTPUT_DIR,
                 timeout=SUBMIT_TIMEOUT_S + 30):
    """Send one image and wait for the resized result. Never raises: returns a dict
    with ok, output, worker_id, processing_s, sha256, error."""
    image_path = Path(image_path)
    report = {"image": str(image_path), "task_id": task_id, "ok": False, "output": None,
              "worker_id": None, "processing_s": None, "sha256": None, "error": None}
    try:
        data = image_path.read_bytes()
        if len(data) > config.MAX_PAYLOAD_BYTES:
            raise ValueError("image is larger than %d bytes" % config.MAX_PAYLOAD_BYTES)

        with socket.create_connection((host, port), timeout=10) as sock:
            sock.settimeout(timeout)
            send_msg(sock, {
                "type": "SUBMIT",
                "task_id": task_id,
                "task_type": "resize",
                "params": {"width": width, "height": height},
                "name": image_path.name,
                "sha256": sha256_hex(data),
            }, data)
            del data

            header, _ = recv_msg(sock)
            if header.get("type") != "ACK":
                raise RuntimeError(header.get("reason", "submission rejected"))

            header, result = recv_msg(sock)
            if header.get("type") != "RESULT":
                raise RuntimeError(header.get("reason", "task failed"))
            if not verify_payload(header, result):
                raise RuntimeError("result corrupted in transfer")

        output = output_path(out_dir, image_path)
        save_atomically(output, result)
        report.update(ok=True, output=str(output.resolve()), worker_id=header.get("worker_id"),
                      processing_s=header.get("processing_s"), sha256=sha256_hex(result))
    except (OSError, ValueError, RuntimeError) as exc:
        report["error"] = "%s: %s" % (type(exc).__name__, exc)
    return report


def submit_batch(images, host="127.0.0.1", port=config.COORDINATOR_PORT, size=DEFAULT_SIZE,
                 out_dir=config.OUTPUT_DIR, parallel=DEFAULT_PARALLEL, on_result=None):
    """Submit many images concurrently. Returns reports in input order;
    on_result(report) is called as each one finishes (from a worker thread)."""
    batch = uuid.uuid4().hex[:6]
    with ThreadPoolExecutor(max_workers=max(1, parallel)) as pool:
        futures = [pool.submit(submit_image, image, "%s-%04d" % (batch, number), host, port,
                               size, size, out_dir)
                   for number, image in enumerate(images, 1)]
        if on_result:
            for future in as_completed(futures):
                on_result(future.result())
        return [future.result() for future in futures]


def main(argv=None):
    parser = argparse.ArgumentParser(description="Resize images on the EdgeShare workers.")
    parser.add_argument("path", help="an image file or a folder of images")
    parser.add_argument("--host", default="127.0.0.1", help="coordinator IP (default: this computer)")
    parser.add_argument("--port", type=int, default=config.COORDINATOR_PORT)
    parser.add_argument("--size", type=int, default=DEFAULT_SIZE, help="output is SIZE x SIZE pixels")
    parser.add_argument("--out", default=config.OUTPUT_DIR, help="folder for results")
    parser.add_argument("--parallel", type=int, default=DEFAULT_PARALLEL,
                        help="images in flight at once (default %d)" % DEFAULT_PARALLEL)
    args = parser.parse_args(argv)

    try:
        images = find_images(args.path)
    except (FileNotFoundError, ValueError) as exc:
        raise SystemExit("error: %s" % exc)
    if not images:
        raise SystemExit("error: no images found in %s" % args.path)

    print("Submitting %d image(s) to %s:%d, %dx%d" % (len(images), args.host, args.port,
                                                     args.size, args.size))

    def progress(report):
        name = Path(report["image"]).name
        if report["ok"]:
            print("  OK    %-30s worker %-12s %.2fs" % (name, report["worker_id"],
                                                       report["processing_s"] or 0))
        else:
            print("  FAIL  %-30s %s" % (name, report["error"]))

    reports = submit_batch(images, args.host, args.port, args.size, args.out,
                           args.parallel, on_result=progress)
    done = [r for r in reports if r["ok"]]
    by_worker = {}
    for report in done:
        by_worker[report["worker_id"]] = by_worker.get(report["worker_id"], 0) + 1

    print("\nBATCH COMPLETE: %d/%d images resized" % (len(done), len(reports)))
    for worker_id, count in sorted(by_worker.items()):
        print("  %-12s %d image(s)" % (worker_id, count))
    print("Output folder: %s" % Path(args.out).resolve())
    return 0 if len(done) == len(reports) else 1


if __name__ == "__main__":
    raise SystemExit(main())
