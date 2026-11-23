"""Jobs: split one big job into independent tasks, then merge the task results (member C).

    split(job_type, **options) -> list of tasks {task_id, task_type, params, payload, name}
    merge(job_type, tasks, results) -> (summary dict, output bytes or None)

`results` maps task_id -> result bytes. Merging the same tasks always gives the same
summary whether the results came from the workers or from one laptop, so a job can be
checked by running it locally and comparing (networking/job.py does that).

| job          | what one task does                                  | data sent per task  |
|--------------|-----------------------------------------------------|---------------------|
| montecarlo   | throws N random darts, counts hits (estimates pi)   | none (just a seed)  |
| primes       | counts primes in a number range (segmented sieve)   | none (just a range) |
| matmul       | computes a block of rows of C = A x B               | none (seed + rows)  |
| image_filter | blurs / finds edges in one horizontal image strip   | the strip (PNG)     |
| wordcount    | counts words in one text chunk (MapReduce map step) | the text chunk      |
"""
import hashlib
import io
import json
import random
import uuid
from collections import Counter

from compute.tasks import FILTERS

STRIP_OVERLAP_PX = 12     # extra rows around each image strip, so filters have no seams

DEFAULTS = {
    "montecarlo": {"samples": 20_000_000, "chunks": 40},
    "primes": {"limit": 30_000_000, "chunks": 40},
    "matmul": {"n": 300, "chunks": 30},
    "image_filter": {"filter": "blur", "chunks": 16},
    "wordcount": {"chunks": 24},
}
JOB_TYPES = tuple(DEFAULTS)


def _ids(prefix, count):
    batch = uuid.uuid4().hex[:6]
    return ["%s-%s-%03d" % (prefix, batch, i) for i in range(count)]


def _ranges(total, chunks):
    chunks = max(1, min(chunks, total))
    edges = [total * i // chunks for i in range(chunks + 1)]
    return [(edges[i], edges[i + 1]) for i in range(chunks) if edges[i] < edges[i + 1]]


def sample_image(width=2400, height=1600, seed=7):
    """A colourful test image, for demos without your own photos."""
    from PIL import Image, ImageDraw
    rnd = random.Random(seed)
    img = Image.new("RGB", (width, height), (12, 14, 20))
    draw = ImageDraw.Draw(img)
    for _ in range(260):
        x, y, r = rnd.randrange(width), rnd.randrange(height), rnd.randrange(10, 160)
        colour = tuple(rnd.randrange(40, 256) for _ in range(3))
        (draw.ellipse if rnd.random() < 0.5 else draw.rectangle)((x - r, y - r, x + r, y + r), fill=colour)
    out = io.BytesIO()
    img.save(out, format="PNG")
    return out.getvalue()


def sample_text(words=1_500_000, seed=3):
    """Random English-like text (Zipf-distributed words), for word-count demos."""
    vocab = ("the of and to a in is it you that he was for on are with as i his they be at one "
             "have this from or had by hot word but what some we can out other were all there when "
             "up use your how said an each she which do their time if will way about many then them "
             "write would like so these her long make thing see him two has look more day could go "
             "come did number sound no most people my over know water than call first who may down "
             "side been now find edge network laptop worker task coordinator cloud data compute").split()
    rnd = random.Random(seed)
    weights = [1 / (rank + 1) for rank in range(len(vocab))]
    picked = rnd.choices(vocab, weights, k=words)
    lines = [" ".join(picked[i:i + 12]) for i in range(0, len(picked), 12)]
    return ("\n".join(lines) + "\n").encode()


def split(job_type, **options):
    if job_type not in DEFAULTS:
        raise ValueError("unknown job type: %s (choose from %s)" % (job_type, ", ".join(JOB_TYPES)))
    opts = dict(DEFAULTS[job_type], **{k: v for k, v in options.items() if v is not None})
    chunks = int(opts["chunks"])

    if job_type == "montecarlo":
        parts = _ranges(int(opts["samples"]), chunks)
        ids = _ids("mc", len(parts))
        return [{"task_id": ids[i], "task_type": "montecarlo", "payload": b"", "name": ids[i],
                 "params": {"seed": 1000 + i, "samples": hi - lo}} for i, (lo, hi) in enumerate(parts)]

    if job_type == "primes":
        parts = _ranges(int(opts["limit"]), chunks)
        ids = _ids("pr", len(parts))
        return [{"task_id": ids[i], "task_type": "primes", "payload": b"", "name": ids[i],
                 "params": {"start": lo, "end": hi}} for i, (lo, hi) in enumerate(parts)]

    if job_type == "matmul":
        n = int(opts["n"])
        parts = _ranges(n, chunks)
        ids = _ids("mm", len(parts))
        return [{"task_id": ids[i], "task_type": "matmul_rows", "payload": b"", "name": ids[i],
                 "params": {"n": n, "seed": int(opts.get("seed", 0)), "row_start": lo, "row_end": hi}}
                for i, (lo, hi) in enumerate(parts)]

    if job_type == "image_filter":
        from PIL import Image
        if opts["filter"] not in FILTERS:
            raise ValueError("filter must be one of: " + ", ".join(FILTERS))
        data = opts.get("image") or sample_image()
        img = Image.open(io.BytesIO(data)).convert("RGB")
        width, height = img.size
        parts = _ranges(height, chunks)
        ids = _ids("fx", len(parts))
        tasks = []
        for i, (top, bottom) in enumerate(parts):
            y0, y1 = max(0, top - STRIP_OVERLAP_PX), min(height, bottom + STRIP_OVERLAP_PX)
            out = io.BytesIO()
            img.crop((0, y0, width, y1)).save(out, format="PNG")
            tasks.append({"task_id": ids[i], "task_type": "image_filter", "payload": out.getvalue(),
                          "name": ids[i], "params": {"filter": opts["filter"], "width": width,
                          "height": height, "top": top, "bottom": bottom, "y0": y0}})
        return tasks

    if job_type == "wordcount":
        text = opts.get("text") or sample_text()
        lines = text.splitlines(keepends=True)
        parts = _ranges(len(lines), chunks)
        ids = _ids("wc", len(parts))
        return [{"task_id": ids[i], "task_type": "wordcount", "payload": b"".join(lines[lo:hi]),
                 "name": ids[i], "params": {}} for i, (lo, hi) in enumerate(parts)]

    raise ValueError("unknown job type: %s (choose from %s)" % (job_type, ", ".join(JOB_TYPES)))


def merge(job_type, tasks, results):
    """Combine task results into one answer. Every task must have a result."""
    missing = [t["task_id"] for t in tasks if t["task_id"] not in results]
    if missing:
        raise ValueError("%d task(s) have no result" % len(missing))
    parts = [results[t["task_id"]] for t in tasks]

    if job_type == "montecarlo":
        inside = sum(json.loads(p)["inside"] for p in parts)
        samples = sum(json.loads(p)["samples"] for p in parts)
        return {"pi_estimate": round(4 * inside / samples, 6), "samples": samples,
                "inside": inside}, None

    if job_type == "primes":
        found = [json.loads(p) for p in parts]
        largest = max((f["largest"] for f in found if f["largest"] is not None), default=None)
        return {"primes_below": tasks[-1]["params"]["end"],
                "count": sum(f["count"] for f in found), "largest": largest}, None

    if job_type == "matmul":
        rows = [row for p in parts for row in json.loads(p)]
        digest = hashlib.sha256(json.dumps(rows).encode()).hexdigest()
        return {"n": len(rows), "checksum": digest[:16],
                "trace": round(sum(rows[i][i] for i in range(len(rows))), 6)}, None

    if job_type == "image_filter":
        from PIL import Image
        first = tasks[0]["params"]
        canvas = Image.new("RGB", (first["width"], first["height"]))
        for task, part in zip(tasks, parts):
            p = task["params"]
            strip = Image.open(io.BytesIO(part))
            canvas.paste(strip.crop((0, p["top"] - p["y0"], p["width"], p["bottom"] - p["y0"])),
                         (0, p["top"]))
        out = io.BytesIO()
        canvas.save(out, format="PNG")
        data = out.getvalue()
        return {"filter": first["filter"], "size": "%dx%d" % (first["width"], first["height"]),
                "checksum": hashlib.sha256(canvas.tobytes()).hexdigest()[:16]}, data

    if job_type == "wordcount":
        total = Counter()
        for p in parts:
            total.update(json.loads(p))
        return {"words": sum(total.values()), "distinct": len(total),
                "top": total.most_common(10)}, None

    raise ValueError("unknown job type: " + job_type)
