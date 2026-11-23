"""Task functions run by workers (member C).

Interface: run_task(task_type, params, payload) -> (result_bytes, processing_seconds)
"""
import hashlib
import io
import json
import math
import random
import re
import time
from collections import Counter


def _resize(params, payload):
    from PIL import Image
    img = Image.open(io.BytesIO(payload))
    img = img.resize((params["width"], params["height"]))
    out = io.BytesIO()
    img.save(out, format="PNG")
    return out.getvalue()


def _hash(params, payload):
    return hashlib.sha256(payload).hexdigest().encode()


def _matmul(params, payload):
    """Multiply two n x n random matrices (pure Python, deliberately CPU heavy)."""
    n, rnd = params["n"], random.Random(params.get("seed", 0))
    a = [[rnd.random() for _ in range(n)] for _ in range(n)]
    b = [[rnd.random() for _ in range(n)] for _ in range(n)]
    c = [[sum(a[i][k] * b[k][j] for k in range(n)) for j in range(n)] for i in range(n)]
    return hashlib.sha256(repr(c).encode()).hexdigest().encode()


def _montecarlo(params, payload):
    """Throw `samples` random darts at a unit square; count those inside the circle.
    pi ~= 4 * inside / samples. Pure CPU, almost no data: ideal for distribution."""
    rnd, n = random.Random(params["seed"]), params["samples"]
    inside = 0
    for _ in range(n):
        x, y = rnd.random(), rnd.random()
        if x * x + y * y <= 1.0:
            inside += 1
    return json.dumps({"inside": inside, "samples": n}).encode()


def _primes(params, payload):
    """Count primes in [start, end) with a segmented sieve; also report the largest."""
    start, end = max(2, params["start"]), params["end"]
    if end <= start:
        return json.dumps({"count": 0, "largest": None}).encode()
    root = math.isqrt(end - 1)
    base = bytearray([1]) * (root + 1)
    base[0:2] = b"\0\0"
    for i in range(2, math.isqrt(root) + 1):
        if base[i]:
            base[i * i::i] = bytearray(len(base[i * i::i]))
    segment = bytearray([1]) * (end - start)
    for p in (i for i in range(2, root + 1) if base[i]):
        first = max(p * p, (start + p - 1) // p * p)
        segment[first - start::p] = bytearray(len(segment[first - start::p]))
    count = sum(segment)
    largest = next((start + i for i in range(len(segment) - 1, -1, -1) if segment[i]), None)
    return json.dumps({"count": count, "largest": largest}).encode()


def _matrices(n, seed):
    rnd = random.Random(seed)
    a = [[rnd.random() for _ in range(n)] for _ in range(n)]
    b = [[rnd.random() for _ in range(n)] for _ in range(n)]
    return a, b


def _matmul_rows(params, payload):
    """Rows [row_start, row_end) of C = A x B. A and B are rebuilt from the seed on the
    worker, so only the row numbers travel over the network."""
    n, r0, r1 = params["n"], params["row_start"], params["row_end"]
    a, b = _matrices(n, params.get("seed", 0))
    columns = list(zip(*b))
    rows = [[sum(x * y for x, y in zip(a[i], col)) for col in columns] for i in range(r0, r1)]
    return json.dumps(rows).encode()


FILTERS = ("blur", "edges", "sharpen", "emboss")


def _image_filter(params, payload):
    """Apply a filter to one strip of an image; the job glues the strips back together."""
    from PIL import Image, ImageFilter
    kinds = {"blur": ImageFilter.GaussianBlur(4), "edges": ImageFilter.FIND_EDGES,
             "sharpen": ImageFilter.SHARPEN, "emboss": ImageFilter.EMBOSS}
    img = Image.open(io.BytesIO(payload)).convert("RGB")
    if params["filter"] == "edges":
        img = img.convert("L").filter(kinds["edges"]).convert("RGB")
    else:
        img = img.filter(kinds[params["filter"]])
    out = io.BytesIO()
    img.save(out, format="PNG")
    return out.getvalue()


def _wordcount(params, payload):
    """Map step of a MapReduce word count: word frequencies of one text chunk."""
    words = re.findall(r"[a-z']+", payload.decode("utf-8", "replace").lower())
    return json.dumps(Counter(words)).encode()


TASKS = {"resize": _resize, "hash": _hash, "matmul": _matmul,
         "montecarlo": _montecarlo, "primes": _primes, "matmul_rows": _matmul_rows,
         "image_filter": _image_filter, "wordcount": _wordcount}


def run_task(task_type, params, payload=b""):
    start = time.perf_counter()
    result = TASKS[task_type](params, payload)
    return result, time.perf_counter() - start
