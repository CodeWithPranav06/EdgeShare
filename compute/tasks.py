"""Task functions run by workers (member C).

Interface: run_task(task_type, params, payload) -> (result_bytes, processing_seconds)
"""
import hashlib
import io
import random
import time


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


TASKS = {"resize": _resize, "hash": _hash, "matmul": _matmul}


def run_task(task_type, params, payload=b""):
    start = time.perf_counter()
    result = TASKS[task_type](params, payload)
    return result, time.perf_counter() - start
