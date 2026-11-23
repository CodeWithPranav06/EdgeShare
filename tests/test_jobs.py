"""New job types: split -> run each task -> merge gives the right answer."""
import io

import pytest
from PIL import Image, ImageFilter

from compute import jobs
from compute.tasks import run_task
from networking.job import run_local


def run(job_type, **options):
    tasks = jobs.split(job_type, **options)
    results, _ = run_local(tasks)
    return tasks, jobs.merge(job_type, tasks, results)


def test_primes_matches_known_count():
    _, (answer, _) = run("primes", limit=1_000_000, chunks=7)
    assert answer["count"] == 78498 and answer["largest"] == 999983


def test_montecarlo_estimates_pi():
    _, (answer, _) = run("montecarlo", samples=400_000, chunks=8)
    assert abs(answer["pi_estimate"] - 3.14159) < 0.02 and answer["samples"] == 400_000


def test_matmul_rows_equal_whole_product():
    _, (split_answer, _) = run("matmul", n=40, chunks=7)
    _, (whole_answer, _) = run("matmul", n=40, chunks=1)
    assert split_answer == whole_answer and split_answer["n"] == 40


def test_image_filter_strips_have_no_seams():
    image = jobs.sample_image(400, 300)
    _, (answer, output) = run("image_filter", image=image, filter="blur", chunks=6)
    whole = Image.open(io.BytesIO(image)).convert("RGB").filter(ImageFilter.GaussianBlur(4))
    assert Image.open(io.BytesIO(output)).tobytes() == whole.tobytes()
    assert answer["size"] == "400x300"


def test_wordcount_counts_every_word():
    text = b"the cat\nthe hat\nA cat sat\n" * 50
    _, (answer, _) = run("wordcount", text=text, chunks=4)
    assert answer["words"] == 350 and dict(answer["top"])["the"] == 100


def test_merge_refuses_missing_results():
    tasks = jobs.split("primes", limit=1000, chunks=4)
    with pytest.raises(ValueError):
        jobs.merge("primes", tasks, {})


def test_unknown_job_and_filter_rejected():
    with pytest.raises(ValueError):
        jobs.split("nope")
    with pytest.raises(ValueError):
        jobs.split("image_filter", filter="nope")
