"""Tests for image_processor.py. Run with:  python -m pytest tests/test_image_processor.py"""

import io
import random

import pytest
from PIL import Image

from compute import image_processor as ip


def _make_images(folder, count=7):
    """Write a mix of JPEG and RGBA PNG test images plus one corrupt file."""
    rng = random.Random(0)
    for i in range(count):
        size = rng.choice([(200, 150), (320, 240), (64, 64)])
        image = Image.frombytes("RGB", size, rng.randbytes(size[0] * size[1] * 3))
        image.save(folder / ("img_%02d.jpg" % i), "JPEG")
    Image.new("RGBA", (50, 80), (255, 0, 0, 128)).save(folder / "alpha.png")
    (folder / "broken.jpg").write_bytes(b"this is not an image")


def test_load_sorted_and_limit(tmp_path):
    _make_images(tmp_path)
    paths = ip.load_image_paths(tmp_path)
    assert [p.name for p in paths] == sorted(p.name for p in paths)
    assert len(ip.load_image_paths(tmp_path, limit=3)) == 3


def test_load_errors(tmp_path):
    with pytest.raises(FileNotFoundError):
        ip.load_image_paths(tmp_path / "missing")
    with pytest.raises(ValueError):
        ip.load_image_paths(tmp_path)                       # empty folder
    Image.new("RGB", (4, 4)).save(tmp_path / "a.jpg")
    Image.new("RGB", (4, 4)).save(tmp_path / "a.png")
    with pytest.raises(ValueError):
        ip.load_image_paths(tmp_path)                       # duplicate stem


def test_split_by_size_and_count_keep_order():
    items = list(range(10))
    assert ip.split_into_batches(items, batch_size=4) == [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9]]
    parts = ip.split_into_batches(items, num_batches=3)
    assert [len(p) for p in parts] == [4, 3, 3]
    assert [x for p in parts for x in p] == items
    assert len(ip.split_into_batches([1, 2], num_batches=5)) == 2     # no empty batches
    with pytest.raises(ValueError):
        ip.split_into_batches(items)
    with pytest.raises(ValueError):
        ip.split_into_batches(items, batch_size=2, num_batches=2)


def test_resize_bytes_sizes():
    source = io.BytesIO()
    Image.new("RGB", (400, 200), (10, 20, 30)).save(source, "JPEG")
    exact = Image.open(io.BytesIO(ip.resize_bytes(source.getvalue(), 128)))
    assert exact.size == (128, 128)
    fitted = Image.open(io.BytesIO(ip.resize_bytes(source.getvalue(), 128, keep_aspect=True)))
    assert fitted.size == (128, 64)


def test_process_folder_handles_failures_and_is_deterministic(tmp_path):
    source = tmp_path / "in"
    source.mkdir()
    _make_images(source)

    first = ip.process_folder(source, tmp_path / "out1", size=64, batch_size=3, verbose=False)
    assert first["images"] == 9                               # 7 jpg + png + broken
    assert len(first["outputs"]) == 8
    assert len(first["failed"]) == 1 and "broken.jpg" in first["failed"][0][0]
    for path in first["outputs"]:
        assert Image.open(path).size == (64, 64)

    second = ip.process_folder(source, tmp_path / "out2", size=64, batch_size=5, verbose=False)
    assert first["output_hash"] == second["output_hash"]      # batch size must not change results


def test_batches_match_whole_run(tmp_path):
    """Processing batches separately (like workers) gives the same hash as one run."""
    source = tmp_path / "in"
    source.mkdir()
    _make_images(source)
    (source / "broken.jpg").unlink()
    whole = ip.process_folder(source, tmp_path / "whole", size=64, batch_size=100, verbose=False)

    outputs = []
    for batch in ip.split_into_batches(ip.load_image_paths(source), num_batches=3):
        outputs += ip.process_batch(batch, tmp_path / "parts", size=64)["outputs"]
    assert ip.hash_files(outputs) == whole["output_hash"]


def test_save_output_cannot_escape_folder(tmp_path):
    saved = ip.save_output(tmp_path / "out", "../../evil.jpg", b"x")
    assert saved.parent == tmp_path / "out"
