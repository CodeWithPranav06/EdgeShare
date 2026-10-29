"""
image_processor.py - load a folder of images, split it into batches, resize, save outputs.

Used by:
  * the single-laptop baseline (process_folder runs everything on this machine)
  * workers in the distributed run (resize_bytes / process_batch on the chunk they receive)
  * the correctness check (hash_files gives one SHA-256 over the outputs, in order)

Quick start:
    python image_processor.py data/images out/baseline --size 512 --batch-size 10

From Python:
    paths   = load_image_paths("data/images")
    batches = split_into_batches(paths, batch_size=10)      # or num_batches=3
    result  = process_batch(batches[0], "out/run1", size=512)
    digest  = hash_files(result["outputs"])

Design notes
  * Order is always preserved: paths are sorted by name, batches keep that order, and
    outputs come back in input order, so distributed and baseline results can be compared.
  * One unreadable image never stops a run; it is reported in result["failed"].
  * Output is deterministic (fixed JPEG quality, no random steps), so two machines with
    the SAME Pillow version produce byte-identical files. Check this with:
        python -c "import PIL; print(PIL.__version__)"      on every laptop.
"""

import argparse
import hashlib
import io
import os
import sys
import time
from pathlib import Path

from PIL import Image, ImageOps

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".bmp", ".webp", ".tif", ".tiff"}
DEFAULT_SIZE = 512
DEFAULT_QUALITY = 90


# --------------------------------------------------------------------------
# 1. Load a folder
# --------------------------------------------------------------------------

def load_image_paths(folder, limit=None):
    """Return the image files in `folder`, sorted by name (stable order).

    limit: keep only the first N images (handy for the 50 / 200 / 500 size experiments).
    Raises a clear error if the folder is missing, empty, or has two files with the
    same name but different extensions (their outputs would overwrite each other).
    """
    folder = Path(folder)
    if not folder.is_dir():
        raise FileNotFoundError("image folder not found: %s" % folder)

    paths = sorted(p for p in folder.iterdir()
                   if p.is_file() and p.suffix.lower() in IMAGE_EXTENSIONS)
    if not paths:
        raise ValueError("no images (%s) found in %s" % (", ".join(sorted(IMAGE_EXTENSIONS)), folder))

    stems = [p.stem for p in paths]
    duplicates = sorted({s for s in stems if stems.count(s) > 1})
    if duplicates:
        raise ValueError("files share a name and would overwrite each other: %s" % ", ".join(duplicates[:5]))

    return paths[:limit] if limit else paths


# --------------------------------------------------------------------------
# 2. Split into batches
# --------------------------------------------------------------------------

def split_into_batches(items, batch_size=None, num_batches=None):
    """Split a list into batches, keeping order. Give EXACTLY ONE of:

        batch_size  : items per batch      (10 images -> [10, 10, 10, ...])
        num_batches : number of batches    (3 batches over 100 items -> [34, 33, 33])
    """
    if (batch_size is None) == (num_batches is None):
        raise ValueError("give exactly one of batch_size or num_batches")
    items = list(items)

    if batch_size is not None:
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        return [items[i:i + batch_size] for i in range(0, len(items), batch_size)]

    if num_batches < 1:
        raise ValueError("num_batches must be at least 1")
    base, extra = divmod(len(items), num_batches)
    batches, start = [], 0
    for i in range(num_batches):
        length = base + (1 if i < extra else 0)
        if length:
            batches.append(items[start:start + length])
        start += length
    return batches


# --------------------------------------------------------------------------
# 3. Resize (works on bytes, so it can run on a worker that received the image over TCP)
# --------------------------------------------------------------------------

def resize_bytes(data, size=DEFAULT_SIZE, quality=DEFAULT_QUALITY, keep_aspect=False):
    """Resize one image given as bytes; return JPEG bytes.

    size        : output is size x size pixels (or fits inside it if keep_aspect=True)
    keep_aspect : False = exactly size x size (matches the "resize to 512x512" task)
    Phone photos are rotated upright using their EXIF tag, and PNG transparency is
    flattened, so any input becomes a normal RGB JPEG.
    """
    with Image.open(io.BytesIO(data)) as image:
        image = ImageOps.exif_transpose(image)
        image = image.convert("RGB")
        if keep_aspect:
            image.thumbnail((size, size), Image.LANCZOS)
        else:
            image = image.resize((size, size), Image.LANCZOS)
        buffer = io.BytesIO()
        image.save(buffer, "JPEG", quality=quality)
        return buffer.getvalue()


def output_name(path, size=DEFAULT_SIZE):
    """Name of the output file for an input image, e.g. photo_01.png -> photo_01_512.jpg"""
    return "%s_%d.jpg" % (Path(path).stem, size)


# --------------------------------------------------------------------------
# 4. Save outputs
# --------------------------------------------------------------------------

def save_output(output_dir, filename, data):
    """Write bytes to output_dir/filename safely (temp file then rename, so a crash
    never leaves a half-written image). Returns the final Path."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    final = output_dir / os.path.basename(filename)       # never write outside output_dir
    temp = final.with_name(final.name + ".tmp")
    with open(temp, "wb") as f:
        f.write(data)
    os.replace(temp, final)
    return final


def process_batch(paths, output_dir, size=DEFAULT_SIZE, quality=DEFAULT_QUALITY, keep_aspect=False):
    """Resize every image in `paths` and save the results into `output_dir`.

    Returns a dict:
        outputs : list of output Paths, in input order (failed images are left out)
        failed  : list of (input_path, error message)
        seconds : time spent on this batch
    """
    start = time.perf_counter()
    outputs, failed = [], []
    for path in paths:
        try:
            with open(path, "rb") as f:
                data = f.read()
            resized = resize_bytes(data, size, quality, keep_aspect)
            outputs.append(save_output(output_dir, output_name(path, size), resized))
        except Exception as error:                        # one bad file must not stop the run
            failed.append((str(path), "%s: %s" % (type(error).__name__, error)))
    return {"outputs": outputs, "failed": failed, "seconds": time.perf_counter() - start}


# --------------------------------------------------------------------------
# 5. Correctness check helper
# --------------------------------------------------------------------------

def hash_files(paths):
    """One SHA-256 over the contents of all files, in the order given.

    Same inputs + same order + same Pillow version = same hash, so
    hash_files(baseline_outputs) == hash_files(distributed_outputs) proves the
    distributed run produced identical results.
    """
    digest = hashlib.sha256()
    for path in paths:
        with open(path, "rb") as f:
            for block in iter(lambda: f.read(1 << 20), b""):
                digest.update(block)
    return digest.hexdigest()


# --------------------------------------------------------------------------
# 6. Whole folder, single laptop (the baseline)
# --------------------------------------------------------------------------

def process_folder(input_dir, output_dir, size=DEFAULT_SIZE, batch_size=10, limit=None,
                   quality=DEFAULT_QUALITY, keep_aspect=False, verbose=True):
    """Load a folder, split into batches, process all batches one after another on
    this machine, and save outputs. Returns a summary dict:

        images, batches, failed, seconds, images_per_second, output_hash, outputs
    """
    paths = load_image_paths(input_dir, limit)
    batches = split_into_batches(paths, batch_size=batch_size)

    start = time.perf_counter()
    outputs, failed = [], []
    for number, batch in enumerate(batches, 1):
        result = process_batch(batch, output_dir, size, quality, keep_aspect)
        outputs.extend(result["outputs"])
        failed.extend(result["failed"])
        if verbose:
            print("  batch %d/%d: %d images in %.2fs" % (number, len(batches), len(batch), result["seconds"]))
    seconds = time.perf_counter() - start

    return {
        "images": len(paths),
        "batches": len(batches),
        "failed": failed,
        "seconds": seconds,
        "images_per_second": len(outputs) / seconds if seconds > 0 else 0.0,
        "output_hash": hash_files(outputs),
        "outputs": outputs,
    }


# --------------------------------------------------------------------------
# Command line
# --------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Resize a folder of images in batches (single-laptop baseline).")
    parser.add_argument("input_dir", help="folder with input images")
    parser.add_argument("output_dir", help="folder for resized images (created if missing)")
    parser.add_argument("--size", type=int, default=DEFAULT_SIZE, help="output size in pixels (default 512)")
    parser.add_argument("--batch-size", type=int, default=10, help="images per batch (default 10)")
    parser.add_argument("--limit", type=int, default=None, help="only use the first N images")
    parser.add_argument("--keep-aspect", action="store_true", help="fit inside size x size instead of stretching")
    args = parser.parse_args()

    try:
        summary = process_folder(args.input_dir, args.output_dir, args.size, args.batch_size,
                                 args.limit, keep_aspect=args.keep_aspect)
    except (FileNotFoundError, ValueError) as error:
        sys.exit("error: %s" % error)

    print("\nDone: %d images, %d batches, %.2fs (%.1f images/s)" % (
        summary["images"], summary["batches"], summary["seconds"], summary["images_per_second"]))
    print("Output hash (SHA-256): %s" % summary["output_hash"])
    if summary["failed"]:
        print("%d image(s) failed:" % len(summary["failed"]))
        for path, message in summary["failed"]:
            print("  %s -> %s" % (path, message))


if __name__ == "__main__":
    main()