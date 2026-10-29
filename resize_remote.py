import socket
import uuid
import hashlib
import sys
from pathlib import Path

import config
from networking.protocol import send_msg, recv_msg

def resize_image(image_path, width, height, output_dir):
    image_path = Path(image_path)

    if not image_path.is_file():
        print(f"SKIPPED: Image not found: {image_path}")
        return False

    task_id = str(uuid.uuid4())
    image_bytes = image_path.read_bytes()

    print(f"\nSending: {image_path.name} ({len(image_bytes)} bytes)")

    try:
        with socket.create_connection(
            ("127.0.0.1", config.COORDINATOR_PORT), timeout=310
        ) as sock:
            sock.settimeout(310)

            send_msg(sock, {
                "type": "SUBMIT",
                "task_id": task_id,
                "task_type": "resize",
                "params": {"width": width, "height": height},
            }, image_bytes)

            response, _ = recv_msg(sock)

            if response.get("type") != "ACK":
                print(f"FAILED: Submission rejected: {response}")
                return False

            print("Task submitted. Waiting for worker...")
            response, result = recv_msg(sock)

            if response.get("type") != "RESULT":
                print(f"FAILED: Worker result: {response}")
                return False

        output_path = output_dir / f"{image_path.stem}_resized.png"
        output_path.write_bytes(result)

        print(f"SUCCESS: {output_path}")
        print(f"SHA-256: {hashlib.sha256(result).hexdigest()}")
        return True

    except (OSError, ConnectionError, TimeoutError) as exc:
        print(f"FAILED: {image_path.name}: {exc}")
        return False

def main():
    if len(sys.argv) == 4:
        # Single-image mode:
        # python resize_remote.py image.png 512 512
        images = [Path(sys.argv[1])]
        width, height = int(sys.argv[2]), int(sys.argv[3])

    elif len(sys.argv) == 2:
        # Batch mode:
        # python resize_remote.py test_images
        input_dir = Path(sys.argv[1])

        if not input_dir.is_dir():
            print(f"Input folder not found: {input_dir}")
            return

        images = sorted(
            p for p in input_dir.iterdir()
            if p.is_file() and p.suffix.lower() in (".jpg", ".jpeg", ".png")
        )
        width, height = 512, 512

    else:
        print("Usage:")
        print("  Single image: python resize_remote.py image.png 512 512")
        print("  Batch:        python resize_remote.py test_images")
        return

    if not images:
        print("No JPG, JPEG, or PNG images found.")
        return

    output_dir = Path("output")
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Images to process: {len(images)}")
    print(f"Target dimensions: {width}x{height}")

    succeeded = 0

    for image in images:
        if resize_image(image, width, height, output_dir):
            succeeded += 1

    print(f"\nBATCH COMPLETE: {succeeded}/{len(images)} images resized.")
    print(f"Output folder: {output_dir.resolve()}")

if __name__ == "__main__":
    main()