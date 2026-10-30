"""Worker-side TCP client for EdgeShare."""
import socket

import config
from networking.protocol import recv_msg, send_msg
from compute.tasks import run_task


def worker_loop(host, port=config.COORDINATOR_PORT, worker_id="mac-worker"):
    while True:
        try:
            with socket.create_connection((host, port), timeout=10) as sock:
                sock.settimeout(None)

                send_msg(sock, {
                    "type": "REGISTER",
                    "worker_id": worker_id
                })

                header, _ = recv_msg(sock)
                print(f"[worker] Connected: {header.get('type')}")

                while True:
                    header, payload = recv_msg(sock)
                    msg_type = header.get("type")

                    if msg_type == "TASK":
                        task_id = header["task_id"]
                        task_type = header["task_type"]
                        params = header.get("params", {})

                        print(f"[worker] Processing task {task_id}")

                        result, elapsed = run_task(
                            task_type, params, payload
                        )

                        send_msg(sock, {
                            "type": "RESULT",
                            "task_id": task_id,
                            "processing_s": elapsed
                        }, result)

                        print(f"[worker] Completed task {task_id}")

                    elif msg_type == "ACK":
                        print(f"[worker] Received ACK: {header}")
                        if "task_id" in header:
                            print("[worker] Requesting next task...")
                            send_msg(sock, {"type": "STATUS"})

                    elif msg_type == "ERROR":
                        print(f"[worker] Server error: {header}")

        except (OSError, ConnectionError, EOFError) as exc:
            print(f"[worker] Connection lost: {exc}")
            print("[worker] Reconnecting in 3 seconds...")
            import time
            time.sleep(3)


if __name__ == "__main__":
    import sys

    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    worker_id = sys.argv[2] if len(sys.argv) > 2 else "mac-worker"
    worker_loop(host, worker_id=worker_id)
