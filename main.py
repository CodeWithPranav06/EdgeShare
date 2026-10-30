"""Entry point.
  python main.py coordinator
  python main.py worker <coordinator-ip> [worker-id]
"""
import sys


def main():
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)

    if sys.argv[1] == "coordinator":
        from networking.server import start_server
        start_server()

    elif sys.argv[1] == "worker":
        from networking.client import worker_loop

        host = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1"
        worker_id = sys.argv[3] if len(sys.argv) > 3 else "mac-worker"

        worker_loop(host, worker_id=worker_id)

    else:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    main()
