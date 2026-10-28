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
        from networking.worker import Worker
        host = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1"
        Worker(host, worker_id=sys.argv[3] if len(sys.argv) > 3 else None).run()
    else:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    main()
