"""Entry point.
  python main.py coordinator [--gui] [--scheduler round_robin|resource_weighted] [--port 5000]
  python main.py worker <coordinator-ip> [worker-id] [--port 5000]
  python main.py worker --discover [worker-id]
  python main.py submit <image-or-folder> [--host <coordinator-ip>] [--size 512] [--port 5000]

On a Mac, port 5000 is used by AirPlay Receiver: turn it off (System Settings >
General > AirDrop & Handoff) or use another --port on the coordinator and every worker.
"""
import sys


def _option(args, name, default=None):
    """Value after `name` in args (and remove both), or default."""
    if name in args:
        index = args.index(name)
        if index + 1 >= len(args):
            raise SystemExit("%s needs a value\n%s" % (name, __doc__))
        value = args[index + 1]
        del args[index:index + 2]
        return value
    return default


def main():
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)

    if sys.argv[1] == "coordinator":
        import config
        from compute.coordinator import TaskCoordinator
        from networking.discovery import local_ip
        from networking.server import start_server

        args = sys.argv[2:]
        gui = "--gui" in args
        strategy = _option(args, "--scheduler", config.SCHEDULER)
        port = int(_option(args, "--port", config.COORDINATOR_PORT))
        try:
            coordinator = TaskCoordinator(strategy)
        except ValueError as exc:
            raise SystemExit(exc)

        port_hint = "" if port == config.COORDINATOR_PORT else " --port %d" % port
        print(f"[server] workers connect with: python main.py worker {local_ip()}{port_hint}")
        try:
            if gui:
                from gui.dashboard import run_dashboard
                run_dashboard(coordinator, port)
            else:
                start_server(port=port, coordinator=coordinator, discovery=True)
        except OSError as exc:
            raise SystemExit(f"[server] cannot listen on port {port}: {exc}\n"
                             "On a Mac, turn off AirPlay Receiver or use --port 5050.")
        except KeyboardInterrupt:
            print("\n[server] stopped")

    elif sys.argv[1] == "worker":
        from networking.client import worker_loop

        args = sys.argv[2:]
        port = _option(args, "--port")
        port = int(port) if port else None
        if args and args[0] == "--discover":
            from networking.discovery import find_coordinator
            print("[worker] looking for a coordinator on the network...")
            found = find_coordinator(timeout=15)
            if found is None:
                raise SystemExit("No coordinator found. Start it first, or pass its IP: "
                                 "python main.py worker <coordinator-ip>")
            host, port = found
            print(f"[worker] found coordinator at {host}:{port}")
        else:
            host = args[0] if args else "127.0.0.1"
        worker_id = args[1] if len(args) > 1 else None

        try:
            if port is None:
                worker_loop(host, worker_id=worker_id)
            else:
                worker_loop(host, port, worker_id=worker_id)
        except KeyboardInterrupt:
            print("\n[worker] stopped")

    elif sys.argv[1] == "submit":
        from networking.submit import main as submit_main
        raise SystemExit(submit_main(sys.argv[2:]))

    else:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    main()
