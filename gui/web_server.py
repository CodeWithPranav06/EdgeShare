"""Web dashboard server (member D): http://<coordinator-ip>:8080 in any browser on the LAN.

Started by `python main.py coordinator` next to the TCP server, sharing the same
TaskCoordinator, so the page shows live backend state.

    GET  /                      the dashboard page (gui/web/index.html)
    GET  /api/state             coordinator.snapshot() + server info + web upload results (JSON)
    POST /api/submit?name=&size=  body = image bytes; queued through the normal SUBMIT path
    GET  /api/result/<task_id>  the resized image of a web upload

Uploads go through networking.submit.submit_bytes() to the coordinator's own TCP port,
exactly like `python main.py submit`, so results are also saved in the output folder.
This is meant for a trusted LAN demo: there is no login.
"""
import json
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import config
from networking.discovery import local_ip
from networking.submit import DEFAULT_SIZE, submit_bytes

PAGE = Path(__file__).with_name("web") / "index.html"
MAX_SIZE_PX = 4096


class WebDashboard:
    """Shared state for the request handlers."""

    def __init__(self, coordinator, coordinator_port, out_dir=None):
        self.coordinator = coordinator
        self.coordinator_port = coordinator_port
        self.out_dir = Path(out_dir or config.OUTPUT_DIR).resolve()
        self.uploads = {}                     # task_id -> submit report
        self.lock = threading.Lock()

    def state(self):
        snapshot = self.coordinator.snapshot()
        with self.lock:
            uploads = {task_id: {"name": r["image"], "ok": r["ok"], "done": r.get("done", False),
                                 "output": r["output"], "error": r["error"],
                                 "worker_id": r["worker_id"]}
                       for task_id, r in self.uploads.items()}
        snapshot.update(
            coordinator={"address": local_ip(), "port": self.coordinator_port,
                         "output_dir": str(self.out_dir),
                         "heartbeat_interval_s": config.HEARTBEAT_INTERVAL_S,
                         "heartbeat_timeout_s": config.HEARTBEAT_TIMEOUT_S,
                         "retry_limit": config.TASK_RETRY_LIMIT},
            uploads=uploads,
        )
        return snapshot

    def submit(self, data, name, size):
        task_id = "web-" + uuid.uuid4().hex[:8]
        with self.lock:
            self.uploads[task_id] = {"image": Path(name).name, "ok": False, "done": False,
                                     "output": None, "error": None, "worker_id": None}

        def work():
            report = submit_bytes(data, name, task_id, "127.0.0.1", self.coordinator_port,
                                  size, size, self.out_dir)
            report["done"] = True
            with self.lock:
                self.uploads[task_id] = report

        threading.Thread(target=work, daemon=True).start()
        return task_id

    def result_path(self, task_id):
        with self.lock:
            report = self.uploads.get(task_id)
        if report and report["ok"] and report["output"]:
            return Path(report["output"])
        return None


def make_handler(dashboard):
    class Handler(BaseHTTPRequestHandler):
        server_version = "EdgeShare"

        def log_message(self, *args):         # keep the coordinator console readable
            pass

        def _send(self, status, body, content_type):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _json(self, status, value):
            self._send(status, json.dumps(value).encode("utf-8"), "application/json")

        def do_GET(self):
            path = urlparse(self.path).path
            if path in ("/", "/index.html"):
                self._send(200, PAGE.read_bytes(), "text/html; charset=utf-8")
            elif path == "/api/state":
                self._json(200, dashboard.state())
            elif path == "/favicon.ico":
                self.send_response(204)
                self.end_headers()
            elif path.startswith("/api/result/"):
                result = dashboard.result_path(path.rsplit("/", 1)[-1])
                if result is None or not result.is_file():
                    self._json(404, {"error": "no result for that task"})
                    return
                self.send_response(200)
                self.send_header("Content-Type", "image/png")
                self.send_header("Content-Length", str(result.stat().st_size))
                safe_name = "".join(c for c in result.name if c.isprintable() and c not in '"\\')
                self.send_header("Content-Disposition", 'attachment; filename="%s"' % safe_name)
                self.end_headers()
                self.wfile.write(result.read_bytes())
            else:
                self._json(404, {"error": "not found"})

        def do_POST(self):
            url = urlparse(self.path)
            if url.path != "/api/submit":
                self._json(404, {"error": "not found"})
                return
            query = parse_qs(url.query)
            try:
                length = int(self.headers.get("Content-Length", "0"))
                size = int(query.get("size", [DEFAULT_SIZE])[0])
            except ValueError:
                self._json(400, {"error": "bad Content-Length or size"})
                return
            if not 0 < length <= config.MAX_PAYLOAD_BYTES:
                self._json(413, {"error": "image must be 1 byte to %d MB"
                                 % (config.MAX_PAYLOAD_BYTES // (1024 * 1024))})
                return
            if not 1 <= size <= MAX_SIZE_PX:
                self._json(400, {"error": "size must be 1 to %d pixels" % MAX_SIZE_PX})
                return
            data = self.rfile.read(length)
            if len(data) != length:
                self._json(400, {"error": "upload was cut off"})
                return
            name = query.get("name", ["image.png"])[0]
            self._json(202, {"task_id": dashboard.submit(data, name, size)})

    return Handler


def start_web_dashboard(coordinator, coordinator_port, port=config.WEB_PORT,
                        host="0.0.0.0", out_dir=None):
    """Serve the dashboard in a background thread. Returns (server, url) or (None, error)."""
    dashboard = WebDashboard(coordinator, coordinator_port, out_dir)
    try:
        server = ThreadingHTTPServer((host, port), make_handler(dashboard))
    except OSError as exc:
        return None, "web dashboard not started (port %d: %s)" % (port, exc)
    server.daemon_threads = True
    server.dashboard = dashboard
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, "http://%s:%d" % (local_ip(), server.server_address[1])
