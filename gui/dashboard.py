"""Dashboard (member D): a Tkinter window for the coordinator.

    python main.py coordinator --gui

Runs the TCP server in a background thread and redraws twice a second from
TaskCoordinator.snapshot(), so everything shown is live backend state: workers with
CPU / RAM / battery / charging and heartbeat age, every task with its state, worker,
and attempts, and an event log (registrations, disconnects, timeouts, requeues,
failures). "Submit images..." sends pictures through the same SUBMIT path as
`python main.py submit`, so results land in the output folder either way.

DashboardModel holds the formatting logic and has no Tk dependency, so it is tested
without opening a window.
"""
import os
import queue
import subprocess
import sys
import threading
import time
from pathlib import Path

import config

WORKER_COLUMNS = [("worker", "Worker", 110), ("state", "State", 90), ("cpu", "CPU", 60),
                  ("cores", "Cores", 50), ("ram", "Free RAM", 90), ("battery", "Battery", 70),
                  ("charging", "Charging", 70), ("seen", "Heartbeat", 80),
                  ("task", "Task", 110), ("done", "Done", 50), ("note", "Note", 220)]
TASK_COLUMNS = [("task", "Task", 110), ("name", "Image", 170), ("state", "State", 90),
                ("worker", "Worker", 100), ("attempts", "Tries", 50), ("time", "Time", 70),
                ("error", "Error", 330)]
STATE_COLOURS = {"idle": "#1b7f3b", "busy": "#1f5fbf", "unavailable": "#b26b00",
                 "failed": "#b3261e", "pending": "#666666", "assigned": "#1f5fbf",
                 "running": "#1f5fbf", "completed": "#1b7f3b"}


def _fmt(value, pattern="%s", missing="n/a"):
    return missing if value is None else pattern % value


class DashboardModel:
    """Turns a coordinator snapshot into rows and text for the window."""

    def __init__(self, snapshot):
        self.snapshot = snapshot

    def worker_rows(self):
        rows = []
        for w in self.snapshot["workers"]:
            charging = w["power_plugged"]
            rows.append((w["worker_id"], w["state"], (
                w["worker_id"],
                w["state"],
                _fmt(w["cpu_percent"], "%.0f%%", "?"),
                _fmt(w["cpu_count"], "%s", "?"),
                _fmt(w["free_ram_mb"], "%d MB", "?"),
                _fmt(w["battery_percent"], "%d%%"),
                "n/a" if charging is None else ("yes" if charging else "no"),
                _fmt(w["last_seen_s"], "%.1fs ago", "-"),
                ", ".join(w["tasks"]) or "-",
                w["completed"],
                w["reason"],
            )))
        return rows

    def task_rows(self):
        rows = []
        for t in self.snapshot["tasks"]:
            rows.append((t["task_id"], t["state"], (
                t["task_id"],
                t.get("name") or t.get("task_type") or "",
                t["state"],
                t["worker"] or "-",
                t["attempts"],
                _fmt(t["processing_s"], "%.2fs", "-"),
                t["error"] or "",
            )))
        return rows

    def summary(self):
        counts = self.snapshot["counts"]
        live = sum(1 for w in self.snapshot["workers"] if w["state"] != "failed")
        failed = len(self.snapshot["workers"]) - live
        return ("Workers: %d connected, %d failed   |   Tasks: %d total, %d pending, "
                "%d running, %d completed, %d failed" % (
                    live, failed, counts["total"], counts["pending"],
                    counts["assigned"] + counts["running"], counts["completed"],
                    counts["failed"]))

    def new_events(self, seen_count):
        """Events logged after the first `seen_count` (as many as are still kept)."""
        unseen = self.snapshot["event_count"] - seen_count
        if unseen <= 0:
            return []
        events = self.snapshot["events"]
        return events[-unseen:] if unseen < len(events) else events

    def progress(self):
        """Percent of tasks finished (completed or failed)."""
        counts = self.snapshot["counts"]
        if not counts["total"]:
            return 0.0
        return 100.0 * (counts["completed"] + counts["failed"]) / counts["total"]

    def uptime(self):
        seconds = int(self.snapshot["uptime_s"])
        return "%02d:%02d:%02d" % (seconds // 3600, seconds // 60 % 60, seconds % 60)


class Dashboard:
    REFRESH_MS = 500

    def __init__(self, root, coordinator, port=config.COORDINATOR_PORT, start_server=True):
        import tkinter as tk
        from tkinter import ttk

        self.tk, self.ttk = tk, ttk
        self.root = root
        self.coordinator = coordinator
        self.port = port
        self.out_dir = Path(config.OUTPUT_DIR).resolve()
        self.results = queue.Queue()               # submit reports from background threads
        self.seen_events = 0
        self.stop = threading.Event()
        self.server_error = None

        root.title("EdgeShare Coordinator")
        root.geometry("1150x720")
        root.protocol("WM_DELETE_WINDOW", self.close)
        self._build()
        if start_server:
            self._start_server()
        self.refresh()

    # ----------------------------------------------------------------------- layout

    def _build(self):
        tk, ttk = self.tk, self.ttk
        from networking.discovery import local_ip

        top = ttk.Frame(self.root, padding=(10, 8))
        top.pack(fill="x")
        ttk.Label(top, text="EdgeShare Coordinator", font=("Helvetica", 16, "bold")).pack(side="left")
        self.info = ttk.Label(top, text="")
        self.info.pack(side="right")
        self.address = "%s:%d" % (local_ip(), self.port)

        bar = ttk.Frame(self.root, padding=(10, 0))
        bar.pack(fill="x")
        self.summary = ttk.Label(bar, text="")
        self.summary.pack(side="left")
        self.progress = ttk.Progressbar(bar, length=220, maximum=100)
        self.progress.pack(side="right")

        controls = ttk.Frame(self.root, padding=(10, 6))
        controls.pack(fill="x")
        ttk.Button(controls, text="Submit images...", command=self.choose_images).pack(side="left")
        ttk.Button(controls, text="Submit folder...", command=self.choose_folder).pack(side="left", padx=6)
        ttk.Label(controls, text="Size:").pack(side="left", padx=(12, 2))
        self.size = tk.StringVar(value="512")
        ttk.Entry(controls, textvariable=self.size, width=6).pack(side="left")
        ttk.Label(controls, text="px").pack(side="left", padx=(2, 12))
        ttk.Button(controls, text="Open output folder", command=self.open_output).pack(side="left")
        self.out_label = ttk.Label(controls, text="Results: %s" % self.out_dir)
        self.out_label.pack(side="left", padx=10)

        panes = ttk.PanedWindow(self.root, orient="vertical")
        panes.pack(fill="both", expand=True, padx=10, pady=(0, 6))
        self.workers = self._table(panes, "Workers", WORKER_COLUMNS, 6)
        self.tasks = self._table(panes, "Tasks", TASK_COLUMNS, 10)

        log_frame = ttk.LabelFrame(panes, text="Events", padding=4)
        self.log = tk.Text(log_frame, height=8, state="disabled", wrap="word")
        self.log.tag_configure("warn", foreground=STATE_COLOURS["failed"])
        self.log.tag_configure("ok", foreground=STATE_COLOURS["completed"])
        scroll = ttk.Scrollbar(log_frame, command=self.log.yview)
        self.log.configure(yscrollcommand=scroll.set)
        scroll.pack(side="right", fill="y")
        self.log.pack(fill="both", expand=True)
        panes.add(log_frame, weight=1)

        self.status = ttk.Label(self.root, text="", padding=(10, 2, 10, 6))
        self.status.pack(fill="x")

    def _table(self, panes, title, columns, height):
        ttk = self.ttk
        frame = ttk.LabelFrame(panes, text=title, padding=4)
        tree = ttk.Treeview(frame, columns=[c[0] for c in columns], show="headings", height=height)
        for key, heading, width in columns:
            tree.heading(key, text=heading)
            tree.column(key, width=width, anchor="w", stretch=key in ("note", "error"))
        for state, colour in STATE_COLOURS.items():
            tree.tag_configure(state, foreground=colour)
        scroll = ttk.Scrollbar(frame, command=tree.yview)
        tree.configure(yscrollcommand=scroll.set)
        scroll.pack(side="right", fill="y")
        tree.pack(fill="both", expand=True)
        panes.add(frame, weight=2 if title == "Tasks" else 1)
        return tree

    # ----------------------------------------------------------------------- server

    def _start_server(self):
        from networking.server import start_server

        ready = threading.Event()

        def serve():
            try:
                start_server(port=self.port, ready=ready, stop=self.stop,
                             coordinator=self.coordinator, discovery=True)
            except OSError as exc:
                self.server_error = "Cannot listen on port %d: %s" % (self.port, exc)
                ready.set()

        threading.Thread(target=serve, daemon=True).start()
        ready.wait(5)

    # ---------------------------------------------------------------------- refresh

    def refresh(self):
        model = DashboardModel(self.coordinator.snapshot())

        state = "ERROR: " + self.server_error if self.server_error else "listening on " + self.address
        self.info.configure(text="%s   |   scheduler: %s   |   uptime %s" % (
            state, model.snapshot["strategy"], model.uptime()))
        self.summary.configure(text=model.summary())
        self.progress.configure(value=model.progress())
        self._sync(self.workers, model.worker_rows())
        self._sync(self.tasks, model.task_rows())
        self._show_events(model.new_events(self.seen_events))
        self.seen_events = model.snapshot["event_count"]
        self._show_results()

        if self.server_error:
            self.status.configure(text=self.server_error + "  (on a Mac: turn off AirPlay "
                                  "Receiver or start with --port 5050)")
        self.root.after(self.REFRESH_MS, self.refresh)

    def _sync(self, tree, rows):
        """Update rows in place so the selection and scroll position survive."""
        wanted = set()
        for iid, state, values in rows:
            wanted.add(iid)
            if tree.exists(iid):
                tree.item(iid, values=values, tags=(state,))
            else:
                tree.insert("", "end", iid=iid, values=values, tags=(state,))
        for iid in tree.get_children():
            if iid not in wanted:
                tree.delete(iid)

    def _show_events(self, events):
        for stamp, text, warn in events:
            self._log("%s  %s" % (time.strftime("%H:%M:%S", time.localtime(stamp)), text),
                      "warn" if warn else None)
            if warn:
                self.status.configure(text=text)

    def _log(self, line, tag=None):
        self.log.configure(state="normal")
        self.log.insert("end", line + "\n", tag or ())
        self.log.see("end")
        self.log.configure(state="disabled")

    def _show_results(self):
        while True:
            try:
                report = self.results.get_nowait()
            except queue.Empty:
                return
            name = Path(report["image"]).name
            stamp = time.strftime("%H:%M:%S")
            if report.get("summary"):
                self._log("%s  %s" % (stamp, report["summary"]), "ok")
                self.status.configure(text=report["summary"])
            elif report["ok"]:
                self._log("%s  %s -> %s (worker %s)" % (stamp, name, report["output"],
                                                       report["worker_id"]), "ok")
            else:
                self._log("%s  %s FAILED: %s" % (stamp, name, report["error"]), "warn")

    # ---------------------------------------------------------------------- actions

    def choose_images(self):
        from tkinter import filedialog
        paths = filedialog.askopenfilenames(
            title="Images to resize",
            filetypes=[("Images", "*.png *.jpg *.jpeg *.bmp *.webp *.tif *.tiff"), ("All files", "*")])
        if paths:
            self.submit([Path(p) for p in paths])

    def choose_folder(self):
        from tkinter import filedialog
        from networking.submit import find_images
        folder = filedialog.askdirectory(title="Folder of images to resize")
        if not folder:
            return
        try:
            images = find_images(folder)
        except (FileNotFoundError, ValueError) as exc:
            self.status.configure(text="Cannot submit: %s" % exc)
            return
        if images:
            self.submit(images)
        else:
            self.status.configure(text="No images found in %s" % folder)

    def submit(self, images):
        """Send images through the coordinator in a background thread."""
        from networking.submit import submit_batch
        try:
            size = int(self.size.get())
            if size < 1:
                raise ValueError
        except ValueError:
            self.status.configure(text="Size must be a positive whole number of pixels")
            return

        self.status.configure(text="Submitting %d image(s) at %dx%d..." % (len(images), size, size))

        def work():
            reports = submit_batch(images, "127.0.0.1", self.port, size, self.out_dir,
                                   on_result=self.results.put)
            done = sum(1 for r in reports if r["ok"])
            self.results.put({"image": "", "summary": "Batch finished: %d/%d resized, saved in %s"
                              % (done, len(reports), self.out_dir)})

        threading.Thread(target=work, daemon=True).start()

    def open_output(self):
        self.out_dir.mkdir(parents=True, exist_ok=True)
        try:
            if sys.platform.startswith("win"):
                os.startfile(str(self.out_dir))
            elif sys.platform == "darwin":
                subprocess.Popen(["open", str(self.out_dir)])
            else:
                subprocess.Popen(["xdg-open", str(self.out_dir)])
        except OSError as exc:
            self.status.configure(text="Cannot open %s: %s" % (self.out_dir, exc))

    def close(self):
        self.stop.set()
        self.root.destroy()


def run_dashboard(coordinator, port=config.COORDINATOR_PORT):
    import tkinter as tk
    root = tk.Tk()
    Dashboard(root, coordinator, port)
    root.mainloop()
