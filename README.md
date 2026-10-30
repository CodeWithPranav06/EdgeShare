# EdgeShare

Resource-aware distributed task processing on a local edge network (3rd sem EL project, Edge Computing theme).

One **coordinator** laptop splits independent tasks (image resizing, hashing, matrix multiplication) and sends them to **worker** laptops on the same LAN. We measure when distributed execution beats a single laptop (speedup, efficiency, break-even task size), compare schedulers, and test recovery when a worker drops.

## Quick start

```bash
git clone <repo-url>
cd EdgeShare
python -m venv .venv
.venv\Scripts\activate          # Windows   (Linux/Mac: source .venv/bin/activate)
pip install -r requirements.txt
python -m pytest                # should pass before you change anything
```

## Running on several laptops

All laptops must be on the **same Wi-Fi/LAN** and use the same code version (`git pull`).

**1. Coordinator (Windows)**

```bat
cd "D:\SEM 3\EL\EdgeShare-repo"
.venv\Scripts\activate
python main.py coordinator --gui                 # dashboard window
python main.py coordinator                       # or: terminal only
python main.py coordinator --scheduler round_robin
```

It prints the command for the workers, e.g. `python main.py worker 192.168.1.20`.
You can also find the IP with `ipconfig` (IPv4 address of the Wi-Fi adapter).

**Windows Firewall:** the first start shows "Allow Python on networks?". Tick **Private networks** and
allow. If you missed it: Windows Security > Firewall & network protection > Allow an app through
firewall > Python > Private. The Wi-Fi network itself must be set to **Private**
(Settings > Network & internet > Wi-Fi > your network). Needed ports: TCP 5000 (tasks) and UDP 5001
(discovery).

**2. Mac worker**

```bash
cd EdgeShare
source .venv/bin/activate
python3 main.py worker 192.168.1.20               # the coordinator's IP; ID = this Mac's name
python3 main.py worker 192.168.1.20 mac-pavan     # or choose the ID yourself
```

**3. More workers** (any mix of Windows and Mac): run the same command on each laptop. Every worker
needs a **different ID**; by default the computer name is used. A duplicate ID is refused with a
clear message. Workers can join, leave, and reconnect at any time; a worker that loses the
connection retries every 3 seconds.

Automatic discovery (optional): `python main.py worker --discover` waits for the coordinator's UDP
announcement. Campus or hotspot Wi-Fi often blocks this; then pass the IP as above.

**Mac as coordinator:** macOS uses port 5000 for AirPlay Receiver. Turn it off (System Settings >
General > AirDrop & Handoff > AirPlay Receiver) or add `--port 5050` to the coordinator **and**
every worker (`python3 main.py worker <ip> --port 5050`).

**4. Resize a batch of images**

- In the dashboard: **Submit images...** or **Submit folder...**, pick the size, watch the Tasks table.
  Results go to the `output/` folder (**Open output folder**).
- Or from a terminal on the coordinator (or any laptop with `--host <coordinator-ip>`):

```bash
python main.py submit test_images --size 512
python main.py submit photo.jpg --host 192.168.1.20 --out results
```

Each result is `output/<name>_resized.png`. The summary lists which worker resized each image.

## Demo checklist (for the mentor)

1. `python -m pytest` on the coordinator: all tests pass.
2. Start `python main.py coordinator --gui --scheduler round_robin`; point out the IP and scheduler.
3. Start 3 workers on 3 laptops; they appear as **idle** with CPU, RAM, battery, and charging.
4. Submit a folder of ~20 images. Tasks go W1, W2, W3, W1, ... (Round-Robin); workers show **busy**.
5. While it runs, close one worker's terminal (or turn off its Wi-Fi). The dashboard shows it as
   **failed**, the event log shows its task **requeued**, and the batch still completes.
   (Closing the window is detected at once; Wi-Fi off is detected after the 6 s heartbeat timeout.)
6. Restart that worker: it registers again and gets work.
7. Unplug a laptop below 20% battery (or lower `LOW_BATTERY_PERCENT` for the demo): it becomes
   **unavailable** and is skipped while others are free.
8. Restart with `--scheduler resource_weighted` and repeat: the stronger laptop gets more tasks.
9. Open `output/`: every image is there, 512×512.

## Layout

| Folder | Owner | Purpose |
|---|---|---|
| `networking/` | A | protocol, server, client, UDP discovery, batch submit |
| `compute/` | B (scheduler, recovery), C (tasks) | tasks, schedulers, failure handling |
| `monitoring/` | D | psutil resource reporting |
| `benchmark/` | C (runner), D (plots) | experiments and charts (`python -m benchmark.experiment_runner --list`) |
| `gui/` | D | Tkinter dashboard |
| `tests/` | everyone | unit and smoke tests |
| `docs/` | D | report, literature review |

Interfaces between modules, and how scheduling and recovery work, are in `INTERFACES.md`.
Workflow rules are in `CONTRIBUTING.md`.
