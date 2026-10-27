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

Run the starter demo (two terminals, **server first**):

```bash
python main.py coordinator
python main.py worker 127.0.0.1     # on another laptop use the coordinator's LAN IP
```

Find the coordinator's IP with `ipconfig` (Windows). Allow Python through the firewall on private networks.

## Layout

| Folder | Owner | Purpose |
|---|---|---|
| `networking/` | A | protocol, server, client, UDP discovery |
| `compute/` | B (scheduler, recovery), C (tasks) | tasks, schedulers, failure handling |
| `monitoring/` | D | psutil resource reporting |
| `benchmark/` | C (runner), D (plots) | experiments and charts |
| `gui/` | D | dashboard (last phase) |
| `tests/` | everyone | unit and smoke tests |
| `docs/` | D | report, literature review |

Interfaces between modules are in `INTERFACES.md`. Workflow rules are in `CONTRIBUTING.md`.
