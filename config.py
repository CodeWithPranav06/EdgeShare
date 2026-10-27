"""Shared settings. Owned by member A: change only through a pull request."""

COORDINATOR_HOST = "0.0.0.0"      # coordinator listens on all interfaces
COORDINATOR_PORT = 5000           # TCP port for tasks, data, results, heartbeats
DISCOVERY_PORT = 5001             # UDP port for worker announcements

HEARTBEAT_INTERVAL_S = 2          # worker sends a heartbeat this often
HEARTBEAT_TIMEOUT_S = 6           # 3 missed heartbeats = worker is dead
TASK_RETRY_LIMIT = 3              # give up on a task after this many failures

RESULTS_CSV = "storage/results/results.csv"
