"""Shared settings. Owned by member A: change only through a pull request."""

COORDINATOR_HOST = "0.0.0.0"      # coordinator listens on all interfaces
COORDINATOR_PORT = 5000           # TCP port for tasks, data, results, heartbeats
DISCOVERY_PORT = 5001             # UDP port for worker announcements

HEARTBEAT_INTERVAL_S = 2          # worker sends a heartbeat this often
HEARTBEAT_TIMEOUT_S = 6           # 3 missed heartbeats = worker is dead
TASK_RETRY_LIMIT = 3              # give up on a task after this many failures
DISCOVERY_INTERVAL_S = 2          # coordinator announces itself on UDP this often

SCHEDULER = "resource_weighted"   # default strategy: "round_robin" or "resource_weighted"
MAX_TASKS_PER_WORKER = 1          # tasks in flight per worker (workers run one at a time)

# A worker is "unavailable" (skipped while another worker is eligible) when:
MAX_CPU_PERCENT = 95              # its CPU usage is at or above this
MIN_FREE_RAM_MB = 256             # it has less free RAM than this
LOW_BATTERY_PERCENT = 20          # it is on battery (not plugged in) and below this
# Missing battery information (desktops) never makes a worker unavailable.

MAX_HEADER_BYTES = 1024 * 1024    # reject larger JSON headers (corrupt stream)
MAX_PAYLOAD_BYTES = 200 * 1024 * 1024   # reject payloads larger than 200 MB

RESULTS_CSV = "storage/results/results.csv"
OUTPUT_DIR = "output"             # where submitted images' results are saved
