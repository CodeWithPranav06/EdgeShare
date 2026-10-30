# Interfaces

Agree on these before coding. To change one, tell the team first and update this file in the same pull request.

| Interface | Shape | Between |
|---|---|---|
| Message framing | `[4-byte header length][JSON header][payload]`; header has `type`, `payload_size`. Headers over `config.MAX_HEADER_BYTES` or payloads over `config.MAX_PAYLOAD_BYTES` are rejected | A with everyone |
| Message types | REGISTER, ACK, STATUS, TASK, RESULT, HEARTBEAT, ERROR, SUBMIT | A, B, C, D |
| Register | `{type: REGISTER, worker_id, status}` → `ACK`, or `ERROR` if that `worker_id` is already connected | worker to A |
| Heartbeat | `{type: HEARTBEAT, worker_id, status}` every `config.HEARTBEAT_INTERVAL_S`, also while a task runs → `ACK` | worker to A, B |
| Task message header | `{type: TASK, task_id, task_type, params, sha256}` + payload bytes | B to worker |
| Result message header | `{type: RESULT, task_id, processing_s, sha256}` + result bytes | worker to B |
| Task error | `{type: ERROR, task_id, reason}` when a task raises or its data fails the checksum; the task is retried up to `config.TASK_RETRY_LIMIT` | worker to B |
| Submit (one image) | `{type: SUBMIT, task_id, task_type, params, name, sha256}` + image → `ACK`, then `{type: RESULT, task_id, worker_id, processing_s, sha256}` + result, or `{type: ERROR, task_id, reason}` | submitter / GUI to A |
| Checksums | `sha256` = SHA-256 hex of the payload; check with `verify_payload(header, payload)` in `networking/protocol.py` | A with everyone |
| Discovery | coordinator sends UDP `{"service": "edgeshare", "tcp_port"}` to `config.DISCOVERY_PORT`; workers use `find_coordinator()` | A |
| Run a task | `run_task(task_type, params, payload) -> (result_bytes, processing_seconds)` in `compute/tasks.py` | C |
| Resource status | `get_status() -> {cpu_count, cpu_percent, free_ram_mb, battery_percent, power_plugged}` in `monitoring/resource_monitor.py`; battery fields are `None` when unknown | D with A and B |
| Scheduler | `assign(tasks, workers, state=None) -> {task_id: worker_id}` in `compute/task_scheduler.py`; `state` is a dict the coordinator keeps between calls | B |
| Eligibility | `eligibility(status) -> (ok, reason)`: not ok when CPU ≥ `MAX_CPU_PERCENT`, free RAM < `MIN_FREE_RAM_MB`, or on battery below `LOW_BATTERY_PERCENT`. Unknown values never exclude a worker | B |
| Coordinator state | `TaskCoordinator.snapshot()` → `{strategy, uptime_s, workers, tasks, counts, events, event_count}` (plain data) | B with D |
| Run log row | `log_run(row)` in `benchmark/run_experiments.py`; columns in `COLUMNS` | C with D |
| Settings | ports, intervals, timeouts, thresholds in `config.py` (owned by A) | everyone |

## How scheduling works

- Workers **pull**: after each message from a worker (heartbeat, result, error) the server asks
  `TaskCoordinator.next_task(worker_id)`. A worker has at most `MAX_TASKS_PER_WORKER` (1) task in flight.
- Queued tasks are planned with the chosen strategy over the **candidate** workers: live workers that
  pass `eligibility()`, or every live worker if none passes (better slow than stuck).
- **round_robin**: W1, W2, W3, W1, ... The rotation continues between calls, so tasks that arrive one at a
  time still rotate. Example, 6 tasks and 3 workers: T1→W1, T2→W2, T3→W3, T4→W1, T5→W2, T6→W3.
- **resource_weighted**: weight = `cpu_count × (100 − cpu_percent) / 100 × free_ram_mb`; each task goes to
  the worker with the highest `weight / (load + 1)`, where load counts tasks it already has.
- Plans are redone when a worker joins, fails, or becomes (un)available. Status arrives with every
  heartbeat, and a worker without a heartbeat for `HEARTBEAT_TIMEOUT_S` is removed, so stale status is
  never used for long.

## Task states and recovery

`pending → assigned → running → completed`, or `failed` after `TASK_RETRY_LIMIT` failures.

- A worker fails when its connection drops (detected immediately) or after `HEARTBEAT_TIMEOUT_S` without a
  heartbeat (one monitor thread checks every `HEARTBEAT_INTERVAL_S`). Its unfinished tasks go back to
  `pending` with one more retry counted, or to `failed` at the limit.
- A long task is not a failure: the worker's heartbeat thread keeps running while it computes.
- Results are accepted only from a worker that was given the task, only once, and only if the checksum
  matches. A worker declared dead may still deliver a task it finished (first valid result wins).
- This is **at-least-once** execution: a task may run twice after a failure; only one result is kept.
  Results are returned in submission order (`results_in_task_order()`).
