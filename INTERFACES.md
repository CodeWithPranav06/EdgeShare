# Interfaces

Agree on these before coding. To change one, tell the team first and update this file in the same pull request.

| Interface | Shape | Between |
|---|---|---|
| Message framing | `[4-byte header length][JSON header][payload]`; header has `type`, `payload_size` | A with everyone |
| Message types | REGISTER, ACK, STATUS, TASK, RESULT, HEARTBEAT, ERROR | A, B, C, D |
| Task message header | `{type: TASK, task_id, task_type, params}` + payload bytes | B to worker |
| Result message header | `{type: RESULT, task_id, processing_s}` + result bytes | worker to B |
| Run a task | `run_task(task_type, params, payload) -> (result_bytes, processing_seconds)` in `compute/tasks.py` | C |
| Resource status | `get_status() -> {cpu_count, cpu_percent, free_ram_mb}` in `monitoring/resource_monitor.py` | D with A and B |
| Scheduler | `assign(tasks, workers) -> {task_id: worker_id}` in `compute/task_scheduler.py` | B |
| Run log row | `log_run(row)` in `benchmark/run_experiments.py`; columns in `COLUMNS` | C with D |
| Settings | ports, intervals, timeouts in `config.py` (owned by A) | everyone |
