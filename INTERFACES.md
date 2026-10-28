# Interfaces

Agree on these before coding. To change one, tell the team first and update this file in the same pull request.

| Interface | Shape | Between |
|---|---|---|
| Message framing | `[4-byte header length][JSON header][payload]`; header has `type`, `payload_size` | A with everyone |
| Message types | REGISTER, ACK, STATUS, TASK, RESULT, HEARTBEAT, ERROR | A, B, C, D |
| Task message header | `{type: TASK, task_id, task_type, params, sha256}` + payload bytes | B to worker |
| Result message header | `{type: RESULT, task_id, status (ok/error), processing_s, sha256, reason}` + result bytes | worker to B |
| Coordinator API | `Coordinator.start()`, `send_task(worker_id, task_id, task_type, params, payload)`, `results` queue of `{task_id, worker_id, status, processing_s, payload}`, `alive_workers()` returns `{worker_id: status}`, `on_worker_lost(worker_id)` callback | A to B |
| File transfer | `send_file(sock, path)` / `recv_file(sock, dest_dir)` with SHA-256 check | A |
| Run a task | `run_task(task_type, params, payload) -> (result_bytes, processing_seconds)` in `compute/tasks.py` | C |
| Resource status | `get_status() -> {cpu_count, cpu_percent, free_ram_mb}` in `monitoring/resource_monitor.py` | D with A and B |
| Scheduler | `assign(tasks, workers) -> {task_id: worker_id}` in `compute/task_scheduler.py` | B |
| Run log row | `log_run(row)` in `benchmark/run_experiments.py`; columns in `COLUMNS` | C with D |
| Settings | ports, intervals, timeouts in `config.py` (owned by A) | everyone |
