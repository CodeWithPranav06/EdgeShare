"""Failure handling (member B). TODO: track last heartbeat time per worker; if
older than config.HEARTBEAT_TIMEOUT_S, mark dead and put its unfinished tasks
back in the queue (respecting config.TASK_RETRY_LIMIT). Log recovery time."""
