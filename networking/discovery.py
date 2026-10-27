"""UDP device discovery (member A). TODO:
- worker: broadcast {"worker_id", "tcp_port"} to config.DISCOVERY_PORT every few seconds
- coordinator: listen on config.DISCOVERY_PORT and add new workers to the registry
- fallback: allow typing the coordinator IP manually (some Wi-Fi blocks broadcast)
"""
