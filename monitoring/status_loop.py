
import time
from monitoring.resource_monitor import get_status


def monitor_loop():
    try:
        while True:
            status = get_status()

            print("\n--- Worker Status ---")
            print(f"CPU cores: {status['cpu_count']}")
            print(f"CPU usage: {status['cpu_percent']}%")
            print(f"Available RAM: {status['free_ram_mb']} MB")

            time.sleep(3)

    except KeyboardInterrupt:
        print("\nMonitoring stopped.")


if __name__ == "__main__":
    monitor_loop()
