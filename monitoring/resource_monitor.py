"""Resource reporting (member D)."""
import psutil


def get_status():
    """Interface: returns {cpu_count, cpu_percent, free_ram_mb}."""
    return {
        "cpu_count": psutil.cpu_count(logical=True),
        "cpu_percent": psutil.cpu_percent(interval=0.2),
        "free_ram_mb": round(psutil.virtual_memory().available / (1024 * 1024)),
    }


if __name__ == "__main__":
    print(get_status())
