"""Resource reporting (member D)."""
import psutil


def get_battery():
    """Return (battery_percent, power_plugged); (None, None) when there is no battery
    (desktops) or the platform cannot read it."""
    try:
        battery = psutil.sensors_battery()
    except (AttributeError, NotImplementedError, OSError, RuntimeError):
        return None, None
    if battery is None:
        return None, None
    percent = None if battery.percent is None else round(battery.percent)
    return percent, battery.power_plugged


def get_status():
    """Interface: returns {cpu_count, cpu_percent, free_ram_mb, battery_percent, power_plugged}.
    The battery fields are None when unknown."""
    battery_percent, power_plugged = get_battery()
    return {
        "cpu_count": psutil.cpu_count(logical=True),
        "cpu_percent": psutil.cpu_percent(interval=0.2),
        "free_ram_mb": round(psutil.virtual_memory().available / (1024 * 1024)),
        "battery_percent": battery_percent,
        "power_plugged": power_plugged,
    }


if __name__ == "__main__":
    print(get_status())
