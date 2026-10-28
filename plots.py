
import csv
from pathlib import Path
import matplotlib.pyplot as plt

RESULTS_FILE = Path("storage/results/results.csv")


def read_results():
    """Read experiment results from CSV."""
    if not RESULTS_FILE.exists():
        print(f"Results file not found: {RESULTS_FILE}")
        return []

    with RESULTS_FILE.open("r", newline="") as file:
        return list(csv.DictReader(file))


def plot_speedup():
    """Graph 1: Speedup versus number of nodes."""
    rows = read_results()
    grouped = {}

    for row in rows:
        try:
            nodes = int(row["nodes"])
            speedup = float(row["speedup"])

            if nodes > 0:
                grouped.setdefault(nodes, []).append(speedup)
        except (ValueError, KeyError, TypeError):
            continue

    if not grouped:
        print("No valid speedup data found.")
        return

    nodes = sorted(grouped)
    averages = [
        sum(grouped[n]) / len(grouped[n])
        for n in nodes
    ]

    fig, ax = plt.subplots()
    ax.plot(nodes, averages, marker="o")
    ax.set_xlabel("Number of Nodes")
    ax.set_ylabel("Speedup")
    ax.set_title("Speedup vs. Number of Nodes")
    ax.grid(True)
    fig.tight_layout()

    output = RESULTS_FILE.parent / "speedup_vs_nodes.png"
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output)
    print(f"Graph saved to: {output}")

    plt.show()
    plt.close(fig)


def plot_efficiency():
    """Graph 2: Parallel efficiency versus number of nodes."""
    rows = read_results()
    grouped = {}

    for row in rows:
        try:
            nodes = int(row["nodes"])
            efficiency = float(row["efficiency"])

            if nodes > 0:
                grouped.setdefault(nodes, []).append(efficiency)
        except (ValueError, KeyError, TypeError):
            continue

    if not grouped:
        print("No valid efficiency data found.")
        return

    nodes = sorted(grouped)
    averages = [
        sum(grouped[n]) / len(grouped[n])
        for n in nodes
    ]

    fig, ax = plt.subplots()
    ax.plot(nodes, averages, marker="o")
    ax.set_xlabel("Number of Nodes")
    ax.set_ylabel("Parallel Efficiency")
    ax.set_title("Parallel Efficiency vs. Number of Nodes")
    ax.grid(True)
    fig.tight_layout()

    output = RESULTS_FILE.parent / "efficiency_vs_nodes.png"
    fig.savefig(output)
    print(f"Graph saved to: {output}")

    plt.show()
    plt.close(fig)


def plot_time_breakdown():
    """Graph 3: Transfer, processing and merge time."""
    rows = read_results()
    grouped = {}

    for row in rows:
        try:
            nodes = int(row["nodes"])
            transfer = float(row["transfer_s"])
            processing = float(row["processing_s"])
            merge = float(row["merge_s"])

            if nodes > 0:
                grouped.setdefault(nodes, []).append(
                    (transfer, processing, merge)
                )
        except (ValueError, KeyError, TypeError):
            continue

    if not grouped:
        print("No valid time-breakdown data found.")
        return

    nodes = sorted(grouped)
    transfer_times = []
    processing_times = []
    merge_times = []

    for n in nodes:
        runs = grouped[n]
        count = len(runs)

        transfer_times.append(
            sum(run[0] for run in runs) / count
        )
        processing_times.append(
            sum(run[1] for run in runs) / count
        )
        merge_times.append(
            sum(run[2] for run in runs) / count
        )

    fig, ax = plt.subplots()

    ax.bar(nodes, transfer_times, label="Transfer")
    ax.bar(
        nodes,
        processing_times,
        bottom=transfer_times,
        label="Processing"
    )

    bottom_merge = [
        transfer_times[i] + processing_times[i]
        for i in range(len(nodes))
    ]

    ax.bar(
        nodes,
        merge_times,
        bottom=bottom_merge,
        label="Merge"
    )

    ax.set_xlabel("Number of Nodes")
    ax.set_ylabel("Time (seconds)")
    ax.set_title("Execution-Time Breakdown")
    ax.set_xticks(nodes)
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()

    output = RESULTS_FILE.parent / "time_breakdown.png"
    fig.savefig(output)
    print(f"Graph saved to: {output}")

    plt.show()
    plt.close(fig)



def plot_break_even():
    rows = read_results()
    node_times = {}

    for row in rows:
        try:
            nodes = int(row["nodes"])
            total_time = float(row["total_s"])
            node_times.setdefault(nodes, []).append(total_time)
        except (ValueError, KeyError, TypeError):
            continue

    if not node_times:
        print("No valid data for Graph 4.")
        return

    nodes = sorted(node_times)
    times = [
        sum(node_times[n]) / len(node_times[n])
        for n in nodes
    ]

    fig, ax = plt.subplots()
    ax.plot(nodes, times, marker="o")
    ax.set_xlabel("Number of Nodes")
    ax.set_ylabel("Total Time (seconds)")
    ax.set_title("Execution Time vs Number of Nodes")
    ax.set_xticks(nodes)
    ax.grid(True)

    output = RESULTS_FILE.parent / "execution_time_vs_nodes.png"
    fig.tight_layout()
    fig.savefig(output)
    print(f"Graph 4 saved: {output}")
    plt.show()
    plt.close(fig)


if __name__ == "__main__":
    print("\nShowing Graph 1: Speedup")
    plot_speedup()

    print("\nShowing Graph 2: Efficiency")
    plot_efficiency()

    print("\nShowing Graph 3: Time Breakdown")
    plot_time_breakdown()

    print("\nShowing Graph 4: Break-even Task Size")
    plot_break_even()

    print("\nAll graphs completed.")