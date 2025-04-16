import json
import matplotlib.pyplot as plt
from datetime import datetime

def plot_performance_usage(name, data, output_file=None, show=False):
    """Plot CPU or performance usage from a Prometheus-style JSON dict."""
    results = data.get("data", {}).get("result", [])
    if not results:
        print("No " + name + " performance usage data found.")
        return

    plt.figure(figsize=(14, 7))

    for instance_data in results:
        instance = instance_data["metric"].get("instance", "unknown")
        timestamps = [datetime.utcfromtimestamp(point[0]) for point in instance_data["values"]]
        values = [float(point[1]) for point in instance_data["values"]]
        plt.plot(timestamps, values, marker='o', linestyle='-', label=instance)

    plt.title(name + " Usage Over Time")
    plt.xlabel("Time (UTC)")
    plt.ylabel(name + " Usage (%)")
    plt.grid(True)
    plt.legend(title="Instance")
    plt.xticks(rotation=45)
    plt.tight_layout()

    if output_file:
        plt.savefig(output_file)
        print(f"✅ Graph saved to: {output_file}")

    if show:
        plt.show()

    plt.close()