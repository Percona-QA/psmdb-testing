import json
import matplotlib.pyplot as plt
from datetime import datetime

def load_data(json_path):
    """Load JSON data from a file path."""
    with open(json_path, 'r') as f:
        return json.load(f)

def plot_performance_usage(data, output_file=None, show=False):
    """Plot CPU or performance usage from a Prometheus-style JSON dict."""
    results = data.get("data", {}).get("result", [])
    if not results:
        print("No performance usage data found.")
        return

    plt.figure(figsize=(14, 7))

    for instance_data in results:
        instance = instance_data["metric"].get("instance", "unknown")
        timestamps = [datetime.utcfromtimestamp(point[0]) for point in instance_data["values"]]
        values = [float(point[1]) for point in instance_data["values"]]
        plt.plot(timestamps, values, marker='o', linestyle='-', label=instance)

    plt.title("Usage Over Time")
    plt.xlabel("Time (UTC)")
    plt.ylabel("CPU Usage (%)")
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