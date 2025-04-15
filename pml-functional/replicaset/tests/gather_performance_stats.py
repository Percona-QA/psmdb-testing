import json
import matplotlib.pyplot as plt
from datetime import datetime
import argparse

def load_data(json_path):
    with open(json_path, 'r') as f:
        data = json.load(f)
    return data

def plot_cpu_usage(data, output_file=None, show=False):
    results = data.get("data", {}).get("result", [])
    if not results:
        print("No CPU usage data found.")
        return

    plt.figure(figsize=(14, 7))

    for instance_data in results:
        instance = instance_data["metric"].get("instance", "unknown")
        timestamps = [datetime.utcfromtimestamp(point[0]) for point in instance_data["values"]]
        values = [float(point[1]) for point in instance_data["values"]]

        plt.plot(timestamps, values, marker='o', linestyle='-', label=instance)

    plt.title("CPU Usage Over Time")
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot CPU usage from a Prometheus JSON export.")
    parser.add_argument("json_file", help="Path to the Prometheus CPU usage JSON file")
    parser.add_argument("-o", "--output", help="Output file path (e.g. cpu_graph.png)", default="cpu_usage.png")
    parser.add_argument("--show", action="store_true", help="Show graph window as well as saving it")
    args = parser.parse_args()

    cpu_data = load_data(args.json_file)
    plot_cpu_usage(cpu_data, output_file=args.output, show=args.show)