import time
import docker
import pandas as pd
import matplotlib.pyplot as plt
import pytest
import os
from cluster import Cluster
from threading import Thread, Event

def collect_metrics(event, interval=0.1):
    client = docker.from_env()
    data = {}
    start_time = time.time()

    while not event.is_set():
        containers = client.containers.list()
        for container in containers:
            if "test" in container.name:
                continue
            if container.name not in data:
                data[container.name] = {'time': [], 'cpu': [], 'memory': []}
            stats = container.stats(stream=False)
            cpu_delta = stats['cpu_stats']['cpu_usage']['total_usage'] - stats['precpu_stats']['cpu_usage']['total_usage']
            system_delta = stats['cpu_stats']['system_cpu_usage'] - stats['precpu_stats']['system_cpu_usage']
            cpu_usage = (cpu_delta / system_delta) * len(stats['cpu_stats']['cpu_usage']['percpu_usage']) * 100 if system_delta > 0 else 0
            memory_usage = stats['memory_stats']['usage'] / (1024 * 1024)

            data[container.name]['time'].append(time.time() - start_time)
            data[container.name]['cpu'].append(cpu_usage)
            data[container.name]['memory'].append(memory_usage)

        time.sleep(interval)

    return data

def plot_metrics(data, test_name):
    fig, axes = plt.subplots(2, 1, figsize=(10, 8))
    for container, metrics in data.items():
        df = pd.DataFrame(metrics)
        axes[0].plot(df['time'], df['cpu'], label=container)
        axes[1].plot(df['time'], df['memory'], label=container)

    axes[0].set_title(f'CPU Usage (%) - {test_name}')
    axes[0].set_xlabel('Time (s)')
    axes[0].set_ylabel('CPU (%)')
    axes[0].legend()

    axes[1].set_title(f'Memory Usage (MB) - {test_name}')
    axes[1].set_xlabel('Time (s)')
    axes[1].set_ylabel('Memory (MB)')
    axes[1].legend()

    plt.tight_layout()
    filename= f'graphs/metrics_{test_name}.png'
    plt.savefig(filename)
    os.chmod(filename, 0o666)
    plt.close()

@pytest.fixture(scope="function")
def metrics_collector(request):
    Cluster.log("Start collecting metrics")
    event = Event()
    metrics_data = {}
    thread = Thread(target=lambda: metrics_data.update(collect_metrics(event)))
    thread.start()
    yield metrics_data, request.node.name
    event.set()
    thread.join()
    Cluster.log(metrics_data)
    Cluster.log("Stop collecting metrics")
    plot_metrics(metrics_data, request.node.name)
