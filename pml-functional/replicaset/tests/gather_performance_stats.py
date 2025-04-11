import time
import urllib3

# Disable HTTPS warnings (self-signed certs)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

URL = "https://localhost/prometheus/api/v1/query"
AUTH = ('admin', 'admin')
VERIFY_SSL = False

CPU_QUERY = '100 - (avg(rate(node_cpu_seconds_total{mode="idle"}[1m])) * 100)'
MEM_QUERY = '((node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes) / node_memory_MemTotal_bytes) * 100'

timestamps = []
cpu_usages = []
mem_usages = []

print("Collecting CPU and Memory usage for 1 minute...")

for _ in range(60):  # run for 60 seconds
    timestamp = int(time.time())

    # Get CPU usage
    cpu_resp = requests.get(URL, params={'query': CPU_QUERY}, auth=AUTH, verify=VERIFY_SSL)
    cpu_val = float(cpu_resp.json()['data']['result'][0]['value'][1])

    # Get Memory usage
    mem_resp = requests.get(URL, params={'query': MEM_QUERY}, auth=AUTH, verify=VERIFY_SSL)
    mem_val = float(mem_resp.json()['data']['result'][0]['value'][1])

    # Store
    timestamps.append(timestamp)
    cpu_usages.append(cpu_val)
    mem_usages.append(mem_val)

    time.sleep(1)

# Convert timestamps to readable format (optional)
from datetime import datetime
readable_times = [datetime.fromtimestamp(ts).strftime('%H:%M:%S') for ts in timestamps]

# Plot both lines
plt.figure(figsize=(12, 6))
plt.plot(readable_times, cpu_usages, label="CPU Usage (%)", linewidth=2)
plt.plot(readable_times, mem_usages, label="Memory Usage (%)", linewidth=2)

plt.title("CPU and Memory Usage Over 1 Minute")
plt.xlabel("Time")
plt.ylabel("Usage (%)")
plt.legend()
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()

plt.savefig("cpu_mem_usage.png")
print("✅ Saved chart as cpu_mem_usage.png")