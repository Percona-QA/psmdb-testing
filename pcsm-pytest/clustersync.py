import docker
import json
import re
import pymongo
import time
from bson.timestamp import Timestamp
from cluster import Cluster

# class Clustersync for creating/manipulating with single clustersync instance
# name = the name of the container
# src = mongodb uri for -source option
# dst = mongodb uri for -target option

class Clustersync:
    def __init__(self, name, src, dst, **kwargs):
        self.name = name
        self.src = src
        self.dst = dst
        self.src_internal = kwargs.get('src_internal')
        self.csync_image = kwargs.get('csync_image', "csync/local")
        self.cmd_stdout = ""
        self.cmd_stderr = ""

    @property
    def container(self):
        client = docker.from_env()
        container = client.containers.get(self.name)
        return container

    def create(self, log_level="debug", env_vars=None, extra_args=""):
        try:
            existing_container = self.container
            Cluster.log(f"Removing existing csync container '{self.name}'...")
            existing_container.remove(force=True)
        except docker.errors.NotFound:
            pass

        Cluster.log(f"Starting csync to sync from '{self.src}' → '{self.dst}'...")
        client = docker.from_env()
        env_vars = env_vars or {}
        cmd = f"pcsm --source {self.src} --target {self.dst} --log-level={log_level} --log-no-color {extra_args}".strip()
        if "PCSM_LOG_LEVEL" in env_vars:
            cmd = f"pcsm --source {self.src} --target {self.dst} --log-no-color {extra_args}".strip()
        client.containers.run(
            image=self.csync_image,
            name=self.name,
            detach=True,
            network="test",
            environment=env_vars if env_vars is not None else {},
            command=cmd
        )
        Cluster.log("Csync process started successfully")

    def destroy(self):
        try:
            self.container.remove(force=True)
        except docker.errors.NotFound:
            pass

    def _wait_for_http_server(self, timeout=30):
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                result = self.container.exec_run("curl -s -m 2 http://localhost:2242/status -d '{}'")
                if result.exit_code == 0:
                    return True
            except Exception:
                pass
            time.sleep(0.1)
        Cluster.log(f"HTTP server not ready after {timeout} seconds")
        return False

    def start(self, mode="http", raw_args=None):
        try:
            if not self._wait_for_http_server():
                Cluster.log("Failed to connect to HTTP server - server may not be ready")
                return False
            if mode == "cli":
                Cluster.log("Using CLI Mode")
                cmd = "pcsm start " + " ".join(raw_args) if raw_args else "pcsm start"
                exec_result = self.container.exec_run(cmd, demux=True)
                stdout, stderr = exec_result.output
                self.cmd_stdout = stdout.decode("utf-8", errors="replace") if stdout else ""
                self.cmd_stderr = stderr.decode("utf-8", errors="replace") if stderr else ""
                print(f"COMMAND: {cmd}")
                print(f"STDOUT: {self.cmd_stdout}")
                print(f"STDERR: {self.cmd_stderr}")
            else:
                Cluster.log("Using API Mode")
                if raw_args is None:
                    payload = {}
                else:
                    payload = raw_args
                cmd = f"curl -s -X POST http://localhost:2242/start -H 'Content-Type: application/json' -d '{json.dumps(payload)}'"
                print(f"COMMAND: {cmd}")
                exec_result = self.container.exec_run(cmd)
                self.cmd_stdout = exec_result.output.decode("utf-8", errors="replace")
                print(f"STDOUT: {self.cmd_stdout}")
                print(f"STDERR: {self.cmd_stderr}")

            status_code = exec_result.exit_code

            if status_code == 0 and self.cmd_stdout:
                try:
                    json_response = json.loads(self.cmd_stdout)

                    if json_response.get("ok") is True:
                        Cluster.log("Synchronization between src and dst started successfully")
                        return True

                    elif json_response.get("ok") is False:
                        error_msg = json_response.get("error", "Unknown error")
                        Cluster.log(f"Failed to start synchronization between src and dst cluster: {error_msg}")
                        return False

                except json.JSONDecodeError:
                    Cluster.log("Received invalid JSON response.")

            Cluster.log("Failed to start synchronization between src and dst cluster")
            return False
        except Exception as e:
            Cluster.log(f"Unexpected error: {e}")
            return False

    def status(self, timeout=45):
        try:
            exec_result = self.container.exec_run(f"curl -m {timeout} -s -X GET http://localhost:2242/status -d '{{}}'")
            response = exec_result.output.decode("utf-8", errors="ignore").strip()
            Cluster.log(response)
            status_code = exec_result.exit_code

            if status_code != 0 or not response:
                return {"success": False, "error": "Failed to execute csync status command"}

            try:
                json_response = json.loads(response.replace("\n", "").replace("\r", "").strip())
                return {"success": True, "data": json_response}
            except json.JSONDecodeError:
                return {"success": False, "error": "Invalid JSON response"}

        except Exception as e:
            return {"success": False, "error": str(e)}

    def metrics(self, timeout=45):
        try:
            exec_result = self.container.exec_run(f"curl -m {timeout} -s -X GET http://localhost:2242/metrics")
            response = exec_result.output.decode("utf-8", errors="ignore").strip()
            status_code = exec_result.exit_code
            if status_code != 0 or not response:
                return {"success": False, "error": "Failed to execute csync metrics command"}
            metrics_data = {}
            for line in response.splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                try:
                    key_value = line.split(None, 1)
                    if len(key_value) == 2:
                        key, value = key_value
                        metrics_data[key] = float(value)
                except ValueError:
                    continue
            return {"success": True, "data": metrics_data}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def restart(self, timeout=60, reset=False):
        if self.container:
            try:
                start_log_time = int(time.time())
                self.container.stop()
                if reset and self.dst:
                    try:
                        src_client = pymongo.MongoClient(self.dst)
                        result = src_client["percona_clustersync_mongodb"].checkpoints.delete_many({})
                        assert result.acknowledged, "Checkpoint deletion not acknowledged"
                        result = src_client["percona_clustersync_mongodb"].heartbeats.delete_many({})
                        assert result.acknowledged, "Heartbeat deletion not acknowledged"
                        Cluster.log("Checkpoints and heartbeats deleted successfully")
                    except Exception as e:
                        Cluster.log(f"Warning: Failed to reset PCSM state: {e}. Continuing with restart anyway.")
                self.container.start()
                log_stream = self.container.logs(stream=True, follow=True, since=start_log_time)
                start_time = time.time()
                for line in log_stream:
                    log_line = line.decode('utf-8').strip()
                    if "Checking Recovery Data" in log_line:
                        Cluster.log("Csync restarted successfully")
                        return True
                    if time.time() - start_time > timeout:
                        Cluster.log(f"Timeout exceeded {timeout} seconds while waiting for csync to start")
                        return False
            except docker.errors.APIError as e:
                Cluster.log(f"Failed to restart container '{self.container.name}': {e}")
                return False
        else:
            Cluster.log("No container to restart")
            return False

    def pause(self):
        try:
            exec_result = self.container.exec_run("curl -s -X POST http://localhost:2242/pause")
            response = exec_result.output.decode("utf-8").strip()
            status_code = exec_result.exit_code
            if status_code == 0 and response:
                try:
                    json_response = json.loads(response)
                    if json_response.get("ok") is True:
                        Cluster.log("Sync paused successfully")
                        return True
                    elif json_response.get("ok") is False:
                        error_msg = json_response.get("error", "Unknown error")
                        Cluster.log(f"Failed to pause sync: {error_msg}")
                        return False
                except json.JSONDecodeError:
                    Cluster.log("Received invalid JSON response.")
            Cluster.log("Failed to pause sync")
            return False
        except Exception as e:
            Cluster.log(f"Unexpected error: {e}")
            return False

    def resume(self, from_failure=False):
        try:
            if from_failure:
                cmd = 'curl -s -d \'{"fromFailure": true}\' -X POST http://localhost:2242/resume'
            else:
                cmd = 'curl -s -X POST http://localhost:2242/resume'
            exec_result = self.container.exec_run(cmd)
            response = exec_result.output.decode("utf-8").strip()
            status_code = exec_result.exit_code
            if status_code == 0 and response:
                try:
                    json_response = json.loads(response)
                    if json_response.get("ok") is True:
                        Cluster.log("Sync resumed successfully")
                        return True
                    elif json_response.get("ok") is False:
                        error_msg = json_response.get("error", "Unknown error")
                        Cluster.log(f"Failed to resume sync: {error_msg}")
                        return False
                except json.JSONDecodeError:
                    Cluster.log("Received invalid JSON response.")
            Cluster.log("Failed to resume sync")
            return False
        except Exception as e:
            Cluster.log(f"Unexpected error: {e}")
            return False

    def finalize(self, timeout=240, interval=1):
        try:
            exec_result = self.container.exec_run("curl -s -X POST http://localhost:2242/finalize -d '{}'")
            response = exec_result.output.decode("utf-8").strip()
            status_code = exec_result.exit_code

            if status_code == 0 and response:
                try:
                    json_response = json.loads(response)

                    if json_response.get("ok") is False:
                        error_msg = json_response.get("error", "Unknown error")
                        Cluster.log(f"Failed to finalize sync between src and dst cluster: {error_msg}")
                        return False

                    start_time = time.time()
                    while time.time() - start_time < timeout:
                        status_response = self.status()
                        if status_response.get("success") and status_response["data"].get("ok") \
                                                and status_response["data"].get("state") == "finalized":
                            Cluster.log("Sync finalized successfully")
                            return True
                        time.sleep(interval)

                    # Timeout reached, provide more descriptive error message
                    if status_response.get("success") and status_response["data"]:
                        current_state = status_response["data"].get("state", "unknown")
                        error_msg = status_response["data"].get("error")
                        if error_msg:
                            Cluster.log(f"Error: finalization failed, error: {error_msg}")
                        else:
                            lag_time = status_response["data"].get("lagTimeSeconds", "N/A")
                            events_read = status_response["data"].get("eventsRead", "N/A")
                            events_applied = status_response["data"].get("eventsApplied", "N/A")
                            Cluster.log(f"Error: finalization timeout after {timeout}s, state stuck in '{current_state}'. "
                                      f"lagTimeSeconds={lag_time}, eventsRead={events_read}, eventsApplied={events_applied}")
                    else:
                        Cluster.log(f"Error: finalization failed, could not retrieve status: {status_response.get('error', 'Unknown error')}")
                    return False
                except json.JSONDecodeError:
                    Cluster.log("Received invalid JSON response.")
                    return False
            Cluster.log("Failed to finalize sync between src and dst cluster")
            return False
        except Exception as e:
            Cluster.log(f"Unexpected error: {e}")
            return False

    def logs(self, tail=50, filter=True, stream=False):
        try:
            if stream:
                return self.container.logs(stream=True, follow=True)
            raw_logs = self.container.logs().decode("utf-8").strip()
            lines = raw_logs.splitlines()
            if filter:
                lines = [line for line in lines if "GET /status" not in line]
            last_logs = "\n".join(lines[-tail:])
            return last_logs if last_logs else "No logs found"

        except docker.errors.NotFound:
            return "Error: csync container not found."
        except Exception as e:
            return f"Error fetching logs: {e}"

    def check_csync_errors(self):
        try:
            logs = self.container.logs().decode("utf-8")

        except docker.errors.NotFound:
            return False, ["Error: csync container not found."]
        except Exception as e:
            return False, [f"Error fetching logs: {e}"]

        ansi_escape_re = re.compile(r"\x1b\[[0-9;]*m")
        error_pattern = re.compile(r"\b(?:ERROR|ERR|error|err)\b")
        def error_lines():
            for line in logs.splitlines():
                clean = ansi_escape_re.sub("", line)
                if error_pattern.search(clean):
                    yield clean
        errors_found = list(error_lines())
        return not bool(errors_found), errors_found

    def wait_for_zero_lag(self, timeout=240, interval=1):
        start_time = time.time()
        last_events_read = None
        last_events_applied = None
        stability_counter = 0

        try:
            src_client = pymongo.MongoClient(self.src_internal or self.src)
        except Exception as e:
            Cluster.log(f"Error: Failed to connect to source MongoDB URI: {e}")
            return False

        while time.time() - start_time < timeout:
            try:
                ping_result = src_client.admin.command("ping")
                cluster_time = ping_result.get("$clusterTime", {}).get("clusterTime")

                if cluster_time is None:
                    Cluster.log("Error: Failed to get clusterTime from source")
                    return False
            except Exception as e:
                Cluster.log(f"Error: Failed to retrieve clusterTime from source: {e}")
                return False

            status_response = self.status()
            if not status_response.get("success") or not status_response["data"].get("ok"):
                error_msg = status_response["data"].get("error", "Unknown error")
                Cluster.log(f"Error: replication failed, error: {error_msg}")
                return False

            status_data = status_response["data"]
            last_repl_op = status_data.get("lastReplicatedOpTime", {}).get("ts")
            current_events_read = status_data.get("eventsRead")
            current_events_applied = status_data.get("eventsApplied")

            if last_repl_op is None:
                Cluster.log("Error: No 'lastReplicatedOpTime' field found in status response")
                return False

            try:
                parts = str(last_repl_op).split(".")
                if len(parts) != 2:
                    Cluster.log("Error: Invalid lastReplicatedOpTime format, expected 'seconds.increment'")
                    return False
                last_ts = Timestamp(int(parts[0]), int(parts[1]))
            except Exception as e:
                Cluster.log(f"Error: Failed to parse lastReplicatedOpTime: {e}")
                return False

            events_stable = (last_events_read is not None and last_events_applied is not None and
                           current_events_read == last_events_read and
                           current_events_applied == last_events_applied)
            if events_stable:
                stability_counter += 1
            else:
                stability_counter = 0
            is_caught_up = cluster_time.time <= last_ts.time + 2

            if is_caught_up and events_stable and stability_counter >= 5:
                lag_info = "0 lag" if last_ts >= cluster_time else "1-2s lag"
                Cluster.log(f"Src and dst are in sync ({lag_info}): last repl TS {last_ts} >= cluster time {cluster_time}, "
                          f"eventsRead={current_events_read}, eventsApplied={current_events_applied}, "
                          f"stability={stability_counter} checks")
                return True
            last_events_read = current_events_read
            last_events_applied = current_events_applied
            time.sleep(interval)

        Cluster.log("Error: Timeout reached while waiting for replication to catch up")
        return False

    def wait_for_repl_stage(self, timeout=60, interval=1, stable_duration=2):
        start_time = time.time()

        while time.time() - start_time < timeout:
            status_response = self.status()
            if not status_response.get("success"):
                error_msg = status_response.get("error", "Unknown error")
                Cluster.log(f"Error: replication failed, error: {error_msg}")
                return False

            data = status_response.get("data")
            if not data or not data.get("ok"):
                error_msg = data.get("error", "Unknown error") if data else "No data received"
                Cluster.log(f"Error: replication failed, error: {error_msg}")
                return False

            initial_sync = status_response["data"].get("initialSync")
            if initial_sync is None:
                time.sleep(interval)
                continue
            if "completed" not in initial_sync:
                time.sleep(interval)
                continue
            if initial_sync["completed"]:
                stable_start = time.time()
                while time.time() - stable_start < stable_duration:
                    stable_status = self.status()
                    if not stable_status["success"]:
                        Cluster.log(f"Error: Impossible to retrieve status, {stable_status['error']}")
                        return False

                    state = stable_status["data"].get("state")
                    if state != "running":
                        return False
                    time.sleep(0.5)
                Cluster.log("Initial sync is completed")
                return True
            time.sleep(interval)

        Cluster.log("Error: Timeout reached while waiting for initial sync to complete")
        return False

    def wait_for_checkpoint(self, timeout=120):
        try:
            dst_client = pymongo.MongoClient(self.dst)
        except Exception as e:
            Cluster.log(f"Error: Failed to connect to dst MongoDB URI: {e}")
            return False
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                collection_names = dst_client["percona_clustersync_mongodb"].list_collection_names()
                if "checkpoints" in collection_names:
                    doc = dst_client["percona_clustersync_mongodb"]["checkpoints"].find_one({
                        "_id": "pcsm",
                        "data.clone.finishTime": {"$exists": True}
                    })
                    if doc:
                        return True
                time.sleep(1)
            except Exception as e:
                Cluster.log(f"Error: Failed while checking for checkpoints collection: {e}")
                return False
        Cluster.log(f"Error: Timeout exceeded {timeout} seconds while waiting for checkpoints collection to appear")
        return False