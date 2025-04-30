import docker
import json
import re
import pymongo
import time
from bson.timestamp import Timestamp
from cluster import Cluster

# class Mongolink for creating/manipulating with single mongolink instance
# name = the name of the container
# src = mongodb uri for -source option
# dst = mongodb uri for -target option

class Mongolink:
    def __init__(self, name, src, dst, **kwargs):
        self.name = name
        self.src = src
        self.dst = dst
        self.mlink_image = kwargs.get('mlink_image', "mlink/local")

    @property
    def container(self):
        client = docker.from_env()
        container = client.containers.get(self.name)
        return container

    def create(self, log_level="debug", env_vars=None, extra_args=""):
        try:
            existing_container = self.container
            Cluster.log(f"Removing existing mlink container '{self.name}'...")
            existing_container.remove(force=True)
        except docker.errors.NotFound:
            pass

        Cluster.log(f"Starting mlink to sync from '{self.src}' → '{self.dst}'...")
        client = docker.from_env()

        cmd = f"percona-mongolink --source {self.src} --target {self.dst} --log-level={log_level} --no-color {extra_args}".strip()
        container = client.containers.run(
            image=self.mlink_image,
            name=self.name,
            detach=True,
            network="test",
            environment=env_vars if env_vars is not None else {},
            command=cmd
        )
        Cluster.log(f"Mlink '{self.name}' started successfully")

    def destroy(self):
        try:
            self.container.remove(force=True)
        except docker.errors.NotFound:
            pass

    def start(self, include_namespaces=None, exclude_namespaces=None):
        try:
            payload = {}
            if include_namespaces:
                payload["includeNamespaces"] = include_namespaces
            if exclude_namespaces:
                payload["excludeNamespaces"] = exclude_namespaces
            json_data = json.dumps(payload)
            exec_result = self.container.exec_run(f"curl -s -X POST http://localhost:2242/start -d '{json_data}'")

            response = exec_result.output.decode("utf-8").strip()
            status_code = exec_result.exit_code

            if status_code == 0 and response:
                try:
                    json_response = json.loads(response)

                    if json_response.get("ok") is True:
                        Cluster.log("Sync started successfully")
                        return True

                    elif json_response.get("ok") is False:
                        error_msg = json_response.get("error", "Unknown error")
                        Cluster.log(f"Failed to start sync between src and dst cluster: {error_msg}")
                        return False

                except json.JSONDecodeError:
                    Cluster.log("Received invalid JSON response.")

            Cluster.log("Failed to start sync between src and dst cluster")
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
                return {"success": False, "error": "Failed to execute mlink status command"}

            try:
                json_response = json.loads(response.replace("\n", "").replace("\r", "").strip())
                return {"success": True, "data": json_response}
            except json.JSONDecodeError as e:
                return {"success": False, "error": "Invalid JSON response"}

        except Exception as e:
            return {"success": False, "error": str(e)}

    def restart(self, timeout=60):
        if self.container:
            try:
                self.container.stop()
                time.sleep(2)
                self.container.start()
                time.sleep(2)
                log_stream = self.container.logs(stream=True, follow=True, since=int(time.time()))
                start_time = time.time()
                for line in log_stream:
                    log_line = line.decode('utf-8').strip()
                    if "Checking Recovery Data" in log_line:
                        Cluster.log("Mlink restarted successfully")
                        return True
                    if time.time() - start_time > timeout:
                        Cluster.log(f"Timeout exceeded {timeout} seconds while waiting for mlink to start")
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

    def resume(self):
        try:
            exec_result = self.container.exec_run("curl -s -X POST http://localhost:2242/resume")
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

    def finalize(self, timeout=60, interval=1):
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
                    error_msg = status_response["data"].get("error", "Unknown error")
                    Cluster.log(f"Error: finalization failed, error: {error_msg}")
                    return False
                except json.JSONDecodeError:
                    Cluster.log("Received invalid JSON response.")
                    return False
            Cluster.log("Failed to finalize sync between src and dst cluster")
            return False
        except Exception as e:
            Cluster.log(f"Unexpected error: {e}")
            return False

    def logs(self, tail=50, stream=False):
        try:
            if stream:
                return self.container.logs(stream=True, follow=True)
            raw_logs = self.container.logs().decode("utf-8").strip()
            filtered_lines = [
                line for line in raw_logs.splitlines()
                if "GET /status" not in line
            ]
            last_logs = "\n".join(filtered_lines[-tail:])
            return last_logs if last_logs else "No logs found"

        except docker.errors.NotFound:
            return "Error: mlink container not found."
        except Exception as e:
            return f"Error fetching logs: {e}"

    def check_mlink_errors(self):
        try:
            logs = self.container.logs().decode("utf-8").strip()

        except docker.errors.NotFound:
            return "Error: mlink container not found."
        except Exception as e:
            return f"Error fetching logs: {e}"

        ansi_escape_re = re.compile(r"\x1b\[[0-9;]*m")
        error_pattern = re.compile(r"\b(ERROR|error|ERR|err)\b")
        clean_lines = [ansi_escape_re.sub("", line) for line in logs.split("\n")]
        errors_found = [line for line in clean_lines if error_pattern.search(line)]

        return not bool(errors_found), errors_found

    def wait_for_zero_lag(self, timeout=120, interval=1):
        start_time = time.time()
        last_events_processed = None
        counter = 0

        try:
            src_client = pymongo.MongoClient(self.src)
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
            last_repl_op = status_data.get("lastReplicatedOpTime")
            current_events_processed = status_data.get("eventsProcessed")

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

            if last_ts >= cluster_time:
                Cluster.log(f"Src and dst are in sync: last repl TS {last_ts} >= cluster time {cluster_time}")
                return True

            if cluster_time.time == last_ts.time + 1:
                if last_events_processed is not None and current_events_processed == last_events_processed:
                    counter += 1
                    if counter >= 2:
                        Cluster.log(f"Src and dst are in sync: 1s lag detected but no new events, "
                                f"last repl TS {last_ts}, cluster time {cluster_time}, "
                                f"eventsProcessed={current_events_processed}, last_eventsProcessed={last_events_processed}")
                        return True
                else:
                    counter = 0
            else:
                counter = 0
            last_events_processed = current_events_processed
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
                collection_names = dst_client["percona_mongolink"].list_collection_names()
                if "checkpoints" in collection_names:
                    return True
                time.sleep(1)
            except Exception as e:
                Cluster.log(f"Error: Failed while checking for checkpoints collection: {e}")
                return False
        Cluster.log(f"Error: Timeout exceeded {timeout} seconds while waiting for checkpoints collection to appear")
        return False