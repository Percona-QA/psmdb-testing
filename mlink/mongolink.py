import docker
import json
import re
import time
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

    def create(self):
        try:
            existing_container = self.container
            Cluster.log(f"Removing existing mlink container '{self.name}'...")
            existing_container.remove(force=True)
        except docker.errors.NotFound:
            pass

        Cluster.log(f"Starting mlink to sync from '{self.src}' → '{self.dst}'...")
        client = docker.from_env()
        container = client.containers.run(
            image=self.mlink_image,
            name=self.name,
            detach=True,
            network="test",
            command=f"percona-mongolink --source {self.src} --target {self.dst} --log-level=trace --no-color"
        )
        Cluster.log(f"Mlink '{self.name}' started successfully")

    def destroy(self):
        try:
            self.container.remove(force=True)
        except docker.errors.NotFound:
            pass

    def start(self):
        try:
            exec_result = self.container.exec_run("curl -s -X POST http://localhost:2242/start -d '{}'")

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

    def status(self):
        try:
            exec_result = self.container.exec_run("curl -s -X GET http://localhost:2242/status -d '{}'")
            response = exec_result.output.decode("utf-8", errors="ignore").strip()
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
                    if "Starting server" in log_line:
                        Cluster.log("Mlink restarted successfully")
                        return
                    if time.time() - start_time > timeout:
                        Cluster.log(f"Timeout exceeded {timeout} seconds while waiting for mlink to start")
                        return
            except docker.errors.APIError as e:
                Cluster.log(f"Failed to restart container '{self.container.name}': {e}")
        else:
            Cluster.log("No container to restart")

    def finalize(self):
        try:
            exec_result = self.container.exec_run("curl -s -X POST http://localhost:2242/finalize -d '{}'")
            response = exec_result.output.decode("utf-8").strip()
            status_code = exec_result.exit_code

            if status_code == 0 and response:
                try:
                    json_response = json.loads(response)

                    if json_response.get("ok") is True:
                        Cluster.log("Sync finalized successfully")
                        return True

                    elif json_response.get("ok") is False:
                        error_msg = json_response.get("error", "Unknown error")
                        Cluster.log(f"Failed to finalize sync between src and dst cluster: {error_msg}")
                        return False

                except json.JSONDecodeError:
                    Cluster.log("Received invalid JSON response.")

            Cluster.log("Failed to finalize sync between src and dst cluster")
            return False
        except Exception as e:
            Cluster.log(f"Unexpected error: {e}")
            return False

    def logs(self):
        try:
            logs = self.container.logs(tail=50).decode("utf-8").strip()
            return logs if logs else "No logs found."

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

        error_pattern = re.compile(r"\b(ERROR|error|ERR|err)\b")
        errors_found = [line for line in logs.split("\n") if error_pattern.search(line)]

        return not bool(errors_found), errors_found

    def wait_for_zero_lag(self, timeout=200, interval=5):
        start_time = time.time()

        while time.time() - start_time < timeout:
            status_response = self.status()

            if not status_response["success"]:
                Cluster.log(f"Error: Impossible to retrieve status, {status_response['error']}")
                return False

            lag_time = status_response["data"].get("lagTime")
            if lag_time is None:
                Cluster.log("Error: No 'lagTime' field not found in status response")
                return False
            if lag_time <= 1:
                Cluster.log("Src and dst clusters are in sync")
                return True
            time.sleep(interval)

        Cluster.log("Error: Timeout reached while waiting for replication to catch up")
        return False

    def wait_for_repl_stage(self, timeout=200, interval=1):
        start_time = time.time()

        while time.time() - start_time < timeout:
            status_response = self.status()

            if not status_response["success"]:
                Cluster.log(f"Error: Impossible to retrieve status, {status_response['error']}")
                return False

            initial_sync = status_response["data"].get("initialSync")
            if initial_sync is None:
                time.sleep(interval)
                continue
            if "completed" not in initial_sync:
                time.sleep(interval)
                continue
            if initial_sync["completed"]:
                Cluster.log("Initial sync is completed")
                return True
            time.sleep(interval)

        Cluster.log("Error: Timeout reached while waiting for initial sync to complete")
        return False

    def wait_for_checkpoint(self, timeout=120):
        try:
            start_time = time.time()
            log_stream = self.container.logs(stream=True, follow=True)

            for line in log_stream:
                log_line = line.decode('utf-8').strip()
                if "Checkpoint saved" in log_line:
                    return True
                if time.time() - start_time > timeout:
                    print(f"Timeout exceeded {timeout} seconds while waiting for checkpoint")
                    return False
        except docker.errors.APIError as e:
            print(f"Failed to read logs from container '{self.name}': {e}")
            return False