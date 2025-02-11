import docker
import json
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
            command=f"mongolink -source {self.src} -target {self.dst} -log-level=debug"
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
            self.container.logs(tail=50).decode("utf-8").strip()
            return logs if logs else "No logs found."

        except docker.errors.NotFound:
            return "Error: mlink container not found."
        except Exception as e:
            return f"Error fetching logs: {e}"
