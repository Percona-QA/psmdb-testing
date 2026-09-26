import threading
import time

from cluster import Cluster
from clustersync import Clustersync

import docker

# Failover budget: lease TTL (10s) plus margin for
# new ACTIVE to renew, recover and publish its role.
FAILOVER_TIMEOUT = 30.0
GROUP_NAME = "qa"

class PCSMGroup:
    """A group of PCSM containers sharing one source/target and one lease."""

    def __init__(self, src, dst, n=3, log_level="debug", env_vars=None):
        self.src = src
        self.dst = dst
        self.n = n
        self.log_level = log_level
        self.env_vars = dict(env_vars or {})
        self.env_vars.setdefault("PCSM_RECOVERY_CHECKPOINT_INTERVAL", "1s")
        self.instances = []
        self._next_index = 0
        self._lock = threading.Lock()

    def start(self):
        errors = []
        def spawn():
            try:
                self.add_instance()
            except Exception as e:  # noqa: BLE001
                errors.append(e)
        threads = [threading.Thread(target=spawn) for _ in range(self.n)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        if errors:
            raise errors[0]
        self.wait_for_single_active()
        return self

    def add_instance(self, reset=False):
        with self._lock:
            name = f"csync{self._next_index}"
            self._next_index += 1
        extra_args = f"--group-name={GROUP_NAME}"
        if reset:
            extra_args = f"--reset-state {extra_args}"
        inst = Clustersync(name, self.src, self.dst)
        inst.create(
            log_level=self.log_level,
            env_vars=self.env_vars,
            extra_args=extra_args)
        # Register before the readiness checks, otherwise a container that
        # fails to come up is never destroyed by stop().
        with self._lock:
            self.instances.append(inst)
        if not inst._wait_for_http_server():
            raise AssertionError(
                f"'{name}' HTTP server not ready on startup: {inst.logs()}"
            )
        self._verify_instance_ready(inst)
        return inst

    def _verify_instance_ready(self, inst, timeout=15):
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            if not inst.is_alive:
                raise AssertionError(
                    f"'{inst.name}' exited on startup: {inst.logs()}"
                )
            _, body = inst.request("GET", "/status")
            last = body
            if not isinstance(body, dict):
                time.sleep(0.3)
                continue
            if len(self.instances) <= 1 or "role" in body:
                return
            time.sleep(0.3)
        raise AssertionError(
            f"'{inst.name}' not ready / no HA role within {timeout}s: {last}")

    def stop(self):
        for inst in self.instances:
            try:
                inst.destroy()
            except docker.errors.APIError as e:
                Cluster.log(f"Warning: failed to destroy '{inst.name}': {e}")

    def alive_instances(self):
        return [inst for inst in self.instances if inst.is_alive]

    def active(self, timeout=FAILOVER_TIMEOUT):
        self.wait_for_single_active(timeout)
        for inst in self.alive_instances():
            if inst.role() == "ACTIVE":
                return inst
        raise AssertionError("no ACTIVE instance found")

    def standbys(self):
        return [inst for inst in self.alive_instances() if inst.role() == "STANDBY"]

    def wait_for_single_active(self, timeout=FAILOVER_TIMEOUT):
        deadline = time.time() + timeout
        last = "no response"
        while time.time() < deadline:
            actives = 0
            seen = 0
            alive = self.alive_instances()
            for inst in alive:
                role = inst.role()
                if role is None:
                    continue
                seen += 1
                if role == "ACTIVE":
                    actives += 1
            if alive and seen == len(alive) and actives == 1:
                return
            last = f"actives={actives} seen={seen}/{len(alive)}"
            time.sleep(0.5)
        raise TimeoutError(f"no single ACTIVE within {timeout}s: {last}")

    def kill_active(self):
        active = self.active()
        active.kill()
        return active

    def logs(self):
        chunks = []
        for inst in self.instances:
            chunks.append(f"===== {inst.name} =====\n{inst.logs()}")
        return "\n".join(chunks) if chunks else "No HA instance logs"
