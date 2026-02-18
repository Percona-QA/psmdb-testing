#!/usr/bin/env python3
"""
PBM install test runner: boto3 + pyinfra (replaces Molecule + Ansible).

Run from the pyinfra directory (no PYTHONPATH needed):

  python -m run ubuntu-jammy
  python -m run ubuntu-jammy --stage deploy   # only run deploy (instance must exist)
  python -m run ubuntu-jammy --no-destroy     # leave instance up
  python -m run --platforms ubuntu-jammy,centos-7,rhel9   # run in parallel
  python -m run --all-platforms --parallel 4              # all EC2 platforms

Requires: instance config; for EC2 create, SSH private key (default ~/.ssh/id_rsa) or SSH_KEY_PATH / MOLECULE_AWS_PRIVATE_KEY;
  psmdb_to_test, install_repo, VERSION in env for deploy/tests.
"""
from __future__ import annotations

import argparse
import logging
import os
import shutil
import socket
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

_PYINFRA_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _PYINFRA_DIR.parent

from config import get_platform, get_suite, load_platforms, load_suites
from inventory import instance_config_to_pyinfra_inventory


def _work_dir() -> Path:
    """Current runner work dir (per-platform when RUNNER_DIR is set, else .runner)."""
    return Path(os.environ.get("RUNNER_DIR", _PYINFRA_DIR / ".runner"))


def _instance_config() -> Path:
    return _work_dir() / "instance_config.yml"


def _inventory_path() -> Path:
    return _work_dir() / "inventory.py"


def _run_single_platform(
    platform_name: str,
    no_destroy: bool,
    suite: str = "pbm",
    log_file: Path | None = None,
    stage: str | None = None,
    runner_base: str | Path | None = None,
    pyinfra_dir: str | Path | None = None,
) -> int:
    """Run full sequence (or single stage) for one platform in a subprocess with isolated work dir. Returns exit code.
    When runner_base/pyinfra_dir are set (parallel path), use them so the worker does not depend on its cwd/__file__.
    """
    if runner_base is not None and pyinfra_dir is not None:
        base_work = Path(runner_base)
        cwd = Path(pyinfra_dir)
    else:
        base_work = Path(os.environ.get("RUNNER_DIR", _PYINFRA_DIR / ".runner"))
        cwd = _PYINFRA_DIR
    work_dir = base_work / platform_name
    work_dir.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["RUNNER_DIR"] = str(work_dir)
    cmd = [sys.executable, "-m", "run", platform_name, "--suite", suite]
    if stage:
        cmd.extend(["--stage", stage])
    if no_destroy:
        cmd.append("--no-destroy")
    log_path = log_file or (work_dir / "run.log")
    with open(log_path, "w") as log:
        result = subprocess.run(
            cmd,
            cwd=str(cwd),
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    return result.returncode


def _run_platform_wrapper(args: tuple[str, bool, str, str | None, bool, str, str]) -> tuple[str, int]:
    """Wrapper for ProcessPoolExecutor: (platform_name, no_destroy, suite, stage, verbose, runner_base, pyinfra_dir) -> (platform_name, exit_code)."""
    platform_name, no_destroy, suite, stage, verbose, runner_base, pyinfra_dir = args
    code = _run_single_platform(
        platform_name, no_destroy, suite, stage=stage,
        runner_base=runner_base, pyinfra_dir=pyinfra_dir,
    )
    work_dir = Path(runner_base) / platform_name
    if verbose and work_dir.exists():
        log_path = work_dir / "run.log"
        if log_path.exists():
            print(f"\n--- {platform_name} (run.log) ---", flush=True)
            try:
                with open(log_path) as f:
                    print(f.read(), end="", flush=True)
            except OSError:
                pass
            print(f"--- end {platform_name} ---\n", flush=True)
    return (platform_name, code)


def _runner_base_and_dir() -> tuple[str, str]:
    """Return (runner_base, pyinfra_dir) as absolute strings for passing to workers."""
    runner_base = str((_PYINFRA_DIR / ".runner").resolve())
    pyinfra_dir = str(_PYINFRA_DIR.resolve())
    return runner_base, pyinfra_dir


def _preflight_connect(
    timeout: float = 10.0,
    max_wait: float = 320.0,
    interval: float = 10.0,
) -> bool:
    """Wait for SSH (TCP connect) to become reachable, then optionally run ssh-keyscan. Returns True if reachable.
    Retries up to max_wait seconds (default 320s, match playbooks/create.yml wait for SSH)."""
    import time
    instance_config = _instance_config()
    if not instance_config.exists():
        return True
    try:
        import yaml
        with open(instance_config) as f:
            instances = yaml.safe_load(f) or []
        if not instances:
            return True
        first = instances[0]
        addr = first.get("address")
        port = int(first.get("port", 22))
        if not addr:
            return True
        deadline = time.monotonic() + max_wait
        attempt = 0
        while True:
            attempt += 1
            print(f"Preflight: TCP connect to {addr}:{port} (attempt {attempt}, timeout={timeout}s)...", flush=True)
            try:
                sock = socket.create_connection((addr, port), timeout=timeout)
                sock.close()
                break
            except Exception as e:
                if time.monotonic() >= deadline:
                    print(f"Preflight: host unreachable after {max_wait}s: {e}", file=sys.stderr)
                    print("  Check instance is running and SSH is reachable on the configured port. Vagrant: vagrant up && vagrant ssh-config. EC2: check security group and instance state.", file=sys.stderr)
                    return False
                print(f"Preflight: not yet reachable ({e}), retrying in {interval}s...", flush=True)
                time.sleep(interval)
        known_hosts = _work_dir() / "known_hosts"
        keyscan_cmd = ["ssh-keyscan", "-p", str(port), "-T", "5", "-f", "-"]
        try:
            result = subprocess.run(
                keyscan_cmd,
                input=f"[{addr}]:{port}\n".encode(),
                capture_output=True,
                timeout=10,
            )
            if result.returncode == 0 and result.stdout:
                with open(known_hosts, "ab") as f:
                    f.write(result.stdout)
                print("Preflight: known_hosts updated.", flush=True)
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass
        print("Preflight: OK (host reachable).", flush=True)
        return True
    except Exception as e:
        print(f"Preflight: host unreachable: {e}", file=sys.stderr)
        print("  Check instance is running and SSH is reachable on the configured port. Vagrant: vagrant up && vagrant ssh-config. EC2: check security group and instance state.", file=sys.stderr)
        return False


def _run_pyinfra(inventory_path: Path, deploy_path: str | Path) -> int:
    """Run pyinfra with inventory (.py) and a deploy script path (relative to pyinfra/ or absolute)."""
    instance_config = _instance_config()
    if instance_config.exists():
        instance_config_to_pyinfra_inventory(instance_config, inventory_path)
    if not inventory_path.exists():
        print(f"Inventory not found: {inventory_path}", file=sys.stderr)
        return 1
    if not _preflight_connect():
        return 1
    # Optional delay after SSH is reachable so instance can finish boot (avoids Connection reset by peer in testinfra)
    # Set POST_PREFLIGHT_DELAY=120 to match playbook "Wait for boot process to finish" (2 min)
    delay = int(os.environ.get("POST_PREFLIGHT_DELAY", "0"))
    if delay > 0:
        print(f"Preflight: waiting {delay}s for instance to settle...", flush=True)
        time.sleep(delay)
    path = Path(deploy_path)
    if not path.is_absolute():
        path = _PYINFRA_DIR / path
    if not path.exists():
        print(f"Deploy script not found: {path}", file=sys.stderr)
        return 1
    # Use pyinfra_runner to avoid gevent LoopExit when no hosts (e.g. debug-inventory or connect failure).
    cmd = [sys.executable, "-m", "pyinfra_runner", str(inventory_path), str(path), "--sudo", "--yes", "-v", "--debug"]
    print("Running:", " ".join(cmd))
    # PYINFRA_PROGRESS=off: avoid progress spinner thread; its join() on exit/Ctrl-C blocks the gevent hub and can cause LoopExit.
    # stdin=DEVNULL: pyinfra/paramiko never block on terminal (e.g. hidden password prompt).
    env = os.environ.copy()
    env["PYINFRA_PROGRESS"] = "off"
    try:
        return subprocess.run(
            cmd, timeout=600, stdin=subprocess.DEVNULL, env=env, cwd=str(_PYINFRA_DIR)
        ).returncode
    except subprocess.TimeoutExpired:
        print("pyinfra did not complete within 600s (connect or run timeout).", file=sys.stderr)
        return 1


def run_create(platform_name: str, platform_config: dict) -> int:
    provider = platform_config.get("provider", "ec2")
    work_dir = _work_dir()
    work_dir.mkdir(parents=True, exist_ok=True)
    instance_config = _instance_config()
    inventory_path = _inventory_path()
    if provider == "vagrant":
        from cloud import vagrant
        vagrant.create_instances(
            platform_name,
            platform_config,
            key_path="",
            out_config_path=str(instance_config),
        )
    else:
        from cloud import ec2
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
        key_path = (
            os.environ.get("SSH_KEY_PATH")
            or os.environ.get("MOLECULE_AWS_PRIVATE_KEY")
            or str(Path("~/.ssh/id_rsa").expanduser())
        )
        if not Path(key_path).exists():
            print("SSH private key not found: set SSH_KEY_PATH, MOLECULE_AWS_PRIVATE_KEY, or use ~/.ssh/id_rsa.", file=sys.stderr)
            return 1
        ec2.create_instances(
            platform_name,
            platform_config,
            key_path,
            str(instance_config),
        )
    instance_config_to_pyinfra_inventory(instance_config, inventory_path)
    return 0


def run_destroy() -> int:
    instance_config = _instance_config()
    if not instance_config.exists():
        return 0
    with open(instance_config) as f:
        import yaml
        data = yaml.safe_load(f) or []
    first = data[0] if data else {}
    provider = "vagrant" if first.get("region") == "local" or first.get("vagrant_dir") else "ec2"
    if provider == "vagrant":
        from cloud import vagrant
        vagrant.destroy_instances(instance_config)
    else:
        from cloud import ec2
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
        ec2.destroy_instances(instance_config)
    return 0


def run_prepare(platform_config: dict, suite_cfg: dict | None) -> int:
    if not platform_config.get("prepare", True):
        return 0
    if not suite_cfg or not suite_cfg.get("prepare") or suite_cfg.get("prepare", "").lower() == "none":
        return 0
    return _run_pyinfra(_inventory_path(), suite_cfg["prepare"])


def run_deploy(suite_cfg: dict | None) -> int:
    deploy_spec = suite_cfg.get("deploy") if suite_cfg else None
    if not deploy_spec or (isinstance(deploy_spec, str) and deploy_spec.lower() == "none"):
        print("Suite has no deploy script configured.", file=sys.stderr)
        return 1
    inventory_path = _inventory_path()
    deploys = [deploy_spec] if isinstance(deploy_spec, str) else deploy_spec
    for deploy_path in deploys:
        if not deploy_path or str(deploy_path).lower() == "none":
            continue
        if _run_pyinfra(inventory_path, deploy_path) != 0:
            return 1
    return 0


def run_cleanup(suite_cfg: dict | None) -> int:
    cleanup_spec = suite_cfg.get("cleanup") if suite_cfg else None
    if not cleanup_spec or (isinstance(cleanup_spec, str) and cleanup_spec.lower() == "none"):
        return 0
    inventory_path = _inventory_path()
    cleanups = [cleanup_spec] if isinstance(cleanup_spec, str) else cleanup_spec
    for cleanup_path in cleanups:
        if not cleanup_path or str(cleanup_path).lower() == "none":
            continue
        if _run_pyinfra(inventory_path, cleanup_path) != 0:
            return 1
    return 0


def run_verify(suite_cfg: dict | None) -> int:
    os.environ["INSTANCE_CONFIG"] = str(_instance_config())
    if "VERSION" not in os.environ:
        os.environ.setdefault("VERSION", "2.4.0")
    verify_spec = suite_cfg.get("verify") if suite_cfg else None
    if not verify_spec or (isinstance(verify_spec, str) and verify_spec.strip().lower() == "none"):
        print("Suite has no verify command configured.", file=sys.stderr)
        return 1
    # Normalize to list of commands: each item is a list of args.
    # One list of args = one command; list of lists = multiple commands.
    if isinstance(verify_spec, str):
        verify_commands = [verify_spec.strip().split()]
    elif isinstance(verify_spec, list) and verify_spec and isinstance(verify_spec[0], list):
        verify_commands = verify_spec
    else:
        verify_commands = [list(verify_spec)] if isinstance(verify_spec, list) else [[str(verify_spec)]]
    for verify_args in verify_commands:
        if not verify_args:
            continue
        args = [str(a) for a in verify_args]
        if args[0].lower() == "pytest":
            cmd = [sys.executable, "-m", "pytest"] + args[1:]
        else:
            cmd = args
        if subprocess.call(cmd, cwd=str(_PYINFRA_DIR)) != 0:
            return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="PBM install runner (boto3 + pyinfra)")
    parser.add_argument("platform", nargs="?", help="Platform name (e.g. ubuntu-jammy)")
    parser.add_argument(
        "--stage",
        choices=["create", "prepare", "deploy", "verify", "cleanup", "destroy"],
        help="Run only this stage (default: full sequence)",
    )
    parser.add_argument("--no-destroy", action="store_true", help="Do not destroy instance after run")
    parser.add_argument("--list-platforms", action="store_true", help="List platform names and exit")
    parser.add_argument(
        "--platforms",
        metavar="LIST",
        help="Comma-separated platform names to run in parallel",
    )
    parser.add_argument(
        "--all-platforms",
        action="store_true",
        help="Run all EC2 platforms in parallel",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=0,
        metavar="N",
        help="Max concurrent platform runs (default: all when using --platforms/--all-platforms)",
    )
    parser.add_argument(
        "--suite",
        default="pbm",
        metavar="NAME",
        help="Test suite name from suites.yaml (default: pbm)",
    )
    parser.add_argument("--list-suites", action="store_true", help="List suite names and exit")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging (e.g. EC2 instance creation)")
    args = parser.parse_args()

    if getattr(args, "verbose", False):
        os.environ["VERBOSE"] = "1"

    if args.list_platforms:
        for name in load_platforms().keys():
            print(name)
        return 0

    if args.list_suites:
        for name in load_suites().keys():
            print(name)
        return 0

    suite_cfg = get_suite(args.suite)
    if not suite_cfg and args.suite != "pbm":
        print(f"Suite '{args.suite}' not found in suites.yaml. Use --list-suites to list.", file=sys.stderr)
        return 1
    if not suite_cfg:
        suite_cfg = {
            "prepare": "deploys/prepare.py",
            "deploy": "deploys/pbm_install.py",
            "verify": ["pytest", "tests/test_pbm.py", "-v"],
            "cleanup": "deploys/cleanup.py",
        }

    if args.all_platforms or args.platforms:
        all_platforms = load_platforms()
        if args.all_platforms:
            platform_list = [
                name for name, cfg in all_platforms.items()
                if cfg.get("provider") == "ec2"
            ]
            if not platform_list:
                print("No EC2 platforms in platforms.yaml", file=sys.stderr)
                return 1
        else:
            platform_list = [p.strip() for p in args.platforms.split(",") if p.strip()]
            missing = [p for p in platform_list if p not in all_platforms]
            if missing:
                print(f"Unknown platform(s): {missing}", file=sys.stderr)
                return 1
        max_workers = args.parallel or len(platform_list)
        no_destroy = args.no_destroy
        suite = args.suite
        stage = getattr(args, "stage", None)
        verbose = getattr(args, "verbose", False)
        runner_base, pyinfra_dir = _runner_base_and_dir()
        results: list[tuple[str, int]] = []
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_run_platform_wrapper, (name, no_destroy, suite, stage, verbose, runner_base, pyinfra_dir)): name
                for name in platform_list
            }
            for future in as_completed(futures):
                platform_name, code = future.result()
                results.append((platform_name, code))
                status = "PASS" if code == 0 else "FAIL"
                print(f"[{status}] {platform_name} (exit code {code})")
        results.sort(key=lambda x: x[0])
        passed = sum(1 for _, c in results if c == 0)
        failed = [(n, c) for n, c in results if c != 0]
        print("")
        print(f"Results: {passed}/{len(results)} passed")
        if failed:
            print("Failed platforms:", file=sys.stderr)
            for name, code in failed:
                print(f"  {name} (exit {code})  log: .runner/{name}/run.log", file=sys.stderr)
            print("", file=sys.stderr)
            # Dump each failed platform's log so the user sees why it failed
            for name, code in failed:
                log_path = _PYINFRA_DIR / ".runner" / name / "run.log"
                if log_path.exists():
                    print(f"============ .runner/{name}/run.log (exit {code}) ============", file=sys.stderr)
                    try:
                        with open(log_path) as f:
                            print(f.read(), end="", file=sys.stderr)
                    except OSError as e:
                        print(f"(could not read log: {e})", file=sys.stderr)
                    print(f"============ end .runner/{name}/run.log ============", file=sys.stderr)
                else:
                    print(f"(log file not found: {log_path})", file=sys.stderr)
            return 1
        return 0

    # Single-platform run: use per-platform work dir .runner/<platform> so parallel and single use same layout
    if args.platform:
        os.environ["RUNNER_DIR"] = str(_PYINFRA_DIR / ".runner" / args.platform)
        os.environ["PLATFORM_NAME"] = args.platform  # for pytest report (see pyinfra/tests/conftest.py)

    if not args.platform and not args.stage:
        parser.error("platform required (or use --stage with existing instance config)")
    platform_name = args.platform or ""
    platform_config = get_platform(platform_name) if platform_name else {}

    if args.stage:
        if args.stage == "create":
            if not platform_config:
                parser.error("platform required for create")
            return run_create(platform_name, platform_config)
        if args.stage == "destroy":
            return run_destroy()
        if args.stage == "prepare":
            if not _inventory_path().exists():
                parser.error("Run create first or provide instance config at RUNNER_DIR")
            return run_prepare(platform_config or {}, suite_cfg)
        if args.stage in ("deploy", "verify", "cleanup"):
            if not _inventory_path().exists():
                parser.error("Run create first or provide instance config at RUNNER_DIR")
            if args.stage == "deploy":
                return run_deploy(suite_cfg)
            if args.stage == "verify":
                return run_verify(suite_cfg)
            return run_cleanup(suite_cfg)

    if not platform_config:
        parser.error("platform required for full run")
    provider = platform_config.get("provider", "ec2")
    if provider not in ("ec2", "vagrant"):
        print("Only provider=ec2 and provider=vagrant are supported.", file=sys.stderr)
        return 1
    _work_dir().mkdir(parents=True, exist_ok=True)
    if run_create(platform_name, platform_config) != 0:
        return 1
    exit_code = 1
    try:
        if run_prepare(platform_config, suite_cfg) != 0:
            return 1
        if run_deploy(suite_cfg) != 0:
            return 1
        if run_verify(suite_cfg) != 0:
            return 1
        if run_cleanup(suite_cfg) != 0:
            pass
        exit_code = 0
        return 0
    finally:
        if not args.no_destroy:
            run_destroy()


if __name__ == "__main__":
    sys.exit(main())
