import pytest
import docker
import testinfra
import time
import json

def setup_pbm_test_environment():
    client = docker.from_env()
    client.containers.run(
        image='replica_member/local',
        name='pbm-cli-validator',
        hostname='pbm-cli-validator',
        detach=True,
        environment={
            'PBM_MONGODB_URI': 'mongodb://localhost:27017/'},
        command="mongod --bind_ip_all --dbpath /data/db --replSet rs0")
    return testinfra.get_host("docker://pbm-cli-validator")

def cleanup_pbm_test_env():
    try:
        client = docker.from_env()
        existing_container = client.containers.get('pbm-cli-validator')
        existing_container.remove(force=True)
    except (docker.errors.NotFound, docker.errors.APIError):
        pass

@pytest.fixture(scope="session")
def setup_pbm_test_env():
    try:
        cleanup_pbm_test_env()
        host = setup_pbm_test_environment()
        max_iterations = 10
        wait_time = 0.02
        for i in range(max_iterations):
            result = host.run("mongosh --eval 'rs.initiate()' --quiet")
            if result.rc == 0 or (result.rc != 0 and 'already initialized' in result.stderr.lower()):
                break
            time.sleep(wait_time)
        else:
            raise RuntimeError("RS wasn't initialized after {} seconds: {}".format(max_iterations, result.stderr))
        result = host.run("pbm config --file /etc/pbm-fs.conf")
        if result.rc != 0:
            raise RuntimeError("Failed to configure PBM: {}".format(result.stderr))
        yield host
    finally:
        cleanup_pbm_test_env()

@pytest.mark.timeout(10, func_only=True)
def test_external_backup_command_PBM_T302(setup_pbm_test_env):
    """Test backup command validation."""
    n = setup_pbm_test_env
    failures = []

    result = n.run("pbm backup --type invalid_type")
    try:
        assert result.rc != 0, "Invalid backup type should fail"
        assert "invalid type value" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Invalid backup type: {e}")
    
    valid_types = ["logical", "physical", "incremental", "external"]
    for backup_type in valid_types:
        result = n.run(f"pbm backup --type {backup_type}")
        try:
            assert result.rc != 0, f"Backup with type {backup_type} can't succeed without agent"
            assert "no available agent" in result.stderr.lower()
        except AssertionError as e:
            failures.append(f"Backup with type {backup_type}: {e}")

    result = n.run("pbm backup --compression invalid_compression")
    try:
        assert result.rc != 0, "Invalid compression type should fail"
        assert "invalid" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Invalid compression type: {e}")
    
    valid_compressions = ["none", "gzip", "snappy", "lz4", "s2", "pgzip", "zstd"]
    for compression in valid_compressions:
        result = n.run(f"pbm backup --compression {compression}")
        try:
            assert result.rc != 0, f"Backup with compression {compression} can't succeed without agent"
            assert "no available agent" in result.stderr.lower()
        except AssertionError as e:
            failures.append(f"Backup with compression {compression}: {e}")
    
    result = n.run("pbm backup --compression-level zzz")
    try:
        assert result.rc != 0, "Invalid compression level should fail"
        assert "invalid" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Invalid compression level: {e}")
    
    result = n.run("pbm backup --num-parallel-collections -1")
    try:
        assert result.rc != 0, "Invalid parallel collections count should fail"
        assert "cannot be negative" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Negative parallel collections: {e}")

    result = n.run("pbm backup --num-parallel-collections 7589236582222")
    try:
        assert result.rc != 0, "Invalid parallel collections count should fail"
        assert "value out of range" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Out of range parallel collections: {e}")

    result = n.run("pbm backup --num-parallel-collections z")
    try:
        assert result.rc != 0, "Invalid parallel collections count should fail"
        assert "invalid syntax" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Invalid syntax parallel collections: {e}")
    
    result = n.run("pbm backup --wait-time invalid_duration")
    try:
        assert result.rc != 0, "Invalid wait time format should fail"
        assert "invalid duration" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Invalid wait time: {e}")

    if failures:
        failure_msg = f"Multiple assertion failures ({len(failures)} total):\n" + "\n".join(f"  - {f}" for f in failures)
        pytest.fail(failure_msg)

@pytest.mark.timeout(10, func_only=True)
def test_external_backup_finish_PBM_T302(setup_pbm_test_env):
    """Test backup-finish command validation."""
    n = setup_pbm_test_env
    failures = []
    
    result = n.run("pbm backup-finish")
    try:
        assert result.rc != 0, "Backup-finish without arguments should fail"
        assert "not found" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Backup-finish without arguments: {e}")

    if failures:
        failure_msg = f"Multiple assertion failures ({len(failures)} total):\n" + "\n".join(f"  - {f}" for f in failures)
        pytest.fail(failure_msg)

@pytest.mark.timeout(10, func_only=True)
def test_external_cancel_backup_PBM_T302(setup_pbm_test_env):
    """Test cancel-backup command validation."""
    n = setup_pbm_test_env
    failures = []
    
    result = n.run("pbm cancel-backup")
    try:
        assert result.rc == 0, "Cancel-backup should succeed"
        assert "backup cancellation has started" in result.stdout.lower()
    except AssertionError as e:
        failures.append(f"Cancel-backup: {e}")

    if failures:
        failure_msg = f"Multiple assertion failures ({len(failures)} total):\n" + "\n".join(f"  - {f}" for f in failures)
        pytest.fail(failure_msg)

@pytest.mark.timeout(10, func_only=True)
def test_external_cleanup_PBM_T302(setup_pbm_test_env):
    """Test cleanup command validation."""
    n = setup_pbm_test_env
    failures = []
    
    result = n.run("pbm cleanup")
    try:
        assert result.rc != 0, "Cleanup should fail"
        assert "invalid format" in result.stderr.lower() or "cleanup" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Cleanup without arguments: {e}")
    
    result = n.run("pbm cleanup --older-than=2025-01-01")
    try:
        assert result.rc == 0, "Cleanup with --older-than should succeed"
        assert "nothing to delete" in result.stdout.lower()
    except AssertionError as e:
        failures.append(f"Cleanup with --older-than: {e}")
    
    result = n.run("pbm cleanup --older-than=invalid-date")
    try:
        assert result.rc != 0, "Cleanup with invalid date should fail"
        assert "expected integer" in result.stderr.lower() or "date" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Cleanup with invalid date: {e}")
    
    result = n.run("pbm cleanup --older-than=2025-01-01 --wait")
    try:
        assert result.rc == 0, "Cleanup with --older-than should succeed"
        assert "nothing to delete" in result.stdout.lower()
    except AssertionError as e:
        failures.append(f"Cleanup with --older-than --wait: {e}")
    
    result = n.run("pbm cleanup --older-than=2025-01-01 --wait-time=invalid-duration")
    try:
        assert result.rc != 0, "Cleanup with invalid wait time should fail"
        assert "invalid duration" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Cleanup with invalid wait time: {e}")
    
    result = n.run("pbm cleanup --older-than=2025-01-01 --yes")
    try:
        assert result.rc == 0, "Cleanup with --older-than should succeed"
        assert "nothing to delete" in result.stdout.lower()
    except AssertionError as e:
        failures.append(f"Cleanup with --older-than --yes: {e}")
    
    result = n.run("pbm cleanup --older-than=2025-01-01 --out=json")
    try:
        assert result.rc == 0, "Cleanup with --older-than should succeed"
        try:
            json.loads(result.stdout)
        except Exception as e:
            raise AssertionError(f"Cleanup output is not valid JSON: {e}\n Output: {result.stdout}")
        assert "nothing to delete" in result.stdout.lower()
    except AssertionError as e:
        failures.append(f"Cleanup with --out=json: {e}")

    if failures:
        failure_msg = f"Multiple assertion failures ({len(failures)} total):\n" + "\n".join(f"  - {f}" for f in failures)
        pytest.fail(failure_msg)

@pytest.mark.timeout(10, func_only=True)
def test_external_delete_backup_PBM_T302(setup_pbm_test_env):
    """Test delete-backup command validation."""
    n = setup_pbm_test_env
    failures = []
    
    result = n.run("pbm delete-backup")
    try:
        assert result.rc != 0, "Delete-backup without arguments should fail"
        assert "either --name or --older-than should be set" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Delete-backup without arguments: {e}")
    
    result = n.run("pbm delete-backup backup_name --older-than=1d")
    try:
        assert result.rc != 0, "Conflicting arguments should fail"
        assert "cannot use" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Conflicting arguments: {e}")
    
    result = n.run("pbm delete-backup -y --older-than=2025-01-01")
    try:
        assert result.rc == 0, "Delete-backup with --older-than should fail"
        assert "waiting for delete to be done" in result.stdout.lower()
    except AssertionError as e:
        failures.append(f"Delete-backup with --older-than: {e}")
    
    result = n.run("pbm delete-backup --older-than=invalid-date")
    try:
        assert result.rc != 0, "Delete-backup with invalid date should fail"
        assert "expected integer" in result.stderr.lower() or "date" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Delete-backup with invalid date: {e}")
    
    result = n.run("pbm delete-backup --type=logical")
    try:
        assert result.rc != 0, "Delete-backup with --type should fail"
        assert "either --name or --older-than should be set" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Delete-backup with --type: {e}")
    
    result = n.run("pbm delete-backup --type=invalid_type")
    try:
        assert result.rc != 0, "Delete-backup with invalid type should fail"
        assert "invalid" in result.stderr.lower() or "type" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Delete-backup with invalid type: {e}")

    result = n.run("pbm delete-backup backup_name --yes")
    try:
        assert result.rc != 0, "Delete-backup should fail"
        assert "not found" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Delete-backup with backup name: {e}")

    result = n.run("pbm delete-backup backup_name --yes --out=json")
    try:
        assert result.rc != 0, "Delete-backup should fail"
        try:
            json.loads(result.stderr)
        except Exception as e:
            raise AssertionError(f"Delete-backup output is not valid JSON: {e}\n Output: {result.stdout}")
        assert "not found" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Delete-backup with --out=json: {e}")
    
    result = n.run("pbm delete-backup -y --older-than=2025-01-01 --out=json")
    try:
        assert result.rc == 0, "Delete-backup with --out=json should succeed"
        try:
            json.loads(result.stdout)
        except Exception as e:
            raise AssertionError(f"Delete-backup output is not valid JSON: {e}\n Output: {result.stdout}")
        assert "not found" in result.stdout.lower()
    except AssertionError as e:
        failures.append(f"Delete-backup with --older-than --out=json: {e}")

    result = n.run("pbm delete-backup --unknown-flag")
    try:
        assert result.rc != 0, "Delete-backup with unknown flag should fail"
        assert "unknown flag" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Delete-backup with unknown flag: {e}")

    if failures:
        failure_msg = f"Multiple assertion failures ({len(failures)} total):\n" + "\n".join(f"  - {f}" for f in failures)
        pytest.fail(failure_msg)

@pytest.mark.timeout(10, func_only=True)
def test_external_delete_pitr_PBM_T302(setup_pbm_test_env):
    """Test delete-pitr command validation."""
    n = setup_pbm_test_env
    failures = []
    result = n.run("pbm delete-pitr")
    try:
        assert result.rc != 0, "Delete-pitr without arguments should fail"
        assert "either --older-than or --all should be set" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Delete-pitr without arguments: {e}")

    result = n.run("pbm delete-pitr --all")
    try:
        assert result.rc == 0, "Delete-pitr with --all should succeed"
        assert "nothing to delete" in result.stdout.lower()
    except AssertionError as e:
        failures.append(f"Delete-pitr with --all: {e}")

    result = n.run("pbm delete-pitr --all --force")
    try:
        assert result.rc == 0, "Delete-pitr with --all --force should succeed"
        assert "nothing to delete" in result.stdout.lower()
    except AssertionError as e:
        failures.append(f"Delete-pitr with --all --force: {e}")

    result = n.run("pbm delete-pitr --older-than=2025-01-01")
    try:
        assert result.rc == 0, "Delete-pitr with --older-than should succeed"
        assert "nothing to delete" in result.stdout.lower()
    except AssertionError as e:
        failures.append(f"Delete-pitr with --older-than: {e}")
    
    result = n.run("pbm delete-pitr --older-than=invalid-date")
    try:
        assert result.rc != 0, "Delete-pitr with invalid date should fail"
        assert "expected integer" in result.stderr.lower() or "date" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Delete-pitr with invalid date: {e}")
    
    result = n.run("pbm delete-pitr --all --wait")
    try:
        assert result.rc == 0, "Delete-pitr with --all --wait should succeed"
        assert "nothing to delete" in result.stdout.lower()
    except AssertionError as e:
        failures.append(f"Delete-pitr with --all --wait: {e}")
    
    result = n.run("pbm delete-pitr --older-than=2025-01-01 --wait-time=5m")
    try:
        assert result.rc == 0, "Delete-pitr with --older-than --wait-time should succeed"
        assert "nothing to delete" in result.stdout.lower()
    except AssertionError as e:
        failures.append(f"Delete-pitr with --older-than --wait-time: {e}")
    
    result = n.run("pbm delete-pitr --wait-time=invalid-duration")
    try:
        assert result.rc != 0, "Delete-pitr with invalid wait time should fail"
        assert "invalid duration" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Delete-pitr with invalid wait time: {e}")
    
    result = n.run("pbm delete-pitr --all --out=json")
    try:
        assert result.rc == 0, "Delete-pitr with --all --out=json should succeed"
        assert "nothing to delete" in result.stdout.lower()
    except AssertionError as e:
        failures.append(f"Delete-pitr with --all --out=json: {e}")
    
    result = n.run("pbm delete-pitr --unknown-flag")
    try:
        assert result.rc != 0, "Delete-pitr with unknown flag should fail"
        assert "unknown flag" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Delete-pitr with unknown flag: {e}")

    if failures:
        failure_msg = f"Multiple assertion failures ({len(failures)} total):\n" + "\n".join(f"  - {f}" for f in failures)
        pytest.fail(failure_msg)

@pytest.mark.timeout(10, func_only=True)
def test_external_describe_backup_PBM_T302(setup_pbm_test_env):
    """Test describe-backup command validation."""
    n = setup_pbm_test_env
    failures = []
    
    result = n.run("pbm describe-backup")
    try:
        assert result.rc != 0, "Describe-backup without arguments should fail"
        assert "not found" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Describe-backup without arguments: {e}")

    if failures:
        failure_msg = f"Multiple assertion failures ({len(failures)} total):\n" + "\n".join(f"  - {f}" for f in failures)
        pytest.fail(failure_msg)

@pytest.mark.timeout(10, func_only=True)
def test_external_describe_restore_PBM_T302(setup_pbm_test_env):
    """Test describe-restore command validation."""
    n = setup_pbm_test_env
    failures = []
    
    result = n.run("pbm describe-restore")
    try:
        assert result.rc != 0, "Describe-restore without arguments should fail"
        assert "not found" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Describe-restore without arguments: {e}")

    if failures:
        failure_msg = f"Multiple assertion failures ({len(failures)} total):\n" + "\n".join(f"  - {f}" for f in failures)
        pytest.fail(failure_msg)

@pytest.mark.timeout(10, func_only=True)
def test_external_diagnostic_PBM_T302(setup_pbm_test_env):
    """Test diagnostic command validation."""
    n = setup_pbm_test_env
    failures = []
    
    result = n.run("pbm diagnostic")
    try:
        assert result.rc != 0, "Diagnostic without flags should fail"
        assert "required flag" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Diagnostic without flags: {e}")

    result = n.run("pbm diagnostic --path=/tmp")
    try:
        assert result.rc != 0, "Diagnostic with only path should fail"
        assert "opid or --name must be provided" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Diagnostic with only path: {e}")
    
    result = n.run("pbm diagnostic --name=invalid_backup --path=/tmp")
    try:
        assert result.rc != 0, "Diagnostic with invalid backup name should fail"
        assert "command not found" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Diagnostic with invalid backup name: {e}")
    
    result = n.run("pbm diagnostic --opid=1111 --path=/tmp")
    try:
        assert result.rc != 0, "Diagnostic with invalid opid should fail"
        assert "invalid command" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Diagnostic with invalid opid: {e}")

    result = n.run("pbm diagnostic --name=backup1 --opid=111111111 --path=/tmp")
    try:
        assert result.rc != 0, "Diagnostic with both name and opid should fail"
        assert "invalid command" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Diagnostic with both name and opid: {e}")

    result = n.run("pbm diagnostic --unknown-flag")
    try:
        assert result.rc != 0, "Diagnostic with unknown flag should fail"
        assert "unknown flag" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Diagnostic with unknown flag: {e}")

    if failures:
        failure_msg = f"Multiple assertion failures ({len(failures)} total):\n" + "\n".join(f"  - {f}" for f in failures)
        pytest.fail(failure_msg)

@pytest.mark.timeout(10, func_only=True)
def test_external_help_PBM_T302(setup_pbm_test_env):
    """Test help command validation."""
    n = setup_pbm_test_env
    failures = []
    
    help_commands = ["--help", "help"]
    for cmd in help_commands:
        result = n.run(f"pbm {cmd}")
        try:
            assert result.rc == 0, f"Help command '{cmd}' should succeed"
            assert len(result.stdout) > 0, f"Help command '{cmd}' should return output"
        except AssertionError as e:
            failures.append(f"Help command '{cmd}': {e}")
    
    commands = ["backup", "restore", "config", "list", "status", "logs"]
    for cmd in commands:
        result = n.run(f"pbm {cmd} --help")
        try:
            assert result.rc == 0, f"Help for {cmd} should succeed"
            assert len(result.stdout) > 0, f"Help for {cmd} should return output"
        except AssertionError as e:
            failures.append(f"Help for {cmd}: {e}")

    if failures:
        failure_msg = f"Multiple assertion failures ({len(failures)} total):\n" + "\n".join(f"  - {f}" for f in failures)
        pytest.fail(failure_msg)

@pytest.mark.timeout(10, func_only=True)
def test_external_list_PBM_T302(setup_pbm_test_env):
    """Test list command validation."""
    n = setup_pbm_test_env
    failures = []
    
    result = n.run("pbm list")
    try:
        assert result.rc == 0, "List should succeed"
    except AssertionError as e:
        failures.append(f"List basic: {e}")
    
    result = n.run("pbm list --full --restore")
    try:
        assert result.rc == 0, "List should succeed"
    except AssertionError as e:
        failures.append(f"List with --full --restore: {e}")

    result = n.run("pbm list --unbacked")
    try:
        assert result.rc == 0, "List should succeed"
    except AssertionError as e:
        failures.append(f"List with --unbacked: {e}")

    result = n.run("pbm list --invalid-flag")
    try:
        assert result.rc != 0, "List with invalid flag should fail"
    except AssertionError as e:
        failures.append(f"List with invalid flag: {e}")

    if failures:
        failure_msg = f"Multiple assertion failures ({len(failures)} total):\n" + "\n".join(f"  - {f}" for f in failures)
        pytest.fail(failure_msg)

@pytest.mark.timeout(10, func_only=True)
def test_external_logs_PBM_T302(setup_pbm_test_env):
    """Test logs command validation."""
    n = setup_pbm_test_env
    failures = []
    
    result = n.run("pbm logs")
    try:
        assert result.rc == 0, "Logs command should succeed"
    except AssertionError as e:
        failures.append(f"Logs basic: {e}")
    
    result = n.run("pbm logs --severity=invalid_severity")
    try:
        assert result.rc != 0, "Invalid severity should fail"
        assert "invalid" in result.stderr.lower() or "severity" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Logs with invalid severity: {e}")

    result = n.run("pbm logs --event=invalid_event")
    try:
        assert result.rc == 0, "Logs with invalid event should succeed"
        assert len(result.stdout) >= 0, "Logs should return output (even if empty)"
    except AssertionError as e:
        failures.append(f"Logs with invalid event: {e}")

    if failures:
        failure_msg = f"Multiple assertion failures ({len(failures)} total):\n" + "\n".join(f"  - {f}" for f in failures)
        pytest.fail(failure_msg)

@pytest.mark.timeout(10, func_only=True)
def test_external_oplog_replay_PBM_T302(setup_pbm_test_env):
    """Test oplog-replay command validation."""
    n = setup_pbm_test_env
    failures = []
    
    result = n.run("pbm oplog-replay")
    try:
        assert result.rc != 0, "Oplog-replay without arguments should fail"
        assert "required" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Oplog-replay without arguments: {e}")

    result = n.run("pbm oplog-replay --start=invalid_time --end=2025-08-01T15:00:00")
    try:
        assert result.rc != 0, "Oplog-replay with invalid time should fail"
    except AssertionError as e:
        failures.append(f"Oplog-replay with invalid time: {e}")

    if failures:
        failure_msg = f"Multiple assertion failures ({len(failures)} total):\n" + "\n".join(f"  - {f}" for f in failures)
        pytest.fail(failure_msg)

@pytest.mark.timeout(10, func_only=True)
def test_external_profile_PBM_T302(setup_pbm_test_env):
    """Test profile command validation."""
    n = setup_pbm_test_env
    failures = []
    
    result = n.run("pbm profile")
    try:
        assert result.rc == 0, "Profile should succeed"
    except AssertionError as e:
        failures.append(f"Profile basic: {e}")
    
    result = n.run("pbm profile add")
    try:
        assert result.rc != 0, "Profile add without arguments should fail"
        assert "error" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Profile add without arguments: {e}")
    
    result = n.run("pbm profile add test_profile")
    try:
        assert result.rc != 0, "Profile add with one argument should fail"
        assert "error" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Profile add with one argument: {e}")

    result = n.run("pbm profile add test_profile /etc/pbm-fs.conf")
    try:
        assert result.rc == 0, "Profile should succeed"
        assert "ok" in result.stdout.lower()
    except AssertionError as e:
        failures.append(f"Profile add with config: {e}")
    
    result = n.run("pbm profile list")
    try:
        assert result.rc == 0, "Profile list should succeed"
    except AssertionError as e:
        failures.append(f"Profile list: {e}")
    
    result = n.run("pbm profile remove")
    try:
        assert result.rc != 0, "Profile remove without arguments should fail"
        assert "error" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Profile remove without arguments: {e}")
    
    result = n.run("pbm profile remove non_existent_profile")
    try:
        assert result.rc != 0, "Profile remove with non-existent profile should fail"
        assert "not found" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Profile remove non-existent: {e}")
    
    result = n.run("pbm profile show")
    try:
        assert result.rc != 0, "Profile show without arguments should fail"
        assert "error" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Profile show without arguments: {e}")
    
    result = n.run("pbm profile show non_existent_profile")
    try:
        assert result.rc != 0, "Profile show with non-existent profile should fail"
        assert "not found" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Profile show non-existent: {e}")
    
    result = n.run("pbm profile sync")
    try:
        assert result.rc != 0, "Profile sync should fail without proper setup"
        assert "error" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Profile sync without setup: {e}")

    result = n.run("pbm profile sync test_profile")
    try:
        assert result.rc == 0, "Profile sync should succeed"
        assert "ok" in result.stdout.lower()
    except AssertionError as e:
        failures.append(f"Profile sync with profile: {e}")

    result = n.run("pbm profile sync --all")
    try:
        assert result.rc == 0, "Profile sync should succeed"
        assert "ok" in result.stdout.lower()
    except AssertionError as e:
        failures.append(f"Profile sync --all: {e}")
    
    result = n.run("pbm profile invalid_command")
    try:
        assert result.rc != 0, "Profile with invalid subcommand should fail"
        assert "unexpected" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Profile invalid command: {e}")

    if failures:
        failure_msg = f"Multiple assertion failures ({len(failures)} total):\n" + "\n".join(f"  - {f}" for f in failures)
        pytest.fail(failure_msg)

@pytest.mark.timeout(10, func_only=True)
def test_external_restore_PBM_T302(setup_pbm_test_env):
    """Test restore command validation."""
    n = setup_pbm_test_env
    failures = []
    
    result = n.run("pbm restore")
    try:
        assert result.rc != 0, "Restore without arguments should fail"
        assert "specify a backup name, --time, or --external" in result.stderr
    except AssertionError as e:
        failures.append(f"Restore without arguments: {e}")

    result = n.run("pbm restore backup_name --time 2025-08-01T15:00:00Z")
    try:
        assert result.rc != 0, "Conflicting arguments should fail"
        assert "backup name and --time cannot be used together" in result.stderr
    except AssertionError as e:
        failures.append(f"Conflicting arguments: {e}")

    result = n.run("pbm restore --time invalid-time-format")
    try:
        assert result.rc != 0, "Invalid time format should fail"
        assert "invalid" in result.stderr.lower() or "time" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Invalid time format: {e}")

    result = n.run("pbm restore --time 2025-08-01T15:00:00")
    try:
        assert result.rc != 0, "Restore with time should fail"
        assert "no base snapshot found" in result.stderr.lower() or "backup" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Restore with time: {e}")

    result = n.run("pbm restore --base-snapshot=backup_name")
    try:
        assert result.rc != 0, "Missing base snapshot should fail"
        assert "specify a backup name" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Missing base snapshot: {e}")

    result = n.run("pbm restore backup_name --ns=test.test --ns-from=test.test")
    try:
        assert result.rc != 0, "Conflicting namespace options should fail"
        assert "ns-to should be specified as the cloning destination" in result.stderr.lower() or "namespace" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Conflicting namespace options: {e}")

    result = n.run("pbm restore backup_name --ns=test.test --ns-from=test.test --ns-to=test.new_test")
    try:
        assert result.rc != 0, "Restore with valid namespace options should fail"
        assert "cloning with selective restore is not possible" in result.stderr.lower() or "backup" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Restore with valid namespace options: {e}")

    result = n.run("pbm restore --external --config=/nonexistent/mongod.conf")
    try:
        assert result.rc != 0, "External restore with invalid config should fail"
        assert "unable to read config file" in result.stderr.lower() or "backup" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"External restore with invalid config: {e}")

    result = n.run("pbm restore --external --ts=invalid-ts-format")
    try:
        assert result.rc != 0, "Invalid ts format should fail"
        assert "invalid" in result.stderr.lower() or "ts" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Invalid ts format: {e}")

    result = n.run("pbm restore backup_name --num-parallel-collections=-1")
    try:
        assert result.rc != 0, "Invalid parallel collections count should fail"
        assert "cannot be negative" in result.stderr.lower() or "no available agent" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Invalid parallel collections count: {e}")

    result = n.run("pbm restore backup_name --num-insertion-workers-per-collection=-1")
    try:
        assert result.rc != 0, "Invalid insertion workers count should fail"
        assert "has to be greater than zero" in result.stderr.lower() or "no available agent" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Invalid insertion workers count: {e}")

    result = n.run("pbm restore backup_name --wait-time=invalid-duration")
    try:
        assert result.rc != 0, "Invalid wait time format should fail"
        assert "invalid duration" in result.stderr.lower() or "no available agent" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Invalid wait time format: {e}")

    result = n.run("pbm restore --unknown-flag")
    try:
        assert result.rc != 0, "Restore with unknown flag should fail"
        assert "unknown flag" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Restore with unknown flag: {e}")

    if failures:
        failure_msg = f"Multiple assertion failures ({len(failures)} total):\n" + "\n".join(f"  - {f}" for f in failures)
        pytest.fail(failure_msg)

@pytest.mark.timeout(10, func_only=True)
def test_external_restore_finish_PBM_T302(setup_pbm_test_env):
    """Test restore-finish command validation."""
    n = setup_pbm_test_env
    failures = []
    
    result = n.run("pbm restore-finish -c /etc/pbm-fs.conf")
    try:
        assert result.rc != 0, "Restore-finish without name should fail"
    except AssertionError as e:
        failures.append(f"Restore-finish: {e}")

    if failures:
        failure_msg = f"Multiple assertion failures ({len(failures)} total):\n" + "\n".join(f"  - {f}" for f in failures)
        pytest.fail(failure_msg)

@pytest.mark.timeout(10, func_only=True)
def test_external_status_PBM_T302(setup_pbm_test_env):
    """Test status command validation."""
    n = setup_pbm_test_env
    failures = []
    
    result = n.run("pbm status")
    try:
        assert result.rc == 0, "Status should succeed"
        assert "cluster" in result.stdout.lower() and "backups" in result.stdout.lower()
    except AssertionError as e:
        failures.append(f"Status basic: {e}")
    
    result = n.run("pbm status --priority --replset-remapping=rs0=rs1 --out=json")
    try:
        assert result.rc == 0, "Status should succeed"
        try:
            json.loads(result.stdout)
        except Exception as e:
            raise AssertionError(f"Cleanup output is not valid JSON: {e}\n Output: {result.stdout}")
        assert "prio_backup" in result.stdout.lower() and "backups" in result.stdout.lower()
    except AssertionError as e:
        failures.append(f"Status with priority and remapping: {e}")

    sections = ["cluster", "pitr", "running", "backups"]
    for section in sections:
        result = n.run(f"pbm status --priority -s={section}")
        try:
            assert result.rc == 0, "Status should succeed"
            assert f"{section}" in result.stdout.lower()
        except AssertionError as e:
            failures.append(f"Status with section {section}: {e}")

    result = n.run("pbm status -s invalid_section")
    try:
        assert result.rc != 0, "Status with invalid section should fail"
        assert "invalid section" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Status with invalid section: {e}")

    result = n.run("pbm status --unknown-flag")
    try:
        assert result.rc != 0, "Status with unknown flag should fail"
        assert "unknown flag" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Status with unknown flag: {e}")

    result = n.run("pbm status --out=invalid_format")
    try:
        assert result.rc != 0, "Status with invalid format should fail"
    except AssertionError as e:
        failures.append(f"Status with invalid format: {e}")

    if failures:
        failure_msg = f"Multiple assertion failures ({len(failures)} total):\n" + "\n".join(f"  - {f}" for f in failures)
        pytest.fail(failure_msg)

@pytest.mark.timeout(10, func_only=True)
def test_external_version_PBM_T302(setup_pbm_test_env):
    """Test version command validation."""
    n = setup_pbm_test_env
    failures = []
    
    result = n.run("pbm version")
    try:
        assert result.rc == 0, "Version command should succeed"
        assert "version" in result.stdout.lower() or "pbm" in result.stdout.lower()
    except AssertionError as e:
        failures.append(f"Version command: {e}")

    if failures:
        failure_msg = f"Multiple assertion failures ({len(failures)} total):\n" + "\n".join(f"  - {f}" for f in failures)
        pytest.fail(failure_msg)

@pytest.mark.timeout(10, func_only=True)
def test_external_unknown_command_PBM_T302(setup_pbm_test_env):
    """Test unknown command validation."""
    n = setup_pbm_test_env
    failures = []
    
    result = n.run("pbm unknown-command")
    try:
        assert result.rc != 0, "Unknown command should fail"
        assert "unknown" in result.stderr.lower() or "command" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Unknown command: {e}")

    if failures:
        failure_msg = f"Multiple assertion failures ({len(failures)} total):\n" + "\n".join(f"  - {f}" for f in failures)
        pytest.fail(failure_msg)

@pytest.mark.timeout(10, func_only=True)
def test_external_config_PBM_T302(setup_pbm_test_env):
    """Test config command validation."""
    n = setup_pbm_test_env
    failures = []
    
    result = n.run("pbm config --file /nonexistent/file.conf")
    try:
        assert result.rc != 0, "Config with missing file should fail"
        assert "unable to get new config" in result.stderr.lower() or "type" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Config with missing file: {e}")
    
    result = n.run("pbm config --set storage.type=invalid_type")
    try:
        assert result.rc != 0, "Config with invalid storage type should fail"
        assert "invalid storage type" in result.stderr.lower()
    except AssertionError as e:
        failures.append(f"Config with missing required fields: {e}")

    if failures:
        failure_msg = f"Multiple assertion failures ({len(failures)} total):\n" + "\n".join(f"  - {f}" for f in failures)
        pytest.fail(failure_msg)   
