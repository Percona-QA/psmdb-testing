import datetime
import pytest
from time import sleep
from cluster import Cluster

@pytest.fixture(scope="package")
def config():
    return {"_id": "rs1", "members": [{"host": "rs101"}]}


@pytest.fixture(scope="package")
def cluster(config):
    return Cluster(config)


@pytest.fixture(scope="function")
def start_cluster(cluster, request):
    try:
        cluster.destroy()
        cluster.create()
        cluster.setup_pbm()
        yield True
    finally:
        if request.config.getoption("--verbose"):
            cluster.get_logs()
        cluster.destroy(cleanup_backups=True)

@pytest.mark.timeout(300, func_only=True)
def test_backup_profile_validation_PBM_T303(start_cluster, cluster):
    """
    Testing --profile validation for pbm backup
    """
    test_cases = [
        ("''", 'Error: invalid argument "" for "--profile" flag: empty profile name', False),
        ("nil", 'Error: profile "nil" is not found', False),
        (" ", 'Error: invalid argument "" for "--profile" flag: empty profile name', False),
        ("test_profile", '"test_profile","storagePath"', True),
    ]
    failures = []
    cluster.exec_pbm_cli("config --set storage.s3.endpointUrl=http://nginx-minio:15380 --wait")
    cluster.exec_pbm_cli("profile add test_profile /etc/pbm-aws-provider.conf")
    for idx, (command, command_return, should_pass) in enumerate(test_cases):
        try:
            current_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
            cluster.exec_pbm_cli(f"delete-backup --profile=test_profile older-than {current_time}")
            if should_pass:
                assert cluster.make_backup(profile=command), f"Expected command to succeed but it failed: {cluster.cmd_stderr}"
                assert command_return in cluster.cmd_stdout
            else:
                assert cluster.make_backup(profile=command, allow_fail=True) == should_pass, f"Expected command to fail but it succeeded: {cluster.cmd_stdout}"
                assert command_return in cluster.cmd_stderr
        except AssertionError:
            failures.append(f"Case {idx + 1}, executed '{command}', expected '{command_return}', stdout='{cluster.cmd_stdout.strip()}', stderr='{cluster.cmd_stderr.strip()}' ")
    if failures:
        pytest.fail(f"Failed {len(failures)}/{len(test_cases)} tests:\n" + "\n".join(failures))

@pytest.mark.timeout(300, func_only=True)
def test_delete_backup_profile_validation_PBM_T303(start_cluster, cluster):
    """
    Testing --profile validation for pbm delete-backup
    """
    test_cases = [
        ("''", 'Error: invalid argument "" for "--profile" flag: empty profile name', False),
        ('nil', 'Error: profile "nil" is not found', False),
        (" ", 'Error: invalid argument "" for "--profile" flag: empty profile name', False),
        ("test_profile", "test_profile", True),
    ]
    failures = []
    cluster.exec_pbm_cli("config --set storage.s3.endpointUrl=http://nginx-minio:15380 --wait")
    cluster.exec_pbm_cli("profile add test_profile /etc/pbm-aws-provider.conf")
    for idx, (command, expected_command_return, should_pass) in enumerate(test_cases):
        try:
            cluster.make_backup(profile="test_profile")
            current_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
            if should_pass:
                assert cluster.delete_backup(profile=command, **{'older-than': current_time}), f"Expected command to succeed but it failed: {cluster.cmd_stderr}"
                assert expected_command_return not in cluster.cmd_stdout
            else:
                assert cluster.delete_backup(profile=command, **{'older-than': current_time}, allow_fail=True) == should_pass, f"Expected command to fail but it succeeded: {cluster.cmd_stdout}"
                assert expected_command_return in cluster.cmd_stderr
        except AssertionError:
            failures.append(f"Test {idx + 1}, executed '{command}', expected '{expected_command_return}', stdout='{cluster.cmd_stdout.strip()}', stderr='{cluster.cmd_stderr.strip()}' ")
    if failures:
        pytest.fail(f"Failed {len(failures)}/{len(test_cases)} tests\n" + "\n".join(failures))


@pytest.mark.timeout(300, func_only=True)
def test_cleanup_profile_validation_PBM_T303(start_cluster, cluster):
    """
    Testing --profile validation for pbm cleanup
    """
    test_cases = [
        ("''", 'Error: invalid argument "" for "--profile" flag: empty profile name', False),
        ('nil', 'Error: profile "nil" is not found', False),
        (" ", 'Error: invalid argument "" for "--profile" flag: empty profile name', False),
        ("test_profile", "test_profile", True),
    ]
    failures = []
    cluster.exec_pbm_cli("config --set storage.s3.endpointUrl=http://nginx-minio:15380 --wait")
    cluster.exec_pbm_cli("profile add test_profile /etc/pbm-aws-provider.conf")
    main_backup = cluster.make_backup()
    for idx, (command, expected_command_return, should_pass) in enumerate(test_cases):
        try:
            backup1 = cluster.make_backup(profile="test_profile")
            cutoff_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
            backup2 = cluster.make_backup(profile="test_profile")
            result = cluster.exec_pbm_cli(f"cleanup --profile={command} --older-than={cutoff_time} --yes --wait")
            if should_pass:
                assert result.rc == 0, f"Expected command to succeed but it failed: {result.stderr}"
                snapshots = [s['name'] for s in (cluster.get_status()['backups']['snapshot'] or [])]
                assert backup1 not in snapshots, f"Expected '{backup1}' to be deleted after cleanup"
                assert backup2 in snapshots, f"Expected '{backup2}' to still exist after cleanup"
                assert main_backup in snapshots, f"Expected main storage backup '{main_backup}' to be unaffected by profile cleanup"
            else:
                assert result.rc != 0, f"Expected command to fail but it succeeded: {result.stdout}"
                assert expected_command_return in result.stderr
        except AssertionError:
            failures.append(f"Test {idx + 1}, executed '{command}', expected '{expected_command_return}', stdout='{result.stdout.strip()}', stderr='{result.stderr.strip()}' ")
    if failures:
        pytest.fail(f"Failed {len(failures)}/{len(test_cases)} tests\n" + "\n".join(failures))


@pytest.mark.timeout(300, func_only=True)
def test_list_profile_validation_PBM_T303(start_cluster, cluster):
    """
    Testing --profile validation for pbm list
    """
    test_cases = [
        ("''", 'Error: invalid argument "" for "--profile" flag: empty profile name', False),
        ('nil', 'Error: profile "nil" is not found', False),
        (" ", 'Error: invalid argument "" for "--profile" flag: empty profile name', False),
        ("test_profile", "test_profile", True),
    ]
    failures = []
    cluster.exec_pbm_cli("config --set storage.s3.endpointUrl=http://nginx-minio:15380 --wait")
    cluster.exec_pbm_cli("profile add test_profile /etc/pbm-aws-provider.conf")
    main_backup = cluster.make_backup()
    profile_backup = cluster.make_backup(profile="test_profile")
    for idx, (command, expected_command_return, should_pass) in enumerate(test_cases):
        try:
            result = cluster.exec_pbm_cli(f"list --profile={command}")
            if should_pass:
                assert result.rc == 0, f"Expected command to succeed but it failed: {result.stderr}"
                assert profile_backup in result.stdout, f"Expected profile backup '{profile_backup}' in list output"
                assert main_backup not in result.stdout, f"Expected main storage backup '{main_backup}' to be absent from profile list output"
            else:
                assert result.rc != 0, f"Expected command to fail but it succeeded: {result.stdout}"
                assert expected_command_return in result.stderr
        except AssertionError:
            failures.append(f"Test {idx + 1}, executed '{command}', expected '{expected_command_return}', stdout='{result.stdout.strip()}', stderr='{result.stderr.strip()}' ")
    if failures:
        pytest.fail(f"Failed {len(failures)}/{len(test_cases)} tests\n" + "\n".join(failures))


@pytest.mark.timeout(300, func_only=True)
def test_status_profile_validation_PBM_T303(start_cluster, cluster):
    """
    Testing --profile validation for pbm status
    """
    test_cases = [
        ("''", 'Error: invalid argument "" for "--profile" flag: empty profile name', False),
        ('nil', 'Error: profile "nil" is not found', False),
        (" ", 'Error: invalid argument "" for "--profile" flag: empty profile name', False),
        ("test_profile", "test_profile", True),
    ]
    failures = []
    cluster.exec_pbm_cli("config --set storage.s3.endpointUrl=http://nginx-minio:15380 --wait")
    cluster.exec_pbm_cli("profile add test_profile /etc/pbm-aws-provider.conf")
    main_backup = cluster.make_backup()
    profile_backup = cluster.make_backup(profile="test_profile")
    for idx, (command, expected_command_return, should_pass) in enumerate(test_cases):
        try:
            result = cluster.exec_pbm_cli(f"status --profile={command}")
            if should_pass:
                assert result.rc == 0, f"Expected command to succeed but it failed: {result.stderr}"
                assert profile_backup in result.stdout, f"Expected profile backup '{profile_backup}' in status output"
                assert main_backup not in result.stdout, f"Expected main storage backup '{main_backup}' to be absent from profile status output"
            else:
                assert result.rc != 0, f"Expected command to fail but it succeeded: {result.stdout}"
                assert expected_command_return in result.stderr
        except AssertionError:
            failures.append(f"Test {idx + 1}, executed '{command}', expected '{expected_command_return}', stdout='{result.stdout.strip()}', stderr='{result.stderr.strip()}' ")
    if failures:
        pytest.fail(f"Failed {len(failures)}/{len(test_cases)} tests\n" + "\n".join(failures))