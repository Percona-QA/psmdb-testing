import os
import time
import pytest
import json
import yaml
import testinfra.utils.ansible_runner
from packaging import version

testinfra_hosts = testinfra.utils.ansible_runner.AnsibleRunner(
    os.environ['MOLECULE_INVENTORY_FILE']).get_hosts('all')

MONGO_FEATURES = ['MemoryEngine', 'HotBackup', 'BackupCursorAggregationStage', 'BackupCursorExtendAggregationStage', 'AWSIAM', 'Kerberos', 'LDAP', 'OIDC', 'TDE', 'FIPSMode', 'FCBIS', 'Auditing', 'ProfilingRateLimit', 'LogReduction', 'ngram']

PSMDB_VER = os.environ.get("PSMDB_VERSION")
toolkit = os.environ.get("ENABLE_TOOLKIT")

def get_default_conf(node):
    with node.sudo():
        conf = node.check_output('cat /etc/mongod.conf.default')
        parsed_conf = yaml.safe_load(conf)
        return parsed_conf

def get_current_conf(node):
    with node.sudo():
        conf = node.check_output('cat /etc/mongod.conf')
        parsed_conf = yaml.safe_load(conf)
        return parsed_conf

def copy_default_conf(node):
    with node.sudo():
        node.check_output('cp -r /etc/mongod.conf.default /etc/mongod.conf')

def erase_data(node):
    with node.sudo():
        if node.system_info.distribution == "debian" or node.system_info.distribution == "ubuntu":
            node.check_output('rm -rf /var/lib/mongodb/*')
        else:
            node.check_output('rm -rf /var/lib/mongo/*')

def get_logs(node):
    with node.sudo():
        if node.system_info.distribution == "debian" or node.system_info.distribution == "ubuntu":
            logs=node.check_output('cat /var/log/mongodb/*')
            logs=logs + node.check_output('journalctl --no-pager -u mongod.service')
        else:
            logs=node.check_output('cat /var/log/mongo/*')
            logs=logs + node.check_output('journalctl --no-pager -u mongod.service')
    return logs

def erase_logs(node):
    with node.sudo():
        if node.system_info.distribution == "debian" or node.system_info.distribution == "ubuntu":
            node.check_output('truncate -s0 /var/log/mongodb/mongod.log')
        else:
            node.check_output('truncate -s0 /var/log/mongo/mongod.log')

def stop_mongod(node):
    with node.sudo():
        node.check_output('systemctl stop mongod')
        mongod = node.service("mongod")
        assert not mongod.is_running

def check_db_start(node):
    result = node.run('mongo --eval="printjson(db.serverStatus().ok)"')
    return result.rc == 0 and "1" in result.stdout

def start_mongod(node,check=True):
    with node.sudo():
        result = node.run('systemctl start mongod')
        if check:
            if result.rc != 0:
                logs = get_logs(node)
                print(logs)
            assert result.rc == 0 ,result.stderr
            mongod = node.service("mongod")
            assert mongod.is_running
            for _ in range(20):
              if check_db_start(node):
                 break
              else:
                 time.sleep(0.5)

def restore_defaults(node):
    stop_mongod(node)
    erase_data(node)
    erase_logs(node)
    copy_default_conf(node)
    time.sleep(1)
    start_mongod(node)
    print("\n")

def apply_conf(node,conf,clear_data=False,check=True):
    stop_mongod(node)
    conf_string = yaml.dump(conf, default_flow_style=False)
    print("Applying config:")
    print(conf_string)
    with node.sudo():
        node.check_output("cat > /etc/mongod.conf <<'EOF'\n%s\nEOF" % conf_string)
    if clear_data:
        erase_data(node)
        erase_logs(node)
    time.sleep(1)
    start_mongod(node,check)

def check_hotbackup(node):
    node.check_output('mongo --quiet --eval "for(i=1; i <= 1000; i++) { db.series.insert( { id: i, name: \'series\'+i })}"')
    result = node.check_output('mongo --quiet --eval "db.series.countDocuments({})"')
    assert "1000" in result
    with node.sudo():
        node.check_output('mkdir -p /tmp/backup')
        node.check_output('chown -R mongod /tmp/backup')
    node.check_output('mongo admin --quiet --eval "db.runCommand({createBackup: 1, backupDir: \'/tmp/backup\'})"')
    stop_mongod(node)
    erase_data(node)
    with node.sudo():
        if node.system_info.distribution == "debian" or node.system_info.distribution == "ubuntu":
            node.check_output('cp -r /tmp/backup/* /var/lib/mongodb/')
            node.check_output('chown -R mongod /var/lib/mongodb')
        else:
            node.check_output('cp -r /tmp/backup/* /var/lib/mongo/')
            node.check_output('chown -R mongod /var/lib/mongo')
    start_mongod(node)
    result = node.check_output('mongo --quiet --eval "db.series.countDocuments({})"')
    assert "1000" in result

def test_version_features(host):
    result = host.run("/usr/bin/mongod --version")
    enabled_features = result.stdout.split('"perconaFeatures":')[1].split(']')[0]

    for feature in MONGO_FEATURES:
        assert feature in enabled_features, f'"{feature}" not found in perconaFeatures: {enabled_features}'

def test_binary_symbol_visibility(host):
    binaries = ["/usr/bin/mongod", "/usr/bin/mongos"]

    for binary in binaries:
        readelf_result = host.run(f"readelf -S {binary}")
        file_result = host.run(f"file {binary}")

        assert readelf_result.rc == 0, f"readelf failed for {binary}"
        assert file_result.rc == 0, f"file failed for {binary}"
        file_output = file_result.stdout.lower()

        assert ".symtab" not in readelf_result.stdout, f"{binary} should NOT have .symtab"
        assert ".strtab" not in readelf_result.stdout, f"{binary} should NOT have .strtab"
        assert "not stripped" not in file_output, f"{binary} should be stripped"

def test_version_pt(host):
    if toolkit != "true" :
        pytest.skip("skipping pt tests")
    cmd = "pt-mongodb-summary -f json 2>/dev/null"
    result = host.run(cmd)
    version = json.loads(result.stdout)['HostInfo']['Version']
    print("mongod version is: " + version)
    assert result.rc == 0
    assert PSMDB_VER in version

def test_telemetry(host):
    TOKEN_FILE="/etc/vault/vault.token"
    CA_FILE="/etc/vault/vault.crt"
    FILES=[TOKEN_FILE,CA_FILE]
    for file in FILES:
        with host.sudo():
            host.check_output('chown mongod ' + file)
            host.check_output('chmod 600 ' + file)
    if version.parse(PSMDB_VER) <= version.parse("5.0.27"):
        pytest.skip("This version doesn't support telemetry")
    if version.parse(PSMDB_VER) >= version.parse("6.0.0") and version.parse(PSMDB_VER) <= version.parse("6.0.15"):
        pytest.skip("This version doesn't support telemetry")
    if version.parse(PSMDB_VER) >= version.parse("7.0.0") and version.parse(PSMDB_VER) <= version.parse("7.0.11"):
        pytest.skip("This version doesn't support telemetry")

    file_path = "/usr/local/percona/telemetry_uuid"
    expected_fields = ["instanceId", "PRODUCT_FAMILY_PSMDB"]
    expected_group = "percona-telemetry"

    assert host.file(file_path).exists, f"Telemetry file '{file_path}' does not exist."

    file_content = host.file(file_path).content_string
    for string in expected_fields:
        assert string in file_content, f"Field '{string}' wasn't found in file '{file_path}'."

    if not (host.system_info.distribution.lower() in ["redhat", "centos", 'rhel'] and host.system_info.release == '7'):
        file_group = host.file(file_path).group
        assert file_group == expected_group, f"File '{file_path}' group is '{file_group}', expected group is '{expected_group}'."

    #reconfigure telemetry agent
    with host.sudo():
        assert host.service('percona-telemetry-agent').is_running
        assert host.service('percona-telemetry-agent').is_enabled
        if host.system_info.distribution == "debian" or host.system_info.distribution == "ubuntu":
            host.check_output("sed -E 's|PERCONA_TELEMETRY_URL=(.+)|PERCONA_TELEMETRY_URL=https://check-dev.percona.com/v1/telemetry/GenericReport|' -i /etc/default/percona-telemetry-agent")
            host.check_output("sed -E 's|PERCONA_TELEMETRY_CHECK_INTERVAL=(.+)|PERCONA_TELEMETRY_CHECK_INTERVAL=10|' -i /etc/default/percona-telemetry-agent")
        else:
            host.check_output("sed -E 's|PERCONA_TELEMETRY_URL=(.+)|PERCONA_TELEMETRY_URL=https://check-dev.percona.com/v1/telemetry/GenericReport|' -i /etc/sysconfig/percona-telemetry-agent")
            host.check_output("sed -E 's|PERCONA_TELEMETRY_CHECK_INTERVAL=(.+)|PERCONA_TELEMETRY_CHECK_INTERVAL=10|' -i /etc/sysconfig/percona-telemetry-agent")
        host.check_output('systemctl restart percona-telemetry-agent')
        assert host.service('percona-telemetry-agent').is_running

    #test mongod telemetry
    restore_defaults(host)
    conf = get_default_conf(host)
    conf['setParameter'] = {}
    conf['setParameter']['perconaTelemetryGracePeriod'] = 2
    conf['security'] = {}
    conf['security']['enableEncryption'] = True
    conf['security']['encryptionCipherMode'] = 'AES256-CBC'
    conf['security']['vault'] = {}
    conf['security']['vault']['serverName'] = 'vault'
    conf['security']['vault']['port'] = 8200
    conf['security']['vault']['tokenFile'] = TOKEN_FILE
    conf['security']['vault']['serverCAFile'] = CA_FILE
    conf['security']['vault']['secret'] = 'secret_v2/data/psmdb/test'
    apply_conf(host,conf,True)
    time.sleep(3)
    with host.sudo():
        assert "1"==host.check_output("ls -1 /usr/local/percona/telemetry/psmdb/ | wc -l")
        telemetry_file = host.check_output("ls -1 /usr/local/percona/telemetry/psmdb/").strip()
        telemetry_path = f"/usr/local/percona/telemetry/psmdb/{telemetry_file}"
        telemetry_content = host.check_output(f"cat {telemetry_path}")
        print("\n\n\n\nKEITH TEST\n\n\n\n\n" + telemetry_content + "\n\n\n\nKEITH TEST\n\n\n\n\n")
        try:
            data = json.loads(telemetry_content)
        except Exception as e:
            pytest.fail(f"Telemetry file {telemetry_path} is not valid JSON: {e}\nContent:\n{telemetry_content}")
        assert data.get("tde_key_storage") == "vault", (
            f"'tde_key_storage' is not 'vault' in {telemetry_path}: {data.get('tde_key_storage')}")
        tv = data.get("tde_vault_info")
        assert isinstance(tv, dict), f"'tde_vault_info' section missing or not an object in {telemetry_path}"
        assert tv.get("title") == "HashiCorp Vault API", (
            f"'title' in tde_vault_info is not 'HashiCorp Vault API' in {telemetry_path}: {tv.get('title')}")
        assert tv.get("version"), (f"'version' field missing or empty in tde_vault_info in {telemetry_path}")
        time.sleep(15)
        logs=host.check_output('cat /var/log/percona/telemetry-agent/telemetry-agent.log')
        assert "Sending request to host=check-dev.percona.com." in logs
        assert "0"==host.check_output("ls -1 /usr/local/percona/telemetry/psmdb/ | wc -l")
        assert "1"==host.check_output("ls -1 /usr/local/percona/telemetry/history/ | wc -l")

def test_profiling(host):
    restore_defaults(host)

    #setup profiler
    conf = get_default_conf(host)
    conf['operationProfiling'] = {}
    conf['operationProfiling']['mode'] = 'all'
    conf['operationProfiling']['slowOpThresholdMs'] = 200
    conf['operationProfiling']['rateLimit'] = 100
    conf['auditLog'] = {}
    conf['auditLog']['destination'] = 'file'
    conf['auditLog']['path'] = '/tmp/audit.json'

    #check startup and audit file creation
    apply_conf(host,conf,False)
    assert host.file('/tmp/audit.json').exists

@pytest.mark.parametrize("auth", ['LDAP','GSSAPI','MONGODB-AWS','MONGODB-OIDC'])
def test_auth(host,auth):

    restore_defaults(host)

    #setup config and users
    conf = get_default_conf(host)
    conf['net']['bindIp'] = '0.0.0.0'
    conf['security'] = {}
    conf['security']['authorization'] = "enabled"
    conf['setParameter'] = {}
    if auth == 'LDAP':
        conf['security']['ldap'] = {}
        conf['security']['ldap']['transportSecurity'] = "none"
        conf['security']['ldap']['servers'] = '127.0.0.1:389'
        conf['security']['ldap']['authz'] = {}
        conf['security']['ldap']['authz']['queryTemplate'] = "ou=groups,dc=percona,dc=com??sub?(member={PROVIDED_USER})"
        conf['security']['ldap']['bind'] = {}
        conf['security']['ldap']['bind']['queryUser'] = "cn=admin,dc=percona,dc=com"
        conf['security']['ldap']['bind']['queryPassword'] = "secret"
        conf['setParameter']['saslauthdPath'] = ""
        conf['setParameter']['authenticationMechanisms'] = "PLAIN"
        result = host.check_output('mongo admin --quiet --eval \'db.createRole({role: "cn=testwriters,ou=groups,dc=percona,dc=com", privileges: [], roles: [ "userAdminAnyDatabase", "clusterMonitor", "clusterManager", "clusterAdmin"]})\'')
        print(result)
    if auth == 'GSSAPI':
        conf['setParameter']['authenticationMechanisms'] = 'GSSAPI'
        result = host.check_output('mongo admin --quiet --eval \'db.getSiblingDB("$external").runCommand({createUser:"exttestrw@PERCONATEST.COM",roles: [{role: "userAdminAnyDatabase", db: "admin"}]})\'')
        print(result)
    if auth == 'MONGODB-AWS':
        conf['setParameter']['authenticationMechanisms'] = 'MONGODB-AWS'
        result = host.check_output('mongo admin --quiet --eval \'db.getSiblingDB("$external").runCommand({createUser:"arn:aws:iam::119175775298:role/jenkins-psmdb-slave",roles: [{role: "userAdminAnyDatabase", db: "admin"}]})\'')
        print(result)
    if auth == 'MONGODB-OIDC':
        conf['setParameter']['authenticationMechanisms'] = 'MONGODB-OIDC'
        conf['setParameter']['oidcIdentityProviders'] = json.dumps([
            {
                "issuer": "https://percona.oktapreview.com/oauth2/ausoxk7qawOSbws7w1d7",
                "audience": "0oaoxk03h6o9jFuRZ1d7",
                "authNamePrefix": "okta",
                "clientId": "0oaoxk03h6o9jFuRZ1d7",
                "useAuthorizationClaim": False,
                "supportsHumanFlows": False
            }
        ])
        result = host.check_output('mongo admin --quiet --eval \'db.getSiblingDB("$external").runCommand({createUser:"okta/0oaoxk03h6o9jFuRZ1d7",roles: [{role: "userAdminAnyDatabase", db: "admin"}]})\'')
        print(result)

    #apply config without erasing data
    apply_conf(host,conf,False)

    #check authorization
    if auth == 'LDAP':
        result = host.check_output('mongo -u "cn=exttestrw,ou=people,dc=percona,dc=com" -p "exttestrw9a5S" --authenticationDatabase \'$external\' --authenticationMechanism=PLAIN --quiet --eval "db.runCommand({connectionStatus : 1})"')
        print(result)
        assert 'ok: 1' in result or '"ok" : 1' in result
    if auth == 'GSSAPI':
        with host.sudo():
            hostname = host.check_output('hostname')
            host.check_output('docker exec kerberos sh -c "kadmin.local -q \'addprinc -pw exttestrw exttestrw\'"')
            host.check_output('bash -c "kinit exttestrw <<<\'exttestrw\'"')
            result = host.check_output('mongo -u exttestrw@PERCONATEST.COM --host '+ hostname +' --authenticationMechanism=GSSAPI --authenticationDatabase \'$external\' --quiet --eval "db.runCommand({connectionStatus : 1})"')
            print(result)
            assert 'ok: 1' in result or '"ok" : 1' in result
    if auth == 'MONGODB-AWS':
        hostname = host.check_output('hostname')
        result = host.check_output('mongo --host '+ hostname + ' --authenticationMechanism MONGODB-AWS --authenticationDatabase \'$external\' --quiet --eval "db.runCommand({connectionStatus : 1})"')
        print(result)
        assert 'ok: 1' in result or '"ok" : 1' in result
    if auth == 'MONGODB-OIDC':
        access_token = host.file('/tmp/oidc_access_token').content_string.strip()
        hostname = host.check_output('hostname')
        result = host.check_output(f'/usr/bin/mongo.bcp --host {hostname} --authenticationMechanism MONGODB-OIDC '
            f'--authenticationDatabase \'$external\' '
            f'--oidcAccessToken "{access_token}" '
            '--quiet --eval "db.runCommand({connectionStatus : 1})"')
        print(result)
        assert 'ok: 1' in result or '"ok" : 1' in result

@pytest.mark.parametrize("encryption,cipher",[('KEYFILE','AES256-CBC'),('KEYFILE','AES256-GCM'),('VAULT','AES256-CBC'),('VAULT','AES256-GCM'),('KMIP','AES256-CBC'),('KMIP','AES256-GCM')])
def test_encryption(host,encryption,cipher):
    #fix privileges
    KEY_FILE='/package-testing/scripts/psmdb_encryption/mongodb-keyfile'
    TOKEN_FILE="/etc/vault/vault.token"
    CA_FILE="/etc/vault/ca.crt"
    CA_KMIP_FILE="/etc/kmip/ca-bundle.pem"
    MONGO_PEM_FILE="/etc/kmip/mongod-kmip-client.pem"
    FILES=[KEY_FILE,TOKEN_FILE,CA_FILE,CA_KMIP_FILE,MONGO_PEM_FILE]
    for file in FILES:
        with host.sudo():
            host.check_output('chown mongod ' + file)
            host.check_output('chmod 600 ' + file)

    #setup config
    restore_defaults(host)
    conf = get_default_conf(host)
    conf['net']['bindIp'] = '0.0.0.0'
    conf['security'] = {}
    conf['security']['enableEncryption'] = True
    conf['security']['encryptionCipherMode'] = cipher
    if encryption == "KEYFILE":
        conf['security']['encryptionKeyFile'] = KEY_FILE
    if encryption == "VAULT":
        conf['security']['vault'] = {}
        conf['security']['vault']['serverName'] = 'vault'
        conf['security']['vault']['port'] = 8200
        conf['security']['vault']['tokenFile'] = TOKEN_FILE
        conf['security']['vault']['serverCAFile'] = CA_FILE
        conf['security']['vault']['secret'] = 'secret_v2/data/psmdb/test'
    if encryption == "KMIP":
        conf['security']['kmip'] = {}
        conf['security']['kmip']['serverName'] = '127.0.0.1'
        conf['security']['kmip']['port'] = '5696'
        conf['security']['kmip']['clientCertificateFile'] = MONGO_PEM_FILE
        conf['security']['kmip']['serverCAFile'] = CA_KMIP_FILE

    #erase data and setup config
    apply_conf(host,conf,True)

    #check startup with encryption
    logs = get_logs(host)
    assert "Encryption keys DB is initialized successfully" in logs, logs

    #check hotbackup with encryption
    check_hotbackup(host)

    #check masterkey rotation
    if encryption == "VAULT" or encryption == "KMIP":
        stop_mongod(host)
        erase_logs(host)
        new_conf = get_current_conf(host)
        if encryption == "VAULT":
            new_conf['security']['vault']['rotateMasterKey'] = 'true'
        if encryption == "KMIP":
            new_conf['security']['kmip']['rotateMasterKey'] = 'true'
        apply_conf(host,new_conf,False,False)
        time.sleep(5)
        logs = get_logs(host)
        assert "Rotated master encryption key" in logs, logs
        assert '"Shutting down","attr":{"exitCode":0}}' in logs, logs
        apply_conf(host,conf)
