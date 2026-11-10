"""Smoke-test the OpenSSL 3 PSMDB tarball on one OS image.

CI runs this once per OS and passes that image in OS_IMAGE. From this directory:

    OS_IMAGE=oraclelinux:9 docker compose run --rm --build test

The default OS image is oraclelinux:9. Pass --client-log, or set CLIENT_LOG=1,
to print each client URI and server response.
"""

import io
import json
import logging
import os
import re
import shlex
import shutil
import socket
import ssl
import tarfile
import tempfile
import time
import uuid
import urllib.parse
import urllib.request
from pathlib import Path

import docker
import pytest
import pymongo
from bson import json_util

ROOT = Path(__file__).resolve().parent
NETWORK = "psmdb-os"
OS_IMAGE = os.environ.get("OS_IMAGE", "oraclelinux:9")
RUNTIME = "/opt/psmdb-runtime-libs"
BASE_ENV = {
    "LD_LIBRARY_PATH": RUNTIME + "/opt/mongo/openssl3/lib:" + RUNTIME + "/lib",
    "OPENSSL_CONF": RUNTIME + "/openssl-fips.cnf",
    "OPENSSL_MODULES": RUNTIME + "/lib/ossl-modules",
}
X509_USER = "emailAddress=pbm@percona.com,CN=pbm,OU=client,O=Percona,L=SanFrancisco,ST=California,C=US"
LDAP_USER = "cn=pbm,ou=users,dc=example,dc=org"
LOGGER = logging.getLogger("psmdb.client")
LOGGER.setLevel(logging.INFO)


def client_log_enabled():
    return os.environ.get("CLIENT_LOG", "").lower() in ("1", "true", "yes")

STOP_SCRIPT = """
i=0
while [ "$i" -lt 20 ]; do
  alive=0
  for d in /proc/[0-9]*; do
    exe=$(readlink "$d/exe" 2>/dev/null || true)
    case "$exe" in
      */mongod|*/mongos)
        kill "${d#/proc/}" 2>/dev/null || true
        alive=1
        ;;
    esac
  done
  if [ "$alive" -eq 0 ]; then
    exit 0
  fi
  i=$((i + 1))
  sleep 1
done
exit 1
"""


def image_names(os_image):
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "-", os_image).strip("-")
    return "psmdb-os:" + safe, "psmdb-os-" + safe.replace(".", "-").replace("_", "-")


def with_fips(binary, args):
    cmd = [
        binary,
        "--tlsFIPSMode",
        "--bind_ip_all",
        "--tlsMode", "requireTLS",
        "--tlsCAFile", "/etc/x509/ca.crt",
        "--tlsCertificateKeyFile", "/etc/x509/psmdb.pem",
        "--tlsClusterFile", "/etc/x509/pbm.pem",
    ]
    if binary == "mongod":
        cmd += ["--wiredTigerCacheSizeGB", "0.25"]
    if "authenticationMechanisms" not in " ".join(args):
        cmd += ["--setParameter", "authenticationMechanisms=SCRAM-SHA-256"]
    return cmd + args


class OsNode:
    def __init__(self, container, workdir, client_host):
        self.container = container
        self.workdir = workdir
        self.client_host = client_host
        self.log_path = "/var/log/mongodb-smoke.log"
        self.container.reload()
        self.host = self.container.name

    def exec(self, script, env=None, check=True):
        merged = dict(BASE_ENV)
        if env:
            merged.update(env)
        result = self.container.exec_run(["/bin/sh", "-c", script], environment=merged)
        output = result.output.decode("utf-8", errors="replace")
        if check and result.exit_code != 0:
            raise AssertionError(script + "\n" + output)
        return output

    def stop(self):
        self.exec(STOP_SCRIPT, check=False)
        self.exec(
            "i=0; while [ \"$i\" -lt 20 ]; do "
            "if ! awk 'NR>1 {split($2,a,\":\"); p=tolower(a[2]); if (p==\"6989\" || p==\"698b\") found=1} "
            "END {exit !found}' /proc/net/tcp /proc/net/tcp6 2>/dev/null; then exit 0; fi; "
            "i=$((i+1)); sleep 1; done; exit 1",
            check=False,
        )

    def spawn(self, binary, args, log_path, env=None):
        self.log_path = log_path
        self.exec("rm -f " + shlex.quote(log_path))
        script = "exec " + " ".join(shlex.quote(part) for part in with_fips(binary, args)) + " > " + shlex.quote(log_path) + " 2>&1"
        merged = dict(BASE_ENV)
        if env:
            merged.update(env)
        self.container.exec_run(["/bin/sh", "-c", script], environment=merged, detach=True)

    def start(self, binary, args, env=None, fresh=True, log_path="/var/log/mongodb-smoke.log"):
        self.stop()
        if fresh:
            self.exec("rm -rf /var/lib/mongo && mkdir -p /var/lib/mongo")
        self.spawn(binary, args, log_path, env=env)

    def read_log(self):
        return self.exec("cat " + shlex.quote(self.log_path) + " 2>/dev/null || true", check=False)

    def server_running(self):
        output = self.exec(
            "alive=0; for d in /proc/[0-9]*; do "
            "exe=$(readlink \"$d/exe\" 2>/dev/null || true); "
            "case \"$exe\" in */mongod|*/mongos) alive=1;; esac; done; "
            "printf '%s' \"$alive\"",
            check=False,
        )
        return output.strip() == "1"

    def wait_exit(self, timeout=90):
        deadline = time.time() + timeout
        while time.time() < deadline:
            log = self.read_log()
            if "Fatal assertion" in log or '"s":"F"' in log:
                raise AssertionError(log)
            if not self.server_running() and "Now exiting" in log:
                return log
            time.sleep(0.5)
        raise AssertionError("server did not exit\n" + self.read_log())

    def wait_ready(self, timeout=90):
        deadline = time.time() + timeout
        while time.time() < deadline:
            log = self.read_log()
            if "Waiting for connections" in log:
                return log
            if "Fatal assertion" in log or '"s":"F"' in log:
                raise AssertionError(log)
            time.sleep(0.5)
        raise AssertionError("server did not accept connections\n" + self.read_log())

    def client(self, port=27017, log_commands=None, **kwargs):
        if log_commands is None:
            log_commands = client_log_enabled()
        kwargs.setdefault("directConnection", True)
        kwargs.setdefault("serverSelectionTimeoutMS", 5000)
        kwargs.setdefault("tls", True)
        kwargs.setdefault("tlsCAFile", str(Path(self.workdir) / "x509" / "ca.crt"))
        kwargs.setdefault(
            "tlsCertificateKeyFile",
            str(Path(self.workdir) / "x509" / (self.client_host + ".pem")),
        )
        uri = mongo_uri(self.host, port, kwargs)
        return LoggedClient(pymongo.MongoClient(self.host, port, **kwargs), uri, log_commands)

    def wait_primary(self, port=27017, timeout=60):
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            client = self.client(port=port, log_commands=False)
            try:
                doc = client.admin.command("hello")
                if doc.get("isWritablePrimary"):
                    log_exchange(client.uri, doc)
                    return doc
                last = doc
            except Exception as exc:
                last = exc
            finally:
                client.close()
            time.sleep(0.5)
        raise AssertionError("no primary: " + str(last) + "\n" + self.read_log())


def mongo_uri(host, port, kwargs):
    user = kwargs.get("username")
    password = kwargs.get("password")
    auth = ""
    if user is not None:
        auth = urllib.parse.quote(str(user), safe="")
        if password is not None:
            auth += ":" + urllib.parse.quote(str(password), safe="")
        auth += "@"
    options = []
    for key, value in kwargs.items():
        if key in ("username", "password"):
            continue
        if isinstance(value, bool):
            text = "true" if value else "false"
        elif isinstance(value, (str, int)):
            text = str(value)
        else:
            continue
        options.append(urllib.parse.quote(str(key), safe="") + "=" + urllib.parse.quote(text, safe=""))
    uri = "mongodb://%s%s:%s/" % (auth, host, port)
    if options:
        uri += "?" + "&".join(options)
    return uri


def log_exchange(uri, response):
    if not client_log_enabled():
        return
    if not isinstance(response, str):
        response = json_util.dumps(response)
    LOGGER.info("client uri: %s", uri)
    LOGGER.info("server response: %s", response)


class LoggedDatabase:
    def __init__(self, database, uri, log_commands):
        self._database = database
        self._uri = uri
        self._log_commands = log_commands

    def command(self, *args, **kwargs):
        try:
            result = self._database.command(*args, **kwargs)
        except Exception as exc:
            if self._log_commands:
                log_exchange(self._uri, str(exc))
            raise
        if self._log_commands:
            log_exchange(self._uri, result)
        return result

    def __getitem__(self, name):
        return LoggedDatabase(self._database[name], self._uri, self._log_commands)

    def __getattr__(self, name):
        return getattr(self._database, name)


class LoggedClient:
    def __init__(self, client, uri, log_commands):
        self._client = client
        self.uri = uri
        self._log_commands = log_commands

    def close(self):
        self._client.close()

    def __getitem__(self, name):
        return LoggedDatabase(self._client[name], self.uri, self._log_commands)

    def __getattr__(self, name):
        value = getattr(self._client, name)
        if type(value).__name__ == "Database":
            return LoggedDatabase(value, self.uri, self._log_commands)
        return value


def put_bytes(container, dest, payload, mode):
    name = os.path.basename(dest)
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        info = tarfile.TarInfo(name=name)
        info.size = len(payload)
        info.mode = mode
        tar.addfile(info, io.BytesIO(payload))
    buf.seek(0)
    container.put_archive(os.path.dirname(dest), buf.getvalue())


def issue_certs(docker_client, server_host, client_hosts):
    client_hosts = list(dict.fromkeys(client_hosts))
    for hostname in [server_host, *client_hosts]:
        if not re.fullmatch(r"[A-Za-z0-9.-]+", hostname):
            raise AssertionError("unsupported hostname: " + hostname)
    if server_host in client_hosts:
        raise AssertionError("server and client hostnames overlap: " + server_host)
    commands = [
        "set -eu",
        "cd /root/easy-rsa/easyrsa3",
        "mkdir -p /etc/issued",
        "./easyrsa --req-ou=server --days=90 --subject-alt-name=DNS:%s --batch build-server-full %s nopass" % (server_host, server_host),
        "cat pki/issued/%s.crt pki/private/%s.key > /etc/issued/server.pem" % (server_host, server_host),
    ]
    for hostname in client_hosts:
        commands.append(
            "./easyrsa --req-ou=client --days=90 --subject-alt-name=DNS:%s --batch build-client-full %s nopass" % (hostname, hostname)
        )
        commands.append(
            "cat pki/issued/%s.crt pki/private/%s.key > /etc/issued/%s.pem" % (hostname, hostname, hostname)
        )
    holder = docker_client.containers.create("easyrsa/psmdb", entrypoint=["/bin/sh", "-c", "; ".join(commands)])
    try:
        holder.start()
        status = holder.wait(timeout=120)
        if status["StatusCode"] != 0:
            raise AssertionError(holder.logs().decode("utf-8", errors="replace"))
        stream, _ = holder.get_archive("/etc/issued")
        pems = {}
        with tarfile.open(fileobj=io.BytesIO(b"".join(stream))) as tar:
            for member in tar.getmembers():
                if member.isfile():
                    pems[os.path.basename(member.name)] = tar.extractfile(member).read()
        return pems
    finally:
        holder.remove(force=True)


def copy_from_image(client, image, src, container, dest_dir):
    holder = client.containers.create(image)
    try:
        stream, _ = holder.get_archive(src)
        container.put_archive(dest_dir, b"".join(stream))
    finally:
        holder.remove()


def extract_tree(container, src, dest):
    stream, _ = container.get_archive(src)
    with tarfile.open(fileobj=io.BytesIO(b"".join(stream))) as tar:
        tar.extractall(dest)


@pytest.fixture(scope="session")
def psmdb_image():
    client = docker.from_env()
    tag, _ = image_names(OS_IMAGE)
    print("Building " + tag + " FROM " + OS_IMAGE, flush=True)
    client.images.build(path=str(ROOT), dockerfile="Dockerfile", buildargs={"OS_IMAGE": OS_IMAGE}, tag=tag, rm=True)
    return tag


@pytest.fixture(scope="function")
def node(psmdb_image):
    client = docker.from_env()
    name = image_names(OS_IMAGE)[1] + "-" + uuid.uuid4().hex[:8]
    try:
        client.containers.get(name).remove(force=True)
    except docker.errors.NotFound:
        pass
    container = client.containers.run(psmdb_image, name=name, hostname=name, detach=True, network=NETWORK)
    workdir = tempfile.mkdtemp(prefix="psmdb-os-")
    try:
        put_bytes(container, "/etc/keyfile", (ROOT / "keyfile").read_bytes(), 0o400)
        put_bytes(container, "/etc/krb5.conf", (ROOT / "conf" / "krb5.conf").read_bytes(), 0o644)
        copy_from_image(client, "easyrsa/psmdb", "/etc/x509", container, "/etc")
        copy_from_image(client, "easyrsa/psmdb", "/etc/cosmian", container, "/etc")
        client_host = socket.gethostname()
        holder = OsNode(container, workdir, client_host)
        holder.exec(
            "mkdir -p /etc/pykmip /etc/vault && "
            "cp /etc/cosmian/mongod.pem /etc/pykmip/mongod.pem && "
            "cp /etc/x509/ca.crt /etc/pykmip/ca.crt && "
            "printf '%s\\n' vaulttoken > /etc/vault/token && "
            "chmod 400 /etc/keyfile /etc/pykmip/mongod.pem /etc/vault/token /etc/x509/*.pem && "
            "chmod 644 /etc/x509/ca.crt /etc/pykmip/ca.crt"
        )
        pems = issue_certs(client, name, [client_host, "kerberos"])
        put_bytes(container, "/etc/x509/psmdb.pem", pems["server.pem"], 0o400)
        for hostname in (client_host, "kerberos"):
            put_bytes(container, "/etc/x509/" + hostname + ".pem", pems[hostname + ".pem"], 0o644)
        extract_tree(container, "/etc/x509", workdir)
        yield holder
    finally:
        container.remove(force=True)
        shutil.rmtree(workdir, ignore_errors=True)


@pytest.mark.timeout(180, func_only=True)
def test_version(node):
    mongod = node.exec("mongod --version")
    mongos = node.exec("mongos --version")
    assert "OpenSSL 3.5.9" in mongod
    assert "OpenSSL 3.5.9" in mongos


@pytest.mark.timeout(180, func_only=True)
def test_standalone_fips_scram(node):
    node.start("mongod", ["--dbpath", "/var/lib/mongo", "--port", "27017"])
    log = node.wait_ready()
    assert "FIPS 140 mode activated" in log
    client = node.client()
    client.admin.command("createUser", "root", pwd="root", roles=["root"])
    client.close()
    node.start("mongod", ["--dbpath", "/var/lib/mongo", "--port", "27017", "--auth"], fresh=False)
    node.wait_ready()
    authed = node.client(username="root", password="root", authSource="admin")
    authed.admin.command("ping")
    authed.close()


@pytest.mark.timeout(180, func_only=True)
def test_replset_keyfile(node):
    node.start("mongod", [
        "--dbpath", "/var/lib/mongo", "--port", "27017",
        "--replSet", "rs1", "--keyFile", "/etc/keyfile", "--transitionToAuth",
    ])
    node.wait_ready()
    client = node.client()
    client.admin.command("replSetInitiate", {
        "_id": "rs1",
        "members": [{"_id": 0, "host": node.host + ":27017"}],
    })
    hello = node.wait_primary()
    assert hello["setName"] == "rs1"
    client.close()


@pytest.mark.timeout(180, func_only=True)
def test_mongos(node):
    node.start("mongod", [
        "--dbpath", "/var/lib/mongo", "--port", "27019",
        "--configsvr", "--replSet", "rscfg", "--keyFile", "/etc/keyfile", "--transitionToAuth",
    ], log_path="/var/log/mongodb-cfg.log")
    node.wait_ready()
    cfg = node.client(port=27019)
    cfg.admin.command("replSetInitiate", {
        "_id": "rscfg",
        "members": [{"_id": 0, "host": node.host + ":27019"}],
    })
    node.wait_primary(port=27019)
    cfg.close()
    node.spawn("mongos", [
        "--configdb", "rscfg/" + node.host + ":27019", "--port", "27017",
        "--keyFile", "/etc/keyfile", "--transitionToAuth",
    ], "/var/log/mongos.log")
    node.wait_ready()
    mongos = node.client()
    mongos.admin.command("ping")
    mongos.close()


@pytest.mark.timeout(180, func_only=True)
def test_x509(node):
    node.start("mongod", [
        "--dbpath", "/var/lib/mongo", "--port", "27017",
        "--setParameter", "authenticationMechanisms=SCRAM-SHA-256,MONGODB-X509",
    ])
    node.wait_ready()
    setup = node.client()
    setup["$external"].command("createUser", X509_USER, roles=[{"role": "readWriteAnyDatabase", "db": "admin"}])
    setup.close()
    node.start("mongod", [
        "--dbpath", "/var/lib/mongo", "--port", "27017", "--auth",
        "--setParameter", "authenticationMechanisms=SCRAM-SHA-256,MONGODB-X509",
    ], fresh=False)
    node.wait_ready()
    client = node.client(
        tlsCertificateKeyFile=str(Path(node.workdir) / "x509" / "pbm.pem"),
        authMechanism="MONGODB-X509",
        authSource="$external",
    )
    client.admin.command("ping")
    client.close()


@pytest.mark.timeout(180, func_only=True)
def test_ldap(node):
    node.start("mongod", ["--dbpath", "/var/lib/mongo", "--port", "27017"])
    node.wait_ready()
    setup = node.client()
    setup.admin.command("createRole", "cn=readers,ou=groups,dc=example,dc=org", privileges=[], roles=[{"role": "readWriteAnyDatabase", "db": "admin"}])
    setup.close()
    node.start("mongod", [
        "--dbpath", "/var/lib/mongo", "--port", "27017", "--auth",
        "--ldapServers", "ldap:1636", "--ldapTransportSecurity", "tls",
        "--ldapAuthzQueryTemplate", "dc=example,dc=org??sub?(&(objectClass=groupOfNames)(member={PROVIDED_USER}))",
        "--setParameter", "authenticationMechanisms=SCRAM-SHA-256,PLAIN",
    ], env={
        "LDAPTLS_CACERT": "/etc/x509/ca.crt",
        "LDAPTLS_REQCERT": "demand",
    }, fresh=False)
    node.wait_ready()
    client = node.client(
        username=LDAP_USER,
        password="pbmpass",
        authSource="$external",
        authMechanism="PLAIN",
    )
    client.admin.command("ping")
    client.close()


@pytest.mark.timeout(180, func_only=True)
def test_oidc(node):
    body = urllib.parse.urlencode({
        "grant_type": "client_credentials",
        "client_id": "pbmclient",
        "client_secret": "pbm-secret",
    }).encode()
    request = urllib.request.Request("https://keycloak:8443/realms/test/protocol/openid-connect/token", data=body)
    with urllib.request.urlopen(request, context=ssl._create_unverified_context(), timeout=30) as response:
        token = json.loads(response.read())["access_token"]
    # Same IdP document as the PBM OIDC tests. serverCAFile is the EasyRSA CA that
    # signed Keycloak. Incoming client TLS uses that CA through with_fips.
    providers = (
        'oidcIdentityProviders=[{'
        '"issuer":"https://keycloak:8443/realms/test",'
        '"clientId":"pbmclient",'
        '"audience":"account",'
        '"authNamePrefix":"keycloak",'
        '"useAuthorizationClaim":false,'
        '"supportsHumanFlows":false,'
        '"principalName":"client_id",'
        '"serverCAFile":"/etc/x509/ca.crt"'
        '}]'
    )
    oidc_args = [
        "--dbpath", "/var/lib/mongo", "--port", "27017",
        "--setParameter", "authenticationMechanisms=SCRAM-SHA-256,MONGODB-OIDC",
        "--setParameter", providers,
    ]
    node.start("mongod", oidc_args)
    node.wait_ready()
    setup = node.client()
    setup["$external"].command("createUser", "keycloak/pbmclient", roles=[{"role": "readWriteAnyDatabase", "db": "admin"}])
    setup.close()
    node.start("mongod", ["--auth"] + oidc_args, fresh=False)
    node.wait_ready()

    from pymongo.auth_oidc import OIDCCallback, OIDCCallbackResult

    class Token(OIDCCallback):
        def fetch(self, context):
            return OIDCCallbackResult(access_token=token)

    client = node.client(
        authMechanism="MONGODB-OIDC",
        authMechanismProperties={"OIDC_CALLBACK": Token()},
        authSource="$external",
        serverSelectionTimeoutMS=60000,
        connectTimeoutMS=20000,
        socketTimeoutMS=60000,
    )
    try:
        client.admin.command("ping")
    except Exception as exc:
        raise AssertionError("OIDC authorization failed: " + str(exc) + "\n" + node.read_log()) from exc
    client.close()


def remember_document(node):
    client = node.client()
    client.encryption.probe.insert_one({"_id": "probe", "value": "kept"})
    client.close()


def read_document(node):
    client = node.client()
    try:
        doc = client.encryption.probe.find_one({"_id": "probe"})
    finally:
        client.close()
    if doc is None or doc.get("value") != "kept":
        raise AssertionError("encrypted document did not survive rotation: " + str(doc))


def rotate_master_key(node, args, flag):
    node.start("mongod", [flag] + args, fresh=False)
    log = node.wait_exit()
    if "rotat" not in log.lower():
        raise AssertionError("master key rotation was not reported\n" + log)
    return log


@pytest.mark.timeout(180, func_only=True)
def test_kmip(node):
    args = [
        "--dbpath", "/var/lib/mongo", "--port", "27017",
        "--enableEncryption",
        "--kmipServerName", "cosmian", "--kmipPort", "5696",
        "--kmipServerCAFile", "/etc/pykmip/ca.crt",
        "--kmipClientCertificateFile", "/etc/pykmip/mongod.pem",
    ]
    node.start("mongod", args)
    node.wait_ready()
    remember_document(node)
    rotate_master_key(node, args, "--kmipRotateMasterKey")
    node.start("mongod", args, fresh=False)
    node.wait_ready()
    read_document(node)


def vault_secret_version():
    vault = None
    for container in docker.from_env().containers.list():
        if container.attrs.get("Config", {}).get("Hostname") == "vault":
            vault = container
            break
    if vault is None:
        raise AssertionError("vault container is not running")
    result = vault.exec_run(
        ["bao", "kv", "metadata", "get", "-format=json", "secret/mongo"],
        environment={
            "BAO_ADDR": "https://vault:8200",
            "BAO_CACERT": "/openbao/tls/ca.crt",
            "BAO_TOKEN": "vaulttoken",
        },
    )
    output = result.output.decode("utf-8", errors="replace")
    if result.exit_code != 0:
        raise AssertionError(output)
    return json.loads(output)["data"]["current_version"]


@pytest.mark.timeout(180, func_only=True)
def test_vault(node):
    args = [
        "--dbpath", "/var/lib/mongo", "--port", "27017",
        "--enableEncryption",
        "--vaultServerName", "vault", "--vaultPort", "8200",
        "--vaultTokenFile", "/etc/vault/token",
        "--vaultSecret", "secret/data/mongo",
        "--vaultServerCAFile", "/etc/x509/ca.crt",
    ]
    node.start("mongod", args)
    node.wait_ready()
    remember_document(node)
    before = vault_secret_version()
    rotate_master_key(node, args, "--vaultRotateMasterKey")
    after = vault_secret_version()
    if after <= before:
        raise AssertionError("vault secret version did not increase: %s -> %s" % (before, after))
    node.start("mongod", args, fresh=False)
    node.wait_ready()
    read_document(node)


def kadmin(container, query):
    result = container.exec_run(["kadmin.local", "-q", query])
    output = result.output.decode("utf-8", errors="replace")
    if result.exit_code != 0:
        raise AssertionError(query + "\n" + output)
    return output


def copy_out(container, src, dest):
    stream, _ = container.get_archive(src)
    with tarfile.open(fileobj=io.BytesIO(b"".join(stream))) as tar:
        member = tar.getmembers()[0]
        dest.write_bytes(tar.extractfile(member).read())
    os.chmod(dest, 0o600)


@pytest.mark.timeout(180, func_only=True)
def test_kerberos(node):
    kerberos = None
    for container in docker.from_env().containers.list():
        if container.attrs.get("Config", {}).get("Hostname") == "kerberos":
            kerberos = container
            break
    if kerberos is None:
        raise AssertionError("kerberos container is not running")
    service = "mongodb/" + node.container.name
    getprinc = kerberos.exec_run(["kadmin.local", "-q", "getprinc pbm"])
    if getprinc.exit_code != 0 or b"does not exist" in getprinc.output.lower():
        kadmin(kerberos, "addprinc -pw pbmkrbpass pbm")
    kerberos.exec_run(["rm", "-f", "/tmp/pbm.keytab", "/tmp/mongodb.keytab"])
    kadmin(kerberos, "addprinc -randkey " + service)
    kadmin(kerberos, "ktadd -k /tmp/pbm.keytab pbm@PERCONATEST.COM")
    kadmin(kerberos, "ktadd -k /tmp/mongodb.keytab " + service)
    server_keytab = Path(node.workdir) / "mongodb.keytab"
    copy_out(kerberos, "/tmp/mongodb.keytab", server_keytab)
    put_bytes(node.container, "/etc/mongodb.keytab", server_keytab.read_bytes(), 0o400)
    krb_env = {"KRB5_CONFIG": "/etc/krb5.conf", "KRB5_KTNAME": "/etc/mongodb.keytab"}
    krb_args = [
        "--dbpath", "/var/lib/mongo", "--port", "27017",
        "--setParameter", "authenticationMechanisms=SCRAM-SHA-256,GSSAPI",
    ]
    node.start("mongod", krb_args, env=krb_env)
    node.wait_ready()
    setup = node.client()
    setup["$external"].command(
        "createUser", "pbm@PERCONATEST.COM",
        roles=[{"role": "readWriteAnyDatabase", "db": "admin"}],
    )
    setup.close()
    node.start("mongod", ["--auth"] + krb_args, env=krb_env, fresh=False)
    node.wait_ready()
    kinit = kerberos.exec_run(
        ["timeout", "15", "kinit", "-kt", "/tmp/pbm.keytab", "pbm@PERCONATEST.COM"],
        environment={"KRB5_CONFIG": "/etc/krb5.conf", "KRB5CCNAME": "FILE:/tmp/krb5cc"},
    )
    if kinit.exit_code != 0:
        raise AssertionError("kinit failed: " + kinit.output.decode("utf-8", errors="replace"))
    put_bytes(kerberos, "/tmp/ca.crt", (Path(node.workdir) / "x509" / "ca.crt").read_bytes(), 0o644)
    put_bytes(kerberos, "/tmp/kerberos.pem", (Path(node.workdir) / "x509" / "kerberos.pem").read_bytes(), 0o644)
    ping = (
        "import os, pymongo\n"
        "client = pymongo.MongoClient(os.environ['MONGO_HOST'], port=27017,\n"
        "    username='pbm@PERCONATEST.COM', authMechanism='GSSAPI', authSource='$external',\n"
        "    authMechanismProperties={'SERVICE_NAME': 'mongodb', 'CANONICALIZE_HOST_NAME': 'none'},\n"
        "    tls=True, tlsCAFile='/tmp/ca.crt', tlsCertificateKeyFile='/tmp/kerberos.pem',\n"
        "    directConnection=True, connectTimeoutMS=10000, socketTimeoutMS=10000,\n"
        "    serverSelectionTimeoutMS=10000)\n"
        "from bson import json_util\n"
        "result = client.admin.command('ping')\n"
        "print('client uri: mongodb://' + os.environ['MONGO_USER'] + '@' + os.environ['MONGO_HOST'] + ':27017/?authMechanism=GSSAPI&authSource=$external&tls=true&tlsCAFile=/tmp/ca.crt&tlsCertificateKeyFile=/tmp/kerberos.pem&directConnection=true', flush=True)\n"
        "print('server response: ' + json_util.dumps(result), flush=True)\n"
    )
    auth = kerberos.exec_run(
        ["timeout", "20", "python3", "-c", ping],
        environment={
            "KRB5_CONFIG": "/etc/krb5.conf",
            "KRB5CCNAME": "FILE:/tmp/krb5cc",
            "MONGO_HOST": node.container.name,
            "MONGO_USER": urllib.parse.quote("pbm@PERCONATEST.COM", safe=""),
        },
    )
    text = auth.output.decode("utf-8", errors="replace").strip()
    if text and client_log_enabled():
        LOGGER.info("%s", text)
    if auth.exit_code != 0:
        raise AssertionError(
            "GSSAPI authorization failed: " + auth.output.decode("utf-8", errors="replace")
            + "\n" + node.read_log()
        )
