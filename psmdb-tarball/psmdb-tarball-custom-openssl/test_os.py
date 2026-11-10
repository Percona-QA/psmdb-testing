"""Smoke-test the OpenSSL 3 PSMDB tarball on one OS image.

CI runs this once per OS and passes that image in OS_IMAGE. From this directory:

    OS_IMAGE=oraclelinux:9 docker compose run --rm --build test

The default OS image is oraclelinux:9.
"""

import io
import json
import os
import re
import shlex
import shutil
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
    cmd = [binary, "--tlsFIPSMode", "--bind_ip_all"]
    if binary == "mongod":
        cmd += ["--wiredTigerCacheSizeGB", "0.25"]
    if "authenticationMechanisms" not in " ".join(args):
        cmd += ["--setParameter", "authenticationMechanisms=SCRAM-SHA-256"]
    return cmd + args


class OsNode:
    def __init__(self, container, workdir):
        self.container = container
        self.workdir = workdir
        self.log_path = "/var/log/mongodb-smoke.log"
        self.container.reload()
        self.ip = self.container.attrs["NetworkSettings"]["Networks"][NETWORK]["IPAddress"]

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

    def client(self, port=27017, **kwargs):
        kwargs.setdefault("directConnection", True)
        kwargs.setdefault("serverSelectionTimeoutMS", 5000)
        return pymongo.MongoClient(self.ip, port, **kwargs)

    def wait_primary(self, port=27017, timeout=60):
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            try:
                doc = self.client(port=port).admin.command("hello")
                if doc.get("isWritablePrimary"):
                    return doc
                last = doc
            except Exception as exc:
                last = exc
            time.sleep(0.5)
        raise AssertionError("no primary: " + str(last) + "\n" + self.read_log())


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
        holder = OsNode(container, workdir)
        holder.exec(
            "mkdir -p /etc/pykmip /etc/vault && "
            "cp /etc/cosmian/mongod.pem /etc/pykmip/mongod.pem && "
            "cp /etc/x509/ca.crt /etc/pykmip/ca.crt && "
            "printf '%s\\n' vaulttoken > /etc/vault/token && "
            "chmod 400 /etc/keyfile /etc/pykmip/mongod.pem /etc/vault/token /etc/x509/*.pem && "
            "chmod 644 /etc/x509/ca.crt /etc/pykmip/ca.crt"
        )
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
        "members": [{"_id": 0, "host": "127.0.0.1:27017"}],
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
        "members": [{"_id": 0, "host": "127.0.0.1:27019"}],
    })
    node.wait_primary(port=27019)
    cfg.close()
    node.spawn("mongos", [
        "--configdb", "rscfg/127.0.0.1:27019", "--port", "27017",
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
        "--tlsMode", "allowTLS",
        "--tlsCAFile", "/etc/x509/ca.crt",
        "--tlsCertificateKeyFile", "/etc/x509/psmdb.pem",
        "--setParameter", "authenticationMechanisms=SCRAM-SHA-256,MONGODB-X509",
    ])
    node.wait_ready()
    setup = node.client()
    setup["$external"].command("createUser", X509_USER, roles=[{"role": "readWriteAnyDatabase", "db": "admin"}])
    setup.close()
    node.start("mongod", [
        "--dbpath", "/var/lib/mongo", "--port", "27017", "--auth",
        "--tlsMode", "allowTLS",
        "--tlsCAFile", "/etc/x509/ca.crt",
        "--tlsCertificateKeyFile", "/etc/x509/psmdb.pem",
        "--setParameter", "authenticationMechanisms=SCRAM-SHA-256,MONGODB-X509",
    ], fresh=False)
    node.wait_ready()
    client = node.client(
        tls=True,
        tlsCertificateKeyFile=str(Path(node.workdir) / "x509" / "pbm.pem"),
        tlsCAFile=str(Path(node.workdir) / "x509" / "ca.crt"),
        tlsAllowInvalidHostnames=True,
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
        "--ldapServers", "ldap:1389", "--ldapTransportSecurity", "none",
        "--ldapAuthzQueryTemplate", "dc=example,dc=org??sub?(&(objectClass=groupOfNames)(member={PROVIDED_USER}))",
        "--setParameter", "authenticationMechanisms=SCRAM-SHA-256,PLAIN",
    ], fresh=False)
    node.wait_ready()
    user = urllib.parse.quote(LDAP_USER, safe="")
    uri = "mongodb://%s:pbmpass@%s:27017/?authSource=$external&authMechanism=PLAIN&directConnection=true&serverSelectionTimeoutMS=5000" % (user, node.ip)
    client = pymongo.MongoClient(uri)
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
    # signed Keycloak. --tlsCAFile is not used: it requires --tlsMode and turns on
    # incoming TLS, which these clients do not speak.
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


@pytest.mark.timeout(180, func_only=True)
def test_kmip(node):
    node.start("mongod", [
        "--dbpath", "/var/lib/mongo", "--port", "27017",
        "--enableEncryption",
        "--kmipServerName", "cosmian", "--kmipPort", "5696",
        "--kmipServerCAFile", "/etc/pykmip/ca.crt",
        "--kmipClientCertificateFile", "/etc/pykmip/mongod.pem",
    ])
    node.wait_ready()
    client = node.client()
    client.admin.command("ping")
    client.close()


@pytest.mark.timeout(180, func_only=True)
def test_vault(node):
    node.start("mongod", [
        "--dbpath", "/var/lib/mongo", "--port", "27017",
        "--enableEncryption",
        "--vaultServerName", "vault", "--vaultPort", "8200",
        "--vaultTokenFile", "/etc/vault/token",
        "--vaultSecret", "secret/data/mongo",
        "--vaultDisableTLSForTesting",
    ])
    node.wait_ready()
    client = node.client()
    client.admin.command("ping")
    client.close()


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
    ping = (
        "import os, pymongo\n"
        "client = pymongo.MongoClient(os.environ['MONGO_HOST'], port=27017,\n"
        "    username='pbm@PERCONATEST.COM', authMechanism='GSSAPI', authSource='$external',\n"
        "    authMechanismProperties={'SERVICE_NAME': 'mongodb', 'CANONICALIZE_HOST_NAME': 'none'},\n"
        "    directConnection=True, connectTimeoutMS=10000, socketTimeoutMS=10000,\n"
        "    serverSelectionTimeoutMS=10000)\n"
        "client.admin.command('ping')\n"
    )
    auth = kerberos.exec_run(
        ["timeout", "20", "python3", "-c", ping],
        environment={
            "KRB5_CONFIG": "/etc/krb5.conf",
            "KRB5CCNAME": "FILE:/tmp/krb5cc",
            "MONGO_HOST": node.container.name,
        },
    )
    if auth.exit_code != 0:
        raise AssertionError(
            "GSSAPI authorization failed: " + auth.output.decode("utf-8", errors="replace")
            + "\n" + node.read_log()
        )
