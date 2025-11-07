(function() {
    'use strict';

    // Helper: run a shell command and always print an exit code marker
    function diag(label, cmd) {
        print("\n==== " + label + " ====");
        return runProgram("bash", "-lc", "set -o pipefail; " + cmd + " ; echo __RC__$?");
    }

    // ---------- Bring up a clean mongod ----------
    var conn = MongoRunner.runMongod();
    var admin = conn.getDB("admin");

    admin.createRole({ role: "keycloak/Everyone", privileges: [], roles: ["readWriteAnyDatabase"] });
    admin.logout();

    var ext = conn.getDB("$external");
    ext.createUser({
        user: "keycloak/pbmclient",
        roles: [ { role: "keycloak/Everyone", db: "admin" } ]
    });
    ext.logout();

    MongoRunner.stopMongod(conn);

    // ---------- Preflight diagnostics (network / TLS / env) ----------
    const ca = "/etc/keycloak/keycloak.crt";
    const issuer = "https://keycloak:8443/realms/test";
    const token_url = issuer + "/protocol/openid-connect/token";
    const well_known = issuer + "/.well-known/openid-configuration";
    const jwks_url = issuer + "/protocol/openid-connect/certs";

    diag("Environment (PATH, proxies)", "echo PATH=$PATH; env | grep -iE 'http_proxy|https_proxy|no_proxy' || echo 'no proxies'");
    diag("Date & timezone", "date -u; date");
    diag("DNS getent", "getent hosts keycloak || true");
    diag("Interfaces", "ip -o addr show | awk '{print $2, $4}' || true");
    diag("Routes", "ip route || true");
    diag("Listening sockets (8443 on host?)", "ss -ltnp | grep :8443 || true");
    diag("CA file presence & hash", "ls -l " + ca + " && sha256sum " + ca + " || true");
    diag("CA validity window", "openssl x509 -in " + ca + " -noout -dates -issuer -subject || true");
    diag("TLS hello brief", "timeout 5 openssl s_client -brief -connect keycloak:8443 -servername keycloak </dev/null || true");

    // Wait for listener (fail fast with a crisp message)
    diag("Wait for keycloak:8443 (10s max)",
        "for i in $(seq 1 10); do if bash -lc 'exec 3<>/dev/tcp/keycloak/8443'; then echo port-open; exit 0; fi; sleep 1; done; echo no-listener; exit 7");

    // Query the realm metadata (no auth required) – proves app readiness vs just a TLS socket
    diag("GET well-known (headers only)",
        "curl -fsS -I --connect-timeout 3 --max-time 8 '" + well_known + "' | sed -n '1,40p' || true");
    diag("GET well-known (first 40 lines)",
        "curl -fsS --connect-timeout 3 --max-time 8 '" + well_known + "' | sed -n '1,40p' || true");

    // Query JWKS – the thing mongod is fetching
    diag("GET JWKS (headers only)",
        "curl -fsS -I --connect-timeout 3 --max-time 8 '" + jwks_url + "' | sed -n '1,40p' || true");
    diag("GET JWKS (first 40 chars)",
        "curl -fsS --connect-timeout 3 --max-time 8 '" + jwks_url + "' | head -c 80; echo || true");

    // If CA issues suspected, try insecure once to separate TLS trust vs connectivity.
    diag("curl -vk (DIAG ONLY; ignores CA)",
        "curl -vk --connect-timeout 3 --max-time 8 '" + token_url + "' -d 'grant_type=client_credentials&client_id=pbmclient&client_secret=pbm-secret' 2>&1 | sed -n '1,120p' || true");

    // Check SNI/hostname paths via resolved IP
    diag("curl -v with --resolve (SNI to keycloak)",
        "IP=$(getent hosts keycloak | awk '{print $1}' | head -1); " +
        "[ -n \"$IP\" ] && curl -v --connect-timeout 3 --max-time 8 --resolve keycloak:8443:$IP " +
        "'" + token_url + "' -d 'grant_type=client_credentials&client_id=pbmclient&client_secret=pbm-secret' 2>&1 | sed -n '1,120p' || true");

    // Try forcing HTTP/1.1 to dodge any HTTP/2/TLS quirkiness
    diag("curl --http1.1 (verbose)",
        "curl -v --http1.1 --connect-timeout 3 --max-time 8 '" + token_url + "' -d 'grant_type=client_credentials&client_id=pbmclient&client_secret=pbm-secret' 2>&1 | sed -n '1,120p' || true");

    // Optional: If Docker is available in the Jenkins namespace, exec into the Keycloak container to prove service readiness
    diag("Docker ps (if available)", "docker ps --format 'table {{.Names}}\\t{{.Ports}}\\t{{.Networks}}' || true");
    diag("Keycloak container inspect (networks)", "docker inspect -f 'Networks: {{range $k,$v := .NetworkSettings.Networks}}{{printf \"%s(%s) \" $k $v.IPAddress}}{{end}}' keycloak || true");
    diag("curl inside keycloak container (well-known)", "docker exec keycloak curl -fsS 'http://127.0.0.1:8080/realms/test/.well-known/openid-configuration' | head -c 120; echo || true");

    // ---------- Restart mongod with OIDC ----------
    var oidcProviders = JSON.stringify([
        {
            issuer: issuer,
            clientId: "pbmclient",
            audience: "account",
            authNamePrefix: "keycloak",
            useAuthorizationClaim: false,
            supportsHumanFlows: false,
            principalName: "client_id",
            serverCAFile: ca,          // trust the issuer CA explicitly in CI
            JWKSPollSecs: 0,           // fetch JWKS immediately (deterministic logs)
            logClaims: ["iss","sub"]   // add claim logs
        }
    ]);

    // Increase verbosity around network & auth
    const logVerbosity = {
        verbosity: 0,
        network: { verbosity: 3, connectionPool: { verbosity: 2 }, asio: { verbosity: 2 } },
        accessControl: { verbosity: 3 },
        command: { verbosity: 2 }
    };

    var conn2 = MongoRunner.runMongod({
        restart: conn,
        auth: '',
        setParameter: {
            authenticationMechanisms: 'SCRAM-SHA-1,MONGODB-OIDC',
            oidcIdentityProviders: oidcProviders,
            logComponentVerbosity: tojson(logVerbosity)
        },
        noCleanData: true
    });
    assert(conn2, "Cannot start mongod instance");

    // ---------- Token fetch (robust) ----------
    const token_raw = MongoRunner.dataPath + "oidc_token.txt";

    // One verbose attempt to stderr (kept short)
    diag("curl -v token attempt",
        "curl -v --connect-timeout 5 --max-time 20 --cacert " + ca + " " +
        "-X POST '" + token_url + "' " +
        "-d 'grant_type=client_credentials&client_id=pbmclient&client_secret=pbm-secret' 2>&1 | sed -n '1,200p'");

    // Real fetch (silent) and basic integrity echo
    let token_command = runProgram("bash", "-lc",
        "set -o pipefail; " +
        "curl --silent --show-error --connect-timeout 5 --max-time 20 " +
        "--cacert " + ca + " " +
        "-X POST '" + token_url + "' " +
        "-d 'grant_type=client_credentials&client_id=pbmclient&client_secret=pbm-secret' " +
        "| jq -r .access_token > " + token_raw + " && " +
        "echo 'token-bytes:' $(wc -c < " + token_raw + ") && head -c 24 " + token_raw + " && echo");

    const token = cat(token_raw).trim();
    print("Token length: " + token.length);

    if (!token || token.length < 16) {
        print("ERROR: OIDC access token not retrieved; see diagnostics above.");
        // Add one more pointer: is system time outside cert validity?
        diag("Extra: compare time vs CA validity",
            "date -u; echo 'CA notBefore/notAfter:'; openssl x509 -in " + ca + " -noout -startdate -enddate || true");
        throw new Error("No token fetched");
    }

    // ---------- Connect with token ----------
    var clientConnect = function(c) {
        return runMongoProgram("/percona-server-mongodb/mongo",
            "--quiet", "--port", c.port,
            "--authenticationDatabase", '$external',
            "--authenticationMechanism", "MONGODB-OIDC",
            "--oidcAccessToken", token,
            "--eval", "db.runCommand({connectionStatus:1})");
    };

    const rc = clientConnect(conn2);
    print("mongo exit code: " + rc);
    assert.eq(rc, 0, "OIDC client connect failed");

    MongoRunner.stopMongod(conn2);
})();
