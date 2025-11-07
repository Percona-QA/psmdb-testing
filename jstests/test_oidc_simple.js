(function() {
    'use strict';

    // Small helper to run a shell command and print its rc/stdout/stderr to resmoke output
    function diag(label, cmd) {
        print("\n==== " + label + " ====");
        const rc = runProgram("bash", "-lc", cmd + " ; echo __RC__$?");
        // We can’t capture stderr separately here, so include -v/-s flags in the cmd when needed.
        // The rc marker lets us see the exit code even with pipes.
        return rc;
    }

    var conn = MongoRunner.runMongod();
    var admin = conn.getDB("admin");

    admin.createRole({
        role: "keycloak/Everyone", privileges: [],
        roles: [ "readWriteAnyDatabase"]
    });
    admin.logout();

    var ext = conn.getDB("$external");
    ext.createUser({
        user: "keycloak/pbmclient",
        roles: [ { role: "keycloak/Everyone", db: "admin" } ]
    });
    ext.logout();

    MongoRunner.stopMongod(conn);

    // --- Connectivity preflight (before restarting with auth) ---
    // 1) DNS resolution the test process will use
    diag("DNS getent", "getent hosts keycloak || true");

    // 2) Basic TCP reachability (no TLS)
    diag("TCP probe 8443", "bash -lc 'exec 3<>/dev/tcp/keycloak/8443 && echo port-open || echo no-listener'");

    // 3) TLS handshake summary (subject/issuer/dates)
    diag("TLS handshake (openssl)", "timeout 5 openssl s_client -brief -connect keycloak:8443 -servername keycloak </dev/null || true");

    // 4) Show network + route context of this job (often different under Jenkins)
    diag("Interfaces", "ip -o addr show | awk '{print $2, $4}' || true");
    diag("Routes", "ip route || true");

    // 5) CA file presence and fingerprint (to catch wrong mounts/paths)
    const ca = "/etc/keycloak/keycloak.crt";
    diag("CA file", "ls -l " + ca + " && sha256sum " + ca + " || true");

    // --- mongod restart with OIDC ---
    // Strongly recommended in CI: tell mongod which CA to trust for the OIDC issuer.
    // This prevents "Failed to load JWKs ... SSL connect error" when the CI image lacks your CA in system trust.
    var oidcProviders = JSON.stringify([
        {
            issuer: "https://keycloak:8443/realms/test",
            clientId: "pbmclient",
            audience: "account",
            authNamePrefix: "keycloak",
            useAuthorizationClaim: false,
            supportsHumanFlows: false,
            principalName: "client_id",
            serverCAFile: ca,           // <---- key addition for Jenkins
            JWKSPollSecs: 0,            // fetch once immediately; useful for deterministic logs
            logClaims: ["iss","sub"]    // adds a bit more context to OIDC logs
        }
    ]);

    // Crank up network/component verbosity a little to see OIDC/JWKS fetch failures better.
    const logVerbosity = {
        verbosity: 0,
        network: { verbosity: 2, connectionPool: { verbosity: 2 } },
        accessControl: { verbosity: 2 }
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

    // --- Token fetch with full curl diagnostics ---
    const token_raw = MongoRunner.dataPath + "oidc_token.txt";

    // First, a quick verbose curl to stderr (no token saved) just to see *why* it fails in Jenkins
    diag("curl -v connectivity",
        "curl -v --connect-timeout 3 --max-time 8 " +
        "--cacert " + ca + " " +
        "-X POST 'https://keycloak:8443/realms/test/protocol/openid-connect/token' " +
        "-d 'grant_type=client_credentials&client_id=pbmclient&client_secret=pbm-secret' 2>&1 | sed -n '1,120p'");

    // Then the real token fetch (silent) into a file; print size and first chars so we know it worked.
    let token_command = runProgram("bash", "-lc",
        "set -o pipefail; " +
        "curl --silent --show-error --connect-timeout 5 --max-time 15 " +
        "--cacert " + ca + " " +
        "-X POST 'https://keycloak:8443/realms/test/protocol/openid-connect/token' " +
        "-d 'grant_type=client_credentials&client_id=pbmclient&client_secret=pbm-secret' " +
        "| jq -r .access_token > " + token_raw + " && " +
        "echo 'token-bytes:' $(wc -c < " + token_raw + ") && " +
        "head -c 24 " + token_raw + " && echo");

    // If that failed, try a forced IP path while keeping SNI/Host via --resolve, which surfaces DNS vs network issues.
    diag("fallback curl with --resolve",
        "IP=$(getent hosts keycloak | awk '{print $1}' | head -1); " +
        "[ -n \"$IP\" ] && curl -v --connect-timeout 3 --max-time 8 --resolve keycloak:8443:$IP " +
        "--cacert " + ca + " " +
        "-X POST 'https://keycloak:8443/realms/test/protocol/openid-connect/token' " +
        "-d 'grant_type=client_credentials&client_id=pbmclient&client_secret=pbm-secret' 2>&1 | sed -n '1,120p' || true");

    // Read token (may be empty if curls failed)
    const token = cat(token_raw).trim();
    print("Token length: " + token.length);

    // If token is empty, dump a final hint and fail early so you don’t get a misleading “Either a username or an access token must be provided”
    if (!token || token.length < 10) {
        print("ERROR: OIDC access token not retrieved; see curl diagnostics above.");
        throw new Error("No token fetched");
    }

    // --- Client connect using the token ---
    var clientConnect = function(c) {
        const exitCode = runMongoProgram("/percona-server-mongodb/mongo",
                                         "--quiet",
                                         "--port", c.port,
                                         "--authenticationDatabase", '$external',
                                         "--authenticationMechanism", "MONGODB-OIDC",
                                         "--oidcAccessToken", token,
                                         "--eval", "db.runCommand({connectionStatus:1})");
        return exitCode;
    };

    const ec = clientConnect(conn2);
    print("mongo exit code: " + ec);
    assert.eq(ec, 0, "OIDC client connect failed");

    MongoRunner.stopMongod(conn2);
})();
