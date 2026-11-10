# Base image of the OS under test. Declared before FROM so `docker build --build-arg OS_IMAGE=...` selects it.
ARG OS_IMAGE=oraclelinux:9
FROM ${OS_IMAGE}

USER 0

COPY percona-server-mongodb-*.tar.gz /tmp/psmdb-dist/
COPY conf/check-bundle-libs.sh /usr/local/bin/check-bundle-libs.sh
RUN set -eu; \
    pkg_tar=$(ls /tmp/psmdb-dist/percona-server-mongodb-*.tar.gz | grep -v -- '-runtime-libs.tar.gz'); \
    rt_tar=$(ls /tmp/psmdb-dist/percona-server-mongodb-*-runtime-libs.tar.gz); \
    tar -C /opt -xzf "$pkg_tar"; \
    tar -C /opt -xzf "$rt_tar"; \
    pkg_dir=/opt/$(basename "$pkg_tar" .tar.gz); \
    rt_dir=/opt/$(basename "$rt_tar" .tar.gz); \
    ln -sfn "$pkg_dir" /opt/psmdb; \
    ln -sfn "$rt_dir" /opt/psmdb-runtime-libs; \
    rm -rf /tmp/psmdb-dist; \
    L=/opt/psmdb-runtime-libs; \
    LD_LIBRARY_PATH="$L/lib" "$L/bin/openssl" fipsinstall \
        -module "$L/lib/ossl-modules/fips.so" \
        -out "$L/fipsmodule.cnf"; \
    fips_cnf=$(readlink -f "$L/fipsmodule.cnf"); \
    printf '%s\n' \
        'config_diagnostics = 1' \
        'openssl_conf = openssl_init' \
        ".include $fips_cnf" \
        '' \
        '[openssl_init]' \
        'providers = provider_sect' \
        'alg_section = algorithm_sect' \
        '' \
        '[provider_sect]' \
        'fips = fips_sect' \
        'base = base_sect' \
        '' \
        '[base_sect]' \
        'activate = 1' \
        '' \
        '[algorithm_sect]' \
        'default_properties = fips=yes' \
        > "$L/openssl-fips.cnf"; \
    chmod 644 "$L/fipsmodule.cnf" "$L/openssl-fips.cnf"; \
    ln -sfn /opt/psmdb/bin/mongod /usr/bin/mongod; \
    ln -sfn /opt/psmdb/bin/mongos /usr/bin/mongos; \
    mkdir -p /var/lib/mongo /var/log /etc; \
    LD_LIBRARY_PATH="$L/opt/mongo/openssl3/lib:$L/lib" \
        OPENSSL_CONF="$L/openssl-fips.cnf" \
        OPENSSL_MODULES="$L/lib/ossl-modules" \
        /opt/psmdb/bin/mongod --version; \
    LD_LIBRARY_PATH="$L/opt/mongo/openssl3/lib:$L/lib" \
        OPENSSL_CONF="$L/openssl-fips.cnf" \
        OPENSSL_MODULES="$L/lib/ossl-modules" \
        /opt/psmdb/bin/mongos --version; \
    sh /usr/local/bin/check-bundle-libs.sh

# Library path and FIPS config are not image-wide. Each test passes them to mongod and mongos.
ENTRYPOINT []
CMD ["/bin/sh", "-c", "while true; do sleep 3600; done"]
