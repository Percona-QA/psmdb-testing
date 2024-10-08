ARG OS=ubuntu:22.04
FROM $OS
ARG branch=release-4.4.9-10
ARG psm_ver=4.4.9
ARG psm_release=10
ARG mongo_tools_tag=100.4.1
ARG jemalloc_tag=psmdb-3.2.11-3.1
ARG repo=https://github.com/percona/percona-server-mongodb.git
ARG special_targets="dbtest mongo"
RUN mkdir -p /opt/percona-server-mongodb/ && mkdir -p /work
RUN if [ -f "/usr/bin/yum" ] ; then yum -y update && yum install -y wget redhat-lsb-core tzdata ; else apt-get update && apt-get -y upgrade && apt-get -y install wget lsb-release iproute2 net-tools tzdata ; fi
WORKDIR work
ADD https://raw.githubusercontent.com/percona/percona-server-mongodb/$branch/percona-packaging/scripts/psmdb_builder.sh .
RUN chmod +x psmdb_builder.sh && ./psmdb_builder.sh --builddir=/opt/percona-server-mongodb --install_deps=1 \
    && sed -E 's/^([ ]+|)link([ ]+|)$//g' -i psmdb_builder.sh \
    && ./psmdb_builder.sh --builddir=/opt/percona-server-mongodb --repo=$repo --branch=$branch --psm_ver=$psm_ver --psm_release=$psm_release \
    --get_sources=1 --mongo_tools_tag=$mongo_tools_tag --jemalloc_tag=$jemalloc_tag --special_targets="$special_targets" --build_tarball=1 \
    && rm -rf /opt/percona-server-mongodb/* \
    && find tarball/ -name '*tar.gz' | sort | tail -1 | xargs tar -C /usr --transform='s,^/*[^/]*,,S' -xzvf \
    && find source_tarball/ -name '*tar.gz' | sort | tail -1 | xargs tar -C /opt/percona-server-mongodb --transform='s,^/*[^/]*,,S' -xzvf \
    && rm -rf *
WORKDIR /opt/percona-server-mongodb
ADD https://raw.githubusercontent.com/Percona-QA/psmdb-testing/main/regression-tests/resmoke2junit.py .
RUN find jstests -type f | xargs chmod 400 \
    && pip install -r etc/pip/dev-requirements.txt \
    && pip install --upgrade cryptography==36.0.2 \
    && ( buildscripts/scons.py CC=/usr/bin/gcc-10 CXX=/usr/bin/g++-10 CPPPATH=/usr/local/include LIBPATH="/usr/local/lib /usr/local/lib64" -j4 build/install/bin/wt || buildscripts/scons.py CC=/usr/bin/gcc-10 CXX=/usr/bin/g++-10 CPPPATH=/usr/local/include LIBPATH="/usr/local/lib /usr/local/lib64" -j4 wt ) \
    && ( cp build/install/bin/wt /usr/bin/ || cp build/opt/third_party/wiredtiger/wt /usr/bin/ )\
    && rm -rf build \
    && rm -f resmoke.ini \
    && mkdir /opt/lib \
    && cp -r /usr/bin/dbtest . \
    && cp -r /usr/bin/mongo* . \
    && sed "s|UNITTEST_ALIAS='unittests',|UNITTEST_ALIAS='install-unittests',|" -i SConstruct \
    && sed "s|INTEGRATION_TEST_ALIAS='integration_tests',|INTEGRATION_TEST_ALIAS='install-integration-tests',|" -i SConstruct \
    && echo "mongo_version: \"$psm_ver-$psm_release\"" > .resmoke_mongo_version.yml \
    && support-files/ldap-sasl/deploy_ldap_and_sasl.sh || true \
    && echo "START=yes" >> /etc/default/saslauthd
ENV PATH="${PATH}:/data/multiversion"
ENV PYTHONPATH="/opt/percona-server-mongodb:/opt/percona-server-mongodb/src"
ENV TZ=America/New_York
ENV PORTABLE=1
ENV USE_SSE=1
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN case "$psm_ver" in \
6.0*) wget http://nz2.archive.ubuntu.com/ubuntu/pool/main/o/openssl/libssl1.1_1.1.1f-1ubuntu2.23_amd64.deb; dpkg -i libssl1.1_1.1.1f-1ubuntu2.23_amd64.deb; esac

