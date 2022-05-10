ARG OS=ubuntu:18.04
FROM $OS
ARG branch=release-4.4.9-10
ARG psm_ver=4.4.9
ARG psm_release=10
ARG mongo_tools_tag=100.4.1
ARG jemalloc_tag=psmdb-3.2.11-3.1
ARG repo=https://github.com/percona/percona-server-mongodb.git
ARG special_targets="dbtest"
RUN mkdir -p /opt/percona-server-mongodb/ && mkdir -p /work
RUN if [ -f "/usr/bin/yum" ] ; then yum -y update && yum install -y wget redhat-lsb-core tzdata ; else apt-get update && apt-get -y upgrade && apt-get -y install wget lsb-release iproute2 net-tools tzdata ; fi
WORKDIR work
ADD https://raw.githubusercontent.com/percona/percona-server-mongodb/$branch/percona-packaging/scripts/psmdb_builder.sh .
RUN chmod +x psmdb_builder.sh && ./psmdb_builder.sh --builddir=/opt/percona-server-mongodb --install_deps=1 \
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
    && pip uninstall -y pymongo && pip install pymongo==3.12 \
    && support-files/ldap-sasl/deploy_ldap_and_sasl.sh \
    && ( buildscripts/scons.py CC=/usr/bin/gcc-8 CXX=/usr/bin/g++-8 CPPPATH=/usr/local/include LIBPATH="/usr/local/lib /usr/local/lib64" -j4 build/install/bin/wt || buildscripts/scons.py CC=/usr/bin/gcc-8 CXX=/usr/bin/g++-8 CPPPATH=/usr/local/include LIBPATH="/usr/local/lib /usr/local/lib64" -j4 wt ) \
    && ( cp build/install/bin/wt /usr/bin/ || cp build/opt/third_party/wiredtiger/wt /usr/bin/ )\
    && rm -rf build \
    && rm -f resmoke.ini \
    && mkdir /opt/lib \
    && cp -r /usr/lib/private /opt/lib/ \
    && cp -r /usr/bin/dbtest . \
    && cp -r /usr/bin/mongo* . \
    && sed "s|UNITTEST_ALIAS='unittests',|UNITTEST_ALIAS='install-unittests',|" -i SConstruct \
    && sed "s|INTEGRATION_TEST_ALIAS='integration_tests',|INTEGRATION_TEST_ALIAS='install-integration-tests',|" -i SConstruct
ENV PATH="${PATH}:/data/multiversion"
ENV PYTHONPATH="/opt/percona-server-mongodb:/opt/percona-server-mongodb/src"
ENV TZ=America/New_York
ENV PORTABLE=1
ENV USE_SSE=1
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

