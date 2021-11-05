ARG OS=ubuntu:18.04
FROM $OS
ARG branch=release-4.4.9-10
ARG psm_ver=4.4.9
ARG psm_release=10
ARG mongo_tools_tag=100.4.1
ARG jemalloc_tag=psmdb-3.2.11-3.1
ARG repo=https://github.com/percona/percona-server-mongodb.git
ARG special_targets=wt
RUN mkdir -p /opt/percona-server-mongodb/ && mkdir -p /work
RUN if [ -f "/usr/bin/yum" ] ; then yum -y update && yum install -y wget redhat-lsb-core ; else apt-get update && apt-get -y upgrade && apt-get -y install wget lsb-release iproute2 net-tools ; fi
WORKDIR work
ADD https://raw.githubusercontent.com/percona/percona-server-mongodb/$branch/percona-packaging/scripts/psmdb_builder.sh .
RUN chmod +x psmdb_builder.sh && ./psmdb_builder.sh --builddir=/opt/percona-server-mongodb --install_deps=1 \
    && echo $special_targets \
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
    && support-files/ldap-sasl/deploy_ldap_and_sasl.sh
ENV PATH="${PATH}:/data/multiversion"
ENV PYTHONPATH="/opt/percona-server-mongodb:/opt/percona-server-mongodb/src"
ENV TZ=America/New_York
ENV PORTABLE=1
ENV USE_SSE=1
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

