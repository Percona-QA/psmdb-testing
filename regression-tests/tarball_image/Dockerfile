ARG OS=ubuntu:18.04
FROM $OS
ARG sources=https://downloads.percona.com/downloads/percona-server-mongodb-LATEST/percona-server-mongodb-4.4.9-10/source/tarball/percona-server-mongodb-4.4.9-10.tar.gz
ARG tarball=https://downloads.percona.com/downloads/percona-server-mongodb-LATEST/percona-server-mongodb-4.4.9-10/binary/tarball/percona-server-mongodb-4.4.9-10-x86_64.glibc2.17.tar.gz
ARG builder=https://raw.githubusercontent.com/percona/percona-server-mongodb/master/percona-packaging/scripts/psmdb_builder.sh
RUN mkdir -p /opt/percona-server-mongodb/ && mkdir -p /work
RUN if [ -f "/usr/bin/yum" ] ; then yum -y update && yum install -y wget redhat-lsb-core ; else apt-get update && apt-get -y upgrade && apt-get -y install wget lsb-release iproute2 net-tools ; fi
WORKDIR work
RUN wget -O psmdb_builder.sh $builder && chmod +x psmdb_builder.sh \
    && ./psmdb_builder.sh --builddir=/opt/percona-server-mongodb --install_deps=1 \
    && wget -O percona-server-mongodb-tarball.tar.gz $tarball && wget -O percona-server-mongodb-sources.tar.gz $sources \
    && tar -C /usr --transform='s,^/*[^/]*,,S' -xzvf percona-server-mongodb-tarball.tar.gz \
    && tar -C /opt/percona-server-mongodb --transform='s,^/*[^/]*,,S' -xzvf percona-server-mongodb-sources.tar.gz \
    && rm -rf *
WORKDIR /opt/percona-server-mongodb
ADD https://raw.githubusercontent.com/Percona-QA/psmdb-testing/main/regression-tests/resmoke2junit.py .
RUN find jstests -type f | xargs chmod 400 \
    && pip install -r etc/pip/dev-requirements.txt \
    && support-files/ldap-sasl/deploy_ldap_and_sasl.sh
ENV PATH="${PATH}:/data/multiversion"
ENV PYTHONPATH="/opt/percona-server-mongodb:/opt/percona-server-mongodb/src"
