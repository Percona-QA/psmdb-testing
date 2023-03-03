ARG PSMDB=percona/percona-server-mongodb
ARG GO_VER=latest
FROM golang:$GO_VER 
ARG PBM_BRANCH=main
WORKDIR /
RUN apt update && apt -y install gcc libgssapi-krb5-2 libkrb5-dev libsasl2-modules-gssapi-mit && \
    git clone -b $PBM_BRANCH --single-branch https://github.com/percona/percona-backup-mongodb.git && \
    cd percona-backup-mongodb && make build

FROM $PSMDB
USER root
RUN yum install -y epel-release && yum install -y supervisor
COPY --from=0 /percona-backup-mongodb/bin/ /usr/bin/
COPY conf/supervisord.d/ /etc/supervisord.d/
COPY conf/pbm/minio.yaml /etc/pbm.conf
COPY keyfile /etc/keyfile
RUN chown mongod /etc/keyfile && chmod 400 /etc/keyfile && \
    if [ -f "/usr/bin/mongosh" ] ; then \
        ln -s /usr/bin/mongosh /usr/bin/mongo ; \
    fi && \
    yum clean all && \
    rm -rf /var/cache/dnf /var/cache/yum && mkdir -p /data/db

ENTRYPOINT []
CMD ["/usr/bin/supervisord","-c","/etc/supervisord.conf","-n"]

