ARG PSMDB=percona/percona-server-mongodb
ARG GO_VER=latest
FROM golang:$GO_VER 
ARG PBM_BRANCH=main
WORKDIR /
RUN apt update && apt -y install gcc libgssapi-krb5-2 libkrb5-dev libsasl2-modules-gssapi-mit && \
    git clone https://github.com/percona/percona-backup-mongodb.git && cd percona-backup-mongodb && \
    git checkout $PBM_BRANCH && \
    sed -E "s|(.+)https://%s.blob.core.windows.net/%s(.+)|\1http://azurite:10000/%s/%s\2|" -i pbm/storage/azure/azure.go && \
    make build

FROM $PSMDB
ARG PBM_VERSION=latest

ARG AWS_ACCESS_KEY_ID=none
ARG AWS_SECRET_ACCESS_KEY=none
ENV AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
ENV AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}

USER root
RUN yum install -y epel-release && yum install -y supervisor && \
    percona-release enable pbm && \
    if [ $PBM_VERSION == "latest" ]; then \
        yum -y install percona-backup-mongodb ; \
    else \
        yum -y install percona-backup-mongodb-${PBM_VERSION}.el8 ; \
    fi && \
    mkdir -p /pbm-old && mkdir -p /pbm-new && mv /usr/bin/pbm* /pbm-old/    
COPY --from=0 /percona-backup-mongodb/bin/ /usr/bin/
COPY conf/supervisord.d/ /etc/supervisord.d/
COPY conf/pbm/minio.yaml /etc/pbm.conf
COPY keyfile /etc/keyfile
COPY mongodb-keyfile /etc/mongodb-keyfile
RUN chown mongod /etc/keyfile && chmod 400 /etc/keyfile && \
    chown mongod /etc/mongodb-keyfile && chmod 400 /etc/mongodb-keyfile && \
    if [ -f "/usr/bin/mongosh" ] ; then \
        ln -s /usr/bin/mongosh /usr/bin/mongo ; \
    fi && \
    cp -rpf /usr/bin/pbm* /pbm-new/ && \
    yum clean all && \
    rm -rf /var/cache/dnf /var/cache/yum && mkdir -p /data/db

ENTRYPOINT []
CMD ["/usr/bin/supervisord","-c","/etc/supervisord.conf","-n"]

