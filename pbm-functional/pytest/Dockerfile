ARG PSMDB=percona/percona-server-mongodb
ARG GO_VER=latest
FROM golang:$GO_VER as builder
ARG PBM_BRANCH=main
WORKDIR /
RUN mkdir -p /pbm-old && mkdir -p /pbm-new && \
    apt update && apt -y install gcc libgssapi-krb5-2 libkrb5-dev libsasl2-modules-gssapi-mit && \
    git clone https://github.com/percona/percona-backup-mongodb.git && cd percona-backup-mongodb && \
    git checkout $PBM_BRANCH && git pull && \
    sed -E "s|(.+)https://%s.blob.core.windows.net/%s(.+)|\1http://azurite:10000/%s/%s\2|" -i pbm/storage/azure/azure.go && \
    make build && cp -rpf bin/* /pbm-new/ && rm -rf bin && \
    git reset --hard && \
    git branch -r | grep release | sed 's/origin\///'| grep -v $PBM_BRANCH | sort --version-sort | tail -1 | xargs git checkout && \
    git pull && \
    sed -E "s|(.+)https://%s.blob.core.windows.net/%s(.+)|\1http://azurite:10000/%s/%s\2|" -i pbm/storage/azure/azure.go && \
    make build && cp -rpf bin/* /pbm-old/ && rm -rf bin 

FROM pykmip/local as pykmip

FROM $PSMDB
USER root
RUN yum install -y epel-release && yum install -y supervisor && \
    mkdir -p /pbm-old && mkdir -p /pbm-new && mkdir /etc/pykmip && mkdir /etc/vault
COPY --from=builder /pbm-new/ /pbm-new/
COPY --from=builder /pbm-old/ /pbm-old/
COPY --from=pykmip /etc/mongod.pem /etc/pykmip/mongod.pem
COPY --from=pykmip /root/easy-rsa/easyrsa3/pki/ca.crt /etc/pykmip/ca.crt
COPY conf/supervisord.d/ /etc/supervisord.d/
COPY conf/pbm/minio.yaml /etc/pbm.conf
COPY keyfile /etc/keyfile
COPY mongodb-keyfile /etc/mongodb-keyfile
RUN echo -e 'vaulttoken' > /etc/vault/token && \
    chown mongod /etc/keyfile && chmod 400 /etc/keyfile && \
    chown mongod /etc/mongodb-keyfile && chmod 400 /etc/mongodb-keyfile && \
    chown -R mongod /etc/pykmip && chmod 400 /etc/pykmip/mongod.pem && \
    chown -R mongod /etc/vault && chmod 400 /etc/vault/token && \
    if [ -f "/usr/bin/mongosh" ] ; then \
        ln -s /usr/bin/mongosh /usr/bin/mongo ; \
    fi && \
    cp -rpf /pbm-new/* /usr/bin/ && \
    yum clean all && \
    rm -rf /var/cache/dnf /var/cache/yum && mkdir -p /data/db

ENTRYPOINT []
CMD ["/usr/bin/supervisord","-c","/etc/supervisord.conf","-n"]

