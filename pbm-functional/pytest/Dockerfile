ARG PSMDB=percona/percona-server-mongodb
ARG GO_VER=bullseye

FROM easyrsa/local AS easyrsa

FROM golang:$GO_VER AS builder
ARG PBM_BRANCH=dev
ARG PR_NUMBER
ARG MAKE_TARGET=build
WORKDIR /
RUN mkdir -p /pbm-old && mkdir -p /pbm-new && \
    echo 'Acquire::Retries "20";' > /etc/apt/apt.conf.d/80-retries && \
    apt update && apt -y install gcc libgssapi-krb5-2 libkrb5-dev libsasl2-modules-gssapi-mit && \
    git clone https://github.com/percona/percona-backup-mongodb.git && cd percona-backup-mongodb && \
    if [ -n "$PR_NUMBER" ]; then git fetch origin pull/$PR_NUMBER/head:CURRENT_PR; PBM_BRANCH=CURRENT_PR; fi && \
    git checkout $PBM_BRANCH && \
    sed 's/5 \* time.Minute/1 \* time.Minute/g' -i pbm/restore/physical.go && \
    make $MAKE_TARGET && cp -rpf bin/* /pbm-new/ && rm -rf bin && \
    git reset --hard && \
    git branch -r | grep release | sed 's/origin\///'| grep -v $PBM_BRANCH | sort --version-sort | tail -1 | xargs git checkout && \
    git pull && \
    make $MAKE_TARGET && cp -rpf bin/* /pbm-old/ && rm -rf bin 

FROM $PSMDB
USER root
RUN microdnf install -y yum || true
RUN yum install https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm -y && yum install -y cyrus-sasl-gssapi supervisor jq && \
    mkdir -p /pbm-old && mkdir -p /pbm-new && mkdir /etc/pykmip && mkdir /etc/vault && mkdir /etc/x509 && \
    VERSION=$(mongod --version | tail -n +2 | sed 's|Build Info: ||' | jq -r .version | sed -E 's|-.+||') && \
    ARCH=$(mongod --version | tail -n +2 | sed 's/Build Info: //' | jq -r .environment.distarch)  && \
    if [ $ARCH == x86_64 ]; then curl -Lf -o mongo-ce.tgz https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-rhel93-$VERSION.tgz || curl -Lf -o mongo-ce.tgz https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-rhel90-$VERSION.tgz ||true; fi && \
    if [ $ARCH == aarch64 ]; then curl -Lf -o mongo-ce.tgz https://fastdl.mongodb.org/linux/mongodb-linux-aarch64-rhel93-$VERSION.tgz || curl -Lf -o mongo-ce.tgz https://fastdl.mongodb.org/linux/mongodb-linux-aarch64-rhel90-$VERSION.tgz || true ; fi && \
    mkdir -p /opt/mongodb && tar -C /opt/mongodb --transform='s,^/*[^/]*,,S' -xzvf mongo-ce.tgz || true && rm -rf mongo-ce.tgz || true
COPY --from=builder /pbm-new/ /pbm-new/
COPY --from=builder /pbm-old/ /pbm-old/
COPY --from=easyrsa /etc/pykmip/mongod.pem /etc/pykmip/mongod.pem
COPY --from=easyrsa /etc/x509/ca.crt /etc/pykmip/ca.crt
COPY --from=easyrsa /etc/x509/ /etc/x509/
COPY --from=easyrsa /etc/nginx-minio/ca.crt /etc/nginx-minio/ca.crt
COPY conf/supervisord.d/ /etc/supervisord.d/
COPY conf/pbm/minio.yaml /etc/pbm.conf
COPY conf/pbm/fs.yaml /etc/pbm-fs.conf
COPY conf/pbm/pbm-1525.yaml /etc/pbm-1525.conf
COPY conf/pbm/pbm-1043.yaml /etc/pbm-1043.conf
COPY conf/pbm/pbm-1211.yaml /etc/pbm-1211.conf
COPY conf/pbm/filesystem-profile.yaml /etc/pbm-fs-profile.conf
COPY conf/pbm/aws.yaml /etc/aws.conf
COPY conf/pbm/gcs.yaml /etc/gcs.conf
COPY conf/pbm/gcs_hmac.yaml /etc/gcs_hmac.conf
COPY conf/pbm/azure.yaml /etc/azure.conf
COPY conf/krb5.conf /etc/krb5.conf
COPY keyfile /etc/keyfile
COPY mongodb-keyfile /etc/mongodb-keyfile
COPY mongodb-keyfile-new /etc/mongodb-keyfile-new
RUN echo -e 'vaulttoken' > /etc/vault/token && \
    mkdir -p /var/lib/mongo && \
    chown -R mongodb /var/lib/mongo && \
    chown mongodb /etc/keyfile && chmod 400 /etc/keyfile && \
    chown mongodb /etc/mongodb-keyfile && chmod 400 /etc/mongodb-keyfile && \
    chown mongodb /etc/mongodb-keyfile-new && chmod 400 /etc/mongodb-keyfile-new && \
    chown -R mongodb /etc/pykmip && chmod 400 /etc/pykmip/mongod.pem && \
    chown -R mongodb /etc/vault && chmod 400 /etc/vault/token && \
    chown -R mongodb /etc/x509 && chmod 400 /etc/x509/* && \
    chown -R mongodb /etc/nginx-minio && \
    if [ -f "/usr/bin/mongosh" ] ; then \
        ln -s /usr/bin/mongosh /usr/bin/mongo ; \
    fi && \
    cp -rpf /pbm-new/* /usr/bin/ && \
    yum clean all && \
    rm -rf /var/cache/dnf /var/cache/yum && mkdir -p /data/db

ENTRYPOINT []
CMD ["/usr/bin/supervisord","-c","/etc/supervisord.conf","-n"]

