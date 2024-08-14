ARG PSMDB=percona/percona-server-mongodb
ARG GO_VER=1.22-bullseye

FROM alpine AS x509
USER root
ENV EASYRSA_DN=org
ENV EASYRSA_REQ_COUNTRY=US
ENV EASYRSA_REQ_PROVINCE=California
ENV EASYRSA_REQ_CITY=SanFrancisco
ENV EASYRSA_REQ_ORG=Percona
ENV EASYRSA_REQ_EMAIL=pbm@percona.com
RUN apk add --no-cache bash git openssl && cd ~ && \
    git clone https://github.com/OpenVPN/easy-rsa.git && \
    cd easy-rsa/easyrsa3/ && \
    ./easyrsa init-pki && \
    ./easyrsa --req-cn=Percona --batch build-ca nopass && \
    ./easyrsa --req-ou=server --subject-alt-name=DNS:*,IP:127.0.0.1 --batch build-server-full psmdb nopass && \
    ./easyrsa --req-ou=client --batch build-client-full pbm nopass && \
    mkdir -p /etc/x509 && \
    cp pki/ca.crt /etc/x509/ca.crt && \
    cat pki/issued/psmdb.crt pki/private/psmdb.key > /etc/x509/psmdb.pem && \
    cat pki/issued/pbm.crt pki/private/pbm.key > /etc/x509/pbm.pem 

FROM pykmip/local AS pykmip

FROM golang:$GO_VER AS builder
ARG PBM_BRANCH=dev
ARG PR_NUMBER
WORKDIR /
RUN mkdir -p /pbm-old && mkdir -p /pbm-new && \
    apt update && apt -y install gcc libgssapi-krb5-2 libkrb5-dev libsasl2-modules-gssapi-mit && \
    git clone https://github.com/percona/percona-backup-mongodb.git && cd percona-backup-mongodb && \
    if [ -n "$PR_NUMBER" ]; then git fetch origin pull/$PR_NUMBER/head:CURRENT_PR; PBM_BRANCH=CURRENT_PR; fi && \
    git checkout $PBM_BRANCH && \
    make build && cp -rpf bin/* /pbm-new/ && rm -rf bin && \
    git reset --hard && \
    git branch -r | grep release | sed 's/origin\///'| grep -v $PBM_BRANCH | sort --version-sort | tail -1 | xargs git checkout && \
    git pull && \
    make build && cp -rpf bin/* /pbm-old/ && rm -rf bin 

FROM $PSMDB
USER root
RUN yum install -y epel-release && yum install -y cyrus-sasl-gssapi && yum install -y supervisor jq && \
    mkdir -p /pbm-old && mkdir -p /pbm-new && mkdir /etc/pykmip && mkdir /etc/vault && mkdir /etc/x509 && \
    VERSION=$(mongod --version | tail -n +2 | sed 's|Build Info: ||' | jq -r .version | sed -E 's|-.+||') && \
    ARCH=$(mongod --version | tail -n +2 | sed 's/Build Info: //' | jq -r .environment.distarch)  && \
    if [ $ARCH == x86_64 ]; then curl -Lf -o mongo-ce.tgz https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-rhel80-$VERSION.tgz || true; fi && \
    if [ $ARCH == aarch64 ]; then curl -Lf -o mongo-ce.tgz https://fastdl.mongodb.org/linux/mongodb-linux-aarch64-rhel82-$VERSION.tgz || true ; fi && \
    mkdir -p /opt/mongodb && tar -C /opt/mongodb --transform='s,^/*[^/]*,,S' -xzvf mongo-ce.tgz || true && rm -rf mongo-ce.tgz || true
COPY --from=builder /pbm-new/ /pbm-new/
COPY --from=builder /pbm-old/ /pbm-old/
COPY --from=pykmip /etc/mongod.pem /etc/pykmip/mongod.pem
COPY --from=pykmip /root/easy-rsa/easyrsa3/pki/ca.crt /etc/pykmip/ca.crt
COPY --from=x509 /etc/x509/ /etc/x509/
COPY conf/supervisord.d/ /etc/supervisord.d/
COPY conf/pbm/minio.yaml /etc/pbm.conf
COPY conf/pbm/pbm-1043.yaml /etc/pbm-1043.conf
COPY conf/pbm/filesystem-profile.yaml /etc/pbm-fs-profile.conf
COPY conf/krb5.conf /etc/krb5.conf
COPY keyfile /etc/keyfile
COPY mongodb-keyfile /etc/mongodb-keyfile
RUN echo -e 'vaulttoken' > /etc/vault/token && \
    mkdir -p /var/lib/mongo && \
    chown -R mongodb /var/lib/mongo && \
    chown mongodb /etc/keyfile && chmod 400 /etc/keyfile && \
    chown mongodb /etc/mongodb-keyfile && chmod 400 /etc/mongodb-keyfile && \
    chown -R mongodb /etc/pykmip && chmod 400 /etc/pykmip/mongod.pem && \
    chown -R mongodb /etc/vault && chmod 400 /etc/vault/token && \
    chown -R mongodb /etc/x509 && chmod 400 /etc/x509/* && \
    if [ -f "/usr/bin/mongosh" ] ; then \
        ln -s /usr/bin/mongosh /usr/bin/mongo ; \
    fi && \
    cp -rpf /pbm-new/* /usr/bin/ && \
    yum clean all && \
    rm -rf /var/cache/dnf /var/cache/yum && mkdir -p /data/db

ENTRYPOINT []
CMD ["/usr/bin/supervisord","-c","/etc/supervisord.conf","-n"]

