#!/usr/bin/env bash
# Created by Tomislav Plavcic, Percona LLC

MONGO_USER="dba"
MONGO_PASS="test1234"
MONGO_BACKUP_USER="backupUser"
MONGO_BACKUP_PASS="test1234"
RS1NAME=rs1
RS2NAME=rs2
CFGRSNAME=config
# static or changed with cmd line options
HOST="localhost"
BASEDIR="$(pwd)"
WORKDIR=""
LAYOUT=""
STORAGE_ENGINE="wiredTiger"
MONGOD_EXTRA="--bind_ip 0.0.0.0"
MONGOS_EXTRA="--bind_ip 0.0.0.0"
CONFIG_EXTRA="--bind_ip 0.0.0.0"
RS_ARBITER=0
RS_HIDDEN=0
RS_DELAYED=0
CIPHER_MODE="AES256-CBC"
ENCRYPTION=0
PBMDIR=""
AUTH=""
BACKUP_AUTH=""
BACKUP_URI_AUTH=""
BACKUP_URI_SUFFIX=""
TLS=0
TLS_CLIENT=""

if [ -z $1 ]; then
  echo "You need to specify at least one of the options for layout: --single, --rSet, --sCluster or use --help!"
  exit 1
fi

# Check if we have a functional getopt(1)
if ! getopt --test
then
    go_out="$(getopt --options=mrsahe:b:o:t:c:p:d:xw: \
        --longoptions=single,rSet,sCluster,arbiter,hidden,delayed,help,binDir:,host:,mongodExtra:,mongosExtra:,configExtra:,encrypt,cipherMode:,pbmDir:,auth,tls,workDir: \
        --name="$(basename "$0")" -- "$@")"
    test $? -eq 0 || exit 1
    eval set -- $go_out
fi

for arg
do
  case "$arg" in
  -- ) shift; break;;
  -m | --single )
    shift
    LAYOUT="single"
    ;;
  -r | --rSet )
    shift
    LAYOUT="rs"
    ;;
  -s | --sCluster )
    shift
    LAYOUT="sh"
    ;;
  -a | --arbiter )
    shift
    RS_ARBITER=1
    ;;
  -h | --help )
    shift
    echo -e "\nThis script can be used to setup single instance, replica set or sharded cluster of mongod or psmdb from binary tarball."
    echo -e "By default it should be run from mongodb/psmdb base directory."
    echo -e "Setup is located in the \"nodes\" subdirectory.\n"
    echo -e "Options:"
    echo -e "-m, --single\t\t\t run single instance"
    echo -e "-r, --rSet\t\t\t run replica set (3 nodes)"
    echo -e "-s, --sCluster\t\t\t run sharding cluster (2 replica sets with 3 nodes each)"
    echo -e "-x, --auth\t\t\t enable authentication"
    echo -e "-b<path>, --binDir=<path>\t specify binary directory if running from some other location (this should end with /bin)"
    echo -e "-w<path>, --workDir=<path>\t specify work directory if not current"
    echo -e "-p<path>, --pbmDir=<path>\t enables Percona Backup for MongoDB (starts agents from binaries)"
    echo -e "-o<name>, --host=<name>\t\t instead of localhost specify some hostname for MongoDB setup"
    echo -e "--mongodExtra=\"...\"\t\t specify extra options to pass to mongod"
    echo -e "--mongosExtra=\"...\"\t\t specify extra options to pass to mongos"
    echo -e "--configExtra=\"...\"\t\t specify extra options to pass to config server"
    echo -e "--tls\t\t\t\t generate tls certificates and start nodes with requiring tls connection"
    echo -e "--encrypt\t\t\t enable data at rest encryption (wiredTiger only)"
    echo -e "--cipherMode=<mode>\t\t specify cipher mode for encryption (AES256-CBC or AES256-GCM)"
    echo -e "--arbiter\t\t\t instead of 3 nodes in replica set add 2 nodes and 1 arbiter"
    echo -e "--hidden\t\t\t enable hidden node for replica"
    echo -e "--delayed\t\t\t enable delayed node for replica"
    echo -e "-h, --help\t\t\t this help"
    exit 0
    ;;
  --hidden )
    shift
    RS_HIDDEN=1
    ;;
  --delayed )
    shift
    RS_DELAYED=1
    ;;
  --mongodExtra )
    shift
    MONGOD_EXTRA="${MONGOD_EXTRA} $1"
    shift
    ;;
  --mongosExtra )
    shift
    MONGOS_EXTRA="${MONGOS_EXTRA} $1"
    shift
    ;;
  --configExtra )
    shift
    CONFIG_EXTRA="${CONFIG_EXTRA} $1"
    shift
    ;;
  --encrypt )
    shift
    ENCRYPTION=1
    ;;
  -c | --cipherMode )
    shift
    CIPHER_MODE="$1"
    shift
    ;;
  -b | --binDir )
    shift
    BINDIR="$1"
    shift
    ;;
  -o | --host )
    shift
    HOST="$1"
    shift
    ;;
  -p | --pbmDir )
    shift
    PBMDIR="$1"
    shift
    ;;
  -x | --auth )
    shift
    AUTH="--username=${MONGO_USER} --password=${MONGO_PASS} --authenticationDatabase=admin"
    BACKUP_AUTH="--username=${MONGO_BACKUP_USER} --password=${MONGO_BACKUP_PASS} --authenticationDatabase=admin"
    BACKUP_URI_AUTH="${MONGO_BACKUP_USER}:${MONGO_BACKUP_PASS}@"
    BACKUP_URI_SUFFIX="?authSource=admin"
    ;;
  --tls )
    shift
    TLS=1
    ;;
  -w | --workDir )
    shift
    WORKDIR="$1"
    shift
    ;;
  esac
done

if [ ${RS_DELAYED} = 1 ] && [ ${RS_HIDDEN} = 1 ]; then
  echo "ERROR: Cannot use hidden and delayed nodes together."
  exit 1
fi
if [ ${RS_ARBITER} = 1 ] && [ ${RS_HIDDEN} = 1 ]; then
  echo "ERROR: Cannot use arbiter and hidden nodes together."
  exit 1
fi
if [ ${RS_ARBITER} = 1 ] && [ ${RS_DELAYED} = 1 ]; then
  echo "ERROR: Cannot use arbiter and delayed nodes together."
  exit 1
fi

if [ -z "${BINDIR}" ]; then
  BINDIR="${BASEDIR}/bin"
fi
if [ -z "${WORKDIR}" ]; then
  WORKDIR="${BASEDIR}/nodes"
fi

if [ ! -x "${BINDIR}/mongod" ]; then
  echo "${BINDIR}/mongod doesn't exists or is not executable!"
  exit 1
elif [ ! -x "${BINDIR}/mongos" ]; then
  echo "${BINDIR}/mongos doesn't exists or is not executable!"
  exit 1
elif [ ! -x "${BINDIR}/mongo" ]; then
  echo "${BINDIR}/mongo doesn't exists or is not executable!"
  exit 1
fi

if [ ! -z "${PBMDIR}" -a ! -x "${PBMDIR}/pbm" ]; then
  echo "${PBMDIR}/pbm doesn't exists or is not executable!"
  exit 1
elif [ ! -z "${PBMDIR}" -a ! -x "${PBMDIR}/pbm-agent" ]; then
  echo "${PBMDIR}/pbm-agent doesn't exists or is not executable!"
  exit 1
fi

if [ -d "${WORKDIR}" ]; then
  echo "${WORKDIR} already exists"
  exit 1
else
  mkdir "${WORKDIR}"
fi

echo "MONGO_USER=\"${MONGO_USER}\"" > ${WORKDIR}/COMMON
echo "MONGO_PASS=\"${MONGO_PASS}\"" >> ${WORKDIR}/COMMON
echo "MONGO_BACKUP_USER=\"${MONGO_BACKUP_USER}\"" >> ${WORKDIR}/COMMON
echo "MONGO_BACKUP_PASS=\"${MONGO_BACKUP_PASS}\"" >> ${WORKDIR}/COMMON
echo "AUTH=\"\"" >> ${WORKDIR}/COMMON
echo "BACKUP_AUTH=\"\"" >> ${WORKDIR}/COMMON
echo "BACKUP_URI_AUTH=\"\"" >> ${WORKDIR}/COMMON

if [ ! -z "${AUTH}" ]; then
  openssl rand -base64 756 > ${WORKDIR}/keyFile
  chmod 400 ${WORKDIR}/keyFile
  MONGOD_EXTRA="${MONGOD_EXTRA} --keyFile ${WORKDIR}/keyFile"
  MONGOS_EXTRA="${MONGOS_EXTRA} --keyFile ${WORKDIR}/keyFile"
  CONFIG_EXTRA="${CONFIG_EXTRA} --keyFile ${WORKDIR}/keyFile"
fi

VERSION_FULL=$(${BINDIR}/mongod --version|head -n1|sed 's/db version v//')
VERSION_MAJOR=$(echo "${VERSION_FULL}"|grep -o '^.\..')

setup_pbm_agent(){
  local NDIR="$1"
  local RS="$2"
  local NPORT="$3"

  mkdir -p "${NDIR}/pbm-agent"

  if [ ! -z "${PBMDIR}" ]; then
   # Create startup script for the agent on the node
   echo "#!/usr/bin/env bash" > ${NDIR}/pbm-agent/start_pbm_agent.sh
   echo "source ${WORKDIR}/COMMON" >> ${NDIR}/pbm-agent/start_pbm_agent.sh
   echo "echo '=== Starting pbm-agent for mongod on port: ${NPORT} replicaset: ${RS} ==='" >> ${NDIR}/pbm-agent/start_pbm_agent.sh
   echo "${PBMDIR}/pbm-agent --mongodb-uri=\"mongodb://\${BACKUP_URI_AUTH}${HOST}:${NPORT}/${BACKUP_URI_SUFFIX}\" 1>${NDIR}/pbm-agent/stdout.log 2>${NDIR}/pbm-agent/stderr.log &" >> ${NDIR}/pbm-agent/start_pbm_agent.sh
   chmod +x ${NDIR}/pbm-agent/start_pbm_agent.sh
   echo "${NDIR}/pbm-agent/start_pbm_agent.sh" >> ${WORKDIR}/start_pbm.sh

   # Create stop script for the agent on the node
   echo "#!/usr/bin/env bash" > ${NDIR}/pbm-agent/stop_pbm_agent.sh
   echo "kill \$(cat ${NDIR}/pbm-agent/pbm-agent.pid)" >> ${NDIR}/pbm-agent/stop_pbm_agent.sh
   chmod +x ${NDIR}/pbm-agent/stop_pbm_agent.sh

   # create a symlink for pbm-agent binary
   ln -s ${PBMDIR}/pbm-agent ${NDIR}/pbm-agent/pbm-agent
  fi
}

start_mongod(){
  local NDIR="$1"
  local RS="$2"
  local PORT="$3"
  local SE="$4"
  local EXTRA="$5"
  local NTYPE="$6"
  local RS_OPT=""
  mkdir -p ${NDIR}/db
  if [ ${RS} != "nors" ]; then
    EXTRA="${EXTRA} --replSet ${RS}"
  fi
  EXTRA="${EXTRA} --wiredTigerCacheSizeGB 1"
  if [ ${ENCRYPTION} -eq 1 ]; then
    openssl rand -base64 32 > ${NDIR}/mongodb-keyfile
    chmod 600 ${NDIR}/mongodb-keyfile
    EXTRA="${EXTRA} --enableEncryption --encryptionKeyFile ${NDIR}/mongodb-keyfile --encryptionCipherMode ${CIPHER_MODE}"
  fi
  if [ ${TLS} -eq 1 ]; then
    EXTRA="${EXTRA} --tlsMode requireTLS --tlsCertificateKeyFile ${WORKDIR}/certificates/server.pem --tlsCAFile ${WORKDIR}/certificates/ca.crt"
  fi

  echo "#!/usr/bin/env bash" > ${NDIR}/start.sh
  echo "source ${WORKDIR}/COMMON" >> ${NDIR}/start.sh
  echo "echo \"Starting mongod on port: ${PORT} storage engine: ${SE} replica set: ${RS#nors}\"" >> ${NDIR}/start.sh
  echo "ENABLE_AUTH=\"\"" >> ${NDIR}/start.sh
  echo "if [ ! -z \"\${AUTH}\" ]; then ENABLE_AUTH=\"--auth\"; fi" >> ${NDIR}/start.sh
  if [ "${NTYPE}" == "arbiter" ]; then
      echo "${BINDIR}/mongod --port ${PORT} --storageEngine ${SE} --dbpath ${NDIR}/db --logpath ${NDIR}/mongod.log --fork ${EXTRA} > /dev/null" >> ${NDIR}/start.sh
  else
      echo "${BINDIR}/mongod \${ENABLE_AUTH} --port ${PORT} --storageEngine ${SE} --dbpath ${NDIR}/db --logpath ${NDIR}/mongod.log --fork ${EXTRA} > /dev/null" >> ${NDIR}/start.sh
  fi
  echo "#!/usr/bin/env bash" > ${NDIR}/cl.sh
  echo "source ${WORKDIR}/COMMON" >> ${NDIR}/cl.sh
  if [ "${NTYPE}" == "arbiter" ]; then
      echo "${BINDIR}/mongo ${HOST}:${PORT} \${TLS_CLIENT} \$@" >> ${NDIR}/cl.sh
  else
      echo "${BINDIR}/mongo ${HOST}:${PORT} \${AUTH} \${TLS_CLIENT} \$@" >> ${NDIR}/cl.sh
  fi
  echo "#!/usr/bin/env bash" > ${NDIR}/stop.sh
  echo "source ${WORKDIR}/COMMON" >> ${NDIR}/stop.sh
  echo "echo \"Stopping mongod on port: ${PORT} storage engine: ${SE} replica set: ${RS#nors}\"" >> ${NDIR}/stop.sh
  if [ "${NTYPE}" == "arbiter" ]; then
      echo "${BINDIR}/mongo localhost:${PORT}/admin --quiet --eval 'db.shutdownServer({force:true})' \${TLS_CLIENT}" >> ${NDIR}/stop.sh
  else
      echo "${BINDIR}/mongo localhost:${PORT}/admin --quiet --eval 'db.shutdownServer({force:true})' \${AUTH} \${TLS_CLIENT}" >> ${NDIR}/stop.sh
  fi
  echo "#!/usr/bin/env bash" > ${NDIR}/wipe.sh
  echo "${NDIR}/stop.sh" >> ${NDIR}/wipe.sh
  echo "rm -rf ${NDIR}/db.PREV" >> ${NDIR}/wipe.sh
  echo "rm -f ${NDIR}/mongod.log.PREV" >> ${NDIR}/wipe.sh
  echo "rm -f ${NDIR}/mongod.log.2*" >> ${NDIR}/wipe.sh
  echo "mv ${NDIR}/mongod.log ${NDIR}/mongod.log.PREV" >> ${NDIR}/wipe.sh
  echo "mv ${NDIR}/db ${NDIR}/db.PREV" >> ${NDIR}/wipe.sh
  echo "mkdir -p ${NDIR}/db" >> ${NDIR}/wipe.sh
  echo "touch ${NDIR}/mongod.log" >> ${NDIR}/wipe.sh
  chmod +x ${NDIR}/start.sh
  chmod +x ${NDIR}/cl.sh
  chmod +x ${NDIR}/stop.sh
  chmod +x ${NDIR}/wipe.sh
  ${NDIR}/start.sh
}

start_replicaset(){
  local RSDIR="$1"
  local RSNAME="$2"
  local RSBASEPORT="$3"
  local EXTRA="$4"
  mkdir -p "${RSDIR}"
  if [ ${RS_ARBITER} = 1 ] && [ "${RSNAME}" != "config" ]; then
    nodes=(config config arbiter)
  else
    nodes=(config config config)
  fi
  echo -e "\n=== Starting replica set: ${RSNAME} ==="
  for i in "${!nodes[@]}"; do
    node_number=$(($i + 1))
    if [ "${RSNAME}" != "config" ]; then
          start_mongod "${RSDIR}/node${node_number}" "${RSNAME}" "$(($RSBASEPORT + ${i}))" "${STORAGE_ENGINE}" "${EXTRA}" "${nodes[$i]}"
    else
          start_mongod "${RSDIR}/node${node_number}" "${RSNAME}" "$(($RSBASEPORT + ${i}))" "wiredTiger" "${EXTRA}" "${nodes[$i]}"
    fi
  done
  sleep 5
  echo "#!/usr/bin/env bash" > ${RSDIR}/init_rs.sh
  echo "source ${WORKDIR}/COMMON" >> ${RSDIR}/init_rs.sh
  echo "echo \"Initializing replica set: ${RSNAME}\"" >> ${RSDIR}/init_rs.sh
  if [ ${RS_ARBITER} = 0 ] && [ ${RS_DELAYED} = 0 ] && [ ${RS_HIDDEN} = 0 ]; then
    MEMBERS="[{\"_id\":1, \"host\":\"${HOST}:$(($RSBASEPORT))\"},{\"_id\":2, \"host\":\"${HOST}:$(($RSBASEPORT + 1))\"},{\"_id\":3, \"host\":\"${HOST}:$(($RSBASEPORT + 2))\"}]"
    if [ "${STORAGE_ENGINE}" == "inMemory" -a "${RSNAME}" != "config" ]; then
      echo "${BINDIR}/mongo localhost:$(($RSBASEPORT + 1)) --quiet \${TLS_CLIENT} --eval 'rs.initiate({_id:\"${RSNAME}\", writeConcernMajorityJournalDefault: false, members: ${MEMBERS}})'" >> ${RSDIR}/init_rs.sh
    else
      echo "${BINDIR}/mongo localhost:$(($RSBASEPORT + 1)) --quiet \${TLS_CLIENT} --eval 'rs.initiate({_id:\"${RSNAME}\", members: ${MEMBERS}})'" >> ${RSDIR}/init_rs.sh
    fi
  else
    if [ ${RS_DELAYED} = 1 ] || [ ${RS_HIDDEN} = 1 ]; then
	    MEMBERS="[{\"_id\":1, \"host\":\"${HOST}:$(($RSBASEPORT))\"},{\"_id\":2, \"host\":\"${HOST}:$(($RSBASEPORT + 1))\"},{\"_id\":3, \"host\":\"${HOST}:$(($RSBASEPORT + 2))\", \"priority\":0, \"hidden\":true}]"
      echo "${BINDIR}/mongo localhost:$(($RSBASEPORT + 1)) --quiet \${TLS_CLIENT} --eval 'rs.initiate({_id:\"${RSNAME}\", members: ${MEMBERS}})'" >> ${RSDIR}/init_rs.sh
    else
      if [ "${RSNAME}" == "config" ]; then
        MEMBERS="[{\"_id\":1, \"host\":\"${HOST}:$(($RSBASEPORT))\"},{\"_id\":2, \"host\":\"${HOST}:$(($RSBASEPORT + 1))\"},{\"_id\":3, \"host\":\"${HOST}:$(($RSBASEPORT + 2))\"}]"
      else
        MEMBERS="[{\"_id\":1, \"host\":\"${HOST}:$(($RSBASEPORT))\"},{\"_id\":2, \"host\":\"${HOST}:$(($RSBASEPORT + 1))\"},{\"_id\":3, \"host\":\"${HOST}:$(($RSBASEPORT + 2))\",\"arbiterOnly\":true}]"
      fi
      echo "${BINDIR}/mongo localhost:$(($RSBASEPORT + 1)) --quiet \${TLS_CLIENT} --eval 'rs.initiate({_id:\"${RSNAME}\", members: ${MEMBERS}})'" >> ${RSDIR}/init_rs.sh
    fi
  fi
  echo "#!/usr/bin/env bash" > ${RSDIR}/stop_mongodb.sh
  echo "echo \"=== Stopping replica set: ${RSNAME} ===\"" >> ${RSDIR}/stop_mongodb.sh
  for cmd in $(find ${RSDIR} -name stop.sh); do
    echo "${cmd}" >> ${RSDIR}/stop_mongodb.sh
  done
  echo "#!/usr/bin/env bash" > ${RSDIR}/start_mongodb.sh
  echo "echo \"=== Starting replica set: ${RSNAME} ===\"" >> ${RSDIR}/start_mongodb.sh
  for cmd in $(find ${RSDIR} -name start.sh); do
    echo "${cmd}" >> ${RSDIR}/start_mongodb.sh
  done
  echo "#!/usr/bin/env bash" > ${RSDIR}/cl_primary.sh
  echo "source ${WORKDIR}/COMMON" >> ${RSDIR}/cl_primary.sh
  echo "${BINDIR}/mongo \"mongodb://${HOST}:${RSBASEPORT},${HOST}:$(($RSBASEPORT + 1)),${HOST}:$(($RSBASEPORT + 2))/?replicaSet=${RSNAME}\" \${AUTH} \${TLS_CLIENT} \$@" >> ${RSDIR}/cl_primary.sh
  chmod +x ${RSDIR}/init_rs.sh
  chmod +x ${RSDIR}/start_mongodb.sh
  chmod +x ${RSDIR}/stop_mongodb.sh
  chmod +x ${RSDIR}/cl_primary.sh
  ${RSDIR}/init_rs.sh

  # for config server this is done via mongos
  if [ ! -z "${AUTH}" -a "${RSNAME}" != "config" ]; then
    sleep 20
    local PRIMARY=$(${BINDIR}/mongo localhost:${RSBASEPORT} --quiet ${TLS_CLIENT} --eval "db.isMaster().primary"|tail -n1|cut -d':' -f2)
    ${BINDIR}/mongo localhost:${PRIMARY} --quiet ${TLS_CLIENT} --eval "db.getSiblingDB(\"admin\").createUser({ user: \"${MONGO_USER}\", pwd: \"${MONGO_PASS}\", roles: [ \"root\" ] })"
    ${BINDIR}/mongo ${AUTH} ${TLS_CLIENT} "mongodb://${HOST}:${RSBASEPORT},${HOST}:$(($RSBASEPORT + 1)),${HOST}:$(($RSBASEPORT + 2))/?replicaSet=${RSNAME}" --quiet --eval "db.getSiblingDB(\"admin\").createRole( { role: \"pbmAnyAction\", privileges: [ { resource: { anyResource: true }, actions: [ \"anyAction\" ] } ], roles: [] } )"
    ${BINDIR}/mongo ${AUTH} ${TLS_CLIENT} "mongodb://${HOST}:${RSBASEPORT},${HOST}:$(($RSBASEPORT + 1)),${HOST}:$(($RSBASEPORT + 2))/?replicaSet=${RSNAME}" --quiet --eval "db.getSiblingDB(\"admin\").createUser({ user: \"${MONGO_BACKUP_USER}\", pwd: \"${MONGO_BACKUP_PASS}\", roles: [ { db: \"admin\", role: \"readWrite\", collection: \"\" }, { db: \"admin\", role: \"backup\" }, { db: \"admin\", role: \"clusterMonitor\" }, { db: \"admin\", role: \"restore\" }, { db: \"admin\", role: \"pbmAnyAction\" } ] })"
    sed -i "/^AUTH=/c\AUTH=\"--username=\${MONGO_USER} --password=\${MONGO_PASS} --authenticationDatabase=admin\"" ${WORKDIR}/COMMON
    sed -i "/^BACKUP_AUTH=/c\BACKUP_AUTH=\"--username=\${MONGO_BACKUP_USER} --password=\${MONGO_BACKUP_PASS} --authenticationDatabase=admin\"" ${WORKDIR}/COMMON
    sed -i "/^BACKUP_URI_AUTH=/c\BACKUP_URI_AUTH=\"\${MONGO_BACKUP_USER}:\${MONGO_BACKUP_PASS}@\"" ${WORKDIR}/COMMON
  fi
  if [ ${RS_DELAYED} = 1 ]; then
     ${BINDIR}/mongo ${AUTH} ${TLS_CLIENT} "mongodb://localhost:$(($RSBASEPORT + 1))/?replicaSet=${RSNAME}" --quiet --eval "cfg = rs.conf(); cfg.members[2].secondaryDelaySecs = 600; rs.reconfig(cfg);"
  fi
  # start PBM agents for replica set nodes
  # for config server replica set this is done in another place after cluster user is added
  if [ "${RSNAME}" != "config" ]; then
    sleep 5
    for i in 1 2 3; do
      if [ ${RS_ARBITER} != 1 -o ${i} -lt 3 ]; then
        setup_pbm_agent "${RSDIR}/node${i}" "${RSNAME}" "$(($RSBASEPORT + ${i} - 1))"
      fi
    done
  fi
}

set_pbm_store(){
  if [ ! -z "${PBMDIR}" ]; then
    echo "=== Setting PBM store config... ==="
    echo -e "Please run nodes/pbm_store_set.sh manually after editing nodes/storage-config.yaml\n"
    echo "#!/usr/bin/env bash" > ${WORKDIR}/pbm_store_set.sh
    chmod +x ${WORKDIR}/pbm_store_set.sh
    if [ "${LAYOUT}" == "single" ]; then
      echo "${WORKDIR}/pbm config --file=${WORKDIR}/storage-config.yaml --mongodb-uri='mongodb://${BACKUP_URI_AUTH}${HOST}:27017/${BACKUP_URI_SUFFIX}'" >> ${WORKDIR}/pbm_store_set.sh
    elif [ "${LAYOUT}" == "rs" ]; then
      local BACKUP_URI_SUFFIX_REPLICA=""
      if [ -z "${BACKUP_URI_SUFFIX}" ]; then BACKUP_URI_SUFFIX_REPLICA="?replicaSet=${RS1NAME}"; else BACKUP_URI_SUFFIX_REPLICA="${BACKUP_URI_SUFFIX}&replicaSet=${RS1NAME}"; fi
      echo "${WORKDIR}/pbm config --file=${WORKDIR}/storage-config.yaml --mongodb-uri='mongodb://${BACKUP_URI_AUTH}${HOST}:27017,${HOST}:27018,${HOST}:27019/${BACKUP_URI_SUFFIX_REPLICA}'" >> ${WORKDIR}/pbm_store_set.sh
    elif [ "${LAYOUT}" == "sh" ]; then
      echo "${WORKDIR}/pbm config --file=${WORKDIR}/storage-config.yaml --mongodb-uri='mongodb://${BACKUP_URI_AUTH}${HOST}:27017/${BACKUP_URI_SUFFIX}'" >> ${WORKDIR}/pbm_store_set.sh
    fi
  fi
}

# General prepare
if [ ${TLS} -eq 1 ]; then
  mkdir -p "${WORKDIR}/certificates"
  pushd "${WORKDIR}/certificates"
  echo -e "\n=== Generating TLS certificates in ${WORKDIR}/certificates ==="
  # Generate self signed root CA cert
  openssl req -nodes -x509 -newkey rsa:4096 -keyout ca.key -out ca.crt -subj "/C=US/ST=California/L=San Francisco/O=Percona/OU=root/CN=${HOST}/emailAddress=test@percona.com"
  # Generate server cert to be signed
  openssl req -nodes -newkey rsa:4096 -keyout server.key -out server.csr -subj "/C=US/ST=California/L=San Francisco/O=Percona/OU=server/CN=${HOST}/emailAddress=test@percona.com"
  # Sign server sert
  openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -set_serial 01 -out server.crt
  # Create server PEM file
  cat server.key server.crt > server.pem
  # Generate client cert to be signed
  openssl req -nodes -newkey rsa:4096 -keyout client.key -out client.csr -subj "/C=US/ST=California/L=San Francisco/O=Percona/OU=client/CN=${HOST}/emailAddress=test@percona.com"
  # Sign the client cert
  openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -set_serial 02 -out client.crt
  # Create client PEM file
  cat client.key client.crt > client.pem
  popd
  TLS_CLIENT="--tls --tlsCAFile ${WORKDIR}/certificates/ca.crt --tlsCertificateKeyFile ${WORKDIR}/certificates/client.pem"
  echo "TLS_CLIENT=\"${TLS_CLIENT}\"" >> ${WORKDIR}/COMMON
fi

# Prepare if running with PBM
mkdir -p "${WORKDIR}/backup"
# create symlinks to PBM binaries
ln -s ${PBMDIR}/pbm ${WORKDIR}/pbm
ln -s ${PBMDIR}/pbm-agent ${WORKDIR}/pbm-agent

# create startup/stop scripts for the whole PBM setup
echo "#!/usr/bin/env bash" > ${WORKDIR}/start_pbm.sh
chmod +x ${WORKDIR}/start_pbm.sh

echo "#!/usr/bin/env bash" > ${WORKDIR}/stop_pbm.sh
echo "killall pbm pbm-agent" >> ${WORKDIR}/stop_pbm.sh
chmod +x ${WORKDIR}/stop_pbm.sh

# Create storages config for node agent
if [ ! -z "${PBMDIR}" ]; then
   echo "storage:" >> ${WORKDIR}/storage-config.yaml
   echo "  type: filesystem" >> ${WORKDIR}/storage-config.yaml
   echo "  filesystem:" >> ${WORKDIR}/storage-config.yaml
   echo "    path: ${WORKDIR}/backup" >> ${WORKDIR}/storage-config.yaml
fi

# Run different configurations
if [ "${LAYOUT}" == "single" ]; then
  start_mongod "${WORKDIR}" "nors" "27017" "${STORAGE_ENGINE}" "${MONGOD_EXTRA}"

  if [[ "${MONGOD_EXTRA}" == *"replSet"* ]]; then
    ${BINDIR}/mongo ${HOST}:27017 --quiet ${TLS_CLIENT} --eval 'rs.initiate()'
    sleep 5
  fi

  if [ ! -z "${AUTH}" ]; then
    ${BINDIR}/mongo localhost:27017/admin --quiet ${TLS_CLIENT} --eval "db.createUser({ user: \"${MONGO_USER}\", pwd: \"${MONGO_PASS}\", roles: [ \"root\" ] });"
    sed -i "/^AUTH=/c\AUTH=\"--username=\${MONGO_USER} --password=\${MONGO_PASS} --authenticationDatabase=admin\"" ${WORKDIR}/COMMON
    sed -i "/^BACKUP_AUTH=/c\BACKUP_AUTH=\"--username=\${MONGO_BACKUP_USER} --password=\${MONGO_BACKUP_PASS} --authenticationDatabase=admin\"" ${WORKDIR}/COMMON
    sed -i "/^BACKUP_URI_AUTH=/c\BACKUP_URI_AUTH=\"\${MONGO_BACKUP_USER}:\${MONGO_BACKUP_PASS}@\"" ${WORKDIR}/COMMON
    ${BINDIR}/mongo ${HOST}:27017/admin ${AUTH} ${TLS_CLIENT} --quiet --eval "db.getSiblingDB(\"admin\").createRole( { role: \"pbmAnyAction\", privileges: [ { resource: { anyResource: true }, actions: [ \"anyAction\" ] } ], roles: [] } )"
    ${BINDIR}/mongo ${HOST}:27017/admin ${AUTH} ${TLS_CLIENT} --quiet --eval "db.createUser({ user: \"${MONGO_BACKUP_USER}\", pwd: \"${MONGO_BACKUP_PASS}\", roles: [ { db: \"admin\", role: \"readWrite\", collection: \"\" }, { db: \"admin\", role: \"backup\" }, { db: \"admin\", role: \"clusterMonitor\" }, { db: \"admin\", role: \"restore\" }, { db: \"admin\", role: \"pbmAnyAction\" } ] });"
  fi
fi

if [ "${LAYOUT}" == "rs" ]; then
  start_replicaset "${WORKDIR}" "${RS1NAME}" "27017" "${MONGOD_EXTRA}"
  if [ ! -z "${PBMDIR}" ]; then
    set_pbm_store
    ${WORKDIR}/start_pbm.sh
  fi
fi

if [ "${LAYOUT}" == "sh" ]; then
  SHPORT=27017
  CFGPORT=27027
  RS1PORT=27018
  RS2PORT=28018
  SHNAME="sh1"
  mkdir -p "${WORKDIR}/${RS1NAME}"
  mkdir -p "${WORKDIR}/${RS2NAME}"
  mkdir -p "${WORKDIR}/${CFGRSNAME}"
  mkdir -p "${WORKDIR}/${SHNAME}"

  if [ ${TLS} -eq 1 ]; then
    MONGOS_EXTRA="${MONGOS_EXTRA} --tlsMode requireTLS --tlsCertificateKeyFile ${WORKDIR}/certificates/server.pem --tlsCAFile ${WORKDIR}/certificates/ca.crt"
  fi

  echo -e "\n=== Configuring sharding cluster: ${SHNAME} ==="
  # setup config replicaset (3 node)
  start_replicaset "${WORKDIR}/${CFGRSNAME}" "${CFGRSNAME}" "${CFGPORT}" "--configsvr ${CONFIG_EXTRA}"

  # setup 2 data replica sets
  start_replicaset "${WORKDIR}/${RS1NAME}" "${RS1NAME}" "${RS1PORT}" "--shardsvr ${MONGOD_EXTRA}"
  start_replicaset "${WORKDIR}/${RS2NAME}" "${RS2NAME}" "${RS2PORT}" "--shardsvr ${MONGOD_EXTRA}"

  # create managing scripts
  echo "#!/usr/bin/env bash" > ${WORKDIR}/${SHNAME}/start_mongos.sh
  echo "echo \"=== Starting sharding server: ${SHNAME} on port ${SHPORT} ===\"" >> ${WORKDIR}/${SHNAME}/start_mongos.sh
  echo "${BINDIR}/mongos --port ${SHPORT} --configdb ${CFGRSNAME}/${HOST}:${CFGPORT},${HOST}:$(($CFGPORT + 1)),${HOST}:$(($CFGPORT + 2)) --logpath ${WORKDIR}/${SHNAME}/mongos.log --fork "$MONGOS_EXTRA" >/dev/null" >> ${WORKDIR}/${SHNAME}/start_mongos.sh
  echo "#!/usr/bin/env bash" > ${WORKDIR}/${SHNAME}/cl_mongos.sh
  echo "source ${WORKDIR}/COMMON" >> ${WORKDIR}/${SHNAME}/cl_mongos.sh
  echo "${BINDIR}/mongo ${HOST}:${SHPORT} \${AUTH} \${TLS_CLIENT} \$@" >> ${WORKDIR}/${SHNAME}/cl_mongos.sh
  ln -s ${WORKDIR}/${SHNAME}/cl_mongos.sh ${WORKDIR}/cl_mongos.sh
  echo "echo \"=== Stopping sharding cluster: ${SHNAME} ===\"" >> ${WORKDIR}/stop_mongodb.sh
  echo "${WORKDIR}/${SHNAME}/stop_mongos.sh" >> ${WORKDIR}/stop_mongodb.sh
  echo "${WORKDIR}/${RS1NAME}/stop_mongodb.sh" >> ${WORKDIR}/stop_mongodb.sh
  echo "${WORKDIR}/${RS2NAME}/stop_mongodb.sh" >> ${WORKDIR}/stop_mongodb.sh
  echo "${WORKDIR}/${CFGRSNAME}/stop_mongodb.sh" >> ${WORKDIR}/stop_mongodb.sh
  echo "#!/usr/bin/env bash" > ${WORKDIR}/${SHNAME}/stop_mongos.sh
  echo "source ${WORKDIR}/COMMON" >> ${WORKDIR}/${SHNAME}/stop_mongos.sh
  echo "echo \"Stopping mongos on port: ${SHPORT}\"" >> ${WORKDIR}/${SHNAME}/stop_mongos.sh
  echo "${BINDIR}/mongo localhost:${SHPORT}/admin --quiet --eval 'db.shutdownServer({force:true})' \${AUTH} \${TLS_CLIENT}" >> ${WORKDIR}/${SHNAME}/stop_mongos.sh
  echo "#!/usr/bin/env bash" > ${WORKDIR}/start_mongodb.sh
  echo "echo \"Starting sharding cluster on port: ${SHPORT}\"" >> ${WORKDIR}/start_mongodb.sh
  echo "${WORKDIR}/${CFGRSNAME}/start_mongodb.sh" >> ${WORKDIR}/start_mongodb.sh
  echo "${WORKDIR}/${RS1NAME}/start_mongodb.sh" >> ${WORKDIR}/start_mongodb.sh
  echo "${WORKDIR}/${RS2NAME}/start_mongodb.sh" >> ${WORKDIR}/start_mongodb.sh
  echo "${WORKDIR}/${SHNAME}/start_mongos.sh" >> ${WORKDIR}/start_mongodb.sh
  chmod +x ${WORKDIR}/${SHNAME}/start_mongos.sh
  chmod +x ${WORKDIR}/${SHNAME}/stop_mongos.sh
  chmod +x ${WORKDIR}/${SHNAME}/cl_mongos.sh
  chmod +x ${WORKDIR}/start_mongodb.sh
  chmod +x ${WORKDIR}/stop_mongodb.sh
  # start mongos
  ${WORKDIR}/${SHNAME}/start_mongos.sh
  if [ ! -z "${AUTH}" ]; then
    ${BINDIR}/mongo localhost:${SHPORT}/admin --quiet ${TLS_CLIENT} --eval "db.createUser({ user: \"${MONGO_USER}\", pwd: \"${MONGO_PASS}\", roles: [ \"root\", \"userAdminAnyDatabase\", \"clusterAdmin\" ] });"
    ${BINDIR}/mongo ${AUTH} ${TLS_CLIENT} localhost:${SHPORT}/admin --quiet --eval "db.getSiblingDB(\"admin\").createRole( { role: \"pbmAnyAction\", privileges: [ { resource: { anyResource: true }, actions: [ \"anyAction\" ] } ], roles: [] } )"
    ${BINDIR}/mongo ${AUTH} ${TLS_CLIENT} localhost:${SHPORT}/admin --quiet --eval "db.createUser({ user: \"${MONGO_BACKUP_USER}\", pwd: \"${MONGO_BACKUP_PASS}\", roles: [ { db: \"admin\", role: \"readWrite\", collection: \"\" }, { db: \"admin\", role: \"backup\" }, { db: \"admin\", role: \"clusterMonitor\" }, { db: \"admin\", role: \"restore\" }, { db: \"admin\", role: \"pbmAnyAction\" } ] });"
  fi
  # add Shards to the Cluster
  echo "Adding shards to the cluster..."
  sleep 20
  ${BINDIR}/mongo ${HOST}:${SHPORT} --quiet --eval "sh.addShard(\"${RS1NAME}/${HOST}:${RS1PORT}\")" ${AUTH} ${TLS_CLIENT}
  ${BINDIR}/mongo ${HOST}:${SHPORT} --quiet --eval "sh.addShard(\"${RS2NAME}/${HOST}:${RS2PORT}\")" ${AUTH} ${TLS_CLIENT}
  echo -e "\n>>> Enable sharding on specific database with: sh.enableSharding(\"<database>\") <<<"
  echo -e ">>> Shard a collection with: sh.shardCollection(\"<database>.<collection>\", { <key> : <direction> } ) <<<\n"

  # start a PBM agent on the config replica set node (needed here because auth is enabled through mongos)
  #start_pbm_agent "${WORKDIR}/${CFGRSNAME}" "${CFGRSNAME}" "${CFGPORT}"
  if [ ! -z "${PBMDIR}" ]; then
    for i in 1 2 3; do
      if [ ${RS_ARBITER} != 1 -o ${i} -lt 3 ]; then
        setup_pbm_agent "${WORKDIR}/${CFGRSNAME}/node${i}" "${CFGRSNAME}" "$(($CFGPORT + ${i} - 1))"
      fi
    done
    set_pbm_store
    ${WORKDIR}/start_pbm.sh
  fi
fi
