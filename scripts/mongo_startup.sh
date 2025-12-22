#!/usr/bin/env bash

################################################################################
# MongoDB/PSMDB Setup Script
################################################################################
#
# QUICK START EXAMPLES:
#
# 1. Start a simple 3-node replica set:
#    ./mongo_startup.sh -r
#
# 2. Start a sharded cluster (2 shards with 3 nodes each + config servers):
#    ./mongo_startup.sh -s
#
# 3. Start 2 replica sets (for testing multiple setups):
#    ./mongo_startup.sh -r -n 2
#    (Setup 1 ports: 27018-27020, Setup 2 ports: 37018-37020)
#
# 4. Start replica set with custom number of nodes:
#    ./mongo_startup.sh -r -c 5
#
# 5. Start with Percona Backup for MongoDB (PBM):
#    ./mongo_startup.sh -r --pbmDir=/usr/bin
#
# 6. Start with authentication enabled:
#    ./mongo_startup.sh -r --auth
#
# 7. Start sharded cluster with 1 node in RS, authentication and PBM:
#    ./mongo_startup.sh -s -c 1 --auth --pbmDir=/usr/bin
#
# 8. Start 2 RS with 1 node in each RS:
#    ./mongo_startup.sh -r -n 2 -c 1
#
# 9. Force recreate if setup already exists:
#    ./mongo_startup.sh -r -f
#
# IMPORTANT NOTES:
#
# - Uses MongoDB binaries from /usr/bin by default (override with --binDir)
# - Creates nodes in ./nodes directory (override with --workDir)
# - Port allocation avoids 27017 to not conflict with system mongod:
#   * Single replica set: 27018-27020 (3 nodes)
#   * Single sharded: mongos=27018, config=27028, shards=27038+
#   * Multiple setups: add 10000 per setup (37018, 47018, etc.)
# - Socket files stored in node directories (not /tmp) to avoid permission issues
# - Use -f/--force to clean up and recreate existing setup
#
# USEFUL COMMANDS AFTER SETUP:
#
# - Stop MongoDB: ./nodes/stop_mongodb.sh
# - Start MongoDB: ./nodes/start_mongodb.sh
# - Configure PBM storage: ./nodes/pbm_store_set.sh (edit storage-config.yaml first)
#
# For full options list, run: ./mongo_startup.sh --help
#
################################################################################

# dynamic
MONGO_USER="dba"
MONGO_PASS="test1234"
MONGO_BACKUP_USER="backupUser"
MONGO_BACKUP_PASS="test1234"
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
NUM_SETUPS=1
RS_NODES=3
ENCRYPTION="no"
PBMDIR=""
ENABLE_AUTH=0
AUTH=""
BACKUP_AUTH=""
BACKUP_URI_AUTH=""
BACKUP_URI_SUFFIX=""
TLS=0
TLS_CLIENT=""
FORCE=0

if [ -z "$1" ]; then
  echo "You need to specify at least one of the options for layout: -r (replicaset), -s (sharded cluster) or use --help!"
  exit 1
fi

if ! getopt --test
then
    go_out="$(getopt --options=rsahe:b:o:tp:n:c:xw:f \
        --longoptions=rSet,sCluster,arbiter,hidden,delayed,help,storageEngine:,binDir:,host:,mongodExtra:,mongosExtra:,configExtra:,encrypt,pbmDir:,auth,tls,workDir:,numSetups:,rsNodes:,force \
        --name="$(basename "$0")" -- "$@")"
    test $? -eq 0 || exit 1
    eval set -- $go_out
fi

for arg
do
  case "$arg" in
  -- ) shift; break;;
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
    echo -e "\nThis script can be used to setup replica set or sharded cluster of mongod or psmdb."
    echo -e "By default it uses binaries from /usr/bin and creates nodes in the \"nodes\" subdirectory."
    echo -e "You can override these defaults with --binDir and --workDir options.\n"
    echo -e "Note: Short options require space (e.g., -n 2). Long options use = (e.g., --numSetups=2).\n"
    echo -e "      Flag options like --arbiter, --hidden, --auth don't take arguments.\n"
    echo -e "Options:"
    echo -e "-r, --rSet\t\t\t run replica set (3 nodes by default)"
    echo -e "-s, --sCluster\t\t\t run sharding cluster (2 replica sets with 3 nodes each by default)"
    echo -e "-a, --arbiter\t\t\t add arbiter node (requires at least 2 normal nodes)"
    echo -e "-n <num>, --numSetups=<num>\t number of setups to create (default: 1)"
    echo -e "-c <num>, --rsNodes=<num>\t number of nodes in replica set (default: 3)"
    echo -e "-e <se>, --storageEngine=<se>\t specify storage engine for data nodes (wiredTiger, mmapv1)"
    echo -e "-b <path>, --binDir=<path>\t specify binary directory if running from some other location (this should end with /bin)"
    echo -e "-w <path>, --workDir=<path>\t specify work directory if not current"
    echo -e "-o <name>, --host=<name>\t instead of localhost specify some hostname for MongoDB setup"
    echo -e "--mongodExtra=\"...\"\t\t specify extra options to pass to mongod"
    echo -e "--mongosExtra=\"...\"\t\t specify extra options to pass to mongos"
    echo -e "--configExtra=\"...\"\t\t specify extra options to pass to config server"
    echo -e "--tls\t\t\t\t generate tls certificates and start nodes with requiring tls connection"
    echo -e "-t, --encrypt\t\t\t enable data at rest encryption with keyfile (wiredTiger only)"
    echo -e "-p <path>, --pbmDir=<path>\t enables Percona Backup for MongoDB (starts agents from binaries)"
    echo -e "-x, --auth\t\t\t enable authentication"
    echo -e "-f, --force\t\t\t remove existing nodes directory and its contents before creating new setup"
    echo -e "-h, --help\t\t\t this help"
    echo -e "--hidden\t\t\t add hidden node (requires at least 1 normal node)"
    echo -e "--delayed\t\t\t add delayed node with 600s delay (requires at least 1 normal node)"
    exit 0
    ;;
  -e | --storageEngine )
    shift
    STORAGE_ENGINE="$1"
    shift
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
  -t | --encrypt )
    shift
    ENCRYPTION="keyfile"
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
  -n | --numSetups )
    shift
    NUM_SETUPS="$1"
    shift
    ;;
  -c | --rsNodes )
    shift
    RS_NODES="$1"
    shift
    ;;
  -x | --auth )
    shift
    ENABLE_AUTH=1
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
  -f | --force )
    shift
    FORCE=1
    ;;
  esac
done

if [ "${STORAGE_ENGINE}" != "wiredTiger" ] && [ "${ENCRYPTION}" != "no" ]; then
  echo "ERROR: Data at rest encryption is possible only with wiredTiger storage engine!"
  exit 1
fi
TOTAL_SPECIAL_NODES=$((RS_ARBITER + RS_HIDDEN + RS_DELAYED))
MIN_NORMAL_NODES=1

if [ ${RS_ARBITER} = 1 ]; then
  MIN_NORMAL_NODES=2
fi

MIN_TOTAL_NODES=$((MIN_NORMAL_NODES + TOTAL_SPECIAL_NODES))

if [ ${RS_NODES} -lt ${MIN_TOTAL_NODES} ]; then
  echo "ERROR: Not enough nodes for the requested configuration."
  echo "  Arbiter: ${RS_ARBITER}, Hidden: ${RS_HIDDEN}, Delayed: ${RS_DELAYED}"
  echo "  Minimum required nodes: ${MIN_TOTAL_NODES} (${MIN_NORMAL_NODES} normal + ${TOTAL_SPECIAL_NODES} special)"
  echo "  Current nodes: ${RS_NODES}"
  exit 1
fi
if [ -z "${BINDIR}" ]; then
  BINDIR="/usr/bin"
fi
if [ -z "${WORKDIR}" ]; then
  WORKDIR="${BASEDIR}/nodes"
fi
if [ ! -x "${BINDIR}/mongod" ]; then
  echo "${BINDIR}/mongod doesn't exist or is not executable!"
  exit 1
elif [ ! -x "${BINDIR}/mongos" ]; then
  echo "${BINDIR}/mongos doesn't exist or is not executable!"
  exit 1
fi
if [ -x "${BINDIR}/mongosh" ]; then
  MONGO="${BINDIR}/mongosh"
elif [ -x "${BINDIR}/mongo" ]; then
  MONGO="${BINDIR}/mongo"
else
  echo "Neither ${BINDIR}/mongo nor ${BINDIR}/mongosh exists or is executable!"
  exit 1
fi
if [ -n "${PBMDIR}" ] && [ ! -x "${PBMDIR}/pbm" ]; then
  echo "${PBMDIR}/pbm doesn't exist or is not executable!"
  exit 1
elif [ -n "${PBMDIR}" ] && [ ! -x "${PBMDIR}/pbm-agent" ]; then
  echo "${PBMDIR}/pbm-agent doesn't exist or is not executable!"
  exit 1
fi
if [ -d "${WORKDIR}" ]; then
  if [ ${FORCE} -eq 1 ]; then
    echo "Stopping any running MongoDB processes from existing setup..."
    if [ -f "${WORKDIR}/stop_mongodb.sh" ]; then
      echo "Running existing stop script..."
      ${WORKDIR}/stop_mongodb.sh 2>/dev/null || true
      sleep 3
    fi
    if [ -f "${WORKDIR}/stop_pbm.sh" ]; then
      echo "Stopping PBM agents..."
      ${WORKDIR}/stop_pbm.sh 2>/dev/null || true
      sleep 2
    fi
    echo "Cleaning up any remaining processes..."
    pkill -f "mongod.*--dbpath ${WORKDIR}" 2>/dev/null || true
    pkill -f "mongos.*--logpath ${WORKDIR}" 2>/dev/null || true
    pkill -f "pbm-agent.*--mongodb-uri=.*${WORKDIR}" 2>/dev/null || true
    echo "Waiting for ports to be released..."
    max_wait=15
    count=0
    while [ ${count} -lt ${max_wait} ]; do
      ports_in_use=0
      for port in 27018 27019 27020 27028 27038 28038 37018 37019 37020 37028 37038 38038 47018 47019 47020; do
        if lsof -ti:${port} >/dev/null 2>&1; then
          ports_in_use=1
          break
        fi
      done
      if [ ${ports_in_use} -eq 0 ]; then
        echo " All ports released"
        break
      fi
      sleep 1
      count=$((count + 1))
      echo -n "."
    done
    if [ ${count} -ge ${max_wait} ]; then
      echo " WARNING: Timeout waiting for ports to be released"
    fi
    echo "Removing existing ${WORKDIR} and its contents..."
    rm -rf "${WORKDIR}"
    mkdir "${WORKDIR}"
  else
    echo "ERROR: ${WORKDIR} already exists"
    echo "Use --force or -f option to remove existing directory and create new setup"
    exit 1
  fi
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

if [ ${ENABLE_AUTH} -eq 1 ]; then
  openssl rand -base64 756 > ${WORKDIR}/keyFile
  chmod 400 ${WORKDIR}/keyFile
  MONGOD_EXTRA="${MONGOD_EXTRA} --keyFile ${WORKDIR}/keyFile"
  MONGOS_EXTRA="${MONGOS_EXTRA} --keyFile ${WORKDIR}/keyFile"
  CONFIG_EXTRA="${CONFIG_EXTRA} --keyFile ${WORKDIR}/keyFile"
fi

wait_for_port(){
  local PORT="$1"
  local LOGFILE="$2"
  local TIMEOUT=60
  local COUNT=0
  echo -n "Waiting for mongod on port ${PORT} to be ready..."
  while [ ${COUNT} -lt ${TIMEOUT} ]; do
    if ${MONGO} localhost:${PORT} --quiet ${TLS_CLIENT} --eval "db.adminCommand('ping')" >/dev/null 2>&1; then
      echo " OK"
      return 0
    fi
    if [ ${COUNT} -gt 5 ]; then
      if ! lsof -ti:${PORT} >/dev/null 2>&1; then
        echo " FAILED"
        echo "ERROR: No process listening on port ${PORT}"
        if [ -f "${LOGFILE}" ]; then
          echo "Last 20 lines from ${LOGFILE}:"
          tail -20 "${LOGFILE}"
        fi
        return 1
      fi
    fi
    sleep 1
    COUNT=$((COUNT + 1))
    echo -n "."
  done
  echo " TIMEOUT"
  echo "ERROR: mongod on port ${PORT} failed to start within ${TIMEOUT} seconds"
  if [ -f "${LOGFILE}" ]; then
    echo "Last 20 lines from ${LOGFILE}:"
    tail -20 "${LOGFILE}"
  fi
  return 1
}

setup_pbm_agent(){
  local NDIR="$1"
  local RS="$2"
  local NPORT="$3"

  rm -f "${NDIR}/pbm-agent"
  mkdir -p "${NDIR}/pbm-agent"

  if [ -n "${PBMDIR}" ]; then
    echo "#!/usr/bin/env bash" > ${NDIR}/pbm-agent/start_pbm_agent.sh
    echo "source ${WORKDIR}/COMMON" >> ${NDIR}/pbm-agent/start_pbm_agent.sh
    echo "echo '=== Starting pbm-agent for mongod on port: ${NPORT} replicaset: ${RS} ==='" >> ${NDIR}/pbm-agent/start_pbm_agent.sh
    echo "${PBMDIR}/pbm-agent --mongodb-uri=\"mongodb://\${BACKUP_URI_AUTH}${HOST}:${NPORT}/${BACKUP_URI_SUFFIX}\" 1>${NDIR}/pbm-agent/stdout.log 2>${NDIR}/pbm-agent/stderr.log &" >> ${NDIR}/pbm-agent/start_pbm_agent.sh
    chmod +x ${NDIR}/pbm-agent/start_pbm_agent.sh
    echo "${NDIR}/pbm-agent/start_pbm_agent.sh" >> ${WORKDIR}/start_pbm.sh

    echo "#!/usr/bin/env bash" > ${NDIR}/pbm-agent/stop_pbm_agent.sh
    echo "kill \$(cat ${NDIR}/pbm-agent/pbm-agent.pid)" >> ${NDIR}/pbm-agent/stop_pbm_agent.sh
    chmod +x ${NDIR}/pbm-agent/stop_pbm_agent.sh

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
  mkdir -p ${NDIR}/db
  if [ ${RS} != "nors" ]; then
    EXTRA="${EXTRA} --replSet ${RS}"
  fi
  if [ "${SE}" = "wiredTiger" ]; then
    EXTRA="${EXTRA} --wiredTigerCacheSizeGB 1"
    if [ "${ENCRYPTION}" = "keyfile" ]; then
      openssl rand -base64 32 > ${NDIR}/mongodb-keyfile
      chmod 600 ${NDIR}/mongodb-keyfile
      EXTRA="${EXTRA} --enableEncryption --encryptionKeyFile ${NDIR}/mongodb-keyfile --encryptionCipherMode AES256-CBC"
    fi
  fi
  if [ ${TLS} -eq 1 ]; then
    EXTRA="${EXTRA} --tlsMode requireTLS --tlsCertificateKeyFile ${WORKDIR}/certificates/server.pem --tlsCAFile ${WORKDIR}/certificates/ca.crt"
  fi

  # Use socket file in node directory to avoid /tmp permission issues
  EXTRA="${EXTRA} --unixSocketPrefix ${NDIR}"

  echo "#!/usr/bin/env bash" > ${NDIR}/start.sh
  echo "source ${WORKDIR}/COMMON" >> ${NDIR}/start.sh
  echo "echo \"Starting mongod on port: ${PORT} storage engine: ${SE} replica set: ${RS#nors}\"" >> ${NDIR}/start.sh
  echo "ENABLE_AUTH=\"\"" >> ${NDIR}/start.sh
  echo "if [ -n \"\${AUTH}\" ]; then ENABLE_AUTH=\"--auth\"; fi" >> ${NDIR}/start.sh
  if [ "${NTYPE}" = "arbiter" ]; then
      echo "if ! ${BINDIR}/mongod --port ${PORT} --storageEngine ${SE} --dbpath ${NDIR}/db --logpath ${NDIR}/mongod.log --fork ${EXTRA} 2>&1 | grep -q 'forked process'; then" >> ${NDIR}/start.sh
      echo "  echo \"ERROR: Failed to start mongod on port ${PORT}\"" >> ${NDIR}/start.sh
      echo "  echo \"Check ${NDIR}/mongod.log for details\"" >> ${NDIR}/start.sh
      echo "  exit 1" >> ${NDIR}/start.sh
      echo "fi" >> ${NDIR}/start.sh
  else
      echo "if ! ${BINDIR}/mongod \${ENABLE_AUTH} --port ${PORT} --storageEngine ${SE} --dbpath ${NDIR}/db --logpath ${NDIR}/mongod.log --fork ${EXTRA} 2>&1 | grep -q 'forked process'; then" >> ${NDIR}/start.sh
      echo "  echo \"ERROR: Failed to start mongod on port ${PORT}\"" >> ${NDIR}/start.sh
      echo "  echo \"Check ${NDIR}/mongod.log for details\"" >> ${NDIR}/start.sh
      echo "  exit 1" >> ${NDIR}/start.sh
      echo "fi" >> ${NDIR}/start.sh
  fi
  echo "#!/usr/bin/env bash" > ${NDIR}/stop.sh
  echo "source ${WORKDIR}/COMMON" >> ${NDIR}/stop.sh
  echo "echo \"Stopping mongod on port: ${PORT} storage engine: ${SE} replica set: ${RS#nors}\"" >> ${NDIR}/stop.sh
  if [ "${NTYPE}" = "arbiter" ]; then
      echo "timeout 10 ${MONGO} localhost:${PORT}/admin --quiet --eval 'db.shutdownServer({force:true,timeoutSecs:2})' \${TLS_CLIENT} 2>/dev/null || true" >> ${NDIR}/stop.sh
  else
      echo "timeout 10 ${MONGO} localhost:${PORT}/admin --quiet --eval 'db.shutdownServer({force:true,timeoutSecs:2})' \${AUTH} \${TLS_CLIENT} 2>/dev/null || true" >> ${NDIR}/stop.sh
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
  local nodes=()
  local normal_nodes=$((RS_NODES - RS_ARBITER - RS_HIDDEN - RS_DELAYED))

  for ((i=1; i<=normal_nodes; i++)); do
    nodes+=("config")
  done

  if [ ${RS_HIDDEN} = 1 ]; then
    nodes+=("hidden")
  fi
  if [ ${RS_DELAYED} = 1 ]; then
    nodes+=("delayed")
  fi
  if [ ${RS_ARBITER} = 1 ] && [ "${RSNAME}" != "config" ]; then
    nodes+=("arbiter")
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
  for i in "${!nodes[@]}"; do
    node_number=$(($i + 1))
    wait_for_port "$(($RSBASEPORT + ${i}))" "${RSDIR}/node${node_number}/mongod.log" || exit 1
  done
  echo "#!/usr/bin/env bash" > ${RSDIR}/init_rs.sh
  echo "source ${WORKDIR}/COMMON" >> ${RSDIR}/init_rs.sh
  echo "echo \"Initializing replica set: ${RSNAME}\"" >> ${RSDIR}/init_rs.sh
  MEMBERS="["
  local member_id=1

  for ((i=0; i<normal_nodes; i++)); do
    if [ ${member_id} -gt 1 ]; then
      MEMBERS="${MEMBERS},"
    fi
    MEMBERS="${MEMBERS}{\"_id\":${member_id}, \"host\":\"${HOST}:$(($RSBASEPORT + ${i}))\"}"
    member_id=$((member_id + 1))
  done

  if [ ${RS_HIDDEN} = 1 ]; then
    if [ ${member_id} -gt 1 ]; then
      MEMBERS="${MEMBERS},"
    fi
    MEMBERS="${MEMBERS}{\"_id\":${member_id}, \"host\":\"${HOST}:$(($RSBASEPORT + normal_nodes))\", \"priority\":0, \"hidden\":true}"
    member_id=$((member_id + 1))
  fi
  if [ ${RS_DELAYED} = 1 ]; then
    if [ ${member_id} -gt 1 ]; then
      MEMBERS="${MEMBERS},"
    fi
    local delayed_port_offset=$((normal_nodes + RS_HIDDEN))
    MEMBERS="${MEMBERS}{\"_id\":${member_id}, \"host\":\"${HOST}:$(($RSBASEPORT + delayed_port_offset))\", \"priority\":0, \"secondaryDelaySecs\":600}"
    member_id=$((member_id + 1))
  fi
  if [ ${RS_ARBITER} = 1 ] && [ "${RSNAME}" != "config" ]; then
    if [ ${member_id} -gt 1 ]; then
      MEMBERS="${MEMBERS},"
    fi
    local arbiter_port_offset=$((normal_nodes + RS_HIDDEN + RS_DELAYED))
    MEMBERS="${MEMBERS}{\"_id\":${member_id}, \"host\":\"${HOST}:$(($RSBASEPORT + arbiter_port_offset))\",\"arbiterOnly\":true}"
  fi

  MEMBERS="${MEMBERS}]"

  # Use second node for initialization if available (more stable for multi-node setups)
  local init_port=${RSBASEPORT}
  if [ ${RS_NODES} -gt 1 ]; then
    init_port=$(($RSBASEPORT + 1))
  fi

  echo "${MONGO} localhost:${init_port} --quiet \${TLS_CLIENT} --eval 'rs.initiate({_id:\"${RSNAME}\", members: ${MEMBERS}})'" >> ${RSDIR}/init_rs.sh
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
  chmod +x ${RSDIR}/init_rs.sh
  chmod +x ${RSDIR}/start_mongodb.sh
  chmod +x ${RSDIR}/stop_mongodb.sh
  if ! ${RSDIR}/init_rs.sh; then
    echo "ERROR: Failed to initialize replica set ${RSNAME}"
    exit 1
  fi

  if [ ${RS_DELAYED} = 1 ]; then
    sleep 3
    local delayed_check=$(${MONGO} localhost:${init_port} --quiet ${TLS_CLIENT} --eval "rs.conf().members.find(m => m.secondaryDelaySecs > 0) ? 'OK' : 'MISSING'" 2>/dev/null || echo "ERROR")
    if [ "${delayed_check}" != "OK" ]; then
      echo "WARNING: Delayed node configuration may not have been applied correctly"
    fi
  fi

  # for config server this is done via mongos
  if [ ${ENABLE_AUTH} -eq 1 ] && [ "${RSNAME}" != "config" ]; then
    if [ ${RS_NODES} -eq 1 ]; then
      sleep 5
      ${MONGO} localhost:${RSBASEPORT} --quiet ${TLS_CLIENT} --eval "db.getSiblingDB(\"admin\").createUser({ user: \"${MONGO_USER}\", pwd: \"${MONGO_PASS}\", roles: [ \"root\" ] })"
      AUTH="--username=${MONGO_USER} --password=${MONGO_PASS} --authenticationDatabase=admin"
      ${MONGO} ${AUTH} ${TLS_CLIENT} localhost:${RSBASEPORT} --quiet --eval "db.getSiblingDB(\"admin\").createRole( { role: \"pbmAnyAction\", privileges: [ { resource: { anyResource: true }, actions: [ \"anyAction\" ] } ], roles: [] } )"
      ${MONGO} ${AUTH} ${TLS_CLIENT} localhost:${RSBASEPORT} --quiet --eval "db.getSiblingDB(\"admin\").createUser({ user: \"${MONGO_BACKUP_USER}\", pwd: \"${MONGO_BACKUP_PASS}\", roles: [ { db: \"admin\", role: \"readWrite\", collection: \"\" }, { db: \"admin\", role: \"backup\" }, { db: \"admin\", role: \"clusterMonitor\" }, { db: \"admin\", role: \"restore\" }, { db: \"admin\", role: \"pbmAnyAction\" } ] })"
    else
      # Wait for primary election after rs.initiate()
      echo -n "Waiting for primary election..."
      local PRIMARY=""
      for i in {1..30}; do
        PRIMARY=$(${MONGO} localhost:${RSBASEPORT} --quiet ${TLS_CLIENT} --eval "db.hello().primary" 2>/dev/null | tail -n1 | cut -d':' -f2)
        if [ -n "${PRIMARY}" ] && [ "${PRIMARY}" != "null" ]; then
          echo " PRIMARY is localhost:${PRIMARY}"
          break
        fi
        sleep 1
        echo -n "."
      done
      if [ -z "${PRIMARY}" ] || [ "${PRIMARY}" = "null" ]; then
        echo " FAILED"
        echo "ERROR: Failed to determine primary node after 30 seconds"
        exit 1
      fi
      ${MONGO} localhost:${PRIMARY} --quiet ${TLS_CLIENT} --eval "db.getSiblingDB(\"admin\").createUser({ user: \"${MONGO_USER}\", pwd: \"${MONGO_PASS}\", roles: [ \"root\" ] })" || {
        echo "ERROR: Failed to create admin user"
        exit 1
      }
      AUTH="--username=${MONGO_USER} --password=${MONGO_PASS} --authenticationDatabase=admin"
      # Wait for user to replicate across the replica set
      sleep 5
      # Connect directly to PRIMARY for subsequent operations (more reliable than connection string immediately after user creation)
      ${MONGO} localhost:${PRIMARY} ${AUTH} ${TLS_CLIENT} --quiet --eval "db.getSiblingDB(\"admin\").createRole( { role: \"pbmAnyAction\", privileges: [ { resource: { anyResource: true }, actions: [ \"anyAction\" ] } ], roles: [] } )" || echo "Warning: Failed to create pbmAnyAction role"
      ${MONGO} localhost:${PRIMARY} ${AUTH} ${TLS_CLIENT} --quiet --eval "db.getSiblingDB(\"admin\").createUser({ user: \"${MONGO_BACKUP_USER}\", pwd: \"${MONGO_BACKUP_PASS}\", roles: [ { db: \"admin\", role: \"readWrite\", collection: \"\" }, { db: \"admin\", role: \"backup\" }, { db: \"admin\", role: \"clusterMonitor\" }, { db: \"admin\", role: \"restore\" }, { db: \"admin\", role: \"pbmAnyAction\" } ] })" || echo "Warning: Failed to create backup user"
    fi
    sed -i "/^AUTH=/c\AUTH=\"--username=\${MONGO_USER} --password=\${MONGO_PASS} --authenticationDatabase=admin\"" ${WORKDIR}/COMMON
    sed -i "/^BACKUP_AUTH=/c\BACKUP_AUTH=\"--username=\${MONGO_BACKUP_USER} --password=\${MONGO_BACKUP_PASS} --authenticationDatabase=admin\"" ${WORKDIR}/COMMON
    sed -i "/^BACKUP_URI_AUTH=/c\BACKUP_URI_AUTH=\"\${MONGO_BACKUP_USER}:\${MONGO_BACKUP_PASS}@\"" ${WORKDIR}/COMMON
  fi
  # for config server replica set this is done in another place after cluster user is added
  if [ -n "${PBMDIR}" ] && [ "${RSNAME}" != "config" ]; then
    sleep 5
    # Skip PBM agent setup for arbiter nodes (arbiters are always the last node)
    for ((i=1; i<=RS_NODES; i++)); do
      local is_arbiter=0
      if [ ${RS_ARBITER} = 1 ] && [ ${i} -gt $((normal_nodes + RS_HIDDEN + RS_DELAYED)) ]; then
        is_arbiter=1
      fi
      if [ ${is_arbiter} = 0 ]; then
        setup_pbm_agent "${RSDIR}/node${i}" "${RSNAME}" "$(($RSBASEPORT + ${i} - 1))"
      fi
    done
  fi
}

set_pbm_store(){
  if [ -n "${PBMDIR}" ]; then
    echo "=== Setting PBM store config... ==="
    echo -e "Please run setup/pbm_store_set.sh manually after editing setup/storage-config.yaml\n"
    echo "#!/usr/bin/env bash" > ${WORKDIR}/pbm_store_set.sh
    chmod +x ${WORKDIR}/pbm_store_set.sh
    if [ "${LAYOUT}" = "rs" ]; then
      for ((setup=1; setup<=NUM_SETUPS; setup++)); do
        if [ ${NUM_SETUPS} -gt 1 ]; then
          RSNAME="rs${setup}"
          RSPORT=$((27018 + (setup-1) * 10000))
        else
          RSNAME="rs1"
          RSPORT=27018
        fi
        local BACKUP_URI_SUFFIX_REPLICA=""
        if [ -z "${BACKUP_URI_SUFFIX}" ]; then BACKUP_URI_SUFFIX_REPLICA="?replicaSet=${RSNAME}"; else BACKUP_URI_SUFFIX_REPLICA="${BACKUP_URI_SUFFIX}&replicaSet=${RSNAME}"; fi
        local URI_HOSTS=""
        for ((i=0; i<RS_NODES; i++)); do
          if [ ${i} -gt 0 ]; then
            URI_HOSTS="${URI_HOSTS},"
          fi
          URI_HOSTS="${URI_HOSTS}${HOST}:$((RSPORT + i))"
        done
        echo "${WORKDIR}/pbm config --file=${WORKDIR}/storage-config.yaml --mongodb-uri='mongodb://${BACKUP_URI_AUTH}${URI_HOSTS}/${BACKUP_URI_SUFFIX_REPLICA}'" >> ${WORKDIR}/pbm_store_set.sh
      done
    elif [ "${LAYOUT}" = "sh" ]; then
      for ((setup=1; setup<=NUM_SETUPS; setup++)); do
        if [ ${NUM_SETUPS} -gt 1 ]; then
          SHPORT=$((27018 + (setup-1) * 10000))
        else
          SHPORT=27018
        fi
        echo "${WORKDIR}/pbm config --file=${WORKDIR}/storage-config.yaml --mongodb-uri='mongodb://${BACKUP_URI_AUTH}${HOST}:${SHPORT}/${BACKUP_URI_SUFFIX}'" >> ${WORKDIR}/pbm_store_set.sh
      done
    fi
  fi
}
if [ ${TLS} -eq 1 ]; then
  mkdir -p "${WORKDIR}/certificates"
  pushd "${WORKDIR}/certificates"
  echo -e "\n=== Generating TLS certificates in ${WORKDIR}/certificates ==="
  openssl req -nodes -x509 -newkey rsa:4096 -keyout ca.key -out ca.crt -subj "/C=US/ST=California/L=San Francisco/O=Percona/OU=root/CN=${HOST}/emailAddress=test@percona.com"
  openssl req -nodes -newkey rsa:4096 -keyout server.key -out server.csr -subj "/C=US/ST=California/L=San Francisco/O=Percona/OU=server/CN=${HOST}/emailAddress=test@percona.com"
  openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -set_serial 01 -out server.crt
  cat server.key server.crt > server.pem
  openssl req -nodes -newkey rsa:4096 -keyout client.key -out client.csr -subj "/C=US/ST=California/L=San Francisco/O=Percona/OU=client/CN=${HOST}/emailAddress=test@percona.com"
  openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -set_serial 02 -out client.crt
  cat client.key client.crt > client.pem
  popd
  TLS_CLIENT="--tls --tlsCAFile ${WORKDIR}/certificates/ca.crt --tlsCertificateKeyFile ${WORKDIR}/certificates/client.pem"
  echo "TLS_CLIENT=\"${TLS_CLIENT}\"" >> ${WORKDIR}/COMMON
fi
if [ -n "${PBMDIR}" ]; then
  mkdir -p "${WORKDIR}/backup"
  # create symlinks to PBM binaries
  ln -s ${PBMDIR}/pbm ${WORKDIR}/pbm
  ln -s ${PBMDIR}/pbm-agent ${WORKDIR}/pbm-agent

  echo "#!/usr/bin/env bash" > ${WORKDIR}/start_pbm.sh
  chmod +x ${WORKDIR}/start_pbm.sh

  echo "#!/usr/bin/env bash" > ${WORKDIR}/stop_pbm.sh
  echo "killall pbm pbm-agent" >> ${WORKDIR}/stop_pbm.sh
  chmod +x ${WORKDIR}/stop_pbm.sh
fi
if [ -n "${PBMDIR}" ]; then
  echo "storage:" >> ${WORKDIR}/storage-config.yaml
  echo "  type: filesystem" >> ${WORKDIR}/storage-config.yaml
  echo "  filesystem:" >> ${WORKDIR}/storage-config.yaml
  echo "    path: ${WORKDIR}/backup" >> ${WORKDIR}/storage-config.yaml
fi
if [ "${LAYOUT}" = "rs" ]; then
  if [ ${NUM_SETUPS} -gt 1 ]; then
    echo "#!/usr/bin/env bash" > ${WORKDIR}/start_mongodb.sh
    echo "echo \"=== Starting all replica sets ===\"" >> ${WORKDIR}/start_mongodb.sh
    echo "#!/usr/bin/env bash" > ${WORKDIR}/stop_mongodb.sh
    echo "echo \"=== Stopping all replica sets ===\"" >> ${WORKDIR}/stop_mongodb.sh
  fi

  for ((setup=1; setup<=NUM_SETUPS; setup++)); do
    if [ ${NUM_SETUPS} -gt 1 ]; then
      RSNAME="rs${setup}"
      RSDIR="${WORKDIR}/rs${setup}"
      RSPORT=$((27018 + (setup-1) * 10000))
    else
      RSNAME="rs1"
      RSDIR="${WORKDIR}"
      RSPORT=27018
    fi
    start_replicaset "${RSDIR}" "${RSNAME}" "${RSPORT}" "${MONGOD_EXTRA}"

    if [ ${NUM_SETUPS} -gt 1 ]; then
      echo "${RSDIR}/start_mongodb.sh" >> ${WORKDIR}/start_mongodb.sh
      echo "${RSDIR}/stop_mongodb.sh" >> ${WORKDIR}/stop_mongodb.sh
    fi
  done

  if [ ${NUM_SETUPS} -gt 1 ]; then
    chmod +x ${WORKDIR}/start_mongodb.sh
    chmod +x ${WORKDIR}/stop_mongodb.sh
  fi
  if [ -n "${PBMDIR}" ]; then
    set_pbm_store
    ${WORKDIR}/start_pbm.sh
  fi
  echo ""
  echo "=========================================="
  echo "MongoDB Connection URIs:"
  echo "=========================================="
  for ((setup=1; setup<=NUM_SETUPS; setup++)); do
    if [ ${NUM_SETUPS} -gt 1 ]; then
      RSNAME="rs${setup}"
      RSPORT=$((27018 + (setup-1) * 10000))
    else
      RSNAME="rs1"
      RSPORT=27018
    fi
    URI_HOSTS=""
    for ((i=0; i<RS_NODES; i++)); do
      if [ ${i} -gt 0 ]; then
        URI_HOSTS="${URI_HOSTS},"
      fi
      URI_HOSTS="${URI_HOSTS}${HOST}:$((RSPORT + i))"
    done

    if [ -n "${AUTH}" ]; then
      echo "Replica Set ${RSNAME}:"
      echo "  mongodb://${MONGO_USER}:${MONGO_PASS}@${URI_HOSTS}/?replicaSet=${RSNAME}&authSource=admin"
    else
      echo "Replica Set ${RSNAME}:"
      echo "  mongodb://${URI_HOSTS}/?replicaSet=${RSNAME}"
    fi
  done
  echo "=========================================="
  echo ""
fi
if [ "${LAYOUT}" = "sh" ]; then
  if [ ${NUM_SETUPS} -gt 1 ]; then
    echo "#!/usr/bin/env bash" > ${WORKDIR}/start_mongodb.sh
    echo "echo \"=== Starting all sharded clusters ===\"" >> ${WORKDIR}/start_mongodb.sh
    echo "#!/usr/bin/env bash" > ${WORKDIR}/stop_mongodb.sh
    echo "echo \"=== Stopping all sharded clusters ===\"" >> ${WORKDIR}/stop_mongodb.sh
  fi

  for ((setup=1; setup<=NUM_SETUPS; setup++)); do
    if [ ${NUM_SETUPS} -gt 1 ]; then
      SHPORT=$((27018 + (setup-1) * 10000))
      CFGPORT=$((27028 + (setup-1) * 10000))
      RS1PORT=$((27038 + (setup-1) * 10000))
      RS2PORT=$((28038 + (setup-1) * 10000))
      SHNAME="sh${setup}"
      RS1NAME="rs${setup}_1"
      RS2NAME="rs${setup}_2"
      CFGRSNAME="config${setup}"
      SETUPDIR="${WORKDIR}/sh${setup}"
    else
      SHPORT=27018
      CFGPORT=27028
      RS1PORT=27038
      RS2PORT=28038
      SHNAME="sh1"
      RS1NAME="rs1"
      RS2NAME="rs2"
      CFGRSNAME="config"
      SETUPDIR="${WORKDIR}"
    fi
    mkdir -p "${SETUPDIR}/${RS1NAME}"
    mkdir -p "${SETUPDIR}/${RS2NAME}"
    mkdir -p "${SETUPDIR}/${CFGRSNAME}"
    mkdir -p "${SETUPDIR}/${SHNAME}"

    MONGOS_TLS_OPTS=""
    if [ ${TLS} -eq 1 ]; then
      MONGOS_TLS_OPTS="--tlsMode requireTLS --tlsCertificateKeyFile ${WORKDIR}/certificates/server.pem --tlsCAFile ${WORKDIR}/certificates/ca.crt"
    fi

    echo -e "\n=== Configuring sharding cluster: ${SHNAME} ==="
    start_replicaset "${SETUPDIR}/${CFGRSNAME}" "${CFGRSNAME}" "${CFGPORT}" "--configsvr ${CONFIG_EXTRA}"

    start_replicaset "${SETUPDIR}/${RS1NAME}" "${RS1NAME}" "${RS1PORT}" "--shardsvr ${MONGOD_EXTRA}"
    start_replicaset "${SETUPDIR}/${RS2NAME}" "${RS2NAME}" "${RS2PORT}" "--shardsvr ${MONGOD_EXTRA}"

    echo "#!/usr/bin/env bash" > ${SETUPDIR}/${SHNAME}/start_mongos.sh
    echo "echo \"=== Starting sharding server: ${SHNAME} on port ${SHPORT} ===\"" >> ${SETUPDIR}/${SHNAME}/start_mongos.sh
    if [ ${RS_NODES} -eq 1 ]; then
      configdb_string="${CFGRSNAME}/${HOST}:${CFGPORT}"
    else
      configdb_string="${CFGRSNAME}/"
      for ((i=0; i<RS_NODES; i++)); do
        if [ ${i} -gt 0 ]; then
          configdb_string="${configdb_string},"
        fi
        configdb_string="${configdb_string}${HOST}:$(($CFGPORT + i))"
      done
    fi
    echo "if ! ${BINDIR}/mongos --port ${SHPORT} --configdb ${configdb_string} --logpath ${SETUPDIR}/${SHNAME}/mongos.log --unixSocketPrefix ${SETUPDIR}/${SHNAME} --fork ${MONGOS_EXTRA} ${MONGOS_TLS_OPTS} 2>&1 | grep -q 'forked process'; then" >> ${SETUPDIR}/${SHNAME}/start_mongos.sh
    echo "  echo \"ERROR: Failed to start mongos on port ${SHPORT}\"" >> ${SETUPDIR}/${SHNAME}/start_mongos.sh
    echo "  echo \"Check ${SETUPDIR}/${SHNAME}/mongos.log for details\"" >> ${SETUPDIR}/${SHNAME}/start_mongos.sh
    echo "  exit 1" >> ${SETUPDIR}/${SHNAME}/start_mongos.sh
    echo "fi" >> ${SETUPDIR}/${SHNAME}/start_mongos.sh

    echo "#!/usr/bin/env bash" > ${SETUPDIR}/${SHNAME}/stop_mongos.sh
    echo "source ${WORKDIR}/COMMON" >> ${SETUPDIR}/${SHNAME}/stop_mongos.sh
    echo "echo \"Stopping mongos on port: ${SHPORT}\"" >> ${SETUPDIR}/${SHNAME}/stop_mongos.sh
    echo "timeout 10 ${MONGO} localhost:${SHPORT}/admin --quiet --eval 'db.shutdownServer({force:true,timeoutSecs:5})' \${AUTH} \${TLS_CLIENT} 2>/dev/null || true" >> ${SETUPDIR}/${SHNAME}/stop_mongos.sh
    echo "#!/usr/bin/env bash" > ${SETUPDIR}/start_mongodb.sh
    echo "echo \"=== Starting sharding cluster: ${SHNAME} ===\"" >> ${SETUPDIR}/start_mongodb.sh
    echo "echo \"Starting sharding cluster on port: ${SHPORT}\"" >> ${SETUPDIR}/start_mongodb.sh
    echo "${SETUPDIR}/${CFGRSNAME}/start_mongodb.sh" >> ${SETUPDIR}/start_mongodb.sh
    echo "${SETUPDIR}/${RS1NAME}/start_mongodb.sh" >> ${SETUPDIR}/start_mongodb.sh
    echo "${SETUPDIR}/${RS2NAME}/start_mongodb.sh" >> ${SETUPDIR}/start_mongodb.sh
    echo "${SETUPDIR}/${SHNAME}/start_mongos.sh" >> ${SETUPDIR}/start_mongodb.sh

    echo "#!/usr/bin/env bash" > ${SETUPDIR}/stop_mongodb.sh
    echo "echo \"=== Stopping sharding cluster: ${SHNAME} ===\"" >> ${SETUPDIR}/stop_mongodb.sh
    echo "${SETUPDIR}/${SHNAME}/stop_mongos.sh" >> ${SETUPDIR}/stop_mongodb.sh
    echo "${SETUPDIR}/${RS1NAME}/stop_mongodb.sh" >> ${SETUPDIR}/stop_mongodb.sh
    echo "${SETUPDIR}/${RS2NAME}/stop_mongodb.sh" >> ${SETUPDIR}/stop_mongodb.sh
    echo "${SETUPDIR}/${CFGRSNAME}/stop_mongodb.sh" >> ${SETUPDIR}/stop_mongodb.sh

    chmod +x ${SETUPDIR}/${SHNAME}/start_mongos.sh
    chmod +x ${SETUPDIR}/${SHNAME}/stop_mongos.sh
    chmod +x ${SETUPDIR}/start_mongodb.sh
    chmod +x ${SETUPDIR}/stop_mongodb.sh

    if [ ${NUM_SETUPS} -gt 1 ]; then
      echo "${SETUPDIR}/start_mongodb.sh" >> ${WORKDIR}/start_mongodb.sh
      echo "${SETUPDIR}/stop_mongodb.sh" >> ${WORKDIR}/stop_mongodb.sh
    fi
    ${SETUPDIR}/${SHNAME}/start_mongos.sh
    wait_for_port "${SHPORT}" "${SETUPDIR}/${SHNAME}/mongos.log" || exit 1

    if [ ${ENABLE_AUTH} -eq 1 ]; then
      ${MONGO} localhost:${SHPORT}/admin --quiet ${TLS_CLIENT} --eval "db.createUser({ user: \"${MONGO_USER}\", pwd: \"${MONGO_PASS}\", roles: [ \"root\", \"userAdminAnyDatabase\", \"clusterAdmin\" ] });"
      AUTH="--username=${MONGO_USER} --password=${MONGO_PASS} --authenticationDatabase=admin"
      ${MONGO} ${AUTH} ${TLS_CLIENT} localhost:${SHPORT}/admin --quiet --eval "db.getSiblingDB(\"admin\").createRole( { role: \"pbmAnyAction\", privileges: [ { resource: { anyResource: true }, actions: [ \"anyAction\" ] } ], roles: [] } )"
      ${MONGO} ${AUTH} ${TLS_CLIENT} localhost:${SHPORT}/admin --quiet --eval "db.createUser({ user: \"${MONGO_BACKUP_USER}\", pwd: \"${MONGO_BACKUP_PASS}\", roles: [ { db: \"admin\", role: \"readWrite\", collection: \"\" }, { db: \"admin\", role: \"backup\" }, { db: \"admin\", role: \"clusterMonitor\" }, { db: \"admin\", role: \"restore\" }, { db: \"admin\", role: \"pbmAnyAction\" } ] });"
    fi
    echo "Adding shards to the cluster..."
    sleep 5
    ${MONGO} ${HOST}:${SHPORT} --quiet --eval "sh.addShard(\"${RS1NAME}/${HOST}:${RS1PORT}\")" ${AUTH} ${TLS_CLIENT}
    ${MONGO} ${HOST}:${SHPORT} --quiet --eval "sh.addShard(\"${RS2NAME}/${HOST}:${RS2PORT}\")" ${AUTH} ${TLS_CLIENT}
    echo -e "\n>>> Enable sharding on specific database with: sh.enableSharding(\"<database>\") <<<"
    echo -e ">>> Shard a collection with: sh.shardCollection(\"<database>.<collection>\", { <key> : <direction> } ) <<<\n"

    if [ -n "${PBMDIR}" ]; then
      for ((i=1; i<=RS_NODES; i++)); do
        if [ "${RS_ARBITER}" != 1 ] || [ "${i}" -lt "${RS_NODES}" ]; then
          setup_pbm_agent "${SETUPDIR}/${CFGRSNAME}/node${i}" "${CFGRSNAME}" "$(($CFGPORT + ${i} - 1))"
        fi
      done
    fi
  done
  if [ ${NUM_SETUPS} -gt 1 ]; then
    chmod +x ${WORKDIR}/start_mongodb.sh
    chmod +x ${WORKDIR}/stop_mongodb.sh
  fi
  if [ -n "${PBMDIR}" ]; then
    set_pbm_store
    ${WORKDIR}/start_pbm.sh
  fi
  echo ""
  echo "=========================================="
  echo "MongoDB Connection URIs:"
  echo "=========================================="
  for ((setup=1; setup<=NUM_SETUPS; setup++)); do
    if [ ${NUM_SETUPS} -gt 1 ]; then
      SHPORT=$((27018 + (setup-1) * 10000))
      SHNAME="sh${setup}"
    else
      SHPORT=27018
      SHNAME="sh1"
    fi
    if [ -n "${AUTH}" ]; then
      echo "Sharded Cluster ${SHNAME} (mongos):"
      echo "  mongodb://${MONGO_USER}:${MONGO_PASS}@${HOST}:${SHPORT}/?authSource=admin"
    else
      echo "Sharded Cluster ${SHNAME} (mongos):"
      echo "  mongodb://${HOST}:${SHPORT}/"
    fi
  done
  echo "=========================================="
  echo ""
fi
