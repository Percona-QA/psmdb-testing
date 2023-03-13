#!/bin/bash

source /package-testing/scripts/psmdb_common.sh
set -e

if [ -f "${BACKUP_CONFIGFILE}" ]; then
  echo "restore defaults"
  stop_service
  clean_datadir
  cp ${BACKUP_CONFIGFILE} ${CONFIGFILE}
  start_service
fi

cp ${CONFIGFILE} ${BACKUP_CONFIGFILE}

mongo admin < /package-testing/scripts/psmdb_aws/user.js

cat /package-testing/scripts/psmdb_aws/mongo_aws.conf >> /etc/mongod.conf
sed -r 's/^  bindIp.+$/  bindIp: 0.0.0.0/' -i /etc/mongod.conf

stop_service
start_service

HOSTNAME=`hostname`

CONN_STATUS=`mongo --host=$HOSTNAME --authenticationMechanism=MONGODB-AWS --authenticationDatabase='$external' --quiet --eval "db.runCommand({connectionStatus : 1})" | grep -c "userAdminAnyDatabase"`
if [[ $CONN_STATUS -eq 1 ]]; then
   echo "AWS auth OK"
else
   echo "AWS auth not OK"
   exit 1
fi
