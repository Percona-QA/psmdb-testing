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

mongo admin < /package-testing/scripts/psmdb_kerberos/roles.js

cat /package-testing/scripts/psmdb_kerberos/mongo_kerberos.conf >> /etc/mongod.conf

stop_service
start_service

HOSTNAME=`hostname`

kadmin.local -q "addprinc -pw exttestro exttestro"
kinit exttestro <<<'exttestro'
ROROLE=`mongo --host=$HOSTNAME --authenticationMechanism=GSSAPI --authenticationDatabase='$external' --username exttestro@PERCONATEST.COM --quiet --eval "db.runCommand({connectionStatus : 1})" | grep -c read`
if [[ $ROROLE ]]; then
   echo "Kerberos RO permissions OK"
else
   echo "Kerberos RO permissions not OK"
   exit 1
fi

kadmin.local -q "addprinc -pw exttestrw exttestrw"
kinit exttestrw <<<'exttestrw'
RWROLE=`mongo --host=$HOSTNAME --authenticationMechanism=GSSAPI --authenticationDatabase='$external' --username exttestro@PERCONATEST.COM --quiet --eval "db.runCommand({connectionStatus : 1})" | grep -c userAdminAnyDatabase`
if [[ $RWROLE ]]; then
   echo "Kerberos RW permissions OK"
else
   echo "Kerberos RW permissions not OK"
   exit 1
fi
