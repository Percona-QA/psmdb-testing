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

mongo admin < /package-testing/scripts/psmdb_ldap/roles.js

cat /package-testing/scripts/psmdb_ldap/mongo_ldap.conf >> /etc/mongod.conf

stop_service
start_service

RWROLE=`mongo -u "cn=exttestrw,ou=people,dc=percona,dc=com" -p "exttestrw9a5S" --authenticationDatabase '$external' --authenticationMechanism 'PLAIN' --quiet --eval "db.runCommand({connectionStatus : 1})" | grep -c "clusterAdmin"`
if [[ $RWROLE ]]; then
   echo "LDAP Full permissions OK"
else
   echo "LDAP Full permissions not OK"
   exit 1
fi

RWROLE=`mongo -u "cn=exttestro,ou=people,dc=percona,dc=com" -p "exttestro9a5S" --authenticationDatabase '$external' --authenticationMechanism 'PLAIN' --quiet --eval "db.runCommand({connectionStatus : 1})" | grep -c "clusterAdmin"` || true
ROROLE=`mongo -u "cn=exttestro,ou=people,dc=percona,dc=com" -p "exttestro9a5S" --authenticationDatabase '$external' --authenticationMechanism 'PLAIN' --quiet --eval "db.runCommand({connectionStatus : 1})" | grep -c "read"`
if [[ $RWROLE == 0 ]] && [[ $ROROLE ]]; then
   echo "LDAP read-only permissions OK"
else
   echo	"LDAP read-only permissions not OK"
   exit	1
fi
