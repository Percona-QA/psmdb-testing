#!/bin/bash

source /package-testing/scripts/psmdb_common.sh
set -e

if [ -f "${BACKUP_CONFIGFILE}" ]; then 
  echo "restore defaults"
  stop_service
  clean_datadir
  /usr/bin/cp ${BACKUP_CONFIGFILE} ${CONFIGFILE}
  start_service
fi

mongo < /package-testing/scripts/mongo_insert.js
