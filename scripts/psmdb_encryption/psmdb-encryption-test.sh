#!/bin/bash

source /package-testing/scripts/psmdb_common.sh
KEY_FILE="/package-testing/scripts/psmdb_encryption/mongodb-keyfile"
TOKEN_FILE="/package-testing/scripts/psmdb_encryption/mongodb-test-vault-token"
CA_FILE="/package-testing/scripts/psmdb_encryption/test.cer"
CA_KMIP_FILE="/pykmip_workdir/ca.crt"
MONGO_PEM_FILE="/pykmip_workdir/mongod.pem"

echo -n > ${LOG}

set -e

if [ -z "$1" ]; then
  echo "This script needs parameter keyring or vault"
  exit 1
elif [ "$1" != "keyfile" -a "$1" != "vault" -a "$1" != "kmip" ]; then
  echo "Key store not recognized!"
  exit 1
else
  KEY_STORE="$1"
fi

if [ -f "${BACKUP_CONFIGFILE}" ]; then 
  echo "restore defaults"
  stop_service
  clean_datadir
  cp ${BACKUP_CONFIGFILE} ${CONFIGFILE}
  start_service
fi

cp ${CONFIGFILE} ${BACKUP_CONFIGFILE}
rm -f ~/.mongorc.js

if [ "$1" == "keyfile" ]; then
  chmod 600 ${KEY_FILE}
  chown mongod:mongod ${KEY_FILE}
  for cipher in AES256-CBC AES256-GCM; do
    echo "===============================================" | tee -a ${LOG}
    echo "testing encryption with ${cipher} using keyfile" | tee -a ${LOG}
    echo "===============================================" | tee -a ${LOG}
    echo "preparing datadir for testing encryption with ${cipher}" | tee -a ${LOG}
    stop_service
    clean_datadir
    sed -i "/^  engine: /s/^/#/g" ${CONFIGFILE}
    engine="wiredTiger"
    sed -i "/engine: *${engine}/s/#//g" ${CONFIGFILE}
    if [ ${cipher} = "AES256-CBC" ]; then
      sed -i "s|#security:|security:\n  enableEncryption: true\n  encryptionCipherMode: ${cipher}\n  encryptionKeyFile: ${KEY_FILE}|" ${CONFIGFILE}
    else
      sed -i "s/encryptionCipherMode: AES256-CBC/encryptionCipherMode: AES256-GCM/" ${CONFIGFILE}
    fi
    start_service
    if [ "$(mongo --quiet --eval "db.serverCmdLineOpts().parsed.security.enableEncryption" | tail -n1)" != "true" ]; then
      echo "ERROR: Encryption is not enabled!" | tee -a ${LOG}
      exit 1
    elif [ "$(mongo --quiet --eval "db.serverCmdLineOpts().parsed.security.encryptionCipherMode" | tail -n1)" != "${cipher}" ]; then
      echo "ERROR: Cipher mode is not set to: ${cipher}" | tee -a ${LOG}
      exit 1
    elif [ "$(mongo --quiet --eval "db.serverCmdLineOpts().parsed.security.encryptionKeyFile" | tail -n1)" != "${KEY_FILE}" ]; then
      echo "ERROR: Encryption key file is not set to: ${KEY_FILE}" | tee -a ${LOG}
      exit 1
    fi
    echo "adding some data and indexes with cipher ${cipher}" | tee -a ${LOG}
    mongo localhost:27017/test --eval "for(i=1; i <= 100000; i++) { db.series.insert( { id: i, name: 'series'+i })}" >> ${LOG}
    mongo localhost:27017/test --eval "db.series.createIndex({ name: 1 })" >> ${LOG}
    echo "testing the hotbackup functionality with ${cipher}" | tee -a ${LOG}
    test_hotbackup
  done
fi

cp ${BACKUP_CONFIGFILE} ${CONFIGFILE}
if [ "$1" == "vault" ]; then
  chmod 600 ${TOKEN_FILE}
  chown mongod:mongod ${TOKEN_FILE}
  chmod 600 ${CA_FILE}
  chown mongod:mongod ${CA_FILE}
  echo ${TOKEN_FILE}
  echo ${CA_FILE}

  for cipher in AES256-CBC AES256-GCM; do
    echo "=============================================" | tee -a ${LOG}
    echo "testing encryption with ${cipher} using vault" | tee -a ${LOG}
    echo "=============================================" | tee -a ${LOG}
    echo "preparing datadir for testing encryption with ${cipher}" | tee -a ${LOG}
    stop_service
    clean_datadir
    sed -i "/^  engine: /s/^/#/g" ${CONFIGFILE}
    engine="wiredTiger"
    sed -i "/engine: *${engine}/s/#//g" ${CONFIGFILE}
    if [ ${cipher} = "AES256-CBC" ]; then
      sed -i "s|#security:|security:\n  enableEncryption: true\n  encryptionCipherMode: ${cipher}\n  vault:\n    serverName: 127.0.0.1\n    port: 8200\n    tokenFile: ${TOKEN_FILE}\n    serverCAFile: ${CA_FILE}\n    secret: secret_v2/data/psmdb-test/package-test|" ${CONFIGFILE}
    else
      sed -i "s/encryptionCipherMode: AES256-CBC/encryptionCipherMode: AES256-GCM/" ${CONFIGFILE}
    fi
    start_service
    if [ "$(mongo --quiet --eval "db.serverCmdLineOpts().parsed.security.enableEncryption" | tail -n1)" != "true" ]; then
      echo "ERROR: Encryption is not enabled!" | tee -a ${LOG}
      exit 1
    elif [ "$(mongo --quiet --eval "db.serverCmdLineOpts().parsed.security.encryptionCipherMode" | tail -n1)" != "${cipher}" ]; then
      echo "ERROR: Cipher mode is not set to: ${cipher}" | tee -a ${LOG}
      exit 1
    elif [ "$(mongo --quiet --eval "db.serverCmdLineOpts().parsed.security.vault.serverName" | tail -n1)" != "127.0.0.1" ]; then
      echo "ERROR: Encryption vault server is not set to: 127.0.0.1" | tee -a ${LOG}
      exit 1
    fi
    echo "adding some data and indexes with cipher ${cipher}" | tee -a ${LOG}
    mongo localhost:27017/test --eval "for(i=1; i <= 100000; i++) { db.series.insert( { id: i, name: 'series'+i })}" >> ${LOG}
    mongo localhost:27017/test --eval "db.series.createIndex({ name: 1 })" >> ${LOG}
    echo "testing the hotbackup functionality with ${cipher}" | tee -a ${LOG}
    test_hotbackup
    echo "testing Vault key rotation with ${cipher}" | tee -a ${LOG}
    stop_service
    sed -i "s|secret: secret_v2/data/psmdb-test/package-test|secret: secret_v2/data/psmdb-test/package-test\n    rotateMasterKey: true|" ${CONFIGFILE}
    start_service || true
    sed -i "s|rotateMasterKey: true||" ${CONFIGFILE}
    start_service

  done
fi

cp ${BACKUP_CONFIGFILE} ${CONFIGFILE}
if [ "$1" == "kmip" ]; then
  chmod 600 ${CA_KMIP_FILE}
  chown mongod:mongod ${CA_KMIP_FILE}
  chmod 600 ${MONGO_PEM_FILE}
  chown mongod:mongod ${MONGO_PEM_FILE}
  echo ${CA_KMIP_FILE}
  echo ${MONGO_PEM_FILE}

  for cipher in AES256-CBC AES256-GCM; do
    echo "=============================================" | tee -a ${LOG}
    echo "testing encryption with ${cipher} using kmip " | tee -a ${LOG}
    echo "=============================================" | tee -a ${LOG}
    echo "preparing datadir for testing encryption with ${cipher}" | tee -a ${LOG}
    stop_service
    clean_datadir
    sed -i "/^  engine: /s/^/#/g" ${CONFIGFILE}
    engine="wiredTiger"
    sed -i "/engine: *${engine}/s/#//g" ${CONFIGFILE}
    if [ ${cipher} = "AES256-CBC" ]; then
      sed -i "s|#security:|security:\n  enableEncryption: true\n  encryptionCipherMode: ${cipher}\n  kmip:\n    serverName: 127.0.0.1\n    clientCertificateFile: ${MONGO_PEM_FILE}\n    serverCAFile: ${CA_KMIP_FILE}|" ${CONFIGFILE}
    else
      sed -i "s/encryptionCipherMode: AES256-CBC/encryptionCipherMode: AES256-GCM/" ${CONFIGFILE}
    fi
    start_service
    if [ "$(mongo --quiet --eval "db.serverCmdLineOpts().parsed.security.enableEncryption" | tail -n1)" != "true" ]; then
      echo "ERROR: Encryption is not enabled!" | tee -a ${LOG}
      exit 1
    elif [ "$(mongo --quiet --eval "db.serverCmdLineOpts().parsed.security.encryptionCipherMode" | tail -n1)" != "${cipher}" ]; then
      echo "ERROR: Cipher mode is not set to: ${cipher}" | tee -a ${LOG}
      exit 1
    elif [ "$(mongo --quiet --eval "db.serverCmdLineOpts().parsed.security.kmip.serverName" | tail -n1)" != "127.0.0.1" ]; then
      echo "ERROR: Encryption kmip server is not set to: 127.0.0.1" | tee -a ${LOG}
      exit 1
    fi
    echo "adding some data and indexes with cipher ${cipher}" | tee -a ${LOG}
    mongo localhost:27017/test --eval "for(i=1; i <= 100000; i++) { db.series.insert( { id: i, name: 'series'+i })}" >> ${LOG}
    mongo localhost:27017/test --eval "db.series.createIndex({ name: 1 })" >> ${LOG}
    echo "testing the hotbackup functionality with ${cipher}" | tee -a ${LOG}
    test_hotbackup
    echo "testing KMIP key rotation with ${cipher}" | tee -a ${LOG}
    stop_service
    sed -i "s|serverCAFile: ${CA_KMIP_FILE}|serverCAFile: ${CA_KMIP_FILE}\n    rotateMasterKey: true|" ${CONFIGFILE}
    start_service || true
    sed -i "s|rotateMasterKey: true||" ${CONFIGFILE}
    start_service
  done
fi
