#!/bin/sh
set -eu

bao server -config=/openbao/config/vault.hcl &
pid=$!

export BAO_ADDR=https://vault:8200
export BAO_CACERT=/openbao/tls/ca.crt

i=0
while [ "$i" -lt 30 ]; do
  if bao status > /tmp/bao-status 2>&1; then
    break
  fi
  if grep -q 'Initialized' /tmp/bao-status; then
    break
  fi
  i=$((i + 1))
  sleep 1
done

if grep -q 'Initialized[[:space:]]*false' /tmp/bao-status; then
  bao operator init -key-shares=1 -key-threshold=1 > /tmp/init.txt
fi

key=$(awk '/Unseal Key 1:/ {print $NF}' /tmp/init.txt)
root=$(awk '/Initial Root Token:/ {print $NF}' /tmp/init.txt)
bao operator unseal "$key"
export BAO_TOKEN="$root"
bao secrets enable -path=secret kv-v2 || true
bao token create -id=vaulttoken -policy=root -ttl=24h >/dev/null

wait "$pid"
