#!/bin/sh
# mongod must load these libraries from the runtime bundle, not the OS copy.
set -eu
L=/opt/psmdb-runtime-libs
ldd_out=$(LD_LIBRARY_PATH="$L/opt/mongo/openssl3/lib:$L/lib" ldd /opt/psmdb/bin/mongod)

check_bundled() {
  name=$1
  expect=$(readlink -f "$L/$2")
  line=$(printf '%s\n' "$ldd_out" | grep -E "^[[:space:]]*${name}(\.so|-)" | head -n 1 || true)
  if [ -z "$line" ]; then
    echo "$name is not linked by mongod" >&2
    printf '%s\n' "$ldd_out" >&2
    exit 1
  fi
  if printf '%s\n' "$line" | grep -q 'not found'; then
    echo "$name was not found" >&2
    printf '%s\n' "$line" >&2
    exit 1
  fi
  resolved=${line#*=> }
  resolved=${resolved%% *}
  resolved_real=$(readlink -f "$resolved")
  case "$resolved_real" in
    "$expect"/*) ;;
    *)
      echo "$name resolved outside $expect" >&2
      printf '%s\n' "$line" >&2
      exit 1
      ;;
  esac
}

check_bundled libkrb5 opt/mongo/openssl3/lib
check_bundled libgssapi_krb5 opt/mongo/openssl3/lib
check_bundled libsasl2 opt/mongo/openssl3/lib
check_bundled libldap lib
check_bundled libcurl lib
check_bundled libssl lib
check_bundled libcrypto lib
check_bundled libz lib
check_bundled libcares lib
