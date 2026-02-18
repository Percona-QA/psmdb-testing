"""
Run pyinfra with fixes for gevent: empty-host LoopExit and blocking SSH.

1) Empty hosts: disconnect_all()/connect_all() use gevent.iwait(); when the host
   list is empty that raises LoopExit. We patch both to no-op when there are
   no hosts.

2) Blocking SSH: gevent.monkey.patch_all() must run before socket/paramiko are
   used so that SSH channel I/O yields to the hub. Otherwise the first remote
   command (e.g. fact gathering) blocks in socket.recv() and the process hangs
   after "Connected". We patch at import time, before any other imports.
"""
from __future__ import annotations

from gevent import monkey

monkey.patch_all()

import pyinfra.api.connect as connect_module

_connect_all_orig = connect_module.connect_all
_disconnect_all_orig = connect_module.disconnect_all


def _connect_all(state):
    hosts = [
        host
        for host in state.inventory
        if state.is_host_in_limit(host)
    ]
    if not hosts:
        return
    _connect_all_orig(state)


def _disconnect_all(state):
    if not list(state.activated_hosts):
        return
    _disconnect_all_orig(state)


connect_module.connect_all = _connect_all
connect_module.disconnect_all = _disconnect_all


def main():
    from pyinfra_cli.main import main as cli_main
    cli_main()


if __name__ == "__main__":
    main()
