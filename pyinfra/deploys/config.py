# Pyinfra config for this deploy directory (loaded when running deploys/*.py).
# Use 60s connect timeout so paramiko does not close during slow kex_exchange_identification.
CONNECT_TIMEOUT = 60
