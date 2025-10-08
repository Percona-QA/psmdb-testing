'storage': {'file': {'path': '/vault/file'}},
'listener': [{'tcp': { 'address': '127.0.0.1:8200', 'tls_disable': true}}],
'default_lease_ttl': '168h',
'max_lease_ttl': '720h',
'ui': true
'tls_cert_file': '/etc/vault/vault.crt'
'tls_key_file': '/etc/vault/vault.key'