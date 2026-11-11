ui = false

storage "raft" {
  path    = "/openbao/file"
  node_id = "vault"
}

listener "tcp" {
  address                  = "0.0.0.0:8200"
  cluster_address          = "0.0.0.0:8201"
  tls_cert_file            = "/openbao/tls/vault.crt"
  tls_key_file             = "/openbao/tls/vault.key"
  tls_min_version          = "tls12"
  tls_disable_client_certs = true
}

api_addr     = "https://vault:8200"
cluster_addr = "https://vault:8201"
