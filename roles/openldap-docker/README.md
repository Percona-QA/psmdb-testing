OpenLDAP Ansible Role
=====================
This role installs the bitnami/openldap container and loads some example data. Intended for development and tests purposes, not to be used as production server

Requirements
------------
Docker

Role Variables
--------------
This role requires the following variables to be defined elsewhere in the playbook that uses it:
```yaml
ldap_basedn:       dc=mydomain,dc=net         # Base DN
ldap_server_uri:   ldap://localhost:389       # LDAP server URI
ldap_bind_pw:      secret                     # bind password
```

Additionally, to load users and groups, follow the data.ldif example under the templates folder.

Dependencies
------------
None

Example Playbook
----------------
Register the role in requirements.yml:
```yaml
- src: capitanh.openldap-ansible-role
  name: openldap
```
Include it in your playbooks:
```yaml
- hosts: servers
  roles:
  - openldap
```

License
-------

BSD
