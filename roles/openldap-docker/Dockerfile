FROM bitnamilegacy/openldap:latest
USER root
ENV LDAP_ADMIN_USERNAME: "admin"
ENV LDAP_ADMIN_PASSWORD: "password"
ENV LDAP_USERS: "pbm"
ENV LDAP_PASSWORDS: "pbmpass"
COPY db.ldif /tmp/db.ldif

