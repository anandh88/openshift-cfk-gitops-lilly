# LDAP / Kerberos / SQL Server architecture

## What this adds

Kerberos authentication between Kafka Connect's JDBC Source Connector and
SQL Server Express, backed by an LDAP-stored Kerberos principal database
(realm `PSYNCOPATE.COM`) instead of Active Directory.

```
auth-services namespace              sqlserver namespace       confluent namespace
┌─────────────┐   LDAP bind   ┌─────────────┐                 ┌──────────────┐
│    ldap     │◄──────────────│     kdc     │                 │   connect    │
│ (principal  │  (kdb5_ldap_  │ (krb5kdc +  │                 │ (JDBC Source │
│  database)  │   util)       │  kadmind)   │                 │  Connector)  │
└─────────────┘               └──────┬──────┘                 └──────┬───────┘
                                      │ 88/tcp+udp                    │
                          Kerberos    │        ┌───────────┐         │
                          ticket      └───────►│  Connect  │◄────────┘
                          exchange             │  & SQL    │  krb5.conf +
                                               │  Server   │  keytab +
                                               │  both     │  jaas.conf
                                               │  request  │
                                               │  tickets  │
                                               └───────────┘
                                                      │ 1433/tcp (SPNEGO
                                                      │  embedded in TDS)
                                                      ▼
                                                ┌─────────────┐
                                                │  sqlserver  │
                                                └─────────────┘
```

## Namespace split

LDAP and the KDC live together in **auth-services** (they're tightly
coupled - the KDC binds to LDAP as its principal database). SQL Server
Express lives in its own **sqlserver** namespace. This means SQL Server
needs its own ServiceAccount (`sqlserver-sa`) rather than reusing
`auth-services-sa` - Kubernetes ServiceAccounts only resolve within their
own namespace, so a Deployment in `sqlserver` cannot reference an SA that
lives in `auth-services`.

## Why LDAP-backed Kerberos, not LDAP-bind auth

SQL Server has exactly two authentication modes on Linux: SQL
authentication (username/password) and Windows/Integrated authentication,
which is Kerberos (SPNEGO) under the hood - there is no third "authenticate
directly against LDAP" mode. So "use LDAP to authenticate the JDBC
connector to SQL Server" has to mean LDAP as the Kerberos KDC's principal
*store*, with Kerberos still being the actual wire protocol - this is the
standard non-Active-Directory way to run "LDAP-backed Kerberos" (MIT krb5's
own `kdb5_ldap_util`/`kldap` backend, used here instead of the default
local-file KDB).

## Component choices and why

- **osixia/openldap:1.5.0** - as specified. Runs with `DISABLE_CHOWN=true`
  and no fixed `runAsUser`/`fsGroup`, matching this repo's no-custom-SCC/
  restricted-v2 pattern (see `docs/troubleshooting.md`).
- **MIT krb5 KDC on debian:12-slim** - there is no maintained public image
  that ships MIT krb5 built with LDAP KDB support, so
  `base/kerberos/kdc-deployment.yaml` installs
  `krb5-kdc krb5-admin-server krb5-kdc-ldap` via `apt-get` at container
  startup. Confirmed live (via local Docker) that these packages install
  cleanly with no interactive prompts and provide
  `kdb5_ldap_util`/`krb5kdc`/`kadmind`. This needs real root (dpkg
  post-install scripts need `CAP_CHOWN`/`CAP_FOWNER`) and outbound internet
  egress for the package mirror - both are real trade-offs of this
  approach, not free. **A production build should bake a custom image with
  these packages pre-installed**, which removes both the root requirement
  (`bootstrap/auth-services-scc.yaml`) and the internet-egress rule
  (`bootstrap/auth-services-network-policies.yaml`'s
  `kdc-package-mirror-egress`) entirely.
- **mcr.microsoft.com/mssql/server:2022-latest**, `MSSQL_PID=Express` -
  Microsoft's official image; Kerberos config is via `MSSQL_NETWORK_*`
  environment variables, which map 1:1 to `mssql-conf set network.*`
  settings (confirmed against Microsoft's own tutorial - see the "Kerberos
  keytab config" section below). Runs under the same no-custom-SCC/
  restricted-v2 pattern as everything else - Microsoft documents official
  OpenShift/`fsGroup` compatibility for this image.

## The MIT krb5 LDAP schema

`base/ldap/ldap-seed-configmap.yaml`'s `00-kerberos-schema.ldif` is the
authoritative MIT krb5 LDAP schema, fetched verbatim from
`https://raw.githubusercontent.com/krb5/krb5/master/src/plugins/kdb/ldap/libkdb_ldap/kerberos.schema`
and mechanically converted (a script, not hand-transcription) from
`slapd.conf` `attributetype`/`objectclass` syntax to OpenLDAP `cn=config`
LDIF (`olcAttributeTypes`/`olcObjectClasses`) - the attribute/objectclass
definitions themselves are byte-for-byte the same values, only the
wrapping syntax changed.

This was verified end-to-end against a live `osixia/openldap:1.5.0`
container (not just eyeballed):

- All 49 attribute types and 12 object classes load with zero errors.
- A `krbRealmContainer` entry and a standalone `krbPrincipal` entry were
  both created successfully using this exact schema.
- Two real findings came out of that testing, both reflected in the
  manifests:
  1. `krbContainer` and `krbRealmContainer` are **both** `STRUCTURAL`
     object classes - combining them on one entry fails with `invalid
     structural object class chain` (LDAP permits only one structural
     class per entry). The realm container entry uses
     `krbRealmContainer` only.
  2. `krbPrincipal` does not permit the `cn` attribute - standalone
     principal entries must use `krbPrincipalName` as the RDN, not `cn`.
     (`kdb5_ldap_util` handles this correctly on its own; this only
     matters if you're hand-crafting LDIF.)
  3. `osixia/openldap`'s default `LDAP_REMOVE_CONFIG_AFTER_SETUP=true`
     runs `rm -rf` on the exact directory the custom bootstrap LDIFs are
     mounted into - confirmed live that this silently deletes the mounted
     files first (the final `rmdir` on the mount point itself is what
     fails, but by then the contents are already gone). Set to `false` in
     `base/ldap/ldap-deployment.yaml`.

The realm container entry itself (`cn=PSYNCOPATE.COM,ou=kerberos,dc=psyncopate,dc=com`)
is deliberately **not** pre-seeded via LDIF - `scripts/kerberos/01-init-kdc.sh`'s
`kdb5_ldap_util create` creates it (along with the master key and default
ticket policy, which a bare LDIF add would not set up).

## Kerberos keytab config for SQL Server (verified against Microsoft's docs)

Confirmed against Microsoft's own tutorial
(`learn.microsoft.com/sql/linux/security/authentication/active-directory-tutorial`),
not assumed:

- Keytab path convention: `/var/opt/mssql/secrets/mssql.keytab`.
- `mssql-conf set network.kerberoskeytabfile <path>` is equivalent to the
  `MSSQL_NETWORK_KERBEROSKEYTABFILE` environment variable used in
  `base/sqlserver/sqlserver-deployment.yaml`.
- SPN format: `MSSQLSvc/<fqdn>:<port>` - used here as
  `MSSQLSvc/sqlserver.sqlserver.svc.cluster.local:1433@PSYNCOPATE.COM`.
- `MSSQL_NETWORK_DISABLESSSD=true` is required specifically because this
  container was never joined to a domain via SSSD/realmd - the tutorial
  notes SQL Server tries SSSD first and only falls back to direct
  OpenLDAP/Kerberos calls after that fails.
- `udp_preference_limit = 0` in `krb5.conf` is the tutorial's recommended
  setting for hosts without a real SSSD domain-join, to skip UDP calls to
  the KDC that fail intermittently in containerized/NAT'd networks.

This tutorial is written for real Active Directory + `ktpass`-generated
keytabs, not MIT krb5 + `kdb5_ldap_util`. Functionally this doesn't matter:
Kerberos/SPNEGO is a standards-based protocol and SQL Server's Linux
implementation is built on the standard MIT `libkrb5`, not an
AD-proprietary one - what matters is a valid keytab for the `MSSQLSvc/...`
principal and a `krb5.conf` pointing at the right realm/KDC, which
`scripts/kerberos/02-create-principals.sh` + `03-export-keytabs.sh`
produce via `kadmin.local`/`ktadd` instead of `ktpass`.

## JDBC Source Connector

`connection.url` sets `integratedSecurity=true;authenticationScheme=JavaKerberos;`,
which makes mssql-jdbc use a JAAS login context (default name
`SQLJDBCDriver`, see `base/confluent-platform/connect-jaas-configmap.yaml`)
backed by `com.sun.security.auth.module.Krb5LoginModule` reading
Connect's own keytab. The connector plugin itself (which bundles the
mssql-jdbc driver) is installed via CFK's `spec.build.onDemand.plugins.confluentHub`
mechanism (`base/confluent-platform/connect-kerberos-patch.yaml`) rather
than a custom image - verify the pinned version (`kafka-connect-jdbc
10.7.4`) against Confluent Hub before relying on it, it was not
re-verified live in this pass.

## Known limitations / what a production build should change

1. **KDC image**: bake `krb5-kdc`/`krb5-kdc-ldap` into a custom image
   instead of `apt-get`-at-runtime. Removes the root/`anyuid` requirement
   and the internet-egress NetworkPolicy rule entirely.
2. **LDAP ACLs**: the KDC currently binds to LDAP as `cn=admin,dc=psyncopate,dc=com`
   (the directory's own superuser) rather than a narrowly-scoped service
   account with ACLs limited to `ou=kerberos,dc=psyncopate,dc=com`. Fine
   for this exercise; not fine for production.
3. **LDAP has no TLS** (`LDAP_TLS=false`) - the LDAP bind password and all
   principal keys transit the `ldap` <-> `kdc` link in cleartext-over-TLS-off
   (though still inside the cluster network, behind the NetworkPolicies in
   `bootstrap/auth-services-network-policies.yaml`). Enabling LDAPS would
   need cert-manager wiring similar to `bootstrap/platform-certificates.yaml`.
4. **`kadmin`/remote admin**: no `admin/admin@PSYNCOPATE.COM` principal was
   created - all principal management in `scripts/kerberos/` goes through
   `kadmin.local` via `oc exec` directly on the KDC pod, never over the
   network on port 749.
