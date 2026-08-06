# LDAP / Kerberos architecture

## What this adds

Kerberos authentication between Kafka Connect's JDBC Source Connector and
SQL Server, backed by an LDAP-stored Kerberos principal database (realm
`PSYNCOPATE.COM`) instead of Active Directory.

SQL Server itself is **not** deployed by this repo - see "Why SQL Server
runs outside this cluster" below. Everything else (LDAP, the KDC, and
Connect's Kerberos wiring) runs in-cluster and is fully covered here.

```
auth-services namespace              confluent namespace         outside the cluster
┌─────────────┐   LDAP bind   ┌─────────────┐                 ┌──────────────┐
│    ldap     │◄──────────────│     kdc     │                 │   connect    │
│ (principal  │  (kdb5_ldap_  │ (krb5kdc +  │                 │ (JDBC Source │
│  database)  │   util)       │  kadmind)   │                 │  Connector)  │
└─────────────┘               └──────┬──────┘                 └──────┬───────┘
                                      │ 88/tcp+udp                    │
                          Kerberos    │                               │
                          ticket      └──────────────────────────────►│ requests
                          exchange                                    │ a ticket
                                                                       ▼
                                                              ┌─────────────────┐
                                                        SPNEGO│   SQL Server    │
                                                     embedded │ (wherever it    │
                                                      in TDS  │  actually runs -│
                                                        1433/tcp  see below)    │
                                                              └─────────────────┘
```

## Namespace split

LDAP and the KDC live together in **auth-services** (they're tightly
coupled - the KDC binds to LDAP as its principal database).

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
  startup. Confirmed live that these packages install cleanly with no
  interactive prompts and provide `kdb5_ldap_util`/`krb5kdc`/`kadmind`.
  This needs real root (dpkg post-install scripts need
  `CAP_CHOWN`/`CAP_FOWNER`) and outbound internet egress for the package
  mirror - both are real trade-offs of this approach, not free. **A
  production build should bake a custom image with these packages
  pre-installed**, which removes both the root requirement
  (`bootstrap/auth-services-scc.yaml`) and the internet-egress rule
  (`bootstrap/auth-services-network-policies.yaml`'s
  `kdc-package-mirror-egress`) entirely.

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
- Three real findings came out of that testing, all reflected in the
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
  4. ConfigMap volumes are always mounted **read-only**, regardless of any
     `volumeMounts.readOnly` setting - a hard Kubernetes platform
     behavior. osixia's own bootstrap script runs `sed -i` (in-place edit)
     on every LDIF it processes, including custom ones, which fails
     against a read-only mount (`sed: couldn't open temporary file ...:
     Read-only file system`). Fixed with the standard pattern: an
     initContainer copies the ConfigMap into a writable `emptyDir`, which
     the main container mounts instead (see
     `base/ldap/ldap-deployment.yaml`).

The realm container entry itself (`cn=PSYNCOPATE.COM,ou=kerberos,dc=psyncopate,dc=com`)
is deliberately **not** pre-seeded via LDIF - `scripts/kerberos/01-init-kdc.sh`'s
`kdb5_ldap_util create` creates it (along with the master key and default
ticket policy, which a bare LDIF add would not set up).

### Two more live-confirmed LDAP/KDC gotchas

- **Kubernetes auto-injects `LDAP_PORT`.** Since the LDAP Service is named
  "ldap", Kubernetes injects a Docker-links-style `LDAP_PORT=tcp://<ip>:389`
  env var into the pod - which collides with osixia's own internal
  `LDAP_PORT` variable and corrupts slapd's listen URL (`parse error=5`).
  Fixed with `enableServiceLinks: false`.
- **Debian's krb5-kdc reads `/etc/krb5kdc/kdc.conf`, not a config file
  copied anywhere else.** `key_stash_file` and friends only take effect if
  set in that exact path - confirmed live, twice, that copying `kdc.conf`
  to `/var/lib/krb5kdc/` instead was silently ignored, with the master key
  stash landing at the *default* profile's own ephemeral
  `/etc/krb5kdc/.k5.REALM` regardless of what the unread copy said.
  `base/kerberos/kdc-deployment.yaml` now writes the config to
  `/etc/krb5kdc/kdc.conf` (ephemeral, recreated every restart, which is
  fine - it's just config) while the paths *within* that config
  (`key_stash_file`, `acl_file`, `admin_keytab`) point at the
  PVC-backed `/var/lib/krb5kdc`, so the actual state survives restarts.
- **`kdb5_ldap_util stashsrvpw` prompts three times**, not two: the bind
  password to authenticate the operation itself, then the value being
  stashed (entered twice to confirm). Feeding only two lines fails with
  `Cannot read password while setting service object password`.

## JDBC Source Connector

`connection.url` sets `integratedSecurity=true;authenticationScheme=JavaKerberos;`,
which makes mssql-jdbc use a JAAS login context (default name
`SQLJDBCDriver`, see `base/confluent-platform/connect-jaas-configmap.yaml`)
backed by `com.sun.security.auth.module.Krb5LoginModule` reading
Connect's own keytab. The connector plugin itself (which bundles the
mssql-jdbc driver) is installed via CFK's `spec.build.onDemand.plugins.confluentHub`
mechanism (`base/confluent-platform/connect-kerberos-patch.yaml`) rather
than a custom image, which needs real internet egress at pod-init time -
see `bootstrap/network-policies.yaml`'s `connect-external-egress` (confirmed
live: without it, `config-init-container`'s `confluent connect plugin
install` step hangs for its full connect timeout and fails, leaving
`connect-0` stuck at `Init:0/1` forever). Verify the pinned connector
version (`kafka-connect-jdbc 10.7.4`) against Confluent Hub before relying
on it in a real deployment.

Connect's real (21-byte, not the 2-byte placeholder) keytab was confirmed
mounted correctly at `/mnt/secrets/connect-keytab/connect.keytab` on a
live, healthy `connect-0` pod with no Kerberos/JAAS errors.

## Why SQL Server runs outside this cluster

`mcr.microsoft.com/mssql/server` (all editions, including Express) is
**amd64-only** - confirmed via `docker inspect --format
'{{.Architecture}}'` on the pulled image (`amd64`). This cluster's node is
arm64 (CRC on Apple Silicon runs its guest VM as native arm64, confirmed
via `oc get nodes -o jsonpath='{.items[0].status.nodeInfo.architecture}'`).
No manifest, SCC, or capabilities change fixes a CPU architecture
mismatch. This was confirmed with a full investigation before concluding
that a Kubernetes-hosted SQL Server isn't viable on this specific
environment, documented here so it isn't re-litigated from scratch:

**The chain, and where it broke:**
```
Apple Silicon (arm64 host)
  -> vfkit (Apple Virtualization.framework - hardware-virtualized arm64 guest)
    -> CRC's Linux VM (arm64, no Rosetta wired up)
      -> binfmt_misc -> qemu-x86_64 (generic TCG software binary translation)
        -> sqlservr (x86_64 ELF)  ->  SIGSEGV, immediately, every time
```
CRC's VM tool (`vfkit`) uses Apple's `Virtualization.framework` to run a
*real*, hardware-virtualized arm64 Linux guest - there is no x86 hardware
anywhere in this path. The only available translation is QEMU's TCG
(software instruction-by-instruction emulation).

**What was ruled out, in order:**
1. **Kubernetes/SCC/securityContext** - ruled out definitively. Reproduced
   the identical `Segmentation fault (core dumped)` (exit 139/SIGSEGV) via
   `podman run` directly on the CRC node's host (`oc debug node/crc`),
   completely bypassing Kubernetes, CRI-O, and every SCC/securityContext
   setting.
2. **QEMU emulation being broken in general** - ruled out. A plain test
   pod pinned to an amd64-only image digest (`alpine@sha256:...`)
   correctly reported `uname -m` -> `x86_64` inside a real CRI-O-launched
   pod. Emulation works; `sqlservr` specifically does not survive it.
3. **SQL Server version** (2017 vs 2019 vs 2022) - ruled out. Tested all
   three directly via `podman` on the node; all three crash identically
   (exit 139), with 2017/2019 crashing even earlier (no log output at all)
   than 2022. This rules out "newer AVX2-heavy codepath" as the specific
   mechanism - SQLPAL (the Linux hosting layer shared by all three
   versions since SQL Server first shipped on Linux in 2017) is doing
   something at a more fundamental level in early startup that QEMU's TCG
   can't correctly emulate, not a version-specific instruction gap.
4. **Rosetta instead of QEMU** - not available today. `vfkit` does support
   a Rosetta-share device (Apple's `VZLinuxRosettaDirectoryShare`, the same
   mechanism Docker Desktop uses), but CRC itself has an open, unmerged
   feature request for it
   ([crc-org/crc#4881](https://github.com/crc-org/crc/issues/4881)) -
   checked against the latest available release (2.62.0) at time of
   writing, still not shipped. A manual workaround exists (mount the
   Rosetta share via virtiofs, register its binfmt handler in place of
   qemu-x86_64) but requires bypassing CRC's own VM lifecycle management,
   and Rosetta itself has a documented gap in x86_64-v3 instruction
   support under vfkit
   ([crc-org/vfkit#265](https://github.com/crc-org/vfkit/issues/265)) that
   could still cause failures even if wired up.
5. **Azure SQL Edge as an arm64-native substitute** - disqualified on
   three independent counts, per Microsoft's own (now-archived) feature
   docs: it's retired (EOL 2025-09-30), it **no longer supports the ARM64
   platform** at all, and it explicitly does **not support Active
   Directory/Kerberos integration** - the last point alone would defeat
   the purpose of this entire feature even if the other two didn't apply.

### Where SQL Server actually runs instead

SQL Server runs as a normal Docker container on the same Mac (Docker
Desktop's own VM does support amd64 emulation well enough - confirmed
live that a plain `docker run` boots it cleanly to "ready for client
connections"). Connect (in-cluster) reaches it via an **SSH reverse
tunnel** into the CRC VM, confirmed working end-to-end:

1. CRC exposes SSH on `127.0.0.1:2222` (key at
   `~/.crc/machines/crc/id_ed25519`, user `core`, passwordless `sudo`).
2. `GatewayPorts yes` must be enabled in the VM's sshd (off by default -
   confirmed live that a `-R` remote forward silently binds to loopback
   only otherwise, unreachable from pod network):
   ```bash
   ssh -i ~/.crc/machines/crc/id_ed25519 -p 2222 core@127.0.0.1 \
     "echo 'GatewayPorts yes' | sudo tee /etc/ssh/sshd_config.d/99-gatewayports.conf && sudo systemctl restart sshd"
   ```
3. Reverse tunnel, binding on *all* the VM's interfaces (not just
   loopback) so pods can reach it via the node's own IP:
   ```bash
   ssh -i ~/.crc/machines/crc/id_ed25519 -p 2222 -N \
     -R 0.0.0.0:14330:localhost:1433 core@127.0.0.1
   ```
4. Confirmed reachable from a real pod:
   `nc -zv <node-internal-ip> 14330` -> `open` (node IP from
   `oc get nodes -o wide`).

This is a **local dev/validation harness**, not committed infrastructure -
it's specific to this Mac's setup and not portable across environments.
Whatever address the tunnel (or a real amd64-hosted SQL Server) ends up
at, pass it to `scripts/kerberos/02-create-principals.sh`,
`03-export-keytabs.sh`, and `05-deploy-connector.sh` via the
`SQLSERVER_HOST` (and optionally `SQLSERVER_PORT`) environment variable -
none of them hardcode an in-cluster Service DNS name, since none exists
for SQL Server anymore.

## Kerberos keytab config for SQL Server (verified against Microsoft's docs)

Confirmed against Microsoft's own tutorial
(`learn.microsoft.com/sql/linux/security/authentication/active-directory-tutorial`),
not assumed - applies wherever SQL Server actually runs:

- Keytab path convention: `/var/opt/mssql/secrets/mssql.keytab`.
- `mssql-conf set network.kerberoskeytabfile <path>` is equivalent to the
  `MSSQL_NETWORK_KERBEROSKEYTABFILE` environment variable.
- SPN format: `MSSQLSvc/<fqdn-or-ip>:<port>` - matching whatever
  `SQLSERVER_HOST`/`SQLSERVER_PORT` you pass to the scripts above.
- `MSSQL_NETWORK_DISABLESSSD=true` is required specifically because the
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
produce via `kadmin.local`/`ktadd` instead of `ktpass`. Whatever host runs
SQL Server also needs its own `krb5.conf` pointing at
`kdc.auth-services.svc.cluster.local` - reachable from outside the
cluster via the same tunnel/network path used to reach SQL Server itself
in reverse, or via a route/LoadBalancer if running on real infrastructure.

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
5. **SQL Server on real amd64 hardware**: the original `base/sqlserver/`
   Kubernetes manifests (Deployment, Service, PVC, SealedSecrets, RBAC) -
   removed from this repo since they never actually ran here - are a
   reasonable starting point to resurrect if deploying to real amd64
   infrastructure instead of this arm64 CRC node.
