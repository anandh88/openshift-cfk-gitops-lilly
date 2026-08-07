# LDAP / Kerberos architecture

## What this adds

Kerberos authentication between Kafka Connect's JDBC Source Connector and
SQL Server, backed by a real Active Directory domain controller (realm
`PSYNCOPATE.COM`) - not a plain MIT KDC.

Neither SQL Server nor the AD domain controller are deployed by this repo
- see "Why SQL Server and the AD DC run outside this cluster" below. Only
Connect's Kerberos wiring runs in-cluster and is covered here.

```
Docker Desktop (this Mac)                              confluent namespace
┌─────────────┐  same Docker      ┌─────────────┐      ┌──────────────┐
│   sambaad   │◄──network, full   │  sqltest2   │      │   connect    │
│ (Samba4 AD  │  TCP+UDP──────────│ (SQL Server)│      │ (JDBC Source │
│  domain     │  (domain join,    │  domain-    │      │  Connector)  │
│  controller)│   sssd)           │  joined)    │      └──────┬───────┘
└──────┬──────┘                   └──────┬──────┘             │
       │ 88/tcp (SSH reverse tunnel,      │ 1433/tcp (SSH reverse tunnel,
       │  through the CRC node)           │  through the CRC node)
       └──────────────────────────────────┴────────────────────┘
              Kerberos ticket exchange +        SPNEGO embedded in TDS
              SQL login (Connect authenticates as PSYNCOPATE\connect-svc)
```

## Why a real AD domain controller, not a plain MIT KDC

This started as LDAP-backed MIT Kerberos (`kdb5_ldap_util`/`kldap`) in
this same cluster - the standard non-Active-Directory way to run
"LDAP-backed Kerberos". That got as far as a fully working Kerberos
ticket exchange between Connect and SQL Server: the GSSAPI handshake
succeeded, proving the keytab/SPN wiring was correct. SQL Server then
rejected the login anyway:

```
Login failed. The login is from an untrusted domain and cannot be used with Integrated authentication.
```

SQL Server's "integrated security" login path doesn't just validate the
Kerberos ticket cryptographically - it also calls into the OS identity
stack (`sssd`'s `ad` provider) to resolve the client principal to a
trusted domain account. `sssd`'s `ad` provider only speaks to a
real AD-schema directory (computer objects, `sAMAccountName`, the AD LDAP
schema, DNS SPN conventions), not a generic LDAP+MIT-KDC pair - there was
no AD for it to trust. Samba's AD DC mode implements the real thing
(LDAP + Kerberos KDC + DNS + SMB/RPC together), which is what makes that
trust check actually pass. The old LDAP+KDC manifests
(`base/ldap/`, `base/kerberos/`) were removed once this was confirmed.

## Why SQL Server *and* the AD DC run outside this cluster

SQL Server was already established as unable to run on this cluster (see
below - it's `amd64`-only, this node is `arm64`, and no manifest change
fixes a CPU architecture mismatch). Standing up the AD DC in-cluster
alongside SQL Server-in-Docker-Desktop was tried first, tunneled over
SSH into the CRC VM the same way SQL Server itself is reached. That
failed for a *different*, more fundamental reason than the SQL Server
case: domain-join (`net ads join`, and separately `sssd`'s own discovery)
depends on a CLDAP "netlogon ping" over **UDP**, and plain `ssh -L`/`-R`
can only carry TCP - a hard SSH protocol limitation, not a config
mistake. Building a UDP-over-TCP relay pair was considered and rejected
as more moving parts than just relocating the DC.

Moving the AD DC into Docker Desktop, colocated with SQL Server, fixes
this at the root: domain-join traffic (CLDAP, DNS SRV lookups, LDAP,
SMB) all happens on Docker's own real bridge network, with full native
UDP support - no tunnel involved for any of it. The only traffic that
*does* need to cross into the cluster is Connect's own Kerberos ticket
requests (AS-REQ/TGS-REQ), which are TCP-only and tunnel fine - reusing
the exact same reverse-SSH-tunnel mechanism already proven reliable for
SQL Server's own TDS traffic (see below), just a second `-R` forwarding
the DC's port 88 instead of SQL Server's 1433.

## Setting up the Samba AD DC (Docker Desktop)

`scripts/kerberos/setup-kerberos.sh` provisions it (section 1). Two
non-obvious requirements found by testing, both baked into that section:

- **`--dns-backend=SAMBA_INTERNAL` is required, not optional.**
  `--dns-backend=NONE` (tried first, since nothing in this setup uses
  DNS-based *discovery* anywhere else) leaves Samba's CLDAP netlogon
  responder unable to answer at all - confirmed live that both
  `net ads join` and `sssd`'s `ad` provider failed discovery even on a
  fully-native, full-UDP Docker network with zero tunnels involved,
  because Samba needs its own DNS zone's site/domain-GUID data to
  construct a CLDAP response, regardless of transport. Once real DNS
  (with real SRV records for `_ldap._tcp.psyncopate.com`) was enabled,
  discovery worked immediately on the same network. Any client (`sssd`,
  `net ads join`, this repo's own scripts) that talks to this DC needs
  its resolver pointed at the DC's own DNS server.
- **`--option="vfs objects = dfs_samba4 acl_xattr xattr_tdb"` is
  required.** Without it, provisioning fails partway through with
  `set_nt_acl_no_snum: fset_nt_acl returned NT_STATUS_ACCESS_DENIED`
  while setting the sysvol NT ACL - writing the `security.NTACL` xattr
  needs `CAP_SYS_ADMIN`, which a plain `docker run` container doesn't
  have (confirmed live: `setfattr -n security.NTACL ...` fails with
  `Operation not permitted` as root, while ordinary `user.*` xattrs and
  POSIX ACLs on the same filesystem work fine - this is a Linux
  capability requirement for the `security.*` xattr namespace, not a
  filesystem limitation). `xattr_tdb` emulates the same ACL storage in a
  Samba-managed tdb file instead of real filesystem xattrs, sidestepping
  the capability requirement entirely - Samba's own documented
  workaround for exactly this class of restricted environment.

**`samba` runs as the container's PID 1, not backgrounded.** An earlier
version of this setup started `samba -i` via `docker exec -d ... nohup
samba &` - confirmed live that this can die (crash, or just get reaped)
with nothing to restart it, silently breaking Connect with `Cannot get a
KDC reply` until someone notices and manually restarts it. Making samba
PID 1 under `docker run --restart=unless-stopped` means Docker's own
restart policy recovers it automatically. `/var/lib/samba` lives on a
named volume (`sambaad-data`) so a crash-triggered container restart -
which re-runs the container's whole `apt-get install && provision-if-
needed && exec samba` command from scratch - doesn't lose the
provisioned domain; it just skips provisioning (sees `sam.ldb` already
there) and restores `/etc/samba/smb.conf` from a copy also kept on that
volume (`smb.conf` itself lives outside `/var/lib/samba`, so a freshly
`apt-get`-installed default one would otherwise overwrite it - confirmed
live this makes `samba` refuse to start at all: "Samba detected
misconfigured 'server role' and exited").

## AD accounts are not the same thing as MIT principals

`scripts/kerberos/setup-kerberos.sh` creates two AD accounts (section 2):
`connect-svc` (Kafka Connect's own identity) and `mssql-svc` (an SPN
holder matching SQL Server's own service identity,
`MSSQLSvc/<host>:<port>`).

The MIT-KDC version of this feature used `connect/connect.confluent.svc.
cluster.local@PSYNCOPATE.COM` as Connect's own principal, and porting
that same string over to the new AD setup seemed natural. It's wrong:
```
kinit: Client 'connect/connect.confluent.svc.cluster.local@PSYNCOPATE.COM' not found in Kerberos database
```
Unlike MIT Kerberos (where any string can be a principal, client or
service), AD only lets an **account's own identity** authenticate as a
client (`connect-svc@PSYNCOPATE.COM`) - SPNs are lookup aliases attached
to an account for *other* clients to find *that account as a service*,
never a separate identity you can `kinit` as yourself. `connect-svc` gets
no SPN at all; it doesn't need one. `mssql-svc` gets exactly one, the
literal string SQL Server's own SPN needs to match.

`base/confluent-platform/connect-jaas-configmap.yaml`'s `principal=` and
`setup-kerberos.sh`'s `exportkeytab --principal=` calls both use the
account form for this reason.

## Domain-joining SQL Server without a working `net ads join`

SQL Server needs `sssd` running with a valid machine keytab for the OS
identity resolution mentioned above. `net ads join` is the normal way to
get both, but its own post-join self-check is unreliable in this setup -
confirmed live, repeatedly, that it fails with
`NT_STATUS_NO_TRUST_SAM_ACCOUNT` ("failed to verify domain membership
after joining") even when the trust account it just created is genuinely
valid (enabled, real password set, confirmable directly via
`samba-tool computer show`). `scripts/kerberos/setup-kerberos.sh`
(section 4) sidesteps the unreliable self-check entirely: it creates the
computer account directly via `samba-tool` (enabling it and setting its
password explicitly - `samba-tool computer create` alone leaves an
account disabled with no key material, which fails keytab export with
`Export one principal...` printing success while writing nothing) and
exports its keytab directly, since `sssd` only needs the keytab, not a
successful `net ads join` run.

Two more account-identity gotchas found getting `sssd` itself to
actually authenticate as this computer account (both confirmed live,
both fixed in that same section):

- `sssd`'s own LDAP bind kinits as the computer account's `host/<fqdn>`
  identity, not its `<NAME>$` sAMAccountName - and just like
  `connect-svc`'s SPN earlier, that `host/...` string isn't a valid
  client principal unless the account's `userPrincipalName` is
  explicitly set to match it (`net ads join` does this automatically;
  creating the account directly via `samba-tool` does not). Set via a
  direct `ldbmodify` against `/var/lib/samba/private/sam.ldb`.
- Kerberos principal matching is case-sensitive. `sssd`'s `ad_hostname`
  config value determines the exact case it kinits with; the SPN/UPN/
  exported keytab all need to match that case exactly, or the keytab
  fails with `no suitable keys` despite being otherwise correct.
  `setup-kerberos.sh` keeps everything lowercase, matching the
  container's own (lowercase) hostname.

`docker restart`ing the SQL Server container wipes `/etc/resolv.conf`,
`/etc/hosts`, and kills the backgrounded `sssd` process (standard Docker
behavior on restart, not something a script controls) - re-run
`setup-kerberos.sh` after every restart.

## Reaching Docker Desktop from the cluster

Two independent SSH reverse tunnels through the CRC VM, both confirmed
working end-to-end with real protocol traffic (not just a TCP handshake):

1. CRC exposes SSH on `127.0.0.1:2222` (key at
   `~/.crc/machines/crc/id_ed25519`, user `core`, passwordless `sudo`).
2. `GatewayPorts yes` must be enabled in the VM's sshd (off by default -
   a `-R` remote forward silently binds to loopback only otherwise,
   unreachable from the pod network):
   ```bash
   ssh -i ~/.crc/machines/crc/id_ed25519 -p 2222 core@127.0.0.1 \
     "echo 'GatewayPorts yes' | sudo tee /etc/ssh/sshd_config.d/99-gatewayports.conf && sudo systemctl restart sshd"
   ```
3. SQL Server (TDS, 1433) - unprivileged remote port, no special
   handling needed:
   ```bash
   ssh -i ~/.crc/machines/crc/id_ed25519 -p 2222 -N \
     -R 0.0.0.0:14330:localhost:1433 core@127.0.0.1
   ```
4. The AD DC (Kerberos, 88) - see the note on UDP below.
   `scripts/kerberos/setup-kerberos.sh` (section 5) sets this one up:
   ```bash
   ssh -i ~/.crc/machines/crc/id_ed25519 -p 2222 -N \
     -R 0.0.0.0:18088:localhost:8088 core@127.0.0.1
   ```

Confirmed reachable from a real pod in both cases:
`nc -zv <node-internal-ip> <port>` -> `open` (node IP from
`oc get nodes -o wide`).

This is a **local dev/validation harness**, not committed infrastructure
- specific to this Mac's setup and not portable across environments.
Whatever addresses the tunnels end up at, pass SQL Server's via
`SQLSERVER_HOST`/`SQLSERVER_PORT` to `scripts/kerberos/setup-kerberos.sh`,
and the AD DC's via `base/confluent-platform/connect-krb5-configmap.yaml`'s
`[realms]` block.

### The UDP wall (and why the fix is an iptables rule, not a config change)

Java's Kerberos client (used by the JDBC connector's JAAS login, not by
any MIT client) attempts a **UDP** send to the KDC first regardless of
`udp_preference_limit` - confirmed live that neither `udp_preference_limit
= 0` nor `= 1` alone stopped it, and the MIT-style `kdc = tcp/host:port`
transport-prefix syntax worked once, then failed on a subsequent JVM
restart with the literal string parsed as a hostname
(`UnknownHostException: tcp/192.168.126.11`) - unreliable across JVM
instances, not a real fix.

`ssh -R` only carries TCP, so nothing listens on UDP 18088 on the node.
Sending UDP to a live host's closed port gets an *immediate* ICMP "port
unreachable" response - which Java's `KdcComm` treats as fatal
(`PortUnreachableException`) instead of a normal timeout, so it never
gets to its own TCP fallback logic. The actual fix is at the network
layer: make the node **silently drop** that UDP traffic instead of
rejecting it, so Java's send just times out - which *does* trigger its
normal TCP fallback correctly. `setup-kerberos.sh` adds this via
`oc debug node/crc` (no Mac-side `sudo` needed, since this rule lives on
the cluster node, not the Mac):
```bash
oc debug node/crc -- chroot /host bash -c \
  'iptables -I INPUT -p udp --dport 18088 -j DROP'
```

## JDBC Source Connector

`connection.url` sets `integratedSecurity=true;authenticationScheme=JavaKerberos;`,
which makes mssql-jdbc use a JAAS login context (default name
`SQLJDBCDriver`, see `base/confluent-platform/connect-jaas-configmap.yaml`)
backed by `com.sun.security.auth.module.Krb5LoginModule` reading
Connect's own keytab. The connector plugin itself (which bundles the
mssql-jdbc driver) is installed via CFK's `spec.build.onDemand.plugins.confluentHub`
mechanism (`base/confluent-platform/connect-kerberos-patch.yaml`) rather
than a custom image, which needs real internet egress at pod-init time -
see `bootstrap/network-policies.yaml`'s `connect-external-egress`.

Two more fixes needed after Kerberos itself started working (both
unrelated to Kerberos, just the next two things that broke once auth
succeeded):

- **Connector plugin/runtime version mismatch**: `kafka-connect-jdbc
  10.7.4` against this Connect runtime failed task startup with
  `IllegalAccessError: failed to access class org.apache.kafka.common.
  utils.SystemTime ... in unnamed module of loader 'app'` - a classic
  duplicate/conflicting bundled `kafka-clients` symptom. Bumped to
  `10.8.4` in `connect-kerberos-patch.yaml`.
- **Schema Registry's self-signed TLS cert**: once the JDBC/Kerberos
  side was healthy, the task still failed serializing Avro records with
  `PKIX path building failed` - Connect's JVM doesn't trust Schema
  Registry's cert by default. Fixed with a JKS truststore built from
  `schemaregistry-tls-secret`'s `ca.crt`
  (`base/confluent-platform/connect-truststore-configmap.yaml`, rebuilt
  by `setup-kerberos.sh` every run), mounted and pointed at via
  `-Djavax.net.ssl.trustStore=...` in `connect-kerberos-patch.yaml`'s
  `KAFKA_OPTS`.
- **Auto topic creation is disabled on this cluster** (standard
  governance default): the connector's own producer got
  `UNKNOWN_TOPIC_OR_PARTITION` until the topic was created declaratively
  (`base/confluent-platform/sqlserver-claims-topic.yaml`, a `KafkaTopic`
  CR). That CR itself needed a NetworkPolicy fix first - the CFK
  operator's existing `confluent-operator-management` ingress rule
  (`bootstrap/network-policies.yaml`) only opened Kafka's Jolokia/JMX
  ports (7777/7778/7203), not its separate REST Admin API port (8090)
  that `KafkaTopic` reconciliation actually calls - confirmed live via
  `dial tcp ...:8090: i/o timeout` in the CR's own status conditions.

Confirmed working end-to-end live: connector and task both `RUNNING`,
worker logs showing `Committing offsets for 3 acknowledged messages`
matching the 3 seeded rows in the test table.

## Why SQL Server runs outside this cluster (arm64/QEMU)

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
4. **Rosetta instead of QEMU** - not available today for CRC's own VM.
   `vfkit` does support a Rosetta-share device (Apple's
   `VZLinuxRosettaDirectoryShare`), but CRC itself has an open, unmerged
   feature request for it
   ([crc-org/crc#4881](https://github.com/crc-org/crc/issues/4881)).
   **Docker Desktop's VM does already use Rosetta** for this - confirmed
   live that plain `docker run` boots both SQL Server and the Samba AD DC
   (an `amd64` image on this arm64 host) cleanly, which is exactly why
   both now run there instead of in-cluster.
5. **Azure SQL Edge as an arm64-native substitute** - disqualified on
   three independent counts, per Microsoft's own (now-archived) feature
   docs: it's retired (EOL 2025-09-30), it **no longer supports the ARM64
   platform** at all, and it explicitly does **not support Active
   Directory/Kerberos integration** - the last point alone would defeat
   the purpose of this entire feature even if the other two didn't apply.

## Known limitations / what a production build should change

1. **Samba AD DC image**: bake the `samba`/`krb5-user`/`winbind` packages
   into a custom image instead of `apt-get`-at-runtime, matching the
   same trade-off already accepted for the old KDC image.
2. **No TLS on LDAP/SMB** - fine for this local Docker Desktop exercise
   (traffic never leaves the Mac), not fine for production.
3. **SQL Server + AD DC on real amd64 hardware**: the original
   `base/sqlserver/` and `base/samba-ad/`/`base/ldap/`/`base/kerberos/`
   Kubernetes manifests - removed from this repo since they never
   actually ran here - are reasonable starting points to resurrect if
   deploying to real amd64 infrastructure instead of this arm64 CRC node,
   where the UDP-tunneling problem that forced the Docker Desktop
   relocation wouldn't exist in the first place (everything would be a
   normal in-cluster peer again).
