# LDAP / Kerberos runbook

See `docs/kerberos-architecture.md` for the full design and the reasoning
behind each choice. This is the operational sequence only.

The Samba AD DC and SQL Server both run as Docker Desktop containers on
your Mac, not in the cluster - see `docs/kerberos-architecture.md` for
why. All steps below assume Docker Desktop is running and a SQL Server
container (default name `sqltest2`) already exists.

## First-time setup

Steps 1-7 below can be run one at a time as shown, or all at once via
`./scripts/kerberos/run-all.sh`, which chains them in order and stops on
the first failure. Use the individual scripts instead when re-running
only part of the flow (e.g. after `docker restart sqltest2`, only step 6
needs re-running - see its header comment for why).

All steps require `SQLSERVER_HOST`/`SQLSERVER_PORT` set to the address
Connect uses to reach SQL Server - typically the CRC node's internal IP
and whatever port an SSH reverse tunnel into Docker Desktop's SQL Server
container is forwarding (see "Reaching Docker Desktop from the cluster"
in the architecture doc). Example: `SQLSERVER_HOST=192.168.126.11
SQLSERVER_PORT=14330`.

1. **Provision the Samba AD DC in Docker Desktop:**
   ```bash
   ./scripts/kerberos/01-setup-docker-ad.sh
   ```
   Idempotent - safe to re-run any time. Creates the `sambaad` container
   and `kerberos-net` Docker network if they don't exist yet.
2. **Create the two AD accounts** (needs `SQLSERVER_HOST` set):
   ```bash
   SQLSERVER_HOST=<address> ./scripts/kerberos/02-create-principals.sh
   ```
3. **Export their keytabs:**
   ```bash
   SQLSERVER_HOST=<address> ./scripts/kerberos/03-export-keytabs.sh
   ```
   Writes `connect.keytab` and `mssql.keytab` to `.kerberos-keytabs/`
   (gitignored - never commit these raw).
4. **Seal the real Connect keytab into git:**
   ```bash
   ./scripts/kerberos/04-seal-keytabs.sh
   git add base/confluent-platform/secrets/connect-keytab-sealed.yaml
   git commit -m "seal real Kerberos keytab for connect"
   git push
   ```
   Argo CD self-heals `connect` once the new sealed secret syncs.
5. **Register the JDBC Source Connector:**
   ```bash
   SQLSERVER_HOST=<address> ./scripts/kerberos/05-deploy-connector.sh
   ```
6. **Join SQL Server to the domain and mount its keytab:**
   ```bash
   ./scripts/kerberos/06-join-sqlserver-domain.sh
   docker restart sqltest2
   ```
   Then create its SQL login once (idempotent, safe to re-run):
   ```sql
   CREATE LOGIN [PSYNCOPATE\connect-svc] FROM WINDOWS;
   -- in the target database:
   CREATE USER [PSYNCOPATE\connect-svc] FOR LOGIN [PSYNCOPATE\connect-svc];
   ALTER ROLE db_datareader ADD MEMBER [PSYNCOPATE\connect-svc];
   ```
   **Re-run this whole step after every `docker restart sqltest2`** -
   Docker regenerates `/etc/resolv.conf`/`/etc/hosts` and kills the
   backgrounded `sssd` process on restart (a Docker platform behavior,
   not something this script controls), which breaks domain trust until
   redone. The SQL login itself survives restarts fine.
7. **Open the Connect-side tunnel and node iptables rule:**
   ```bash
   ./scripts/kerberos/07-setup-connect-tunnel.sh
   ```
   Re-run after every `crc stop`/`crc start` or Mac reboot - neither the
   SSH tunnel nor the iptables rule survives those.
8. **Validate the whole chain:**
   ```bash
   ./scripts/kerberos/validate-kerberos.sh
   ```

## Re-running after a keytab rotation

Repeat steps 2-4 for whichever principal changed (`exportkeytab`
invalidates any previously-exported keytab for that principal if the
account's password changed - see `scripts/kerberos/03-export-keytabs.sh`'s
header comment).

## Troubleshooting

**`net ads join` (inside `scripts/kerberos/06-join-sqlserver-domain.sh`,
if you run it manually) fails with `NT_STATUS_NO_TRUST_SAM_ACCOUNT`:**
Confirmed live this is unreliable/spurious in this setup - the trust
account it just created is usually genuinely valid (check with
`docker exec sambaad samba-tool computer list -U administrator%...`).
06's own approach (creating the account directly via `samba-tool` and
exporting its keytab, bypassing `net ads join` entirely) sidesteps this.

**`kinit`/CLDAP discovery fails with `SRV record not found` or `Cannot
find KDC for realm`:** Almost always a DNS issue, not a Kerberos one -
`sssd`'s `ad` provider and `net ads join` both need to resolve
`_ldap._tcp.psyncopate.com` SRV records, which only Samba's own DNS
server (enabled via `--dns-backend=SAMBA_INTERNAL` in
`01-setup-docker-ad.sh`) can answer. Check the querying container's
`/etc/resolv.conf` points at the `sambaad` container's IP.

**JDBC connector fails with `Unable to obtain password from user`:**
This misleading message almost always means the keytab-based login
itself failed, not that it's genuinely trying to prompt. Check:
- The keytab's principal matches `jaas.conf`'s `principal=` exactly. It
  must be the AD **account** identity (`connect-svc@PSYNCOPATE.COM`), not
  an SPN string - see `scripts/kerberos/02-create-principals.sh`'s header
  comment for why AD (unlike MIT Kerberos) requires this.
- The keytab file is actually readable by the process reading it (a
  `13/Permission denied` deep in a `KRB5_TRACE` run means file
  permissions, not a real Kerberos failure).
- Set `debug=true` on the last line of `jaas.conf`'s `Krb5LoginModule`
  block (temporarily, and disable Argo CD auto-sync first with
  `oc patch application confluent-platform -n argocd --type=merge -p
  '{"spec":{"syncPolicy":{"automated":null}}}'` so self-heal doesn't
  revert the debug change before you see it) for a detailed trace.

**JDBC connector fails with `java.net.PortUnreachableException` in a
Kerberos stack trace:** Java's krb5 client attempted UDP to the KDC
despite `udp_preference_limit` being set low - a real JDK quirk, not a
config mistake (confirmed live neither `udp_preference_limit=0` nor `=1`
alone stopped it, and the `kdc = tcp/host:port` transport-prefix syntax
was unreliable across JVM restarts). Fix is network-level: make the node
drop that UDP traffic instead of rejecting it, so Java's normal TCP
fallback triggers - `scripts/kerberos/07-setup-connect-tunnel.sh` does
this via an `iptables ... -j DROP` rule on the node.

**Connector reaches `RUNNING` but records never appear in the topic /
task fails with `PKIX path building failed`:** Unrelated to Kerberos -
Connect's JVM doesn't trust Schema Registry's self-signed TLS cert. Fix
via `scripts/kerberos/08-build-truststore.sh` (regenerates
`base/confluent-platform/connect-truststore-configmap.yaml`).

**Task fails with `UNKNOWN_TOPIC_OR_PARTITION`:** Auto topic creation is
disabled on this cluster. Create the topic declaratively - see
`base/confluent-platform/sqlserver-claims-topic.yaml`. If the
`KafkaTopic` CR itself sits in `ERROR` with `dial tcp ...:8090: i/o
timeout`, the CFK operator's NetworkPolicy ingress rule
(`confluent-operator-management` in `bootstrap/network-policies.yaml`)
is missing port 8090 (Kafka's REST Admin API, separate from its
Jolokia/JMX ports).

**Task fails with `IllegalAccessError` mentioning
`org.apache.kafka.common.utils.SystemTime`:** JDBC connector plugin
version mismatch with the Connect runtime's own `kafka-clients` - bump
the version in `base/confluent-platform/connect-kerberos-patch.yaml`'s
`build.onDemand.plugins.confluentHub` entry.

**NetworkPolicy blocks any of the above:** see the "NetworkPolicy
blocking traffic: diagnosis commands" section of `docs/troubleshooting.md`
- the same diagnostic commands apply, just against the `confluent`
namespace and `bootstrap/network-policies.yaml`'s `connect-external-egress`.
