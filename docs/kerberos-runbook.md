# LDAP / Kerberos runbook

See `docs/kerberos-architecture.md` for the full design and the reasoning
behind each choice. This is the operational sequence only.

The Samba AD DC and SQL Server both run as Docker Desktop containers on
your Mac, not in the cluster - see `docs/kerberos-architecture.md` for
why. All steps below assume Docker Desktop is running and a SQL Server
container (default name `sqltest2`) already exists.

## First-time setup

Everything - provisioning the AD DC, creating accounts, exporting/sealing
keytabs, joining SQL Server to the domain, opening the Connect-side
tunnel, building the Schema Registry truststore, and registering the
connector - runs via one script:

```bash
SQLSERVER_HOST=<address> SQLSERVER_PORT=<port> ./scripts/kerberos/setup-kerberos.sh
```

`SQLSERVER_HOST`/`SQLSERVER_PORT` is the address Connect uses to reach
SQL Server - typically the CRC node's internal IP and whatever port an
SSH reverse tunnel into Docker Desktop's SQL Server container is
forwarding (see "Reaching Docker Desktop from the cluster" in the
architecture doc). Example: `SQLSERVER_HOST=192.168.126.11
SQLSERVER_PORT=14330`.

Idempotent throughout - safe to re-run in full any time, including after
a Mac reboot or `crc stop`/`crc start` (neither of which any of this
survives - see below). It prints numbered section headers as it goes;
each section's own checks (`OK`/`FAIL`) tell you exactly what state
things were in before it acted.

One manual, one-time step it doesn't do for you: creating SQL Server's
own login, after the first successful run:
```sql
CREATE LOGIN [PSYNCOPATE\connect-svc] FROM WINDOWS;
-- in the target database:
CREATE USER [PSYNCOPATE\connect-svc] FOR LOGIN [PSYNCOPATE\connect-svc];
ALTER ROLE db_datareader ADD MEMBER [PSYNCOPATE\connect-svc];
```

Validate the whole chain any time with:
```bash
./scripts/kerberos/validate-kerberos.sh
```

## What needs re-running, and when

Nothing here is one-and-done - three different things stop working for
three different reasons, and the fix each time is the same: re-run
`setup-kerberos.sh`.

- **After `docker restart sqltest2`**: Docker regenerates SQL Server's
  `/etc/resolv.conf`/`/etc/hosts` and kills its backgrounded `sssd`
  process (standard Docker behavior on restart, not something a script
  controls) - this breaks domain trust (`Login failed... untrusted
  domain`) until redone. The AD domain itself and the SQL login both
  survive fine.
- **After a Mac reboot or `crc stop`/`crc start`**: kills the SSH reverse
  tunnel and clears the node's iptables rule - Connect starts failing
  with `Cannot get a KDC reply`/`PortUnreachableException`.
- **After a `sambaad` container crash** (rare - Docker's own
  `--restart=unless-stopped` policy normally recovers this
  automatically, see the architecture doc): if it doesn't, `docker start
  sambaad` then re-run the script.

## Re-running after a keytab rotation

Just re-run `setup-kerberos.sh` - keytab export is idempotent
(`exportkeytab` always writes the account's *current* key, producing the
same keytab unless the account's password changed since).

## Troubleshooting

**`net ads join` fails with `NT_STATUS_NO_TRUST_SAM_ACCOUNT`:** Not
something `setup-kerberos.sh` calls (it creates the SQL Server computer
account directly via `samba-tool` instead, precisely because this
self-check is unreliable/spurious in this setup - confirmed live, the
trust account it just created is usually genuinely valid, checkable
directly with `docker exec sambaad samba-tool computer list -U
administrator%...`). If you see this, you're running `net ads join`
manually outside the script - don't; use the script's approach instead.

**`kinit`/CLDAP discovery fails with `SRV record not found` or `Cannot
find KDC for realm`:** Almost always a DNS issue, not a Kerberos one -
`sssd`'s `ad` provider needs to resolve `_ldap._tcp.psyncopate.com` SRV
records, which only Samba's own DNS server (enabled via
`--dns-backend=SAMBA_INTERNAL`) can answer. Check the querying
container's `/etc/resolv.conf` points at the `sambaad` container's IP.

**`kinit`/sssd fails with `Client not found in Kerberos database` for a
`host/...` or account principal:** The AD account's `userPrincipalName`
doesn't match the principal string being used to authenticate. AD only
lets an account's own identity (or an explicitly-set UPN matching what's
being requested) authenticate as a Kerberos client - an SPN attached to
an account is not itself a valid client identity, even though it looks
like one. `setup-kerberos.sh` sets this explicitly via `ldbmodify` for
SQL Server's computer account; if you've created any other account by
hand, it needs the same treatment.

**`kinit`/sssd fails with `Keytab contains no suitable keys for
<principal>` despite the keytab looking right:** Check case - Kerberos
principal matching is case-sensitive, and a keytab exported for
`HOST/SOMEHOST.domain` will not satisfy a request for
`host/somehost.domain` (confirmed live, this exact mismatch). Keep
hostnames lowercase everywhere consistently.

**JDBC connector fails with `Unable to obtain password from user`:**
This misleading message almost always means the keytab-based login
itself failed, not that it's genuinely trying to prompt. Check:
- The keytab's principal matches `jaas.conf`'s `principal=` exactly. It
  must be the AD **account** identity (`connect-svc@PSYNCOPATE.COM`), not
  an SPN string - same underlying reason as the `Client not found` case
  above.
- The keytab file is actually readable by the process reading it (a
  `13/Permission denied` deep in a `KRB5_TRACE` run means file
  permissions, not a real Kerberos failure).
- Set `debug=true` on the last line of `jaas.conf`'s `Krb5LoginModule`
  block (temporarily, and disable Argo CD auto-sync first with
  `oc patch application confluent-platform -n argocd --type=merge -p
  '{"spec":{"syncPolicy":{"automated":null}}}'`, then re-enable it with
  `'{"spec":{"syncPolicy":{"automated":{"prune":true,"selfHeal":true}}}}'`
  once done - otherwise self-heal reverts the debug change, and separately
  every other uncommitted live fix, before you get to see it) for a
  detailed trace.

**JDBC connector fails with `java.net.PortUnreachableException` or
`Cannot get a KDC reply` in a Kerberos stack trace:** Java's krb5 client
attempts UDP to the KDC first regardless of `udp_preference_limit` - a
real JDK quirk, not a config mistake (confirmed live neither
`udp_preference_limit=0` nor `=1` alone stopped it, and the `kdc =
tcp/host:port` transport-prefix syntax was unreliable across JVM
restarts). Fix is network-level, not config: make the node drop that UDP
traffic instead of rejecting it, so Java's normal TCP fallback triggers -
`setup-kerberos.sh` does this via an `iptables ... -j DROP` rule on the
node. If you see this, the tunnel/iptables rule most likely didn't
survive a reboot - re-run the script.

**Connector reaches `RUNNING` but records never appear in the topic /
task fails with `PKIX path building failed`:** Unrelated to Kerberos -
Connect's JVM doesn't trust Schema Registry's self-signed TLS cert.
`setup-kerberos.sh` rebuilds
`base/confluent-platform/connect-truststore-configmap.yaml` every run;
re-run it if the CA has rotated since.

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

**Connector/task status shows an error that doesn't match what you just
fixed:** Kafka Connect's REST status endpoint can return a stale
snapshot from a previous attempt (confirmed live, repeatedly - same
`ClientConnectionId` reappearing across unrelated runs). Check the
`ClientConnectionId` in the trace against connect's own logs
(`oc logs -n confluent connect-0 -c connect`) before troubleshooting
further; if it's from an earlier timestamp, restart the task
(`curl -sk -X POST ".../connectors/sqlserver-claims-source/tasks/0/restart"`)
and re-check.

**NetworkPolicy blocks any of the above:** see the "NetworkPolicy
blocking traffic: diagnosis commands" section of `docs/troubleshooting.md`
- the same diagnostic commands apply, just against the `confluent`
namespace and `bootstrap/network-policies.yaml`'s `connect-external-egress`.
