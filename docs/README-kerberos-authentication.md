# How Kerberos Authentication Works: Kafka Connect → AD DC → SQL Server

This README explains, end-to-end, how the JDBC Source Connector running
in Kafka Connect authenticates to SQL Server using Kerberos, how the
Active Directory domain controller (the "LDAP" backing it) is
provisioned with its service accounts and database-level access
boundaries, and which network paths/policies have to be open for the
whole chain to work. It complements `docs/kerberos-architecture.md`
(the deeper design-decision log) with a single top-to-bottom narrative.

## 1. The moving parts

```
┌─────────────────────────── Docker Desktop (Mac host) ───────────────────────────┐
│                                                                                   │
│   ┌──────────────┐   kerberos-net (Docker bridge, full TCP+UDP)   ┌───────────┐  │
│   │   sambaad    │◄────────────────────────────────────────────► │  sqltest2 │  │
│   │ Samba4 AD DC │   domain-join, CLDAP, DNS SRV, LDAP, SMB/RPC   │SQL Server │  │
│   │ PSYNCOPATE   │                                                │ (domain-  │  │
│   │   .COM       │                                                │  joined)  │  │
│   └──────┬───────┘                                                └─────┬─────┘  │
│          │ port 88 (KDC)                                   port 1433   │        │
└──────────┼──────────────────────────────────────────────────────────────┼────────┘
           │           reverse SSH tunnels through the CRC VM             │
           │           (ssh -R, GatewayPorts yes)                         │
┌──────────▼─────────────────────────────────────────────────────────────▼────────┐
│  CRC node (192.168.126.11)             confluent namespace (OpenShift)          │
│  :18088 → sambaad:88                   ┌──────────────────────────────┐        │
│  :14330 → sqltest2:1433                │      connect-0 (Connect)     │        │
│                                        │  JDBC Source Connector       │        │
│                                        │  "sqlserver-claims-source"   │        │
│                                        └──────────────────────────────┘        │
└──────────────────────────────────────────────────────────────────────────────────┘
```

- **`sambaad`** is a real Active Directory domain controller (Samba 4 in
  AD-DC mode), realm `PSYNCOPATE.COM` - not a plain MIT KDC. This is the
  "LDAP" in the request: Samba's AD DC mode *is* an LDAP directory (AD
  schema) plus a Kerberos KDC plus DNS plus SMB/RPC, all as one service.
- **`sqltest2`** is SQL Server, domain-joined to `PSYNCOPATE.COM` so its
  OS identity layer (`sssd`, `ad` provider) can validate Windows/Kerberos
  logins against that directory.
- **`connect-0`** is Kafka Connect, running the Confluent JDBC Source
  Connector (`sqlserver-claims-source`), configured for
  `authenticationScheme=JavaKerberos` in its JDBC URL.
- Provisioning and wiring for all of the above is one script:
  `scripts/kerberos/setup-kerberos.sh` (idempotent, safe to re-run any
  time - see its own header and `docs/kerberos-runbook.md`).

## 2. How the AD DC ("LDAP") is created, with service accounts and DB access boundaries

### 2.1 Provisioning the directory

`setup-kerberos.sh` section 1 runs `samba-tool domain provision` inside
the `sambaad` container, creating:
- The `PSYNCOPATE.COM` realm / `PSYNCOPATE` domain, with Samba acting as
  both the AD DC and its own DNS server (`--dns-backend=SAMBA_INTERNAL` -
  required for CLDAP/discovery to work at all, see
  `docs/kerberos-architecture.md`).
- A persistent domain database (`/var/lib/samba/private/sam.ldb`) on a
  named Docker volume (`sambaad-data`), so the domain survives container
  recreation.

### 2.2 Service accounts (least-privilege, purpose-scoped identities)

Section 2 of the script creates exactly two AD accounts, each scoped to
one job:

| Account | Purpose | Gets an SPN? |
|---|---|---|
| `connect-svc` | Kafka Connect's own Kerberos client identity - the account it `kinit`s as | No - it authenticates as itself, it isn't looked up as a service |
| `mssql-svc` | Holds the SPN (`MSSQLSvc/<host>:<port>`) SQL Server's own Kerberos service identity resolves against | Yes - one, matching SQL Server's own service string |

This is a deliberate AD-specific distinction (see
`docs/kerberos-architecture.md`, "AD accounts are not the same thing as
MIT principals"): in AD, only an account's own identity can be a client
principal; SPNs are lookup aliases for *services*, never a separate
identity you authenticate as. Using the wrong shape here
(`connect/host@REALM`, the MIT-style form) fails outright with
`Client not found in Kerberos database`.

SQL Server itself gets a third, separate machine identity - its own AD
computer account (`samba-tool computer create`, section 4) with a
`host/<fqdn>` SPN, used by `sssd` to resolve incoming Kerberos identities
to trusted domain accounts.

Each account's keytab is exported and, for Connect's, sealed into git
(`base/confluent-platform/secrets/connect-keytab-sealed.yaml`) so it
never exists in the repo as plaintext.

### 2.3 Database-level access boundary (authorization, separate from authentication)

A successful Kerberos ticket exchange only proves *who* `connect-svc`
is - it does not, by itself, grant that account any rights inside SQL
Server. Authorization is a second, independent step, done once per
target database (`setup-kerberos.sh` section 4, idempotent):

```sql
-- Server-level: trust the Windows/AD identity as a SQL Server login
CREATE LOGIN [PSYNCOPATE\connect-svc] FROM WINDOWS;

-- Database-level: map that login to a user, in *this* database only
USE claims_db;
CREATE USER [PSYNCOPATE\connect-svc] FOR LOGIN [PSYNCOPATE\connect-svc];

-- Least privilege: read-only, nothing else
ALTER ROLE db_datareader ADD MEMBER [PSYNCOPATE\connect-svc];
```

This is the actual **access boundary**: `connect-svc` can authenticate
via Kerberos from anywhere in the domain, but SQL Server only lets it
read (`db_datareader`) inside `claims_db` - no write, no DDL, no access
to any other database, because no corresponding `CREATE USER` exists
there. Without this step, the connector authenticates successfully at
the Kerberos layer and then fails at the SQL layer with
`Login failed for user 'PSYNCOPATE\connect-svc'` - a real, previously
manual failure mode now automated by the script (see
`docs/kerberos-runbook.md`).

## 3. How Kafka Connect authenticates itself

Three pieces come together inside the `connect-0` pod, all mounted via
`base/confluent-platform/connect-kerberos-patch.yaml`:

1. **`connect-krb5-conf`** (`base/confluent-platform/connect-krb5-configmap.yaml`)
   - the client-side `krb5.conf` pointing at the realm and KDC:
   ```
   [realms]
     PSYNCOPATE.COM = { kdc = 192.168.126.11:18088 }
   ```
   `192.168.126.11` is the CRC node's real IP; `18088` is the reverse
   SSH tunnel's port on that node, forwarding to `sambaad:88` in Docker
   Desktop (§4 covers why this must reach the node's *real* IP, not just
   `localhost`).

2. **`connect-jaas-conf`** (`base/confluent-platform/connect-jaas-configmap.yaml`)
   - the JAAS login module mssql-jdbc's `JavaKerberos` auth scheme reads,
   naming the keytab and principal to authenticate as:
   ```
   SQLJDBCDriver {
       com.sun.security.auth.module.Krb5LoginModule required
       useKeyTab=true
       keyTab="/mnt/secrets/connect-keytab/connect.keytab"
       principal="connect-svc@PSYNCOPATE.COM"
       ...
   };
   ```

3. **The JDBC connection URL** (registered via the Connect REST API by
   `setup-kerberos.sh` section 7):
   ```
   jdbc:sqlserver://<SQLSERVER_HOST>:<SQLSERVER_PORT>;databaseName=claims_db;
     integratedSecurity=true;authenticationScheme=JavaKerberos;encrypt=false;
   ```

**The actual handshake, step by step:**

1. mssql-jdbc's driver sees `authenticationScheme=JavaKerberos` and
   invokes the `SQLJDBCDriver` JAAS module.
2. That module loads `connect-svc`'s keytab and, using
   `connect-krb5-conf`'s realm config, sends an **AS-REQ** to the KDC
   (`sambaad`, via the `192.168.126.11:18088` tunnel) - "I am
   `connect-svc@PSYNCOPATE.COM`, give me a Ticket-Granting Ticket."
3. With a TGT in hand, the driver sends a **TGS-REQ** for a service
   ticket to SQL Server's own SPN (`MSSQLSvc/<host>:<port>`, held by the
   `mssql-svc` account) - both requests are plain TCP Kerberos traffic
   over that same tunnel.
4. The driver embeds that service ticket in a **SPNEGO** token inside
   the TDS login packet it sends to SQL Server on port 1433 (via the
   *other* tunnel, to `sqltest2`).
5. SQL Server validates the ticket cryptographically, then asks its own
   `sssd` (`ad` provider) to resolve the client identity against the
   domain - this is the AD-specific trust check that a non-AD
   LDAP+MIT-KDC pair cannot satisfy (see `docs/kerberos-architecture.md`).
6. Once trusted, SQL Server maps the now-authenticated
   `PSYNCOPATE\connect-svc` identity to its SQL login and applies the
   `db_datareader`-only authorization boundary from §2.3.

Only step 2/3 (Kerberos itself) and step 4 (TDS/SPNEGO) cross into the
cluster; everything else (domain trust, CLDAP, DNS) stays entirely
within Docker Desktop's own network, for the UDP reasons in
`docs/kerberos-architecture.md`.

## 4. Network paths and policies that make Connect → AD DC → SQL Server possible

### 4.1 The two reverse SSH tunnels

Both are plain `ssh -R` tunnels through the CRC VM's sshd
(`core@127.0.0.1:2222`), carrying only TCP (Kerberos AS-REQ/TGS-REQ and
TDS/SPNEGO are both TCP-only, so no UDP relay is ever needed for
Connect's side of things):

| Tunnel | Node port | Forwards to | Carries |
|---|---|---|---|
| KDC | `18088` | `sambaad:88` (Docker Desktop) | Kerberos AS-REQ/TGS-REQ |
| SQL Server | `14330` (example - configurable via `SQLSERVER_PORT`) | `sqltest2:1433` (Docker Desktop) | TDS + embedded SPNEGO |

**Critical, previously-undocumented requirement:** the CRC node's sshd
must have `GatewayPorts yes`. Without it, `ssh -R 0.0.0.0:PORT:...`
silently binds to loopback only (`127.0.0.1`/`::1`) regardless of what
was requested - the tunnel looks perfectly healthy from the Mac or the
CRC node itself, but pods (which reach the node via its real IP,
`192.168.126.11`) get `Connection refused`. `setup-kerberos.sh` now
detects and fixes this automatically (see `docs/kerberos-runbook.md`).

A second Java-specific fix, already baked into the node: an iptables
`DROP` rule for UDP on the KDC tunnel's port. Java's Kerberos client
tries UDP first regardless of `udp_preference_limit` in `krb5.conf` (a
real JDK quirk); since the tunnel is TCP-only, an unanswered UDP send
would otherwise get an instant ICMP "port unreachable" that Java treats
as fatal. Dropping (not rejecting) that UDP traffic makes the send
silently time out instead, which does trigger Java's normal TCP
fallback.

### 4.2 In-cluster NetworkPolicy (`bootstrap/network-policies.yaml`)

The `confluent` namespace is default-deny (ingress and egress) with
narrow allow rules punched through per component. The one relevant here
is scoped to the `connect` pod only:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: connect-external-egress
  namespace: confluent
spec:
  podSelector:
    matchLabels:
      app: connect
  policyTypes:
    - Egress
  egress:
    - to:
        - ipBlock:
            cidr: 0.0.0.0/0
            except:
              - 169.254.169.254/32   # cloud metadata endpoint, explicitly excluded
```

This is deliberately the only pod in the namespace allowed broad
external egress, for two reasons that both apply here:
1. Reaching the AD DC and SQL Server, neither of which are in-cluster
   Services - they're external addresses (the CRC node's real IP over
   the tunnel ports above), not resolvable via a `podSelector`/
   `namespaceSelector` rule the way every other policy in this file
   uses.
2. Installing the JDBC connector plugin itself via Confluent Hub at
   pod-init time (`connect-kerberos-patch.yaml`'s
   `spec.build.onDemand.plugins.confluentHub`), a real internet
   download.

Every other NetworkPolicy in the namespace restricts by **peer only**,
never by port, due to a confirmed OVN-Kubernetes bug on this cluster
where port-restricted egress rules silently drop all traffic on that
port - the corresponding destination's *ingress* rule is what actually
enforces the port in each case. That constraint doesn't apply to
`connect-external-egress` since it isn't port-restricted at all.

### 4.3 Putting it together: why each hop is reachable

| Hop | Path | Enforced by |
|---|---|---|
| `connect-0` → KDC (port 88) | Pod egress → node IP:18088 → tunnel → `sambaad`:88 | `connect-external-egress` (cluster egress) + `GatewayPorts yes` (tunnel reachable from pod network) |
| `connect-0` → SQL Server (port 1433) | Pod egress → node IP:14330 → tunnel → `sqltest2`:1433 | Same as above |
| `sambaad` ↔ `sqltest2` (domain join, CLDAP/DNS/LDAP/SMB, TCP+UDP) | Docker Desktop bridge network (`kerberos-net`) | Native Docker networking - no tunnel, no NetworkPolicy involved (outside the cluster entirely) |
| SQL Server → AD DC (`sssd` identity resolution) | Same Docker bridge | Same as above |

## 5. Validating the whole chain

```bash
./scripts/kerberos/validate-kerberos.sh
```

Checks, in order: Samba AD DC provisioned and accounts present → SQL
Server's `sssd` trusts the domain → `GatewayPorts` enabled → both
tunnels reachable **from a pod**, not just locally → SQL Server login
exists for `connect-svc` → Connect's keytab is mounted and non-empty →
the connector/task are `RUNNING`.

## 6. Re-running after things reset

Nothing here is one-and-done. See `docs/kerberos-runbook.md` for exactly
what breaks and when (Mac reboot, `crc stop`/`start`, `docker restart
sqltest2`) - the fix in every case is the same: re-run
`scripts/kerberos/setup-kerberos.sh` in full. It is idempotent and prints
`OK`/`FAIL` per step so you can see exactly what it found broken.
