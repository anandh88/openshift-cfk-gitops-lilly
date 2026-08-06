# LDAP / Kerberos runbook

See `docs/kerberos-architecture.md` for the full design and the reasoning
behind each choice, including "Why SQL Server runs outside this cluster"
and how to reach it from Connect. This is the operational sequence only.

All steps below that touch SQL Server require `SQLSERVER_HOST` (and
optionally `SQLSERVER_PORT`, default `1433`) set in your shell first -
SQL Server has no in-cluster Service DNS name to fall back on.

## First-time setup

Steps 3-9 below (realm init through validation) can be run one at a time
as shown, or all at once via `./scripts/kerberos/run-all.sh`, which chains
01-05 and validate-kerberos.sh in order and stops on the first failure.
Use the individual scripts instead when re-running only part of the flow
(e.g. the keytab-rotation flow further down only needs steps 4-6).

1. **Deploy the manifests.** If Argo CD's app-of-apps is already running
   this repo, `git push` is enough - it picks up `apps/auth-services-app.yaml`
   and `apps/kerberos-kdc-app.yaml` automatically. Otherwise apply directly:
   ```bash
   oc apply -f base/namespaces/all-namespaces.yaml
   oc apply -f bootstrap/auth-services-scc.yaml
   oc apply -f bootstrap/auth-services-network-policies.yaml
   oc apply -f bootstrap/network-policies.yaml
   oc apply -f apps/auth-services-app.yaml
   oc apply -f apps/kerberos-kdc-app.yaml
   ```
2. **Wait for ldap and kdc pods to be Running:**
   ```bash
   oc get pods -n auth-services -w
   ```
   The `kdc` pod will sit at `0/1 Running` (not Ready) until step 3 - its
   startup script deliberately waits (`sleep infinity`) until the realm is
   initialized. This is expected, not a failure.
3. **Initialize the KDC's LDAP-backed realm:**
   ```bash
   ./scripts/kerberos/01-init-kdc.sh
   ```
   This restarts the `kdc` Deployment at the end - wait for it to become
   Ready (`kadmin` port 749 responding) before continuing.
4. **Create the two service principals** (needs `SQLSERVER_HOST` set):
   ```bash
   SQLSERVER_HOST=<address> ./scripts/kerberos/02-create-principals.sh
   ```
5. **Export their keytabs out of the KDC pod:**
   ```bash
   SQLSERVER_HOST=<address> ./scripts/kerberos/03-export-keytabs.sh
   ```
   Writes `connect.keytab` and `mssql.keytab` to `.kerberos-keytabs/`
   (gitignored - never commit these raw).
6. **Seal the real Connect keytab into git, replacing the placeholder:**
   ```bash
   ./scripts/kerberos/04-seal-keytabs.sh
   git add base/confluent-platform/secrets/connect-keytab-sealed.yaml
   git commit -m "seal real Kerberos keytab for connect"
   git push
   ```
   Argo CD self-heals `connect` once the new sealed secret syncs, and it
   restarts with the real keytab mounted. `mssql.keytab` is left in
   `.kerberos-keytabs/` - mount it directly into wherever SQL Server
   actually runs (see `docs/kerberos-architecture.md`), it's never sealed
   into git.
7. **Confirm Connect is healthy with the real keytab:**
   ```bash
   oc get pods -n confluent -l app=connect
   oc exec -n confluent deploy/connect -- stat -c%s /mnt/secrets/connect-keytab/connect.keytab
   ```
   Should show a real size (tens of bytes), not `2` (the placeholder).
8. **Register the JDBC Source Connector:**
   ```bash
   SQLSERVER_HOST=<address> ./scripts/kerberos/05-deploy-connector.sh
   ```
9. **Validate the whole chain:**
   ```bash
   SQLSERVER_HOST=<address> ./scripts/kerberos/validate-kerberos.sh
   ```

## Re-running after a keytab rotation

Repeat steps 4-6 for whichever principal changed (`ktadd` invalidates any
previously-exported keytab for that principal - see
`scripts/kerberos/03-export-keytabs.sh`'s header comment). No need to
re-run `01-init-kdc.sh` - the realm itself doesn't change.

## Troubleshooting

**`kdc` pod never becomes Ready after `01-init-kdc.sh`:**
Check the pod's own logs first - it re-runs `apt-get install` on every
restart (see `docs/kerberos-architecture.md` for why), so a transient
package-mirror failure will surface here:
```bash
oc logs -n auth-services deploy/kdc
```
If `apt-get` succeeds but it's still not Ready, check for the master-key
stash file on the persistent path:
```bash
oc exec -n auth-services deploy/kdc -- ls -la /var/lib/krb5kdc/
```

**SQL Server rejects the keytab / SPN mismatch:**
Check that the keytab's principal name exactly matches
`MSSQLSvc/<SQLSERVER_HOST>:<SQLSERVER_PORT>@PSYNCOPATE.COM` - SQL Server
fails fast if the SPN in the keytab doesn't match its own FQDN:port. Check
its error log wherever it actually runs for Kerberos-related messages.

**Connector fails with `GSSException: No valid credentials provided`:**
Almost always one of: the keytab is still the empty placeholder (run
`validate-kerberos.sh`, which checks the file size), the principal in
`base/confluent-platform/connect-jaas-configmap.yaml` doesn't match what
was actually created in step 4, or `KRB5_CONFIG`/`KAFKA_OPTS` didn't reach
the JVM (check `oc get connect connect -n confluent -o yaml` under
`spec.podTemplate.envVars`).

**NetworkPolicy blocks any of the above:** see the "NetworkPolicy blocking
traffic: diagnosis commands" section of `docs/troubleshooting.md` - the
same diagnostic commands apply, just against the `auth-services` namespace
and `bootstrap/auth-services-network-policies.yaml`/
`bootstrap/network-policies.yaml`'s `connect-external-egress` instead.
