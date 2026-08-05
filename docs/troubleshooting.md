# Troubleshooting

## Pod not starting: SCC violation

**Symptoms:** Pod stuck in `CreateContainerConfigError` or never scheduled;
`oc describe pod` shows `unable to validate against any security context
constraint`.

1. Check the pod's events: `oc describe pod <pod> -n confluent`.
2. Confirm the pod's service account has `confluent-platform-scc` bound:
   ```
   oc get scc confluent-platform-scc -o jsonpath='{.users}'
   ```
3. If missing, add it: `oc adm policy add-scc-to-user confluent-platform-scc system:serviceaccount:<ns>:<sa>`.
4. Confirm the pod spec matches the SCC's constraints — `runAsUser` in
   1000-65534, `fsGroup`/`supplementalGroups` in the same range, no
   `allowPrivilegeEscalation`, capabilities dropped to `ALL` with only
   `NET_BIND_SERVICE` allowed back.

## PVC not binding: storage class mismatch

**Symptoms:** PVC stuck `Pending`; `oc describe pvc` shows no matching
StorageClass or provisioner errors.

1. Confirm the expected class exists: `oc get storageclass crc-csi-hostpath-provisioner`.
2. Confirm every CR (`kraft.yaml`, `kafka.yaml`, `controlcenter.yaml`)
   references `storageClass.name: crc-csi-hostpath-provisioner` exactly —
   a typo here is the most common cause.
3. Check provisioner pod health: `oc get pods -n hostpath-provisioner`.
4. On CRC specifically, confirm the hostpath provisioner add-on is enabled:
   `crc config get disk-size` / `crc console --credentials`.

## Argo CD OutOfSync: ignoreDifferences patterns

**Symptoms:** Application shows `OutOfSync` forever even though the CR looks
correct, usually because CFK's operator writes back into `.status`.

1. Confirm the Application has an `ignoreDifferences` entry for `/status` on
   the relevant `group`/`kind` (see `apps/confluent-platform-app.yaml` and
   `apps/flink-jobs-app.yaml`).
2. Confirm the same customization exists in `argocd-cm`
   (`resource.customizations.ignoreDifferences.<group>_<Kind>`) if you expect
   it to apply cluster-wide rather than per-Application.
3. Force a refresh to confirm it's actually a status-only diff:
   `argocd app diff confluent-platform`.
4. If the diff includes spec fields, this is a real drift — check for manual
   `oc edit` changes that bypassed git.

## Kafka broker not joining cluster: KRaft quorum issues

**Symptoms:** Kafka pods `CrashLoopBackOff` or logs show
`NOT_LEADER_OR_FOLLOWER` / quorum errors.

1. Check KRaftController health first — Kafka depends on it:
   `oc get kraftcontroller kraftcontroller -n confluent -o wide`.
2. Confirm all KRaft replicas are up: `oc get pods -n confluent -l app=kraftcontroller`.
3. Check for a lost quorum (majority of KRaft pods down) — with
   `replicas: 3` this tolerates 1 failure, not 2.
4. Inspect broker logs for the specific rejection reason:
   `oc logs kafka-0 -n confluent | grep -i quorum`.
5. If storage was wiped on one node only, that node's KRaft/Kafka pod may
   need its PVC deleted and recreated to rejoin cleanly (data loss on that
   replica only, assuming `replication.factor` covers it).

## Flink job not starting: CMF and JM log diagnosis

**Symptoms:** `FlinkApplication` stuck in `CREATING`/`FAILED`.

1. Check the FlinkApplication status: `oc get flinkapplication statefarm-claims-processor -n flink-jobs -o yaml`.
2. Check the CMF operator logs: `oc logs -n cmf-operator deploy/cmf-operator`.
3. Check the JobManager pod logs directly:
   `oc logs -n flink-jobs -l component=jobmanager`.
4. Common causes: `jarURI` path wrong inside the image, `flink-kafka-sasl`
   secret missing/misnamed, or `FlinkEnvironment` not yet `RUNNING` (Flink
   apps depend on their environment, sync-wave 1 vs 2).

## cert-manager cert not issuing: issuer readiness checks

**Symptoms:** `Certificate` stuck `False` on `Ready`; secret never appears.

1. Check the Certificate's conditions: `oc describe certificate kafka-tls -n confluent`.
2. Confirm the issuer chain is healthy, root first:
   ```
   oc get clusterissuer selfsigned-bootstrap platform-ca-issuer
   ```
3. Check the CertificateRequest and Order objects for the real error:
   `oc get certificaterequest -n confluent`.
4. Confirm `platform-root-ca-secret` exists in `cert-manager` namespace —
   `platform-ca-issuer` can't issue without it.

## Sealed secret not decrypting: controller key backup/restore

**Symptoms:** SealedSecret exists but no corresponding `Secret` is created;
controller logs show `no key could decrypt secret`.

1. Sealed values are bound to the sealing key of the cluster that sealed
   them — this is expected after any cluster recreation without a key backup.
2. Check controller logs: `oc logs -n kube-system deploy/sealed-secrets`.
3. If you backed up the signing key before teardown, restore it:
   ```
   oc apply -f sealed-secrets-key-backup.yaml -n kube-system
   oc delete pod -n kube-system -l name=sealed-secrets
   ```
4. If no backup exists, re-run `scripts/seal-secrets.sh` against the new
   controller's public cert to regenerate every sealed manifest from scratch.

## NetworkPolicy blocking traffic: diagnosis commands

**Symptoms:** Component can't reach a dependency despite both pods running
(e.g. Connect can't reach Kafka).

1. Confirm `default-deny-all` is in effect: `oc get networkpolicy default-deny-all -n confluent`.
2. Confirm the specific allow policy exists and matches labels exactly:
   `oc get networkpolicy connect-to-kafka-sr -n confluent -o yaml`.
3. Verify the source pod's labels match the policy's `from.podSelector`:
   `oc get pod <pod> -n confluent --show-labels`.
4. Test connectivity directly: `oc exec -n confluent <connect-pod> -- nc -zv kafka 9092`.
5. For cross-namespace traffic (e.g. `flink-to-kafka`), confirm the source
   namespace carries the label the policy's `namespaceSelector` expects:
   `oc get ns flink-jobs --show-labels` (needs `kubernetes.io/metadata.name=flink-jobs`).
