# Troubleshooting

## Pod not starting: SCC violation

**Symptoms:** Pod stuck in `CreateContainerConfigError` or never scheduled;
`oc describe pod` shows `unable to validate against any security context
constraint`, often listing a specific rejected `runAsUser`/`fsGroup` value
against every built-in SCC's allowed range.

This repo carries **no custom SecurityContextConstraints** by design — every
CFK CR's `podTemplate.podSecurityContext` (and the CMF chart's
`podSecurity.enabled: false`) deliberately omits `runAsUser`/`fsGroup` so
OpenShift's built-in `restricted-v2` SCC can assign both from the
namespace's own allocated range at admission. If you see this symptom:

1. Check the pod's events: `oc describe pod <pod> -n <namespace>`. The
   message enumerates every SCC tried and why each failed - look for a
   `runAsUser`/`fsGroup` value that doesn't fall in `restricted-v2`'s
   namespace range (`oc get ns <namespace> -o jsonpath='{.metadata.annotations}'`
   shows the allocated range under `openshift.io/sa.scc.uid-range`).
2. If the rejected value is a small fixed number (e.g. `999`, `1001`) rather
   than something in that namespace range, a workload's manifest or Helm
   chart is hardcoding a UID/GID. Fix it at the source - add/adjust
   `podTemplate.podSecurityContext` (CFK CRs) or the equivalent chart value
   (e.g. `podSecurity.enabled: false` for CMF) so nothing sets `runAsUser`/
   `fsGroup` explicitly, rather than reaching for a custom SCC to widen the
   allowed range.
3. Confirm the container-level fields `restricted-v2` actually requires are
   still present: `allowPrivilegeEscalation: false`, `capabilities.drop:
   [ALL]`, a `seccompProfile`. CFK sets these automatically on the pods it
   generates regardless of your CR's podTemplate; third-party charts (like
   the upstream Argo CD `redis` Deployment) may need a manual
   `securityContext` added if they don't.

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

**Known gap as of CFK 3.3.0 (certified-operators catalog):** `CMFRestClass`,
`FlinkEnvironment`, and `FlinkApplication` all pass schema validation and sync
cleanly, and CMF itself (`cmf-operator` Application, deployed via Argo CD's
native Helm support from `https://packages.confluent.io/helm`) runs fine —
but no JobManager/TaskManager pods ever get created, and the CFK operator
shows zero log activity for these CRs. Confirmed:
  - `oc get deployment confluent-operator -n confluent-operator -o jsonpath='{.spec.template.spec.containers[0].env}'`
    has a `RELATED_IMAGE_<component>` env var for every other component
    (Kafka, Connect, SchemaRegistry, ksqlDB, ...) but **none for Flink**.
  - `oc get pods,deployment -A | grep -i flink` shows only the CMF pod itself —
    no separate Flink Kubernetes Operator anywhere on the cluster.

Working theory: this operator bundle doesn't ship a live Flink reconciler,
and CFK's Flink integration needs an additional operator component (a Flink
Kubernetes Operator that CMF drives) not provisioned by this repo. Confirm
against official CFK 3.3 Flink-architecture docs before assuming this is
fixable by further CR changes alone.

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
