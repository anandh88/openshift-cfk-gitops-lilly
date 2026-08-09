# Local OpenShift CFK Platform Setup Guide

Complete step-by-step guide to set up Confluent for Kubernetes with SQL Server integration, Kerberos, and LDAP authentication on your local OpenShift cluster.

---

## Phase 0: Prerequisites & Environment Check

### 0.1 Prerequisites

Before starting, ensure you have:

- **CRC (CodeReady Containers)** installed and configured
- **Docker Desktop** running (for SQL Server and Samba AD DC)
- **OpenShift CLI** (`oc`) installed and configured
- **Helm** 3.x installed
- **Git** and **kubeseal** CLI tools
- **kubectl** or **oc** access to your cluster
- Network access to pull container images (Confluent, cert-manager, sealed-secrets)

### 0.2 Quick OpenShift Status Check

Run these commands to verify your OpenShift cluster is operational:

```bash
# 1. Check if you're logged in
oc whoami

# 2. Verify cluster is healthy
oc cluster-info

# 3. Check node status
oc get nodes -o wide

# 4. Verify CRC is running (should show your node)
crc status

# 5. Check available storage classes
oc get storageclass

# Expected output for CRC:
# NAME                             PROVISIONER                             RECLAIM POLICY   STATUS
# crc-csi-hostpath-provisioner     hostpath.csi.k8s.io                     Delete           Available

# 6. (Optional) Start CRC if not running
crc start
```

**If any of these fail, see "Troubleshooting OpenShift" section below.**

---

## Phase 1: Bootstrap CFK Platform on OpenShift

This phase installs the base infrastructure (cert-manager, sealed-secrets, Argo CD, RBAC) that CFK depends on.

### 1.1 Navigate to Repository Root

```bash
cd /Users/anandhvasu/Documents/confluent-ps/openshift-cfk-gitops-lite/openshift-cfk-gitops
```

### 1.2 Run Bootstrap Script

The `scripts/bootstrap.sh` automates steps 1-10 below. Review it first, then run:

```bash
# Make the script executable
chmod +x scripts/bootstrap.sh

# Run the bootstrap (takes ~5-10 minutes)
./scripts/bootstrap.sh
```

**What this does:**
1. Verifies cluster login (`oc whoami`)
2. Creates all namespaces (argocd, cert-manager, confluent-operator, confluent, cmf-operator, flink-jobs)
3. Installs cert-manager v1.14.0 via Helm
4. Waits for cert-manager webhooks to be ready
5. Creates CA hierarchy (selfsigned root → platform-ca-issuer)
6. Issues leaf TLS certificates for all components
7. Installs sealed-secrets controller
8. Saves sealed-secrets public key to `/tmp/sealed-secrets-public-cert.pem`
9. Applies platform RBAC and default-deny NetworkPolicies
10. Installs ArgoCD v2.10.0 (core non-HA install)

### 1.3 Verify Bootstrap Success

```bash
# Wait for cert-manager to be ready
oc rollout status deployment/cert-manager -n cert-manager --timeout=120s
oc rollout status deployment/cert-manager-webhook -n cert-manager --timeout=120s

# Wait for sealed-secrets to be ready
oc rollout status deployment/sealed-secrets -n kube-system --timeout=120s

# Verify certificates are issued
oc get certificate -n confluent-operator
oc get certificate -n confluent
# Expected: All certificates should show READY=True

# Verify ArgoCD is running
oc get pods -n argocd | grep argocd-server
oc get pods -n argocd | grep argocd-repo-server

# Should see something like:
# argocd-application-controller-0                          1/1     Running
# argocd-repo-server-6d8f7f7b8-abc12                        1/1     Running
# argocd-server-xyz                                         1/1     Running
```

**If bootstrap fails, see "Troubleshooting Bootstrap" section below.**

---

## Phase 2: Seal Secrets for Kafka Credentials

Before syncing CFK apps, you need to generate encrypted SealedSecrets for database credentials and Kerberos keys.

### 2.1 Create Plaintext Secrets (Local Only, Never Commit)

Create a temporary secrets file with your intended credentials:

```bash
# Create temporary secrets directory
mkdir -p /tmp/confluent-secrets

# 1. Kafka SASL credentials (internal brokers)
cat > /tmp/confluent-secrets/kafka-sasl-secret.yaml <<'EOF'
apiVersion: v1
kind: Secret
metadata:
  name: kafka-sasl-secret
  namespace: confluent
type: Opaque
stringData:
  username: kafkaadmin
  password: KafkaPass123!
EOF

# 2. Kafka external SASL credentials (client access)
cat > /tmp/confluent-secrets/kafka-external-sasl-secret.yaml <<'EOF'
apiVersion: v1
kind: Secret
metadata:
  name: kafka-external-sasl-secret
  namespace: confluent
type: Opaque
stringData:
  username: clientuser
  password: ClientPass456!
EOF

# 3. ControlCenter credentials
cat > /tmp/confluent-secrets/c3-credentials-secret.yaml <<'EOF'
apiVersion: v1
kind: Secret
metadata:
  name: c3-credentials-secret
  namespace: confluent
type: Opaque
stringData:
  c3_username: admin
  c3_password: ControlCenterPass789!
EOF
```

### 2.2 Seal the Secrets

Using the sealed-secrets public key saved during bootstrap:

```bash
# Seal each secret
kubeseal -f /tmp/confluent-secrets/kafka-sasl-secret.yaml \
  --cert /tmp/sealed-secrets-public-cert.pem \
  -o yaml > base/confluent-platform/secrets/kafka-sasl-sealed.yaml

kubeseal -f /tmp/confluent-secrets/kafka-external-sasl-secret.yaml \
  --cert /tmp/sealed-secrets-public-cert.pem \
  -o yaml > base/confluent-platform/secrets/kafka-external-sasl-sealed.yaml

kubeseal -f /tmp/confluent-secrets/c3-credentials-secret.yaml \
  --cert /tmp/sealed-secrets-public-cert.pem \
  -o yaml > base/confluent-platform/secrets/c3-credentials-sealed.yaml

# Verify the sealed secrets were created
ls -la base/confluent-platform/secrets/

# Clean up plaintext secrets
rm -rf /tmp/confluent-secrets
```

### 2.3 Verify Sealed Secrets

```bash
# Check that SealedSecret manifests don't contain plaintext passwords
grep -r "KafkaPass123" base/confluent-platform/secrets/
# Should return nothing (no matches)

# Check that they are proper SealedSecret objects
grep -r "kind: SealedSecret" base/confluent-platform/secrets/
# Should show all three files
```

---

## Phase 3: Bootstrap ArgoCD Apps (Operators & Confluent Platform)

Now sync the GitOps applications to deploy CFK operator and Confluent Platform.

### 3.1 Create ArgoCD Root Application

First, apply the app-of-apps root application:

```bash
# Apply the root application
oc apply -f apps/app-of-apps.yaml

# Verify it's created
oc get application -n argocd platform-root
oc get application -n argocd -w
# Wait for STATUS to become "Healthy"
```

### 3.2 Sync CFK Operator (Wave 0)

```bash
# Get ArgoCD CLI access (from bootstrap output, or retrieve password)
ARGOCD_PASSWORD=$(oc get secret argocd-initial-admin-secret -n argocd -o jsonpath='{.data.password}' | base64 -d)
ARGOCD_SERVER=$(oc get route -n argocd argocd-server -o jsonpath='{.spec.host}')

echo "ArgoCD Server: https://$ARGOCD_SERVER"
echo "ArgoCD Password: $ARGOCD_PASSWORD"

# Log in locally
argocd login "$ARGOCD_SERVER" \
  --username admin \
  --password "$ARGOCD_PASSWORD" \
  --insecure

# Or use the UI at https://$ARGOCD_SERVER (username: admin, password as above)

# Sync the operator app
oc apply -f apps/confluent-operator-app.yaml

# Watch the sync
oc get application -n argocd confluent-operator -w

# Wait for it to be Healthy
oc get application -n argocd confluent-operator -o jsonpath='{.status.health.status}'
# Should output: Healthy
```

### 3.3 Verify CFK Operator Installation

```bash
# Check operator deployment
oc get pods -n confluent-operator
# Should show one running pod: confluent-operator-*

# Check operator logs
oc logs -n confluent-operator -l app.kubernetes.io/name=confluent-operator -f --tail=100

# Expected log line:
# "confluent-for-kubernetes operator is healthy"
```

### 3.4 Sync Confluent Platform (Wave 1 & Beyond)

```bash
# Apply the confluent-platform app
oc apply -f apps/confluent-platform-app.yaml

# Watch the sync (this takes longer - ~5 minutes)
oc get application -n argocd confluent-platform -w

# Check status periodically
oc get application -n argocd confluent-platform -o jsonpath='{.status.health.status}{"\n"}'
```

### 3.5 Verify Confluent Platform Deployment

This can take 5-10 minutes. Monitor progress:

```bash
# Watch KRaft controllers come up
oc get pods -n confluent -l app.kubernetes.io/name=kraft-controller -w
# Should eventually show 1 pod in Running state

# Watch Kafka brokers
oc get pods -n confluent -l app.kubernetes.io/name=kafka -w
# Should eventually show 1 pod in Running state

# Watch Schema Registry
oc get pods -n confluent -l app.kubernetes.io/name=schema-registry -w

# Watch Connect
oc get pods -n confluent -l app.kubernetes.io/name=connect -w

# Watch ControlCenter
oc get pods -n confluent -l app.kubernetes.io/name=control-center -w

# Overall status
oc get pods -n confluent -o wide

# All should eventually be Running

# Check KRaft cluster status
oc exec -n confluent kraft-controller-0 -c kraft-controller -- bash -c \
  '/opt/confluent/confluent-cli/bin/confluent cluster state' || echo "KRaft not ready yet"

# Check Kafka broker readiness
oc exec -n confluent kafka-0 -c kafka -- bash -c \
  'kafka-broker-api-versions.sh --bootstrap-server kafka:9092' 2>/dev/null || echo "Kafka not ready yet"
```

**Expected final state:**
```
NAME                        READY   STATUS    RESTARTS   AGE
kraft-controller-0          1/1     Running   0          3m
kafka-0                     1/1     Running   0          2m
schema-registry-0           1/1     Running   0          90s
connect-0                   1/1     Running   0          80s
restproxy-0                 1/1     Running   0          70s
control-center-0            1/1     Running   0          60s
```

---

## Phase 4: Set Up SQL Server in Docker Desktop

The SQL Server database runs in Docker on your Mac, not in the cluster. Connect will access it via an SSH reverse tunnel.

### 4.1 Start SQL Server Container

```bash
# Pull SQL Server image (this is large, ~4GB)
docker pull mcr.microsoft.com/mssql/server:2019-latest

# Start SQL Server container
docker run -d \
  --name sqltest2 \
  --restart unless-stopped \
  -e "ACCEPT_EULA=Y" \
  -e "SA_PASSWORD=YourSaPassword123!" \
  -p 1433:1433 \
  mcr.microsoft.com/mssql/server:2019-latest

# Verify it's running
docker ps | grep sqltest2

# Get the Docker internal IP (you'll need this for the tunnel)
docker inspect sqltest2 | grep '"IPAddress"'
# Note the IP, typically 172.17.0.x

# Test SQL Server connectivity from Mac
sqlcmd -S localhost -U sa -P 'YourSaPassword123!' -Q "SELECT @@VERSION"
# Should return SQL Server version info
```

### 4.2 Create Test Database and Table

```bash
# Create a test database for claims data
sqlcmd -S localhost -U sa -P 'YourSaPassword123!' <<'EOF'
CREATE DATABASE claims_db;
GO

USE claims_db;
GO

CREATE TABLE claims (
  claim_id BIGINT IDENTITY(1,1) PRIMARY KEY,
  customer_id INT NOT NULL,
  claim_amount DECIMAL(10, 2) NOT NULL,
  claim_date DATETIME NOT NULL DEFAULT GETDATE(),
  claim_status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
  description VARCHAR(500)
);
GO

-- Insert some sample data
INSERT INTO claims (customer_id, claim_amount, claim_status, description) VALUES
  (1001, 500.00, 'PENDING', 'Vehicle damage claim'),
  (1002, 1250.50, 'APPROVED', 'Medical claim'),
  (1003, 750.25, 'PENDING', 'Property damage claim');
GO

-- Verify data
SELECT * FROM claims;
GO
EOF

echo "Database 'claims_db' created successfully"
```

### 4.3 Open SSH Reverse Tunnel from CRC Node

The CRC node needs to forward traffic to Docker Desktop's SQL Server. Get the CRC node's internal IP and set up the tunnel:

```bash
# Get CRC node's IP
CRC_IP=$(crc info | grep "DNS" | awk '{print $NF}')
echo "CRC Node IP: $CRC_IP"

# Get Docker Desktop's IP from inside CRC node
crc ssh << 'EOF'
# Test connectivity to Docker Desktop (host.docker.internal on Mac, or use docker0 bridge)
ping -c 1 192.168.126.1
# Or check what Docker Desktop forwards as:
echo "Checking routing..."
netstat -rn | head -20
EOF

# Simpler approach: Use host.docker.internal DNS name from Mac (Docker Desktop special hostname)
# Set up reverse SSH tunnel from Mac to CRC node
# This forwards port 14330 on CRC to port 1433 on Docker Desktop (SQL Server)
ssh -R 14330:127.0.0.1:1433 core@$CRC_IP -N &
TUNNEL_PID=$!
echo "SSH tunnel PID: $TUNNEL_PID"

# Verify tunnel is open
sleep 2
netstat -an | grep 14330 | head -5
# Should show listening on port 14330

# Test from CRC node
crc ssh -- nc -zv localhost 14330
# Should show: Connection to localhost (127.0.0.1) 14330 [14330/tcp] succeeded!
```

### 4.4 Test SQL Server Connectivity from CRC

```bash
# SSH into CRC node
crc ssh

# Inside CRC node, install MSSQL tools if not present
sudo dnf install -y mssql-tools

# Test connection to SQL Server (via reverse tunnel)
sqlcmd -S localhost,14330 -U sa -P 'YourSaPassword123!' -Q "SELECT @@VERSION"

# Exit CRC node
exit
```

---

## Phase 5: Set Up Kerberos & LDAP (Samba AD DC)

This phase provisions a Samba4 Active Directory domain controller in Docker Desktop to provide Kerberos authentication for SQL Server.

### 5.1 Prerequisites for Kerberos Setup

Ensure you have:
- SQL Server container running (`docker ps | grep sqltest2`)
- Docker Desktop running with network connectivity
- The `scripts/kerberos/setup-kerberos.sh` script from the repo
- Sufficient disk space (~2GB for Samba domain)

### 5.2 Determine SQL Server Host Address

From the perspective of Connect running in the cluster, how does it reach SQL Server?

**Option A: Via reverse SSH tunnel (recommended)**
- CRC node forwards port to Docker Desktop
- Connect accesses: `<CRC_IP>:14330` (or whatever tunnel port you chose)

**Option B: Direct Docker network access (if Docker Desktop networking configured)**
- Direct IP to Docker Desktop

For this guide, assume **Option A** (reverse SSH tunnel).

```bash
# Get the CRC IP that Connect will use
CRC_IP=$(crc info | grep "DNS" | awk '{print $NF}')

# The tunnel forwards <CRC_IP>:14330 to 127.0.0.1:1433 (SQL Server)
# So Connect will use SQLSERVER_HOST=<CRC_IP> SQLSERVER_PORT=14330
echo "Set SQLSERVER_HOST=$CRC_IP and SQLSERVER_PORT=14330"
```

### 5.3 Run Kerberos Setup Script

```bash
# Ensure reverse SSH tunnel is still open (or restart it)
CRC_IP=$(crc info | grep "DNS" | awk '{print $NF}')
ps aux | grep "ssh -R 14330" || ssh -R 14330:127.0.0.1:1433 core@$CRC_IP -N &

# Wait a moment for tunnel to establish
sleep 2

# Run the kerberos setup script
cd /Users/anandhvasu/Documents/confluent-ps/openshift-cfk-gitops-lite/openshift-cfk-gitops

SQLSERVER_HOST=$CRC_IP SQLSERVER_PORT=14330 ./scripts/kerberos/setup-kerberos.sh

# This script will:
# 1. Create Docker network for Samba
# 2. Start Samba AD DC container (sambaad)
# 3. Provision PSYNCOPATE.COM domain
# 4. Create connect-svc account with keytab
# 5. Join SQL Server to the domain
# 6. Create SPNEGO tunnel for Kerberos KDC
# 7. Export keytab and create SealedSecret
# 8. Build Schema Registry truststore
# 9. Create sqlserver-claims-topic in Kafka
# 10. Register JDBC source connector

# Expected output (sections):
# 1. Samba AD domain controller (Docker Desktop)
#    - OK: Network created / OK: Volume created / OK: Container started
# 2. SQL Server domain join
#    - OK: SQL Server container joined
# 3. Connect tunnel for KDC access
#    - OK: Tunnel established
# 4. Connect service account & keytab
#    - OK: Keytab exported
# 5. Schema Registry truststore
#    - OK: Truststore built
# 6. Kafka topic & connector
#    - OK: Topic created / OK: Connector registered
```

**This script takes 5-10 minutes. Be patient.**

### 5.4 Verify Kerberos Setup

```bash
# Run validation script
./scripts/kerberos/validate-kerberos.sh

# Should output OK for:
# - Samba domain accessible
# - Connect service account exists
# - Keytab is valid
# - SQL Server domain trust works
# - Connect pod can reach KDC
# - JDBC connector is registered

# Additionally, verify manually:

# 1. Check Samba AD DC is running
docker ps | grep sambaad
# Should show: sambaad container running

# 2. Check Connect keytab exists and is sealed
oc get sealedsecret -n confluent connect-keytab-secret
# Should show: connect-keytab-secret exists

# 3. Check sqlserver-claims-topic exists
oc exec -n confluent kafka-0 -c kafka -- bash -c \
  'kafka-topics.sh --list --bootstrap-server kafka:9092 | grep sqlserver-claims'
# Should output: sqlserver-claims-topic

# 4. Check connector is registered
oc exec -n confluent connect-0 -c connect -- curl -s localhost:8083/connectors | jq .
# Should show: ["sqlserver-claims-connector"] (or similar)

# 5. Check connector status
oc exec -n confluent connect-0 -c connect -- curl -s localhost:8083/connectors/sqlserver-claims-connector/status | jq .
# Status should be RUNNING
```

---

## Phase 6: Verify Data Streaming (SQL → Kafka)

Now test the complete flow: data flows from SQL Server → Connect JDBC Source → Kafka topic.

### 6.1 Monitor Connector Task Status

```bash
# Check connector status in real-time
oc exec -n confluent connect-0 -c connect -- bash -c \
  'watch -n 2 "curl -s localhost:8083/connectors/sqlserver-claims-connector/status | jq .status"'

# Expected: {"state": "RUNNING", "worker_id": "..."}

# Check task status
oc exec -n confluent connect-0 -c connect -- bash -c \
  'curl -s localhost:8083/connectors/sqlserver-claims-connector/status | jq .tasks'

# Expected: [{"id": 0, "state": "RUNNING", "worker_id": "..."}]
```

### 6.2 Verify Records in Kafka Topic

```bash
# Check topic has messages
oc exec -n confluent kafka-0 -c kafka -- bash -c \
  'kafka-log-dirs.sh --bootstrap-server kafka:9092 --describe | grep sqlserver-claims'

# Consume messages from the topic
oc exec -n confluent kafka-0 -c kafka -- bash -c \
  'kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 \
    --topic sqlserver-claims-topic \
    --from-beginning \
    --consumer.config /opt/confluentinc/etc/kafka/client.properties \
    --max-messages 10'

# Should output Avro-encoded messages (binary, but recognizable)
# Each message is a row from the SQL Server claims table
```

### 6.3 View in ControlCenter UI

Access ControlCenter to visualize the data flow:

```bash
# Get ControlCenter route
oc get route -n confluent control-center -o jsonpath='{.spec.host}'
# Output: controlcenter.apps-crc.testing

# Open browser to https://controlcenter.apps-crc.testing
# Username/password from sealed secrets (ControlCenter admin creds)

# Or retrieve from sealed secret
oc get sealedsecret -n confluent c3-credentials-secret -o yaml | \
  yq '.spec.encryptedData | keys[]'
# Shows: c3_password, c3_username

# You can view in UI:
# - Topic: sqlserver-claims-topic (messages in, message rate)
# - Connectors: sqlserver-claims-connector (RUNNING status, task metrics)
# - Schema Registry: claims schema (Avro schema for records)
# - Monitor → Alerts: real-time metrics
```

### 6.4 Insert New Data and Verify It Flows

```bash
# Add a new claim to SQL Server
sqlcmd -S localhost -U sa -P 'YourSaPassword123!' <<'EOF'
USE claims_db;
INSERT INTO claims (customer_id, claim_amount, claim_status, description) VALUES
  (1004, 2000.00, 'PENDING', 'New claim from test');
GO
EOF

# Wait 5-10 seconds for connector to pick it up

# Consume again (should see the new record)
oc exec -n confluent kafka-0 -c kafka -- bash -c \
  'kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 \
    --topic sqlserver-claims-topic \
    --consumer.config /opt/confluentinc/etc/kafka/client.properties \
    --max-messages 1'

# Should show the newly inserted record
```

---

## Phase 7: LDAP User Authentication (Optional)

If you want to add LDAP user management (beyond Kerberos), you can configure Connect and ControlCenter to use LDAP for user accounts.

### 7.1 Create LDAP Users in Samba AD

```bash
# The Samba AD DC container has LDAP built-in
# Create Connect service account with specific permissions:

docker exec sambaad samba-tool user create \
  connect-svc \
  'ConnectServicePass123!' \
  --mail=connect@psyncopate.com \
  --use-rfc2307

# Create ControlCenter admin user
docker exec sambaad samba-tool user create \
  c3admin \
  'ControlCenterAdminPass123!' \
  --mail=c3admin@psyncopate.com \
  --use-rfc2307

# Verify users exist
docker exec sambaad samba-tool user list | grep -E "connect-svc|c3admin"
```

### 7.2 Configure Connect JAAS for LDAP

The Connect pod uses LDAP for authentication. The setup script already handles this, but verify:

```bash
# Check if JAAS config is applied
oc get configmap -n confluent connect-jaas-configmap

# View the JAAS config
oc get configmap -n confluent connect-jaas-configmap -o yaml

# Should have sections for both Kerberos and LDAP
```

### 7.3 Configure ControlCenter LDAP (Advanced)

This requires editing the ControlCenter configuration. For now, skip this - it's documented in `docs/kerberos-architecture.md`.

---

## Troubleshooting

### Troubleshooting OpenShift

**Issue: `oc whoami` returns error**
```bash
# Solution: Log in to cluster
oc login -u kubeadmin -p <password> https://api.crc.testing:6443
# Get password from: crc console --credentials
```

**Issue: CRC is not running**
```bash
# Solution: Start CRC
crc start

# If CRC keeps stopping, check system resources
crc status
crc config get memory  # Check if at least 16GB
crc config get cpus    # Check if at least 4 CPUs
```

**Issue: No storage class available**
```bash
# Solution: Enable hostpath provisioner on CRC
crc config set enable-cluster-monitoring true
crc config set enable-crc-storage-driver true
crc start  # Restart for changes to take effect

# Then verify
oc get storageclass
```

### Troubleshooting Bootstrap

**Issue: cert-manager installation fails**
```bash
# Solution: Check Helm repository is accessible
helm repo add jetstack https://charts.jetstack.io --force-update
helm repo update jetstack

# Try bootstrap again
./scripts/bootstrap.sh
```

**Issue: Sealed-secrets controller fails to start**
```bash
# Solution: Check pod logs
oc logs -n kube-system -l app.kubernetes.io/name=sealed-secrets --tail=50

# Common cause: No RBAC permissions - ensure bootstrap step 10 ran
oc apply -f bootstrap/platform-rbac.yaml

# Restart sealed-secrets pod
oc delete pod -n kube-system -l app.kubernetes.io/name=sealed-secrets
```

**Issue: ArgoCD UI not accessible**
```bash
# Solution: Get the route and reset password
oc get route -n argocd argocd-server

# Reset admin password
oc delete secret -n argocd argocd-initial-admin-secret
# Password will be auto-generated on next restart
oc delete pod -n argocd -l app.kubernetes.io/name=argocd-server

# Retrieve new password
oc get secret argocd-initial-admin-secret -n argocd -o jsonpath='{.data.password}' | base64 -d
```

### Troubleshooting Sealed Secrets

**Issue: `kubeseal` command not found**
```bash
# Solution: Install kubeseal
brew install kubeseal
# Or download from: https://github.com/bitnami-labs/sealed-secrets/releases
```

**Issue: SealedSecret won't decrypt**
```bash
# Cause: Sealed with different cluster's public key
# Solution: Regenerate with current cluster's key

# Verify your kubeseal cert is for the right cluster
kubeseal --fetch-cert --controller-name=sealed-secrets --controller-namespace=kube-system

# Re-seal the secret
kubeseal -f plaintext-secret.yaml --cert /tmp/sealed-secrets-public-cert.pem -o yaml > sealed-secret.yaml
```

### Troubleshooting CFK Platform

**Issue: Kafka pods stuck in pending**
```bash
# Check logs
oc describe pod kafka-0 -n confluent

# Common causes:
# 1. PVC waiting for storage - check storageclass exists
oc get pvc -n confluent
oc get storageclass

# 2. SCC violation - check pod events
oc get events -n confluent | grep kafka

# Solution: Ensure crc-csi-hostpath-provisioner exists
oc get storageclass crc-csi-hostpath-provisioner
```

**Issue: Connect pod not starting**
```bash
# Check logs for Kerberos errors
oc logs -n confluent connect-0 --tail=100 | grep -i "error\|krb5"

# Common causes:
# 1. Keytab not sealed - re-run sealing step
oc describe sealedsecret -n confluent connect-keytab-secret

# 2. krb5.conf ConfigMap missing - verify it exists
oc get cm -n confluent connect-krb5-configmap

# 3. SSH tunnel to KDC not open - restart tunnel
ps aux | grep "ssh -R"
# If not running, restart it (see Phase 5.3)
```

**Issue: ControlCenter not accessible**
```bash
# Check if pod is running
oc get pods -n confluent -l app.kubernetes.io/name=control-center

# Check logs
oc logs -n confluent control-center-0 --tail=100 | grep -i "error"

# Check route exists
oc get route -n confluent control-center

# Try accessing via port-forward instead of route
oc port-forward -n confluent svc/control-center 9021:9021 &
# Then access http://localhost:9021
```

### Troubleshooting SQL Server Connection

**Issue: Connector task fails with `Connection refused`**
```bash
# Check if tunnel is still open
netstat -an | grep 14330 | grep LISTEN

# If not, restart tunnel
CRC_IP=$(crc info | grep "DNS" | awk '{print $NF}')
ssh -R 14330:127.0.0.1:1433 core@$CRC_IP -N &

# Test from CRC node
crc ssh -- nc -zv localhost 14330
```

**Issue: Connector fails with Kerberos error `Cannot get a KDC reply`**
```bash
# Cause: KDC tunnel not open or iptables rule dropped
# Solution: Re-run Kerberos setup

./scripts/kerberos/setup-kerberos.sh

# Or manually restart tunnel for KDC (port 88)
CRC_IP=$(crc info | grep "DNS" | awk '{print $NF}')
ssh -R 14330:127.0.0.1:1433 \
    -R 18088:127.0.0.1:88 \
    core@$CRC_IP -N &
```

**Issue: Connector JDBC connection fails with `Login failed`**
```bash
# Check Connect logs
oc logs -n confluent connect-0 --tail=200 | grep -i "login\|kerberos\|jdbc"

# Verify SQL Server user exists (manual step from Phase 4)
sqlcmd -S localhost -U sa -P 'YourSaPassword123!' <<'EOF'
SELECT * FROM master.syslogins WHERE loginname = 'PSYNCOPATE\connect-svc';
EOF

# If missing, create it:
sqlcmd -S localhost -U sa -P 'YourSaPassword123!' <<'EOF'
CREATE LOGIN [PSYNCOPATE\connect-svc] FROM WINDOWS;
GO
USE claims_db;
CREATE USER [PSYNCOPATE\connect-svc] FOR LOGIN [PSYNCOPATE\connect-svc];
ALTER ROLE db_datareader ADD MEMBER [PSYNCOPATE\connect-svc];
GO
EOF
```

### Troubleshooting Kerberos Setup

**Issue: `./scripts/kerberos/setup-kerberos.sh` fails**
```bash
# Common causes and solutions:

# 1. SQL Server container not found
docker ps | grep sqltest2
# If not running: docker start sqltest2

# 2. SQLSERVER_HOST not set
echo $SQLSERVER_HOST  # Should not be empty
# If empty, set it: export SQLSERVER_HOST=<CRC_IP>

# 3. Docker network issues
docker network ls | grep kerberos-net
# If missing, remove and recreate
docker network rm kerberos-net || true

# 4. Samba container crashed
docker ps -a | grep sambaad
# If exited, check logs:
docker logs sambaad --tail=50
# Restart:
docker restart sambaad

# 5. Run with detailed debug output
bash -x ./scripts/kerberos/setup-kerberos.sh 2>&1 | tee /tmp/kerberos-setup.log
# Review the log for specific failures
```

**Issue: `validate-kerberos.sh` reports failures**
```bash
# Run validation to see what's failing
./scripts/kerberos/validate-kerberos.sh

# Common failures:
# - "Samba container not responding": docker restart sambaad
# - "KDC tunnel not working": SSH tunnel died, restart it
# - "SQL Server domain trust failed": Re-run setup-kerberos.sh
# - "Connector can't reach KDC": Check iptables rule exists

# Check iptables rule (from inside CRC node)
crc ssh
sudo iptables -L -n -v | grep 18088
exit
```

---

## Testing Checklist

Run these tests to verify everything is working:

### ✅ Infrastructure Tests

- [ ] OpenShift cluster is running: `oc cluster-info`
- [ ] All nodes are ready: `oc get nodes` shows Ready
- [ ] cert-manager is running: `oc get pods -n cert-manager | grep cert-manager`
- [ ] sealed-secrets is running: `oc get pods -n kube-system | grep sealed-secrets`
- [ ] ArgoCD is running: `oc get pods -n argocd | grep argocd-server`

### ✅ CFK Platform Tests

- [ ] CFK operator is running: `oc get pods -n confluent-operator`
- [ ] KRaft controller is running: `oc get pods -n confluent | grep kraft-controller`
- [ ] Kafka brokers are running: `oc get pods -n confluent | grep kafka` shows 1 pod
- [ ] Schema Registry is running: `oc get pods -n confluent | grep schema-registry`
- [ ] Connect is running: `oc get pods -n confluent | grep connect`
- [ ] ControlCenter is running: `oc get pods -n confluent | grep control-center`

### ✅ Database Tests

- [ ] SQL Server container is running: `docker ps | grep sqltest2`
- [ ] claims_db exists: `sqlcmd -S localhost -U sa -P '<password>' -Q "SELECT name FROM sys.databases WHERE name='claims_db'"`
- [ ] claims table exists: `sqlcmd -S localhost -U sa -P '<password>' -Q "USE claims_db; SELECT COUNT(*) FROM claims;"`

### ✅ Kerberos Tests

- [ ] Samba AD DC is running: `docker ps | grep sambaad`
- [ ] Connect keytab is sealed: `oc get sealedsecret -n confluent connect-keytab-secret`
- [ ] krb5.conf ConfigMap exists: `oc get cm -n confluent connect-krb5-configmap`
- [ ] JAAS config ConfigMap exists: `oc get cm -n confluent connect-jaas-configmap`
- [ ] Validation script passes: `./scripts/kerberos/validate-kerberos.sh` shows all OK

### ✅ Connector Tests

- [ ] sqlserver-claims-topic exists: `oc exec kafka-0 -c kafka -n confluent -- kafka-topics.sh --list --bootstrap-server kafka:9092 | grep sqlserver-claims`
- [ ] Connector is registered: `oc exec connect-0 -c connect -n confluent -- curl -s localhost:8083/connectors | jq .` shows connector
- [ ] Connector task is running: `oc exec connect-0 -c connect -n confluent -- curl -s localhost:8083/connectors/sqlserver-claims-connector/status | jq .status.state` shows RUNNING
- [ ] Topic has messages: `oc exec kafka-0 -c kafka -n confluent -- kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic sqlserver-claims-topic --from-beginning --max-messages 1`

### ✅ Data Flow Test

- [ ] Insert new data into SQL Server: `sqlcmd -S localhost -U sa -P '<password>' -Q "USE claims_db; INSERT INTO claims ..."`
- [ ] Verify record appears in Kafka topic within 10 seconds
- [ ] View record in ControlCenter UI at `https://controlcenter.apps-crc.testing`

---

## Next Steps

Once the complete setup is verified:

1. **Produce test messages** to Kafka topics for Flink processing (when ready)
2. **Explore ControlCenter** UI for monitoring and alerting
3. **Scale up** for production: Update `overlays/prod/kustomization.yaml` for 3-node Kafka cluster
4. **Enable Flink** by uncommenting flink-jobs Application in `apps/app-of-apps.yaml`
5. **Integrate CI/CD**: Set up GitHub Actions workflows for automated validation and deployment

---

## Cleanup (Teardown)

If you need to clean up:

```bash
# Teardown just the platform (keeps cluster running)
./scripts/teardown.sh

# Stop OpenShift cluster
crc stop

# Stop SQL Server and Samba AD DC containers
docker stop sqltest2 sambaad
docker rm sqltest2 sambaad

# Remove Docker volumes
docker volume rm sambaad-data

# Remove Docker network
docker network rm kerberos-net

# Full cluster reset (if needed)
crc delete
```
