# Quick Reference: Local CFK Setup Commands

Fast reference guide for common commands during setup.

---

## 🚀 Quick Start (TL;DR - 30 minutes)

```bash
# 1. Clone repo and navigate
cd /Users/anandhvasu/Documents/confluent-ps/openshift-cfk-gitops-lite/openshift-cfk-gitops

# 2. Verify OpenShift is running
oc whoami
crc status  # Should show: "CRC instance is running"

# 3. Run bootstrap (takes ~5 min)
./scripts/bootstrap.sh

# 4. Seal secrets
kubeseal -f /tmp/secret.yaml --cert /tmp/sealed-secrets-public-cert.pem -o yaml > sealed-secret.yaml

# 5. Deploy via ArgoCD
oc apply -f apps/app-of-apps.yaml

# 6. Start SQL Server in Docker
docker run -d --name sqltest2 -e SA_PASSWORD=YourPassword123! -p 1433:1433 mcr.microsoft.com/mssql/server:2019-latest

# 7. Set up Kerberos
CRC_IP=$(crc info | grep "DNS" | awk '{print $NF}')
ssh -R 14330:127.0.0.1:1433 core@$CRC_IP -N &
SQLSERVER_HOST=$CRC_IP SQLSERVER_PORT=14330 ./scripts/kerberos/setup-kerberos.sh

# 8. Verify data flow
oc exec kafka-0 -c kafka -n confluent -- kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic sqlserver-claims-topic --max-messages 5
```

---

## Phase 0: OpenShift Status Checks

### Check cluster is running
```bash
oc whoami                           # Verify login
crc status                          # Should show: "running"
oc cluster-info                     # Cluster endpoints
oc get nodes                        # All nodes ready?
oc get storageclass                 # crc-csi-hostpath-provisioner exists?
```

### If not running
```bash
crc start                           # Start CRC (takes ~3 minutes)
crc console --credentials           # Get credentials if needed
oc login -u kubeadmin -p <PASSWORD> https://api.crc.testing:6443
```

---

## Phase 1: Bootstrap CFK Platform

### Run bootstrap script
```bash
cd /Users/anandhvasu/Documents/confluent-ps/openshift-cfk-gitops-lite/openshift-cfk-gitops
chmod +x scripts/bootstrap.sh
./scripts/bootstrap.sh               # Takes ~5-10 minutes
```

### Verify bootstrap
```bash
# Check cert-manager
oc rollout status deployment/cert-manager -n cert-manager --timeout=120s
oc rollout status deployment/cert-manager-webhook -n cert-manager --timeout=120s
oc get certificate -A                # All should be READY=True

# Check sealed-secrets
oc rollout status deployment/sealed-secrets -n kube-system --timeout=120s
ls -la /tmp/sealed-secrets-public-cert.pem

# Check ArgoCD
oc get pods -n argocd | grep argocd-server
oc get pods -n argocd | grep argocd-repo-server
```

---

## Phase 2: Seal Secrets

### Create plaintext secrets (local only)
```bash
mkdir -p /tmp/secrets

# Kafka SASL
cat > /tmp/secrets/kafka-sasl-secret.yaml <<'EOF'
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

# Control Center
cat > /tmp/secrets/c3-credentials-secret.yaml <<'EOF'
apiVersion: v1
kind: Secret
metadata:
  name: c3-credentials-secret
  namespace: confluent
type: Opaque
stringData:
  c3_username: admin
  c3_password: C3Pass789!
EOF
```

### Seal all secrets
```bash
cd /Users/anandhvasu/Documents/confluent-ps/openshift-cfk-gitops-lite/openshift-cfk-gitops

kubeseal -f /tmp/secrets/kafka-sasl-secret.yaml \
  --cert /tmp/sealed-secrets-public-cert.pem \
  -o yaml > base/confluent-platform/secrets/kafka-sasl-sealed.yaml

kubeseal -f /tmp/secrets/kafka-external-sasl-secret.yaml \
  --cert /tmp/sealed-secrets-public-cert.pem \
  -o yaml > base/confluent-platform/secrets/kafka-external-sasl-sealed.yaml

kubeseal -f /tmp/secrets/c3-credentials-secret.yaml \
  --cert /tmp/sealed-secrets-public-cert.pem \
  -o yaml > base/confluent-platform/secrets/c3-credentials-sealed.yaml

# Verify no plaintext passwords
grep -r "KafkaPass123" base/confluent-platform/secrets/  # Should return nothing

# Clean up plaintext
rm -rf /tmp/secrets
```

---

## Phase 3: Deploy via ArgoCD

### Apply root app
```bash
oc apply -f apps/app-of-apps.yaml
oc get application -n argocd platform-root -w  # Wait for Healthy
```

### Get ArgoCD credentials
```bash
ARGOCD_PASSWORD=$(oc get secret argocd-initial-admin-secret -n argocd -o jsonpath='{.data.password}' | base64 -d)
ARGOCD_SERVER=$(oc get route -n argocd argocd-server -o jsonpath='{.spec.host}')
echo "Server: https://$ARGOCD_SERVER"
echo "User: admin"
echo "Password: $ARGOCD_PASSWORD"
```

### Sync operators
```bash
oc apply -f apps/confluent-operator-app.yaml
oc get application -n argocd confluent-operator -w  # Wait for Healthy

# Verify operator
oc get pods -n confluent-operator
oc get csv -n confluent-operator
```

### Sync platform
```bash
oc apply -f apps/confluent-platform-app.yaml
oc get application -n argocd confluent-platform -w  # Wait for Healthy

# Watch pods come up
oc get pods -n confluent -w

# Expected: kraft-controller-0, kafka-0, schema-registry-0, connect-0, control-center-0, restproxy-0
```

### Verify platform
```bash
# All pods running?
oc get pods -n confluent -o wide

# Kafka ready?
oc exec kafka-0 -c kafka -n confluent -- bash -c \
  'kafka-broker-api-versions.sh --bootstrap-server kafka:9092' 2>/dev/null || echo "Not ready"

# Schema Registry connected?
oc logs schema-registry-0 -n confluent | tail -20 | grep -i "ready\|started"

# Connect ready?
oc logs connect-0 -n confluent | tail -20 | grep -i "ready\|started"
```

---

## Phase 4: SQL Server Setup

### Start container
```bash
docker run -d \
  --name sqltest2 \
  --restart unless-stopped \
  -e "ACCEPT_EULA=Y" \
  -e "SA_PASSWORD=YourPassword123!" \
  -p 1433:1433 \
  mcr.microsoft.com/mssql/server:2019-latest

# Verify running
docker ps | grep sqltest2

# Test connection
sqlcmd -S localhost -U sa -P 'YourPassword123!' -Q "SELECT @@VERSION"
```

### Create database
```bash
sqlcmd -S localhost -U sa -P 'YourPassword123!' <<'EOF'
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

INSERT INTO claims (customer_id, claim_amount, claim_status, description) VALUES
  (1001, 500.00, 'PENDING', 'Vehicle damage'),
  (1002, 1250.50, 'APPROVED', 'Medical claim');
GO

SELECT * FROM claims;
GO
EOF
```

### Open SSH tunnel
```bash
CRC_IP=$(crc info | grep "DNS" | awk '{print $NF}')
echo "CRC IP: $CRC_IP"

# Open tunnel for SQL Server (port 1433 → 14330)
ssh -R 14330:127.0.0.1:1433 core@$CRC_IP -N &
TUNNEL_PID=$!
echo "Tunnel PID: $TUNNEL_PID"

# Verify tunnel
sleep 2
netstat -an | grep 14330 | grep LISTEN

# Test from CRC
crc ssh -- nc -zv localhost 14330
```

---

## Phase 5: Kerberos & LDAP Setup

### Run setup script
```bash
cd /Users/anandhvasu/Documents/confluent-ps/openshift-cfk-gitops-lite/openshift-cfk-gitops

# Ensure tunnel is open
CRC_IP=$(crc info | grep "DNS" | awk '{print $NF}')
ps aux | grep "ssh -R 14330" || ssh -R 14330:127.0.0.1:1433 core@$CRC_IP -N &
sleep 2

# Run setup (takes ~10 min)
SQLSERVER_HOST=$CRC_IP SQLSERVER_PORT=14330 ./scripts/kerberos/setup-kerberos.sh
```

### Verify setup
```bash
./scripts/kerberos/validate-kerberos.sh

# Should show all OK:
# ✓ Samba domain accessible
# ✓ Connect service account exists
# ✓ Keytab is valid
# ✓ SQL Server domain joined
# ✓ Connect tunnel working
# ✓ Connector registered
```

### Manual verification
```bash
# Samba AD running?
docker ps | grep sambaad

# Connect keytab sealed?
oc get sealedsecret -n confluent connect-keytab-secret

# Topic created?
oc exec kafka-0 -c kafka -n confluent -- kafka-topics.sh --list --bootstrap-server kafka:9092 | grep sqlserver-claims

# Connector registered?
oc exec connect-0 -c connect -n confluent -- curl -s localhost:8083/connectors | jq .

# Connector running?
oc exec connect-0 -c connect -n confluent -- curl -s localhost:8083/connectors/sqlserver-claims-connector/status | jq .status.state
# Should output: "RUNNING"

# Create SQL Server login (manual step)
sqlcmd -S localhost -U sa -P 'YourPassword123!' <<'EOF'
CREATE LOGIN [PSYNCOPATE\connect-svc] FROM WINDOWS;
GO
USE claims_db;
CREATE USER [PSYNCOPATE\connect-svc] FOR LOGIN [PSYNCOPATE\connect-svc];
ALTER ROLE db_datareader ADD MEMBER [PSYNCOPATE\connect-svc];
GO
EOF
```

---

## Phase 6: Test Data Flow

### Check connector status
```bash
oc exec -n confluent connect-0 -c connect -- curl -s localhost:8083/connectors/sqlserver-claims-connector/status | jq .
```

### Consume messages from topic
```bash
oc exec -n confluent kafka-0 -c kafka -- kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic sqlserver-claims-topic \
  --from-beginning \
  --consumer.config /opt/confluentinc/etc/kafka/client.properties \
  --max-messages 10
```

### Insert new data and verify
```bash
# Add new record
sqlcmd -S localhost -U sa -P 'YourPassword123!' <<'EOF'
USE claims_db;
INSERT INTO claims (customer_id, claim_amount, claim_status, description) VALUES
  (1004, 2000.00, 'PENDING', 'Test claim');
GO
EOF

# Wait 5-10 seconds, then consume
oc exec -n confluent kafka-0 -c kafka -- kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic sqlserver-claims-topic \
  --consumer.config /opt/confluentinc/etc/kafka/client.properties \
  --max-messages 1
```

### View in ControlCenter
```bash
# Get URL
oc get route -n confluent control-center -o jsonpath='{.spec.host}'
# Open https://controlcenter.apps-crc.testing in browser

# Get credentials
oc get sealedsecret -n confluent c3-credentials-secret -o yaml | grep c3_username
# Default from sealed secret: admin / ControlCenterPass789! (or your set value)

# View in UI:
# - Topics → sqlserver-claims-topic (message count, throughput)
# - Connectors → sqlserver-claims-connector (status, metrics)
# - Schema Registry (claims schema)
```

---

## Common Troubleshooting Commands

### Restart a pod
```bash
oc delete pod kafka-0 -n confluent  # Pod will auto-restart
oc get pod kafka-0 -n confluent -w
```

### View pod logs
```bash
# Recent logs
oc logs connect-0 -n confluent --tail=100

# Follow logs
oc logs connect-0 -n confluent -f

# Previous logs (if pod crashed)
oc logs connect-0 -n confluent --previous
```

### Port forward to service
```bash
# Access ControlCenter via port-forward (if route doesn't work)
oc port-forward -n confluent svc/control-center 9021:9021 &
# Then open http://localhost:9021

# Connect to Kafka broker
oc port-forward -n confluent svc/kafka 9092:9092 &

# Schema Registry
oc port-forward -n confluent svc/schema-registry 8081:8081 &
```

### Describe pod (for debugging)
```bash
oc describe pod connect-0 -n confluent  # Shows events, volumes, conditions
oc describe pod kafka-0 -n confluent

# Shows recent errors/warnings
oc get events -n confluent | head -20
```

### Check resource usage
```bash
oc top pods -n confluent        # CPU/memory per pod
oc top nodes                    # Node usage
oc describe node                # Node details
```

### Check PVC status
```bash
oc get pvc -n confluent                          # All PVCs
oc describe pvc kafka-data-kafka-0 -n confluent  # Specific PVC

# Check storage
oc get storageclass
oc describe storageclass crc-csi-hostpath-provisioner
```

### Check sealed secrets decryption
```bash
# Verify SealedSecret exists
oc get sealedsecret -n confluent

# Check if it decrypted to Secret
oc get secret kafka-sasl-secret -n confluent

# View decrypted secret (careful - shows plaintext!)
oc get secret kafka-sasl-secret -n confluent -o jsonpath='{.data}' | jq .
```

### SSH into CRC node
```bash
crc ssh                             # Interactive shell
crc ssh -- <command>               # Run single command

# Check node resources
crc ssh -- free -h
crc ssh -- df -h

# Check Docker access
crc ssh -- docker ps               # See pods from node view
```

### Check ArgoCD sync status
```bash
oc get application -n argocd                    # All apps
oc get application -n argocd platform-root      # Specific app
oc describe application confluent-platform -n argocd  # Details

# View actual vs desired state
oc get application confluent-platform -n argocd -o yaml | grep -A 20 "status:"
```

---

## Monitoring & Observability

### Kafka topic stats
```bash
# List all topics
oc exec kafka-0 -c kafka -n confluent -- kafka-topics.sh \
  --list --bootstrap-server kafka:9092

# Topic details
oc exec kafka-0 -c kafka -n confluent -- kafka-topics.sh \
  --describe --topic sqlserver-claims-topic --bootstrap-server kafka:9092

# Message count
oc exec kafka-0 -c kafka -n confluent -- kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --list
```

### Connect metrics
```bash
# Connector metrics via REST API
oc exec connect-0 -c connect -n confluent -- curl -s localhost:8083/connectors/sqlserver-claims-connector/metrics | jq .

# All tasks
oc exec connect-0 -c connect -n confluent -- curl -s localhost:8083/connectors/sqlserver-claims-connector/tasks | jq .
```

### Schema Registry schemas
```bash
# List all schemas
oc exec schema-registry-0 -c schema-registry -n confluent -- curl -s http://localhost:8081/subjects | jq .

# Get schema
oc exec schema-registry-0 -c schema-registry -n confluent -- curl -s http://localhost:8081/subjects/sqlserver-claims-value/versions/latest | jq .
```

---

## Database Management

### Query claims table
```bash
sqlcmd -S localhost -U sa -P 'YourPassword123!' <<'EOF'
USE claims_db;
SELECT * FROM claims;
GO
EOF
```

### Add more test data
```bash
sqlcmd -S localhost -U sa -P 'YourPassword123!' <<'EOF'
USE claims_db;
INSERT INTO claims (customer_id, claim_amount, claim_status, description) VALUES
  (1005, 3000.00, 'PENDING', 'Large claim'),
  (1006, 150.00, 'APPROVED', 'Small claim'),
  (1007, 5000.00, 'PENDING', 'Major claim');
GO
SELECT COUNT(*) AS claim_count FROM claims;
GO
EOF
```

### Backup database
```bash
docker exec sqltest2 /opt/mssql-tools/bin/sqlcmd \
  -S localhost -U sa -P 'YourPassword123!' \
  -Q "BACKUP DATABASE claims_db TO DISK = '/var/opt/mssql/backup/claims_db.bak'"
```

---

## Cleanup & Teardown

### Remove just the platform (keep cluster)
```bash
./scripts/teardown.sh
```

### Stop but don't delete
```bash
crc stop
docker stop sqltest2 sambaad
```

### Full cleanup
```bash
# Delete all
crc delete

# Remove Docker containers
docker rm sqltest2 sambaad

# Remove Docker volumes and networks
docker volume rm sambaad-data
docker network rm kerberos-net
```

---

## Useful Links

- **ArgoCD UI**: https://controlcenter.apps-crc.testing (get exact URL: `oc get route -n argocd argocd-server`)
- **ControlCenter UI**: https://controlcenter.apps-crc.testing (get exact URL: `oc get route -n confluent control-center`)
- **Kubernetes Dashboard**: `crc console`
- **CRC Docs**: https://crc.dev
- **Confluent Docs**: https://docs.confluent.io
- **ArgoCD Docs**: https://argo-cd.readthedocs.io
