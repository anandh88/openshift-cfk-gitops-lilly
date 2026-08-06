#!/usr/bin/env bash
# Registers the JDBC Source Connector against the running Connect cluster
# via its REST API. Uses the external route by default (connect.yaml
# exposes one at externalAccess.type: route) - override CONNECT_URL to use
# the in-cluster Service DNS instead if running this from inside the
# cluster network.
#
# authenticationScheme=JavaKerberos + integratedSecurity=true is
# mssql-jdbc's Kerberos mode; jaasConfigurationName defaults to
# "SQLJDBCDriver" (see base/confluent-platform/connect-jaas-configmap.yaml),
# so it's left unset here rather than repeated.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-https://connect.apps-crc.testing}"
CONNECTOR_NAME="sqlserver-claims-source"

echo "==> Registering ${CONNECTOR_NAME} against ${CONNECT_URL}"
curl -sk -X PUT "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config" \
  -H "Content-Type: application/json" \
  -d '{
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:sqlserver://sqlserver.sqlserver.svc.cluster.local:1433;databaseName=ClaimsDB;integratedSecurity=true;authenticationScheme=JavaKerberos;encrypt=false;",
    "table.whitelist": "Claims",
    "mode": "timestamp+incrementing",
    "incrementing.column.name": "ClaimId",
    "timestamp.column.name": "UpdatedAt",
    "topic.prefix": "sqlserver-",
    "poll.interval.ms": "10000",
    "tasks.max": "1",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "https://schemaregistry.confluent.svc.cluster.local:8081"
  }'

echo
echo "==> Connector status:"
curl -sk "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" | python3 -m json.tool || true

echo "==> Expect topic sqlserver-Claims to start receiving records within poll.interval.ms."
echo "    Check with: oc exec -n confluent deploy/connect -- kafka-avro-console-consumer ..."
echo "    or via Control Center's topic browser."
