#!/usr/bin/env bash
# Generates real Kafka traffic against a dedicated test topic so the
# dashboards in docs/grafana-dashboard-guide.md have something to show
# besides an idle cluster - and optionally creates deliberate consumer lag
# so you can watch Grafana react (Requirement 31).
#
# Runs entirely via `oc exec` into kafka-0 itself (it already has the CLI
# tools, and its own mounted TLS truststore/password, on disk) - nothing
# runs on your Mac, and nothing here touches sqlserver-Claims or the JDBC
# connector.
#
# Usage:
#   ./scripts/generate-observability-load.sh start          # producer only
#   ./scripts/generate-observability-load.sh start --lag    # producer + a
#                                                             # deliberately
#                                                             # slow consumer
#   ./scripts/generate-observability-load.sh stop
set -euo pipefail

TOPIC="observability-load-test"
GROUP="observability-lag-demo"
ACTION="${1:-}"
LAG_FLAG="${2:-}"

# Written once per action, remotely inside kafka-0, reading the real
# truststore password off disk instead of hardcoding it (confirmed live:
# it's NOT "changeit" - it's whatever mssql-conf/CFK generated into
# /mnt/sslcerts/jksPassword.txt, in "jksPassword=<value>" format).
WRITE_CLIENT_PROPS='
PASS=$(grep -oP "(?<=jksPassword=).*" /mnt/sslcerts/jksPassword.txt)
cat > /tmp/obs-load-client.properties <<PROPS
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="kafka-admin" password="KafkaAdmin@Local2024!";
ssl.truststore.location=/mnt/sslcerts/truststore.p12
ssl.truststore.password=${PASS}
PROPS
'

case "${ACTION}" in
  start)
    echo "==> Ensuring topic ${TOPIC} exists (auto-create is disabled on this cluster)"
    oc exec -n confluent kafka-0 -c kafka -- bash -c "
      ${WRITE_CLIENT_PROPS}
      kafka-topics --bootstrap-server kafka.confluent.svc.cluster.local:9071 \
        --command-config /tmp/obs-load-client.properties \
        --create --if-not-exists --topic ${TOPIC} --partitions 3 --replication-factor 1
    "

    echo "==> Starting producer in the background (~50 records/sec, 512 bytes each)"
    oc exec -n confluent kafka-0 -c kafka -- bash -c "
      ${WRITE_CLIENT_PROPS}
      nohup kafka-producer-perf-test --topic ${TOPIC} --num-records 10000000000 \
        --throughput 50 --record-size 512 \
        --producer.config /tmp/obs-load-client.properties \
        --producer-props bootstrap.servers=kafka.confluent.svc.cluster.local:9071 \
        > /tmp/obs-load-producer.log 2>&1 &
      disown
    "
    echo "    -> topic ${TOPIC} (3 partitions)"

    if [[ "${LAG_FLAG}" == "--lag" ]]; then
      echo "==> Starting a deliberately slow consumer in group ${GROUP} to build up lag"
      oc exec -n confluent kafka-0 -c kafka -- bash -c "
        ${WRITE_CLIENT_PROPS}
        nohup bash -c '
          while read -r line; do sleep 1; done < <(
            kafka-console-consumer --topic ${TOPIC} \
              --bootstrap-server kafka.confluent.svc.cluster.local:9071 \
              --consumer-property group.id=${GROUP} \
              --consumer.config /tmp/obs-load-client.properties
          )
        ' > /tmp/obs-load-consumer.log 2>&1 &
        disown
      "
      echo "    -> consumer group ${GROUP} started, throttled to ~1 msg/sec against a"
      echo "       ~50 msg/sec producer - lag on ${TOPIC} will climb steadily. Watch it:"
      echo "       oc exec -n confluent kafka-0 -c kafka -- bash -c \\"
      echo "         'kafka-consumer-groups --bootstrap-server kafka.confluent.svc.cluster.local:9071 \\"
      echo "          --command-config /tmp/obs-load-client.properties --describe --group ${GROUP}'"
    fi

    echo
    echo "Load running. Give Prometheus 1-2 scrape intervals (60s) then check the"
    echo "'04 - Kafka Topics' dashboard (and the kafka-consumer-groups command above"
    echo "if --lag was used) - see docs/grafana-dashboard-guide.md."
    echo "Stop everything with: $0 stop"
    ;;

  stop)
    echo "==> Stopping producer/consumer processes inside kafka-0"
    oc exec -n confluent kafka-0 -c kafka -- bash -c "
      pkill -f 'kafka-producer-perf-test.*${TOPIC}' 2>/dev/null || true
      pkill -f 'kafka-console-consumer.*${TOPIC}' 2>/dev/null || true
    "
    echo "    -> stopped (topic ${TOPIC} and any accumulated lag are left in place -"
    echo "       delete the topic yourself for a clean slate:"
    echo "       oc exec -n confluent kafka-0 -c kafka -- bash -c \\"
    echo "         'kafka-topics --bootstrap-server kafka.confluent.svc.cluster.local:9071 \\"
    echo "          --command-config /tmp/obs-load-client.properties --delete --topic ${TOPIC}'"
    ;;

  *)
    echo "Usage: $0 start [--lag] | stop"
    exit 1
    ;;
esac
