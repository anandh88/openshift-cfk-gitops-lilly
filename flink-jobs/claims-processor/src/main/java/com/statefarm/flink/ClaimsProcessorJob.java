package com.statefarm.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

/**
 * Minimal passthrough job: raw-claims -> dedupe by exact content (MapState
 * with TTL, RocksDB-compaction-filter cleanup) -> key by a bucket derived
 * from the record -> keep a per-bucket running count (ValueState) -> stamp
 * a processed-at timestamp -> processed-claims. Two different operators,
 * two different RocksDB column families ("seen-claim-ids",
 * "claims-seen-per-bucket") - a purely stateless map() gives RocksDB
 * nothing to track at all, confirmed live. Deliberately does no real
 * claims business logic - its job is to exist and emit real Flink metrics
 * (checkpoints, throughput, backpressure, watermarks, RocksDB state) into
 * base/observability/grafana/dashboards/24-flink-mvp.json, which until now
 * had never seen a running job.
 */
public class ClaimsProcessorJob {

    // Internal listener - TLS + SASL/PLAIN, see base/confluent-platform/kafka.yaml.
    // Only resolvable cross-namespace via the Service FQDN, not the short name.
    private static final String BOOTSTRAP_SERVERS = "kafka.confluent.svc.cluster.local:9071";

    // Mounted by base/flink-jobs/flink-application.yaml's podTemplate.
    private static final String SASL_CREDENTIALS_PATH = "/mnt/secrets/flink-kafka-sasl/plain.txt";
    private static final String TRUSTSTORE_PATH = "/mnt/kafka-ca/ca.crt";

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String inputTopic = params.get("input-topic", "raw-claims");
        String outputTopic = params.get("output-topic", "processed-claims");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties kafkaProps = kafkaSecurityProperties();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(inputTopic)
                .setGroupId("claims-processor")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(kafkaProps)
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setKafkaProducerConfig(kafkaProps)
                // AT_LEAST_ONCE, not EXACTLY_ONCE: a transactional producer needs its
                // own transactional.id pool management, which is unnecessary complexity
                // for this MVP job. execution.checkpointing.mode=EXACTLY_ONCE (set in
                // flink-application.yaml) still governs Flink's own state consistency
                // independent of this sink's delivery guarantee.
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        DataStreamSource<String> claims = env.fromSource(source, WatermarkStrategy.noWatermarks(), "raw-claims-source");
        DataStream<String> deduped = claims
                .keyBy(record -> record)
                .process(new DedupeByContent())
                .name("dedupe-by-content");
        DataStream<String> processed = deduped
                .keyBy(record -> Math.floorMod(record.hashCode(), 8))
                .process(new RunningCountPerBucket())
                .name("running-count-per-bucket");
        processed.sinkTo(sink);

        env.execute("claims-processor");
    }

    private static Properties kafkaSecurityProperties() throws IOException {
        Properties props = new Properties();
        props.setProperty("security.protocol", "SASL_SSL");
        props.setProperty("sasl.mechanism", "PLAIN");
        props.setProperty("sasl.jaas.config", buildJaasConfig());
        // PEM truststore type (KIP-651) - a raw CA cert file works directly,
        // no keytool/JKS conversion needed. Lighter than the JKS approach
        // base/confluent-platform/connect-truststore-configmap.yaml uses.
        props.setProperty("ssl.truststore.type", "PEM");
        props.setProperty("ssl.truststore.location", TRUSTSTORE_PATH);
        return props;
    }

    private static String buildJaasConfig() throws IOException {
        List<String> lines = Files.readAllLines(Path.of(SASL_CREDENTIALS_PATH));
        String username = null;
        String password = null;
        for (String line : lines) {
            if (line.startsWith("username=")) {
                username = line.substring("username=".length()).trim();
            } else if (line.startsWith("password=")) {
                password = line.substring("password=".length()).trim();
            }
        }
        if (username == null || password == null) {
            throw new IOException("Expected username= and password= lines in " + SASL_CREDENTIALS_PATH);
        }
        return "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                + username + "\" password=\"" + password + "\";";
    }

    /**
     * Drops exact-duplicate records (keyed by their own content) already
     * seen within the last 5 minutes. MapState<String, Long> - key doubles
     * as the value's own dedupe entry, value is the timestamp last seen
     * (unused beyond existence-check, but real - lets a future version
     * report "how stale" a dedupe hit was). TTL cleanup runs via RocksDB's
     * own compaction filter (cleanupInRocksdbCompactFilter), not a separate
     * Flink-side timer - the RocksDB-specific way to do this, exercising a
     * different code path than the plain ValueState in RunningCountPerBucket.
     */
    private static class DedupeByContent extends KeyedProcessFunction<String, String, String> {
        private transient MapState<String, Long> seenState;

        @Override
        public void open(Configuration parameters) {
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Duration.ofMinutes(5))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .cleanupInRocksdbCompactFilter(1000)
                    .build();
            MapStateDescriptor<String, Long> descriptor =
                    new MapStateDescriptor<>("seen-claim-ids", String.class, Long.class);
            descriptor.enableTimeToLive(ttlConfig);
            seenState = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(String record, Context ctx, Collector<String> out) throws Exception {
            if (seenState.contains(record)) {
                return;
            }
            seenState.put(record, ctx.timestamp() == null ? System.currentTimeMillis() : ctx.timestamp());
            out.collect(record);
        }
    }

    /**
     * Real keyed state (one Long counter per bucket key) - deliberately
     * simple, just enough that RocksDB registers a column family and has
     * real, if tiny, data to report on.
     */
    private static class RunningCountPerBucket extends KeyedProcessFunction<Integer, String, String> {
        private transient ValueState<Long> countState;

        @Override
        public void open(Configuration parameters) {
            countState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("claims-seen-per-bucket", Long.class));
        }

        @Override
        public void processElement(String record, Context ctx, Collector<String> out) throws Exception {
            long count = countState.value() == null ? 0L : countState.value();
            count++;
            countState.update(count);
            out.collect("{\"processedAt\":\"" + Instant.now() + "\",\"bucket\":" + ctx.getCurrentKey()
                    + ",\"seqForBucket\":" + count + ",\"claim\":" + record + "}");
        }
    }
}
