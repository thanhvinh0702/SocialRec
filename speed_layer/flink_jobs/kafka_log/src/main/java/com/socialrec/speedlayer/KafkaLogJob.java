package com.socialrec.speedlayer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaLogJob {
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "kafka-cluster-kafka-bootstrap:9092";
    private static final String DEFAULT_TOPIC = "postgres.public.interactions";
    private static final String DEFAULT_GROUP_ID = "socialrec-flink-kafka-log";

    public static void main(String[] args) throws Exception {
        String bootstrapServers = envOrDefault("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS);
        String topic = envOrDefault("KAFKA_TOPIC", DEFAULT_TOPIC);
        String groupId = envOrDefault("KAFKA_GROUP_ID", DEFAULT_GROUP_ID);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "postgres-interactions-debezium-source")
                .name("Read Debezium interactions from Kafka")
                .uid("postgres-interactions-debezium-source")
                .addSink(new DebeziumEventLogSink())
                .name("Log raw Debezium events")
                .uid("log-raw-debezium-events");

        env.execute("socialrec-kafka-log");
    }

    private static String envOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value;
    }

    public static class DebeziumEventLogSink extends RichSinkFunction<String> {
        private static final long serialVersionUID = 1L;
        private static final Logger LOG = LoggerFactory.getLogger(DebeziumEventLogSink.class);

        @Override
        public void invoke(String value, Context context) {
            LOG.info("Debezium interaction event: {}", value);
        }
    }
}
