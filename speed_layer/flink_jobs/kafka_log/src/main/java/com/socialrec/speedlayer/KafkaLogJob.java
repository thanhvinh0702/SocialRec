package com.socialrec.speedlayer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPooled;

public class KafkaLogJob {
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "kafka-cluster-kafka-bootstrap:9092";
    private static final String DEFAULT_TOPIC = "postgres.public.interactions";
    private static final String DEFAULT_GROUP_ID = "socialrec-flink-kafka-log";
    private static final String DEFAULT_REDIS_HOST = "redis.socialrec.svc.cluster.local";
    private static final int DEFAULT_REDIS_PORT = 6379;
    private static final int DEFAULT_RECENT_VIEWS_LIMIT = 100;

    public static void main(String[] args) throws Exception {
        String bootstrapServers = envOrDefault("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS);
        String topic = envOrDefault("KAFKA_TOPIC", DEFAULT_TOPIC);
        String groupId = envOrDefault("KAFKA_GROUP_ID", DEFAULT_GROUP_ID);
        String redisHost = envOrDefault("REDIS_HOST", DEFAULT_REDIS_HOST);
        int redisPort = envIntOrDefault("REDIS_PORT", DEFAULT_REDIS_PORT);
        int recentViewsLimit = envIntOrDefault("REDIS_RECENT_VIEWS_LIMIT", DEFAULT_RECENT_VIEWS_LIMIT);

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
                .addSink(new DebeziumInteractionRedisSink(redisHost, redisPort, recentViewsLimit))
                .name("Log Debezium events and write Redis interaction views")
                .uid("log-debezium-events-write-redis");

        env.execute("socialrec-kafka-log");
    }

    private static String envOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value;
    }

    private static int envIntOrDefault(String key, int defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Environment variable " + key + " must be an integer: " + value, e);
        }
    }

    public static class DebeziumInteractionRedisSink extends RichSinkFunction<String> {
        private static final long serialVersionUID = 1L;
        private static final Logger LOG = LoggerFactory.getLogger(DebeziumInteractionRedisSink.class);

        private final String redisHost;
        private final int redisPort;
        private final int recentViewsLimit;

        private transient JedisPooled redis;
        private transient ObjectMapper objectMapper;

        public DebeziumInteractionRedisSink(String redisHost, int redisPort, int recentViewsLimit) {
            this.redisHost = redisHost;
            this.redisPort = redisPort;
            this.recentViewsLimit = recentViewsLimit;
        }

        @Override
        public void open(Configuration parameters) {
            redis = new JedisPooled(redisHost, redisPort);
            objectMapper = new ObjectMapper();
            LOG.info("Connected Redis sink to {}:{}", redisHost, redisPort);
        }

        @Override
        public void invoke(String value, Context context) {
            LOG.info("Debezium interaction event: {}", value);
            try {
                InteractionEvent event = parseCreateInteraction(value);
                if (event == null) {
                    return;
                }

                redis.zincrby("trending:global", 1.0, event.postId);
                String recentViewsKey = "user:" + event.userId + ":recent_views";
                redis.lpush(recentViewsKey, event.postId);
                redis.ltrim(recentViewsKey, 0, recentViewsLimit - 1L);
                LOG.info("Updated Redis trending:global and {} for post {}", recentViewsKey, event.postId);
            } catch (Exception e) {
                LOG.warn("Failed to process Debezium interaction event for Redis: {}", value, e);
            }
        }

        @Override
        public void close() {
            if (redis != null) {
                redis.close();
            }
        }

        private InteractionEvent parseCreateInteraction(String value) throws Exception {
            JsonNode root = objectMapper.readTree(value);
            JsonNode envelope = root.hasNonNull("payload") ? root.get("payload") : root;
            JsonNode opNode = envelope.get("op");
            if (opNode == null || !"c".equals(opNode.asText())) {
                return null;
            }

            JsonNode after = envelope.get("after");
            if (after == null || after.isNull()) {
                LOG.warn("Skipping create event without after payload: {}", value);
                return null;
            }

            JsonNode userId = after.get("user_id");
            JsonNode postId = after.get("post_id");
            if (userId == null || userId.isNull() || postId == null || postId.isNull()) {
                LOG.warn("Skipping create event without user_id or post_id: {}", value);
                return null;
            }

            return new InteractionEvent(userId.asText(), postId.asText());
        }
    }

    private static class InteractionEvent {
        private final String userId;
        private final String postId;

        private InteractionEvent(String userId, String postId) {
            this.userId = userId;
            this.postId = postId;
        }
    }
}
