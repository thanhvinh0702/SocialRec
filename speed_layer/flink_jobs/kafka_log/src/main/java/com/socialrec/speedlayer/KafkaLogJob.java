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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class KafkaLogJob {
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "kafka-cluster-kafka-bootstrap:9092";
    private static final String DEFAULT_TOPIC = "postgres.public.interactions,postgres.public.posts";
    private static final String DEFAULT_GROUP_ID = "socialrec-flink-kafka-log";
    private static final String DEFAULT_REDIS_HOST = "redis.socialrec.svc.cluster.local";
    private static final int DEFAULT_REDIS_PORT = 6379;
    private static final int DEFAULT_RECENT_VIEWS_LIMIT = 100;

    public static void main(String[] args) throws Exception {
        String bootstrapServers = envOrDefault("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS);
        List<String> topics = parseTopics(envOrDefault("KAFKA_TOPIC", DEFAULT_TOPIC));
        String groupId = envOrDefault("KAFKA_GROUP_ID", DEFAULT_GROUP_ID);
        String redisHost = envOrDefault("REDIS_HOST", DEFAULT_REDIS_HOST);
        int redisPort = envIntOrDefault("REDIS_PORT", DEFAULT_REDIS_PORT);
        int recentViewsLimit = envIntOrDefault("REDIS_RECENT_VIEWS_LIMIT", DEFAULT_RECENT_VIEWS_LIMIT);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "postgres-debezium-source")
                .name("Read Debezium interactions and posts from Kafka")
                .uid("postgres-debezium-source")
                .addSink(new DebeziumRedisSink(redisHost, redisPort, recentViewsLimit))
                .name("Log Debezium events and write Redis speed-layer state")
                .uid("log-debezium-events-write-redis");

        env.execute("socialrec-kafka-log");
    }

    static List<String> parseTopics(String topicConfig) {
        return Arrays.stream(topicConfig.split(","))
                .map(String::trim)
                .filter(topic -> !topic.isEmpty())
                .toList();
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

    public static class DebeziumRedisSink extends RichSinkFunction<String> {
        private static final long serialVersionUID = 1L;
        private static final Logger LOG = LoggerFactory.getLogger(DebeziumRedisSink.class);

        private final String redisHost;
        private final int redisPort;
        private final int recentViewsLimit;

        private transient RedisOperations redis;
        private transient ObjectMapper objectMapper;

        public DebeziumRedisSink(String redisHost, int redisPort, int recentViewsLimit) {
            this.redisHost = redisHost;
            this.redisPort = redisPort;
            this.recentViewsLimit = recentViewsLimit;
        }

        @Override
        public void open(Configuration parameters) {
            redis = new JedisRedisOperations(new JedisPooled(redisHost, redisPort));
            objectMapper = new ObjectMapper();
            LOG.info("Connected Redis sink to {}:{}", redisHost, redisPort);
        }

        @Override
        public void invoke(String value, Context context) {
            LOG.info("Debezium event: {}", value);
            try {
                ProcessedEvent event = processDebeziumEvent(value, redis, objectMapper, recentViewsLimit);
                logProcessedEvent(event);
            } catch (Exception e) {
                LOG.warn("Failed to process Debezium event for Redis: {}", value, e);
            }
        }

        @Override
        public void close() {
            if (redis != null) {
                redis.close();
            }
        }

        private void logProcessedEvent(ProcessedEvent event) {
            if (event == null) {
                return;
            }
            if (event == ProcessedEvent.INTERACTION) {
                LOG.info("Updated Redis interaction speed-layer state");
            } else if (event == ProcessedEvent.POST_WITH_SCORE) {
                LOG.info("Updated Redis post cold start metadata and sorted set");
            } else if (event == ProcessedEvent.POST_WITHOUT_SCORE) {
                LOG.warn("Updated Redis post metadata but skipped coldstart:posts because event timestamp was missing");
            }
        }
    }

    static ProcessedEvent processDebeziumEvent(
            String value,
            RedisOperations redis,
            ObjectMapper objectMapper,
            int recentViewsLimit
    ) throws Exception {
        JsonNode root = objectMapper.readTree(value);
        JsonNode envelope = root.hasNonNull("payload") ? root.get("payload") : root;
        JsonNode opNode = envelope.get("op");
        if (opNode == null || !"c".equals(opNode.asText())) {
            return null;
        }

        JsonNode after = envelope.get("after");
        if (after == null || after.isNull()) {
            return null;
        }

        String tableName = tableName(envelope);
        if ("interactions".equals(tableName)) {
            return processInteractionCreate(after, redis, recentViewsLimit);
        }
        if ("posts".equals(tableName)) {
            return processPostCreate(envelope, after, redis);
        }

        return null;
    }

    private static ProcessedEvent processInteractionCreate(JsonNode after, RedisOperations redis, int recentViewsLimit) {
        JsonNode userId = after.get("user_id");
        JsonNode postId = after.get("post_id");
        if (userId == null || userId.isNull() || postId == null || postId.isNull()) {
            return null;
        }

        redis.zincrby("trending:global", 1.0, postId.asText());
        String recentViewsKey = "user:" + userId.asText() + ":recent_views";
        redis.lpush(recentViewsKey, postId.asText());
        redis.ltrim(recentViewsKey, 0, recentViewsLimit - 1L);
        return ProcessedEvent.INTERACTION;
    }

    private static ProcessedEvent processPostCreate(JsonNode envelope, JsonNode after, RedisOperations redis) {
        JsonNode postId = after.get("post_id");
        if (postId == null || postId.isNull()) {
            return null;
        }

        String postIdValue = postId.asText();
        redis.hset("post:" + postIdValue + ":meta", postMetaFields(after));

        Long timestampMs = eventTimestampMs(envelope);
        if (timestampMs == null) {
            return ProcessedEvent.POST_WITHOUT_SCORE;
        }

        redis.zadd("coldstart:posts", timestampMs.doubleValue(), postIdValue);
        return ProcessedEvent.POST_WITH_SCORE;
    }

    private static Map<String, String> postMetaFields(JsonNode after) {
        Map<String, String> fields = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> iterator = after.fields();
        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> field = iterator.next();
            JsonNode value = field.getValue();
            if (value == null || value.isNull()) {
                fields.put(field.getKey(), "");
            } else if (value.isValueNode()) {
                fields.put(field.getKey(), value.asText());
            } else {
                fields.put(field.getKey(), value.toString());
            }
        }
        return fields;
    }

    private static String tableName(JsonNode envelope) {
        JsonNode source = envelope.get("source");
        if (source == null || source.isNull()) {
            return null;
        }

        JsonNode table = source.get("table");
        if (table == null || table.isNull()) {
            return null;
        }

        return table.asText();
    }

    private static Long eventTimestampMs(JsonNode envelope) {
        JsonNode timestamp = envelope.get("ts_ms");
        if (timestamp != null && timestamp.canConvertToLong()) {
            return timestamp.asLong();
        }

        JsonNode source = envelope.get("source");
        if (source == null || source.isNull()) {
            return null;
        }

        JsonNode sourceTimestamp = source.get("ts_ms");
        if (sourceTimestamp != null && sourceTimestamp.canConvertToLong()) {
            return sourceTimestamp.asLong();
        }

        return null;
    }

    enum ProcessedEvent {
        INTERACTION,
        POST_WITH_SCORE,
        POST_WITHOUT_SCORE
    }

    interface RedisOperations extends AutoCloseable {
        void zincrby(String key, double increment, String member);

        void lpush(String key, String value);

        void ltrim(String key, long start, long stop);

        void hset(String key, Map<String, String> fields);

        void zadd(String key, double score, String member);

        @Override
        void close();
    }

    private static class JedisRedisOperations implements RedisOperations {
        private final JedisPooled jedis;

        private JedisRedisOperations(JedisPooled jedis) {
            this.jedis = jedis;
        }

        @Override
        public void zincrby(String key, double increment, String member) {
            jedis.zincrby(key, increment, member);
        }

        @Override
        public void lpush(String key, String value) {
            jedis.lpush(key, value);
        }

        @Override
        public void ltrim(String key, long start, long stop) {
            jedis.ltrim(key, start, stop);
        }

        @Override
        public void hset(String key, Map<String, String> fields) {
            jedis.hset(key, fields);
        }

        @Override
        public void zadd(String key, double score, String member) {
            jedis.zadd(key, score, member);
        }

        @Override
        public void close() {
            jedis.close();
        }
    }
}
