package com.socialrec.speedlayer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaLogJobTest {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void parsesCommaSeparatedTopics() {
        assertEquals(
                List.of("postgres.public.interactions", "postgres.public.posts"),
                KafkaLogJob.parseTopics(" postgres.public.interactions, postgres.public.posts ")
        );
    }

    @Test
    void interactionCreateUpdatesTrendingAndRecentViews() throws Exception {
        FakeRedis redis = new FakeRedis();

        KafkaLogJob.ProcessedEvent result = KafkaLogJob.processDebeziumEvent("""
                {
                  "op": "c",
                  "source": {"table": "interactions"},
                  "after": {"user_id": "42", "post_id": "10001"}
                }
                """, redis, objectMapper, 100);

        assertEquals(KafkaLogJob.ProcessedEvent.INTERACTION, result);
        assertEquals(List.of("zincrby trending:global 1.0 10001", "lpush user:42:recent_views 10001", "ltrim user:42:recent_views 0 99"), redis.calls);
    }

    @Test
    void postCreateWritesMetaAndColdstartScore() throws Exception {
        FakeRedis redis = new FakeRedis();

        KafkaLogJob.ProcessedEvent result = KafkaLogJob.processDebeziumEvent("""
                {
                  "op": "c",
                  "ts_ms": 1710000000123,
                  "source": {"table": "posts", "ts_ms": 1700000000000},
                  "after": {
                    "post_id": "10001",
                    "author_id": "7",
                    "title": "Cold start post",
                    "canonical_url": null,
                    "comments_count": 3
                  }
                }
                """, redis, objectMapper, 100);

        assertEquals(KafkaLogJob.ProcessedEvent.POST_WITH_SCORE, result);
        assertEquals("10001", redis.hashes.get("post:10001:meta").get("post_id"));
        assertEquals("7", redis.hashes.get("post:10001:meta").get("author_id"));
        assertEquals("Cold start post", redis.hashes.get("post:10001:meta").get("title"));
        assertEquals("", redis.hashes.get("post:10001:meta").get("canonical_url"));
        assertEquals("3", redis.hashes.get("post:10001:meta").get("comments_count"));
        assertEquals(1710000000123.0, redis.zsets.get("coldstart:posts").get("10001"));
    }

    @Test
    void postCreateFallsBackToSourceTimestamp() throws Exception {
        FakeRedis redis = new FakeRedis();

        KafkaLogJob.ProcessedEvent result = KafkaLogJob.processDebeziumEvent("""
                {
                  "op": "c",
                  "source": {"table": "posts", "ts_ms": 1700000000000},
                  "after": {"post_id": "10001", "title": "Cold start post"}
                }
                """, redis, objectMapper, 100);

        assertEquals(KafkaLogJob.ProcessedEvent.POST_WITH_SCORE, result);
        assertEquals(1700000000000.0, redis.zsets.get("coldstart:posts").get("10001"));
    }

    @Test
    void postCreateWithoutTimestampStillWritesMeta() throws Exception {
        FakeRedis redis = new FakeRedis();

        KafkaLogJob.ProcessedEvent result = KafkaLogJob.processDebeziumEvent("""
                {
                  "op": "c",
                  "source": {"table": "posts"},
                  "after": {"post_id": "10001", "title": "Cold start post"}
                }
                """, redis, objectMapper, 100);

        assertEquals(KafkaLogJob.ProcessedEvent.POST_WITHOUT_SCORE, result);
        assertEquals("Cold start post", redis.hashes.get("post:10001:meta").get("title"));
        assertFalse(redis.zsets.containsKey("coldstart:posts"));
    }

    @Test
    void nonCreateEventsAreIgnored() throws Exception {
        FakeRedis redis = new FakeRedis();

        KafkaLogJob.ProcessedEvent result = KafkaLogJob.processDebeziumEvent("""
                {
                  "op": "u",
                  "source": {"table": "posts"},
                  "after": {"post_id": "10001"}
                }
                """, redis, objectMapper, 100);

        assertNull(result);
        assertTrue(redis.calls.isEmpty());
        assertTrue(redis.hashes.isEmpty());
    }

    @Test
    void eventsMissingRequiredIdsAreIgnored() throws Exception {
        FakeRedis redis = new FakeRedis();

        assertNull(KafkaLogJob.processDebeziumEvent("""
                {
                  "op": "c",
                  "source": {"table": "interactions"},
                  "after": {"user_id": "42"}
                }
                """, redis, objectMapper, 100));
        assertNull(KafkaLogJob.processDebeziumEvent("""
                {
                  "op": "c",
                  "source": {"table": "posts"},
                  "after": {"title": "Missing ID"}
                }
                """, redis, objectMapper, 100));
        assertTrue(redis.calls.isEmpty());
        assertTrue(redis.hashes.isEmpty());
    }

    private static class FakeRedis implements KafkaLogJob.RedisOperations {
        private final List<String> calls = new ArrayList<>();
        private final Map<String, Map<String, String>> hashes = new HashMap<>();
        private final Map<String, Map<String, Double>> zsets = new HashMap<>();

        @Override
        public void zincrby(String key, double increment, String member) {
            calls.add("zincrby " + key + " " + increment + " " + member);
        }

        @Override
        public void lpush(String key, String value) {
            calls.add("lpush " + key + " " + value);
        }

        @Override
        public void ltrim(String key, long start, long stop) {
            calls.add("ltrim " + key + " " + start + " " + stop);
        }

        @Override
        public void hset(String key, Map<String, String> fields) {
            hashes.put(key, new HashMap<>(fields));
        }

        @Override
        public void zadd(String key, double score, String member) {
            zsets.computeIfAbsent(key, ignored -> new HashMap<>()).put(member, score);
        }

        @Override
        public void close() {
        }
    }
}
