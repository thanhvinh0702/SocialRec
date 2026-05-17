CREATE KEYSPACE IF NOT EXISTS socialrec
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

CREATE TABLE IF NOT EXISTS socialrec.user_interactions (
    user_id text,
    item_id text,
    interaction_type text,
    score float,
    timestamp bigint,
    PRIMARY KEY (user_id, item_id)
);

CREATE TABLE IF NOT EXISTS socialrec.item_popularity (
    item_id text PRIMARY KEY,
    total_interactions int,
    total_score float,
    last_updated bigint
);

CREATE TABLE IF NOT EXISTS socialrec.user_profile (
    user_id text PRIMARY KEY,
    total_interactions int,
    avg_score float,
    last_active bigint
);

CREATE TABLE IF NOT EXISTS socialrec.item_embeddings (
    item_id text PRIMARY KEY,
    embedding list<float>
);

CREATE TABLE IF NOT EXISTS socialrec.user_embeddings (
    user_id text PRIMARY KEY,
    embedding list<float>
);