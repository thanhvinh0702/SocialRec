CREATE KEYSPACE IF NOT EXISTS socialrec
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

CREATE TABLE IF NOT EXISTS socialrec.user_interactions (
    user_id text,
    item_id text,
    score float,
    PRIMARY KEY (user_id, item_id)
);