USE socialrec;

-- =====================
-- user_interactions
-- =====================
INSERT INTO user_interactions (user_id, item_id, interaction_type, score, timestamp)
VALUES ('u1', 'i1', 'click', 1.0, 1710000001);

INSERT INTO user_interactions (user_id, item_id, interaction_type, score, timestamp)
VALUES ('u1', 'i2', 'like', 2.0, 1710000002);

INSERT INTO user_interactions (user_id, item_id, interaction_type, score, timestamp)
VALUES ('u2', 'i1', 'view', 0.5, 1710000003);

INSERT INTO user_interactions (user_id, item_id, interaction_type, score, timestamp)
VALUES ('u2', 'i3', 'like', 2.0, 1710000004);

INSERT INTO user_interactions (user_id, item_id, interaction_type, score, timestamp)
VALUES ('u3', 'i2', 'click', 1.0, 1710000005);


-- =====================
-- item_popularity
-- =====================
INSERT INTO item_popularity (item_id, total_interactions, total_score, last_updated)
VALUES ('i1', 2, 3.5, 1710000000);

INSERT INTO item_popularity (item_id, total_interactions, total_score, last_updated)
VALUES ('i2', 3, 5.0, 1710000000);

INSERT INTO item_popularity (item_id, total_interactions, total_score, last_updated)
VALUES ('i3', 2, 3.0, 1710000000);


-- =====================
-- user_profile
-- =====================
INSERT INTO user_profile (user_id, total_interactions, avg_score, last_active)
VALUES ('u1', 5, 1.5, 1710000003);

INSERT INTO user_profile (user_id, total_interactions, avg_score, last_active)
VALUES ('u2', 3, 1.2, 1710000004);

INSERT INTO user_profile (user_id, total_interactions, avg_score, last_active)
VALUES ('u3', 2, 1.8, 1710000005);


-- =====================
-- item_embeddings
-- =====================
INSERT INTO item_embeddings (item_id, embedding)
VALUES ('i1', [0.1, 0.2, 0.3, 0.4]);

INSERT INTO item_embeddings (item_id, embedding)
VALUES ('i2', [0.5, 0.6, 0.7, 0.8]);

INSERT INTO item_embeddings (item_id, embedding)
VALUES ('i3', [0.9, 0.1, 0.2, 0.3]);


-- =====================
-- user_embeddings
-- =====================
INSERT INTO user_embeddings (user_id, embedding)
VALUES ('u1', [0.2, 0.3, 0.4, 0.5]);

INSERT INTO user_embeddings (user_id, embedding)
VALUES ('u2', [0.6, 0.7, 0.8, 0.9]);

INSERT INTO user_embeddings (user_id, embedding)
VALUES ('u3', [0.9, 0.8, 0.7, 0.6]);