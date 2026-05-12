DROP TABLE IF EXISTS interactions;
DROP TABLE IF EXISTS comments;
DROP TABLE IF EXISTS post_media;
DROP TABLE IF EXISTS posts;
DROP TABLE IF EXISTS users;

DROP TABLE IF EXISTS staging_interactions;
DROP TABLE IF EXISTS staging_comments;
DROP TABLE IF EXISTS staging_posts;
DROP TABLE IF EXISTS staging_users;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'socialrec') THEN
        ALTER ROLE socialrec WITH REPLICATION;
    END IF;
END $$;

CREATE TABLE staging_users (
    user_id TEXT,
    username TEXT,
    user_type TEXT,
    num_posts TEXT,
    num_comments TEXT,
    interests TEXT,
    created_at TEXT
);

CREATE TABLE staging_posts (
    post_id TEXT,
    title TEXT,
    author_id TEXT,
    author TEXT,
    url TEXT,
    content TEXT,
    excerpt TEXT,
    categories TEXT,
    tags TEXT,
    published_at TEXT,
    comments_count TEXT,
    image_url TEXT,
    image_path TEXT
);

CREATE TABLE staging_comments (
    comment_id TEXT,
    post_id TEXT,
    user_id TEXT,
    username TEXT,
    comment_text TEXT,
    comment_time TEXT,
    parent_comment_id TEXT
);

CREATE TABLE staging_interactions (
    event_id TEXT,
    user_id TEXT,
    post_id TEXT,
    event_type TEXT,
    event_timestamp TEXT,
    session_id TEXT,
    device TEXT,
    source TEXT,
    dwell_time TEXT,
    weight TEXT,
    interest_score TEXT
);

COPY staging_users (
    user_id,
    username,
    user_type,
    num_posts,
    num_comments,
    interests,
    created_at
)
FROM '/seed-data/users.csv'
WITH (FORMAT csv, HEADER true);

COPY staging_posts (
    post_id,
    title,
    author_id,
    author,
    url,
    content,
    excerpt,
    categories,
    tags,
    published_at,
    comments_count,
    image_url,
    image_path
)
FROM '/seed-data/posts.csv'
WITH (FORMAT csv, HEADER true);

COPY staging_comments (
    comment_id,
    post_id,
    user_id,
    username,
    comment_text,
    comment_time,
    parent_comment_id
)
FROM '/seed-data/comments.csv'
WITH (FORMAT csv, HEADER true);

COPY staging_interactions (
    event_id,
    user_id,
    post_id,
    event_type,
    event_timestamp,
    session_id,
    device,
    source,
    dwell_time,
    weight,
    interest_score
)
FROM '/seed-data/interactions.csv'
WITH (FORMAT csv, HEADER true);

CREATE TABLE users (
    user_id TEXT PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    account_type TEXT NOT NULL DEFAULT 'commenter',
    authored_posts_count INTEGER NOT NULL DEFAULT 0,
    comments_count INTEGER NOT NULL DEFAULT 0,
    interest_keywords TEXT,
    joined_on DATE
);

CREATE TABLE posts (
    post_id TEXT PRIMARY KEY,
    author_id TEXT NOT NULL REFERENCES users(user_id),
    title TEXT NOT NULL,
    canonical_url TEXT,
    body_text TEXT,
    excerpt TEXT,
    categories TEXT,
    tags TEXT,
    published_at_raw TEXT,
    comments_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE post_media (
    media_id BIGSERIAL PRIMARY KEY,
    post_id TEXT NOT NULL REFERENCES posts(post_id) ON DELETE CASCADE,
    media_type TEXT NOT NULL DEFAULT 'image',
    source_url TEXT,
    object_key TEXT,
    storage_provider TEXT NOT NULL DEFAULT 'minio',
    display_order INTEGER NOT NULL DEFAULT 1,
    UNIQUE (post_id, object_key)
);

CREATE TABLE comments (
    comment_id TEXT PRIMARY KEY,
    post_id TEXT NOT NULL REFERENCES posts(post_id) ON DELETE CASCADE,
    user_id TEXT NOT NULL REFERENCES users(user_id),
    parent_comment_id TEXT,
    body_text TEXT,
    commented_at_raw TEXT
);

CREATE TABLE interactions (
    event_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(user_id),
    post_id TEXT NOT NULL REFERENCES posts(post_id) ON DELETE CASCADE,
    interaction_type TEXT NOT NULL CHECK (
        interaction_type IN ('view', 'like', 'save', 'share', 'comment', 'follow_author')
    ),
    occurred_at_raw TEXT,
    session_id TEXT,
    device_type TEXT,
    source_surface TEXT,
    dwell_time_seconds INTEGER
);

INSERT INTO users (
    user_id,
    username,
    account_type,
    authored_posts_count,
    comments_count,
    interest_keywords,
    joined_on
)
SELECT
    su.user_id,
    su.username,
    COALESCE(NULLIF(su.user_type, ''), 'commenter'),
    COALESCE(NULLIF(su.num_posts, '')::INTEGER, 0),
    COALESCE(NULLIF(su.num_comments, '')::INTEGER, 0),
    NULLIF(su.interests, ''),
    NULLIF(su.created_at, '')::DATE
FROM staging_users su
WHERE NULLIF(su.user_id, '') IS NOT NULL
  AND NULLIF(su.username, '') IS NOT NULL;

INSERT INTO posts (
    post_id,
    author_id,
    title,
    canonical_url,
    body_text,
    excerpt,
    categories,
    tags,
    published_at_raw,
    comments_count
)
SELECT
    sp.post_id,
    sp.author_id,
    COALESCE(NULLIF(sp.title, ''), '(untitled post)'),
    NULLIF(sp.url, ''),
    NULLIF(sp.content, ''),
    NULLIF(sp.excerpt, ''),
    NULLIF(sp.categories, ''),
    NULLIF(sp.tags, ''),
    NULLIF(sp.published_at, ''),
    COALESCE(NULLIF(sp.comments_count, '')::INTEGER, 0)
FROM staging_posts sp
WHERE NULLIF(sp.post_id, '') IS NOT NULL
  AND NULLIF(sp.author_id, '') IS NOT NULL;

INSERT INTO post_media (
    post_id,
    media_type,
    source_url,
    object_key,
    storage_provider,
    display_order
)
SELECT
    sp.post_id,
    'image',
    NULLIF(sp.image_url, ''),
    NULLIF(sp.image_path, ''),
    'minio',
    1
FROM staging_posts sp
WHERE NULLIF(sp.post_id, '') IS NOT NULL
  AND NULLIF(sp.image_path, '') IS NOT NULL;

INSERT INTO comments (
    comment_id,
    post_id,
    user_id,
    parent_comment_id,
    body_text,
    commented_at_raw
)
SELECT
    sc.comment_id,
    sc.post_id,
    sc.user_id,
    NULLIF(sc.parent_comment_id, ''),
    NULLIF(sc.comment_text, ''),
    NULLIF(sc.comment_time, '')
FROM staging_comments sc
WHERE NULLIF(sc.comment_id, '') IS NOT NULL
  AND NULLIF(sc.post_id, '') IS NOT NULL
  AND NULLIF(sc.user_id, '') IS NOT NULL;

INSERT INTO interactions (
    event_id,
    user_id,
    post_id,
    interaction_type,
    occurred_at_raw,
    session_id,
    device_type,
    source_surface,
    dwell_time_seconds
)
SELECT
    si.event_id,
    si.user_id,
    si.post_id,
    si.event_type,
    NULLIF(si.event_timestamp, ''),
    NULLIF(si.session_id, ''),
    NULLIF(si.device, ''),
    NULLIF(si.source, ''),
    NULLIF(si.dwell_time, '')::INTEGER
FROM staging_interactions si
WHERE NULLIF(si.event_id, '') IS NOT NULL
  AND NULLIF(si.user_id, '') IS NOT NULL
  AND NULLIF(si.post_id, '') IS NOT NULL
  AND NULLIF(si.event_type, '') IS NOT NULL;

DROP TABLE staging_interactions;
DROP TABLE staging_comments;
DROP TABLE staging_posts;
DROP TABLE staging_users;

CREATE INDEX idx_posts_author_id ON posts(author_id);
CREATE INDEX idx_post_media_post_id ON post_media(post_id);
CREATE INDEX idx_comments_post_id ON comments(post_id);
CREATE INDEX idx_comments_user_id ON comments(user_id);
CREATE INDEX idx_comments_parent_comment_id ON comments(parent_comment_id);
CREATE INDEX idx_interactions_post_id ON interactions(post_id);
CREATE INDEX idx_interactions_user_id ON interactions(user_id);
CREATE INDEX idx_interactions_type ON interactions(interaction_type);
CREATE INDEX idx_interactions_session_id ON interactions(session_id);
CREATE UNIQUE INDEX uq_like_interaction_per_user_post
    ON interactions(user_id, post_id, interaction_type)
    WHERE interaction_type = 'like';
