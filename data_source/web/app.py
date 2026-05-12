import hashlib
import os
import uuid
from datetime import datetime, timezone
from functools import wraps

import psycopg
import requests
from flask import Flask, Response, jsonify, redirect, render_template, request, session
from psycopg.rows import dict_row


DATABASE_URL = os.environ["DATABASE_URL"]
MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"].rstrip("/")
MINIO_BUCKET = os.environ["MINIO_BUCKET"]
APP_PORT = int(os.environ.get("APP_PORT", "8000"))


app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("APP_SECRET_KEY", "dev-secret-key")


def db_connection():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def make_user_id(username):
    return f"user_{hashlib.md5(username.strip().lower().encode('utf-8')).hexdigest()[:12]}"


def require_user(view_func):
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if "user_id" not in session:
            return jsonify({"error": "authentication_required"}), 401
        return view_func(*args, **kwargs)

    return wrapped


def fetch_post_comments(conn, post_ids, limit_per_post=3):
    if not post_ids:
        return {}

    query = """
        WITH ranked_comments AS (
            SELECT
                c.post_id,
                c.comment_id,
                c.user_id,
                u.username,
                c.body_text,
                c.commented_at_raw,
                ROW_NUMBER() OVER (
                    PARTITION BY c.post_id
                    ORDER BY c.commented_at_raw DESC NULLS LAST, c.comment_id DESC
                ) AS row_num
            FROM comments c
            JOIN users u ON u.user_id = c.user_id
            WHERE c.post_id = ANY(%s)
        )
        SELECT
            post_id,
            comment_id,
            user_id,
            username,
            body_text,
            commented_at_raw
        FROM ranked_comments
        WHERE row_num <= %s
        ORDER BY commented_at_raw DESC NULLS LAST, comment_id DESC
    """

    grouped = {post_id: [] for post_id in post_ids}
    with conn.cursor() as cur:
        cur.execute(query, (post_ids, limit_per_post))
        for row in cur.fetchall():
            grouped[row["post_id"]].append(
                {
                    "comment_id": row["comment_id"],
                    "user_id": row["user_id"],
                    "username": row["username"],
                    "body_text": row["body_text"] or "",
                    "commented_at": row["commented_at_raw"] or "",
                }
            )
    return grouped


def fetch_posts(offset=0, limit=10, viewer_id=None):
    query = """
        SELECT
            p.post_id,
            p.title,
            p.body_text,
            p.excerpt,
            p.tags,
            p.categories,
            p.published_at_raw,
            p.canonical_url,
            u.user_id AS author_id,
            u.username AS author_username,
            pm.object_key,
            COALESCE(comment_stats.comment_count, 0) AS comment_count,
            COALESCE(like_stats.like_count, 0) AS like_count,
            CASE
                WHEN %s::TEXT IS NULL THEN FALSE
                ELSE EXISTS (
                    SELECT 1
                    FROM interactions i
                    WHERE i.post_id = p.post_id
                      AND i.user_id = %s::TEXT
                      AND i.interaction_type = 'like'
                )
            END AS viewer_has_liked
        FROM posts p
        JOIN users u ON u.user_id = p.author_id
        LEFT JOIN post_media pm
            ON pm.post_id = p.post_id
           AND pm.display_order = 1
        LEFT JOIN (
            SELECT post_id, COUNT(*)::INT AS comment_count
            FROM comments
            GROUP BY post_id
        ) comment_stats ON comment_stats.post_id = p.post_id
        LEFT JOIN (
            SELECT post_id, COUNT(*)::INT AS like_count
            FROM interactions
            WHERE interaction_type = 'like'
            GROUP BY post_id
        ) like_stats ON like_stats.post_id = p.post_id
        ORDER BY p.published_at_raw DESC NULLS LAST, p.post_id DESC
        OFFSET %s
        LIMIT %s
    """

    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (viewer_id, viewer_id, offset, limit))
            rows = cur.fetchall()

        post_ids = [row["post_id"] for row in rows]
        comments_by_post = fetch_post_comments(conn, post_ids)

    posts = []
    for row in rows:
        posts.append(
            {
                "post_id": row["post_id"],
                "title": row["title"],
                "body_text": row["body_text"] or "",
                "excerpt": row["excerpt"] or "",
                "tags": row["tags"] or "",
                "categories": row["categories"] or "",
                "published_at": row["published_at_raw"] or "",
                "canonical_url": row["canonical_url"] or "",
                "author": {
                    "user_id": row["author_id"],
                    "username": row["author_username"],
                },
                "image_url": f"/media/{row['object_key']}" if row["object_key"] else "",
                "comment_count": row["comment_count"],
                "like_count": row["like_count"],
                "viewer_has_liked": bool(row["viewer_has_liked"]),
                "comments": comments_by_post.get(row["post_id"], []),
            }
        )

    return posts


@app.get("/")
def index():
    return render_template("index.html", username=session.get("username"))


@app.post("/signin")
def signin():
    payload = request.get_json(silent=True) or {}
    username = (payload.get("username") or "").strip()
    if not username:
        return jsonify({"error": "username_required"}), 400

    user_id = make_user_id(username)

    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT user_id, username
                FROM users
                WHERE LOWER(username) = LOWER(%s)
                LIMIT 1
                """,
                (username,),
            )
            row = cur.fetchone()

            if row is None:
                cur.execute(
                    """
                    INSERT INTO users (
                        user_id,
                        username,
                        account_type,
                        authored_posts_count,
                        comments_count,
                        interest_keywords,
                        joined_on
                    )
                    VALUES (%s, %s, 'reader', 0, 0, NULL, CURRENT_DATE)
                    RETURNING user_id, username
                    """,
                    (user_id, username),
                )
                row = cur.fetchone()
            else:
                cur.execute(
                    """
                    UPDATE users
                    SET username = %s
                    WHERE user_id = %s
                    RETURNING user_id, username
                    """,
                    (username, row["user_id"]),
                )
                row = cur.fetchone()
        conn.commit()

    session["user_id"] = row["user_id"]
    session["username"] = row["username"]
    return jsonify({"user_id": row["user_id"], "username": row["username"]})


@app.post("/signout")
def signout():
    session.clear()
    return jsonify({"ok": True})


@app.get("/api/me")
def me():
    return jsonify(
        {
            "authenticated": "user_id" in session,
            "user_id": session.get("user_id"),
            "username": session.get("username"),
        }
    )


@app.get("/api/posts")
def api_posts():
    limit = min(max(int(request.args.get("limit", 10)), 1), 20)
    offset = max(int(request.args.get("offset", 0)), 0)
    posts = fetch_posts(offset=offset, limit=limit, viewer_id=session.get("user_id"))
    return jsonify(
        {
            "posts": posts,
            "next_offset": offset + len(posts),
            "has_more": len(posts) == limit,
        }
    )


@app.get("/api/posts/<post_id>/comments")
def api_post_comments(post_id):
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.comment_id, c.user_id, u.username, c.body_text, c.commented_at_raw
                FROM comments c
                JOIN users u ON u.user_id = c.user_id
                WHERE c.post_id = %s
                ORDER BY c.commented_at_raw DESC NULLS LAST, c.comment_id DESC
                LIMIT 50
                """,
                (post_id,),
            )
            rows = cur.fetchall()

    return jsonify(
        {
            "comments": [
                {
                    "comment_id": row["comment_id"],
                    "user_id": row["user_id"],
                    "username": row["username"],
                    "body_text": row["body_text"] or "",
                    "commented_at": row["commented_at_raw"] or "",
                }
                for row in rows
            ]
        }
    )


@app.post("/api/posts/<post_id>/like")
@require_user
def like_post(post_id):
    user_id = session["user_id"]

    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT event_id
                FROM interactions
                WHERE user_id = %s
                  AND post_id = %s
                  AND interaction_type = 'like'
                LIMIT 1
                """,
                (user_id, post_id),
            )
            existing = cur.fetchone()

            if existing:
                cur.execute("DELETE FROM interactions WHERE event_id = %s", (existing["event_id"],))
                liked = False
            else:
                cur.execute(
                    """
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
                    VALUES (%s, %s, %s, 'like', %s, %s, 'web', 'feed', NULL)
                    """,
                    (
                        str(uuid.uuid4()),
                        user_id,
                        post_id,
                        now_iso(),
                        f"session_{user_id}",
                    ),
                )
                liked = True

            cur.execute(
                """
                SELECT COUNT(*)::INT AS like_count
                FROM interactions
                WHERE post_id = %s
                  AND interaction_type = 'like'
                """,
                (post_id,),
            )
            like_count = cur.fetchone()["like_count"]
        conn.commit()

    return jsonify({"liked": liked, "like_count": like_count})


@app.post("/api/posts/<post_id>/comments")
@require_user
def create_comment(post_id):
    payload = request.get_json(silent=True) or {}
    body_text = (payload.get("body_text") or "").strip()
    if not body_text:
        return jsonify({"error": "comment_required"}), 400

    comment_id = str(uuid.uuid4())
    occurred_at = now_iso()

    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO comments (
                    comment_id,
                    post_id,
                    user_id,
                    parent_comment_id,
                    body_text,
                    commented_at_raw
                )
                VALUES (%s, %s, %s, NULL, %s, %s)
                """,
                (comment_id, post_id, session["user_id"], body_text, occurred_at),
            )
            cur.execute(
                """
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
                VALUES (%s, %s, %s, 'comment', %s, %s, 'web', 'feed', NULL)
                """,
                (
                    str(uuid.uuid4()),
                    session["user_id"],
                    post_id,
                    occurred_at,
                    f"session_{session['user_id']}",
                ),
            )
            cur.execute(
                """
                UPDATE users
                SET comments_count = comments_count + 1
                WHERE user_id = %s
                """,
                (session["user_id"],),
            )
        conn.commit()

    return jsonify(
        {
            "comment": {
                "comment_id": comment_id,
                "user_id": session["user_id"],
                "username": session["username"],
                "body_text": body_text,
                "commented_at": occurred_at,
            }
        }
    )


@app.get("/media/<path:object_key>")
def media_proxy(object_key):
    url = f"{MINIO_ENDPOINT}/{MINIO_BUCKET}/{object_key}"
    upstream = requests.get(url, timeout=30)
    excluded_headers = {"content-encoding", "content-length", "transfer-encoding", "connection"}
    headers = [(name, value) for name, value in upstream.headers.items() if name.lower() not in excluded_headers]
    return Response(upstream.content, status=upstream.status_code, headers=headers)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=APP_PORT, debug=False)
