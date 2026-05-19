from clients.cassandra_client import get_feed_by_user


def build_feed(user_id: str):
    rows = get_feed_by_user(user_id)

    return [
        {
            "post_id": r.post_id,
            "content": r.content,
            "created_at": str(r.created_at)
        }
        for r in rows
    ]