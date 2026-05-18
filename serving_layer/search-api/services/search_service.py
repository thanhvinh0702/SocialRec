from clients.es_client import es


def search_posts(query: str, size: int = 20):
    res = es.search(
        index="posts",
        size=size,
        query={
            "match": {
                "content": query
            }
        }
    )

    return res["hits"]["hits"]