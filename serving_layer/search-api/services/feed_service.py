from clients.es_client import es


def get_feed(size: int = 20):
    res = es.search(
        index="posts",
        size=size,
        sort=[
            {"trending_score": "desc"}
        ]
    )

    return res["hits"]["hits"]