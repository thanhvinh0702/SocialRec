from clients.es_client import search


def search_posts(q: str):
    res = search(q)

    return res["hits"]["hits"]