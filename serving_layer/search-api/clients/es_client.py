from elasticsearch import Elasticsearch

es = Elasticsearch("http://elasticsearch:9200")

def search(q):
    return es.search(
        index="posts",
        query={
            "match": {
                "content": q
            }
        }
    )

def get_feed():
    return es.search(
        index="posts",
        size=20,
        sort=[{"trending_score": "desc"}],
        query={"match_all": {}}
    )