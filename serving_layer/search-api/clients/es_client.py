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