from elasticsearch import Elasticsearch
import time

es = Elasticsearch("http://elasticsearch:9200")

# wait ES ready (correct way)
for i in range(10):
    try:
        info = es.info()
        print("ES READY:", info["cluster_name"])
        break
    except Exception as e:
        print("waiting ES connection...", repr(e))
        time.sleep(3)


# create index safely
if not es.indices.exists(index="posts"):
    es.indices.create(
        index="posts",
        mappings={
            "properties": {
                "post_id": {"type": "keyword"},
                "content": {"type": "text"},
                "user_id": {"type": "keyword"},
                "like_count": {"type": "integer"},
                "trending_score": {"type": "integer"}
            }
        }
    )
    print("INDEX CREATED")
else:
    print("INDEX EXISTS")