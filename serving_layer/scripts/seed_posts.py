from elasticsearch import Elasticsearch

es = Elasticsearch("http://elasticsearch:9200")

INDEX = "posts"

def create_index_if_not_exists():
    if not es.indices.exists(index=INDEX):
        es.indices.create(
            index=INDEX,
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

def seed_data():
    posts = [
        {
            "post_id": "1",
            "content": "flink kafka elasticsearch big data system",
            "user_id": "u1",
            "like_count": 10,
            "trending_score": 50
        },
        {
            "post_id": "2",
            "content": "I love distributed streaming systems",
            "user_id": "u2",
            "like_count": 30,
            "trending_score": 80
        },
        {
            "post_id": "3",
            "content": "kubernetes elasticsearch cluster clusterip only",
            "user_id": "u3",
            "like_count": 5,
            "trending_score": 20
        }
    ]

    for p in posts:
        es.index(index=INDEX, id=p["post_id"], document=p)

    es.indices.refresh(index=INDEX)
    print("Seed data done")

if __name__ == "__main__":
    create_index_if_not_exists()
    seed_data()