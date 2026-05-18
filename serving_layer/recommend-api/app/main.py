import time
import math
import random
from fastapi import FastAPI
from cassandra.cluster import Cluster
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Counter, Histogram

app = FastAPI()

request_counter = Counter(
    "recommendation_requests_total",
    "Total recommendation requests"
)

latency_histogram = Histogram(
    "recommendation_latency_seconds",
    "Recommendation latency"
)

fallback_counter = Counter(
    "recommendation_fallback_total",
    "Fallback to trending"
)

Instrumentator().instrument(app).expose(app)

cluster = None
session = None


def init_db():
    global cluster, session

    for i in range(30):
        try:
            print(f"[DB] Connecting Cassandra... attempt {i+1}/30")

            cluster = Cluster(["cassandra"], port=9042)
            session = cluster.connect("socialrec")

            print("[DB] Cassandra connected")
            return

        except Exception as e:
            print("[DB] not ready:", repr(e))
            time.sleep(3)

    print("[DB] Cassandra NOT ready")
    session = None


@app.on_event("startup")
def startup():
    init_db()


def normalize(x, max_val):
    return x / max_val if max_val > 0 else 0


def get_global_popularity(item_id: str) -> float:
    # giả lập batch popularity (thay bằng Cassandra/Mongo sau)
    return math.log(1 + (hash(item_id) % 100))


def get_recency_score() -> float:
    # giả lập realtime freshness
    return random.uniform(0.5, 1.0)


def get_hot_score(item_id: str) -> float:
    # giả lập speed layer (Redis sau này)
    return random.uniform(0, 1)


def get_candidates(user_id: str):
    if session is None:
        return []

    query = """
        SELECT item_id, score
        FROM socialrec.user_interactions
        WHERE user_id=%s
    """

    rows = session.execute(query, (user_id,))

    return [
        {
            "item_id": r.item_id,
            "interaction_score": r.score
        }
        for r in rows
    ]


def compute_score(interaction, popularity, recency, hot):
    return (
        0.45 * interaction +
        0.25 * popularity +
        0.15 * recency +
        0.15 * hot
    )


def trending_fallback():
    fallback_counter.inc()

    return [
        {
            "item_id": f"trend_{i}",
            "score": random.uniform(0.7, 1.0),
            "reason": "fallback trending"
        }
        for i in range(10)
    ]

@app.get("/")
def root():
    return {"msg": "serving layer running"}


@app.get("/recommend/{user_id}")
def recommend(user_id: str):
    start = time.time()

    request_counter.inc()

    if session is None:
        return {
            "error": "Cassandra not ready"
        }

    # 1. Get candidates (batch layer output)
    candidates = get_candidates(user_id)

    # fallback nếu cold start
    if not candidates:
        return {
            "user_id": user_id,
            "recommendations": trending_fallback()
        }

    # 2. normalization
    max_interaction = max(c["interaction_score"] for c in candidates)

    results = []

    # 3. ranking
    for c in candidates:
        interaction = normalize(c["interaction_score"], max_interaction)
        popularity = get_global_popularity(c["item_id"])
        recency = get_recency_score()
        hot = get_hot_score(c["item_id"])

        score = compute_score(interaction, popularity, recency, hot)

        results.append({
            "item_id": c["item_id"],
            "score": score,
            "explain": {
                "interaction": interaction,
                "popularity": popularity,
                "recency": recency,
                "hot": hot
            }
        })

    # 4. sort
    results.sort(key=lambda x: x["score"], reverse=True)

    # 5. latency metric
    latency_histogram.observe(time.time() - start)

    return {
        "user_id": user_id,
        "k": 10,
        "recommendations": results[:10]
    }