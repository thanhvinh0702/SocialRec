import time
import math
from fastapi import FastAPI
from cassandra.cluster import Cluster

app = FastAPI()

cluster = None
session = None


def init_db():
    global cluster, session

    for i in range(30):
        try:
            print(f"Connecting Cassandra... attempt {i+1}/30")
            cluster = Cluster(["cassandra"], port=9042)
            session = cluster.connect("socialrec")

            print("Cassandra connected")
            return

        except Exception as e:
            print("Cassandra not ready:", repr(e))
            time.sleep(3)

    print("Cassandra NOT ready, API still running")
    session = None


@app.on_event("startup")
def startup():
    init_db()



def get_global_popularity(item_id: str) -> float:
    # giả lập global popularity
    return math.log(1 + hash(item_id) % 100)


def recency_score():
    return 1.0


def normalize(x, max_val):
    if max_val == 0:
        return 0
    return x / max_val

def get_candidates(user_id: str):
    query = """
        SELECT item_id, score
        FROM socialrec.user_interactions
        WHERE user_id=%s
    """

    rows = session.execute(query, (user_id,))

    candidates = []

    for r in rows:
        candidates.append({
            "item_id": r.item_id,
            "interaction_score": r.score
        })

    return candidates


def compute_score(interaction, popularity, recency):

    return (
        0.6 * interaction +
        0.3 * popularity +
        0.1 * recency
    )


@app.get("/recommend/{user_id}")
def recommend_user(user_id: str):

    if session is None:
        return {"error": "Cassandra not ready"}

    # 1. Candidate generation
    candidates = get_candidates(user_id)

    if not candidates:
        return {
            "user_id": user_id,
            "recommendations": []
        }

    results = []

    # 2. compute max for normalization
    max_interaction = max(c["interaction_score"] for c in candidates)

    # 3. ranking
    for c in candidates:

        interaction = normalize(c["interaction_score"], max_interaction)
        popularity = get_global_popularity(c["item_id"])
        recency = recency_score()

        final_score = compute_score(interaction, popularity, recency)

        results.append({
            "item_id": c["item_id"],
            "score": final_score,
            "explain": {
                "interaction": interaction,
                "popularity": popularity,
                "recency": recency
            }
        })

    # 4. sort
    results.sort(key=lambda x: x["score"], reverse=True)

    # 5. return top-K
    return {
        "user_id": user_id,
        "k": 10,
        "recommendations": results[:10]
    }