import time
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
            cluster = Cluster(["cassandra"], port = 9042)
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


@app.get("/recommend/{user_id}")
def recommend_user(user_id: str):

    if session is None:
        return {"error": "Cassandra not ready"}

    query = """
        SELECT item_id, score
        FROM socialrec.user_interactions
        WHERE user_id=%s
    """

    rows = session.execute(query, (user_id,))

    results = []

    for r in rows:
        results.append({
            "item_id": r.item_id,
            "score": r.score
        })

    results.sort(key=lambda x: x["score"], reverse=True)

    return {
        "user_id": user_id,
        "recommendations": results[:10]
    }