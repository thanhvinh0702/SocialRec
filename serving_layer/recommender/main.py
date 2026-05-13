from fastapi import FastAPI
from cassandra.cluster import Cluster
from recommender import recommend

app = FastAPI()

cluster = Cluster(["cassandra"])
session = cluster.connect()
session.set_keyspace("socialrec")


@app.get("/recommend/{user_id}")
def get_recommendation(user_id: str):
    result = recommend(user_id, session)
    return {
        "user_id": user_id,
        "recommendations": result
    }