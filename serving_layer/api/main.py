from fastapi import FastAPI
import requests

app = FastAPI()

RECOMMENDER_URL = "http://recommender:8001"


@app.get("/recommend/{user_id}")
def recommend(user_id: str):

    res = requests.get(f"{RECOMMENDER_URL}/recommend/{user_id}")

    return res.json()