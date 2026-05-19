from fastapi import FastAPI

from prometheus_fastapi_instrumentator import Instrumentator

from app.search import router as search_router
from app.feed import router as feed_router
from app.post import router as post_router

app = FastAPI()

app.include_router(search_router)
app.include_router(feed_router)
app.include_router(post_router)


@app.get("/health")
def health():
    return {"status": "ok"}


# Prometheus metrics endpoint
Instrumentator().instrument(app).expose(app)