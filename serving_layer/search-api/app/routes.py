from fastapi import APIRouter
from services.search_service import search_posts
from services.feed_service import get_feed

router = APIRouter()


@router.get("/search")
def search(q: str):
    return search_posts(q)


@router.get("/feed")
def feed():
    return get_feed()

@router.get("/health")
def health():
    return {"status": "ok"}