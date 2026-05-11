import os
import re
import ast
import json
import uuid
import random
import hashlib
import threading
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import requests
from tqdm import tqdm
from datasets import load_dataset

from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DATASET_NAME = "hybridfree/hackaday-posts"
OUTPUT_DIR = Path("hackaday_social_data_small")
IMAGE_DIR = OUTPUT_DIR / "images"

MAX_POSTS = 1000          # doi thanh 1000 neu muon test nhanh
RANDOM_SEED = 42
POST_WORKERS = min(32, (os.cpu_count() or 4) * 4)
IMAGE_WORKERS = min(64, (os.cpu_count() or 4) * 8)

random.seed(RANDOM_SEED)
thread_local = threading.local()
STRING_COMMENT_PATTERN = re.compile(
    r"'comment_id':\s*'(?P<comment_id>[^']*)'.*?"
    r"'author':\s*'(?P<author>[^']*)'.*?"
    r"'timestamp':\s*datetime\.datetime\((?P<timestamp>[^\)]*)\).*?"
    r"'content':\s*'(?P<content>(?:\\.|[^'\\])*)'.*?"
    r"'parent_id':\s*(?P<parent_id>None|'[^']*')",
    re.S,
)


def make_id(prefix, value):
    raw = str(value).encode("utf-8")
    return f"{prefix}_{hashlib.md5(raw).hexdigest()[:12]}"


def safe_text(x):
    if x is None:
        return ""
    if isinstance(x, (list, dict)):
        return json.dumps(x, ensure_ascii=False)
    return str(x)


def clean_filename(text):
    text = re.sub(r"[^a-zA-Z0-9_-]", "_", str(text))
    return text[:80]


def create_session():
    session = requests.Session()

    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    adapter = HTTPAdapter(
        pool_connections=64,
        pool_maxsize=64,
        max_retries=retry
    )

    session.mount("http://", adapter)
    session.mount("https://", adapter)

    session.headers.update({
        "User-Agent": "Mozilla/5.0"
    })

    return session


def get_thread_session():
    session = getattr(thread_local, "session", None)
    if session is None:
        session = create_session()
        thread_local.session = session
    return session


def get_image_filename(url, post_id):
    if not url or not isinstance(url, str) or not url.startswith("http"):
        return ""

    ext = url.split("?")[0].split(".")[-1].lower()
    if ext not in ["jpg", "jpeg", "png", "webp"]:
        ext = "jpg"

    return f"{post_id}.{ext}"


def download_image_with_session(task):
    url, post_id = task

    filename = get_image_filename(url, post_id)
    if not filename:
        return post_id, ""

    image_path = IMAGE_DIR / filename
    relative_path = f"images/{filename}"

    if image_path.exists() and image_path.stat().st_size > 1000:
        return post_id, relative_path

    session = get_thread_session()

    try:
        response = session.get(url, timeout=(5, 20), stream=True)

        if response.status_code == 200:
            content_type = response.headers.get("Content-Type", "")

            if "image" not in content_type.lower():
                return post_id, ""

            temp_path = image_path.with_suffix(image_path.suffix + ".tmp")

            with open(temp_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            if temp_path.stat().st_size > 1000:
                temp_path.replace(image_path)
                return post_id, relative_path
            else:
                temp_path.unlink(missing_ok=True)

    except Exception:
        pass

    return post_id, ""

def download_images_parallel(image_tasks, max_workers=IMAGE_WORKERS):
    """
    image_tasks: list of (image_url, post_id)
    return: dict {post_id: relative_image_path}
    """
    result = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(download_image_with_session, task)
            for task in image_tasks
        ]

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Downloading images"
        ):
            post_id, image_path = future.result()
            result[post_id] = image_path

    return result

def normalize_comment_time(value):
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    return safe_text(value)


def parse_datetime_repr(value):
    parts = [part.strip() for part in str(value).split(",")]

    try:
        numbers = [int(part) for part in parts if part]
        return datetime(*numbers).isoformat()
    except Exception:
        return str(value)


def decode_python_string_literal(value):
    try:
        return ast.literal_eval(f"'{value}'")
    except Exception:
        return value


def iter_comment_dicts(comments):
    if comments is None:
        return

    if hasattr(comments, "tolist") and not isinstance(comments, list):
        comments = comments.tolist()

    if isinstance(comments, dict):
        comments = [comments]
    elif isinstance(comments, tuple):
        comments = list(comments)

    if not isinstance(comments, list):
        return

    stack = list(comments)
    while stack:
        comment = stack.pop(0)
        if hasattr(comment, "tolist") and not isinstance(comment, list):
            comment = comment.tolist()

        if isinstance(comment, list):
            stack = comment + stack
            continue

        if not isinstance(comment, dict):
            continue

        yield comment

        replies = comment.get("replies")
        if hasattr(replies, "tolist") and not isinstance(replies, list):
            replies = replies.tolist()
        if isinstance(replies, dict):
            replies = [replies]
        elif isinstance(replies, tuple):
            replies = list(replies)
        if isinstance(replies, list):
            stack = replies + stack


def extract_comments_from_objects(post_id, comments):
    rows = []
    users = []

    for i, c in enumerate(iter_comment_dicts(comments) or []):
        username = (
            c.get("author")
            or c.get("username")
            or c.get("user")
            or c.get("name")
            or f"anonymous_{i}"
        )

        raw_comment_id = (
            c.get("comment_id")
            or c.get("id")
            or c.get("commentId")
            or f"{post_id}_{i}_{username}"
        )

        user_id = make_id("user", username)
        comment_id = make_id("comment", raw_comment_id)

        text = (
            c.get("text")
            or c.get("content")
            or c.get("body")
            or c.get("comment")
            or ""
        )

        raw_comment_time = (
            c.get("date")
            or c.get("time")
            or c.get("created_at")
            or c.get("timestamp")
            or ""
        )
        parent_comment_id = c.get("parent_id") or ""

        rows.append({
            "comment_id": comment_id,
            "post_id": post_id,
            "user_id": user_id,
            "username": username,
            "comment_text": safe_text(text),
            "comment_time": normalize_comment_time(raw_comment_time),
            "parent_comment_id": safe_text(parent_comment_id)
        })

        users.append(username)

    return rows, users


def extract_comments_from_string(post_id, comments):
    rows = []
    users = []

    for i, match in enumerate(STRING_COMMENT_PATTERN.finditer(comments)):
        username = decode_python_string_literal(match.group("author"))
        raw_comment_id = match.group("comment_id") or f"{post_id}_{i}_{username}"
        raw_parent_id = match.group("parent_id")

        parent_comment_id = ""
        if raw_parent_id and raw_parent_id != "None":
            parent_comment_id = decode_python_string_literal(raw_parent_id[1:-1])

        rows.append({
            "comment_id": make_id("comment", raw_comment_id),
            "post_id": post_id,
            "user_id": make_id("user", username),
            "username": username,
            "comment_text": decode_python_string_literal(match.group("content")),
            "comment_time": parse_datetime_repr(match.group("timestamp")),
            "parent_comment_id": parent_comment_id
        })
        users.append(username)

    return rows, users


def extract_comment_users_and_rows(post_id, comments):
    if comments is None:
        return [], []

    if isinstance(comments, str):
        try:
            parsed_comments = json.loads(comments)
            return extract_comments_from_objects(post_id, parsed_comments)
        except Exception:
            return extract_comments_from_string(post_id, comments)

    return extract_comments_from_objects(post_id, comments)


def generate_event_time():
    start = datetime(2024, 1, 1)
    end = datetime(2026, 5, 1)
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

from collections import defaultdict, Counter
import math


def parse_tags(text):
    text = str(text).lower()
    words = re.findall(r"[a-zA-Z0-9_+#.-]{3,}", text)
    return set(words)


def build_user_interest_profile(posts_df, comments_df):
    """
    Tạo profile sở thích user từ các bài họ đã comment.
    Comment là tín hiệu mạnh.
    """
    user_interest = defaultdict(Counter)

    post_tag_map = {}

    for _, post in posts_df.iterrows():
        post_id = post["post_id"]
        tags = parse_tags(str(post.get("tags", "")) + " " + str(post.get("categories", "")))
        post_tag_map[post_id] = tags

    for _, c in comments_df.iterrows():
        user_id = c["user_id"]
        post_id = c["post_id"]

        for tag in post_tag_map.get(post_id, []):
            user_interest[user_id][tag] += 5   # comment = tín hiệu rất mạnh

    return user_interest, post_tag_map


def interest_match_score(user_id, post_id, user_interest, post_tag_map):
    """
    Score càng cao nếu user từng comment nhiều vào tag giống bài hiện tại.
    """
    user_profile = user_interest.get(user_id, Counter())
    post_tags = post_tag_map.get(post_id, set())

    if not user_profile or not post_tags:
        return 0.0

    score = 0
    total = sum(user_profile.values())

    for tag in post_tags:
        score += user_profile.get(tag, 0)

    return min(score / max(total, 1), 1.0)


def generate_interactions(posts_df, users_df, comments_df):
    interactions = []

    user_ids = users_df["user_id"].tolist()

    user_interest, post_tag_map = build_user_interest_profile(posts_df, comments_df)

    for _, post in tqdm(posts_df.iterrows(), total=len(posts_df), desc="Generating interactions"):
        post_id = post["post_id"]
        author_id = post["author_id"]

        comments_count = int(post.get("comments_count", 0) or 0)
        popularity = min(comments_count / 50, 1.0)

        # Post hot thì nhiều người thấy hơn
        num_candidate_users = random.randint(30, 120) + int(popularity * 200)
        candidate_users = random.sample(user_ids, min(num_candidate_users, len(user_ids)))

        for user_id in candidate_users:
            if user_id == author_id:
                continue

            match_score = interest_match_score(
                user_id=user_id,
                post_id=post_id,
                user_interest=user_interest,
                post_tag_map=post_tag_map
            )

            # xác suất user xem bài
            view_prob = 0.15 + 0.65 * match_score + 0.20 * popularity

            if random.random() > view_prob:
                continue

            event_time = generate_event_time()
            session_id = make_id("session", f"{user_id}_{event_time}_{random.random()}")

            dwell_time = int(
                random.randint(5, 60)
                + match_score * random.randint(60, 300)
                + popularity * random.randint(20, 120)
            )

            interactions.append({
                "event_id": str(uuid.uuid4()),
                "user_id": user_id,
                "post_id": post_id,
                "event_type": "view",
                "timestamp": event_time.isoformat(),
                "session_id": session_id,
                "device": random.choice(["web", "mobile", "tablet"]),
                "source": random.choice(["feed", "search", "profile", "recommendation"]),
                "dwell_time": dwell_time,
                "weight": 1,
                "interest_score": round(match_score, 4)
            })

            # User càng match interest thì càng dễ like/save/share/comment
            like_prob = 0.05 + 0.55 * match_score + 0.15 * popularity
            save_prob = 0.02 + 0.30 * match_score + 0.08 * popularity
            share_prob = 0.01 + 0.18 * match_score + 0.06 * popularity
            comment_prob = 0.005 + 0.15 * match_score + 0.05 * popularity
            follow_prob = 0.005 + 0.12 * match_score + 0.04 * popularity

            if random.random() < like_prob:
                interactions.append({
                    "event_id": str(uuid.uuid4()),
                    "user_id": user_id,
                    "post_id": post_id,
                    "event_type": "like",
                    "timestamp": (event_time + timedelta(seconds=random.randint(5, 120))).isoformat(),
                    "session_id": session_id,
                    "device": random.choice(["web", "mobile", "tablet"]),
                    "source": "post_detail",
                    "dwell_time": "",
                    "weight": 2,
                    "interest_score": round(match_score, 4)
                })

                # like cũng cập nhật sở thích user
                for tag in post_tag_map.get(post_id, []):
                    user_interest[user_id][tag] += 2

            if random.random() < save_prob:
                interactions.append({
                    "event_id": str(uuid.uuid4()),
                    "user_id": user_id,
                    "post_id": post_id,
                    "event_type": "save",
                    "timestamp": (event_time + timedelta(seconds=random.randint(20, 180))).isoformat(),
                    "session_id": session_id,
                    "device": random.choice(["web", "mobile", "tablet"]),
                    "source": "post_detail",
                    "dwell_time": "",
                    "weight": 3,
                    "interest_score": round(match_score, 4)
                })

                for tag in post_tag_map.get(post_id, []):
                    user_interest[user_id][tag] += 3

            if random.random() < share_prob:
                interactions.append({
                    "event_id": str(uuid.uuid4()),
                    "user_id": user_id,
                    "post_id": post_id,
                    "event_type": "share",
                    "timestamp": (event_time + timedelta(seconds=random.randint(30, 240))).isoformat(),
                    "session_id": session_id,
                    "device": random.choice(["web", "mobile", "tablet"]),
                    "source": "post_detail",
                    "dwell_time": "",
                    "weight": 4,
                    "interest_score": round(match_score, 4)
                })

                for tag in post_tag_map.get(post_id, []):
                    user_interest[user_id][tag] += 4

            if random.random() < comment_prob:
                interactions.append({
                    "event_id": str(uuid.uuid4()),
                    "user_id": user_id,
                    "post_id": post_id,
                    "event_type": "comment",
                    "timestamp": (event_time + timedelta(seconds=random.randint(40, 300))).isoformat(),
                    "session_id": session_id,
                    "device": random.choice(["web", "mobile", "tablet"]),
                    "source": "post_detail",
                    "dwell_time": "",
                    "weight": 5,
                    "interest_score": round(match_score, 4)
                })

                for tag in post_tag_map.get(post_id, []):
                    user_interest[user_id][tag] += 5

            if random.random() < follow_prob:
                interactions.append({
                    "event_id": str(uuid.uuid4()),
                    "user_id": user_id,
                    "post_id": post_id,
                    "event_type": "follow_author",
                    "timestamp": (event_time + timedelta(seconds=random.randint(30, 300))).isoformat(),
                    "session_id": session_id,
                    "device": random.choice(["web", "mobile", "tablet"]),
                    "source": "author_profile",
                    "dwell_time": "",
                    "weight": 4,
                    "interest_score": round(match_score, 4)
                })

    # comment thật từ dataset
    for _, c in comments_df.iterrows():
        interactions.append({
            "event_id": str(uuid.uuid4()),
            "user_id": c["user_id"],
            "post_id": c["post_id"],
            "event_type": "comment",
            "timestamp": c["comment_time"] if c["comment_time"] else generate_event_time().isoformat(),
            "session_id": make_id("session", f"{c['user_id']}_{c['post_id']}_comment"),
            "device": random.choice(["web", "mobile", "tablet"]),
            "source": "post_detail",
            "dwell_time": "",
            "weight": 5,
            "interest_score": 1.0
        })

    return pd.DataFrame(interactions)


def process_post_row(task):
    idx, row = task

    title = row.get("title", f"post_{idx}")
    post_id = make_id("post", row.get("url", title + str(idx)))

    author = row.get("author", "unknown_author")
    author_id = make_id("user", author)

    image_url = (
        row.get("featured_image")
        or row.get("image")
        or row.get("image_url")
        or ""
    )

    comments = row.get("comments", None)
    comment_rows, comment_users = extract_comment_users_and_rows(post_id, comments)

    return {
        "index": idx,
        "post": {
            "post_id": post_id,
            "title": safe_text(title),
            "author_id": author_id,
            "author": safe_text(author),
            "url": safe_text(row.get("url", "")),
            "content": safe_text(row.get("content", "")),
            "excerpt": safe_text(row.get("excerpt", "")),
            "categories": safe_text(row.get("categories", "")),
            "tags": safe_text(row.get("tags", "")),
            "published_at": safe_text(row.get("published_at", row.get("date", ""))),
            "comments_count": row.get("comments_count", len(comment_rows)),
            "image_url": safe_text(image_url),
            "image_path": ""
        },
        "comment_rows": comment_rows,
        "usernames": [author, *comment_users],
        "image_task": (image_url, post_id),
    }


def build_user_row(username, posts_df, comments_df):
    user_id = make_id("user", username)

    num_posts = int((posts_df["author"] == username).sum())
    num_comments = int((comments_df["username"] == username).sum()) if len(comments_df) else 0

    if num_posts > 0 and num_comments > 0:
        user_type = "both"
    elif num_posts > 0:
        user_type = "author"
    else:
        user_type = "commenter"

    authored_posts = posts_df[posts_df["author"] == username]
    commented_post_ids = comments_df[comments_df["username"] == username]["post_id"].tolist() if len(comments_df) else []
    commented_posts = posts_df[posts_df["post_id"].isin(commented_post_ids)]

    text_interest = " ".join(
        authored_posts["tags"].astype(str).tolist()
        + authored_posts["categories"].astype(str).tolist()
        + commented_posts["tags"].astype(str).tolist()
        + commented_posts["categories"].astype(str).tolist()
    )

    words = re.findall(r"[a-zA-Z0-9_+#.-]{3,}", text_interest.lower())
    interests = list(dict.fromkeys(words))[:10]

    return {
        "user_id": user_id,
        "username": username,
        "user_type": user_type,
        "num_posts": num_posts,
        "num_comments": num_comments,
        "interests": json.dumps(interests, ensure_ascii=False),
        "created_at": generate_event_time().date().isoformat()
    }


def main():
    OUTPUT_DIR.mkdir(exist_ok=True)
    IMAGE_DIR.mkdir(exist_ok=True)

    print("Loading dataset...")
    ds = load_dataset(DATASET_NAME, split="train")

    if MAX_POSTS:
        ds = ds.select(range(min(MAX_POSTS, len(ds))))

    raw_df = ds.to_pandas()
    print("Columns:", list(raw_df.columns))

    posts_by_index = {}
    comments_all = []
    usernames = set()
    image_tasks = []
    raw_rows = list(enumerate(raw_df.to_dict("records")))

    with ThreadPoolExecutor(max_workers=POST_WORKERS) as executor:
        futures = [executor.submit(process_post_row, task) for task in raw_rows]

        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing posts"):
            result = future.result()
            posts_by_index[result["index"]] = result["post"]
            comments_all.extend(result["comment_rows"])
            usernames.update(result["usernames"])
            image_tasks.append(result["image_task"])

    image_map = download_images_parallel(image_tasks, max_workers=IMAGE_WORKERS)

    posts = []
    for idx in range(len(raw_rows)):
        post = posts_by_index[idx]
        post["image_path"] = image_map.get(post["post_id"], "")
        posts.append(post)

    posts_df = pd.DataFrame(posts)
    comments_df = pd.DataFrame(comments_all)

    with ThreadPoolExecutor(max_workers=POST_WORKERS) as executor:
        user_rows = list(
            tqdm(
                executor.map(lambda username: build_user_row(username, posts_df, comments_df), usernames),
                total=len(usernames),
                desc="Building users"
            )
        )

    users_df = pd.DataFrame(user_rows)

    interactions_df = generate_interactions(posts_df, users_df, comments_df)

    posts_df.to_csv(OUTPUT_DIR / "posts.csv", index=False)
    users_df.to_csv(OUTPUT_DIR / "users.csv", index=False)
    comments_df.to_csv(OUTPUT_DIR / "comments.csv", index=False)
    interactions_df.to_csv(OUTPUT_DIR / "interactions.csv", index=False)

    print("\nDone!")
    print(f"Output folder: {OUTPUT_DIR}")
    print(f"posts.csv: {len(posts_df)} rows")
    print(f"users.csv: {len(users_df)} rows")
    print(f"comments.csv: {len(comments_df)} rows")
    print(f"interactions.csv: {len(interactions_df)} rows")
    print(f"images folder: {IMAGE_DIR}")


if __name__ == "__main__":
    main()
