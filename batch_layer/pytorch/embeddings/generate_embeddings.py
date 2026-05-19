import os
import json
import numpy as np
import pandas as pd
import torch
import s3fs
from sentence_transformers import SentenceTransformer
from PIL import Image
from io import BytesIO
from transformers import CLIPProcessor, CLIPVisionModelWithProjection

# =====================================================
# CONFIG
# =====================================================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio-batch:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "socialrec-batch")
ASSETS_ENDPOINT = os.getenv("ASSETS_ENDPOINT", "http://minio:9000")
BASE_PATH = f"{MINIO_BUCKET}/gold/embeddings"

# =====================================================
# S3 CONNECT
# =====================================================
fs = s3fs.S3FileSystem(
    endpoint_url=MINIO_ENDPOINT,
    key=MINIO_ACCESS_KEY,
    secret=MINIO_SECRET_KEY,
    use_ssl=False,
    skip_instance_cache=True,
    use_listings_cache=False,
)

def save_parquet(df, path):
    print(f"[SAVE] {path}")
    with fs.open(path, "wb") as f:
        # engine='pyarrow' natively supports saving lists/arrays in columns
        df.to_parquet(f, index=False, engine="pyarrow")

# =====================================================
# LOAD SILVER DATA
# =====================================================
print("Loading SILVER tables...")
users = pd.read_parquet(f"{MINIO_BUCKET}/silver/users", filesystem=fs)
posts = pd.read_parquet(f"{MINIO_BUCKET}/silver/posts", filesystem=fs)
print(f"Users: {len(users)} | Posts: {len(posts)}")

# =====================================================
# CLEAN + TEXT BUILDING
# =====================================================
def safe_json(val):
    try:
        return json.loads(val) if isinstance(val, str) else []
    except:
        return []

# -------------------------
# USERS TEXT
# -------------------------
users["interest_list"] = users.get("interest_keywords", "").apply(safe_json)
users["interest_text"] = users["interest_list"].apply(
    lambda x: " ".join(x) if isinstance(x, list) else "")

user_texts = users["interest_text"].fillna("").tolist()

# -------------------------
# POSTS TEXT
# -------------------------
def safe_list(val):
    try:
        return json.loads(val) if isinstance(val, str) else []
    except:
        return []

posts["tags_list"] = posts.get("tags", "").apply(safe_list)
posts["categories_list"] = posts.get("categories", "").apply(safe_list)

posts["tags_text"] = posts["tags_list"].apply(lambda x: " ".join(x) if isinstance(x, list) else "")
posts["categories_text"] = posts["categories_list"].apply(lambda x: " ".join(x) if isinstance(x, list) else "")

post_texts = (
    posts["title"].fillna("") + " " +
    posts["body_text"].fillna("").str[:512] + " " +
    posts["tags_text"] + " " +
    posts["categories_text"]
).tolist()

# =====================================================
# TEXT EMBEDDINGS
# =====================================================
device = "cuda" if torch.cuda.is_available() else "cpu"
print(f"Device: {device}")

text_model = SentenceTransformer("all-MiniLM-L6-v2", device=device)

print("Encoding users...")
user_embeddings = text_model.encode(
    user_texts,
    batch_size=64,
    normalize_embeddings=True,
    show_progress_bar=True
).astype(np.float32)

print("Encoding posts...")
post_embeddings = text_model.encode(
    post_texts,
    batch_size=64,
    normalize_embeddings=True,
    show_progress_bar=True
).astype(np.float32)

del text_model
if torch.cuda.is_available():
    torch.cuda.empty_cache()

# =====================================================
# IMAGE EMBEDDINGS (CLIP)
# =====================================================
print("Loading CLIP model...")
clip_model = CLIPVisionModelWithProjection.from_pretrained(
    "openai/clip-vit-base-patch32").to(device)
clip_processor = CLIPProcessor.from_pretrained(
    "openai/clip-vit-base-patch32")
clip_model.eval()

# Initialize empty arrays for image embeddings
image_embeddings = np.zeros((len(posts), 512), dtype=np.float32)

def load_image(key):
    try:
        with fs.open(f"socialrec-assets/{key}", "rb") as f:
            return Image.open(BytesIO(f.read())).convert("RGB")
    except Exception as e:
        print(f"[WARN] image failed {key}: {e}")
        return None

print("Encoding images...")
# Ensure "has_image" exists as a column properly
posts["has_image"] = posts.get("has_image", False)

for i, row in posts.iterrows():
    if not row["has_image"]:
        continue
    
    key = row.get("object_key", None)
    if pd.isna(key):
        continue
        
    img = load_image(key)
    if img is None:
        continue
        
    inputs = clip_processor(images=img, return_tensors="pt").to(device)
    with torch.no_grad():
        out = clip_model(**inputs)
        emb = out.image_embeds
        emb = emb / emb.norm(p=2, dim=-1, keepdim=True)
    
    image_embeddings[i] = emb.cpu().numpy()[0].astype(np.float32)

# =====================================================
# MAP IDs TO EMBEDDINGS & SAVE AS PARQUET
# =====================================================
print("Creating direct ID -> Embedding mappings...")

# Reset indices to ensure alignment
users = users.reset_index(drop=True)
posts = posts.reset_index(drop=True)

# 1. Map User IDs directly to their Text Embeddings
user_mapping = pd.DataFrame({
    "user_id": users["user_id"].astype(str),
    "text_embedding": list(user_embeddings) # converts (N, dim) matrix to list of arrays
})

# 2. Map Post IDs directly to their Text and Image Embeddings
post_mapping = pd.DataFrame({
    "post_id": posts["post_id"].astype(str),
    "has_image": posts["has_image"],
    "text_embedding": list(post_embeddings),
    "image_embedding": list(image_embeddings) 
})

# Save the mappings
save_parquet(user_mapping, f"{BASE_PATH}/user_embeddings_mapping.parquet")
save_parquet(post_mapping, f"{BASE_PATH}/post_embeddings_mapping.parquet")

# =====================================================
# DONE
# =====================================================
print("✅ Embedding pipeline completed successfully")
print(f"User Mapping Saved: {len(user_mapping)} rows")
print(f"Post Mapping Saved: {len(post_mapping)} rows")