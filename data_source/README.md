# Data Source Stack

This Docker stack provides:

- PostgreSQL for social metadata
- MinIO for image storage
- a simple Flask web app for browsing and interacting with posts

## Start

```bash
cd data_source
docker compose up -d --build
```

## Endpoints

- PostgreSQL: `localhost:5432`
- MinIO API: `http://localhost:9000`
- MinIO Console: `http://localhost:9001`
- Web app: `http://localhost:8000`

## Default Data Source

By default the stack loads:

- CSVs from `../hackaday_social_data_small`
- images from `../hackaday_social_data_small/images`

Override those paths at startup if needed:

```bash
cd data_source
DATASET_DIR=../hackaday_social_data IMAGE_DIR=../hackaday_social_data/images docker compose up -d --build
```

## Web App Features

- username-only sign-in
- lazy-loaded social feed
- image rendering through the app media proxy
- like and unlike posts
- view comments
- add comments

## Reset

If you change the seed CSVs or the SQL init file:

```bash
cd data_source
docker compose down -v
docker compose up -d --build
```
